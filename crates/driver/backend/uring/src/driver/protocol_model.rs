use veloq_driver_core::{
    driver::{CancelMode, CancelTicket, OpToken},
    slot::Generation,
};
use veloq_std::{vec, vec::Vec};

/// 提交边界的协议回执。每种回执都携带完整的消费证据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionScope {
    Enter,
    ExtendedWait,
    SqPoll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionReceipt {
    NoEntries {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    ConsumedPrefix {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    PublishedToSqPoll {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    Rejected {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
        error: SubmissionError,
    },
    Unknown {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
        error: SubmissionError,
    },
}

impl SubmissionReceipt {
    fn no_entries(requested: usize) -> Self {
        Self::NoEntries {
            requested,
            published: requested,
            consumed: 0,
            scope: SubmissionScope::Enter,
        }
    }

    fn no_entries_wait(requested: usize) -> Self {
        Self::NoEntries {
            requested,
            published: requested,
            consumed: 0,
            scope: SubmissionScope::ExtendedWait,
        }
    }

    fn consumed(requested: usize, consumed: usize) -> Self {
        Self::ConsumedPrefix {
            requested,
            published: requested,
            consumed,
            scope: SubmissionScope::Enter,
        }
    }

    fn published(requested: usize, published: usize) -> Self {
        Self::PublishedToSqPoll {
            requested,
            published,
            consumed: 0,
            scope: SubmissionScope::SqPoll,
        }
    }

    fn rejected(requested: usize, error: SubmissionError) -> Self {
        Self::Rejected {
            requested,
            published: requested,
            consumed: 0,
            scope: SubmissionScope::Enter,
            error,
        }
    }

    fn unknown(requested: usize, error: SubmissionError) -> Self {
        Self::Unknown {
            requested,
            published: requested,
            consumed: 0,
            scope: SubmissionScope::Enter,
            error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionError {
    KernelRejected,
    ReceiptUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageEntry {
    User(OpToken),
    Cancel {
        ticket: CancelTicket,
        target: OpToken,
    },
    Waker {
        generation: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagePhase {
    Staged,
    Published,
    KernelOutstanding,
    Completed,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerError {
    Capacity,
    DuplicateToken(OpToken),
    DuplicateTicket(CancelTicket),
    InvalidReceipt,
    InvalidTransition(OpToken),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedRecord {
    entry: StageEntry,
    phase: StagePhase,
}

/// 无内核依赖的 staged ledger 模型。
///
/// 模型刻意保留记录直到显式 CQE 结算，因此 Published 不能被误认为
/// KernelOutstanding，Unknown 也不会留下可重试的 SQE。
struct StagedLedger {
    capacity: usize,
    records: Vec<StagedRecord>,
}

impl StagedLedger {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: Vec::new(),
        }
    }

    fn stage(&mut self, entry: StageEntry) -> Result<(), LedgerError> {
        self.stage_batch(core::slice::from_ref(&entry))
    }

    /// 预检整个批次后再写入，模拟 SQ 写入 Full 时事务整体回滚。
    fn stage_batch(&mut self, entries: &[StageEntry]) -> Result<(), LedgerError> {
        if self.records.len() + entries.len() > self.capacity {
            return Err(LedgerError::Capacity);
        }

        for (index, entry) in entries.iter().copied().enumerate() {
            match entry {
                StageEntry::User(token) => {
                    let duplicate = self.records.iter().any(|record| {
                        record.phase != StagePhase::Completed
                            && matches!(record.entry, StageEntry::User(existing) if existing == token)
                    }) || entries[..index].iter().any(
                        |candidate| matches!(candidate, StageEntry::User(existing) if *existing == token),
                    );
                    if duplicate {
                        return Err(LedgerError::DuplicateToken(token));
                    }
                }
                StageEntry::Cancel { ticket, .. } => {
                    let duplicate = self.records.iter().any(|record| {
                        record.phase != StagePhase::Completed
                            && matches!(record.entry, StageEntry::Cancel { ticket: existing, .. } if existing == ticket)
                    }) || entries[..index].iter().any(|candidate| {
                        matches!(candidate, StageEntry::Cancel { ticket: existing, .. } if *existing == ticket)
                    });
                    if duplicate {
                        return Err(LedgerError::DuplicateTicket(ticket));
                    }
                }
                StageEntry::Waker { .. } => {}
            }
        }

        self.records
            .extend(entries.iter().copied().map(|entry| StagedRecord {
                entry,
                phase: StagePhase::Staged,
            }));
        Ok(())
    }

    fn apply_receipt(&mut self, receipt: SubmissionReceipt) -> Result<(), LedgerError> {
        match receipt {
            SubmissionReceipt::NoEntries {
                requested,
                published,
                consumed,
                ..
            } if published <= requested && consumed == 0 => Ok(()),
            SubmissionReceipt::Rejected {
                requested,
                published,
                consumed,
                ..
            } => {
                if published <= requested && consumed == 0 {
                    Ok(())
                } else {
                    Err(LedgerError::InvalidReceipt)
                }
            }
            SubmissionReceipt::ConsumedPrefix {
                requested,
                published,
                consumed,
                ..
            } => {
                if published > requested || consumed > published {
                    return Err(LedgerError::InvalidReceipt);
                }
                self.apply_prefix(requested, consumed, StagePhase::KernelOutstanding)
            }
            SubmissionReceipt::PublishedToSqPoll {
                requested,
                published,
                consumed,
                ..
            } => {
                if published > requested || consumed != 0 {
                    return Err(LedgerError::InvalidReceipt);
                }
                self.apply_prefix(published, published, StagePhase::Published)
            }
            SubmissionReceipt::Unknown {
                requested,
                published,
                consumed,
                ..
            } if published <= requested && consumed == 0 => {
                for record in &mut self.records {
                    if record.phase == StagePhase::Staged {
                        record.phase = StagePhase::Quarantined;
                    }
                }
                Ok(())
            }
            SubmissionReceipt::NoEntries { .. } | SubmissionReceipt::Unknown { .. } => {
                Err(LedgerError::InvalidReceipt)
            }
        }
    }

    fn apply_prefix(
        &mut self,
        requested: usize,
        progressed: usize,
        phase: StagePhase,
    ) -> Result<(), LedgerError> {
        let staged = self
            .records
            .iter()
            .filter(|record| record.phase == StagePhase::Staged)
            .count();
        if requested > staged {
            return Err(LedgerError::InvalidReceipt);
        }

        let mut seen = 0;
        for record in &mut self.records {
            if record.phase != StagePhase::Staged {
                continue;
            }
            if seen < progressed {
                record.phase = phase;
            }
            seen += 1;
            if seen == requested {
                break;
            }
        }
        Ok(())
    }

    fn settle_user_cqe(&mut self, token: OpToken) -> Result<(), LedgerError> {
        let Some(index) = self.records.iter().position(
            |record| matches!(record.entry, StageEntry::User(existing) if existing == token),
        ) else {
            return Err(LedgerError::InvalidTransition(token));
        };

        let record = self.records[index];
        if !matches!(
            record.phase,
            StagePhase::Published | StagePhase::KernelOutstanding
        ) {
            return Err(LedgerError::InvalidTransition(token));
        }
        self.records[index].phase = StagePhase::Completed;
        self.records.remove(index);
        Ok(())
    }

    fn phase_of(&self, token: OpToken) -> Option<StagePhase> {
        self.records.iter().find_map(|record| {
            matches!(record.entry, StageEntry::User(existing) if existing == token)
                .then_some(record.phase)
        })
    }

    fn snapshot(&self) -> Vec<StagedRecord> {
        self.records.clone()
    }

    fn quarantined_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.phase == StagePhase::Quarantined)
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BacklogEntry {
    token: OpToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BacklogError {
    Capacity,
    Duplicate,
    GenerationMismatch,
    Missing,
}

#[derive(Debug, Clone, Copy, Default)]
struct BacklogLink {
    entry: Option<BacklogEntry>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// 固定容量的双向 backlog 链表模型。
struct BacklogLedger {
    links: Vec<BacklogLink>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    peak_len: usize,
}

impl BacklogLedger {
    fn new(capacity: usize) -> Self {
        Self {
            links: vec![BacklogLink::default(); capacity],
            head: None,
            tail: None,
            len: 0,
            peak_len: 0,
        }
    }

    fn insert(&mut self, entry: BacklogEntry) -> Result<(), BacklogError> {
        let index = entry.token.index();
        let Some(link) = self.links.get(index) else {
            return Err(BacklogError::Capacity);
        };
        if let Some(existing) = link.entry {
            return if existing.token == entry.token {
                Err(BacklogError::Duplicate)
            } else {
                Err(BacklogError::GenerationMismatch)
            };
        }

        let previous_tail = self.tail;
        self.links[index] = BacklogLink {
            entry: Some(entry),
            prev: previous_tail,
            next: None,
        };
        if let Some(previous_tail) = previous_tail {
            self.links[previous_tail].next = Some(index);
        } else {
            self.head = Some(index);
        }
        self.tail = Some(index);
        self.len += 1;
        self.peak_len = self.peak_len.max(self.len);
        Ok(())
    }

    fn remove(&mut self, token: OpToken) -> Result<BacklogEntry, BacklogError> {
        let Some(link) = self.links.get(token.index()) else {
            return Err(BacklogError::Capacity);
        };
        let Some(entry) = link.entry else {
            return Err(BacklogError::Missing);
        };
        if entry.token != token {
            return Err(BacklogError::GenerationMismatch);
        }
        Ok(self.unlink(token.index()))
    }

    fn pop_front(&mut self) -> Option<BacklogEntry> {
        self.head.map(|index| self.unlink(index))
    }

    fn unlink(&mut self, index: usize) -> BacklogEntry {
        let link = self.links[index];
        let entry = link.entry.expect("linked backlog node must have an entry");
        if let Some(previous) = link.prev {
            self.links[previous].next = link.next;
        } else {
            self.head = link.next;
        }
        if let Some(next) = link.next {
            self.links[next].prev = link.prev;
        } else {
            self.tail = link.prev;
        }
        self.links[index] = BacklogLink::default();
        self.len -= 1;
        entry
    }

    fn len(&self) -> usize {
        self.len
    }

    fn peak_len(&self) -> usize {
        self.peak_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelSubmitOutcome {
    Submitted { ticket: CancelTicket },
    AlreadyPending { ticket: CancelTicket },
    Merged { ticket: CancelTicket },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelError {
    Capacity,
    TicketExhausted,
    UnknownTicket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReconcileOutcome {
    Deferred,
    Resolved,
    Quarantined,
}

#[derive(Debug, Clone, Copy)]
struct CancelIntent {
    target: OpToken,
    mode: CancelMode,
    ticket: CancelTicket,
    not_found: bool,
    target_active: bool,
    target_completed: bool,
}

/// 取消目标账本模型：重复 target 只产生一个 ticket，并将 ENOENT 延迟到收割同步点。
struct CancelReconcileLedger {
    capacity: usize,
    intents: Vec<CancelIntent>,
    quarantined_targets: Vec<OpToken>,
    next_ticket: u64,
}

impl CancelReconcileLedger {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            intents: Vec::new(),
            quarantined_targets: Vec::new(),
            next_ticket: 1,
        }
    }

    fn request(
        &mut self,
        target: OpToken,
        mode: CancelMode,
    ) -> Result<CancelSubmitOutcome, CancelError> {
        if let Some(intent) = self
            .intents
            .iter_mut()
            .find(|intent| intent.target == target)
        {
            if mode == CancelMode::Abandon && intent.mode != CancelMode::Abandon {
                intent.mode = CancelMode::Abandon;
                return Ok(CancelSubmitOutcome::Merged {
                    ticket: intent.ticket,
                });
            }
            return Ok(CancelSubmitOutcome::AlreadyPending {
                ticket: intent.ticket,
            });
        }
        if self.intents.len() == self.capacity {
            return Err(CancelError::Capacity);
        }

        let ticket =
            CancelTicket::try_new(self.next_ticket).map_err(|_| CancelError::TicketExhausted)?;
        self.next_ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or(CancelError::TicketExhausted)?;
        self.intents.push(CancelIntent {
            target,
            mode,
            ticket,
            not_found: false,
            target_active: true,
            target_completed: false,
        });
        Ok(CancelSubmitOutcome::Submitted { ticket })
    }

    fn ack_not_found(&mut self, ticket: CancelTicket) -> Result<(), CancelError> {
        let Some(intent) = self
            .intents
            .iter_mut()
            .find(|intent| intent.ticket == ticket)
        else {
            return Err(CancelError::UnknownTicket);
        };
        intent.not_found = true;
        Ok(())
    }

    fn observe_target_completion(&mut self, target: OpToken) -> bool {
        let Some(intent) = self
            .intents
            .iter_mut()
            .find(|intent| intent.target == target)
        else {
            return false;
        };
        intent.target_active = false;
        intent.target_completed = true;
        true
    }

    fn reconcile(
        &mut self,
        ticket: CancelTicket,
        batch_exhausted: bool,
    ) -> Result<CancelReconcileOutcome, CancelError> {
        let Some(index) = self
            .intents
            .iter()
            .position(|intent| intent.ticket == ticket)
        else {
            return Err(CancelError::UnknownTicket);
        };
        let intent = self.intents[index];
        if !intent.not_found {
            return Ok(CancelReconcileOutcome::Deferred);
        }
        if intent.target_completed || !intent.target_active {
            self.intents.remove(index);
            return Ok(CancelReconcileOutcome::Resolved);
        }
        if !batch_exhausted {
            return Ok(CancelReconcileOutcome::Deferred);
        }

        self.intents.remove(index);
        self.quarantined_targets.push(intent.target);
        Ok(CancelReconcileOutcome::Quarantined)
    }

    fn settle_late_target_completion(&mut self, target: OpToken) -> bool {
        let Some(index) = self
            .quarantined_targets
            .iter()
            .position(|candidate| *candidate == target)
        else {
            return false;
        };
        self.quarantined_targets.remove(index);
        true
    }

    fn mode_for(&self, target: OpToken) -> Option<CancelMode> {
        self.intents
            .iter()
            .find(|intent| intent.target == target)
            .map(|intent| intent.mode)
    }

    fn ticket_for(&self, target: OpToken) -> Option<CancelTicket> {
        self.intents
            .iter()
            .find(|intent| intent.target == target)
            .map(|intent| intent.ticket)
    }

    fn pending_len(&self) -> usize {
        self.intents.len()
    }

    fn quarantined(&self, target: OpToken) -> bool {
        self.quarantined_targets.contains(&target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, Generation::new(generation)).expect("test token")
    }

    fn user(index: usize, generation: u32) -> StageEntry {
        StageEntry::User(token(index, generation))
    }

    #[test]
    fn consumed_receipt_advances_only_the_reported_prefix() {
        let mut ledger = StagedLedger::new(3);
        let first = token(0, 1);
        let second = token(1, 1);
        let third = token(2, 1);
        ledger
            .stage_batch(&[
                StageEntry::User(first),
                StageEntry::User(second),
                StageEntry::User(third),
            ])
            .expect("stage batch");

        ledger
            .apply_receipt(SubmissionReceipt::consumed(3, 2))
            .expect("partial receipt");

        assert_eq!(ledger.phase_of(first), Some(StagePhase::KernelOutstanding));
        assert_eq!(ledger.phase_of(second), Some(StagePhase::KernelOutstanding));
        assert_eq!(ledger.phase_of(third), Some(StagePhase::Staged));
    }

    #[test]
    fn zero_and_rejected_receipts_leave_staged_entries_retriable() {
        let mut ledger = StagedLedger::new(2);
        let first = token(0, 1);
        let second = token(1, 1);
        ledger.stage(user(0, 1)).expect("stage first");
        ledger
            .apply_receipt(SubmissionReceipt::no_entries(1))
            .expect("zero receipt");
        ledger
            .apply_receipt(SubmissionReceipt::no_entries_wait(1))
            .expect("timed wait receipt");
        ledger
            .apply_receipt(SubmissionReceipt::rejected(
                1,
                SubmissionError::KernelRejected,
            ))
            .expect("rejected receipt");
        assert_eq!(ledger.phase_of(first), Some(StagePhase::Staged));

        ledger.stage(user(1, 1)).expect("stage second");
        assert_eq!(ledger.phase_of(second), Some(StagePhase::Staged));
    }

    #[test]
    fn unknown_receipt_quarantines_entries_instead_of_retrying_them() {
        let mut ledger = StagedLedger::new(2);
        let first = token(0, 1);
        ledger.stage(user(0, 1)).expect("stage first");

        ledger
            .apply_receipt(SubmissionReceipt::unknown(
                1,
                SubmissionError::ReceiptUnavailable,
            ))
            .expect("unknown receipt");

        assert_eq!(ledger.phase_of(first), Some(StagePhase::Quarantined));
        assert_eq!(ledger.quarantined_count(), 1);
        assert_eq!(
            ledger.stage(user(0, 1)),
            Err(LedgerError::DuplicateToken(first))
        );
    }

    #[test]
    fn sqpoll_published_does_not_become_kernel_outstanding_before_cqe() {
        let mut ledger = StagedLedger::new(1);
        let first = token(0, 1);
        ledger.stage(user(0, 1)).expect("stage first");

        ledger
            .apply_receipt(SubmissionReceipt::published(1, 1))
            .expect("published receipt");

        assert_eq!(ledger.phase_of(first), Some(StagePhase::Published));
        ledger.settle_user_cqe(first).expect("published CQE");
        assert_eq!(ledger.phase_of(first), None);
    }

    #[test]
    fn stage_full_rolls_back_the_whole_batch_and_rejects_duplicate_tokens() {
        let mut ledger = StagedLedger::new(1);
        let first = token(0, 1);
        ledger.stage(user(0, 1)).expect("stage first");
        let before = ledger.snapshot();

        assert_eq!(ledger.stage(user(1, 1)), Err(LedgerError::Capacity));
        assert_eq!(ledger.snapshot(), before);

        let mut duplicate_ledger = StagedLedger::new(2);
        duplicate_ledger.stage(user(0, 1)).expect("stage first");
        assert_eq!(
            duplicate_ledger.stage(user(0, 1)),
            Err(LedgerError::DuplicateToken(first))
        );
        assert_eq!(duplicate_ledger.snapshot().len(), 1);
    }

    #[test]
    fn staged_control_entries_share_the_same_receipt_boundary() {
        let target = token(0, 1);
        let ticket = CancelTicket::try_new(1).expect("cancel ticket");
        let mut ledger = StagedLedger::new(2);

        ledger
            .stage_batch(&[
                StageEntry::Cancel { ticket, target },
                StageEntry::Waker { generation: 1 },
            ])
            .expect("control entries should stage atomically");
        ledger
            .apply_receipt(SubmissionReceipt::consumed(2, 2))
            .expect("control receipt");
        assert_eq!(ledger.snapshot().len(), 2);
    }

    #[test]
    fn backlog_remove_is_generation_safe_and_keeps_links_consistent() {
        let first = token(1, 1);
        let second = token(2, 1);
        let replacement = token(1, 2);
        let mut backlog = BacklogLedger::new(3);

        backlog
            .insert(BacklogEntry { token: first })
            .expect("insert first");
        backlog
            .insert(BacklogEntry { token: second })
            .expect("insert second");
        assert_eq!(backlog.len(), 2);
        assert_eq!(backlog.peak_len(), 2);
        assert_eq!(
            backlog.remove(replacement),
            Err(BacklogError::GenerationMismatch)
        );
        assert_eq!(backlog.remove(first).expect("remove first").token, first);
        assert_eq!(backlog.pop_front().expect("pop second").token, second);
        assert_eq!(backlog.len(), 0);

        backlog
            .insert(BacklogEntry { token: replacement })
            .expect("reuse index with new generation");
        assert_eq!(
            backlog.pop_front().expect("pop replacement").token,
            replacement
        );
    }

    #[test]
    fn backlog_capacity_is_fixed_and_duplicate_entries_do_not_grow_it() {
        let first = token(0, 1);
        let second = token(1, 1);
        let mut backlog = BacklogLedger::new(1);

        backlog
            .insert(BacklogEntry { token: first })
            .expect("insert first");
        assert_eq!(
            backlog.insert(BacklogEntry { token: first }),
            Err(BacklogError::Duplicate)
        );
        assert_eq!(
            backlog.insert(BacklogEntry { token: second }),
            Err(BacklogError::Capacity)
        );
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.peak_len(), 1);
    }

    #[test]
    fn repeated_cancel_requests_merge_and_abandon_wins() {
        let target = token(0, 1);
        let mut ledger = CancelReconcileLedger::new(1);
        let first = ledger
            .request(target, CancelMode::UserVisible)
            .expect("first cancel");
        let ticket = match first {
            CancelSubmitOutcome::Submitted { ticket } => ticket,
            _ => panic!("first cancel must submit"),
        };

        assert_eq!(
            ledger.request(target, CancelMode::UserVisible),
            Ok(CancelSubmitOutcome::AlreadyPending { ticket })
        );
        assert_eq!(
            ledger.request(target, CancelMode::Abandon),
            Ok(CancelSubmitOutcome::Merged { ticket })
        );
        assert_eq!(ledger.mode_for(target), Some(CancelMode::Abandon));
        assert_eq!(ledger.pending_len(), 1);
        assert_eq!(ledger.ticket_for(target), Some(ticket));
    }

    #[test]
    fn cancel_enoent_waits_for_same_batch_target_cqe_before_resolving() {
        let target = token(0, 1);
        let mut ledger = CancelReconcileLedger::new(1);
        let ticket = match ledger.request(target, CancelMode::Abandon).expect("cancel") {
            CancelSubmitOutcome::Submitted { ticket } => ticket,
            _ => panic!("cancel must submit"),
        };
        ledger.ack_not_found(ticket).expect("ENOENT ack");
        assert_eq!(
            ledger.reconcile(ticket, false),
            Ok(CancelReconcileOutcome::Deferred)
        );

        assert!(ledger.observe_target_completion(target));
        assert_eq!(
            ledger.reconcile(ticket, true),
            Ok(CancelReconcileOutcome::Resolved)
        );
        assert_eq!(ledger.pending_len(), 0);
        assert!(!ledger.quarantined(target));
    }

    #[test]
    fn cancel_enoent_defers_across_drives_then_quarantines_and_accepts_late_cqe() {
        let target = token(0, 1);
        let mut ledger = CancelReconcileLedger::new(1);
        let ticket = match ledger
            .request(target, CancelMode::UserVisible)
            .expect("cancel")
        {
            CancelSubmitOutcome::Submitted { ticket } => ticket,
            _ => panic!("cancel must submit"),
        };
        ledger.ack_not_found(ticket).expect("ENOENT ack");
        assert_eq!(
            ledger.reconcile(ticket, false),
            Ok(CancelReconcileOutcome::Deferred)
        );
        assert_eq!(
            ledger.reconcile(ticket, true),
            Ok(CancelReconcileOutcome::Quarantined)
        );
        assert!(ledger.quarantined(target));
        assert_eq!(ledger.pending_len(), 0);
        assert!(ledger.settle_late_target_completion(target));
        assert!(!ledger.quarantined(target));
    }
}
