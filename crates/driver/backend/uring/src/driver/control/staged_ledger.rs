use veloq_driver_core::driver::{
    CancelTicket, CompletionControlKind, CompletionToken, CompletionTokenClass, OpToken,
};
use veloq_std::{vec, vec::Vec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedEntry {
    User(OpToken),
    Cancel {
        ticket: CancelTicket,
        target: OpToken,
    },
    Waker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedLedgerError {
    Capacity,
    DuplicateUser(OpToken),
    DuplicateCancel(CancelTicket),
    InvalidReceipt,
    InvalidTransition,
    GenerationMismatch {
        existing: OpToken,
        candidate: OpToken,
    },
}

/// Fixed-capacity FIFO ledger for SQEs written to the user-space SQ.
///
/// The ledger uses an intrusive free-list and an intrusive FIFO.  A completion may remove any
/// token without shifting or allocating metadata, while staging rollback can still remove the
/// most recently allocated node in O(1).  The token-indexed user table is only a uniqueness
/// index; phase transitions are stored with the complete entry and never inferred from a queue
/// prefix.
/// 该固定容量账本以侵入式空闲链表和 FIFO 管理已入队 SQE；按 token 索引的表仅用于唯一性校验。
pub(crate) struct StagedLedger {
    links: Vec<StagedLink>,
    free_head: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    user_entries: Vec<Option<OpToken>>,
    capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedPhase {
    Staged,
    Published,
    KernelOutstanding,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedRecord {
    entry: StagedEntry,
    phase: StagedPhase,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StagedLink {
    record: Option<StagedRecord>,
    prev: Option<usize>,
    next: Option<usize>,
    free_next: Option<usize>,
}

impl StagedLedger {
    pub(crate) fn new(capacity: usize) -> Self {
        let mut links = vec![StagedLink::default(); capacity];
        for (index, link) in links.iter_mut().enumerate() {
            link.free_next = index.checked_add(1).filter(|next| *next < capacity);
        }
        Self {
            free_head: (capacity != 0).then_some(0),
            links,
            head: None,
            tail: None,
            len: 0,
            user_entries: vec![None; capacity],
            capacity,
        }
    }

    pub(crate) fn validate(&self, entry: StagedEntry) -> Result<(), StagedLedgerError> {
        if self.len >= self.capacity {
            return Err(StagedLedgerError::Capacity);
        }
        if let StagedEntry::User(candidate) = entry {
            let Some(active) = self.user_entries.get(candidate.index()).copied() else {
                return Err(StagedLedgerError::Capacity);
            };
            if let Some(active) = active {
                if active == candidate {
                    return Err(StagedLedgerError::DuplicateUser(candidate));
                }
                return Err(StagedLedgerError::GenerationMismatch {
                    existing: active,
                    candidate,
                });
            }
        }

        if let StagedEntry::Cancel {
            ticket: candidate,
            target: candidate_target,
        } = entry
        {
            let mut current = self.head;
            while let Some(index) = current {
                let link = self.links[index];
                if let Some(StagedRecord {
                    entry:
                        StagedEntry::Cancel {
                            ticket: active,
                            target: active_target,
                        },
                    ..
                }) = link.record
                    && active == candidate
                {
                    if active_target == candidate_target {
                        return Err(StagedLedgerError::DuplicateCancel(candidate));
                    }
                    return Err(StagedLedgerError::GenerationMismatch {
                        existing: active_target,
                        candidate: candidate_target,
                    });
                }
                current = link.next;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn push(&mut self, entry: StagedEntry) -> Result<(), StagedLedgerError> {
        self.validate(entry)?;
        self.push_validated(entry);
        Ok(())
    }

    pub(crate) fn push_validated(&mut self, entry: StagedEntry) {
        debug_assert!(self.len < self.capacity);
        let index = self
            .free_head
            .expect("validated staged entry must have a free ledger node");
        let previous_tail = self.tail;
        self.free_head = self.links[index].free_next;
        self.links[index] = StagedLink {
            record: Some(StagedRecord {
                entry,
                phase: StagedPhase::Staged,
            }),
            prev: previous_tail,
            next: None,
            free_next: None,
        };
        if let StagedEntry::User(token) = entry {
            self.user_entries[token.index()] = Some(token);
        }
        if let Some(previous_tail) = previous_tail {
            self.links[previous_tail].next = Some(index);
        } else {
            self.head = Some(index);
        }
        self.tail = Some(index);
        self.len += 1;
    }

    #[cfg(test)]
    pub(crate) fn pop_front(&mut self) -> Option<StagedEntry> {
        self.head.map(|index| self.unlink(index))
    }

    pub(crate) fn pop_back(&mut self) -> Option<StagedEntry> {
        self.tail.map(|index| self.unlink(index))
    }

    fn clear_index(&mut self, entry: StagedEntry) {
        if let StagedEntry::User(token) = entry
            && self.user_entries.get(token.index()).copied().flatten() == Some(token)
        {
            self.user_entries[token.index()] = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn unpublished_len(&self) -> usize {
        let mut count = 0;
        let mut current = self.head;
        while let Some(index) = current {
            let link = self.links[index];
            if link
                .record
                .is_some_and(|record| record.phase == StagedPhase::Staged)
            {
                count += 1;
            }
            current = link.next;
        }
        count
    }

    pub(crate) fn mark_published(&mut self) -> usize {
        let mut count = 0;
        let mut current = self.head;
        while let Some(index) = current {
            let next = self.links[index].next;
            if let Some(record) = self.links[index].record.as_mut()
                && record.phase == StagedPhase::Staged
            {
                record.phase = StagedPhase::Published;
                count += 1;
            }
            current = next;
        }
        count
    }

    pub(crate) fn mark_consumed(
        &mut self,
        requested: usize,
        consumed: usize,
        published_in_queue: usize,
    ) -> Result<usize, StagedLedgerError> {
        if consumed > requested || published_in_queue > requested {
            return Err(StagedLedgerError::InvalidReceipt);
        }

        let consumed_unpublished = consumed.saturating_sub(published_in_queue);
        let unpublished = self.unpublished_len();
        if consumed_unpublished > unpublished {
            return Err(StagedLedgerError::InvalidReceipt);
        }

        let mut marked = 0;
        let mut current = self.head;
        while let Some(index) = current {
            let next = self.links[index].next;
            if let Some(record) = self.links[index].record.as_mut()
                && record.phase == StagedPhase::Staged
                && marked < consumed_unpublished
            {
                record.phase = StagedPhase::KernelOutstanding;
                marked += 1;
            }
            current = next;
        }
        Ok(marked)
    }

    pub(crate) fn quarantine_unpublished(&mut self) -> usize {
        let mut count = 0;
        let mut current = self.head;
        while let Some(index) = current {
            let next = self.links[index].next;
            if let Some(record) = self.links[index].record.as_mut()
                && record.phase == StagedPhase::Staged
            {
                record.phase = StagedPhase::Quarantined;
                count += 1;
            }
            current = next;
        }
        count
    }

    pub(crate) fn settle_completion(
        &mut self,
        token: CompletionToken,
        final_completion: bool,
    ) -> Result<bool, StagedLedgerError> {
        let mut current = self.head;
        let index = loop {
            let Some(index) = current else {
                return Ok(false);
            };
            let link = self.links[index];
            if link.record.is_some_and(|record| record.matches(token)) {
                break index;
            }
            current = link.next;
        };

        let record = self.links[index]
            .record
            .expect("matched staged ledger node must contain a record");
        if record.phase == StagedPhase::Staged {
            return Err(StagedLedgerError::InvalidTransition);
        }
        if final_completion {
            self.unlink(index);
        }
        Ok(true)
    }

    pub(crate) fn for_each_kernel_user(&self, mut visit: impl FnMut(OpToken)) {
        let mut current = self.head;
        while let Some(index) = current {
            let link = self.links[index];
            if let Some(record) = link.record
                && record.phase == StagedPhase::KernelOutstanding
                && let StagedEntry::User(token) = record.entry
            {
                visit(token);
            }
            current = link.next;
        }
    }

    pub(crate) fn for_each_kernel_cancel(&self, mut visit: impl FnMut(CancelTicket, OpToken)) {
        let mut current = self.head;
        while let Some(index) = current {
            let link = self.links[index];
            if let Some(record) = link.record
                && record.phase == StagedPhase::KernelOutstanding
                && let StagedEntry::Cancel { ticket, target } = record.entry
            {
                visit(ticket, target);
            }
            current = link.next;
        }
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    fn unlink(&mut self, index: usize) -> StagedEntry {
        let link = self.links[index];
        let record = link
            .record
            .expect("linked staged ledger node must contain a record");
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
        self.clear_index(record.entry);
        self.links[index] = StagedLink {
            free_next: self.free_head,
            ..StagedLink::default()
        };
        self.free_head = Some(index);
        self.len -= 1;
        record.entry
    }
}

impl StagedRecord {
    fn matches(self, token: CompletionToken) -> bool {
        match (self.entry, token.classify()) {
            (StagedEntry::User(expected), CompletionTokenClass::User(actual)) => expected == actual,
            (
                StagedEntry::Cancel { ticket, .. },
                CompletionTokenClass::Control {
                    kind: CompletionControlKind::Cancel,
                    payload,
                },
            ) => CancelTicket::try_new(payload).ok() == Some(ticket),
            (
                StagedEntry::Waker,
                CompletionTokenClass::Control {
                    kind: CompletionControlKind::Waker,
                    payload: 0,
                },
            ) => true,
            _ => false,
        }
    }
}
