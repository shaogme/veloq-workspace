use crate::driver::registration::file_table::OwnedFdOwnershipTicket;
use crate::{
    driver::{
        control::{
            CancelIntentError, CancelRequestDisposition, PendingCancel, UringCancelManager,
            UringTimerWheel, UringWakerManager,
        },
        control::{ControlPlaneEvent, ControlPlaneObserver},
        env::{
            CompletionControlParts, CompletionLedgerParts, CompletionMetadata,
            SubmissionControlParts, SubmissionLedgerParts, SubmissionSidecarParts,
        },
        lifecycle::CancellationPhase,
    },
    op::CompletionCleanupHintFn,
};
use veloq_driver_core::{
    driver::{
        CancelRequest, CancelTicket, CompletionControlKind, CompletionToken, CompletionTokenClass,
        OpToken, RawCompletion,
    },
    slot::Generation,
};
use veloq_std::{
    collections::{HashMap, HashSet},
    time::Instant,
    vec,
    vec::Vec,
};
use veloq_wheel::TimerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogEntry {
    token: OpToken,
}

impl BacklogEntry {
    pub(crate) const fn token(self) -> OpToken {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BacklogError {
    Capacity,
    Duplicate,
    GenerationMismatch,
    Missing,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BacklogLink {
    entry: Option<BacklogEntry>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// Fixed-capacity, generation-aware FIFO backlog.
///
/// The slot index is the node address.  A node can only contain one generation, so removing a
/// stale token can never remove a newer operation that reused the same registry slot.  Unlinking
/// updates both neighbours directly; there are no stale queue nodes and no membership map to
/// maintain in the hot path.
pub(crate) struct SubmissionBacklog {
    links: Vec<BacklogLink>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    peak_len: usize,
}

impl SubmissionBacklog {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            links: vec![BacklogLink::default(); capacity],
            head: None,
            tail: None,
            len: 0,
            peak_len: 0,
        }
    }

    pub(crate) fn push(&mut self, token: OpToken) -> Result<(), BacklogError> {
        let index = token.index();
        let Some(link) = self.links.get(index) else {
            return Err(BacklogError::Capacity);
        };
        if let Some(existing) = link.entry {
            return if existing.token == token {
                Err(BacklogError::Duplicate)
            } else {
                Err(BacklogError::GenerationMismatch)
            };
        }
        if self.len == self.links.len() {
            return Err(BacklogError::Capacity);
        }

        let previous_tail = self.tail;
        self.links[index] = BacklogLink {
            entry: Some(BacklogEntry { token }),
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

    pub(crate) fn front(&self) -> Option<BacklogEntry> {
        self.head.and_then(|index| self.links[index].entry)
    }

    pub(crate) fn pop_front(&mut self) -> Option<BacklogEntry> {
        self.head.map(|index| self.unlink(index))
    }

    pub(crate) fn remove(&mut self, token: OpToken) -> Result<BacklogEntry, BacklogError> {
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

    pub(crate) fn contains(&self, token: OpToken) -> bool {
        self.links
            .get(token.index())
            .and_then(|link| link.entry)
            .is_some_and(|entry| entry.token == token)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn peak_len(&self) -> usize {
        self.peak_len
    }

    fn unlink(&mut self, index: usize) -> BacklogEntry {
        let link = self.links[index];
        let entry = link
            .entry
            .expect("linked backlog node must contain an entry");
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

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn entries(&self) -> Vec<BacklogEntry> {
        let mut entries = Vec::with_capacity(self.len);
        let mut current = self.head;
        while let Some(index) = current {
            let link = self.links[index];
            if let Some(entry) = link.entry {
                entries.push(entry);
            }
            current = link.next;
        }
        entries
    }
}

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

/// A typed post-completion action owned by the uring control plane.
///
/// The identity is deliberately carried with the action instead of being reconstructed by the
/// executor. This makes a stale completion unable to apply an effect to a recycled slot and gives
/// tests a deterministic sequence to assert against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UringControlEffect {
    sequence: u64,
    token: Option<OpToken>,
    generation: Option<Generation>,
    kind: UringControlEffectKind,
}

impl UringControlEffect {
    pub(crate) const fn kind(self) -> UringControlEffectKind {
        self.kind
    }

    pub(crate) const fn token(self) -> Option<OpToken> {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UringControlEffectKind {
    CancelAck {
        cancel_ticket: CancelTicket,
        phase: CancellationPhase,
    },
    CancelReconcile {
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CloseUnregister {
        ticket: OwnedFdOwnershipTicket,
    },
    WakerRebuild {
        generation: u64,
    },
    WakerRearm {
        generation: u64,
    },
    UdpRearm {
        logical_receiver_generation: u32,
    },
    BacklogKick,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeferredCancelReconcile {
    cancel_ticket: CancelTicket,
    request: PendingCancel,
    raw: RawCompletion,
    since: Instant,
}

impl DeferredCancelReconcile {
    pub(crate) const fn cancel_ticket(self) -> CancelTicket {
        self.cancel_ticket
    }

    pub(crate) const fn request(self) -> PendingCancel {
        self.request
    }

    pub(crate) const fn raw(self) -> RawCompletion {
        self.raw
    }

    pub(crate) const fn since(self) -> Instant {
        self.since
    }
}

/// Deferred effects produced while routing one completion or one CQ batch.
///
/// Completion routing only appends to this queue. The driver drains it after the core has
/// finished routing the current completion or the complete CQ batch, which keeps control-plane
/// side effects out of the slot borrow and lets cancel ENOENT be reconciled against all user CQEs
/// in the batch.
pub(crate) struct UringPostCompletionEffects {
    effects: Vec<UringControlEffect>,
    capacity: usize,
    next_sequence: u64,
    waker_rebuild_generation: Option<u64>,
    waker_rebuild_index: Option<usize>,
    waker_rearm_generation: Option<u64>,
    waker_rearm_index: Option<usize>,
    backlog_kick: bool,
    overflowed: bool,
}

impl UringPostCompletionEffects {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            effects: Vec::with_capacity(capacity),
            capacity,
            next_sequence: 0,
            waker_rebuild_generation: None,
            waker_rebuild_index: None,
            waker_rearm_generation: None,
            waker_rearm_index: None,
            backlog_kick: false,
            overflowed: false,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, effect: UringControlEffect) {
        let replace_index = match effect.kind {
            UringControlEffectKind::WakerRearm { generation } => {
                if self.waker_rearm_generation == Some(generation) {
                    return;
                }
                self.waker_rearm_index
            }
            UringControlEffectKind::WakerRebuild { generation } => {
                if self.waker_rebuild_generation == Some(generation) {
                    return;
                }
                self.waker_rebuild_index
            }
            UringControlEffectKind::BacklogKick => {
                if self.backlog_kick {
                    return;
                }
                None
            }
            UringControlEffectKind::CancelAck { .. }
            | UringControlEffectKind::CancelReconcile { .. }
            | UringControlEffectKind::CloseUnregister { .. }
            | UringControlEffectKind::UdpRearm { .. } => None,
        };
        if let Some(index) = replace_index {
            self.effects[index] = effect;
            match effect.kind {
                UringControlEffectKind::WakerRearm { generation } => {
                    self.waker_rearm_generation = Some(generation);
                }
                UringControlEffectKind::WakerRebuild { generation } => {
                    self.waker_rebuild_generation = Some(generation);
                }
                UringControlEffectKind::BacklogKick
                | UringControlEffectKind::CancelAck { .. }
                | UringControlEffectKind::CancelReconcile { .. }
                | UringControlEffectKind::CloseUnregister { .. }
                | UringControlEffectKind::UdpRearm { .. } => {}
            }
            return;
        }
        if self.effects.len() >= self.capacity {
            self.overflowed = true;
            return;
        }
        let index = self.effects.len();
        match effect.kind {
            UringControlEffectKind::WakerRearm { generation } => {
                self.waker_rearm_generation = Some(generation);
                self.waker_rearm_index = Some(index);
            }
            UringControlEffectKind::WakerRebuild { generation } => {
                self.waker_rebuild_generation = Some(generation);
                self.waker_rebuild_index = Some(index);
            }
            UringControlEffectKind::BacklogKick => self.backlog_kick = true,
            UringControlEffectKind::CancelAck { .. }
            | UringControlEffectKind::CancelReconcile { .. }
            | UringControlEffectKind::CloseUnregister { .. }
            | UringControlEffectKind::UdpRearm { .. } => {}
        }
        self.effects.push(effect);
    }

    #[inline]
    pub(crate) fn append(
        &mut self,
        token: Option<OpToken>,
        generation: Option<Generation>,
        kind: UringControlEffectKind,
    ) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.push(UringControlEffect {
            sequence,
            token,
            generation,
            kind,
        });
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn extend(&mut self, other: Self) {
        for mut effect in other.effects {
            effect.sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.wrapping_add(1);
            self.push(effect);
        }
        self.overflowed |= other.overflowed;
    }

    #[inline]
    pub(crate) fn drain_into(&mut self, destination: &mut Self) {
        for mut effect in self.effects.drain(..) {
            effect.sequence = destination.next_sequence;
            destination.next_sequence = destination.next_sequence.wrapping_add(1);
            destination.push(effect);
        }
        destination.overflowed |= self.overflowed;
        self.waker_rebuild_generation = None;
        self.waker_rebuild_index = None;
        self.waker_rearm_generation = None;
        self.waker_rearm_index = None;
        self.backlog_kick = false;
        self.overflowed = false;
        self.next_sequence = 0;
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &UringControlEffect> {
        self.effects.iter()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.effects.len()
    }

    #[inline]
    pub(crate) fn is_overflowed(&self) -> bool {
        self.overflowed
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.effects.clear();
        self.waker_rebuild_generation = None;
        self.waker_rebuild_index = None;
        self.waker_rearm_generation = None;
        self.waker_rearm_index = None;
        self.backlog_kick = false;
        self.overflowed = false;
        self.next_sequence = 0;
    }

    #[inline]
    pub(crate) fn has_backlog_kick(&self) -> bool {
        self.backlog_kick
    }

    #[cfg(test)]
    pub(crate) fn effects(&self) -> &[UringControlEffect] {
        &self.effects
    }
}

impl Default for UringPostCompletionEffects {
    fn default() -> Self {
        Self::with_capacity(64)
    }
}

/// The single owner of uring's backend control-plane state.
pub(crate) struct UringControlPlane {
    backlog: SubmissionBacklog,
    staged_entries: StagedLedger,
    cancellations: UringCancelManager,
    waker: UringWakerManager,
    timers: UringTimerWheel,
    completion_cleanup_hints: HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    completion_cleanup_capacity: usize,
    observer: ControlPlaneObserver,
    post: UringPostCompletionEffects,
    deferred_cancel_reconciles: Vec<DeferredCancelReconcile>,
    quarantined_timers: HashSet<OpToken>,
    quarantined_tokens: HashSet<OpToken>,
    waker_stage_pending: bool,
    #[cfg(feature = "test-hooks")]
    push_entry_failure: bool,
}

impl UringControlPlane {
    #[cfg(test)]
    pub(crate) fn new(waker: UringWakerManager) -> Self {
        Self::with_capacity(waker, 1024)
    }

    pub(crate) fn with_capacity(waker: UringWakerManager, capacity: usize) -> Self {
        let completion_cleanup_capacity = capacity.saturating_mul(2);
        let post_capacity = capacity.saturating_mul(4).saturating_add(8);
        Self {
            backlog: SubmissionBacklog::new(capacity),
            staged_entries: StagedLedger::new(capacity),
            cancellations: UringCancelManager::with_capacity(capacity),
            waker,
            timers: UringTimerWheel::new(),
            completion_cleanup_hints: HashMap::with_capacity_and_hasher(
                completion_cleanup_capacity,
                Default::default(),
            ),
            completion_cleanup_capacity,
            observer: ControlPlaneObserver::default(),
            post: UringPostCompletionEffects::with_capacity(post_capacity),
            deferred_cancel_reconciles: Vec::with_capacity(capacity),
            quarantined_timers: HashSet::default(),
            quarantined_tokens: HashSet::default(),
            waker_stage_pending: false,
            #[cfg(feature = "test-hooks")]
            push_entry_failure: false,
        }
    }

    pub(crate) fn waker(&self) -> &UringWakerManager {
        &self.waker
    }

    pub(crate) fn waker_mut(&mut self) -> &mut UringWakerManager {
        &mut self.waker
    }

    pub(crate) fn timers(&self) -> &UringTimerWheel {
        &self.timers
    }

    pub(crate) fn timers_mut(&mut self) -> &mut UringTimerWheel {
        &mut self.timers
    }

    pub(crate) fn cancel_timer(&mut self, task_id: TimerId, token: OpToken) {
        self.timers.cancel(task_id);
        self.observer
            .record(ControlPlaneEvent::TimerCancel { task_id, token });
    }

    // Backlog commands are deliberately generation-aware and record their observer event at the
    // mutation site. Callers cannot remove an entry without keeping the control-plane observer in
    // sync.
    pub(crate) fn backlog_contains(&self, token: OpToken) -> bool {
        self.backlog.contains(token)
    }

    pub(crate) fn backlog_front(&self) -> Option<BacklogEntry> {
        self.backlog.front()
    }

    pub(crate) fn backlog_len(&self) -> usize {
        self.backlog.len()
    }

    pub(crate) fn push_backlog(&mut self, token: OpToken) -> Result<(), BacklogError> {
        let result = self.backlog.push(token);
        if result.is_ok() {
            self.observer.record(ControlPlaneEvent::BacklogPush(token));
        }
        result
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<BacklogEntry> {
        let entry = self.backlog.pop_front();
        if let Some(entry) = entry {
            self.observer
                .record(ControlPlaneEvent::BacklogPop(entry.token()));
            Some(entry)
        } else {
            None
        }
    }

    pub(crate) fn remove_backlog(&mut self, token: OpToken) -> Result<BacklogEntry, BacklogError> {
        let result = self.backlog.remove(token);
        if result.is_ok() {
            self.observer
                .record(ControlPlaneEvent::BacklogRemove(token));
        }
        result
    }

    pub(crate) fn try_recv_cancel(&mut self) -> Option<CancelRequest> {
        self.cancellations.try_recv_remote()
    }

    pub(crate) fn remote_cancel_sender(&self) -> veloq_driver_core::driver::RemoteCancelSender {
        self.cancellations.remote_sender()
    }

    pub(crate) fn pending_cancel_len(&self) -> usize {
        self.cancellations.pending_len()
    }

    pub(crate) fn front_pending_cancel(&self) -> Option<PendingCancel> {
        self.cancellations.front_pending().copied()
    }

    pub(crate) fn cancel_ticket_for(&self, token: OpToken) -> Option<CancelTicket> {
        self.cancellations.ticket_for(token)
    }

    pub(crate) fn request_cancel(
        &mut self,
        request: PendingCancel,
    ) -> Result<CancelRequestDisposition, CancelIntentError> {
        self.cancellations.request(request)
    }

    pub(crate) fn mark_cancel_staged(
        &mut self,
        ticket: CancelTicket,
        target: OpToken,
    ) -> Result<(), CancelIntentError> {
        self.cancellations.mark_staged(ticket, target)
    }

    pub(crate) fn fail_cancel_request(&mut self, ticket: CancelTicket, target: OpToken) {
        self.cancellations.cancel_request_failed(ticket, target);
    }

    pub(crate) fn remove_pending_cancel(&mut self, target: OpToken) -> Option<PendingCancel> {
        self.cancellations.remove_pending_target(target)
    }

    pub(crate) fn cancel_ticket_for_completion(
        &mut self,
        ticket: CancelTicket,
        target: OpToken,
    ) -> Option<PendingCancel> {
        self.cancellations.finish_ticket(ticket, target)
    }

    pub(crate) fn cancel_in_flight_len(&self) -> usize {
        self.cancellations.in_flight_len()
    }

    pub(crate) fn clear_cancel_in_flight(&mut self) {
        self.cancellations.clear_in_flight();
    }

    pub(crate) fn for_each_kernel_cancel(&self, operation: impl FnMut(CancelTicket, OpToken)) {
        self.staged_entries.for_each_kernel_cancel(operation);
    }

    pub(crate) fn for_each_kernel_user(&self, operation: impl FnMut(OpToken)) {
        self.staged_entries.for_each_kernel_user(operation);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn backlog_entries(&self) -> Vec<BacklogEntry> {
        self.backlog.entries()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn pending_cancel_targets(&self) -> Vec<OpToken> {
        self.cancellations.pending_targets().collect()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn in_flight_cancel_targets(&self) -> Vec<(CancelTicket, OpToken)> {
        self.cancellations.in_flight_targets().collect()
    }

    pub(crate) fn completion_cleanup_hints_mut(
        &mut self,
    ) -> &mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>> {
        &mut self.completion_cleanup_hints
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn completion_cleanup_hints(
        &self,
    ) -> &HashMap<CompletionToken, Option<CompletionCleanupHintFn>> {
        &self.completion_cleanup_hints
    }

    pub(crate) fn observer_mut(&mut self) -> &mut ControlPlaneObserver {
        &mut self.observer
    }

    pub(crate) fn set_waker_stage_pending(&mut self, pending: bool) {
        self.waker_stage_pending = pending;
    }

    pub(crate) const fn waker_stage_pending(&self) -> bool {
        self.waker_stage_pending
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_push_entry_failure(&mut self) {
        self.push_entry_failure = true;
    }

    pub(crate) fn submission_parts<'d>(&'d mut self) -> SubmissionControlParts<'d> {
        let staged_entries = &mut self.staged_entries;
        let timers = &mut self.timers;
        let observer = &mut self.observer;
        let completion_cleanup_hints = &mut self.completion_cleanup_hints;
        let completion_cleanup_capacity = self.completion_cleanup_capacity;
        let cancel_capacity = self.cancellations.capacity();
        let pending_cancel_cqes = self.cancellations.in_flight_mut();
        let ledger =
            SubmissionLedgerParts::new(staged_entries, timers, observer, completion_cleanup_hints);
        #[cfg(not(feature = "test-hooks"))]
        let sidecar = SubmissionSidecarParts::new(
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
        );
        #[cfg(feature = "test-hooks")]
        let sidecar = SubmissionSidecarParts::new(
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
            &mut self.push_entry_failure,
        );
        SubmissionControlParts::new(ledger, sidecar)
    }

    pub(crate) fn completion_parts<'d>(&'d mut self) -> CompletionControlParts<'d> {
        let waker_view = self.waker.hooks_view();
        let metadata = CompletionMetadata::new(waker_view.buf_len(), waker_view.generation());
        let pending_cancel_cqes = self.cancellations.in_flight_mut();
        let completion_cleanup_hints = &mut self.completion_cleanup_hints;
        let observer = &mut self.observer;
        let post = &mut self.post;
        let ledger = CompletionLedgerParts::new(
            pending_cancel_cqes,
            completion_cleanup_hints,
            observer,
            post,
        );
        CompletionControlParts::new(ledger, metadata)
    }

    #[inline]
    pub(crate) fn record(&mut self, event: ControlPlaneEvent) {
        self.observer.record(event);
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    pub(crate) fn take_events(&mut self) -> veloq_std::vec::Vec<ControlPlaneEvent> {
        self.observer.take_events()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn record_completion_observation(&mut self, token: OpToken, final_completion: bool) {
        self.observer.record(ControlPlaneEvent::CompletionRecord {
            token,
            final_completion,
        });
        if final_completion {
            self.observer
                .record(ControlPlaneEvent::CompletionFinalize(token));
        }
    }

    #[inline]
    pub(crate) fn drain_post_effects_into(&mut self, destination: &mut UringPostCompletionEffects) {
        self.post.drain_into(destination);
    }

    pub(crate) fn defer_cancel_reconcile(
        &mut self,
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    ) -> Result<(), ()> {
        if self
            .deferred_cancel_reconciles
            .iter()
            .any(|entry| entry.cancel_ticket == cancel_ticket)
        {
            return Ok(());
        }
        if self.deferred_cancel_reconciles.len() >= self.cancellations.capacity() {
            return Err(());
        }
        self.deferred_cancel_reconciles
            .push(DeferredCancelReconcile {
                cancel_ticket,
                request,
                raw,
                since: Instant::now(),
            });
        Ok(())
    }

    #[inline]
    pub(crate) fn deferred_cancel_reconciles(&self) -> &[DeferredCancelReconcile] {
        &self.deferred_cancel_reconciles
    }

    #[inline]
    pub(crate) fn remove_deferred_cancel_reconcile(
        &mut self,
        index: usize,
    ) -> Option<DeferredCancelReconcile> {
        (index < self.deferred_cancel_reconciles.len())
            .then(|| self.deferred_cancel_reconciles.swap_remove(index))
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn stage_entry(&mut self, entry: StagedEntry) -> Result<(), StagedLedgerError> {
        self.staged_entries.push(entry)
    }

    /// Atomically records a staged cancel SQE in the sidecar and submission metadata.
    ///
    /// The caller invokes this only after the SQE has entered the user-space submission queue.
    /// A full queue therefore never creates an in-flight sidecar entry, while an allocator bug
    /// cannot overwrite an existing request.
    #[cfg(test)]
    pub(crate) fn stage_cancel(
        &mut self,
        ticket: CancelTicket,
        request: PendingCancel,
    ) -> Result<(), PendingCancel> {
        if self
            .staged_entries
            .validate(StagedEntry::Cancel {
                ticket,
                target: request.target(),
            })
            .is_err()
        {
            return Err(request);
        }
        self.cancellations.insert_in_flight(ticket, request)?;
        if self
            .staged_entries
            .push(StagedEntry::Cancel {
                ticket,
                target: request.target(),
            })
            .is_err()
        {
            let _ = self.cancellations.in_flight_mut().remove(&ticket);
            return Err(request);
        }
        self.observer
            .record(ControlPlaneEvent::CancelInFlightInsert {
                ticket,
                target: request.target(),
            });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn staged_entry_count(&self) -> usize {
        self.staged_entries.len()
    }

    pub(crate) fn unpublished_staged_entry_count(&self) -> usize {
        self.staged_entries.unpublished_len()
    }

    pub(crate) fn mark_staged_published(&mut self) -> usize {
        self.staged_entries.mark_published()
    }

    pub(crate) fn mark_staged_consumed(
        &mut self,
        requested: usize,
        consumed: usize,
        published_in_queue: usize,
    ) -> Result<usize, StagedLedgerError> {
        self.staged_entries
            .mark_consumed(requested, consumed, published_in_queue)
    }

    pub(crate) fn quarantine_unpublished_staged(&mut self) -> usize {
        self.staged_entries.quarantine_unpublished()
    }

    pub(crate) fn settle_staged_completion(
        &mut self,
        token: CompletionToken,
        final_completion: bool,
    ) -> Result<bool, StagedLedgerError> {
        self.staged_entries
            .settle_completion(token, final_completion)
    }

    pub(crate) fn mark_kernel_cancel_intents(&mut self) {
        let (staged_entries, cancellations) = (&self.staged_entries, &mut self.cancellations);
        staged_entries.for_each_kernel_cancel(|ticket, target| {
            cancellations.mark_outstanding(ticket, target);
        });
    }

    #[cfg(test)]
    pub(crate) fn pop_staged_entry(&mut self) -> Option<StagedEntry> {
        self.staged_entries.pop_front()
    }

    #[cfg(test)]
    pub(crate) fn staged_capacity(&self) -> usize {
        self.staged_entries.capacity()
    }

    #[inline]
    pub(crate) fn quarantine_timer(&mut self, token: OpToken) {
        self.quarantined_timers.insert(token);
    }

    #[inline]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn is_timer_quarantined(&self, token: OpToken) -> bool {
        self.quarantined_timers.contains(&token)
    }

    #[inline]
    pub(crate) fn quarantine_token(&mut self, token: OpToken) {
        self.quarantined_tokens.insert(token);
    }

    #[inline]
    pub(crate) fn release_quarantined_token(&mut self, token: OpToken) {
        self.quarantined_tokens.remove(&token);
    }

    #[inline]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn quarantined_tokens(&self) -> Vec<OpToken> {
        self.quarantined_tokens.iter().copied().collect()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn timer_for(&self, token: OpToken) -> Option<veloq_wheel::TimerId> {
        self.observer.timer_for(token)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn timer_entries(&self) -> veloq_std::vec::Vec<(veloq_wheel::TimerId, OpToken)> {
        self.observer.timer_entries()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn has_staged_kernel_token(&self, token: OpToken) -> bool {
        self.observer.has_staged_kernel_token(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IoFd;
    use veloq_driver_core::driver::CancelMode;
    use veloq_driver_core::slot::Generation;

    fn token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, Generation::new(generation)).expect("test token")
    }

    #[test]
    fn backlog_removal_is_generation_safe_and_keeps_fifo() {
        let first = token(0, 1);
        let second = token(1, 1);
        let stale = token(0, 2);
        let mut backlog = SubmissionBacklog::new(2);

        assert!(backlog.push(first).is_ok());
        assert!(backlog.push(second).is_ok());
        assert_eq!(backlog.len(), 2);
        assert_eq!(backlog.peak_len(), 2);
        assert_eq!(backlog.push(first), Err(BacklogError::Duplicate));
        assert_eq!(backlog.remove(stale), Err(BacklogError::GenerationMismatch));
        assert_eq!(backlog.remove(first).map(|entry| entry.token), Ok(first));
        assert_eq!(backlog.remove(first), Err(BacklogError::Missing));
        assert_eq!(backlog.front(), Some(BacklogEntry { token: second }));
        assert_eq!(backlog.pop_front().map(|entry| entry.token), Some(second));
        assert!(backlog.front().is_none());
    }

    #[test]
    fn backlog_rejects_capacity_overflow_without_growing() {
        let first = token(0, 1);
        let second = token(1, 1);
        let mut backlog = SubmissionBacklog::new(1);

        backlog.push(first).expect("first entry should fit");
        assert_eq!(backlog.push(second), Err(BacklogError::Capacity));
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.peak_len(), 1);
        assert!(backlog.contains(first));
    }

    #[test]
    fn staged_ledger_rejects_duplicate_and_generation_mismatch() {
        let first = token(0, 1);
        let replacement = token(0, 2);
        let mut ledger = StagedLedger::new(2);

        ledger
            .push(StagedEntry::User(first))
            .expect("first staged entry should fit");
        assert_eq!(
            ledger.push(StagedEntry::User(first)),
            Err(StagedLedgerError::DuplicateUser(first))
        );
        assert_eq!(
            ledger.push(StagedEntry::User(replacement)),
            Err(StagedLedgerError::GenerationMismatch {
                existing: first,
                candidate: replacement,
            })
        );
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn staged_ledger_capacity_is_fixed() {
        let mut ledger = StagedLedger::new(1);
        ledger
            .push(StagedEntry::Waker)
            .expect("first staged entry should fit");
        assert_eq!(
            ledger.push(StagedEntry::Waker),
            Err(StagedLedgerError::Capacity)
        );
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn staged_entries_preserve_submit_order() {
        let first = token(3, 2);
        let second = token(4, 2);
        let mut plane = UringControlPlane::new(UringWakerManager::new().expect("test eventfd"));
        assert_eq!(plane.staged_capacity(), 1024);
        plane
            .stage_entry(StagedEntry::User(first))
            .expect("stage user");
        plane.stage_entry(StagedEntry::Waker).expect("stage waker");
        plane
            .stage_entry(StagedEntry::Cancel {
                ticket: CancelTicket::try_new(7).expect("test ticket"),
                target: second,
            })
            .expect("stage cancel");

        assert_eq!(plane.staged_entry_count(), 3);
        assert_eq!(plane.pop_staged_entry(), Some(StagedEntry::User(first)));
        assert_eq!(plane.pop_staged_entry(), Some(StagedEntry::Waker));
        assert_eq!(plane.staged_entry_count(), 1);
        assert_eq!(
            plane.pop_staged_entry(),
            Some(StagedEntry::Cancel {
                ticket: CancelTicket::try_new(7).expect("test ticket"),
                target: second,
            })
        );
    }

    #[test]
    fn staged_receipts_keep_published_entries_until_their_cqe() {
        let first = token(0, 1);
        let second = token(1, 1);
        let mut ledger = StagedLedger::new(4);

        ledger
            .push(StagedEntry::User(first))
            .expect("first SQE should stage");
        assert_eq!(ledger.mark_published(), 1);
        assert_eq!(ledger.unpublished_len(), 0);

        ledger
            .push(StagedEntry::User(second))
            .expect("second SQE should stage");
        assert_eq!(ledger.unpublished_len(), 1);
        assert_eq!(
            ledger
                .mark_consumed(2, 2, 1)
                .expect("receipt should skip the published SQ prefix"),
            1
        );

        assert!(
            ledger
                .settle_completion(CompletionToken::user(second), true)
                .expect("second completion should settle")
        );
        assert!(
            ledger
                .settle_completion(CompletionToken::user(first), true)
                .expect("published completion should settle")
        );
        assert_eq!(ledger.len(), 0);
    }

    #[test]
    fn staged_final_completion_unlinks_any_node_and_reuses_its_capacity() {
        let first = token(0, 1);
        let middle = token(1, 1);
        let last = token(2, 1);
        let replacement = token(1, 2);
        let mut ledger = StagedLedger::new(3);

        for token in [first, middle, last] {
            ledger
                .push(StagedEntry::User(token))
                .expect("staged entry should fit");
        }
        assert_eq!(ledger.mark_consumed(3, 3, 0), Ok(3));
        assert!(
            ledger
                .settle_completion(CompletionToken::user(middle), true)
                .expect("middle completion should settle")
        );

        ledger
            .push(StagedEntry::User(replacement))
            .expect("freed ledger node should be reusable");
        assert_eq!(ledger.pop_front(), Some(StagedEntry::User(first)));
        assert_eq!(ledger.pop_front(), Some(StagedEntry::User(last)));
        assert_eq!(ledger.pop_front(), Some(StagedEntry::User(replacement)));
        assert_eq!(ledger.pop_front(), None);
    }

    #[test]
    fn unknown_receipt_quarantines_only_unpublished_entries() {
        let published = token(0, 1);
        let unknown = token(1, 1);
        let mut ledger = StagedLedger::new(4);

        ledger
            .push(StagedEntry::User(published))
            .expect("published SQE should stage");
        ledger.mark_published();
        ledger
            .push(StagedEntry::User(unknown))
            .expect("unknown SQE should stage");

        assert_eq!(ledger.quarantine_unpublished(), 1);
        assert!(
            ledger
                .settle_completion(CompletionToken::user(unknown), true)
                .expect("quarantined completion should settle")
        );
        assert!(
            ledger
                .settle_completion(CompletionToken::user(published), true)
                .expect("published completion should settle")
        );
        assert_eq!(ledger.len(), 0);
    }

    #[test]
    fn staged_completion_before_a_submit_receipt_is_rejected() {
        let token = token(0, 1);
        let mut ledger = StagedLedger::new(1);
        ledger
            .push(StagedEntry::User(token))
            .expect("SQE should stage");

        assert_eq!(
            ledger.settle_completion(CompletionToken::user(token), true),
            Err(StagedLedgerError::InvalidTransition)
        );
    }

    #[test]
    fn stage_cancel_records_one_sidecar_entry_and_one_staged_entry() {
        let target = token(8, 5);
        let ticket = CancelTicket::try_new(11).expect("test ticket");
        let request = PendingCancel::from_parts(target, CancelMode::Abandon);
        let mut plane = UringControlPlane::new(UringWakerManager::new().expect("test eventfd"));

        plane
            .stage_cancel(ticket, request)
            .expect("ticket should be staged");

        assert_eq!(plane.staged_entry_count(), 1);
        assert_eq!(plane.cancellations.in_flight_len(), 1);
        assert_eq!(plane.cancellations.in_flight_targets().count(), 1);
        assert_eq!(
            plane.observer.take_events(),
            veloq_std::vec![ControlPlaneEvent::CancelInFlightInsert { ticket, target }]
        );
    }

    #[test]
    fn duplicate_stage_cancel_preserves_the_original_request() {
        let target = token(9, 5);
        let replacement = token(10, 6);
        let ticket = CancelTicket::try_new(12).expect("test ticket");
        let first = PendingCancel::from_parts(target, CancelMode::UserVisible);
        let second = PendingCancel::from_parts(replacement, CancelMode::Abandon);
        let mut plane = UringControlPlane::new(UringWakerManager::new().expect("test eventfd"));

        plane.stage_cancel(ticket, first).expect("first stage");
        assert!(plane.stage_cancel(ticket, second).is_err());

        assert_eq!(plane.staged_entry_count(), 1);
        assert_eq!(
            plane
                .cancellations
                .in_flight_targets()
                .collect::<veloq_std::vec::Vec<_>>(),
            veloq_std::vec![(ticket, target)]
        );
        assert_eq!(plane.observer.take_events().len(), 1);
    }

    #[test]
    fn post_effects_are_typed_and_waker_generation_is_deduplicated() {
        let token = token(5, 3);
        let mut effects = UringPostCompletionEffects::default();
        effects.push(UringControlEffect {
            sequence: 1,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 9 },
        });
        effects.push(UringControlEffect {
            sequence: 2,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 9 },
        });
        effects.push(UringControlEffect {
            sequence: 3,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRebuild { generation: 9 },
        });

        assert_eq!(effects.effects().len(), 2);
        assert!(matches!(
            effects.effects()[0].kind,
            UringControlEffectKind::WakerRearm { generation: 9 }
        ));
        assert_eq!(effects.effects()[0].sequence, 1);
        assert_eq!(effects.effects()[0].generation, Some(token.generation()));
    }

    #[test]
    fn post_effects_keep_only_the_latest_waker_generation() {
        let token = token(7, 2);
        let mut effects = UringPostCompletionEffects::with_capacity(1);
        effects.push(UringControlEffect {
            sequence: 1,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 3 },
        });
        effects.push(UringControlEffect {
            sequence: 2,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 4 },
        });

        assert_eq!(effects.effects().len(), 1);
        assert!(matches!(
            effects.effects()[0].kind,
            UringControlEffectKind::WakerRearm { generation: 4 }
        ));
        assert!(!effects.is_overflowed());
    }

    #[test]
    fn post_effects_report_capacity_exhaustion() {
        let token = token(8, 2);
        let mut effects = UringPostCompletionEffects::with_capacity(1);
        effects.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::BacklogKick,
        );
        effects.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::CloseUnregister {
                ticket: OwnedFdOwnershipTicket::new(token, IoFd::fixed(12)),
            },
        );

        assert_eq!(effects.effects().len(), 1);
        assert!(effects.is_overflowed());
    }

    #[test]
    fn batch_effects_receive_a_deterministic_sequence() {
        let token = token(6, 4);
        let mut first = UringPostCompletionEffects::default();
        first.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::BacklogKick,
        );
        let mut second = UringPostCompletionEffects::default();
        second.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::CancelAck {
                cancel_ticket: CancelTicket::try_new(2).expect("test ticket"),
                phase: CancellationPhase::Acked,
            },
        );

        first.extend(second);

        assert_eq!(first.effects()[0].sequence, 0);
        assert_eq!(first.effects()[1].sequence, 1);
    }
}
