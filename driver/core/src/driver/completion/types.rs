use crate::driver::completion::{CompletionEvent, CompletionPacket, CompletionToken};
use crate::slot;
use crate::{DriverCoreError, DriverResult};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverCompletionDiagnostics {
    user_completed: u64,
    user_orphan_completed: u64,
    unknown_completion: u64,
    stale_completion: u64,
    slot_corruption: u64,
    cancel_submitted: u64,
    cancel_ack_ok: u64,
    cancel_ack_not_found: u64,
    cancel_ack_error: u64,
    waker_ok: u64,
    waker_error: u64,
    waker_rebuild: u64,
    completion_rejected: u64,
    internal_unknown: u64,
    orphan_cleanup_error: u64,
}

impl DriverCompletionDiagnostics {
    #[inline]
    pub fn inc_user_completed(&mut self) {
        self.user_completed = self.user_completed.saturating_add(1);
    }

    #[inline]
    pub fn inc_user_orphan_completed(&mut self) {
        self.user_orphan_completed = self.user_orphan_completed.saturating_add(1);
    }

    #[inline]
    pub fn inc_unknown_completion(&mut self) {
        self.unknown_completion = self.unknown_completion.saturating_add(1);
    }

    #[inline]
    pub fn inc_stale_completion(&mut self) {
        self.stale_completion = self.stale_completion.saturating_add(1);
    }

    #[inline]
    pub fn inc_slot_corruption(&mut self) {
        self.slot_corruption = self.slot_corruption.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_submitted(&mut self) {
        self.cancel_submitted = self.cancel_submitted.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_ack_ok(&mut self) {
        self.cancel_ack_ok = self.cancel_ack_ok.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_ack_not_found(&mut self) {
        self.cancel_ack_not_found = self.cancel_ack_not_found.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_ack_error(&mut self) {
        self.cancel_ack_error = self.cancel_ack_error.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_ok(&mut self) {
        self.waker_ok = self.waker_ok.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_error(&mut self) {
        self.waker_error = self.waker_error.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_rebuild(&mut self) {
        self.waker_rebuild = self.waker_rebuild.saturating_add(1);
    }

    #[inline]
    pub fn inc_completion_rejected(&mut self) {
        self.completion_rejected = self.completion_rejected.saturating_add(1);
    }

    #[inline]
    pub fn inc_internal_unknown(&mut self) {
        self.internal_unknown = self.internal_unknown.saturating_add(1);
    }

    #[inline]
    pub fn inc_orphan_cleanup_error(&mut self) {
        self.orphan_cleanup_error = self.orphan_cleanup_error.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyReason {
    UnknownSlot,
    UnknownControlToken,
    StaleGeneration,
    NonActiveSlot,
    SlotCorruption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionBackend {
    Core,
    Uring,
    Iocp,
    Rio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionAnomaly {
    pub token: CompletionToken,
    pub index: Option<usize>,
    pub expected_generation: Option<u32>,
    pub actual_generation: Option<u32>,
    pub state: Option<slot::SlotState>,
    pub backend: Option<CompletionBackend>,
    pub raw_result: Option<i32>,
    pub flags: Option<u32>,
    pub slot_snapshot: Option<slot::SlotSnapshot>,
    pub reason: CompletionAnomalyReason,
}

impl CompletionAnomaly {
    #[inline]
    pub fn unknown_control(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            backend: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::UnknownControlToken,
        }
    }

    #[inline]
    pub fn unknown_slot(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: None,
            state: None,
            backend: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::UnknownSlot,
        }
    }

    #[inline]
    pub fn stale(
        token: CompletionToken,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(expected_generation),
            actual_generation: Some(actual_generation),
            state: Some(state),
            backend: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::StaleGeneration,
        }
    }

    #[inline]
    pub fn non_active(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: Some(state),
            backend: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::NonActiveSlot,
        }
    }

    #[inline]
    pub fn corrupt(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: Some(state),
            backend: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::SlotCorruption,
        }
    }

    #[inline]
    pub fn with_backend(mut self, backend: CompletionBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    #[inline]
    pub fn with_event(mut self, event: CompletionEvent) -> Self {
        self.token = event.token;
        self.raw_result = Some(event.res);
        self.flags = Some(event.flags);
        self
    }

    #[inline]
    pub fn with_raw_result(mut self, raw_result: i32) -> Self {
        self.raw_result = Some(raw_result);
        self
    }

    #[inline]
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = Some(flags);
        self
    }

    #[inline]
    pub fn with_slot_snapshot(mut self, snapshot: slot::SlotSnapshot) -> Self {
        self.index = Some(snapshot.index);
        self.actual_generation = Some(snapshot.generation);
        self.state = Some(snapshot.state);
        self.slot_snapshot = Some(snapshot);
        self
    }
}

pub struct CompletionCleanup {
    action: Box<dyn FnOnce() -> DriverResult<(), DriverCoreError> + Send + 'static>,
}

impl CompletionCleanup {
    #[inline]
    pub fn new(
        action: impl FnOnce() -> DriverResult<(), DriverCoreError> + Send + 'static,
    ) -> Self {
        Self {
            action: Box::new(action),
        }
    }

    #[inline]
    pub fn run(self) -> DriverResult<(), DriverCoreError> {
        (self.action)()
    }
}

impl std::fmt::Debug for CompletionCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionCleanup").finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct CompletionCleanupGuard {
    cleanup: Option<CompletionCleanup>,
}

impl CompletionCleanupGuard {
    #[inline]
    pub fn new(cleanup: CompletionCleanup) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    #[inline]
    pub fn none() -> Self {
        Self::default()
    }

    #[inline]
    pub fn is_armed(&self) -> bool {
        self.cleanup.is_some()
    }

    #[inline]
    pub fn disarm(&mut self) -> bool {
        self.cleanup.take().is_some()
    }

    #[inline]
    pub fn run(&mut self) -> DriverResult<bool, DriverCoreError> {
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(false);
        };
        cleanup.run().map(|()| true)
    }
}

impl Drop for CompletionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.run();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCompletionOutcome {
    Recorded,
    OrphanedDropped,
    Missing(CompletionAnomaly),
    Stale(CompletionAnomaly),
    NonActive(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

pub enum RecordCompletionResult<UP, E, R = usize> {
    Recorded,
    Rejected {
        outcome: RecordCompletionOutcome,
        packet: CompletionPacket<UP, E, R>,
    },
}

impl<UP, E, R> RecordCompletionResult<UP, E, R> {
    #[inline]
    pub fn outcome(&self) -> &RecordCompletionOutcome {
        match self {
            Self::Recorded => &RecordCompletionOutcome::Recorded,
            Self::Rejected { outcome, .. } => outcome,
        }
    }

    #[inline]
    pub fn into_outcome(self) -> RecordCompletionOutcome {
        match self {
            Self::Recorded => RecordCompletionOutcome::Recorded,
            Self::Rejected { outcome, .. } => outcome,
        }
    }
}

impl DriverCompletionDiagnostics {
    #[inline]
    pub fn record_completion_outcome(&mut self, outcome: &RecordCompletionOutcome) {
        if !matches!(outcome, RecordCompletionOutcome::Recorded) {
            self.inc_completion_rejected();
        }
        match outcome {
            RecordCompletionOutcome::Recorded => self.inc_user_completed(),
            RecordCompletionOutcome::OrphanedDropped => self.inc_user_orphan_completed(),
            RecordCompletionOutcome::Missing(_) | RecordCompletionOutcome::NonActive(_) => {
                self.inc_unknown_completion();
            }
            RecordCompletionOutcome::Stale(_) => self.inc_stale_completion(),
            RecordCompletionOutcome::Corrupt(_) => self.inc_slot_corruption(),
        }
    }

    #[inline]
    pub fn record_completion_result<UP, E, R>(
        &mut self,
        result: &RecordCompletionResult<UP, E, R>,
    ) {
        self.record_completion_outcome(result.outcome());
    }
}
