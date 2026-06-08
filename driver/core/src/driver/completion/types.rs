use crate::driver::completion::{CompletionPacket, CompletionToken};
use crate::slot;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverCompletionDiagnostics {
    pub user_completed: u64,
    pub user_orphan_completed: u64,
    pub unknown_completion: u64,
    pub stale_completion: u64,
    pub slot_corruption: u64,
    pub cancel_submitted: u64,
    pub cancel_cqe_ok: u64,
    pub cancel_cqe_enoent: u64,
    pub cancel_cqe_error: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub waker_rebuild: u64,
    pub completion_rejected: u64,
    pub internal_unknown: u64,
    pub orphan_cleanup_error: u64,
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
    pub fn inc_cancel_cqe_ok(&mut self) {
        self.cancel_cqe_ok = self.cancel_cqe_ok.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_cqe_enoent(&mut self) {
        self.cancel_cqe_enoent = self.cancel_cqe_enoent.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_cqe_error(&mut self) {
        self.cancel_cqe_error = self.cancel_cqe_error.saturating_add(1);
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
pub struct CompletionAnomaly {
    pub token: CompletionToken,
    pub index: Option<usize>,
    pub expected_generation: Option<u32>,
    pub actual_generation: Option<u32>,
    pub state: Option<slot::SlotState>,
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
            reason: CompletionAnomalyReason::SlotCorruption,
        }
    }
}

pub struct CompletionCleanup {
    action: Box<dyn FnOnce() + Send + 'static>,
}

impl CompletionCleanup {
    #[inline]
    pub fn new(action: impl FnOnce() + Send + 'static) -> Self {
        Self {
            action: Box::new(action),
        }
    }

    #[inline]
    pub fn run(self) {
        (self.action)();
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
}

impl Drop for CompletionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.run();
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
