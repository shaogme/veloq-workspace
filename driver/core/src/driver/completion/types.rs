use crate::driver::completion::{CompletionEvent, CompletionPacket, CompletionToken};
use crate::slot;
use crate::{DriverCoreError, DriverResult};
use std::sync::Arc;
use veloq_shim::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
struct DriverCompletionDiagnosticsInner {
    user_completed: AtomicU64,
    user_lost: AtomicU64,
    user_orphan_completed: AtomicU64,
    unknown_completion: AtomicU64,
    stale_completion: AtomicU64,
    slot_corruption: AtomicU64,
    payload_missing: AtomicU64,
    cancel_submitted: AtomicU64,
    cancel_ack_ok: AtomicU64,
    cancel_ack_not_found: AtomicU64,
    cancel_ack_error: AtomicU64,
    waker_ok: AtomicU64,
    waker_error: AtomicU64,
    waker_rebuild: AtomicU64,
    completion_rejected: AtomicU64,
    internal_unknown: AtomicU64,
    rio_malformed_context: AtomicU64,
    rio_missing_context: AtomicU64,
    rio_stale_context: AtomicU64,
    orphan_cleanup_error: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct DriverCompletionDiagnostics {
    inner: Arc<DriverCompletionDiagnosticsInner>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverCompletionDiagnosticsSnapshot {
    pub user_completed: u64,
    pub user_lost: u64,
    pub user_orphan_completed: u64,
    pub unknown_completion: u64,
    pub stale_completion: u64,
    pub slot_corruption: u64,
    pub payload_missing: u64,
    pub cancel_submitted: u64,
    pub cancel_ack_ok: u64,
    pub cancel_ack_not_found: u64,
    pub cancel_ack_error: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub waker_rebuild: u64,
    pub completion_rejected: u64,
    pub internal_unknown: u64,
    pub rio_malformed_context: u64,
    pub rio_missing_context: u64,
    pub rio_stale_context: u64,
    pub orphan_cleanup_error: u64,
}

impl DriverCompletionDiagnostics {
    #[inline]
    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    #[inline]
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn snapshot(&self) -> DriverCompletionDiagnosticsSnapshot {
        DriverCompletionDiagnosticsSnapshot {
            user_completed: Self::load(&self.inner.user_completed),
            user_lost: Self::load(&self.inner.user_lost),
            user_orphan_completed: Self::load(&self.inner.user_orphan_completed),
            unknown_completion: Self::load(&self.inner.unknown_completion),
            stale_completion: Self::load(&self.inner.stale_completion),
            slot_corruption: Self::load(&self.inner.slot_corruption),
            payload_missing: Self::load(&self.inner.payload_missing),
            cancel_submitted: Self::load(&self.inner.cancel_submitted),
            cancel_ack_ok: Self::load(&self.inner.cancel_ack_ok),
            cancel_ack_not_found: Self::load(&self.inner.cancel_ack_not_found),
            cancel_ack_error: Self::load(&self.inner.cancel_ack_error),
            waker_ok: Self::load(&self.inner.waker_ok),
            waker_error: Self::load(&self.inner.waker_error),
            waker_rebuild: Self::load(&self.inner.waker_rebuild),
            completion_rejected: Self::load(&self.inner.completion_rejected),
            internal_unknown: Self::load(&self.inner.internal_unknown),
            rio_malformed_context: Self::load(&self.inner.rio_malformed_context),
            rio_missing_context: Self::load(&self.inner.rio_missing_context),
            rio_stale_context: Self::load(&self.inner.rio_stale_context),
            orphan_cleanup_error: Self::load(&self.inner.orphan_cleanup_error),
        }
    }

    #[inline]
    pub fn inc_user_completed(&self) {
        Self::inc(&self.inner.user_completed);
    }

    #[inline]
    pub fn inc_user_lost(&self) {
        Self::inc(&self.inner.user_lost);
    }

    #[inline]
    pub fn inc_user_orphan_completed(&self) {
        Self::inc(&self.inner.user_orphan_completed);
    }

    #[inline]
    pub fn inc_unknown_completion(&self) {
        Self::inc(&self.inner.unknown_completion);
    }

    #[inline]
    pub fn inc_stale_completion(&self) {
        Self::inc(&self.inner.stale_completion);
    }

    #[inline]
    pub fn inc_slot_corruption(&self) {
        Self::inc(&self.inner.slot_corruption);
    }

    #[inline]
    pub fn inc_payload_missing(&self) {
        Self::inc(&self.inner.payload_missing);
    }

    #[inline]
    pub fn inc_cancel_submitted(&self) {
        Self::inc(&self.inner.cancel_submitted);
    }

    #[inline]
    pub fn inc_cancel_ack_ok(&self) {
        Self::inc(&self.inner.cancel_ack_ok);
    }

    #[inline]
    pub fn inc_cancel_ack_not_found(&self) {
        Self::inc(&self.inner.cancel_ack_not_found);
    }

    #[inline]
    pub fn inc_cancel_ack_error(&self) {
        Self::inc(&self.inner.cancel_ack_error);
    }

    #[inline]
    pub fn inc_cancel_observed_ok(&self) {
        self.inc_cancel_ack_ok();
    }

    #[inline]
    pub fn inc_cancel_observed_not_found(&self) {
        self.inc_cancel_ack_not_found();
    }

    #[inline]
    pub fn inc_cancel_observed_error(&self) {
        self.inc_cancel_ack_error();
    }

    #[inline]
    pub fn inc_waker_ok(&self) {
        Self::inc(&self.inner.waker_ok);
    }

    #[inline]
    pub fn inc_waker_error(&self) {
        Self::inc(&self.inner.waker_error);
    }

    #[inline]
    pub fn inc_waker_rebuild(&self) {
        Self::inc(&self.inner.waker_rebuild);
    }

    #[inline]
    pub fn inc_completion_rejected(&self) {
        Self::inc(&self.inner.completion_rejected);
    }

    #[inline]
    pub fn inc_internal_unknown(&self) {
        Self::inc(&self.inner.internal_unknown);
    }

    #[inline]
    pub fn inc_rio_malformed_context(&self) {
        Self::inc(&self.inner.rio_malformed_context);
    }

    #[inline]
    pub fn inc_rio_missing_context(&self) {
        Self::inc(&self.inner.rio_missing_context);
    }

    #[inline]
    pub fn inc_rio_stale_context(&self) {
        Self::inc(&self.inner.rio_stale_context);
    }

    #[inline]
    pub fn inc_orphan_cleanup_error(&self) {
        Self::inc(&self.inner.orphan_cleanup_error);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyReason {
    UnknownSlot,
    UnknownControlToken,
    ControlCompletionUntracked,
    RioMalformedContext,
    RioMissingContext,
    RioStaleContext,
    StaleGeneration,
    NonActiveSlot,
    SlotCorruption,
    OpMissing,
    PayloadMissing,
    BackendInvariantBroken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionMutationOutcome {
    Applied,
    Missing(CompletionAnomaly),
    Stale(CompletionAnomaly),
    NonActive(CompletionAnomaly),
    UnknownControl(CompletionAnomaly),
}

impl CompletionMutationOutcome {
    #[inline]
    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    #[inline]
    pub const fn anomaly(&self) -> Option<&CompletionAnomaly> {
        match self {
            Self::Applied => None,
            Self::Missing(anomaly)
            | Self::Stale(anomaly)
            | Self::NonActive(anomaly)
            | Self::UnknownControl(anomaly) => Some(anomaly),
        }
    }
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
    pub backend_context: Option<u64>,
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
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::UnknownControlToken,
        }
    }

    #[inline]
    pub fn control_completion_untracked(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::ControlCompletionUntracked,
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
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::UnknownSlot,
        }
    }

    #[inline]
    pub fn rio_malformed_context(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            backend: Some(CompletionBackend::Rio),
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::RioMalformedContext,
        }
    }

    #[inline]
    pub fn rio_missing_context(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: None,
            state: None,
            backend: Some(CompletionBackend::Rio),
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::RioMissingContext,
        }
    }

    #[inline]
    pub fn rio_stale_context(
        token: CompletionToken,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(expected_generation),
            actual_generation: Some(actual_generation),
            state: None,
            backend: Some(CompletionBackend::Rio),
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::RioStaleContext,
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
            backend_context: None,
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
            backend_context: None,
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
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::SlotCorruption,
        }
    }

    #[inline]
    pub fn op_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::OpMissing,
        }
    }

    #[inline]
    pub fn payload_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::PayloadMissing,
        }
    }

    #[inline]
    pub fn backend_invariant_broken(
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
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::BackendInvariantBroken,
        }
    }

    #[inline]
    pub fn with_backend(mut self, backend: CompletionBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    #[inline]
    pub fn with_backend_context(mut self, context: u64) -> Self {
        self.backend_context = Some(context);
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

    #[inline]
    pub fn rio_malformed_context_raw(raw_context: u64) -> Self {
        Self::rio_malformed_context(CompletionToken::rio_wake(0)).with_backend_context(raw_context)
    }

    #[inline]
    pub fn rio_missing_context_raw(raw_context: u64, index: usize, generation: u32) -> Self {
        Self::rio_missing_context(CompletionToken::rio_wake(0), index, generation)
            .with_backend_context(raw_context)
    }

    #[inline]
    pub fn rio_stale_context_raw(
        raw_context: u64,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
    ) -> Self {
        Self::rio_stale_context(
            CompletionToken::rio_wake(0),
            index,
            expected_generation,
            actual_generation,
        )
        .with_backend_context(raw_context)
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
    RecordedUser,
    RecordedLost,
    OrphanedDropped,
    Missing(CompletionAnomaly),
    Stale(CompletionAnomaly),
    NonActive(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

pub enum RecordCompletionResult<UP, E, R = usize> {
    Recorded(RecordCompletionOutcome),
    Rejected {
        outcome: RecordCompletionOutcome,
        packet: Box<CompletionPacket<UP, E, R>>,
    },
}

impl<UP, E, R> RecordCompletionResult<UP, E, R> {
    #[inline]
    pub fn outcome(&self) -> &RecordCompletionOutcome {
        match self {
            Self::Recorded(outcome) => outcome,
            Self::Rejected { outcome, .. } => outcome,
        }
    }

    #[inline]
    pub fn into_outcome(self) -> RecordCompletionOutcome {
        match self {
            Self::Recorded(outcome) => outcome,
            Self::Rejected { outcome, .. } => outcome,
        }
    }
}

impl DriverCompletionDiagnostics {
    #[inline]
    pub fn record_anomaly(&self, anomaly: &CompletionAnomaly) {
        match anomaly.reason {
            CompletionAnomalyReason::UnknownSlot
            | CompletionAnomalyReason::UnknownControlToken
            | CompletionAnomalyReason::NonActiveSlot => self.inc_unknown_completion(),
            CompletionAnomalyReason::ControlCompletionUntracked
            | CompletionAnomalyReason::BackendInvariantBroken => self.inc_internal_unknown(),
            CompletionAnomalyReason::RioMalformedContext => self.inc_rio_malformed_context(),
            CompletionAnomalyReason::RioMissingContext => self.inc_rio_missing_context(),
            CompletionAnomalyReason::RioStaleContext => self.inc_rio_stale_context(),
            CompletionAnomalyReason::OpMissing | CompletionAnomalyReason::SlotCorruption => {
                self.inc_slot_corruption()
            }
            CompletionAnomalyReason::PayloadMissing => self.inc_payload_missing(),
            CompletionAnomalyReason::StaleGeneration => self.inc_stale_completion(),
        }
    }

    #[inline]
    pub fn record_completion_outcome(&self, outcome: &RecordCompletionOutcome) {
        if !matches!(
            outcome,
            RecordCompletionOutcome::RecordedUser | RecordCompletionOutcome::RecordedLost
        ) {
            self.inc_completion_rejected();
        }
        match outcome {
            RecordCompletionOutcome::RecordedUser => self.inc_user_completed(),
            RecordCompletionOutcome::RecordedLost => self.inc_user_lost(),
            RecordCompletionOutcome::OrphanedDropped => self.inc_user_orphan_completed(),
            RecordCompletionOutcome::Missing(anomaly)
            | RecordCompletionOutcome::Stale(anomaly)
            | RecordCompletionOutcome::NonActive(anomaly)
            | RecordCompletionOutcome::Corrupt(anomaly) => self.record_anomaly(anomaly),
        }
    }

    #[inline]
    pub fn record_completion_result<UP, E, R>(&self, result: &RecordCompletionResult<UP, E, R>) {
        self.record_completion_outcome(result.outcome());
    }
}
