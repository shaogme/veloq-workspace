mod anomaly;

pub use anomaly::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionMutationOutcome,
};

use crate::slot;
use crate::{DriverCoreError, DriverResult};
use std::sync::Arc;
use veloq_shim::atomic::{AtomicU64, Ordering};

use super::CompletionPacket;

// --- Cleanup (from cleanup.rs) ---
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

// --- Record (from record.rs) ---
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

pub enum RecordCompletionResult<Spec: slot::SlotSpec> {
    Recorded(RecordCompletionOutcome),
    Rejected {
        outcome: RecordCompletionOutcome,
        packet: Box<CompletionPacket<Spec>>,
    },
}

impl<Spec: slot::SlotSpec> RecordCompletionResult<Spec> {
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

// --- Diagnostics (from diagnostics.rs) ---
#[derive(Debug, Default)]
struct DriverCompletionDiagnosticsInner<B = ()> {
    user_completed: AtomicU64,
    user_lost: AtomicU64,
    user_orphan_completed: AtomicU64,
    unknown_completion: AtomicU64,
    stale_completion: AtomicU64,
    slot_corruption: AtomicU64,
    payload_missing: AtomicU64,
    completion_rejected: AtomicU64,
    internal_unknown: AtomicU64,
    orphan_cleanup_error: AtomicU64,
    backend: B,
}

pub trait DriverCompletionDiagnosticsBackend: Default + Send + Sync + 'static {
    type Snapshot: Default;

    fn snapshot(&self) -> Self::Snapshot;

    #[inline]
    fn record_backend_anomaly(&self, _anomaly: &CompletionAnomaly) -> bool {
        false
    }
}

impl DriverCompletionDiagnosticsBackend for () {
    type Snapshot = ();

    #[inline]
    fn snapshot(&self) -> Self::Snapshot {}
}

#[derive(Debug, Default)]
pub struct DriverCompletionDiagnostics<B = ()> {
    inner: Arc<DriverCompletionDiagnosticsInner<B>>,
}

impl<B> Clone for DriverCompletionDiagnostics<B> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverCompletionDiagnosticsSnapshot<B = ()> {
    pub user_completed: u64,
    pub user_lost: u64,
    pub user_orphan_completed: u64,
    pub unknown_completion: u64,
    pub stale_completion: u64,
    pub slot_corruption: u64,
    pub payload_missing: u64,
    pub completion_rejected: u64,
    pub internal_unknown: u64,
    pub orphan_cleanup_error: u64,
    pub backend: B,
}

impl<B> DriverCompletionDiagnostics<B> {
    #[inline]
    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    #[inline]
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn backend(&self) -> &B {
        &self.inner.backend
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
    pub fn inc_completion_rejected(&self) {
        Self::inc(&self.inner.completion_rejected);
    }

    #[inline]
    pub fn inc_internal_unknown(&self) {
        Self::inc(&self.inner.internal_unknown);
    }

    #[inline]
    pub fn inc_orphan_cleanup_error(&self) {
        Self::inc(&self.inner.orphan_cleanup_error);
    }
}

impl<B> DriverCompletionDiagnostics<B>
where
    B: DriverCompletionDiagnosticsBackend,
{
    #[inline]
    pub fn snapshot(&self) -> DriverCompletionDiagnosticsSnapshot<B::Snapshot> {
        DriverCompletionDiagnosticsSnapshot {
            user_completed: Self::load(&self.inner.user_completed),
            user_lost: Self::load(&self.inner.user_lost),
            user_orphan_completed: Self::load(&self.inner.user_orphan_completed),
            unknown_completion: Self::load(&self.inner.unknown_completion),
            stale_completion: Self::load(&self.inner.stale_completion),
            slot_corruption: Self::load(&self.inner.slot_corruption),
            payload_missing: Self::load(&self.inner.payload_missing),
            completion_rejected: Self::load(&self.inner.completion_rejected),
            internal_unknown: Self::load(&self.inner.internal_unknown),
            orphan_cleanup_error: Self::load(&self.inner.orphan_cleanup_error),
            backend: self.inner.backend.snapshot(),
        }
    }

    #[inline]
    pub fn record_anomaly(&self, anomaly: &CompletionAnomaly) {
        if self.inner.backend.record_backend_anomaly(anomaly) {
            return;
        }

        match anomaly.reason {
            CompletionAnomalyReason::UnknownSlot
            | CompletionAnomalyReason::UnknownControlToken
            | CompletionAnomalyReason::NonActiveSlot => self.inc_unknown_completion(),
            CompletionAnomalyReason::ControlCompletionUntracked
            | CompletionAnomalyReason::BackendInvariantBroken
            | CompletionAnomalyReason::CompletionKeyMismatch
            | CompletionAnomalyReason::FinalizeFailed
            | CompletionAnomalyReason::CancelAckTargetStillActive
            | CompletionAnomalyReason::BackendContextUnknown
            | CompletionAnomalyReason::RioMalformedContext
            | CompletionAnomalyReason::RioMissingContext
            | CompletionAnomalyReason::RioStaleContext => self.inc_internal_unknown(),
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
}
