use std::sync::Arc;

use veloq_shim::atomic::{AtomicU64, Ordering};

use super::anomaly::{
    AnomalyAttach, AnomalyOutcome, CompletionAnomaly, CompletionAnomalyKind,
    CompletionAnomalyReason,
};
use super::record::RecordCompletionOutcome;

#[derive(Debug, Default)]
struct DriverCompletionDiagnosticsInner<B = ()> {
    user_completed: AtomicU64,
    user_lost: AtomicU64,
    user_orphan_completed: AtomicU64,
    unknown_completion: AtomicU64,
    stale_completion: AtomicU64,
    completion_rejected: AtomicU64,
    internal_unknown: AtomicU64,
    orphan_cleanup_error: AtomicU64,
    backend: B,
}

pub trait DriverCompletionDiagnosticsBackend: Default + Send + Sync + 'static {
    type Snapshot: Default;

    fn snapshot(&self) -> Self::Snapshot;

    fn record_backend_anomaly(&self, _anomaly: &CompletionAnomaly) -> bool {
        false
    }
}

impl DriverCompletionDiagnosticsBackend for () {
    type Snapshot = ();

    fn snapshot(&self) -> Self::Snapshot {}
}

#[derive(Debug, Default)]
pub struct DriverCompletionDiagnostics<B = ()> {
    inner: Arc<DriverCompletionDiagnosticsInner<B>>,
}

impl<B> Clone for DriverCompletionDiagnostics<B> {
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
    pub completion_rejected: u64,
    pub internal_unknown: u64,
    pub orphan_cleanup_error: u64,
    pub backend: B,
}

impl<B> DriverCompletionDiagnostics<B> {
    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend(&self) -> &B {
        &self.inner.backend
    }

    pub fn inc_user_completed(&self) {
        Self::inc(&self.inner.user_completed);
    }

    pub fn inc_user_lost(&self) {
        Self::inc(&self.inner.user_lost);
    }

    pub fn inc_user_orphan_completed(&self) {
        Self::inc(&self.inner.user_orphan_completed);
    }

    pub fn inc_unknown_completion(&self) {
        Self::inc(&self.inner.unknown_completion);
    }

    pub fn inc_stale_completion(&self) {
        Self::inc(&self.inner.stale_completion);
    }

    pub fn inc_completion_rejected(&self) {
        Self::inc(&self.inner.completion_rejected);
    }

    pub fn inc_internal_unknown(&self) {
        Self::inc(&self.inner.internal_unknown);
    }

    pub fn inc_orphan_cleanup_error(&self) {
        Self::inc(&self.inner.orphan_cleanup_error);
    }
}

impl<B> DriverCompletionDiagnostics<B>
where
    B: DriverCompletionDiagnosticsBackend,
{
    pub fn snapshot(&self) -> DriverCompletionDiagnosticsSnapshot<B::Snapshot> {
        DriverCompletionDiagnosticsSnapshot {
            user_completed: Self::load(&self.inner.user_completed),
            user_lost: Self::load(&self.inner.user_lost),
            user_orphan_completed: Self::load(&self.inner.user_orphan_completed),
            unknown_completion: Self::load(&self.inner.unknown_completion),
            stale_completion: Self::load(&self.inner.stale_completion),
            completion_rejected: Self::load(&self.inner.completion_rejected),
            internal_unknown: Self::load(&self.inner.internal_unknown),
            orphan_cleanup_error: Self::load(&self.inner.orphan_cleanup_error),
            backend: self.inner.backend.snapshot(),
        }
    }

    pub fn record_anomaly(&self, anomaly: &CompletionAnomaly) {
        if self.inner.backend.record_backend_anomaly(anomaly) {
            return;
        }

        match anomaly.reason() {
            CompletionAnomalyReason::UnknownSlot | CompletionAnomalyReason::NonActiveSlot => {
                self.inc_unknown_completion()
            }
            CompletionAnomalyReason::FinalizeFailed
            | CompletionAnomalyReason::BackendContextUnknown
            | CompletionAnomalyReason::BackendSpecific(_) => self.inc_internal_unknown(),
            CompletionAnomalyReason::StaleGeneration => self.inc_stale_completion(),
        }
    }

    pub fn record_anomaly_kind(&self, kind: CompletionAnomalyKind, attach: AnomalyAttach) {
        let anomaly = kind.materialize(attach);
        self.record_anomaly(&anomaly);
    }

    pub fn record_anomaly_outcome(&self, outcome: AnomalyOutcome, attach: AnomalyAttach) {
        self.record_anomaly_kind(outcome.kind(), attach);
    }

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
            RecordCompletionOutcome::Rejected(_) => {}
        }
    }
}
