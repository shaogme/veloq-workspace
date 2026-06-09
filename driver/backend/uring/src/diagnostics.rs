use std::sync::atomic::{AtomicU64, Ordering};

use veloq_driver_core::driver::{
    CompletionAnomaly, DriverCompletionDiagnosticsBackend,
};

#[derive(Debug, Default)]
pub struct UringCompletionDiagnostics {
    cancel_submitted: AtomicU64,
    cancel_ack_ok: AtomicU64,
    cancel_ack_not_found: AtomicU64,
    cancel_ack_error: AtomicU64,
    cancel_ack_enoent_active: AtomicU64,
    waker_ok: AtomicU64,
    waker_error: AtomicU64,
    waker_rebuild: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UringCompletionDiagnosticsSnapshot {
    pub cancel_submitted: u64,
    pub cancel_ack_ok: u64,
    pub cancel_ack_not_found: u64,
    pub cancel_ack_error: u64,
    pub cancel_ack_enoent_active: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub waker_rebuild: u64,
}

impl UringCompletionDiagnostics {
    #[inline]
    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    #[inline]
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_cancel_submitted(&self) {
        Self::inc(&self.cancel_submitted);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_ok(&self) {
        Self::inc(&self.cancel_ack_ok);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_not_found(&self) {
        Self::inc(&self.cancel_ack_not_found);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_error(&self) {
        Self::inc(&self.cancel_ack_error);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_enoent_active(&self) {
        Self::inc(&self.cancel_ack_enoent_active);
    }

    #[inline]
    pub(crate) fn inc_waker_ok(&self) {
        Self::inc(&self.waker_ok);
    }

    #[inline]
    pub(crate) fn inc_waker_error(&self) {
        Self::inc(&self.waker_error);
    }

    #[inline]
    pub(crate) fn inc_waker_rebuild(&self) {
        Self::inc(&self.waker_rebuild);
    }
}

impl DriverCompletionDiagnosticsBackend for UringCompletionDiagnostics {
    type Snapshot = UringCompletionDiagnosticsSnapshot;

    #[inline]
    fn snapshot(&self) -> Self::Snapshot {
        UringCompletionDiagnosticsSnapshot {
            cancel_submitted: Self::load(&self.cancel_submitted),
            cancel_ack_ok: Self::load(&self.cancel_ack_ok),
            cancel_ack_not_found: Self::load(&self.cancel_ack_not_found),
            cancel_ack_error: Self::load(&self.cancel_ack_error),
            cancel_ack_enoent_active: Self::load(&self.cancel_ack_enoent_active),
            waker_ok: Self::load(&self.waker_ok),
            waker_error: Self::load(&self.waker_error),
            waker_rebuild: Self::load(&self.waker_rebuild),
        }
    }

    #[inline]
    fn record_backend_anomaly(&self, _anomaly: &CompletionAnomaly) -> bool {
        false
    }
}
