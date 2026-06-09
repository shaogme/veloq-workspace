use std::sync::atomic::{AtomicU64, Ordering};

use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionAnomalyReason, DriverCompletionDiagnosticsBackend,
};

#[derive(Debug, Default)]
pub struct IocpCompletionDiagnostics {
    cancel_submitted: AtomicU64,
    cancel_completed_locally: AtomicU64,
    cancel_not_found: AtomicU64,
    cancel_no_handle: AtomicU64,
    cancel_non_active: AtomicU64,
    cancel_not_found_active: AtomicU64,
    cancel_error: AtomicU64,
    waker_ok: AtomicU64,
    waker_error: AtomicU64,
    rio_malformed_context: AtomicU64,
    rio_missing_context: AtomicU64,
    rio_stale_context: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IocpCompletionDiagnosticsSnapshot {
    pub cancel_submitted: u64,
    pub cancel_completed_locally: u64,
    pub cancel_not_found: u64,
    pub cancel_no_handle: u64,
    pub cancel_non_active: u64,
    pub cancel_not_found_active: u64,
    pub cancel_error: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub rio: RioCompletionDiagnosticsSnapshot,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RioCompletionDiagnosticsSnapshot {
    pub malformed_context: u64,
    pub missing_context: u64,
    pub stale_context: u64,
}

impl IocpCompletionDiagnostics {
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
    pub(crate) fn inc_cancel_completed_locally(&self) {
        Self::inc(&self.cancel_completed_locally);
    }

    #[inline]
    pub(crate) fn inc_cancel_not_found(&self) {
        Self::inc(&self.cancel_not_found);
    }

    #[inline]
    pub(crate) fn inc_cancel_no_handle(&self) {
        Self::inc(&self.cancel_no_handle);
    }

    #[inline]
    pub(crate) fn inc_cancel_non_active(&self) {
        Self::inc(&self.cancel_non_active);
    }

    #[inline]
    pub(crate) fn inc_cancel_not_found_active(&self) {
        Self::inc(&self.cancel_not_found_active);
    }

    #[inline]
    pub(crate) fn inc_cancel_error(&self) {
        Self::inc(&self.cancel_error);
    }

    #[inline]
    pub(crate) fn inc_waker_ok(&self) {
        Self::inc(&self.waker_ok);
    }

    #[inline]
    pub(crate) fn inc_waker_error(&self) {
        Self::inc(&self.waker_error);
    }
}

impl DriverCompletionDiagnosticsBackend for IocpCompletionDiagnostics {
    type Snapshot = IocpCompletionDiagnosticsSnapshot;

    #[inline]
    fn snapshot(&self) -> Self::Snapshot {
        IocpCompletionDiagnosticsSnapshot {
            cancel_submitted: Self::load(&self.cancel_submitted),
            cancel_completed_locally: Self::load(&self.cancel_completed_locally),
            cancel_not_found: Self::load(&self.cancel_not_found),
            cancel_no_handle: Self::load(&self.cancel_no_handle),
            cancel_non_active: Self::load(&self.cancel_non_active),
            cancel_not_found_active: Self::load(&self.cancel_not_found_active),
            cancel_error: Self::load(&self.cancel_error),
            waker_ok: Self::load(&self.waker_ok),
            waker_error: Self::load(&self.waker_error),
            rio: RioCompletionDiagnosticsSnapshot {
                malformed_context: Self::load(&self.rio_malformed_context),
                missing_context: Self::load(&self.rio_missing_context),
                stale_context: Self::load(&self.rio_stale_context),
            },
        }
    }

    #[inline]
    fn record_backend_anomaly(&self, anomaly: &CompletionAnomaly) -> bool {
        match anomaly.reason {
            CompletionAnomalyReason::RioMalformedContext => {
                Self::inc(&self.rio_malformed_context);
                false
            }
            CompletionAnomalyReason::RioMissingContext => {
                Self::inc(&self.rio_missing_context);
                false
            }
            CompletionAnomalyReason::RioStaleContext => {
                Self::inc(&self.rio_stale_context);
                false
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rio_backend_anomaly_keeps_core_counting_enabled() {
        let diagnostics = IocpCompletionDiagnostics::default();
        let anomaly = CompletionAnomaly::rio_malformed_context_raw(0xa700_0001);

        assert!(!diagnostics.record_backend_anomaly(&anomaly));
        assert_eq!(diagnostics.snapshot().rio.malformed_context, 1);
    }
}
