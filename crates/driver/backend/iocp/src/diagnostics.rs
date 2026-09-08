use veloq_std::sync::atomic::{AtomicU64, Ordering};

use crate::rio::runtime::control_flow::{
    RIO_ANOMALY_MALFORMED, RIO_ANOMALY_MISSING, RIO_ANOMALY_STALE,
};

#[cfg(test)]
use crate::driver::RIO_EVENT_TOKEN;
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionAnomalyReason, DriverCompletionDiagnosticsBackend,
};

#[derive(Debug, Default)]
pub struct IocpCompletionDiagnostics {
    cancel_submitted: AtomicU64,
    cancel_queued: AtomicU64,
    cancel_local_completed: AtomicU64,
    cancel_target_missing: AtomicU64,
    cancel_target_stale: AtomicU64,
    cancel_target_corrupt: AtomicU64,
    cancel_ack_not_found: AtomicU64,
    cancel_no_handle: AtomicU64,
    cancel_ack_not_found_active: AtomicU64,
    cancel_error: AtomicU64,
    waker_ok: AtomicU64,
    waker_error: AtomicU64,
    waker_rebuild: AtomicU64,
    wait_enter: AtomicU64,
    wait_block: AtomicU64,
    wait_probe_return: AtomicU64,
    wait_timeout: AtomicU64,
    wait_waker_return: AtomicU64,
    wait_ready_preflight: AtomicU64,
    wait_zero: AtomicU64,
    wait_external_timeout: AtomicU64,
    wait_timer_return: AtomicU64,
    wait_completion_return: AtomicU64,
    waker_rearm: AtomicU64,
    rio_malformed_context: AtomicU64,
    rio_missing_context: AtomicU64,
    rio_stale_context: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IocpCompletionDiagnosticsSnapshot {
    pub cancel_submitted: u64,
    pub cancel_queued: u64,
    pub cancel_local_completed: u64,
    pub cancel_target_missing: u64,
    pub cancel_target_stale: u64,
    pub cancel_target_corrupt: u64,
    pub cancel_ack_not_found: u64,
    pub cancel_no_handle: u64,
    pub cancel_ack_not_found_active: u64,
    pub cancel_error: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub waker_rebuild: u64,
    pub wait_enter: u64,
    pub wait_block: u64,
    pub wait_probe_return: u64,
    pub wait_timeout: u64,
    pub wait_waker_return: u64,
    pub wait_ready_preflight: u64,
    pub wait_zero: u64,
    pub wait_external_timeout: u64,
    pub wait_timer_return: u64,
    pub wait_completion_return: u64,
    pub waker_rearm: u64,
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
    pub(crate) fn inc_cancel_local_completed(&self) {
        Self::inc(&self.cancel_local_completed);
    }

    #[inline]
    pub(crate) fn inc_cancel_target_missing(&self) {
        Self::inc(&self.cancel_target_missing);
    }

    #[inline]
    pub(crate) fn inc_cancel_target_stale(&self) {
        Self::inc(&self.cancel_target_stale);
    }

    #[inline]
    pub(crate) fn inc_cancel_target_corrupt(&self) {
        Self::inc(&self.cancel_target_corrupt);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_not_found(&self) {
        Self::inc(&self.cancel_ack_not_found);
    }

    #[inline]
    pub(crate) fn inc_cancel_no_handle(&self) {
        Self::inc(&self.cancel_no_handle);
    }

    #[inline]
    pub(crate) fn inc_cancel_ack_not_found_active(&self) {
        Self::inc(&self.cancel_ack_not_found_active);
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

    #[inline]
    pub(crate) fn inc_wait_enter(&self) {
        Self::inc(&self.wait_enter);
    }

    #[inline]
    pub(crate) fn inc_wait_block(&self) {
        Self::inc(&self.wait_block);
    }

    #[inline]
    pub(crate) fn inc_wait_probe_return(&self) {
        Self::inc(&self.wait_probe_return);
    }

    #[inline]
    pub(crate) fn inc_wait_timeout(&self) {
        Self::inc(&self.wait_timeout);
    }

    #[inline]
    pub(crate) fn inc_wait_waker_return(&self) {
        Self::inc(&self.wait_waker_return);
    }

    #[inline]
    pub(crate) fn inc_wait_ready_preflight(&self) {
        Self::inc(&self.wait_ready_preflight);
    }

    #[inline]
    pub(crate) fn inc_wait_zero(&self) {
        Self::inc(&self.wait_zero);
    }

    #[inline]
    pub(crate) fn inc_wait_external_timeout(&self) {
        Self::inc(&self.wait_external_timeout);
    }

    #[inline]
    pub(crate) fn inc_wait_timer_return(&self) {
        Self::inc(&self.wait_timer_return);
    }

    #[inline]
    pub(crate) fn inc_wait_completion_return(&self) {
        Self::inc(&self.wait_completion_return);
    }

    #[inline]
    pub(crate) fn inc_waker_rearm(&self) {
        Self::inc(&self.waker_rearm);
    }
}

impl DriverCompletionDiagnosticsBackend for IocpCompletionDiagnostics {
    type Snapshot = IocpCompletionDiagnosticsSnapshot;

    #[inline]
    fn snapshot(&self) -> Self::Snapshot {
        IocpCompletionDiagnosticsSnapshot {
            cancel_submitted: Self::load(&self.cancel_submitted),
            cancel_queued: Self::load(&self.cancel_queued),
            cancel_local_completed: Self::load(&self.cancel_local_completed),
            cancel_target_missing: Self::load(&self.cancel_target_missing),
            cancel_target_stale: Self::load(&self.cancel_target_stale),
            cancel_target_corrupt: Self::load(&self.cancel_target_corrupt),
            cancel_ack_not_found: Self::load(&self.cancel_ack_not_found),
            cancel_no_handle: Self::load(&self.cancel_no_handle),
            cancel_ack_not_found_active: Self::load(&self.cancel_ack_not_found_active),
            cancel_error: Self::load(&self.cancel_error),
            waker_ok: Self::load(&self.waker_ok),
            waker_error: Self::load(&self.waker_error),
            waker_rebuild: Self::load(&self.waker_rebuild),
            wait_enter: Self::load(&self.wait_enter),
            wait_block: Self::load(&self.wait_block),
            wait_probe_return: Self::load(&self.wait_probe_return),
            wait_timeout: Self::load(&self.wait_timeout),
            wait_waker_return: Self::load(&self.wait_waker_return),
            wait_ready_preflight: Self::load(&self.wait_ready_preflight),
            wait_zero: Self::load(&self.wait_zero),
            wait_external_timeout: Self::load(&self.wait_external_timeout),
            wait_timer_return: Self::load(&self.wait_timer_return),
            wait_completion_return: Self::load(&self.wait_completion_return),
            waker_rearm: Self::load(&self.waker_rearm),
            rio: RioCompletionDiagnosticsSnapshot {
                malformed_context: Self::load(&self.rio_malformed_context),
                missing_context: Self::load(&self.rio_missing_context),
                stale_context: Self::load(&self.rio_stale_context),
            },
        }
    }

    #[inline]
    fn record_backend_anomaly(&self, anomaly: &CompletionAnomaly) -> bool {
        match anomaly.reason() {
            CompletionAnomalyReason::BackendSpecific(code) => match code {
                RIO_ANOMALY_MALFORMED => {
                    Self::inc(&self.rio_malformed_context);
                    false
                }
                RIO_ANOMALY_MISSING => {
                    Self::inc(&self.rio_missing_context);
                    false
                }
                RIO_ANOMALY_STALE => {
                    Self::inc(&self.rio_stale_context);
                    false
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rio::runtime::control_flow::rio_malformed_context_kind;
    use veloq_driver_core::driver::AnomalyAttach;

    #[test]
    fn rio_backend_anomaly_keeps_core_counting_enabled() {
        let diagnostics = IocpCompletionDiagnostics::default();
        let kind = rio_malformed_context_kind(0xa700_0001);
        let attach = AnomalyAttach::token_only(RIO_EVENT_TOKEN);

        assert!(!diagnostics.record_backend_anomaly(&kind.materialize(attach)));
        assert_eq!(diagnostics.snapshot().rio.malformed_context, 1);
    }
}
