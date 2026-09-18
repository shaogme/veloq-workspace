use veloq_std::sync::atomic::{AtomicU64, Ordering};

use veloq_driver_core::driver::{
    CompletionAnomaly, DriverCapabilities, DriverCompletionDiagnosticsBackend,
};
use veloq_io_uring::{
    KernelCapabilities, OpcodeProbeSnapshot, ProbeStatus, ResourceRegistrationCapability,
};

/// The minimum Linux kernel version used by the current backend baseline.
pub const KERNEL_BASELINE: &str = "5.6";

/// Setup profile facts captured while negotiating the ring.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UringSetupSnapshot {
    /// Flags requested by the backend before best-effort negotiation.
    pub requested_flags: u32,
    /// Requested flags removed from the accepted setup profile.
    pub rejected_flags: u32,
    /// Errno from the rejected full profile, if any.
    pub failure_errno: Option<i32>,
}

/// Immutable diagnostics for the capabilities observed while creating a ring.
///
/// This is intentionally a baseline snapshot, not the final capability model. It records the
/// accepted setup/features bits, the probe result, and the backend paths that are currently
/// enabled without changing any setup or fallback decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringCapabilitySnapshot {
    /// Operating system target used to build the backend.
    pub target_os: &'static str,
    /// CPU architecture used to build the backend.
    pub target_arch: &'static str,
    /// Kernel baseline declared by the current implementation.
    pub kernel_baseline: &'static str,
    /// Setup flags accepted in `io_uring_params`.
    pub setup_flags: u32,
    /// Setup flags requested by the backend before best-effort negotiation.
    pub setup_requested_flags: u32,
    /// Requested setup flags removed while finding a working profile.
    pub setup_rejected_flags: u32,
    /// Errno from the rejected setup profile, if one was observed.
    pub setup_failure_errno: Option<i32>,
    /// Feature bits returned in `io_uring_params`.
    pub features: u32,
    /// Whether `IORING_REGISTER_PROBE` succeeded during initialization.
    pub probe_registered: bool,
    /// Errno returned when the opcode probe registration failed.
    pub probe_error: Option<i32>,
    /// Fixed-size opcode probe result.
    pub opcode_probe: OpcodeProbeSnapshot,
    /// Whether the current backend still enables multishot accept.
    pub accept_multi: bool,
    /// Whether the current backend still enables multishot receive.
    pub recv_multi: bool,
    /// Whether fixed-buffer registration is currently available.
    pub fixed_buffers: bool,
    /// Errno returned by the initial fixed-buffer registration, if unavailable.
    pub fixed_buffers_error: Option<i32>,
    /// Whether a provided-buffer ring is currently registered.
    pub provided_buffers: bool,
    /// Errno explaining why a requested provided-buffer ring was not registered.
    pub provided_buffers_error: Option<i32>,
    /// Buffer resource registration capability and actual slot count.
    pub buffer_registration: ResourceRegistrationCapability,
    /// File resource registration capability and actual slot count.
    pub file_registration: ResourceRegistrationCapability,
}

impl UringCapabilitySnapshot {
    pub(crate) const fn new(
        kernel: KernelCapabilities,
        capabilities: DriverCapabilities,
        setup: UringSetupSnapshot,
        fixed_buffers: bool,
        fixed_buffers_error: Option<i32>,
        provided_buffers: bool,
        provided_buffers_error: Option<i32>,
    ) -> Self {
        let probe_registered = matches!(kernel.probe_status, ProbeStatus::Succeeded);
        let probe_error = match kernel.probe_status {
            ProbeStatus::Failed { errno } => Some(errno),
            ProbeStatus::NotAttempted | ProbeStatus::Succeeded => None,
        };
        Self {
            target_os: kernel.target_os,
            target_arch: kernel.target_arch,
            kernel_baseline: KERNEL_BASELINE,
            setup_flags: kernel.setup_flags,
            setup_requested_flags: setup.requested_flags,
            setup_rejected_flags: setup.rejected_flags,
            setup_failure_errno: setup.failure_errno,
            features: kernel.features,
            probe_registered,
            probe_error,
            opcode_probe: match kernel.opcode_probe {
                Some(probe) => probe,
                None => OpcodeProbeSnapshot {
                    last_op: 0,
                    supported: [0; 4],
                },
            },
            accept_multi: capabilities.accept_multi,
            recv_multi: capabilities.recv_multi,
            fixed_buffers,
            fixed_buffers_error,
            provided_buffers,
            provided_buffers_error,
            buffer_registration: kernel.buffer_registration,
            file_registration: kernel.file_registration,
        }
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn baseline_snapshot_preserves_observed_state() {
        let opcode_probe = OpcodeProbeSnapshot {
            last_op: 23,
            supported: [1 << 22, 1 << 23, 0, 0],
        };
        let kernel = KernelCapabilities {
            target_os: "linux",
            target_arch: "x86_64",
            setup_flags: 0x100,
            features: 0x200,
            probe_status: ProbeStatus::Succeeded,
            opcode_probe: Some(opcode_probe),
            buffer_registration: ResourceRegistrationCapability::NotAttempted,
            file_registration: ResourceRegistrationCapability::NotAttempted,
        };
        let snapshot = UringCapabilitySnapshot::new(
            kernel,
            DriverCapabilities {
                accept_multi: true,
                recv_multi: false,
                provided_buffers: false,
            },
            UringSetupSnapshot {
                requested_flags: 0x100,
                ..UringSetupSnapshot::default()
            },
            true,
            None,
            false,
            None,
        );

        assert_eq!(snapshot.target_os, kernel.target_os);
        assert_eq!(snapshot.target_arch, kernel.target_arch);
        assert_eq!(snapshot.kernel_baseline, KERNEL_BASELINE);
        assert_eq!(snapshot.setup_flags, 0x100);
        assert_eq!(snapshot.setup_requested_flags, 0x100);
        assert_eq!(snapshot.setup_rejected_flags, 0);
        assert_eq!(snapshot.setup_failure_errno, None);
        assert_eq!(snapshot.features, 0x200);
        assert!(snapshot.probe_registered);
        assert_eq!(snapshot.probe_error, None);
        assert_eq!(snapshot.opcode_probe, opcode_probe);
        assert!(snapshot.accept_multi);
        assert!(!snapshot.recv_multi);
        assert!(snapshot.fixed_buffers);
        assert_eq!(snapshot.fixed_buffers_error, None);
        assert!(!snapshot.provided_buffers);
        assert_eq!(snapshot.provided_buffers_error, None);
        assert_eq!(
            snapshot.buffer_registration,
            ResourceRegistrationCapability::NotAttempted
        );
        assert_eq!(
            snapshot.file_registration,
            ResourceRegistrationCapability::NotAttempted
        );
    }
}

#[derive(Debug, Default)]
pub struct UringCompletionDiagnostics {
    cancel_submitted: AtomicU64,
    cancel_queued: AtomicU64,
    cancel_local_completed: AtomicU64,
    cancel_target_missing: AtomicU64,
    cancel_target_stale: AtomicU64,
    cancel_target_corrupt: AtomicU64,
    cancel_ack_ok: AtomicU64,
    cancel_ack_not_found: AtomicU64,
    cancel_ack_error: AtomicU64,
    cancel_ack_enoent_active: AtomicU64,
    cancel_reconcile_timeout: AtomicU64,
    cancel_reconcile_deferred: AtomicU64,
    cqe_batches: AtomicU64,
    cqe_collected: AtomicU64,
    cqe_budget_hits: AtomicU64,
    cqe_emergency_drains: AtomicU64,
    cqe_overflow: AtomicU64,
    timer_synthetic: AtomicU64,
    timer_budget_hits: AtomicU64,
    completion_effect_overflows: AtomicU64,
    cancel_ticket_exhausted: AtomicU64,
    cancel_duplicate_ticket: AtomicU64,
    cancel_untracked_cqe: AtomicU64,
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
    file_table_cleanup_failures: AtomicU64,
    file_table_cleanup_short_updates: AtomicU64,
    file_table_quarantines: AtomicU64,
    file_table_rollback_failures: AtomicU64,
    file_table_poisonings: AtomicU64,
    file_table_update_applied: AtomicU64,
    file_table_update_rejected: AtomicU64,
    file_table_update_unknown: AtomicU64,
    fixed_chunk_identity_mismatches: AtomicU64,
    provided_unknown_bids: AtomicU64,
    provided_drop_skipped_unregister: AtomicU64,
    provided_drop_active_unregister_attempts: AtomicU64,
    provided_drop_deferred_unregister: AtomicU64,
    corrupt_cleanup_attempts: AtomicU64,
    corrupt_raw_fd_cleanups: AtomicU64,
    corrupt_cleanup_hint_missing: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UringCompletionDiagnosticsSnapshot {
    pub cancel_submitted: u64,
    pub cancel_queued: u64,
    pub cancel_local_completed: u64,
    pub cancel_target_missing: u64,
    pub cancel_target_stale: u64,
    pub cancel_target_corrupt: u64,
    pub cancel_ack_ok: u64,
    pub cancel_ack_not_found: u64,
    pub cancel_ack_error: u64,
    pub cancel_ack_enoent_active: u64,
    pub cancel_reconcile_timeout: u64,
    pub cancel_reconcile_deferred: u64,
    pub cqe_batches: u64,
    pub cqe_collected: u64,
    pub cqe_budget_hits: u64,
    pub cqe_emergency_drains: u64,
    pub cqe_overflow: u64,
    pub timer_synthetic: u64,
    pub timer_budget_hits: u64,
    pub completion_effect_overflows: u64,
    pub cancel_ticket_exhausted: u64,
    pub cancel_duplicate_ticket: u64,
    pub cancel_untracked_cqe: u64,
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
    /// Number of owned Close cleanup attempts that failed and quarantined a slot.
    pub file_table_cleanup_failures: u64,
    /// Number of quarantines caused by a short `register_files_update` result.
    pub file_table_cleanup_short_updates: u64,
    /// Number of fixed-file slots permanently removed from reuse.
    pub file_table_quarantines: u64,
    /// Number of batch rollback operations that stopped at their first failed update.
    pub file_table_rollback_failures: u64,
    /// Number of transitions from a healthy to a poisoned file table.
    pub file_table_poisonings: u64,
    /// Number of file-table update calls with a known applied result.
    pub file_table_update_applied: u64,
    /// Number of test-injected file-table update calls known not to have run.
    pub file_table_update_rejected: u64,
    /// Number of file-table update calls whose kernel effect is unknown.
    pub file_table_update_unknown: u64,
    /// Number of fixed chunks submitted with metadata that differs from the ledger.
    pub fixed_chunk_identity_mismatches: u64,
    /// Number of provided-buffer completions with an unknown local bid.
    pub provided_unknown_bids: u64,
    /// Number of driver drops that had no provided ring to unregister.
    pub provided_drop_skipped_unregister: u64,
    /// Number of driver drops that attempted provided-ring unregister with active operations.
    pub provided_drop_active_unregister_attempts: u64,
    /// Number of driver drops that deferred provided-ring unregister until ring teardown.
    pub provided_drop_deferred_unregister: u64,
    /// Number of corrupt completions whose registered cleanup hint was invoked.
    pub corrupt_cleanup_attempts: u64,
    /// Number of non-negative raw fd results handled through a corrupt-completion hint.
    pub corrupt_raw_fd_cleanups: u64,
    /// Number of kernel corrupt completions for which no sidecar metadata existed.
    pub corrupt_cleanup_hint_missing: u64,
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
    pub(crate) fn inc_cancel_queued(&self) {
        Self::inc(&self.cancel_queued);
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
    pub(crate) fn inc_cancel_reconcile_timeout(&self) {
        Self::inc(&self.cancel_reconcile_timeout);
    }

    #[inline]
    pub(crate) fn inc_cancel_reconcile_deferred(&self) {
        Self::inc(&self.cancel_reconcile_deferred);
    }

    #[inline]
    pub(crate) fn inc_cqe_batch(&self) {
        Self::inc(&self.cqe_batches);
    }

    #[inline]
    pub(crate) fn add_cqes_collected(&self, count: usize) {
        self.cqe_collected
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_cqe_budget_hit(&self) {
        Self::inc(&self.cqe_budget_hits);
    }

    #[inline]
    pub(crate) fn inc_cqe_emergency_drain(&self) {
        Self::inc(&self.cqe_emergency_drains);
    }

    #[inline]
    pub(crate) fn inc_cqe_overflow(&self) {
        Self::inc(&self.cqe_overflow);
    }

    #[inline]
    pub(crate) fn add_timer_synthetic(&self, count: usize) {
        self.timer_synthetic
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn inc_timer_budget_hit(&self) {
        Self::inc(&self.timer_budget_hits);
    }

    #[inline]
    pub(crate) fn inc_completion_effect_overflow(&self) {
        Self::inc(&self.completion_effect_overflows);
    }

    #[inline]
    pub(crate) fn inc_cancel_ticket_exhausted(&self) {
        Self::inc(&self.cancel_ticket_exhausted);
    }

    #[inline]
    pub(crate) fn inc_cancel_untracked_cqe(&self) {
        Self::inc(&self.cancel_untracked_cqe);
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

    #[inline]
    pub(crate) fn inc_file_table_cleanup_failure(&self) {
        Self::inc(&self.file_table_cleanup_failures);
    }

    #[inline]
    pub(crate) fn inc_file_table_cleanup_short_update(&self) {
        Self::inc(&self.file_table_cleanup_short_updates);
    }

    #[inline]
    pub(crate) fn inc_file_table_quarantine(&self) {
        Self::inc(&self.file_table_quarantines);
    }

    #[inline]
    pub(crate) fn inc_file_table_rollback_failure(&self) {
        Self::inc(&self.file_table_rollback_failures);
    }

    #[inline]
    pub(crate) fn inc_file_table_poisoning(&self) {
        Self::inc(&self.file_table_poisonings);
    }

    #[inline]
    pub(crate) fn inc_file_table_update_applied(&self) {
        Self::inc(&self.file_table_update_applied);
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    pub(crate) fn inc_file_table_update_rejected(&self) {
        Self::inc(&self.file_table_update_rejected);
    }

    #[inline]
    pub(crate) fn inc_file_table_update_unknown(&self) {
        Self::inc(&self.file_table_update_unknown);
    }

    #[inline]
    pub(crate) fn inc_fixed_chunk_identity_mismatch(&self) {
        Self::inc(&self.fixed_chunk_identity_mismatches);
    }

    #[inline]
    pub(crate) fn inc_provided_unknown_bid(&self) {
        Self::inc(&self.provided_unknown_bids);
    }

    #[inline]
    pub(crate) fn inc_provided_drop_skipped_unregister(&self) {
        Self::inc(&self.provided_drop_skipped_unregister);
    }

    #[inline]
    pub(crate) fn inc_provided_drop_deferred_unregister(&self) {
        Self::inc(&self.provided_drop_deferred_unregister);
    }

    #[inline]
    pub(crate) fn inc_corrupt_cleanup_attempt(&self) {
        Self::inc(&self.corrupt_cleanup_attempts);
    }

    #[inline]
    pub(crate) fn inc_corrupt_raw_fd_cleanup(&self) {
        Self::inc(&self.corrupt_raw_fd_cleanups);
    }

    #[inline]
    pub(crate) fn inc_corrupt_cleanup_hint_missing(&self) {
        Self::inc(&self.corrupt_cleanup_hint_missing);
    }
}

impl DriverCompletionDiagnosticsBackend for UringCompletionDiagnostics {
    type Snapshot = UringCompletionDiagnosticsSnapshot;

    #[inline]
    fn snapshot(&self) -> Self::Snapshot {
        UringCompletionDiagnosticsSnapshot {
            cancel_submitted: Self::load(&self.cancel_submitted),
            cancel_queued: Self::load(&self.cancel_queued),
            cancel_local_completed: Self::load(&self.cancel_local_completed),
            cancel_target_missing: Self::load(&self.cancel_target_missing),
            cancel_target_stale: Self::load(&self.cancel_target_stale),
            cancel_target_corrupt: Self::load(&self.cancel_target_corrupt),
            cancel_ack_ok: Self::load(&self.cancel_ack_ok),
            cancel_ack_not_found: Self::load(&self.cancel_ack_not_found),
            cancel_ack_error: Self::load(&self.cancel_ack_error),
            cancel_ack_enoent_active: Self::load(&self.cancel_ack_enoent_active),
            cancel_reconcile_timeout: Self::load(&self.cancel_reconcile_timeout),
            cancel_reconcile_deferred: Self::load(&self.cancel_reconcile_deferred),
            cqe_batches: Self::load(&self.cqe_batches),
            cqe_collected: Self::load(&self.cqe_collected),
            cqe_budget_hits: Self::load(&self.cqe_budget_hits),
            cqe_emergency_drains: Self::load(&self.cqe_emergency_drains),
            cqe_overflow: Self::load(&self.cqe_overflow),
            timer_synthetic: Self::load(&self.timer_synthetic),
            timer_budget_hits: Self::load(&self.timer_budget_hits),
            completion_effect_overflows: Self::load(&self.completion_effect_overflows),
            cancel_ticket_exhausted: Self::load(&self.cancel_ticket_exhausted),
            cancel_duplicate_ticket: Self::load(&self.cancel_duplicate_ticket),
            cancel_untracked_cqe: Self::load(&self.cancel_untracked_cqe),
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
            file_table_cleanup_failures: Self::load(&self.file_table_cleanup_failures),
            file_table_cleanup_short_updates: Self::load(&self.file_table_cleanup_short_updates),
            file_table_quarantines: Self::load(&self.file_table_quarantines),
            file_table_rollback_failures: Self::load(&self.file_table_rollback_failures),
            file_table_poisonings: Self::load(&self.file_table_poisonings),
            file_table_update_applied: Self::load(&self.file_table_update_applied),
            file_table_update_rejected: Self::load(&self.file_table_update_rejected),
            file_table_update_unknown: Self::load(&self.file_table_update_unknown),
            fixed_chunk_identity_mismatches: Self::load(&self.fixed_chunk_identity_mismatches),
            provided_unknown_bids: Self::load(&self.provided_unknown_bids),
            provided_drop_skipped_unregister: Self::load(&self.provided_drop_skipped_unregister),
            provided_drop_active_unregister_attempts: Self::load(
                &self.provided_drop_active_unregister_attempts,
            ),
            provided_drop_deferred_unregister: Self::load(&self.provided_drop_deferred_unregister),
            corrupt_cleanup_attempts: Self::load(&self.corrupt_cleanup_attempts),
            corrupt_raw_fd_cleanups: Self::load(&self.corrupt_raw_fd_cleanups),
            corrupt_cleanup_hint_missing: Self::load(&self.corrupt_cleanup_hint_missing),
        }
    }

    #[inline]
    fn record_backend_anomaly(&self, _anomaly: &CompletionAnomaly) -> bool {
        false
    }
}
