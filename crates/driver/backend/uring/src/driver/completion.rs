use veloq_std::{
    format,
    num::NonZeroU8,
    time::{Duration, Instant},
};

#[cfg(test)]
use veloq_std::sync::atomic::Ordering;

#[cfg(test)]
use veloq_std::{collections::HashMap, sync::atomic::AtomicU8};

use diagweave::prelude::*;
use tracing::{debug, trace, warn};

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::control::{
        ControlPlaneEvent, DeferredCancelReconcile, ExpiredBatch, UringControlEffectKind,
        UringPostCompletionEffects,
    },
    driver::lifecycle::{CancellationPhase, SubmissionPhase},
    driver::{CompletionControlView, CqeEnv, PendingCancel, ProvidedBufGroup, UringDriver},
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{
        Close, CompletionCleanupHintFn, Slot, UringOperationDescriptor, UringRecordItem,
        UringSlotSpec,
    },
};

use crate::config::UringDriveLimits;
use crate::driver::submission::{KernelEnterPlan, WaitBudgetSource, txn::slot_access_report};

#[cfg(any(test, feature = "test-hooks"))]
use crate::driver::UringControlPlane;

#[cfg(test)]
use crate::driver::control::ControlPlaneObserver;
#[cfg(test)]
use crate::driver::control::waker::{WAKER_NOTIFIED, WAKER_PROCESSING};
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelMode, CancelTicket, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionCleanupGuard, CompletionContinuation, CompletionControl,
        CompletionEnvelope, CompletionFailure, CompletionFlowExt, CompletionFlowOutcome,
        CompletionIngress, CompletionSettlement, CompletionSource, CompletionToken, DriveMode,
        DriverCompletionDiagnostics, OpToken, PlatformOp, RawCompletion, SyntheticCompletionSource,
        UserCompletionEvent, run_completion_cleanup,
    },
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};

pub(crate) type CompletionProgress = CompletionFlowOutcome;
pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(2) {
        Some(val) => val,
        None => unreachable!(),
    });

enum CompletionCleanupHintState {
    Missing,
    Known(Option<CompletionCleanupHintFn>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitBudget {
    duration: Duration,
    source: WaitBudgetSource,
}

fn wait_budget(
    external_timeout: Option<Duration>,
    internal_timeout: Option<Duration>,
    probe: Duration,
) -> WaitBudget {
    let mut budget = WaitBudget {
        duration: probe,
        source: WaitBudgetSource::Probe,
    };

    if let Some(internal) = internal_timeout
        && internal <= budget.duration
    {
        budget = WaitBudget {
            duration: internal,
            source: WaitBudgetSource::Timer,
        };
    }
    if let Some(external) = external_timeout
        && external <= budget.duration
    {
        budget = WaitBudget {
            duration: external,
            source: WaitBudgetSource::External,
        };
    }
    budget
}

pub(crate) enum UringSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
    SubmissionFailure { report: Option<Report<UringError>> },
}

impl UringSyntheticCompletion {
    #[inline]
    fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None | Self::SubmissionFailure { .. } => CancelMode::UserVisible,
        }
    }

    #[inline]
    fn take_submission_failure(&mut self) -> Option<Report<UringError>> {
        match self {
            Self::SubmissionFailure { report } => report.take(),
            Self::None | Self::Cancel { .. } => None,
        }
    }
}

enum UringBackendEffect {
    None,
    Waker {
        recovery: WakerRecovery,
        generation: u64,
    },
    CancelEnoent {
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CancelPhase {
        target: OpToken,
        phase: CancellationPhase,
        cancel_ticket: CancelTicket,
    },
    CloseCompleted {
        token: OpToken,
        fd: IoFd,
    },
}

#[derive(Clone, Copy)]
enum WakerRecovery {
    Rearm,
    RebuildAndRearm,
}

#[inline]
fn remember_first_error(first_error: &mut Option<Report<UringError>>, report: Report<UringError>) {
    if first_error.is_none() {
        *first_error = Some(report);
    }
}

impl Default for UringBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct UringCompletionHooks<'a> {
    diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    control: CompletionControlView<'a>,
    provided_buffers: Option<&'a mut ProvidedBufGroup>,
    synthetic: UringSyntheticCompletion,
}

impl<'a> UringCompletionHooks<'a> {
    fn new(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        control: CompletionControlView<'a>,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
        synthetic: UringSyntheticCompletion,
    ) -> Self {
        Self {
            diagnostics,
            control,
            provided_buffers,
            synthetic,
        }
    }

    #[inline]
    fn cqe_env(&mut self) -> CqeEnv<'_> {
        CqeEnv::new(
            self.provided_buffers.as_deref_mut(),
            self.diagnostics.backend(),
        )
    }

    fn completion_cleanup_hint_for(
        &mut self,
        token: CompletionToken,
        flags: u32,
    ) -> CompletionCleanupHintState {
        let entry = if io_uring::cqueue::more(flags) {
            self.control.peek_completion_cleanup_hint(token)
        } else {
            self.control.remove_completion_cleanup_hint(token, flags)
        };
        match entry {
            Some(hint) => CompletionCleanupHintState::Known(hint),
            None => CompletionCleanupHintState::Missing,
        }
    }

    fn cleanup_corrupt_completion(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        source: CompletionSource<'_, ()>,
    ) -> CompletionCleanupGuard {
        let raw = event.raw();
        self.cqe_env().return_provided_buf(raw.flags);
        let hint = if matches!(source, CompletionSource::Kernel) {
            match self
                .control
                .remove_completion_cleanup_hint(raw.token, raw.flags)
            {
                Some(hint) => {
                    if hint.is_some() {
                        self.diagnostics.backend().inc_corrupt_cleanup_attempt();
                        if raw.res >= 0 {
                            self.diagnostics.backend().inc_corrupt_raw_fd_cleanup();
                        }
                    }
                    hint
                }
                None => {
                    self.diagnostics
                        .backend()
                        .inc_corrupt_cleanup_hint_missing();
                    None
                }
            }
        } else {
            None
        };

        debug!(
            completion_token = raw.token.raw(),
            cqe_result = raw.res,
            cqe_flags = raw.flags,
            anomaly = ?kind,
            "corrupt uring completion"
        );
        hint.map_or_else(CompletionCleanupGuard::default, |hint| hint(raw.res))
    }

    fn handle_waker_control(
        &mut self,
        raw: RawCompletion,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        let generation = self.control.waker_generation();
        if raw.res == self.control.waker_buf_len() as i32 {
            self.diagnostics.backend().inc_waker_ok();
            self.diagnostics.backend().inc_wait_waker_return();
            CompletionSettlement::ControlHandled {
                effect: UringBackendEffect::Waker {
                    recovery: WakerRecovery::Rearm,
                    generation,
                },
            }
        } else if raw.res >= 0 {
            self.diagnostics.backend().inc_waker_error();
            warn!(
                res = raw.res,
                expected = self.control.waker_buf_len(),
                "eventfd waker read returned unexpected byte count"
            );
            CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::control(
                    UringError::CompletionWait
                        .report(
                            "uring.completion.handle_waker_control",
                            format!(
                                "eventfd waker read returned {} bytes, expected {}",
                                raw.res,
                                self.control.waker_buf_len()
                            ),
                        )
                        .with_ctx("completion_result", raw.res),
                    UringBackendEffect::Waker {
                        recovery: WakerRecovery::RebuildAndRearm,
                        generation,
                    },
                ),
            }
        } else {
            self.diagnostics.backend().inc_waker_error();
            match -raw.res {
                libc::EAGAIN | libc::EINTR => {
                    debug!(res = raw.res, "recoverable eventfd waker read completion");
                    CompletionSettlement::ControlHandled {
                        effect: UringBackendEffect::Waker {
                            recovery: WakerRecovery::Rearm,
                            generation,
                        },
                    }
                }
                errno => {
                    warn!(res = raw.res, errno, "eventfd waker read failed");
                    CompletionSettlement::TerminalFailure {
                        failure: CompletionFailure::control(
                            UringError::CompletionWait
                                .report(
                                    "uring.completion.handle_waker_control",
                                    "eventfd waker read failed",
                                )
                                .set_error_code(errno),
                            UringBackendEffect::Waker {
                                recovery: WakerRecovery::RebuildAndRearm,
                                generation,
                            },
                        ),
                    }
                }
            }
        }
    }

    fn handle_cancel_control(
        &mut self,
        cancel_ticket: CancelTicket,
        raw: RawCompletion,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        let request = self.control.take_pending_cancel(cancel_ticket);
        let Some(request) = request else {
            self.diagnostics.backend().inc_cancel_untracked_cqe();
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::control(
                    UringError::InvalidState.report(
                        "uring.completion.handle_cancel_control",
                        format!(
                            "async cancel completion had no pending request for cancel_ticket: {}",
                            cancel_ticket.raw()
                        ),
                    ),
                    UringBackendEffect::None,
                ),
            };
        };
        match raw.res {
            value if value >= 0 => {
                self.diagnostics.backend().inc_cancel_ack_ok();
                trace!(
                    cancel_ticket = cancel_ticket.raw(),
                    request = ?request,
                    result = value,
                    "async cancel completed"
                );
                CompletionSettlement::ControlHandled {
                    effect: UringBackendEffect::CancelPhase {
                        target: request.target,
                        phase: CancellationPhase::Acked,
                        cancel_ticket,
                    },
                }
            }
            value if value == -libc::ENOENT => {
                self.diagnostics.backend().inc_cancel_ack_not_found();
                debug!(
                    cancel_ticket = cancel_ticket.raw(),
                    request = ?request,
                    "async cancel target was already complete or absent"
                );
                CompletionSettlement::ControlHandled {
                    effect: UringBackendEffect::CancelEnoent {
                        cancel_ticket,
                        request,
                        raw,
                    },
                }
            }
            value => {
                self.diagnostics.backend().inc_cancel_ack_error();
                warn!(
                    cancel_ticket = cancel_ticket.raw(),
                    request = ?request,
                    result = value,
                    errno = -value,
                    "async cancel request failed"
                );
                CompletionSettlement::ControlHandled {
                    effect: UringBackendEffect::CancelPhase {
                        target: request.target,
                        phase: CancellationPhase::Failed,
                        cancel_ticket,
                    },
                }
            }
        }
    }
}

impl CompletionBackendHooks<UringSlotSpec> for UringCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = UringBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionSettlement<UringSlotSpec, Self::BackendEffect> {
        match control {
            CompletionControl::Waker { raw, .. } => self.handle_waker_control(raw),
            CompletionControl::Cancel { ticket, raw } => self.handle_cancel_control(ticket, raw),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<UringSlotSpec, Self::BackendEffect> {
        match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => {
                complete_timer_waiting_slot(slot, event)
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => {
                complete_cancel_waiting_slot(slot, event, self.synthetic.cancel_mode())
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure) => {
                complete_submission_failure_slot(
                    slot,
                    event,
                    self.synthetic.take_submission_failure(),
                )
            }
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Backend(_) => {
                let raw = event.raw();
                if raw.res == -libc::ENOBUFS {
                    self.cqe_env().note_exhausted();
                }
                let hint = if matches!(source, CompletionSource::Kernel) {
                    Some(
                        match self
                            .control
                            .peek_completion_cleanup_hint(event.completion_token())
                        {
                            Some(hint) => CompletionCleanupHintState::Known(hint),
                            None => CompletionCleanupHintState::Missing,
                        },
                    )
                } else {
                    None
                };
                let mut cqe_env = self.cqe_env();
                match complete_kernel_waiting_slot(slot, event.token(), raw, &mut cqe_env) {
                    Ok(outcome) => {
                        if matches!(source, CompletionSource::Kernel)
                            && !io_uring::cqueue::more(raw.flags)
                        {
                            let _ = self.control.remove_completion_cleanup_hint(
                                event.completion_token(),
                                raw.flags,
                            );
                        }
                        outcome
                    }
                    Err(error) => {
                        // The hook may fail before it can take a selected provided buffer. The
                        // completion transaction owns that fallback, while the operation's own
                        // cleanup guard remains the sole owner of operation-specific cleanup.
                        cqe_env.return_provided_buf(raw.flags);
                        if error.fallback_cleanup
                            && let Some(CompletionCleanupHintState::Known(Some(hint))) = hint
                        {
                            let mut cleanup = hint(raw.res);
                            let _ = run_completion_cleanup(self.diagnostics, &mut cleanup);
                        }
                        if matches!(source, CompletionSource::Kernel)
                            && !io_uring::cqueue::more(raw.flags)
                        {
                            let _ = self.control.remove_completion_cleanup_hint(
                                event.completion_token(),
                                raw.flags,
                            );
                        }
                        if io_uring::cqueue::more(raw.flags) {
                            CompletionSettlement::Quarantined {
                                failure: CompletionFailure::quarantined(
                                    error.report,
                                    error.cleanup,
                                    UringBackendEffect::None,
                                ),
                            }
                        } else {
                            CompletionSettlement::TerminalFailure {
                                failure: CompletionFailure::terminal(
                                    error.report,
                                    error.cleanup,
                                    UringBackendEffect::None,
                                ),
                            }
                        }
                    }
                }
            }
        }
    }

    /// 一条完成落到了不存在 / 已陈旧的 slot 上。
    ///
    /// 记录本身没什么可救的，但它可能带着一个 provided buffer——那个 bid 已经被内核从环里
    /// 取走，不还就是永久少一个。core 的默认实现只记异常，所以这里必须覆盖。
    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<UringSlotSpec, Self::BackendEffect> {
        let cleanup = self.cleanup_corrupt_completion(event, kind, _source);
        CompletionSettlement::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            cleanup,
            effect: UringBackendEffect::None,
        }
    }

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<UringSlotSpec, Self::BackendEffect> {
        let res = match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => 0,
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => event.res(),
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure)
            | CompletionSource::Kernel
            | CompletionSource::User
            | CompletionSource::Backend(_) => event.raw().res,
        };
        // 这条完成要被丢弃，但内核已经从环里取走了它选中的 buffer——不还回去就是每次取消
        // 泄漏一个 bid。「取消不等于结束」在 provided buffer 上的具体形态。
        self.cqe_env().return_provided_buf(event.raw().flags);
        // 一个已放弃的 multishot 会继续投递完成，每一条都要跑 cleanup（accept 的话就是
        // 关掉那个内核已经建好的连接），但只有终态那条才能归还 slot。
        let continuation = if io_uring::cqueue::more(event.raw().flags) {
            CompletionContinuation::More
        } else {
            CompletionContinuation::Final
        };
        let hint = if matches!(source, CompletionSource::Kernel) {
            Some(self.completion_cleanup_hint_for(event.completion_token(), event.raw().flags))
        } else {
            None
        };
        let slot_cleanup_result = if continuation.is_more() {
            cleanup_orphaned_streaming_slot(slot, res)
        } else {
            cleanup_orphaned_slot(slot, res)
        };
        let (slot_cleanup, slot_accessed) = match slot_cleanup_result {
            Ok(value) => value,
            Err(error) => {
                return if continuation.is_more() {
                    CompletionSettlement::Quarantined {
                        failure: CompletionFailure::quarantined(
                            error,
                            CompletionCleanupGuard::default(),
                            UringBackendEffect::None,
                        ),
                    }
                } else {
                    CompletionSettlement::TerminalFailure {
                        failure: CompletionFailure::terminal(
                            error,
                            CompletionCleanupGuard::default(),
                            UringBackendEffect::None,
                        ),
                    }
                };
            }
        };
        let cleanup = if slot_accessed {
            slot_cleanup
        } else {
            match hint {
                Some(CompletionCleanupHintState::Known(Some(hint))) => hint(res),
                _ => CompletionCleanupGuard::default(),
            }
        };
        CompletionSettlement::Cleanup {
            cleanup,
            continuation,
            effect: UringBackendEffect::None,
        }
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> UringResult<()> {
        match effect {
            UringBackendEffect::None => Ok(()),
            UringBackendEffect::Waker {
                recovery,
                generation,
            } => {
                self.control.append_waker_effect(
                    generation,
                    matches!(recovery, WakerRecovery::RebuildAndRearm),
                );
                Ok(())
            }
            UringBackendEffect::CancelEnoent {
                cancel_ticket,
                request,
                raw,
            } => {
                self.control
                    .append_cancel_enoent(cancel_ticket, request, raw);
                Ok(())
            }
            UringBackendEffect::CancelPhase {
                target,
                phase,
                cancel_ticket,
            } => {
                self.control
                    .append_cancel_phase_update(cancel_ticket, target, phase);
                Ok(())
            }
            UringBackendEffect::CloseCompleted { token, fd } => {
                self.control.append_close_unregister(token, fd);
                Ok(())
            }
        }
    }
}

fn completion_observation(ingress: &CompletionIngress<()>) -> Option<(OpToken, bool)> {
    let (token, flags) = match ingress {
        CompletionIngress::Kernel(envelope) => (envelope.raw.token.op_token()?, envelope.raw.flags),
        CompletionIngress::User(event) | CompletionIngress::Synthetic { event, .. } => {
            (event.token(), event.flags())
        }
        CompletionIngress::Backend(_) | CompletionIngress::Anomaly { .. } => return None,
    };
    Some((token, !io_uring::cqueue::more(flags)))
}

#[cfg(any(test, feature = "test-hooks"))]
fn observe_completion_result(
    control: &mut UringControlPlane,
    observation: Option<(OpToken, bool)>,
    result: &UringResult<CompletionFlowOutcome>,
) {
    let Some((token, final_completion)) = observation else {
        return;
    };
    let Ok(progress) = result else {
        return;
    };
    if progress.user_completed == 0 && progress.orphan_cleaned == 0 {
        return;
    }

    control.record_completion_observation(token, final_completion);
}

pub(crate) struct DriveCycle {
    mode: DriveMode,
}

#[derive(Debug)]
struct DriveBudget {
    limits: UringDriveLimits,
    control_events: usize,
    cancel_actions: usize,
    backlog_actions: usize,
    cqes: usize,
    timers: usize,
}

impl DriveBudget {
    fn new(limits: UringDriveLimits) -> Self {
        Self {
            limits,
            control_events: limits.max_control_events,
            cancel_actions: limits.max_cancel_actions,
            backlog_actions: limits.max_backlog_actions,
            cqes: limits.max_cqes_per_drive,
            timers: limits.max_timer_expirations,
        }
    }
}

#[derive(Debug, Default)]
struct CqeCollection {
    count: usize,
    remaining: usize,
    collector_exhausted: bool,
    cqe_budget_hit: bool,
    emergency: bool,
    overflow: u32,
}

#[derive(Debug, Default)]
pub(crate) struct CompletionBatchProgress {
    pub(crate) flow: CompletionProgress,
    pub(crate) timer_count: usize,
    pub(crate) cqe_count: usize,
    pub(crate) pending_completion: bool,
    pub(crate) cqe_budget_hit: bool,
    pub(crate) emergency_drain: bool,
    pub(crate) cqe_overflow: bool,
}

impl DriveCycle {
    pub(crate) const fn new(mode: DriveMode) -> Self {
        Self { mode }
    }

    pub(crate) fn run(self, driver: &mut UringDriver<'_>) -> UringResult<()> {
        let mut budget = DriveBudget::new(driver.drive_limits);
        if matches!(self.mode, DriveMode::Wait { .. }) {
            driver.completion_diagnostics.backend().inc_wait_enter();
        }
        let drained = driver.drain_cancel_requests_bounded(budget.control_events)?;
        budget.control_events -= drained;
        let cancelled = driver.stage_pending_cancellations(budget.cancel_actions)?;
        budget.cancel_actions -= cancelled;
        let backlog = driver.stage_backlog_entries(budget.backlog_actions)?;
        budget.backlog_actions = budget.backlog_actions.saturating_sub(backlog.actions);
        driver.submit_waker()?;

        let mut next_mode = self.mode;
        for round in 0..budget.limits.max_submit_rounds {
            driver.submit_waker()?;
            let plan = driver.build_kernel_enter_plan(next_mode);
            let submit_progress = driver.submit_to_kernel(plan)?;
            trace!(
                round,
                staged = submit_progress.staged,
                kernel_outstanding = submit_progress.kernel_outstanding,
                pending_submit = submit_progress.pending_submit,
                receipt = ?submit_progress.receipt,
                "completed kernel enter round"
            );

            let completion = driver.process_completion_batch(&mut budget)?;
            let diagnostics = driver.completion_diagnostics.backend();
            diagnostics.inc_cqe_batch();
            diagnostics.add_cqes_collected(completion.cqe_count);
            diagnostics.add_timer_synthetic(completion.timer_count);
            if completion.cqe_budget_hit {
                diagnostics.inc_cqe_budget_hit();
            }
            if completion.emergency_drain {
                diagnostics.inc_cqe_emergency_drain();
            }
            if completion.cqe_overflow {
                diagnostics.inc_cqe_overflow();
            }
            if matches!(next_mode, DriveMode::Wait { .. }) && completion.flow.user_completed > 0 {
                driver
                    .completion_diagnostics
                    .backend()
                    .inc_wait_completion_return();
            }

            if matches!(next_mode, DriveMode::Wait { .. })
                && completion.timer_count > 0
                && !submit_progress.timed_out
            {
                driver
                    .completion_diagnostics
                    .backend()
                    .inc_wait_timer_return();
            }
            if submit_progress.timed_out {
                driver.completion_diagnostics.backend().inc_wait_timeout();
                if let Some(source) = plan.wait_source {
                    match source {
                        WaitBudgetSource::External => driver
                            .completion_diagnostics
                            .backend()
                            .inc_wait_external_timeout(),
                        WaitBudgetSource::Timer => driver
                            .completion_diagnostics
                            .backend()
                            .inc_wait_timer_return(),
                        WaitBudgetSource::Probe => driver
                            .completion_diagnostics
                            .backend()
                            .inc_wait_probe_return(),
                    }
                }
            }

            let cancelled = driver.stage_pending_cancellations(budget.cancel_actions)?;
            budget.cancel_actions -= cancelled;
            let backlog = driver.stage_backlog_entries(budget.backlog_actions)?;
            budget.backlog_actions = budget.backlog_actions.saturating_sub(backlog.actions);
            driver.check_control_plane_invariants()?;

            let pending_submit = submit_progress.pending_submit
                || driver.control.unpublished_staged_entry_count() > 0;
            if !pending_submit
                && !completion.pending_completion
                && driver.control.cancellations.pending_len() == 0
                && !driver.control.backlog.contains_any()
                && !driver.control.timers.has_pending_expired()
            {
                break;
            }
            if round + 1 == budget.limits.max_submit_rounds {
                trace!("submit round budget reached with staged entries pending");
                break;
            }
            next_mode = DriveMode::Poll;
        }
        Ok(())
    }
}

impl<'a> UringDriver<'a> {
    const WAKE_FAILURE_PROBE_INTERVAL: Duration = Duration::from_secs(1);

    pub(crate) fn build_kernel_enter_plan(&mut self, mode: DriveMode) -> KernelEnterPlan {
        let to_submit = self.ring.submission().len();
        match mode {
            DriveMode::Poll => KernelEnterPlan::poll(to_submit),
            DriveMode::Wait { timeout } => {
                let cq_ready = {
                    let mut completion = self.ring.completion();
                    completion.sync();
                    !completion.is_empty()
                };
                let ready_completion = self.ops.shared.has_ready_completion();
                if cq_ready || ready_completion {
                    self.completion_diagnostics
                        .backend()
                        .inc_wait_ready_preflight();
                    return KernelEnterPlan::poll_ready(to_submit);
                }

                let budget = wait_budget(
                    timeout,
                    self.control.timers.next_timeout(),
                    Self::WAKE_FAILURE_PROBE_INTERVAL,
                );
                if budget.duration.is_zero() {
                    self.completion_diagnostics.backend().inc_wait_zero();
                    KernelEnterPlan::poll_zero_timeout(to_submit)
                } else {
                    self.completion_diagnostics.backend().inc_wait_block();
                    KernelEnterPlan::wait(to_submit, budget.duration, budget.source)
                }
            }
        }
    }

    /// Advances the timer wheel by however many whole ticks elapsed since the last poll.
    fn advance_timer_clock(&mut self) -> ExpiredBatch {
        let now = Instant::now();
        let expired = self.control.timers.advance_timer_wheel(now);

        #[cfg(any(test, feature = "test-hooks"))]
        for &token in expired.newly_expired_iter() {
            let task_id = self.control.timer_for(token);
            self.control
                .record(ControlPlaneEvent::TimerExpire { task_id, token });
        }
        expired
    }

    fn quarantine_expired_timer(&mut self, token: OpToken) {
        self.control.quarantine_timer(token);
        let Ok(view) = self.ops.checked_slot_view(token) else {
            return;
        };
        match view {
            CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                slot.platform_mut().timer_id = None;
                slot.platform_mut().control.submission = SubmissionPhase::Terminal;
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                slot.platform_mut().timer_id = None;
                slot.platform_mut().control.submission = SubmissionPhase::Terminal;
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                slot.platform_mut().timer_id = None;
                slot.platform_mut().control.submission = SubmissionPhase::Terminal;
            }
            CheckedSlotView::Empty(_)
            | CheckedSlotView::Missing { .. }
            | CheckedSlotView::Stale(_) => {}
        }
    }

    fn collect_cqes(&mut self, max_cqes: usize) -> CqeCollection {
        let mut collection = CqeCollection::default();
        let mut cq = self.ring.completion();
        cq.sync();
        let visible = cq.len();
        collection.overflow = cq.overflow();
        let normal_limit = visible.min(max_cqes).min(self.drive_limits.max_cqe_batch);
        let high_water = cq.capacity().saturating_mul(3) / 4;
        let emergency =
            visible > normal_limit && (visible >= high_water.max(1) || collection.overflow != 0);
        let limit = if emergency {
            visible
                .min(self.drive_limits.emergency_drain_limit)
                .min(self.cqe_buffer.capacity())
        } else {
            normal_limit
        };

        trace!(visible, limit, emergency, "collecting uring completions");
        for _ in 0..limit {
            let Some(cqe) = cq.next() else {
                break;
            };
            let raw_token = cqe.user_data();
            if raw_token == CompletionToken::waker(0).raw() {
                self.control.waker.begin_processing();
            }
            self.cqe_buffer.push((raw_token, cqe.result(), cqe.flags()));
            collection.count += 1;
        }
        collection.remaining = cq.len();
        collection.collector_exhausted = collection.remaining == 0;
        collection.cqe_budget_hit = visible > normal_limit;
        collection.emergency = emergency;
        collection
    }

    fn process_completion_batch(
        &mut self,
        budget: &mut DriveBudget,
    ) -> UringResult<CompletionBatchProgress> {
        self.cqe_buffer.clear();
        let collection = self.collect_cqes(budget.cqes);
        budget.cqes = budget.cqes.saturating_sub(collection.count);

        let mut batch_effects = self
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        batch_effects.clear();
        let mut progress = CompletionProgress::default();
        let mut first_error = None;

        for index in 0..self.cqe_buffer.len() {
            let (raw_token, cqe_res, cqe_flags) = self.cqe_buffer[index];
            let envelope = CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_URING,
                raw_token,
                cqe_res,
                cqe_flags,
            );
            if let Err(error) = self
                .control
                .settle_staged_completion(envelope.raw.token, !io_uring::cqueue::more(cqe_flags))
            {
                remember_first_error(
                    &mut first_error,
                    UringError::InvalidState
                        .report(
                            "uring.completion.settle_staged",
                            format!("staged completion settlement failed: {error:?}"),
                        )
                        .attach_note(
                            "a CQE could not be correlated with its staged ledger entry; routing continues for cleanup",
                        ),
                );
            }
            let (_observation, outcome) = self.accept_completion_transaction_into(
                CompletionIngress::Kernel(envelope),
                UringSyntheticCompletion::None,
                &mut batch_effects,
            );
            #[cfg(any(test, feature = "test-hooks"))]
            observe_completion_result(&mut self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => remember_first_error(&mut first_error, report),
            }
        }

        let expired = self.advance_timer_clock();
        let timer_count = expired.len().min(budget.timers);
        budget.timers = budget.timers.saturating_sub(timer_count);
        if expired.len() > timer_count {
            self.completion_diagnostics.backend().inc_timer_budget_hit();
        }
        for &token in expired.iter().take(timer_count) {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            let (_observation, outcome) = self.accept_completion_transaction_into(
                CompletionIngress::Synthetic {
                    event,
                    source: SyntheticCompletionSource::Timer,
                },
                UringSyntheticCompletion::None,
                &mut batch_effects,
            );
            #[cfg(any(test, feature = "test-hooks"))]
            observe_completion_result(&mut self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => {
                    self.quarantine_expired_timer(token);
                    remember_first_error(&mut first_error, report);
                }
            }
        }
        self.control.timers.recycle_expired(expired, timer_count);

        if let Err(report) =
            self.apply_post_completion_effects(&batch_effects, collection.collector_exhausted)
        {
            remember_first_error(&mut first_error, report);
        }
        batch_effects.clear();
        self.effect_accumulator = Some(batch_effects);
        self.cqe_buffer.clear();

        let invariant_result = self.check_control_plane_invariants();
        if let Err(invariant_error) = invariant_result {
            remember_first_error(&mut first_error, invariant_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(CompletionBatchProgress {
            flow: progress,
            timer_count,
            cqe_count: collection.count,
            pending_completion: collection.remaining > 0,
            cqe_budget_hit: collection.cqe_budget_hit,
            emergency_drain: collection.emergency,
            cqe_overflow: collection.overflow != 0,
        })
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            UringSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let mut effects = self
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        effects.clear();
        let (_observation, flow_result) =
            self.accept_completion_transaction_into(ingress, synthetic, &mut effects);
        #[cfg(any(test, feature = "test-hooks"))]
        observe_completion_result(&mut self.control, _observation, &flow_result);
        let post_result = self.apply_post_completion_effects(&effects, true);
        effects.clear();
        self.effect_accumulator = Some(effects);
        match (flow_result, post_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(post_error)) => Err(post_error),
            (Err(flow_error), Err(post_error)) => Err(post_error.with_diag_src_err(flow_error)),
        }
    }

    fn accept_completion_transaction_into(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: UringSyntheticCompletion,
        effects: &mut UringPostCompletionEffects,
    ) -> (Option<(OpToken, bool)>, UringResult<CompletionFlowOutcome>) {
        let observation = completion_observation(&ingress);
        let control = self.control.with_completion_view();
        let mut hooks = UringCompletionHooks::new(
            &self.completion_diagnostics,
            control,
            self.buffer_registry.provided_buffers_mut(),
            synthetic,
        );
        let flow_result = self.ops.accept_completion(
            &self.completion_table,
            &self.completion_diagnostics,
            &mut hooks,
            ingress,
        );
        drop(hooks);
        self.capabilities.provided_buffers = self.buffer_registry.provided_buffers_enabled();
        self.control.drain_post_effects_into(effects);
        if flow_result.is_err()
            && let Some((token, _)) = observation
        {
            self.control.quarantine_token(token);
        }
        if let Some((token, true)) = observation
            && let Ok(progress) = &flow_result
            && (progress.user_completed > 0 || progress.orphan_cleaned > 0)
        {
            self.control.release_quarantined_token(token);
        }
        (observation, flow_result)
    }

    fn apply_post_completion_effects(
        &mut self,
        post: &UringPostCompletionEffects,
        collector_exhausted: bool,
    ) -> UringResult<()> {
        let mut first_error = None;
        if post.is_overflowed() {
            self.completion_diagnostics
                .backend()
                .inc_completion_effect_overflow();
            remember_first_error(
                &mut first_error,
                UringError::InvalidState
                    .report(
                        "uring.completion.effects",
                        "completion effect accumulator capacity was exhausted",
                    )
                    .attach_note(
                        "control effects were bounded; rebuild the driver after this error",
                    ),
            );
        }
        self.execute_bookkeeping(post, collector_exhausted, &mut first_error);

        // The user completion has already been published when this post-effect runs. A cleanup
        // failure must be reported without stopping the remaining Close effects: every successful
        // kernel Close still has to consume its owned handle exactly once.
        self.execute_close_effects(post, &mut first_error);
        self.execute_waker_effects(post, &mut first_error);
        if post.has_backlog_kick() {
            // A backlog kick is only a request for the next bounded staging round. It must not
            // recursively enter submission while completion effects are being applied.
            trace!("completion batch requested a backlog submit round");
        }
        first_error.map_or(Ok(()), Err)
    }

    fn execute_bookkeeping(
        &mut self,
        effects: &UringPostCompletionEffects,
        collector_exhausted: bool,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            match effect.kind {
                UringControlEffectKind::CancelAck {
                    cancel_ticket,
                    phase,
                } => {
                    if let Some(target) = effect.token {
                        self.set_cancel_phase(target, phase);
                        let _ = self
                            .control
                            .cancellations
                            .finish_ticket(cancel_ticket, target);
                    }
                }
                UringControlEffectKind::CancelReconcile {
                    cancel_ticket,
                    request,
                    raw,
                } => {
                    self.completion_diagnostics
                        .backend()
                        .inc_cancel_reconcile_deferred();
                    if let Err(()) =
                        self.control
                            .defer_cancel_reconcile(cancel_ticket, request, raw)
                    {
                        remember_first_error(
                            first_error,
                            UringError::InvalidState
                                .report(
                                    "uring.cancel.reconcile",
                                    "cancel reconcile ledger is full",
                                )
                                .attach_note(
                                    "the target remains quarantined because its ENOENT could not be tracked",
                                ),
                        );
                    }
                }
                _ => {}
            }
        }
        self.reconcile_deferred_cancels(collector_exhausted, first_error);
    }

    fn execute_close_effects(
        &mut self,
        effects: &UringPostCompletionEffects,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            let UringControlEffectKind::CloseUnregister { fd } = effect.kind else {
                continue;
            };
            if let Err(report) = self.unregister_close_owned_fd(fd) {
                remember_first_error(first_error, report);
            }
        }
    }

    fn execute_waker_effects(
        &mut self,
        effects: &UringPostCompletionEffects,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            if let UringControlEffectKind::WakerRebuild { generation } = effect.kind {
                if generation != self.control.waker.armed_generation() {
                    continue;
                }
                self.completion_diagnostics.backend().inc_waker_rebuild();
                if let Err(report) = self
                    .rebuild_waker_fd()
                    .attach_note("failed to rebuild eventfd waker")
                {
                    remember_first_error(first_error, report);
                }
            }
        }

        for effect in effects.iter() {
            if let UringControlEffectKind::WakerRearm { generation } = effect.kind
                && self.control.waker.prepare_rearm(generation)
            {
                self.control
                    .record(ControlPlaneEvent::WakerArm { armed: false });
                self.control.record(ControlPlaneEvent::WakerRearmRequested);
                self.control.waker_stage_pending = true;
                self.completion_diagnostics.backend().inc_waker_rearm();
            }
        }
    }

    fn reconcile_deferred_cancels(
        &mut self,
        collector_exhausted: bool,
        first_error: &mut Option<Report<UringError>>,
    ) {
        let timeout = self.drive_limits.cancel_reconcile_timeout;
        let mut index = 0;
        while index < self.control.deferred_cancel_reconciles().len() {
            let entry = self.control.deferred_cancel_reconciles()[index];
            let active = match self.ops.checked_slot_view(entry.request.target) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => Some(slot.snapshot()),
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Empty(_))
                | Ok(CheckedSlotView::Missing { .. })
                | Ok(CheckedSlotView::Stale(_)) => None,
                Err(report) => {
                    remember_first_error(first_error, report);
                    index += 1;
                    continue;
                }
            };
            let watchdog_expired = Instant::now().saturating_duration_since(entry.since) >= timeout;
            if !collector_exhausted && !watchdog_expired {
                index += 1;
                continue;
            }

            if active.is_none() {
                let _ = self.control.remove_deferred_cancel_reconcile(index);
                let _ = self
                    .control
                    .cancellations
                    .finish_ticket(entry.cancel_ticket, entry.request.target);
                continue;
            }
            if watchdog_expired && !collector_exhausted {
                self.completion_diagnostics
                    .backend()
                    .inc_cancel_reconcile_timeout();
            }
            match self.quarantine_cancel_target(entry) {
                Ok(()) => {
                    let _ = self.control.remove_deferred_cancel_reconcile(index);
                    let _ = self
                        .control
                        .cancellations
                        .finish_ticket(entry.cancel_ticket, entry.request.target);
                }
                Err(report) => {
                    remember_first_error(first_error, report);
                    index += 1;
                }
            }
        }
    }

    fn quarantine_cancel_target(&mut self, entry: DeferredCancelReconcile) -> UringResult<()> {
        let snapshot = match self.ops.checked_slot_view(entry.request.target)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let snapshot = slot.snapshot();
                if !self
                    .completion_table
                    .mark_orphaned(entry.request.target)
                    .is_applied()
                {
                    return Err(UringError::InvalidState.report(
                        "uring.cancel.reconcile.quarantine",
                        "unable to move active cancel target to orphaned state",
                    ));
                }
                snapshot
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => slot.snapshot(),
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot.snapshot(),
            CheckedSlotView::Empty(_)
            | CheckedSlotView::Missing { .. }
            | CheckedSlotView::Stale(_) => return Ok(()),
        };
        self.completion_diagnostics
            .backend()
            .inc_cancel_ack_enoent_active();
        self.control.quarantine_token(entry.request.target);
        self.set_cancel_phase(entry.request.target, CancellationPhase::NotFound);
        Err(UringError::InvalidState
            .report(
                "uring.cancel.reconcile",
                "io_uring cancel returned ENOENT while target remained active",
            )
            .with_ctx("cancel_ticket", entry.cancel_ticket.raw())
            .with_ctx("expected_index", entry.request.target.index())
            .with_ctx("expected_generation", entry.request.target.generation())
            .with_ctx("actual_index", snapshot.index)
            .with_ctx("actual_generation", snapshot.generation)
            .with_ctx("slot_status", format!("{:?}", snapshot.status))
            .with_ctx("raw_cqe_res", entry.raw.res)
            .with_ctx("raw_cqe_flags", entry.raw.flags)
            .attach_note(
                "The target was moved to orphan cleanup and will not be reused until its final CQE arrives.",
            ))
    }
}

struct KernelCompletionError {
    report: Report<UringError>,
    fallback_cleanup: bool,
    cleanup: CompletionCleanupGuard,
}

fn record_item_policy_report(operation: &'static str, token: OpToken) -> Report<UringError> {
    UringError::Internal
        .report(
            "uring.driver.completion.record_item",
            "operation record-item policy did not produce a completion item",
        )
        .with_ctx("operation", operation)
        .with_ctx("token_index", token.index())
        .with_ctx("token_generation", token.generation())
}

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
    cqe_env: &mut CqeEnv<'_>,
) -> Result<CompletionSettlement<UringSlotSpec, UringBackendEffect>, KernelCompletionError> {
    // `IORING_CQE_F_MORE`：内核声明这个操作还会继续投递完成。flags 的解读到此为止，
    // core 只见 `CompletionContinuation`。
    let continuation = if io_uring::cqueue::more(raw.flags) {
        CompletionContinuation::More
    } else {
        CompletionContinuation::Final
    };

    let (final_res, cleanup, record_item, operation_name) = match slot.with_access_mut(|access| {
        let descriptor = access.operation().get_ref().descriptor();
        let final_res = unsafe { (descriptor.on_complete)(access, token, raw.res) };
        let cleanup = (descriptor.completion_cleanup)(raw.res);
        let record_item =
            unsafe { (descriptor.record_item)(access, token, raw.res, raw.flags, cqe_env) };
        (final_res, cleanup, record_item, descriptor.name)
    }) {
        Ok(result) => result,
        Err(err) => {
            return Err(KernelCompletionError {
                report: UringError::InvalidState.report(
                    "uring.complete_kernel_waiting_slot",
                    format!("slot corruption detected on completion: {:?}", err),
                ),
                fallback_cleanup: true,
                cleanup: CompletionCleanupGuard::default(),
            });
        }
    };
    let record_item = match record_item {
        Ok(record_item) => record_item,
        Err(report) => {
            return Err(KernelCompletionError {
                report,
                fallback_cleanup: false,
                cleanup,
            });
        }
    };
    let res_code = driver_result_to_event_res(&final_res);
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags);
    let res_is_ok = final_res.is_ok();
    // 完成本身的错误在没有更具体的 `detail` 时充当 detail，与队列化之前逐条等价。
    let mut res_error = final_res.err();

    if continuation.is_more() {
        // slot 原地不动：op 与提交 payload 还要给内核后续的完成用，cell 也必须停在
        // `InFlightWaiting` 才能继续路由。
        let UringRecordItem::New(item) = record_item else {
            return Err(KernelCompletionError {
                report: record_item_policy_report(operation_name, token),
                fallback_cleanup: false,
                cleanup,
            });
        };
        return Ok(CompletionSettlement::User {
            event,
            payload: item,
            detail: res_error.take().map(Err),
            cleanup,
            continuation,
            effect: UringBackendEffect::None,
        });
    }

    slot.platform_mut().control.submission = SubmissionPhase::Terminal;
    let mut completed = slot.complete();
    let (submit_payload, detail) = completed.take_completion_data();
    // multishot 的终态完成同样产出一条 item，提交 payload（监听 socket 之类）到此为止。
    let payload = match record_item {
        UringRecordItem::New(item) => {
            drop(submit_payload);
            item
        }
        UringRecordItem::UseSubmitPayload => {
            let Some(payload) = submit_payload else {
                drop(detail);
                return Err(KernelCompletionError {
                    report: UringError::InvalidState.report(
                        "uring.complete_kernel_waiting_slot",
                        "slot payload missing on completion",
                    ),
                    fallback_cleanup: false,
                    cleanup,
                });
            };
            payload
        }
    };

    let effect = if res_is_ok {
        if let Some(close) = <Close as UringOperationDescriptor>::user_payload_ref(&payload) {
            UringBackendEffect::CloseCompleted {
                token,
                fd: close.fd,
            }
        } else {
            UringBackendEffect::None
        }
    } else {
        UringBackendEffect::None
    };

    let detail = detail.or_else(|| res_error.take().map(Err));
    let _ = completed.take_op();

    Ok(CompletionSettlement::User {
        event,
        payload,
        detail,
        cleanup,
        continuation,
        effect,
    })
}

fn complete_timer_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
    slot.platform_mut().timer_id = None;
    slot.platform_mut().control.submission = SubmissionPhase::Terminal;
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                UringError::InvalidState.report(
                    "uring.complete_timer_waiting_slot",
                    "slot payload missing on timer completion",
                ),
                CompletionCleanupGuard::default(),
                UringBackendEffect::None,
            ),
        };
    };

    CompletionSettlement::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        // 软件定时器只会触发一次。
        continuation: CompletionContinuation::Final,
        effect: UringBackendEffect::None,
    }
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
    complete_local_cancel_slot(slot, event, mode, false)
}

fn complete_submission_failure_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<UringError>>,
) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
    let event_res = event.res();
    slot.platform_mut().control.submission = SubmissionPhase::Terminal;
    let cleanup = match slot
        .with_access_mut(|access| PlatformOp::completion_cleanup(access.operation_mut(), event_res))
    {
        Ok(cleanup) => cleanup,
        Err(err) => {
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    slot_access_report("uring.complete_submission_failure_slot.cleanup", err),
                    CompletionCleanupGuard::default(),
                    UringBackendEffect::None,
                ),
            };
        }
    };
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                UringError::InvalidState.report(
                    "uring.complete_submission_failure_slot",
                    "slot payload missing on submission failure",
                ),
                cleanup,
                UringBackendEffect::None,
            ),
        };
    };

    CompletionSettlement::User {
        event,
        payload,
        detail: detail.or(report.map(Err)),
        cleanup,
        // 提交失败的操作从未进入内核，不会再有完成。
        continuation: CompletionContinuation::Final,
        effect: UringBackendEffect::None,
    }
}

fn complete_local_cancel_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
    orphaned: bool,
) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
    slot.platform_mut().control.submission = SubmissionPhase::Terminal;
    let cleanup = match slot.with_access_mut(|access| {
        let operation = access.operation_mut();
        if mode == CancelMode::Abandon || orphaned {
            PlatformOp::orphan_cleanup(operation, event.res())
        } else {
            PlatformOp::completion_cleanup(operation, event.res())
        }
    }) {
        Ok(cleanup) => cleanup,
        Err(err) => {
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    slot_access_report("uring.complete_local_cancel_slot.cleanup", err),
                    CompletionCleanupGuard::default(),
                    UringBackendEffect::None,
                ),
            };
        }
    };
    let mut completed = slot.complete();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();

    match (mode, payload) {
        (CancelMode::UserVisible, Some(payload)) => CompletionSettlement::User {
            event,
            payload,
            detail,
            cleanup,
            // 本地取消是这个操作的终点，不管它原本是不是 multishot。
            continuation: CompletionContinuation::Final,
            effect: UringBackendEffect::None,
        },
        (CancelMode::UserVisible, None) => {
            drop(detail);
            CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    UringError::InvalidState.report(
                        "uring.complete_local_cancel_slot",
                        "slot payload missing on cancel",
                    ),
                    cleanup,
                    UringBackendEffect::None,
                ),
            }
        }
        (CancelMode::Abandon, payload) => {
            drop(payload);
            drop(detail);
            CompletionSettlement::Cleanup {
                cleanup,
                continuation: CompletionContinuation::Final,
                effect: UringBackendEffect::None,
            }
        }
    }
}

/// 一个仍在途的、已被放弃的 multishot 收到中间完成：只取 cleanup，**不掏空 slot**。
///
/// 与 [`cleanup_orphaned_slot`] 的差别就在这里——那个会 `take_op` / `take_completion_data`，
/// 而内核还要用它们继续投递完成。
fn cleanup_orphaned_streaming_slot(
    mut slot: Slot<'_, InFlightOrphaned>,
    cqe_res: i32,
) -> UringResult<(CompletionCleanupGuard, bool)> {
    let cleanup = slot
        .with_access_mut(|access| PlatformOp::orphan_cleanup(access.operation_mut(), cqe_res))
        .map_err(|err| slot_access_report("uring.cleanup_orphaned_streaming_slot.cleanup", err))?;
    Ok((cleanup, true))
}

fn cleanup_orphaned_slot(
    mut slot: Slot<'_, InFlightOrphaned>,
    cqe_res: i32,
) -> UringResult<(CompletionCleanupGuard, bool)> {
    slot.platform_mut().control.submission = SubmissionPhase::Terminal;
    let cleanup = slot
        .with_access_mut(|access| PlatformOp::orphan_cleanup(access.operation_mut(), cqe_res))
        .map_err(|err| slot_access_report("uring.cleanup_orphaned_slot.cleanup", err))?;
    let mut completed = slot.complete();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();
    drop(payload);
    drop(detail);
    Ok((cleanup, true))
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{ProvidedBufGroup, registration::test_group};
    use crate::op::{
        Accept, AcceptMulti, Open, ReadRaw, Recv, UringOpRegistry, UringOperationDescriptor,
        WriteRaw,
    };
    use veloq_driver_core::driver::{CompletionToken, SharedCompletionTable};
    use veloq_driver_core::slot::Generation;

    fn test_hooks<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        _waker_armed: &'a mut bool,
        _notification_state: &'a AtomicU8,
        observer: &'a mut ControlPlaneObserver,
        post: &'a mut UringPostCompletionEffects,
    ) -> UringCompletionHooks<'a> {
        let control = CompletionControlView::new(
            pending_cancel_cqes,
            completion_cleanup_hints,
            8,
            0,
            observer,
            post,
        );
        UringCompletionHooks::new(diagnostics, control, None, UringSyntheticCompletion::None)
    }

    #[allow(clippy::too_many_arguments)]
    fn test_hooks_with_buffers<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        _waker_armed: &'a mut bool,
        _notification_state: &'a AtomicU8,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
        observer: &'a mut ControlPlaneObserver,
        post: &'a mut UringPostCompletionEffects,
    ) -> UringCompletionHooks<'a> {
        let control = CompletionControlView::new(
            pending_cancel_cqes,
            completion_cleanup_hints,
            8,
            0,
            observer,
            post,
        );
        UringCompletionHooks::new(
            diagnostics,
            control,
            provided_buffers,
            UringSyntheticCompletion::None,
        )
    }

    fn accept_corrupt(
        registry: &mut UringOpRegistry,
        sidecar: &mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        token: OpToken,
        result: i32,
        flags: u32,
    ) -> CompletionFlowOutcome {
        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared.clone();
        let mut pending_cancel_cqes = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        for token in sidecar.keys().copied() {
            observer.record(ControlPlaneEvent::CleanupHintInsert(token));
        }
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            sidecar,
            &mut waker_armed,
            &notification_state,
            &mut observer,
            &mut post,
        );
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            result,
            flags,
        );
        registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled")
    }

    fn open_test_fds() -> [i32; 2] {
        let mut fds = [-1; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        assert!(fds[0] > 2 && fds[1] > 2);
        fds
    }

    fn assert_closed(fd: i32) {
        assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    }

    #[test]
    fn waker_control_records_unexpected_byte_count_as_error() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
            &mut observer,
            &mut post,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 4, 0);

        let outcome = hooks.handle_waker_control(raw);
        let (error, effect) = match outcome {
            CompletionSettlement::TerminalFailure { failure } => (failure.error, failure.effect),
            _ => panic!("unexpected byte count must produce a failed outcome"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker recovery effect should be recorded");

        assert_eq!(*error.inner(), UringError::CompletionWait);
        drop(hooks);
        assert!(waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_PROCESSING);
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind, UringControlEffectKind::WakerRebuild { .. }))
        );
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind, UringControlEffectKind::WakerRearm { .. }))
        );
        assert_eq!(diagnostics.snapshot().backend.waker_error, 1);
    }

    #[test]
    fn waker_control_rearms_successful_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
            &mut observer,
            &mut post,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 8, 0);

        let outcome = hooks.handle_waker_control(raw);
        let effect = match outcome {
            CompletionSettlement::ControlHandled { effect } => effect,
            _ => panic!("a valid eventfd read must be handled successfully"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker rearm effect should be recorded");

        drop(hooks);
        assert!(waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_PROCESSING);
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind, UringControlEffectKind::WakerRearm { .. }))
        );
        assert!(
            !post
                .effects()
                .iter()
                .any(|effect| matches!(effect.kind, UringControlEffectKind::WakerRebuild { .. }))
        );
        assert_eq!(diagnostics.snapshot().backend.waker_ok, 1);
        assert_eq!(diagnostics.snapshot().backend.waker_error, 0);
    }

    #[test]
    fn recoverable_waker_errors_still_rearm() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        for res in [-libc::EAGAIN, -libc::EINTR] {
            let mut pending_cancel_cqes = HashMap::default();
            let mut completion_cleanup_hints = HashMap::default();
            let mut waker_armed = true;
            let notification_state = AtomicU8::new(WAKER_PROCESSING);
            let mut observer = ControlPlaneObserver::default();
            let mut post = UringPostCompletionEffects::default();
            let mut hooks = test_hooks(
                &diagnostics,
                &mut pending_cancel_cqes,
                &mut completion_cleanup_hints,
                &mut waker_armed,
                &notification_state,
                &mut observer,
                &mut post,
            );
            let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), res, 0);

            let outcome = hooks.handle_waker_control(raw);
            let effect = match outcome {
                CompletionSettlement::ControlHandled { effect } => effect,
                _ => panic!("recoverable eventfd errors must be rearmed"),
            };
            hooks
                .finish_backend_effect(effect)
                .expect("waker rearm effect should be recorded");
            drop(hooks);

            assert!(waker_armed);
            assert_eq!(notification_state.load(Ordering::Acquire), WAKER_PROCESSING);
        }

        let snapshot = diagnostics.snapshot().backend;
        assert_eq!(snapshot.waker_error, 2);
        assert_eq!(snapshot.waker_rearm, 0);
    }

    #[test]
    fn waker_finish_does_not_overwrite_a_concurrent_notification() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
            &mut observer,
            &mut post,
        );

        notification_state.store(WAKER_NOTIFIED, Ordering::Release);
        hooks
            .finish_backend_effect(UringBackendEffect::Waker {
                recovery: WakerRecovery::Rearm,
                generation: 0,
            })
            .expect("waker rearm effect should be recorded");
        drop(hooks);

        assert!(waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_NOTIFIED);
    }

    #[test]
    fn wait_budget_keeps_the_explicit_deadline_source() {
        assert_eq!(
            wait_budget(
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(3)),
                Duration::from_secs(1),
            ),
            WaitBudget {
                duration: Duration::from_secs(1),
                source: WaitBudgetSource::Probe,
            }
        );
        assert_eq!(
            wait_budget(
                Some(Duration::from_millis(3)),
                Some(Duration::from_millis(7)),
                Duration::from_secs(1),
            ),
            WaitBudget {
                duration: Duration::from_millis(3),
                source: WaitBudgetSource::External,
            }
        );
        assert_eq!(
            wait_budget(None, Some(Duration::from_millis(5)), Duration::from_secs(1),),
            WaitBudget {
                duration: Duration::from_millis(5),
                source: WaitBudgetSource::Timer,
            }
        );
    }

    #[test]
    fn untracked_cancel_cqe_is_anomaly_not_user_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
            &mut observer,
            &mut post,
        );
        let cancel_ticket = CancelTicket::try_new(7).expect("test ticket");
        let raw = RawCompletion::new(
            COMP_BACKEND_URING,
            CompletionToken::cancel(cancel_ticket),
            0,
            0,
        );

        let outcome = hooks.handle_cancel_control(cancel_ticket, raw);

        let err = match outcome {
            CompletionSettlement::TerminalFailure { failure } => {
                assert!(matches!(failure.effect, UringBackendEffect::None));
                failure.error
            }
            _ => panic!("untracked cancel must produce a failed outcome"),
        };
        assert_eq!(*err.inner(), UringError::InvalidState);
        assert_eq!(diagnostics.snapshot().backend.cancel_untracked_cqe, 1);
    }

    #[test]
    fn raw_fd_cleanup_hint_is_exposed_only_for_fd_producing_ops() {
        assert!(
            <Open as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <Accept as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <AcceptMulti as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <ReadRaw as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <WriteRaw as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <Recv as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
    }

    #[test]
    fn stale_fd_completion_is_closed_and_final_hint_is_removed() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let hint = <Open as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("open must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(fd);
        unsafe { libc::close(fd_pair[1]) };
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 1);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn negative_fd_completion_does_not_close_or_retain_hint() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let hint = <Accept as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, token, -libc::ECANCELED, 0);

        assert!(sidecar.is_empty());
        assert_ne!(unsafe { libc::fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd_pair[0]);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn corrupt_completion_returns_selected_provided_buffer() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let mut sidecar = HashMap::default();
        let diagnostics = registry.shared.completion_diagnostics();
        let mut pending_cancel_cqes = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut provided_buffers = test_group(2);
        let before = provided_buffers.stats();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared.clone();
        let mut hooks = test_hooks_with_buffers(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut sidecar,
            &mut waker_armed,
            &notification_state,
            Some(&mut provided_buffers),
            &mut observer,
            &mut post,
        );
        let flags = 1 | (1 << 16);
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            7,
            flags,
        );

        let outcome = registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled");

        assert_eq!(outcome.anomaly, 1);
        assert_eq!(provided_buffers.stats().returned, before.returned + 1);
        assert_eq!(provided_buffers.stats().available, before.available);
    }

    #[test]
    fn stale_non_fd_completion_does_not_close_a_positive_result() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), None);

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn missing_fd_cleanup_hint_does_not_guess_close() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();

        accept_corrupt(&mut registry, &mut HashMap::default(), token, fd_pair[0], 0);

        assert_ne!(unsafe { libc::fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd_pair[0]);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 1);
    }

    #[test]
    fn multishot_corrupt_completions_close_each_fd_and_retain_hint_until_final() {
        const CQE_MORE: u32 = 2;

        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let first_pair = open_test_fds();
        let second_pair = open_test_fds();
        let hint = <AcceptMulti as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept multi must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let first = accept_corrupt(&mut registry, &mut sidecar, token, first_pair[0], CQE_MORE);
        assert_eq!(first.anomaly, 1);
        assert_eq!(sidecar.len(), 1);
        assert_closed(first_pair[0]);

        let final_outcome = accept_corrupt(&mut registry, &mut sidecar, token, second_pair[0], 0);
        assert_eq!(final_outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(second_pair[0]);
        unsafe {
            libc::close(first_pair[1]);
            libc::close(second_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 2);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 2);
    }

    #[test]
    fn generation_reuse_keeps_old_and_new_cleanup_hints_separate() {
        let mut registry = UringOpRegistry::new(1);
        let old_token = OpToken::from_registry_parts(0, Generation::new(1)).expect("old token");
        let new_token = OpToken::from_registry_parts(0, Generation::new(2)).expect("new token");
        let old_pair = open_test_fds();
        let new_pair = open_test_fds();
        let hint = <Accept as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(old_token), Some(hint));
        sidecar.insert(CompletionToken::user(new_token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, old_token, old_pair[0], 0);
        assert_closed(old_pair[0]);
        assert_eq!(sidecar.len(), 1);
        assert!(sidecar.contains_key(&CompletionToken::user(new_token)));

        accept_corrupt(&mut registry, &mut sidecar, new_token, new_pair[0], 0);
        assert_closed(new_pair[0]);
        assert!(sidecar.is_empty());
        unsafe {
            libc::close(old_pair[1]);
            libc::close(new_pair[1]);
        }
    }
}
