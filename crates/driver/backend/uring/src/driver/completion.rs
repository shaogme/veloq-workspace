use veloq_std::{format, num::NonZeroU8, time::Instant};

use diagweave::prelude::*;
use tracing::{debug, trace, warn};
use veloq_io_uring::cqueue;
use veloq_std::vec::Vec;

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::completion::effects::CompletionEffectBatch,
    driver::control::ExpiredBatch,
    driver::{
        context::{CompletionContext, check_control_plane_invariants},
        control::{PendingCancel, UringControlPlane},
        env::{CompletionControlView, CqeEnv},
        registration::provided_buf::ProvidedBufPort,
    },
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{
        CheckedSlotView, Close, CompletionCleanupHintFn, RecordPolicy, Slot, SlotView,
        UringOpRegistryExt, UringOperationDescriptor, UringRecordItem, UringSlotSpec,
    },
};

#[cfg(any(test, feature = "test-hooks"))]
use crate::driver::control::ControlPlaneEvent;

use crate::driver::drive::RoundBudget;
use crate::driver::lifecycle::{CancellationPhase, SubmissionPhase};
use crate::driver::submission::txn::slot_access_report;

pub(crate) mod effects;

pub(crate) struct CompletionEngine {
    cqe_buffer: Vec<(u64, i32, u32)>,
    effect_accumulator: Option<CompletionEffectBatch>,
    pending_effects: Option<CompletionEffectBatch>,
    pending_collector_exhausted: bool,
    limits: crate::config::UringDriveLimits,
}

impl CompletionEngine {
    pub(crate) fn with_capacity(
        cqe_capacity: usize,
        effect_capacity: usize,
        limits: crate::config::UringDriveLimits,
    ) -> Self {
        Self {
            cqe_buffer: Vec::with_capacity(cqe_capacity),
            effect_accumulator: Some(CompletionEffectBatch::with_capacity(effect_capacity)),
            pending_effects: None,
            pending_collector_exhausted: true,
            limits,
        }
    }

    pub(crate) fn has_pending_cqes(&self) -> bool {
        !self.cqe_buffer.is_empty()
    }

    pub(crate) const fn drive_limits(&self) -> crate::config::UringDriveLimits {
        self.limits
    }

    pub(crate) fn take_effects(&mut self) -> (CompletionEffectBatch, bool) {
        (
            self.pending_effects
                .take()
                .expect("completion effect batch must be available"),
            self.pending_collector_exhausted,
        )
    }

    pub(crate) fn recycle_effects(&mut self, mut effects: CompletionEffectBatch) {
        effects.clear();
        debug_assert!(self.effect_accumulator.is_none());
        self.effect_accumulator = Some(effects);
    }

    pub(crate) fn pending_effects_len(&self) -> usize {
        self.pending_effects
            .as_ref()
            .map_or(0, CompletionEffectBatch::len)
    }
}

use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelMode, CancelTicket, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionBackendIngressAction, CompletionCleanupGuard,
        CompletionContinuation, CompletionControl, CompletionEnvelope, CompletionFailure,
        CompletionFlowExt, CompletionFlowOutcome, CompletionIngress, CompletionSettlement,
        CompletionSource, CompletionToken, DriverCompletionDiagnostics, OpToken, PlatformOp,
        RawCompletion, SharedCompletionTable, SyntheticCompletionSource, UserCompletionEvent,
        run_completion_cleanup,
    },
    slot::{InFlightOrphaned, InFlightWaiting},
};

pub(crate) type CompletionProgress = CompletionFlowOutcome;
pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(2) {
        Some(val) => val,
        None => unreachable!(),
    });

enum UringBackendIngress {
    ResumeUdpRecvMulti {
        token: OpToken,
        logical_receiver_generation: u32,
    },
}

enum CompletionCleanupHintState {
    Missing,
    Known(Option<CompletionCleanupHintFn>),
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
    UdpRearm {
        token: OpToken,
        logical_receiver_generation: u32,
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

struct UringCompletionHooks<'control, 'provided> {
    diagnostics: &'control DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    control: CompletionControlView<'control>,
    provided_buffers: Option<ProvidedBufPort<'provided>>,
    synthetic: UringSyntheticCompletion,
}

impl<'control, 'provided> UringCompletionHooks<'control, 'provided> {
    fn new(
        diagnostics: &'control DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        control: CompletionControlView<'control>,
        provided_buffers: Option<ProvidedBufPort<'provided>>,
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
    fn with_cqe_env<R>(&mut self, f: impl FnOnce(&mut CqeEnv<'provided, 'control>) -> R) -> R {
        let provided = self.provided_buffers.take();
        let mut env = CqeEnv::new(provided, self.diagnostics.backend());
        let result = f(&mut env);
        self.provided_buffers = env.take_provided();
        result
    }

    #[inline]
    fn take_provided_buffers(&mut self) -> Option<ProvidedBufPort<'provided>> {
        self.provided_buffers.take()
    }

    fn completion_cleanup_hint_for(
        &mut self,
        token: CompletionToken,
        flags: u32,
    ) -> CompletionCleanupHintState {
        let entry = if cqueue::more(flags) {
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
        source: CompletionSource<'_, UringBackendIngress>,
    ) -> CompletionCleanupGuard {
        let raw = event.raw();
        self.with_cqe_env(|env| env.return_provided_buf(raw.flags));
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
                        target: request.target(),
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
                        target: request.target(),
                        phase: CancellationPhase::Failed,
                        cancel_ticket,
                    },
                }
            }
        }
    }
}

impl CompletionBackendHooks<UringSlotSpec> for UringCompletionHooks<'_, '_> {
    type BackendIngress = UringBackendIngress;
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

    fn complete_backend_ingress(
        &mut self,
        ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<UringSlotSpec, Self::BackendEffect> {
        match ingress {
            UringBackendIngress::ResumeUdpRecvMulti { token, .. } => {
                CompletionBackendIngressAction::RouteUser(UserCompletionEvent::from_parts(
                    COMP_BACKEND_URING,
                    *token,
                    0,
                    cqueue::Flags::MORE.bits(),
                ))
            }
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
            CompletionSource::Backend(UringBackendIngress::ResumeUdpRecvMulti {
                logical_receiver_generation,
                ..
            }) => match self.with_cqe_env(|env| {
                complete_udp_resume_waiting_slot(slot, event, *logical_receiver_generation, env)
            }) {
                Ok(outcome) => outcome,
                Err(error) => CompletionSettlement::Quarantined {
                    failure: CompletionFailure::quarantined(
                        error.report,
                        error.cleanup,
                        UringBackendEffect::None,
                    ),
                },
            },
            CompletionSource::Kernel | CompletionSource::User => {
                let raw = event.raw();
                if raw.res == -libc::ENOBUFS {
                    self.with_cqe_env(|env| env.note_exhausted());
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
                match self.with_cqe_env(|cqe_env| {
                    complete_kernel_waiting_slot(slot, event.token(), raw, cqe_env)
                }) {
                    Ok(outcome) => {
                        if matches!(source, CompletionSource::Kernel) && !cqueue::more(raw.flags) {
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
                        self.with_cqe_env(|env| env.return_provided_buf(raw.flags));
                        if error.fallback_cleanup
                            && let Some(CompletionCleanupHintState::Known(Some(hint))) = hint
                        {
                            let mut cleanup = hint(raw.res);
                            let _ = run_completion_cleanup(self.diagnostics, &mut cleanup);
                        }
                        if matches!(source, CompletionSource::Kernel) && !cqueue::more(raw.flags) {
                            let _ = self.control.remove_completion_cleanup_hint(
                                event.completion_token(),
                                raw.flags,
                            );
                        }
                        if cqueue::more(raw.flags) {
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
        self.with_cqe_env(|env| env.return_provided_buf(event.raw().flags));
        // 一个已放弃的 multishot 会继续投递完成，每一条都要跑 cleanup（accept 的话就是
        // 关掉那个内核已经建好的连接），但只有终态那条才能归还 slot。
        let continuation = if cqueue::more(event.raw().flags) {
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
            UringBackendEffect::UdpRearm {
                token,
                logical_receiver_generation,
            } => {
                self.control
                    .append_udp_rearm(token, logical_receiver_generation);
                Ok(())
            }
        }
    }
}

fn completion_observation(
    ingress: &CompletionIngress<UringBackendIngress>,
) -> Option<(OpToken, bool)> {
    let (token, flags) = match ingress {
        CompletionIngress::Kernel(envelope) => (envelope.raw.token.op_token()?, envelope.raw.flags),
        CompletionIngress::User(event) | CompletionIngress::Synthetic { event, .. } => {
            (event.token(), event.flags())
        }
        CompletionIngress::Backend(_) | CompletionIngress::Anomaly { .. } => return None,
    };
    Some((token, !cqueue::more(flags)))
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
    flow: CompletionProgress,
    timer_count: usize,
    cqe_count: usize,
    pending_completion: bool,
    cqe_budget_hit: bool,
    emergency_drain: bool,
    cqe_overflow: bool,
}

impl CompletionBatchProgress {
    pub(crate) const fn user_completed(&self) -> usize {
        self.flow.user_completed
    }

    pub(crate) const fn timer_count(&self) -> usize {
        self.timer_count
    }

    pub(crate) const fn cqe_count(&self) -> usize {
        self.cqe_count
    }

    pub(crate) const fn cqe_budget_hit(&self) -> bool {
        self.cqe_budget_hit
    }

    pub(crate) const fn emergency_drain(&self) -> bool {
        self.emergency_drain
    }

    pub(crate) const fn cqe_overflow(&self) -> bool {
        self.cqe_overflow
    }

    pub(crate) const fn pending_work(&self) -> bool {
        self.pending_completion
    }
}

struct CompletionPorts<'engine, 'context> {
    completion: &'engine mut CompletionEngine,
    ops: &'context mut crate::driver::operation::OperationLedger,
    completion_diagnostics: &'context DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    completion_table: &'context SharedCompletionTable<UringSlotSpec>,
    control: &'context mut UringControlPlane,
    ring: &'context mut veloq_io_uring::IoUring,
    provided_buffers: Option<ProvidedBufPort<'context>>,
}

impl CompletionEngine {
    fn ports<'engine, 'context>(
        &'engine mut self,
        context: &'context mut CompletionContext<'_, '_, '_, '_>,
    ) -> CompletionPorts<'engine, 'context> {
        let (operation, resources) = context.parts().split();
        let (ops, control, completion_table, completion_diagnostics) = operation.into_parts();
        let (ring, provided_buffers) = resources.into_parts();
        CompletionPorts {
            completion: self,
            ops,
            completion_diagnostics,
            completion_table,
            control,
            ring,
            provided_buffers,
        }
    }

    pub(crate) fn process_completion_batch(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        self.ports(context).process_completion_batch(budget)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.ports(context)
            .accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.ports(context).accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            UringSyntheticCompletion::None,
        )
    }
}

impl<'engine, 'context> CompletionPorts<'engine, 'context> {
    /// Advances the timer wheel by however many whole ticks elapsed since the last poll.
    fn advance_timer_clock(&mut self) -> UringResult<ExpiredBatch> {
        let now = Instant::now();
        let expired = self
            .control
            .timers_mut()
            .advance_timer_wheel(now)
            .map_err(|error| {
                UringError::InvalidState
                    .report(
                        "uring.completion.advance_timer",
                        "timer wheel could not advance",
                    )
                    .with_ctx("timer_error", format!("{error:?}"))
            })?;

        #[cfg(any(test, feature = "test-hooks"))]
        for entry in expired.newly_expired_iter() {
            self.control.record(ControlPlaneEvent::TimerExpire {
                task_id: Some(entry.id),
                token: entry.item,
            });
        }
        Ok(expired)
    }

    fn quarantine_expired_timer(&mut self, token: OpToken) {
        self.control.quarantine_timer(token);
        let _ = self.ops.mark_terminal(
            token,
            self.control.observer_mut(),
            "expired timer was quarantined",
        );
    }

    fn collect_cqes(&mut self, max_cqes: usize) -> CqeCollection {
        let mut collection = CqeCollection::default();
        let mut cq = self.ring.completion();
        cq.sync();
        let visible = cq.len();
        collection.overflow = cq.overflow();
        let normal_limit = visible
            .min(max_cqes)
            .min(self.completion.limits.max_cqe_batch);
        let high_water = cq.capacity().saturating_mul(3) / 4;
        let emergency =
            visible > normal_limit && (visible >= high_water.max(1) || collection.overflow != 0);
        let limit = if emergency {
            visible
                .min(self.completion.limits.emergency_drain_limit)
                .min(self.completion.cqe_buffer.capacity())
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
                self.control.waker().begin_processing();
            }
            self.completion
                .cqe_buffer
                .push((raw_token, cqe.result(), cqe.flags()));
            collection.count += 1;
        }
        collection.remaining = cq.len();
        collection.collector_exhausted = collection.remaining == 0;
        collection.cqe_budget_hit = visible > normal_limit;
        collection.emergency = emergency;
        collection
    }

    fn collect_udp_resume_tokens(&mut self, limit: usize) -> Vec<(OpToken, u32)> {
        let tokens: Vec<_> = self.ops.active_tokens().collect();
        tokens
            .into_iter()
            .filter_map(|token| {
                let view = self.ops.checked_slot_view(token).ok()?;
                let CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) = view else {
                    return None;
                };
                slot.with_access_mut(|access| {
                    let descriptor = access.operation().get_ref().descriptor();
                    if descriptor.record_policy != RecordPolicy::UdpMultishot {
                        return None;
                    }
                    unsafe { (descriptor.receive_generation)(access) }
                        .map(|generation| (token, generation))
                })
                .ok()
                .flatten()
            })
            .take(limit)
            .collect()
    }

    fn process_completion_batch(
        &mut self,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        let missing_cleanup_hints_before = self
            .completion_diagnostics
            .backend()
            .corrupt_cleanup_hint_missing_count();
        self.completion.cqe_buffer.clear();
        let collection = self.collect_cqes(budget.cqes());
        budget.consume_cqes(collection.count);

        let mut batch_effects = self
            .completion
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        batch_effects.clear();
        let mut progress = CompletionProgress::default();
        let mut first_error = None;

        for index in 0..self.completion.cqe_buffer.len() {
            let (raw_token, cqe_res, cqe_flags) = self.completion.cqe_buffer[index];
            let envelope = CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_URING,
                raw_token,
                cqe_res,
                cqe_flags,
            );
            if let Err(error) = self
                .control
                .settle_staged_completion(envelope.raw.token, !cqueue::more(cqe_flags))
            {
                self.control.quarantine_unpublished_staged();
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
            observe_completion_result(self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => remember_first_error(&mut first_error, report),
            }
        }

        let missing_cleanup_hints = self
            .completion_diagnostics
            .backend()
            .corrupt_cleanup_hint_missing_count()
            .saturating_sub(missing_cleanup_hints_before);
        if missing_cleanup_hints != 0 {
            self.control.quarantine_unpublished_staged();
            remember_first_error(
                &mut first_error,
                UringError::InvalidState
                    .report(
                        "uring.completion.cleanup_hint",
                        "kernel completion cleanup sidecar is missing",
                    )
                    .with_ctx("missing_hints", missing_cleanup_hints),
            );
        }

        let expired = self.advance_timer_clock()?;
        let timer_count = expired.len().min(budget.timers());
        budget.consume_timers(timer_count);
        if expired.len() > timer_count {
            self.completion_diagnostics.backend().inc_timer_budget_hit();
        }
        for entry in expired.iter().take(timer_count) {
            let token = entry.item;
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
            observe_completion_result(self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => {
                    self.quarantine_expired_timer(token);
                    remember_first_error(&mut first_error, report);
                }
            }
        }
        self.control
            .timers_mut()
            .recycle_expired(expired, timer_count);

        for (token, logical_receiver_generation) in self.collect_udp_resume_tokens(32) {
            for _ in 0..16 {
                let ingress = CompletionIngress::Backend(UringBackendIngress::ResumeUdpRecvMulti {
                    token,
                    logical_receiver_generation,
                });
                let (_observation, outcome) = self.accept_completion_transaction_into(
                    ingress,
                    UringSyntheticCompletion::None,
                    &mut batch_effects,
                );
                match outcome {
                    Ok(outcome) => {
                        let user_completed = outcome.user_completed;
                        progress.merge(outcome);
                        if user_completed == 0 {
                            break;
                        }
                    }
                    Err(report) => {
                        remember_first_error(&mut first_error, report);
                        break;
                    }
                }
            }
        }

        self.completion.pending_collector_exhausted = collection.collector_exhausted;
        self.completion.pending_effects = Some(batch_effects);
        self.completion.cqe_buffer.clear();

        let invariant_result = check_control_plane_invariants(self.ops, self.control);
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

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<UringBackendIngress>,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let mut effects = self
            .completion
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        effects.clear();
        let (_observation, flow_result) =
            self.accept_completion_transaction_into(ingress, synthetic, &mut effects);
        #[cfg(any(test, feature = "test-hooks"))]
        observe_completion_result(self.control, _observation, &flow_result);
        self.completion.pending_collector_exhausted = true;
        self.completion.pending_effects = Some(effects);
        flow_result
    }

    fn accept_completion_transaction_into(
        &mut self,
        ingress: CompletionIngress<UringBackendIngress>,
        synthetic: UringSyntheticCompletion,
        effects: &mut CompletionEffectBatch,
    ) -> (Option<(OpToken, bool)>, UringResult<CompletionFlowOutcome>) {
        let observation = completion_observation(&ingress);
        let control = self.control.with_completion_view();
        let mut hooks = UringCompletionHooks::new(
            self.completion_diagnostics,
            control,
            self.provided_buffers.take(),
            synthetic,
        );
        let flow_result = self.ops.accept_completion(
            self.completion_table,
            self.completion_diagnostics,
            &mut hooks,
            ingress,
        );
        self.provided_buffers = hooks.take_provided_buffers();
        drop(hooks);
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

fn complete_udp_resume_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    logical_receiver_generation: u32,
    cqe_env: &mut CqeEnv<'_, '_>,
) -> Result<CompletionSettlement<UringSlotSpec, UringBackendEffect>, KernelCompletionError> {
    let (record_item, operation_name) = match slot.with_access_mut(|access| {
        let descriptor = access.operation().get_ref().descriptor();
        if descriptor.record_policy != RecordPolicy::UdpMultishot {
            return Err(record_item_policy_report(descriptor.name, event.token()));
        }
        let record_item = unsafe {
            (descriptor.resume_item)(access, event.token(), logical_receiver_generation, cqe_env)
        };
        Ok((record_item, descriptor.name))
    }) {
        Ok(Ok((record_item, operation_name))) => (record_item, operation_name),
        Ok(Err(report)) => {
            return Err(KernelCompletionError {
                report,
                fallback_cleanup: false,
                cleanup: CompletionCleanupGuard::default(),
            });
        }
        Err(err) => {
            return Err(KernelCompletionError {
                report: UringError::InvalidState.report(
                    "uring.complete_udp_resume_waiting_slot",
                    format!("slot corruption detected during UDP resume: {:?}", err),
                ),
                fallback_cleanup: false,
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
                cleanup: CompletionCleanupGuard::default(),
            });
        }
    };
    match record_item {
        UringRecordItem::Ignored => Ok(CompletionSettlement::Ignore {
            effect: UringBackendEffect::None,
        }),
        UringRecordItem::Retained {
            logical_receiver_generation,
        } => {
            let effect =
                logical_receiver_generation.map_or(UringBackendEffect::None, |generation| {
                    UringBackendEffect::UdpRearm {
                        token: event.token(),
                        logical_receiver_generation: generation,
                    }
                });
            Ok(CompletionSettlement::Retained {
                cleanup: CompletionCleanupGuard::default(),
                effect,
            })
        }
        UringRecordItem::NewWithRearm {
            item,
            logical_receiver_generation,
        } => Ok(CompletionSettlement::User {
            event,
            payload: item,
            detail: None,
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: UringBackendEffect::UdpRearm {
                token: event.token(),
                logical_receiver_generation,
            },
        }),
        UringRecordItem::New(item) => Ok(CompletionSettlement::User {
            event,
            payload: item,
            detail: None,
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: UringBackendEffect::None,
        }),
        UringRecordItem::UseSubmitPayload => Err(KernelCompletionError {
            report: record_item_policy_report(operation_name, event.token()),
            fallback_cleanup: false,
            cleanup: CompletionCleanupGuard::default(),
        }),
    }
}

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
    cqe_env: &mut CqeEnv<'_, '_>,
) -> Result<CompletionSettlement<UringSlotSpec, UringBackendEffect>, KernelCompletionError> {
    // `IORING_CQE_F_MORE`：内核声明这个操作还会继续投递完成。flags 的解读到此为止，
    // core 只见 `CompletionContinuation`。
    let continuation = if cqueue::more(raw.flags) {
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

    if let UringRecordItem::Retained {
        logical_receiver_generation,
    } = record_item
    {
        let effect = logical_receiver_generation.map_or(UringBackendEffect::None, |generation| {
            UringBackendEffect::UdpRearm {
                token,
                logical_receiver_generation: generation,
            }
        });
        return Ok(CompletionSettlement::Retained { cleanup, effect });
    }

    if let UringRecordItem::NewWithRearm {
        item,
        logical_receiver_generation,
    } = record_item
    {
        return Ok(CompletionSettlement::User {
            event,
            payload: item,
            detail: res_error.take().map(Err),
            cleanup,
            continuation: CompletionContinuation::More,
            effect: UringBackendEffect::UdpRearm {
                token,
                logical_receiver_generation,
            },
        });
    }

    if continuation.is_more() {
        // slot 原地不动：op 与提交 payload 还要给内核后续的完成用，cell 也必须停在
        // `InFlightWaiting` 才能继续路由。
        let item = match record_item {
            UringRecordItem::New(item) => item,
            UringRecordItem::NewWithRearm { item, .. } => item,
            UringRecordItem::UseSubmitPayload
            | UringRecordItem::Ignored
            | UringRecordItem::Retained { .. } => {
                return Err(KernelCompletionError {
                    report: record_item_policy_report(operation_name, token),
                    fallback_cleanup: false,
                    cleanup,
                });
            }
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

    slot.platform_mut()
        .set_submission_phase(SubmissionPhase::Terminal);
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
        UringRecordItem::Ignored
        | UringRecordItem::NewWithRearm { .. }
        | UringRecordItem::Retained { .. } => {
            drop(submit_payload);
            drop(detail);
            return Err(KernelCompletionError {
                report: record_item_policy_report(operation_name, token),
                fallback_cleanup: false,
                cleanup,
            });
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
    slot.platform_mut().clear_timer();
    slot.platform_mut()
        .set_submission_phase(SubmissionPhase::Terminal);
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
    slot.platform_mut()
        .set_submission_phase(SubmissionPhase::Terminal);
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
    slot.platform_mut()
        .set_submission_phase(SubmissionPhase::Terminal);
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
    slot.platform_mut()
        .set_submission_phase(SubmissionPhase::Terminal);
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

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::driver::control::{
        ControlPlaneObserver, UringControlEffectKind, UringPostCompletionEffects,
        waker::{WAKER_NOTIFIED, WAKER_PROCESSING},
    };
    use crate::driver::{
        drive::{WaitBudget, wait_budget},
        registration::provided_buf::{ProvidedBufGroup, ProvidedBufPort, test_group},
        submission::WaitBudgetSource,
    };
    use crate::op::{
        Accept, AcceptMulti, Open, ReadRaw, Recv, UringOpRegistry, UringOperationDescriptor,
        WriteRaw,
    };
    use crate::test_alloc::measure;
    use veloq_driver_core::driver::{CompletionToken, SharedCompletionTable};
    use veloq_driver_core::slot::Generation;
    use veloq_std::{
        collections::HashMap,
        sync::atomic::{AtomicU8, Ordering},
        time::Duration,
    };

    #[test]
    fn completion_effect_scratch_recycles_without_allocating() {
        let mut engine =
            CompletionEngine::with_capacity(8, 8, crate::config::UringDriveLimits::for_entries(8));
        let _ = engine.effect_accumulator.take();
        engine.pending_effects = Some(CompletionEffectBatch::with_capacity(8));

        let (_, counts) = measure(|| {
            let (effects, _) = engine.take_effects();
            engine.recycle_effects(effects);
        });

        assert_eq!(counts.dynamic_allocations(), 0);
        assert_eq!(counts.deallocations, 0);
    }

    fn test_hooks<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        _waker_armed: &'a mut bool,
        _notification_state: &'a AtomicU8,
        observer: &'a mut ControlPlaneObserver,
        post: &'a mut UringPostCompletionEffects,
    ) -> UringCompletionHooks<'a, 'a> {
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
    ) -> UringCompletionHooks<'a, 'a> {
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
            provided_buffers.map(ProvidedBufPort::new),
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
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared_table();
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
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRebuild { .. }))
        );
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRearm { .. }))
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
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRearm { .. }))
        );
        assert!(
            !post
                .effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRebuild { .. }))
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
            WaitBudget::new(Duration::from_secs(1), WaitBudgetSource::Probe)
        );
        assert_eq!(
            wait_budget(
                Some(Duration::from_millis(3)),
                Some(Duration::from_millis(7)),
                Duration::from_secs(1),
            ),
            WaitBudget::new(Duration::from_millis(3), WaitBudgetSource::External,)
        );
        assert_eq!(
            wait_budget(None, Some(Duration::from_millis(5)), Duration::from_secs(1),),
            WaitBudget::new(Duration::from_millis(5), WaitBudgetSource::Timer)
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
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared_table();
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
        assert_eq!(provided_buffers.stats().returned(), before.returned() + 1);
        assert_eq!(provided_buffers.stats().available(), before.available());
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
