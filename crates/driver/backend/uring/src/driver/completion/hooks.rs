use veloq_std::format;

use diagweave::prelude::*;
use tracing::{debug, trace, warn};
use veloq_io_uring::cqueue;

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::{
        completion::CompletionSlotRouter,
        control::PendingCancel,
        env::{CompletionControlView, CqeEnv},
        lifecycle::CancellationPhase,
        registration::ProvidedBufPort,
    },
    error::{UringError, UringResult},
    op::{CompletionCleanupHintFn, Slot, UringSlotSpec},
};
use veloq_std::num::NonZeroU8;

use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelMode, CancelTicket, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionBackendIngressAction, CompletionCleanupGuard,
        CompletionContinuation, CompletionControl, CompletionFailure, CompletionFlowOutcome,
        CompletionSettlement, CompletionSource, CompletionToken, DriverCompletionDiagnostics,
        OpToken, RawCompletion, SyntheticCompletionSource, UserCompletionEvent,
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

pub(crate) enum UringBackendIngress {
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

pub(crate) enum UringBackendEffect {
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
pub(crate) enum WakerRecovery {
    Rearm,
    RebuildAndRearm,
}

impl Default for UringBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

pub(crate) struct UringCompletionHooks<'control, 'provided> {
    diagnostics: &'control DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    control: CompletionControlView<'control>,
    provided_buffers: Option<ProvidedBufPort<'provided>>,
    synthetic: UringSyntheticCompletion,
}

impl<'control, 'provided> UringCompletionHooks<'control, 'provided> {
    pub(super) fn new(
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
    pub(super) fn take_provided_buffers(&mut self) -> Option<ProvidedBufPort<'provided>> {
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

    pub(super) fn handle_waker_control(
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

    pub(super) fn handle_cancel_control(
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
                CompletionSlotRouter::complete_timer_waiting_slot(slot, event)
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => {
                CompletionSlotRouter::complete_cancel_waiting_slot(
                    slot,
                    event,
                    self.synthetic.cancel_mode(),
                )
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure) => {
                CompletionSlotRouter::complete_submission_failure_slot(
                    slot,
                    event,
                    self.synthetic.take_submission_failure(),
                )
            }
            CompletionSource::Backend(UringBackendIngress::ResumeUdpRecvMulti {
                logical_receiver_generation,
                ..
            }) => match self.with_cqe_env(|env| {
                CompletionSlotRouter::complete_udp_resume_waiting_slot(
                    slot,
                    event,
                    *logical_receiver_generation,
                    env,
                )
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
                    CompletionSlotRouter::complete_kernel_waiting_slot(
                        slot,
                        event.token(),
                        raw,
                        cqe_env,
                    )
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
                        // 本 hook 可能在取出 selected buffer 前失败；完成事务负责退还该 buffer，操作 cleanup guard 独占操作清理。
                        // The hook can fail before taking the selected buffer; the completion transaction returns it while the operation cleanup guard owns operation cleanup.
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

    /// 一条完成落到了不存在或已陈旧的 slot 上；可能仍携带必须归还的 selected buffer。
    /// A completion can target a missing or stale slot while carrying a selected buffer that must be returned.
    ///
    /// 内核已取走这个 BID，必须归还以避免永久损失；core 默认只记录异常，因此这里覆盖处理。
    /// The kernel already removed the BID, so this override returns it; the core default only records the anomaly.
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
        // This completion is discarded after the kernel consumed its selected buffer; returning it prevents each cancellation from leaking a bid.
        self.with_cqe_env(|env| env.return_provided_buf(event.raw().flags));
        // 一个已放弃的 multishot 会继续投递完成，每一条都要跑 cleanup（accept 的话就是
        // 关掉那个内核已经建好的连接），但只有终态那条才能归还 slot。
        // An orphaned multishot still needs cleanup for every completion (including closing accepted sockets); only its final completion releases the slot.
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
            CompletionSlotRouter::cleanup_orphaned_streaming_slot(slot, res)
        } else {
            CompletionSlotRouter::cleanup_orphaned_slot(slot, res)
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
