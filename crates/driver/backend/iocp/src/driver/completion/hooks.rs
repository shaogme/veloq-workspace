use veloq_std::sync::Arc;

use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::{
    driver::{
        CompletionBackendHooks, CompletionContinuation, CompletionControl, CompletionFailure,
        CompletionSettlement, CompletionSource, CompletionToken, OpToken,
        SyntheticCompletionSource, UserCompletionEvent,
    },
    op::OpKind,
    slot::{InFlightOrphaned, InFlightWaiting},
};

use crate::{
    config::RegisteredSlot,
    driver::{IocpDriverCompletionDiagnostics, polling::CompletionPump},
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::{IocpSlotSpec, Slot, SubmissionResult, SubmitContext},
    rio::{RioState, SocketInflightToken},
    win32::{IoCompletionPort, Overlapped},
};

use super::{
    protocol::{IocpBackendEffect, IocpPostCompletionEffects, IocpSyntheticCompletion},
    settlement::SettlementService,
};

pub(super) struct IocpCompletionHooks<'a> {
    ext: &'a Extensions,
    diagnostics: &'a IocpDriverCompletionDiagnostics,
    rio: &'a mut RioState,
    completion: &'a CompletionPump,
    port: Arc<IoCompletionPort>,
    registered_slots: &'a mut [RegisteredSlot],
    registrar: &'a dyn BufferRegistrar,
    shutting_down: bool,
    synthetic: IocpSyntheticCompletion,
    post: IocpPostCompletionEffects,
}

pub(super) struct IocpCompletionHooksArgs<'a> {
    pub(super) ext: &'a Extensions,
    pub(super) diagnostics: &'a IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut RioState,
    pub(super) completion: &'a CompletionPump,
    pub(super) port: Arc<IoCompletionPort>,
    pub(super) registered_slots: &'a mut [RegisteredSlot],
    pub(super) registrar: &'a dyn BufferRegistrar,
    pub(super) shutting_down: bool,
    pub(super) synthetic: IocpSyntheticCompletion,
}

impl<'a> IocpCompletionHooks<'a> {
    pub(super) fn new(args: IocpCompletionHooksArgs<'a>) -> Self {
        Self {
            ext: args.ext,
            diagnostics: args.diagnostics,
            rio: args.rio,
            completion: args.completion,
            port: args.port,
            registered_slots: args.registered_slots,
            registrar: args.registrar,
            shutting_down: args.shutting_down,
            synthetic: args.synthetic,
            post: IocpPostCompletionEffects::default(),
        }
    }

    pub(super) fn into_post_effects(self) -> IocpPostCompletionEffects {
        self.post
    }

    fn rearm_accept_multi(
        &mut self,
        slot: &mut Slot<'_, InFlightWaiting>,
        token: OpToken,
    ) -> IocpResult<()> {
        // 提交替代请求期间保留已完成请求的 token，避免同步失败提前结算旧请求。
        // Keep the completed request's token alive during replacement submission to prevent an early settlement on synchronous failure.
        let previous =
            SettlementService::take_socket_inflight_from_slot(slot).ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .with_ctx("operation", "AcceptMulti")
                    .attach_note("completed AcceptMulti request has no socket inflight token")
            })?;

        let overlapped = slot.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);
        let result = slot.with_access_mut(|access| -> IocpResult<()> {
            let op = access.operation_mut().get_mut();
            if op.kind() != OpKind::AcceptMulti {
                return Err(IocpError::InvalidState
                    .to_report()
                    .with_ctx("operation", op.kind() as u16)
                    .attach_note("AcceptMulti rearm reached a non-AcceptMulti operation"));
            }
            let Some(vtable) = op.multishot_vtable() else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .attach_note("AcceptMulti operation has no multishot vtable"));
            };
            let mut context = SubmitContext {
                port: self.port.clone(),
                overlapped,
                op_token: token,
                completion_token: CompletionToken::user(token),
                ext: self.ext,
                registered_slots: self.registered_slots,
                registrar: self.registrar,
                rio: self.rio,
            };
            let submitted = (vtable.rearm)(op, &mut context)?;
            match submitted {
                SubmissionResult::Pending => Ok(()),
                _ => Err(IocpError::InvalidState
                    .to_report()
                    .with_ctx("operation", "AcceptMulti")
                    .attach_note("AcceptEx replacement returned a non-pending state")),
            }
        });
        let replacement = match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(IocpError::InvalidState
                .to_report()
                .with_ctx("operation", "AcceptMulti")
                .attach_note("failed to access AcceptMulti slot during replacement")),
        };

        let previous_identity = previous.identity();
        let release = self.rio.release_socket_inflight_token(previous);
        if release.is_ok() {
            self.post.request_socket_cleanup();
        }
        match (replacement, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error.trans()),
            (Err(replacement), Err(release)) => {
                let release: Report<IocpError> = release.trans();
                let restored = slot
                    .with_access_mut(|access| {
                        let header = &mut access.operation_mut().get_mut().header;
                        if header.socket_inflight.is_none() {
                            header.socket_inflight = Some(SocketInflightToken::new(
                                previous_identity.socket_key(),
                                previous_identity.request_id(),
                            ));
                            true
                        } else {
                            false
                        }
                    })
                    .unwrap_or(false);
                let error = replacement
                    .with_diag_src_err(release)
                    .attach_note("failed to release completed AcceptMulti request token");
                if restored {
                    Err(error)
                } else {
                    Err(error.attach_note(
                        "failed to restore completed AcceptMulti request token after replacement failure",
                    ))
                }
            }
        }
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for IocpCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = IocpBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        match control {
            CompletionControl::Waker { raw, .. } => {
                let rearmed = match self.completion.clear_notification() {
                    Ok(rearmed) => rearmed,
                    Err(error) => {
                        return CompletionSettlement::TerminalFailure {
                            failure: CompletionFailure::control(
                                error.attach_note("failed to clear IOCP waker notification"),
                                IocpBackendEffect::None,
                            ),
                        };
                    }
                };
                if raw.res >= 0 {
                    self.diagnostics.backend().inc_waker_ok();
                    self.diagnostics.backend().inc_wait_waker_return();
                    if rearmed {
                        self.diagnostics.backend().inc_waker_rearm();
                    }
                } else {
                    self.diagnostics.backend().inc_waker_error();
                    return CompletionSettlement::TerminalFailure {
                        failure: CompletionFailure::control(
                            IocpError::Internal
                                .to_report()
                                .push_ctx("scope", "iocp.driver.completion.waker")
                                .set_error_code(-raw.res)
                                .attach_note("IOCP waker completion reported an error"),
                            IocpBackendEffect::None,
                        ),
                    };
                }
                CompletionSettlement::ControlHandled {
                    effect: IocpBackendEffect::None,
                }
            }
            CompletionControl::Cancel { .. } => CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::control(
                    IocpError::InvalidState.report(
                        "iocp.completion.handle_control",
                        "async cancel completion had no pending request (programming error)",
                    ),
                    IocpBackendEffect::None,
                ),
            },
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        mut slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => {
                SettlementService::complete_timer_waiting_slot(slot, event)
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => {
                SettlementService::complete_cancel_waiting_slot(
                    slot,
                    event,
                    self.synthetic.cancel_mode(),
                )
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure) => {
                SettlementService::complete_submission_failure_slot(
                    slot,
                    event,
                    self.synthetic.take_submission_failure(),
                )
            }
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Backend(_) => {
                let is_accept_multi = slot
                    .with_access_mut(|access| {
                        access.operation().get_ref().kind() == OpKind::AcceptMulti
                    })
                    .unwrap_or(false);
                if is_accept_multi {
                    let validation = slot
                        .with_access_mut(|access| {
                            access
                                .operation()
                                .get_ref()
                                .validate_accept_multi_completion(event.token())
                        })
                        .map_err(|_| {
                            IocpError::InvalidState
                                .to_report()
                                .attach_note("failed to access AcceptMulti completion state")
                        })
                        .and_then(|result| result);
                    if let Err(error) = validation {
                        let socket_inflight =
                            SettlementService::take_socket_inflight_from_slot(&mut slot);
                        return SettlementService::complete_iocp_failure_slot(
                            slot,
                            error,
                            socket_inflight
                                .map(IocpBackendEffect::SocketInflight)
                                .unwrap_or_default(),
                        );
                    }
                }
                match SettlementService::calculate_io_result_from_slot(
                    self.ext,
                    &mut slot,
                    event.res(),
                ) {
                    Ok(io_result) => {
                        if is_accept_multi && io_result.is_ok() {
                            let cancel_requested = slot.platform().iocp_user_cancel_requested;
                            if self.shutting_down {
                                let socket_inflight =
                                    SettlementService::take_socket_inflight_from_slot(&mut slot);
                                return SettlementService::complete_iocp_abandoned_waiting_slot(
                                    slot,
                                    io_result,
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                );
                            }
                            if cancel_requested {
                                let socket_inflight =
                                    SettlementService::take_socket_inflight_from_slot(&mut slot);
                                return SettlementService::complete_iocp_cancelled_accept_multi_slot(
                                    slot,
                                    io_result,
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                );
                            }
                            match self.rearm_accept_multi(&mut slot, event.token()) {
                                Ok(()) => SettlementService::complete_iocp_waiting_slot(
                                    slot, event, io_result, None, true,
                                ),
                                Err(error) => {
                                    let socket_inflight =
                                        SettlementService::take_socket_inflight_from_slot(
                                            &mut slot,
                                        );
                                    SettlementService::complete_iocp_replacement_failure_slot(
                                        slot,
                                        event,
                                        io_result,
                                        error,
                                        socket_inflight
                                            .map(IocpBackendEffect::SocketInflight)
                                            .unwrap_or_default(),
                                    )
                                }
                            }
                        } else {
                            let socket_inflight =
                                SettlementService::take_socket_inflight_from_slot(&mut slot);
                            if is_accept_multi {
                                SettlementService::complete_iocp_failure_slot(
                                    slot,
                                    io_result
                                        .err()
                                        .unwrap_or_else(|| IocpError::InvalidState.to_report()),
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                )
                            } else {
                                SettlementService::complete_iocp_waiting_slot(
                                    slot,
                                    event,
                                    io_result,
                                    socket_inflight,
                                    false,
                                )
                            }
                        }
                    }
                    Err(error) => {
                        let socket_inflight =
                            SettlementService::take_socket_inflight_from_slot(&mut slot);
                        SettlementService::complete_iocp_failure_slot(
                            slot,
                            error,
                            socket_inflight
                                .map(IocpBackendEffect::SocketInflight)
                                .unwrap_or_default(),
                        )
                    }
                }
            }
        }
    }

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let (cleanup, socket_inflight) =
            SettlementService::complete_iocp_orphaned_slot(slot, event.res());
        CompletionSettlement::Cleanup {
            cleanup,
            continuation: CompletionContinuation::Final,
            effect: socket_inflight
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default(),
        }
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        effect.finish(self.rio, &mut self.post)
    }
}
