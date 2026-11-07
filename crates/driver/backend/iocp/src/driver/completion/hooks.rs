mod raw_udp;

use veloq_buf::FixedBuf;
use veloq_std::{io::Error as IoError, num::NonZeroUsize, slice::from_raw_parts, sync::Arc};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

use diagweave::prelude::*;
use veloq_driver_core::{
    AnomalyAttach, CompletionBackendHooks, CompletionBackendIngressAction, CompletionCleanupGuard,
    CompletionContinuation, CompletionControl, CompletionFailure, CompletionSettlement,
    CompletionSource, CompletionToken, InFlightOrphaned, InFlightWaiting, OpKind, OpToken,
    ProvidedBuf, SyntheticCompletionSource, UserCompletionEvent,
};

use crate::{
    config::RegisteredSlot,
    driver::OverlappedRegistry,
    driver::{IocpDriverCompletionDiagnostics, polling::CompletionPump},
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::{IocpOpPayload, IocpSlotSpec, IocpUserPayload, Slot, SubmissionResult, SubmitContext},
    rio::{RioState, SocketInflightToken},
    win32::{IoCompletionPort, Overlapped},
};

use super::{
    protocol::{
        COMP_BACKEND_IOCP, IocpBackendEffect, IocpBackendIngress, IocpPostCompletionEffects,
        IocpSyntheticCompletion,
    },
    settlement::SettlementService,
};

use raw_udp::{RawUdpCompletion, RawUdpCompletionServices};

struct OrphanedRawUdpChild {
    socket_inflight: SocketInflightToken,
    has_active_children: bool,
}

pub(super) struct IocpCompletionHooks<'a> {
    ext: &'a Extensions,
    diagnostics: &'a IocpDriverCompletionDiagnostics,
    rio: &'a mut RioState,
    completion: &'a CompletionPump,
    port: Arc<IoCompletionPort>,
    registered_slots: &'a mut [RegisteredSlot],
    overlapped_registry: &'a mut OverlappedRegistry,
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
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
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
            overlapped_registry: args.overlapped_registry,
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
                overlapped_registry: None,
                buffer_pool: None,
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

    fn rearm_raw_recv_multi(
        &mut self,
        slot: &mut Slot<'_, InFlightWaiting>,
        token: OpToken,
    ) -> IocpResult<SubmissionResult> {
        let overlapped = slot.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);
        slot.with_access_mut(|access| {
            let op = access.operation_mut().get_mut();
            let Some(vtable) = op.multishot_vtable() else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .with_ctx("operation", op.kind() as u16)
                    .attach_note("RecvMulti operation has no multishot vtable"));
            };
            let mut context = SubmitContext {
                port: self.port.clone(),
                overlapped,
                op_token: token,
                completion_token: CompletionToken::user(token),
                ext: self.ext,
                registered_slots: self.registered_slots,
                overlapped_registry: None,
                buffer_pool: None,
                rio: self.rio,
            };
            (vtable.rearm)(op, &mut context)
        })
        .map_err(|_| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("operation", "RecvMulti")
                .attach_note("failed to access RecvMulti slot during raw receive replacement")
        })?
    }

    fn complete_raw_recv_multi(
        &mut self,
        event: UserCompletionEvent,
        mut slot: Slot<'_, InFlightWaiting>,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let io_result = match SettlementService::calculate_io_result_from_slot(
            self.ext,
            &mut slot,
            event.res(),
        ) {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                    .map(IocpBackendEffect::SocketInflight)
                    .unwrap_or_default();
                return SettlementService::complete_iocp_failure_slot(slot, error, effect);
            }
            Err(error) => {
                let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                    .map(IocpBackendEffect::SocketInflight)
                    .unwrap_or_default();
                return SettlementService::complete_iocp_failure_slot(slot, error, effect);
            }
        };

        if io_result == 0 {
            let output = match slot.with_access_mut(|access| -> IocpResult<FixedBuf> {
                let op = access.operation_mut().get_mut();
                let IocpOpPayload::RecvMulti(payload) = &mut op.payload else {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .attach_note("raw RecvMulti EOF has the wrong payload kind"));
                };
                let mut buffer = payload.raw_buffer.take().ok_or_else(|| {
                    IocpError::InvalidState
                        .to_report()
                        .attach_note("raw RecvMulti EOF has no backend buffer")
                })?;
                buffer.set_len(0);
                Ok(buffer)
            }) {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => {
                    let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                        .map(IocpBackendEffect::SocketInflight)
                        .unwrap_or_default();
                    return SettlementService::complete_iocp_failure_slot(slot, error, effect);
                }
                Err(_) => {
                    let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                        .map(IocpBackendEffect::SocketInflight)
                        .unwrap_or_default();
                    return SettlementService::complete_iocp_failure_slot(
                        slot,
                        IocpError::InvalidState
                            .to_report()
                            .attach_note("failed to read raw RecvMulti EOF buffer"),
                        effect,
                    );
                }
            };
            let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default();
            if self.shutting_down {
                return SettlementService::complete_iocp_abandoned_waiting_slot(
                    slot,
                    Ok(0),
                    effect,
                );
            }
            if slot.platform().iocp_user_cancel_requested {
                return SettlementService::complete_iocp_failure_slot(
                    slot,
                    IocpError::CompletionWait
                        .io_report(
                            "iocp.recv_multi.cancel",
                            IoError::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
                        )
                        .attach_note("raw RecvMulti was cancelled"),
                    effect,
                );
            }
            return SettlementService::complete_iocp_recv_provided_slot(
                slot,
                event,
                Ok(0),
                IocpUserPayload::ProvidedBuf(ProvidedBuf { buf: Some(output) }),
                effect,
            );
        }

        let output = match slot.with_access_mut(|access| -> IocpResult<FixedBuf> {
            let op = access.operation().get_ref();
            let IocpOpPayload::RecvMulti(payload) = &op.payload else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .attach_note("raw RecvMulti completion has the wrong payload kind"));
            };
            let buffer = payload.raw_buffer.as_ref().ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("raw RecvMulti completion has no backend buffer")
            })?;
            if io_result > buffer.capacity() {
                return Err(IocpError::CompletionWait
                    .to_report()
                    .with_ctx("received", io_result)
                    .with_ctx("buffer_capacity", buffer.capacity())
                    .attach_note("raw RecvMulti completion exceeds its buffer capacity"));
            }
            let capacity = NonZeroUsize::new(io_result).ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("non-empty raw RecvMulti output has zero capacity")
            })?;
            let mut output = FixedBuf::alloc_heap(capacity, 0).map_err(|_| {
                IocpError::Submission
                    .to_report()
                    .with_ctx("operation", "RecvMulti")
                    .with_ctx("buffer_capacity", io_result)
                    .attach_note("failed to allocate RecvMulti output buffer")
            })?;
            let source = unsafe { from_raw_parts(buffer.as_ptr(), io_result) };
            output.spare_capacity_mut()[..io_result].copy_from_slice(source);
            output.set_len(io_result);
            Ok(output)
        }) {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                    .map(IocpBackendEffect::SocketInflight)
                    .unwrap_or_default();
                return SettlementService::complete_iocp_failure_slot(slot, error, effect);
            }
            Err(_) => {
                let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                    .map(IocpBackendEffect::SocketInflight)
                    .unwrap_or_default();
                return SettlementService::complete_iocp_failure_slot(
                    slot,
                    IocpError::InvalidState
                        .to_report()
                        .attach_note("failed to read raw RecvMulti completion buffer"),
                    effect,
                );
            }
        };

        if self.shutting_down {
            let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default();
            return SettlementService::complete_iocp_abandoned_waiting_slot(
                slot,
                Ok(io_result),
                effect,
            );
        }
        if slot.platform().iocp_user_cancel_requested {
            let effect = SettlementService::take_socket_inflight_from_slot(&mut slot)
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default();
            return SettlementService::complete_iocp_failure_slot(
                slot,
                IocpError::CompletionWait
                    .io_report(
                        "iocp.recv_multi.cancel",
                        IoError::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
                    )
                    .attach_note("raw RecvMulti was cancelled"),
                effect,
            );
        }

        let previous = match SettlementService::take_socket_inflight_from_slot(&mut slot) {
            Some(previous) => previous,
            None => {
                return SettlementService::complete_iocp_failure_slot(
                    slot,
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("operation", "RecvMulti")
                        .attach_note(
                            "completed raw RecvMulti request has no socket inflight token",
                        ),
                    IocpBackendEffect::None,
                );
            }
        };
        let effect = IocpBackendEffect::SocketInflight(previous);
        let replacement = self.rearm_raw_recv_multi(&mut slot, event.token());
        let record = IocpUserPayload::ProvidedBuf(ProvidedBuf { buf: Some(output) });
        match replacement {
            Ok(SubmissionResult::Pending) => CompletionSettlement::User {
                event: UserCompletionEvent::from_parts(
                    COMP_BACKEND_IOCP,
                    event.token(),
                    io_result.min(i32::MAX as usize) as i32,
                    0,
                ),
                payload: record,
                detail: Some(Ok(io_result)),
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::More,
                effect,
            },
            Ok(_) => SettlementService::complete_iocp_multishot_replacement_failure_slot(
                slot,
                event,
                Ok(io_result),
                record,
                IocpError::InvalidState
                    .to_report()
                    .attach_note("raw RecvMulti replacement returned a non-pending state"),
                effect,
            ),
            Err(error) => SettlementService::complete_iocp_multishot_replacement_failure_slot(
                slot,
                event,
                Ok(io_result),
                record,
                error,
                effect,
            ),
        }
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for IocpCompletionHooks<'_> {
    type BackendIngress = IocpBackendIngress;
    type BackendEffect = IocpBackendEffect;

    fn complete_backend_ingress(
        &mut self,
        ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<IocpSlotSpec, Self::BackendEffect> {
        match ingress {
            IocpBackendIngress::UdpReceive { event, .. } => {
                CompletionBackendIngressAction::RouteUser(*event)
            }
        }
    }

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
                if raw.res() >= 0 {
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
                                .set_error_code(-raw.res())
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
                let is_raw_udp_recv_multi = slot
                    .with_access_mut(|access| {
                        let op = access.operation().get_ref();
                        op.kind() == OpKind::UdpRecvMulti && !op.uses_rio_backend()
                    })
                    .unwrap_or(false);
                if is_raw_udp_recv_multi {
                    let request = match source {
                        CompletionSource::Backend(IocpBackendIngress::UdpReceive {
                            request,
                            ..
                        }) => Some(*request),
                        _ => None,
                    };
                    return RawUdpCompletion::complete(
                        event,
                        request,
                        slot,
                        RawUdpCompletionServices {
                            ext: self.ext,
                            rio: self.rio,
                            port: self.port.clone(),
                            registered_slots: self.registered_slots,
                            overlapped_registry: self.overlapped_registry,
                            shutting_down: self.shutting_down,
                        },
                    );
                }
                let is_raw_recv_multi = slot
                    .with_access_mut(|access| {
                        let op = access.operation().get_ref();
                        op.kind() == OpKind::RecvMulti && !op.uses_rio_backend()
                    })
                    .unwrap_or(false);
                if is_raw_recv_multi {
                    return self.complete_raw_recv_multi(event, slot);
                }
                let is_accept_multi = slot
                    .with_access_mut(|access| {
                        access.operation().get_ref().kind() == OpKind::AcceptMulti
                    })
                    .unwrap_or(false);
                let is_recv_provided = slot
                    .with_access_mut(|access| {
                        access.operation().get_ref().kind() == OpKind::RecvProvided
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
                        if is_recv_provided {
                            let record = slot
                                .with_access_mut(|access| {
                                    access
                                        .operation_mut()
                                        .get_mut()
                                        .take_recv_provided_record(io_result.is_ok())
                                })
                                .map_err(|_| {
                                    IocpError::InvalidState.to_report().attach_note(
                                        "failed to access RecvProvided completion payload",
                                    )
                                })
                                .and_then(|result| result);
                            let effect =
                                SettlementService::take_socket_inflight_from_slot(&mut slot)
                                    .map(IocpBackendEffect::SocketInflight)
                                    .unwrap_or_default();
                            return match record {
                                Ok(record) => SettlementService::complete_iocp_recv_provided_slot(
                                    slot, event, io_result, record, effect,
                                ),
                                Err(error) => SettlementService::complete_iocp_failure_slot(
                                    slot, error, effect,
                                ),
                            };
                        }
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
        mut slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        if let CompletionSource::Backend(IocpBackendIngress::UdpReceive { request, .. }) = source {
            let child = slot.with_access_mut(|access| -> IocpResult<OrphanedRawUdpChild> {
                let op = access.operation_mut().get_mut();
                let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .attach_note("orphaned raw UDP completion has the wrong payload kind"));
                };
                let child = payload.raw_request_mut(request.slot_id).ok_or_else(|| {
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("slot_id", request.slot_id)
                        .attach_note("orphaned raw UDP child request owner is missing")
                })?;
                if child.slot_id() != request.slot_id
                    || child.header().receive_request != Some(*request)
                    || !child.header().in_flight
                {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .with_ctx("request_id", request.request_id)
                        .with_ctx("slot_id", request.slot_id)
                        .with_ctx("slot_generation", request.slot_generation)
                        .attach_note("orphaned raw UDP child completion identity is stale"));
                }
                let header = child.header_mut();
                let Some(socket_inflight) = header.socket_inflight.take() else {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .with_ctx("request_id", request.request_id)
                        .attach_note("orphaned raw UDP child has no socket inflight token"));
                };
                header.in_flight = false;
                header.receive_request = None;
                let has_active_children = payload.raw_requests().is_some_and(|requests| {
                    requests.iter().any(|request| request.header().in_flight)
                });
                Ok(OrphanedRawUdpChild {
                    socket_inflight,
                    has_active_children,
                })
            });
            let child = match child {
                Ok(Ok(child)) => child,
                _ => {
                    return CompletionSettlement::Anomaly {
                        kind: veloq_driver_core::CompletionAnomalyKind::backend_context(
                            COMP_BACKEND_IOCP,
                            event.token().index() as u64,
                        ),
                        attach: AnomalyAttach::from_raw_completion(event.raw()),
                        cleanup: CompletionCleanupGuard::default(),
                        effect: IocpBackendEffect::None,
                    };
                }
            };
            let effect = IocpBackendEffect::SocketInflight(child.socket_inflight);
            if child.has_active_children || self.overlapped_registry.has_children_for(event.token())
            {
                return CompletionSettlement::Cleanup {
                    cleanup: CompletionCleanupGuard::default(),
                    continuation: CompletionContinuation::More,
                    effect,
                };
            }
            let (cleanup, root_socket_inflight) =
                SettlementService::complete_iocp_orphaned_slot(slot, event.res());
            debug_assert!(root_socket_inflight.is_none());
            return CompletionSettlement::Cleanup {
                cleanup,
                continuation: CompletionContinuation::Final,
                effect,
            };
        }
        let has_active_raw_udp_children = slot
            .with_access_mut(|access| {
                let op = access.operation().get_ref();
                if op.kind() != OpKind::UdpRecvMulti || op.uses_rio_backend() {
                    return false;
                }
                let IocpOpPayload::UdpRecvMulti(payload) = &op.payload else {
                    return false;
                };
                payload.raw_requests().is_some_and(|requests| {
                    requests.iter().any(|request| request.header().in_flight)
                })
            })
            .unwrap_or(true);
        if has_active_raw_udp_children || self.overlapped_registry.has_children_for(event.token()) {
            return CompletionSettlement::Cleanup {
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::More,
                effect: IocpBackendEffect::None,
            };
        }
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
