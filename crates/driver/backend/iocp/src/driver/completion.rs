use veloq_std::{format, io, mem, num::NonZeroU8, sync::Arc, vec::Vec};

use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelMode, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionCleanupGuard, CompletionContinuation, CompletionControl,
        CompletionEnvelope, CompletionFailure, CompletionFlowExt, CompletionFlowOutcome,
        CompletionIngress, CompletionSettlement, CompletionSource, CompletionToken, OpToken,
        PlatformOp, SyntheticCompletionSource, UserCompletionEvent,
    },
    op::OpKind,
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

use crate::{
    config::RegisteredSlot,
    driver::{IocpDriver, IocpDriverCompletionDiagnostics, polling::CompletionPump},
    error::{IocpError, IocpResult, iocp_report_to_event_res},
    ext::Extensions,
    op::{IocpOp, IocpOpPayload, IocpSlotSpec, Slot, SubmissionResult, SubmitContext},
    rio::{RioState, SocketInflightToken},
    win32::{IoCompletionPort, Overlapped},
};

pub(crate) const COMP_BACKEND_IOCP: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(1) {
        Some(val) => val,
        None => unreachable!(),
    });

pub(crate) const COMP_BACKEND_RIO: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(3) {
        Some(val) => val,
        None => unreachable!(),
    });

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IocpSubmissionFailurePhase {
    Initial,
    Replacement,
}

pub(crate) enum IocpSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
}

impl IocpSyntheticCompletion {
    #[inline]
    fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None => CancelMode::UserVisible,
        }
    }

    #[inline]
    fn take_submission_failure(&mut self) -> Option<Report<IocpError>> {
        None
    }
}

#[derive(Default)]
struct IocpPostCompletionEffects {
    drain_socket_cleanup: bool,
}

enum IocpBackendEffect {
    None,
    SocketInflight(SocketInflightToken),
}

impl Default for IocpBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct IocpCompletionHooks<'a> {
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

impl<'a> IocpCompletionHooks<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        ext: &'a Extensions,
        diagnostics: &'a IocpDriverCompletionDiagnostics,
        rio: &'a mut RioState,
        completion: &'a CompletionPump,
        port: Arc<IoCompletionPort>,
        registered_slots: &'a mut [RegisteredSlot],
        registrar: &'a dyn BufferRegistrar,
        shutting_down: bool,
        synthetic: IocpSyntheticCompletion,
    ) -> Self {
        Self {
            ext,
            diagnostics,
            rio,
            completion,
            port,
            registered_slots,
            registrar,
            shutting_down,
            synthetic,
            post: IocpPostCompletionEffects::default(),
        }
    }

    fn into_post_effects(self) -> IocpPostCompletionEffects {
        self.post
    }

    fn rearm_accept_multi(
        &mut self,
        slot: &mut Slot<'_, InFlightWaiting>,
        token: OpToken,
    ) -> IocpResult<()> {
        // Keep the completed request's token alive while the replacement is submitted. This
        // prevents a synchronous replacement failure from making the old request look settled
        // before its accepted socket record has been handed to the completion table.
        let previous = take_socket_inflight_from_slot(slot).ok_or_else(|| {
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
            self.post.drain_socket_cleanup = true;
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
                        let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                        return complete_iocp_failure_slot(
                            slot,
                            error,
                            socket_inflight
                                .map(IocpBackendEffect::SocketInflight)
                                .unwrap_or_default(),
                        );
                    }
                }
                match calculate_io_result_from_slot(self.ext, &mut slot, event.res()) {
                    Ok(io_result) => {
                        if is_accept_multi && io_result.is_ok() {
                            let cancel_requested = slot.platform().iocp_user_cancel_requested;
                            if self.shutting_down {
                                let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                                return complete_iocp_abandoned_waiting_slot(
                                    slot,
                                    io_result,
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                );
                            }
                            if cancel_requested {
                                let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                                return complete_iocp_cancelled_accept_multi_slot(
                                    slot,
                                    io_result,
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                );
                            }
                            match self.rearm_accept_multi(&mut slot, event.token()) {
                                Ok(()) => {
                                    complete_iocp_waiting_slot(slot, event, io_result, None, true)
                                }
                                Err(error) => {
                                    let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                                    complete_iocp_replacement_failure_slot(
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
                            let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                            if is_accept_multi {
                                complete_iocp_failure_slot(
                                    slot,
                                    io_result
                                        .err()
                                        .unwrap_or_else(|| IocpError::InvalidState.to_report()),
                                    socket_inflight
                                        .map(IocpBackendEffect::SocketInflight)
                                        .unwrap_or_default(),
                                )
                            } else {
                                complete_iocp_waiting_slot(
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
                        let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                        complete_iocp_failure_slot(
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
        let (cleanup, socket_inflight) = complete_iocp_orphaned_slot(slot, event.res());
        CompletionSettlement::Cleanup {
            cleanup,
            continuation: CompletionContinuation::Final,
            effect: socket_inflight
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default(),
        }
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        match effect {
            IocpBackendEffect::None => Ok(()),
            IocpBackendEffect::SocketInflight(token) => {
                self.rio.release_socket_inflight_token(token).trans()?;
                self.post.drain_socket_cleanup = true;
                Ok(())
            }
        }
    }
}

impl<'a> IocpDriver<'a> {
    pub(super) fn process_timers(&mut self) -> IocpResult<usize> {
        let timer_buffer = self.state_mut().timer.take_buffer();
        let mut expired = Vec::new();
        for entry in &timer_buffer {
            let token = entry.item;
            match self.state_mut().ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => {
                    slot.platform_mut().timer_id = None;
                    expired.push(token);
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot))) => {
                    slot.platform_mut().timer_id = None;
                    expired.push(token);
                }
                _ => expired.push(token),
            }
        }

        let expired_count = expired.len();
        for token in expired {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, token, 0, 0);
            self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                IocpSyntheticCompletion::None,
            )?;
        }
        self.state_mut().timer.restore_cleared_buffer(timer_buffer);
        Ok(expired_count)
    }

    pub(super) fn process_completion_envelope(
        &mut self,
        envelope: CompletionEnvelope,
    ) -> IocpResult<usize> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(envelope),
            IocpSyntheticCompletion::None,
        )?;
        Ok(1)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            IocpSyntheticCompletion::None,
        )
    }

    pub(crate) fn accept_raw_completion(
        &mut self,
        raw_token: u64,
        res: i32,
        flags: u32,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_IOCP,
                raw_token,
                res,
                flags,
            )),
            IocpSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        let registrar = self.registrar;
        let state = self.state_mut();
        let port = state.completion.port_arc();
        let (rio, registrar) = state.rio.state_and_registrar_mut(registrar);
        let registered_slots = state.handles.submission_slots();
        let mut hooks = IocpCompletionHooks::new(
            &state.extensions,
            &state.completion_diagnostics,
            rio,
            &state.completion,
            port,
            registered_slots,
            registrar,
            state.shutting_down,
            synthetic,
        );
        let outcome = state.ops.accept_completion(
            state.completion.table(),
            &state.completion_diagnostics,
            &mut hooks,
            ingress,
        )?;
        let post = hooks.into_post_effects();
        if post.drain_socket_cleanup {
            self.drain_deferred_socket_cleanup();
        }
        Ok(outcome)
    }
}

fn calculate_io_result_from_slot(
    ext: &Extensions,
    guard: &mut Slot<'_, InFlightWaiting>,
    event_res: i32,
) -> IocpResult<IocpResult<usize>> {
    let user_data = guard.snapshot().index;
    let mut io_result = if event_res < 0 {
        Err(IocpError::CompletionWait.io_report(
            "iocp.driver.calculate_io_result_from_slot",
            io::Error::from_raw_os_error(-event_res),
        ))
    } else {
        Ok(event_res as usize)
    };

    let op_res = guard.with_access_mut(|access| {
        let iocp_op = access.operation_mut().get_mut();
        let blocking_res = iocp_op
            .header
            .blocking_completion
            .take()
            .and_then(|completion| completion.take_result());
        if let Some(res) = blocking_res {
            io_result = res;
        } else if matches!(
            &iocp_op.payload,
            IocpOpPayload::Open(_)
                | IocpOpPayload::Close(_)
                | IocpOpPayload::Fsync(_)
                | IocpOpPayload::FsyncRaw(_)
                | IocpOpPayload::SyncRange(_)
                | IocpOpPayload::SyncRangeRaw(_)
                | IocpOpPayload::Fallocate(_)
                | IocpOpPayload::FallocateRaw(_)
        ) {
            io_result = IocpError::CompletionWait
                .push_ctx("scope", "iocp/driver")
                .with_ctx("user_data", user_data)
                .attach_note("missing blocking result for offloaded file completion");
        } else if let Ok(val) = io_result {
            io_result = iocp_op
                .on_complete(val, ext)
                .attach_note("IOCP completion hook failed");
        }
    });

    if let Err(err) = op_res {
        return Err(IocpError::InvalidState.report(
            "iocp.calculate_io_result_from_slot",
            format!("slot op missing on completion: {:?}", err),
        ));
    }

    Ok(io_result)
}

fn complete_iocp_waiting_slot(
    guard: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    io_result: IocpResult<usize>,
    socket_inflight: Option<SocketInflightToken>,
    accept_multi: bool,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let mut io_detail = Some(io_result);
    let effect = socket_inflight
        .map(IocpBackendEffect::SocketInflight)
        .unwrap_or_default();
    let mut guard = guard.complete();

    if guard.platform_mut().is_background {
        let _ = guard.take_op();
        let _ = guard.take_completion_data();
        let _data = mem::take(guard.platform_mut());
        return CompletionSettlement::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::Final,
            effect,
        };
    }

    let completion_res = io_detail
        .as_ref()
        .map(io_result_to_event_res)
        .unwrap_or(event.res());
    let cleanup = if let Some(io_result) = io_detail.as_ref() {
        guard
            .with_access_mut(|access| {
                let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), io_result);
                if !accept_multi {
                    access.operation_mut().get_mut().unbind_user_payload();
                }
                cleanup
            })
            .unwrap_or_default()
    } else {
        let _ = guard.with_access_mut(|access| {
            access.operation_mut().get_mut().unbind_user_payload();
        });
        CompletionCleanupGuard::default()
    };
    let event =
        UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, event.token(), completion_res, 0);

    if accept_multi {
        let record_payload = guard
            .with_access_mut(|access| access.operation().get_ref().accepted_socket_record())
            .ok()
            .flatten();
        if let Some(payload) = record_payload {
            let detail = io_detail.take();
            let continuation = guard
                .with_access_mut(|access| {
                    access.operation().get_ref().multishot_vtable().map_or(
                        CompletionContinuation::Final,
                        |vtable| {
                            (vtable.continuation)(
                                detail
                                    .as_ref()
                                    .expect("IOCP completion detail must remain available"),
                            )
                        },
                    )
                })
                .unwrap_or(CompletionContinuation::Final);
            return CompletionSettlement::User {
                event,
                payload,
                detail,
                cleanup,
                continuation,
                effect,
            };
        }

        let _ = guard.with_access_mut(|access| {
            access.operation_mut().get_mut().unbind_user_payload();
        });
        let _ = guard.take_op();
        let _ = guard.take_completion_data();
        return CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                IocpError::InvalidState
                    .to_report()
                    .push_ctx("scope", "iocp.complete_iocp_waiting_slot")
                    .attach_note("AcceptMulti record payload encoder is missing"),
                cleanup,
                effect,
            ),
        };
    }

    let (payload, detail) = guard.take_completion_data();
    if let Some(payload) = payload {
        let _ = guard.take_op();
        let _data = mem::take(guard.platform_mut());
        CompletionSettlement::User {
            event,
            payload,
            detail: detail.or_else(|| io_detail.take()),
            cleanup,
            continuation: CompletionContinuation::Final,
            effect,
        }
    } else {
        drop(detail);
        let _ = guard.take_op();
        let _data = mem::take(guard.platform_mut());
        CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                IocpError::InvalidState.report(
                    "iocp.complete_iocp_waiting_slot",
                    "slot payload missing on completion",
                ),
                cleanup,
                effect,
            ),
        }
    }
}

fn complete_iocp_replacement_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    io_result: IocpResult<usize>,
    replacement_error: Report<IocpError>,
    effect: IocpBackendEffect,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let replacement_error = replacement_error
        .with_ctx(
            "submission_phase",
            format!("{:?}", IocpSubmissionFailurePhase::Replacement),
        )
        .attach_note("multishot replacement submission failed after a record completed");
    let terminal_res = iocp_report_to_event_res(&replacement_error);
    let mut guard = slot.complete();
    let record_payload = guard
        .with_access_mut(|access| {
            access
                .operation()
                .get_ref()
                .multishot_vtable()
                .map(|vtable| (vtable.encode_accepted_socket)())
        })
        .ok()
        .flatten();
    let cleanup = guard
        .with_access_mut(|access| {
            let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
            access.operation_mut().get_mut().unbind_user_payload();
            cleanup
        })
        .unwrap_or_default();

    let Some(payload) = record_payload else {
        let _ = guard.take_op();
        let _ = guard.take_completion_data();
        return CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                IocpError::InvalidState
                    .to_report()
                    .push_ctx("scope", "iocp.complete_iocp_replacement_failure")
                    .attach_note("AcceptMulti record payload encoder is missing"),
                cleanup,
                effect,
            ),
        };
    };

    CompletionSettlement::UserThenTerminal {
        event: UserCompletionEvent::from_parts(
            COMP_BACKEND_IOCP,
            event.token(),
            io_result_to_event_res(&io_result),
            0,
        ),
        payload,
        detail: Some(io_result),
        cleanup,
        effect,
        terminal_event: UserCompletionEvent::from_parts(
            COMP_BACKEND_IOCP,
            event.token(),
            terminal_res,
            0,
        ),
        terminal_error: replacement_error,
        terminal_cleanup: CompletionCleanupGuard::default(),
        terminal_effect: IocpBackendEffect::None,
    }
}

fn complete_iocp_failure_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    error: Report<IocpError>,
    effect: IocpBackendEffect,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let completion_result: IocpResult<usize> = Err(error);
    let cleanup = slot
        .with_access_mut(|access| {
            let cleanup =
                PlatformOp::completion_cleanup(access.operation_mut(), &completion_result);
            access.operation_mut().get_mut().unbind_user_payload();
            cleanup
        })
        .unwrap_or_default();
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (_, detail) = completed.take_completion_data();
    drop(detail);
    let error = match completion_result {
        Err(error) => error,
        Ok(_) => unreachable!("failure result must contain the original completion error"),
    };
    CompletionSettlement::TerminalFailure {
        failure: CompletionFailure::terminal(error, cleanup, effect),
    }
}

fn complete_iocp_abandoned_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    io_result: IocpResult<usize>,
    effect: IocpBackendEffect,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let cleanup = slot
        .with_access_mut(|access| {
            let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
            access.operation_mut().get_mut().unbind_user_payload();
            cleanup
        })
        .unwrap_or_default();
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let _ = completed.take_completion_data();
    CompletionSettlement::Cleanup {
        cleanup,
        continuation: CompletionContinuation::Final,
        effect,
    }
}

fn complete_iocp_cancelled_accept_multi_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    io_result: IocpResult<usize>,
    effect: IocpBackendEffect,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let cleanup = slot
        .with_access_mut(|access| {
            let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
            access.operation_mut().get_mut().unbind_user_payload();
            cleanup
        })
        .unwrap_or_default();
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let _ = completed.take_completion_data();
    let error = IocpError::CompletionWait.io_report(
        "iocp.driver.accept_multi.cancel",
        io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
    );
    CompletionSettlement::TerminalFailure {
        failure: CompletionFailure::terminal(error, cleanup, effect),
    }
}

fn complete_timer_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let io_result: IocpResult<usize> = Ok(0);
    complete_iocp_waiting_slot(slot, event, io_result, None, false)
}

fn complete_submission_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<IocpError>>,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let io_result = report.unwrap_or_else(|| {
        IocpError::Submission
            .to_report()
            .push_ctx("scope", "iocp.driver.submission_failure")
            .set_error_code((-event.res()).max(1))
            .attach_note("IOCP submission failed")
    });
    complete_iocp_waiting_slot(slot, event, Err(io_result), None, false)
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
    let abort_result: IocpResult<usize> = IocpError::CompletionWait
        .push_ctx("scope", "iocp.driver.cancel")
        .set_error_code((-event.res()).max(1))
        .attach_note("operation aborted locally");
    if mode == CancelMode::UserVisible {
        complete_iocp_waiting_slot(slot, event, abort_result, None, false)
    } else {
        let mut completed = slot.complete();
        let cleanup = completed
            .with_access_mut(|access| {
                PlatformOp::orphan_cleanup(access.operation_mut(), &abort_result)
            })
            .unwrap_or_default();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        drop(payload);
        drop(detail);
        CompletionSettlement::Cleanup {
            cleanup,
            continuation: CompletionContinuation::Final,
            effect: IocpBackendEffect::None,
        }
    }
}

fn complete_iocp_orphaned_slot(
    slot: Slot<'_, InFlightOrphaned>,
    event_res: i32,
) -> (CompletionCleanupGuard, Option<SocketInflightToken>) {
    let mut completed = slot.complete();
    let io_result = if event_res >= 0 {
        Ok(event_res as usize)
    } else {
        Err(IocpError::CompletionWait.io_report(
            "iocp.driver.process_completion.orphaned",
            io::Error::from_raw_os_error(-event_res),
        ))
    };
    let (cleanup, socket_inflight) = completed
        .with_access_mut(|access| {
            let cleanup = PlatformOp::orphan_cleanup(access.operation_mut(), &io_result);
            let socket_inflight = take_socket_inflight_from_op(access.operation_mut().get_mut());
            (cleanup, socket_inflight)
        })
        .unwrap_or_default();
    let _ = completed.take_op();
    let _ = completed.take_completion_data();
    (cleanup, socket_inflight)
}

#[inline]
fn take_socket_inflight_from_slot(
    slot: &mut Slot<'_, InFlightWaiting>,
) -> Option<SocketInflightToken> {
    slot.with_access_mut(|access| take_socket_inflight_from_op(access.operation_mut().get_mut()))
        .ok()
        .flatten()
}

#[inline]
fn take_socket_inflight_from_op(op: &mut IocpOp) -> Option<SocketInflightToken> {
    if op.header.in_flight {
        op.header.in_flight = false;
    }
    op.header.socket_inflight.take()
}

#[inline]
fn io_result_to_event_res(res: &IocpResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}
