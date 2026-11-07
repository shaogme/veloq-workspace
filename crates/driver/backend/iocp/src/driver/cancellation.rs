use diagweave::prelude::*;
use veloq_driver_core::{
    AnomalyAttach, CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason,
    CheckedSlotView, CompletionToken, OpKind, OpToken, RawCompletion, Slot, SlotMarker,
    SlotRegistryExt, SlotView, SyntheticCompletionSource, UdpRecvMultiBackend, UserCompletionEvent,
};
use veloq_std::{format, ptr, string::ToString, sync::Arc};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

use crate::{
    config::RegisteredSlot,
    driver::{
        completion::{COMP_BACKEND_IOCP, CompletionService, IocpSyntheticCompletion},
        state::{CancellationStateView, IocpDriverState},
    },
    error::{IocpError, IocpResult},
    op,
    op::{
        IocpOpPayload, IocpOpRegistry, IocpSlotSpec, IocpUserPayload,
        RAW_UDP_RECV_MULTI_RESUME_BYTES, UdpReceiveSubmit,
    },
    win32::{CancelRequestResult, IoCompletionPort, Overlapped},
};

struct CancelContext<'a> {
    port: Arc<IoCompletionPort>,
    registered_slots: &'a [RegisteredSlot],
}

struct CancelTargetState {
    is_rio: bool,
    op_kind: OpKind,
    has_kernel_request: bool,
}

enum CancelPerformStatus {
    Submitted,
    NotFound,
    RioRequested,
    NoHandle,
    NonActive,
}

fn mark_receive_pump_cancelled<S: SlotMarker>(
    slot: &mut Slot<'_, S, IocpSlotSpec>,
) -> IocpResult<()> {
    slot.with_access_mut(|access| {
        let (operation, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| IocpError::InvalidState.to_report())?;
        match &mut *payload {
            IocpUserPayload::UdpRecvMulti(user) => {
                let pump = user.receive_pump_mut();
                pump.bind_resume_token(0);
                pump.cancel()
            }
            IocpUserPayload::RecvMulti(_) => {
                let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                    return Ok(());
                };
                pump.cancel()
            }
            _ => return Ok(()),
        }
        .map(|_| ())
        .map_err(|error| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("receive_pump_error", error.to_string())
        })
    })
    .map_err(|_| IocpError::InvalidState.to_report())?
}

fn mark_receive_user_cancel<S: SlotMarker>(
    slot: &mut Slot<'_, S, IocpSlotSpec>,
    op_kind: OpKind,
) -> IocpResult<()> {
    if matches!(op_kind, OpKind::RecvMulti | OpKind::UdpRecvMulti) {
        mark_receive_pump_cancelled(slot)?;
    }
    slot.platform_mut().rio_user_cancel_requested = true;
    Ok(())
}

fn has_raw_udp_child_request<S: SlotMarker>(slot: &mut Slot<'_, S, IocpSlotSpec>) -> bool {
    slot.with_access_mut(|access| {
        let op = access.operation().get_ref();
        let IocpOpPayload::UdpRecvMulti(payload) = &op.payload else {
            return false;
        };
        payload
            .raw_requests()
            .is_some_and(|requests| requests.iter().any(|request| request.header().in_flight))
    })
    .unwrap_or(false)
}

fn cancel_raw_udp_children<S: SlotMarker>(
    slot: &mut Slot<'_, S, IocpSlotSpec>,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<()> {
    slot.with_access_mut(|access| -> IocpResult<()> {
        let op = access.operation_mut().get_mut();
        let raw = match op.header.resolved_handle {
            Some(raw) => Some(raw),
            None => op
                .get_fd()
                .and_then(|fd| op::resolve_fd_handle(&fd, registered_slots).ok()),
        };
        let Some(raw) = raw else {
            return Ok(());
        };
        let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
            return Ok(());
        };
        UdpReceiveSubmit::cancel_raw_requests(payload, raw);
        Ok(())
    })
    .map_err(|_| IocpError::InvalidState.to_report())?
}

pub(super) struct CancellationService<'view, 'state> {
    state: &'view mut CancellationStateView<'state>,
}

impl<'view, 'state> CancellationService<'view, 'state> {
    pub(super) fn new(state: &'view mut CancellationStateView<'state>) -> Self {
        Self { state }
    }

    fn state(&self) -> &CancellationStateView<'state> {
        self.state
    }

    fn state_mut(&mut self) -> &mut CancellationStateView<'state> {
        self.state
    }

    pub(super) fn cancel_op_internal(
        &mut self,
        request: CancelRequest,
    ) -> IocpResult<CancelSubmitOutcome> {
        let token = request.target();

        let timer_id = match self.state_mut().ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                slot.platform_mut().timer_id.take()
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                slot.platform_mut().timer_id.take()
            }
            _ => None,
        };
        if let Some(tid) = timer_id {
            self.state_mut().timer.cancel(tid);
            self.complete_local_cancel(token, request.mode())?;
            self.state()
                .completion_diagnostics
                .backend()
                .inc_cancel_local_completed();
            return Ok(CancelSubmitOutcome::CompletedLocally);
        }

        let state = self.state_mut().ops.checked_slot_view(token)?;
        match state {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(_))
            | CheckedSlotView::Valid(SlotView::InFlightOrphaned(_)) => {
                let state = self.state_mut();
                let port = state.completion.port_arc();
                let ctx = CancelContext {
                    port,
                    registered_slots: state.handles.registered_slots(),
                };
                let status = Self::perform_cancel(ctx, token, request.mode(), state.ops);
                self.record_cancel_status(token, status)
            }
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let guard = slot.start_submission_with(None).map_err(|err| {
                        IocpError::InvalidState.report(
                            "cancel_op_internal",
                            format!(
                                "Reserved slot has_op but start_submission_with failed: {:?}",
                                err
                            ),
                        )
                    })?;
                    let _ = guard.persist();
                    self.complete_local_cancel(token, request.mode())?;
                } else {
                    let _ = self.state_mut().ops.remove(token);
                }
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_cancel_local_completed();
                Ok(CancelSubmitOutcome::CompletedLocally)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)) => {
                let (reason, kind) = CancelRequest::target_kind(token, view);
                self.record_cancel_target_gone(reason);
                let attach = AnomalyAttach::from_raw_completion(RawCompletion::new(
                    COMP_BACKEND_IOCP,
                    CompletionToken::user(token),
                    -(ERROR_OPERATION_ABORTED as i32),
                    0,
                ));
                let mut view = IocpDriverState::completion_view_from_cancellation(self.state_mut());
                let _ =
                    CompletionService::new(&mut view).accept_completion_anomaly(kind, attach)?;
                Ok(CancelSubmitOutcome::TargetGone { reason })
            }
        }
    }

    fn complete_local_cancel(&mut self, token: OpToken, mode: CancelMode) -> IocpResult<()> {
        let event = UserCompletionEvent::from_parts(
            COMP_BACKEND_IOCP,
            token,
            -(ERROR_OPERATION_ABORTED as i32),
            0,
        );
        let mut view = IocpDriverState::completion_view_from_cancellation(self.state_mut());
        CompletionService::new(&mut view).accept_synthetic_completion(
            event,
            SyntheticCompletionSource::Cancel,
            IocpSyntheticCompletion::Cancel { mode },
        )?;
        Ok(())
    }

    fn record_cancel_status(
        &mut self,
        token: OpToken,
        status: IocpResult<CancelPerformStatus>,
    ) -> IocpResult<CancelSubmitOutcome> {
        match status {
            Ok(CancelPerformStatus::Submitted) | Ok(CancelPerformStatus::RioRequested) => {
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_cancel_submitted();
                Ok(CancelSubmitOutcome::Submitted)
            }
            Ok(CancelPerformStatus::NotFound) => {
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_cancel_ack_not_found();
                if self.record_cancel_not_found_active(token)? {
                    Ok(CancelSubmitOutcome::CompletionPending)
                } else {
                    self.record_cancel_target_gone(CancelTargetGoneReason::Missing);
                    Ok(CancelSubmitOutcome::target_missing())
                }
            }
            Ok(CancelPerformStatus::NoHandle) => {
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_cancel_no_handle();
                Ok(CancelSubmitOutcome::NoBackendHandle)
            }
            Ok(CancelPerformStatus::NonActive) => {
                self.record_cancel_target_gone(CancelTargetGoneReason::Missing);
                Ok(CancelSubmitOutcome::target_missing())
            }
            Err(report) => {
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_cancel_error();
                Err(report)
            }
        }
    }

    fn record_cancel_not_found_active(&mut self, token: OpToken) -> IocpResult<bool> {
        let active_target = match self.state_mut().ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => Some(slot.snapshot()),
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => Some(slot.snapshot()),
            _ => None,
        };

        let Some(_snapshot) = active_target else {
            return Ok(false);
        };

        self.state()
            .completion_diagnostics
            .backend()
            .inc_cancel_ack_not_found_active();
        Ok(true)
    }

    fn record_cancel_target_gone(&self, reason: CancelTargetGoneReason) {
        match reason {
            CancelTargetGoneReason::Missing => self
                .state()
                .completion_diagnostics
                .backend()
                .inc_cancel_target_missing(),
            CancelTargetGoneReason::Stale => self
                .state()
                .completion_diagnostics
                .backend()
                .inc_cancel_target_stale(),
            CancelTargetGoneReason::Corrupt => self
                .state()
                .completion_diagnostics
                .backend()
                .inc_cancel_target_corrupt(),
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        token: OpToken,
        mode: CancelMode,
        ops: &mut IocpOpRegistry,
    ) -> IocpResult<CancelPerformStatus> {
        let status = match ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut guard)) => {
                let mut target_state = guard
                    .with_access_mut(|access| {
                        let op = access.operation().get_ref();
                        CancelTargetState {
                            is_rio: op.uses_rio_backend(),
                            op_kind: op.kind(),
                            has_kernel_request: op.header.in_flight,
                        }
                    })
                    .unwrap_or(CancelTargetState {
                        is_rio: false,
                        op_kind: OpKind::Wakeup,
                        has_kernel_request: false,
                    });
                if target_state.op_kind == OpKind::UdpRecvMulti && !target_state.is_rio {
                    target_state.has_kernel_request = has_raw_udp_child_request(&mut guard);
                }
                let CancelTargetState {
                    is_rio,
                    op_kind,
                    has_kernel_request,
                } = target_state;

                if is_rio {
                    if mode == CancelMode::UserVisible {
                        mark_receive_user_cancel(&mut guard, op_kind)?;
                    }
                    CancelPerformStatus::RioRequested
                } else {
                    if mode == CancelMode::UserVisible {
                        guard.platform_mut().iocp_user_cancel_requested = true;
                        if matches!(op_kind, OpKind::UdpRecvMulti | OpKind::RecvMulti) {
                            mark_receive_pump_cancelled(&mut guard)?;
                        }
                    }
                    if op_kind == OpKind::UdpRecvMulti {
                        cancel_raw_udp_children(&mut guard, ctx.registered_slots)?;
                        if !has_kernel_request {
                            // SAFETY: A null OVERLAPPED is valid for a posted resume notification. / 空 OVERLAPPED 可用于投递恢复通知。
                            unsafe {
                                ctx.port.post(
                                    RAW_UDP_RECV_MULTI_RESUME_BYTES,
                                    CompletionToken::user(token).raw() as usize,
                                    ptr::null_mut(),
                                )?;
                            }
                        }
                        return Ok(CancelPerformStatus::Submitted);
                    }
                    let raw_handle = guard
                        .with_access_mut(|access| {
                            let op = access.operation().get_ref();
                            op.header.resolved_handle.or_else(|| {
                                let fd = op.get_fd()?;
                                op::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                            })
                        })
                        .ok()
                        .flatten();
                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.raw().as_handle();
                        let overlapped_ptr =
                            guard.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);
                        match unsafe { IoCompletionPort::cancel_request(handle, overlapped_ptr) }? {
                            CancelRequestResult::Submitted => CancelPerformStatus::Submitted,
                            CancelRequestResult::NotFound => CancelPerformStatus::NotFound,
                        }
                    } else {
                        CancelPerformStatus::NoHandle
                    }
                }
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut guard)) => {
                let mut target_state = guard
                    .with_access_mut(|access| {
                        let op = access.operation().get_ref();
                        CancelTargetState {
                            is_rio: op.uses_rio_backend(),
                            op_kind: op.kind(),
                            has_kernel_request: op.header.in_flight,
                        }
                    })
                    .unwrap_or(CancelTargetState {
                        is_rio: false,
                        op_kind: OpKind::Wakeup,
                        has_kernel_request: false,
                    });
                if target_state.op_kind == OpKind::UdpRecvMulti && !target_state.is_rio {
                    target_state.has_kernel_request = has_raw_udp_child_request(&mut guard);
                }

                if target_state.is_rio {
                    // orphaned slot 已脱离用户取消流程，清理语义由 slot 状态决定。
                    // An orphaned slot is outside user-visible cancellation; its slot state controls cleanup semantics.
                    CancelPerformStatus::RioRequested
                } else if target_state.op_kind == OpKind::UdpRecvMulti {
                    cancel_raw_udp_children(&mut guard, ctx.registered_slots)?;
                    if !target_state.has_kernel_request {
                        // SAFETY: A null OVERLAPPED is valid for a posted token notification. / 空 OVERLAPPED 可用于投递 token 通知。
                        unsafe {
                            ctx.port.post(
                                RAW_UDP_RECV_MULTI_RESUME_BYTES,
                                CompletionToken::user(token).raw() as usize,
                                ptr::null_mut(),
                            )?;
                        }
                    }
                    CancelPerformStatus::Submitted
                } else {
                    let raw_handle = guard
                        .with_access_mut(|access| {
                            let op = access.operation().get_ref();
                            op.header.resolved_handle.or_else(|| {
                                let fd = op.get_fd()?;
                                op::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                            })
                        })
                        .ok()
                        .flatten();
                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.raw().as_handle();
                        let overlapped_ptr =
                            guard.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);
                        match unsafe { IoCompletionPort::cancel_request(handle, overlapped_ptr) }? {
                            CancelRequestResult::Submitted => CancelPerformStatus::Submitted,
                            CancelRequestResult::NotFound => CancelPerformStatus::NotFound,
                        }
                    } else {
                        CancelPerformStatus::NoHandle
                    }
                }
            }
            _ => CancelPerformStatus::NonActive,
        };

        Ok(status)
    }
}
