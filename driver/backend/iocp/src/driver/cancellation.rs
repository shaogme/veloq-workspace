use veloq_driver_core::{
    driver::{
        CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason, CompletionAnomaly,
        CompletionToken, OpToken, RawCompletion, SyntheticCompletionSource, UserCompletionEvent,
        cancel_target_anomaly,
    },
    slot::{CheckedSlotView, SlotRegistryExt, SlotView},
};
use windows_sys::Win32::Foundation::{ERROR_NOT_FOUND, ERROR_OPERATION_ABORTED};

use crate::{
    config::RegisteredSlot,
    driver::{
        IocpDriver, IocpOpRegistry,
        completion::{COMP_BACKEND_IOCP, IocpSyntheticCompletion},
    },
    error::{IocpError, IocpResult},
    op,
    win32::{CancelRequestResult, IoCompletionPort, Overlapped},
};

struct CancelContext<'a> {
    registered_slots: &'a [RegisteredSlot],
}

enum CancelPerformStatus {
    Submitted,
    NotFound,
    RioRequested,
    NoHandle,
    NonActive,
}

impl<'a> IocpDriver<'a> {
    pub(super) fn cancel_op_internal(
        &mut self,
        request: CancelRequest,
    ) -> IocpResult<CancelSubmitOutcome> {
        let token = request.target;

        let timer_id = match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                slot.platform_mut().timer_id.take()
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                slot.platform_mut().timer_id.take()
            }
            _ => None,
        };
        if let Some(tid) = timer_id {
            self.timer.cancel(tid);
            self.complete_local_cancel(token, request.mode);
            self.completion_diagnostics
                .backend()
                .inc_cancel_local_completed();
            return Ok(CancelSubmitOutcome::CompletedLocally);
        }

        let state = self.ops.checked_slot_view(token);
        match state {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(_))
            | CheckedSlotView::Valid(SlotView::InFlightOrphaned(_)) => {
                let ctx = CancelContext {
                    registered_slots: self.handles.registered_slots(),
                };
                let status = Self::perform_cancel(ctx, token, &mut self.ops);
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
                    self.complete_local_cancel(token, request.mode);
                } else {
                    let _ = self.ops.remove(token);
                }
                self.completion_diagnostics
                    .backend()
                    .inc_cancel_local_completed();
                Ok(CancelSubmitOutcome::CompletedLocally)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)
            | CheckedSlotView::Corrupt(_)) => {
                let corrupt_index = match &view {
                    CheckedSlotView::Corrupt(snapshot) => Some(snapshot.index),
                    CheckedSlotView::Missing { .. }
                    | CheckedSlotView::Empty(_)
                    | CheckedSlotView::Stale(_)
                    | CheckedSlotView::Valid(_) => None,
                };
                let (reason, anomaly) = cancel_target_anomaly(
                    COMP_BACKEND_IOCP,
                    token,
                    -(ERROR_OPERATION_ABORTED as i32),
                    0,
                    view,
                );
                if let Some(index) = corrupt_index {
                    self.release_socket_inflight_for_op(index)?;
                    self.drain_deferred_socket_cleanup();
                }
                self.record_cancel_target_gone(reason);
                let _ = self.accept_completion_anomaly(anomaly)?;
                Ok(CancelSubmitOutcome::TargetGone { reason })
            }
        }
    }

    fn complete_local_cancel(&mut self, token: OpToken, mode: CancelMode) {
        let event = UserCompletionEvent::from_parts(
            COMP_BACKEND_IOCP,
            token,
            -(ERROR_OPERATION_ABORTED as i32),
            0,
        );
        let _ = self.accept_synthetic_completion(
            event,
            SyntheticCompletionSource::Cancel,
            IocpSyntheticCompletion::Cancel { mode },
        );
    }

    fn record_cancel_status(
        &mut self,
        token: OpToken,
        status: IocpResult<CancelPerformStatus>,
    ) -> IocpResult<CancelSubmitOutcome> {
        match status {
            Ok(CancelPerformStatus::Submitted) | Ok(CancelPerformStatus::RioRequested) => {
                self.completion_diagnostics.backend().inc_cancel_submitted();
                Ok(CancelSubmitOutcome::Submitted)
            }
            Ok(CancelPerformStatus::NotFound) => {
                self.completion_diagnostics
                    .backend()
                    .inc_cancel_ack_not_found();
                if let Some(anomaly) = self.record_cancel_not_found_if_target_active(token)? {
                    return Ok(CancelSubmitOutcome::DiagnosticOnly { anomaly });
                }
                self.record_cancel_target_gone(CancelTargetGoneReason::Missing);
                Ok(CancelSubmitOutcome::target_missing())
            }
            Ok(CancelPerformStatus::NoHandle) => {
                self.completion_diagnostics.backend().inc_cancel_no_handle();
                Ok(CancelSubmitOutcome::NoBackendHandle)
            }
            Ok(CancelPerformStatus::NonActive) => {
                self.record_cancel_target_gone(CancelTargetGoneReason::Missing);
                Ok(CancelSubmitOutcome::target_missing())
            }
            Err(report) => {
                self.completion_diagnostics.backend().inc_cancel_error();
                Err(report)
            }
        }
    }

    fn record_cancel_not_found_if_target_active(
        &mut self,
        token: OpToken,
    ) -> IocpResult<Option<CompletionAnomaly>> {
        let active_target = match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => Some(slot.snapshot()),
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => Some(slot.snapshot()),
            _ => None,
        };

        let Some(snapshot) = active_target else {
            return Ok(None);
        };

        self.completion_diagnostics
            .backend()
            .inc_cancel_ack_not_found_active();
        let raw = RawCompletion::new(
            COMP_BACKEND_IOCP,
            CompletionToken::user(token),
            -(ERROR_NOT_FOUND as i32),
            0,
        );
        let anomaly = CompletionAnomaly::cancel_ack_target_still_active(
            raw.token,
            snapshot.index,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw);
        let _ = self.accept_completion_anomaly(anomaly)?;
        Ok(Some(anomaly))
    }

    fn record_cancel_target_gone(&self, reason: CancelTargetGoneReason) {
        match reason {
            CancelTargetGoneReason::Missing => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_missing(),
            CancelTargetGoneReason::Stale => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_stale(),
            CancelTargetGoneReason::Corrupt => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_corrupt(),
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        token: OpToken,
        ops: &mut IocpOpRegistry,
    ) -> IocpResult<CancelPerformStatus> {
        let status = match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut guard)) => {
                let is_rio = guard
                    .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                    .unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                    CancelPerformStatus::RioRequested
                } else {
                    let raw_handle = guard
                        .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                        .ok()
                        .flatten()
                        .or_else(|| {
                            let fd = guard
                                .with_op_mut(|iocp_op| iocp_op.get_fd())
                                .ok()
                                .flatten()?;
                            op::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
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
                let is_rio = guard
                    .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                    .unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                    CancelPerformStatus::RioRequested
                } else {
                    let raw_handle = guard
                        .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                        .ok()
                        .flatten()
                        .or_else(|| {
                            let fd = guard
                                .with_op_mut(|iocp_op| iocp_op.get_fd())
                                .ok()
                                .flatten()?;
                            op::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
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
