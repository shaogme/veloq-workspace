use std::sync::atomic::Ordering;

use tracing::{debug, warn};
use veloq_driver_core::driver::{CancelMode, CancelRequest, RecordCompletionOutcome};
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};

use crate::common::{completion_record, push_completion_shared};
use crate::driver::completion::EmitContext;
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::IocpResult;
use crate::op::submit;
use crate::win32::CancelRequestResult;

struct CancelContext<'a> {
    registered_slots: &'a [crate::config::RegisteredSlot],
}

enum CancelPerformStatus {
    Submitted,
    NotFound,
    RioRequested,
    NoHandle,
    NonActive,
}

impl<'a> IocpDriver<'a> {
    pub(super) fn cancel_op_internal(&mut self, request: CancelRequest) {
        let Some((user_data, generation)) = request.token.user_parts() else {
            self.completion_diagnostics.inc_unknown_completion();
            debug!(
                token = request.token.raw(),
                "IOCP cancel request token is not user op"
            );
            return;
        };

        if !self.ops.contains(user_data) {
            self.completion_diagnostics.inc_unknown_completion();
            return;
        }
        let current_generation = self.ops.shared.slots[user_data].generation(Ordering::Acquire);
        if current_generation != generation {
            self.completion_diagnostics.inc_stale_completion();
            debug!(
                user_data,
                generation, current_generation, "ignoring stale IOCP cancel request"
            );
            return;
        }

        let emit_ctx = EmitContext {
            completion_events: self.completion.events(),
            completion_table: self.completion.table(),
        };

        let timer_id = self
            .ops
            .get_mut(user_data)
            .and_then(|op| op.platform_data.timer_id);
        if let Some(tid) = timer_id {
            self.timer.cancel(tid);
            if let Some(outcome) = Self::abort_slot_inner(
                emit_ctx,
                user_data,
                &mut self.ops,
                request.mode == CancelMode::UserVisible,
            ) {
                self.completion_diagnostics
                    .record_completion_outcome(&outcome);
            }
            self.completion_diagnostics.inc_cancel_submitted();
            return;
        }

        let state = self.ops.unchecked_slot_view(user_data);
        match state {
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) => {
                let ctx = CancelContext {
                    registered_slots: self.handles.registered_slots(),
                };
                let status = Self::perform_cancel(ctx, user_data, &mut self.ops);
                self.record_cancel_status(status);
            }
            _ => {
                if let Some(outcome) = Self::abort_slot_inner(
                    emit_ctx,
                    user_data,
                    &mut self.ops,
                    request.mode == CancelMode::UserVisible,
                ) {
                    self.completion_diagnostics
                        .record_completion_outcome(&outcome);
                }
                self.completion_diagnostics.inc_cancel_submitted();
            }
        }
    }

    fn record_cancel_status(&mut self, status: IocpResult<CancelPerformStatus>) {
        match status {
            Ok(CancelPerformStatus::Submitted) | Ok(CancelPerformStatus::RioRequested) => {
                self.completion_diagnostics.inc_cancel_submitted();
            }
            Ok(CancelPerformStatus::NotFound) => {
                self.completion_diagnostics.inc_cancel_cqe_enoent();
                debug!("CancelIoEx target was already complete or absent");
            }
            Ok(CancelPerformStatus::NoHandle) | Ok(CancelPerformStatus::NonActive) => {
                self.completion_diagnostics.inc_unknown_completion();
            }
            Err(report) => {
                self.completion_diagnostics.inc_cancel_cqe_error();
                warn!(report = ?report, "CancelIoEx failed");
            }
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        user_data: usize,
        ops: &mut IocpOpRegistry,
    ) -> IocpResult<CancelPerformStatus> {
        let status = match ops.unchecked_slot_view(user_data) {
            Some(SlotView::InFlightWaiting(mut guard)) => {
                let is_rio = guard
                    .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                    .unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                    CancelPerformStatus::RioRequested
                } else {
                    let raw_handle = guard
                        .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                        .flatten()
                        .or_else(|| {
                            let fd = guard.with_op_mut(|iocp_op| iocp_op.get_fd()).flatten()?;
                            submit::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
                        // SAFETY: `guard.storage` exposes the overlapped entry for this cancelled slot.
                        let overlapped_ptr =
                            guard.storage.with_mut(|_result, _payload, sidecar| {
                                &mut sidecar.inner as *mut crate::win32::Overlapped
                            });
                        // SAFETY: handle and overlapped_ptr are valid for this operation.
                        match unsafe {
                            crate::win32::IoCompletionPort::cancel_request(handle, overlapped_ptr)
                        }? {
                            CancelRequestResult::Submitted => CancelPerformStatus::Submitted,
                            CancelRequestResult::NotFound => CancelPerformStatus::NotFound,
                        }
                    } else {
                        CancelPerformStatus::NoHandle
                    }
                }
            }
            Some(SlotView::InFlightOrphaned(mut guard)) => {
                let is_rio = guard.op.as_ref().map(Self::is_rio_op).unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                    CancelPerformStatus::RioRequested
                } else {
                    let raw_handle = guard
                        .op
                        .as_mut()
                        .and_then(|iocp_op| iocp_op.header.resolved_handle)
                        .or_else(|| {
                            let fd = guard.op.as_mut().and_then(|iocp_op| iocp_op.get_fd())?;
                            submit::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
                        let overlapped_ptr =
                            guard.storage.with_mut(|_result, _payload, sidecar| {
                                &mut sidecar.inner as *mut crate::win32::Overlapped
                            });
                        match unsafe {
                            crate::win32::IoCompletionPort::cancel_request(handle, overlapped_ptr)
                        }? {
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

        if matches!(status, CancelPerformStatus::NonActive) {
            debug!(user_data, "Skipping cancel for non in-flight slot");
        }
        Ok(status)
    }

    fn abort_slot_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        ops: &mut IocpOpRegistry,
        emit_completion: bool,
    ) -> Option<RecordCompletionOutcome> {
        let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
        let inflight = match ops.unchecked_slot_view(user_data) {
            Some(SlotView::InFlightWaiting(guard)) => {
                let mut guard = guard.complete();
                let _ = guard.take_op();
                Some(guard.take_completion_data())
            }
            Some(SlotView::InFlightOrphaned(guard)) => {
                let mut guard = guard.complete();
                let _ = guard.take_op();
                Some(guard.take_completion_data())
            }
            _ => None,
        };

        let (payload, detail) = if let Some(data) = inflight {
            data
        } else {
            ops.with_slot_storage_mut(user_data, |result, payload, _sidecar| {
                (payload.take(), result.take())
            })
            .unwrap_or((None, None))
        };

        let outcome = if emit_completion {
            Some(push_completion_shared(
                ctx.completion_events,
                ctx.completion_table,
                completion_record(CompletionSidecar {
                    user_data,
                    generation,
                    res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                    flags: 0,
                    payload,
                    detail,
                }),
            ))
        } else {
            drop(payload);
            drop(detail);
            None
        };

        ops.remove(user_data);
        outcome
    }
}
