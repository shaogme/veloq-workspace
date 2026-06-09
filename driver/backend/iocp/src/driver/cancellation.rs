use diagweave::prelude::*;
use tracing::{debug, warn};
use veloq_driver_core::driver::{
    CancelMode, CancelRequest, CancelSubmitOutcome, CompletionAnomalyReason, CompletionBackend,
    CompletionCleanupGuard, CompletionToken, OpToken, RawCompletion, RecordCompletionOutcome,
    UserCompletionEvent, record_completion_anomaly, record_lost_completion, run_completion_cleanup,
    slot_view_anomaly,
};
use veloq_driver_core::slot::{CheckedSlotView, SlotRegistryExt, SlotView};

use crate::common::{completion_record, push_completion_shared};
use crate::driver::completion::EmitContext;
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::IocpResult;
use crate::op;
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
    pub(super) fn cancel_op_internal(
        &mut self,
        request: CancelRequest,
    ) -> IocpResult<CancelSubmitOutcome> {
        let token = request.target;
        let (user_data, generation) = token.parts();

        let emit_ctx = EmitContext {
            completion_table: self.completion.table(),
        };

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
            if let Some(outcome) = Self::abort_slot_inner(
                emit_ctx,
                &mut self.ops,
                &mut self.completion_diagnostics,
                request.mode == CancelMode::UserVisible,
                token,
            ) {
                let _ = outcome;
            }
            self.completion_diagnostics.inc_cancel_submitted();
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
                self.record_cancel_status(status)
            }
            CheckedSlotView::Valid(SlotView::Reserved(_)) => {
                if let Some(outcome) = Self::abort_slot_inner(
                    emit_ctx,
                    &mut self.ops,
                    &mut self.completion_diagnostics,
                    request.mode == CancelMode::UserVisible,
                    token,
                ) {
                    let _ = outcome;
                }
                self.completion_diagnostics.inc_cancel_submitted();
                Ok(CancelSubmitOutcome::CompletedLocally)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)
            | CheckedSlotView::Corrupt(_)) => {
                let raw = RawCompletion::new(
                    CompletionBackend::Iocp,
                    CompletionToken::user(token),
                    -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                    0,
                );
                let anomaly = match slot_view_anomaly(CompletionBackend::Iocp, token, raw, view) {
                    Ok(_) => return Ok(CancelSubmitOutcome::TargetMissing),
                    Err(anomaly) => anomaly,
                };
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                match anomaly.reason {
                    CompletionAnomalyReason::StaleGeneration => {
                        debug!(
                            user_data,
                            generation,
                            actual_generation = anomaly.actual_generation,
                            state = ?anomaly.state,
                            "IOCP cancel request is stale"
                        );
                        Ok(CancelSubmitOutcome::TargetStale)
                    }
                    CompletionAnomalyReason::OpMissing
                    | CompletionAnomalyReason::PayloadMissing
                    | CompletionAnomalyReason::SlotCorruption => {
                        debug!(
                            user_data,
                            generation,
                            snapshot = ?anomaly.slot_snapshot,
                            "IOCP cancel request found corrupt slot"
                        );
                        Ok(CancelSubmitOutcome::TargetMissing)
                    }
                    _ => {
                        debug!(
                            user_data,
                            generation,
                            reason = ?anomaly.reason,
                            "IOCP cancel request found non-active slot"
                        );
                        Ok(CancelSubmitOutcome::TargetMissing)
                    }
                }
            }
        }
    }

    fn record_cancel_status(
        &mut self,
        status: IocpResult<CancelPerformStatus>,
    ) -> IocpResult<CancelSubmitOutcome> {
        match status {
            Ok(CancelPerformStatus::Submitted) | Ok(CancelPerformStatus::RioRequested) => {
                self.completion_diagnostics.inc_cancel_submitted();
                self.completion_diagnostics.inc_cancel_observed_ok();
                Ok(CancelSubmitOutcome::Submitted)
            }
            Ok(CancelPerformStatus::NotFound) => {
                self.completion_diagnostics.inc_cancel_observed_not_found();
                debug!("CancelIoEx target was already complete or absent");
                Ok(CancelSubmitOutcome::TargetMissing)
            }
            Ok(CancelPerformStatus::NoHandle) | Ok(CancelPerformStatus::NonActive) => {
                Ok(CancelSubmitOutcome::NoBackendHandle)
            }
            Err(report) => {
                self.completion_diagnostics.inc_cancel_observed_error();
                warn!(report = ?report, "CancelIoEx failed");
                Err(report)
            }
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        token: OpToken,
        ops: &mut IocpOpRegistry,
    ) -> IocpResult<CancelPerformStatus> {
        let user_data = token.index();
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
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut guard)) => {
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
                            op::resolve_fd_handle(&fd, ctx.registered_slots).ok()
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
        ops: &mut IocpOpRegistry,
        diagnostics: &mut veloq_driver_core::driver::DriverCompletionDiagnostics,
        emit_completion: bool,
        token: OpToken,
    ) -> Option<RecordCompletionOutcome> {
        let abort_result: IocpResult<usize> = Err(crate::error::IocpError::CompletionWait
            .to_report()
            .push_ctx("scope", "iocp.driver.abort_slot_inner")
            .set_error_code(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
            .attach_note("operation aborted locally"));
        let mut cleanup = CompletionCleanupGuard::default();
        let mut snapshot = None;
        let inflight = match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(guard)) => {
                snapshot = Some(guard.snapshot());
                let mut guard = guard.complete();
                cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| {
                        if emit_completion {
                            op.completion_cleanup(&abort_result)
                        } else {
                            op.orphan_cleanup(&abort_result)
                        }
                    })
                    .unwrap_or_default();
                let _ = guard.take_op();
                Some(guard.take_completion_data())
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(guard)) => {
                snapshot = Some(guard.snapshot());
                let mut guard = guard.complete();
                cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| op.orphan_cleanup(&abort_result))
                    .unwrap_or_default();
                let _ = guard.take_op();
                Some(guard.take_completion_data())
            }
            CheckedSlotView::Valid(SlotView::Reserved(guard)) => {
                snapshot = Some(guard.snapshot());
                cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| op.completion_cleanup(&abort_result))
                    .unwrap_or_default();
                let _ = guard.op.take();
                Some(
                    guard
                        .storage
                        .with_mut(|result, payload, _sidecar| (payload.take(), result.take())),
                )
            }
            _ => None,
        };

        let (payload, detail) = if let Some(data) = inflight {
            data
        } else {
            ops.with_slot_storage_mut(token, |result, payload, _sidecar| {
                (payload.take(), result.take())
            })
            .unwrap_or((None, None))
        };

        let outcome = if emit_completion {
            if let Some(payload) = payload {
                Some(push_completion_shared(
                    ctx.completion_table,
                    diagnostics,
                    completion_record(CompletionSidecar::new(
                        UserCompletionEvent::from_parts(
                            CompletionBackend::Iocp,
                            token,
                            -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                            0,
                        ),
                        payload,
                        detail,
                        cleanup,
                    )),
                ))
            } else if let Some(snapshot) = snapshot {
                drop(detail);
                let raw = RawCompletion::new(
                    CompletionBackend::Iocp,
                    CompletionToken::user(token),
                    -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                    0,
                );
                let anomaly = veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                Some(record_lost_completion(
                    ctx.completion_table,
                    diagnostics,
                    UserCompletionEvent::from_parts(
                        CompletionBackend::Iocp,
                        token,
                        raw.res,
                        raw.flags,
                    ),
                    anomaly,
                    cleanup,
                ))
            } else {
                drop(detail);
                None
            }
        } else {
            let payload_missing = payload.is_none();
            drop(payload);
            drop(detail);
            if payload_missing && let Some(snapshot) = snapshot {
                let raw = RawCompletion::new(
                    CompletionBackend::Iocp,
                    CompletionToken::user(token),
                    -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                    0,
                );
                let anomaly = veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                record_completion_anomaly(diagnostics, &anomaly);
            }
            let _ = run_completion_cleanup(diagnostics, &mut cleanup);
            None
        };

        let _ = ops.remove(token);
        outcome
    }
}
