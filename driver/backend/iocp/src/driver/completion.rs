use std::time::Instant;

use diagweave::prelude::*;
use tracing::{debug, error};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionCleanupGuard, DriverCompletionDiagnostics,
    OpToken, RawCompletion, RecordCompletionOutcome, RoutedSlotCompletion,
    record_completion_anomaly, record_lost_completion, route_checked_slot_completion,
};
use veloq_driver_core::slot::{CheckedSlotView, InFlightWaiting, SlotRegistryExt, SlotView};

use crate::common::{completion_record, io_result_to_event_res, push_completion_shared};
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::{IocpError, IocpResult};
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpUserPayload};

pub(super) struct EmitContext<'a> {
    pub(super) completion_events: &'a veloq_driver_core::driver::SharedCompletionQueue,
    pub(super) completion_table:
        &'a veloq_driver_core::driver::SharedCompletionTable<IocpUserPayload, IocpError>,
}

enum CompletionRoute {
    Waiting,
    Orphaned,
    Missing(CompletionAnomaly),
    Empty(CompletionAnomaly),
    Stale(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

impl<'a> IocpDriver<'a> {
    pub(super) fn process_timers(&mut self) {
        let timer_buffer = self.timer.take_buffer();
        let mut pending_events: Vec<CompletionSidecar> = Vec::new();
        let now = Instant::now();

        let mut expired = Vec::new();
        for &token in &timer_buffer {
            let user_data = token.index();
            let in_flight = matches!(
                self.ops.checked_slot_view(token),
                CheckedSlotView::Valid(SlotView::InFlightWaiting(_))
                    | CheckedSlotView::Valid(SlotView::InFlightOrphaned(_))
            );
            if let Some(op) = self.ops.local.get_mut(user_data) {
                if in_flight {
                    if let Some(deadline) = op.entry.platform_data.timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        op.entry.platform_data.timer_id = Some(self.timer.insert(token, remain));
                        continue;
                    }
                    expired.push(token);
                } else {
                    op.entry.platform_data.timer_id = None;
                    op.entry.platform_data.timer_deadline = None;
                }
            }
        }
        let mut finished_timers = Vec::new();
        for token in expired {
            if Self::finish_timer_op(&mut self.ops, token, &mut pending_events) {
                finished_timers.push(token);
            }
        }

        for completion in pending_events {
            let outcome = push_completion_shared(
                self.completion.events(),
                self.completion.table(),
                &mut self.completion_diagnostics,
                completion_record(completion),
            );
            let _ = outcome;
        }
        for token in finished_timers {
            let _ = self.ops.remove_token(token);
        }
        self.timer.restore_cleared_buffer(timer_buffer);
    }

    fn finish_timer_op(
        ops: &mut IocpOpRegistry,
        token: OpToken,
        pending_events: &mut Vec<CompletionSidecar>,
    ) -> bool {
        let mut guard = match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => slot.complete(),
            _ => return false,
        };

        let _ = guard.take_op();
        let (payload_erased, detail) = guard.take_completion_data();
        pending_events.push(CompletionSidecar {
            token,
            res: 0,
            flags: 0,
            payload: payload_erased,
            detail,
            cleanup: CompletionCleanupGuard::default(),
        });
        true
    }

    pub(super) fn process_completion(
        &mut self,
        token: OpToken,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) {
        let (user_data, completed_generation) = token.parts();
        let raw = RawCompletion::new(
            CompletionBackend::Iocp,
            veloq_driver_core::driver::CompletionToken::user(token),
            iocp_completion_res(success, error_code, bytes_transferred),
            0,
        );

        match self.completion_route(token, raw) {
            CompletionRoute::Waiting => {
                let io_result =
                    self.calculate_io_result(token, success, error_code, bytes_transferred);
                self.release_socket_inflight_for_op(user_data);
                let ctx = EmitContext {
                    completion_events: self.completion.events(),
                    completion_table: self.completion.table(),
                };
                let _ = Self::emit_event_inner(
                    ctx,
                    &mut self.ops,
                    &mut self.completion_diagnostics,
                    token,
                    io_result,
                );
            }
            CompletionRoute::Orphaned => {
                self.release_socket_inflight_for_op(user_data);
                let CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) =
                    self.ops.checked_slot_view(token)
                else {
                    return;
                };
                let mut completed = slot.complete();
                let io_result = if success {
                    Ok(bytes_transferred as usize)
                } else {
                    Err(IocpError::CompletionWait.io_report(
                        "iocp.driver.process_completion.orphaned",
                        std::io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32),
                    ))
                };
                let cleanup = completed
                    .op
                    .as_mut()
                    .map(|op| op.completion_cleanup(&io_result))
                    .unwrap_or_default();
                let _ = completed.take_op();
                let _ = completed.take_completion_data();
                drop(completed);
                let anomaly = CompletionAnomaly::non_active(
                    raw.token,
                    user_data,
                    completed_generation,
                    veloq_driver_core::slot::SlotState::InFlightOrphaned,
                )
                .with_raw_completion(raw);
                let _ = record_lost_completion(
                    self.completion.events(),
                    self.completion.table(),
                    &mut self.completion_diagnostics,
                    raw.event(),
                    anomaly,
                    cleanup,
                );
                let _ = self
                    .ops
                    .recycle_token(token, completed_generation.wrapping_add(1));
            }
            CompletionRoute::Missing(anomaly) | CompletionRoute::Empty(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring completion for non-active slot"
                );
            }
            CompletionRoute::Stale(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring stale IOCP completion"
                );
            }
            CompletionRoute::Corrupt(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                error!(
                    user_data,
                    completed_generation, "IOCP completion found corrupt slot; recycling"
                );
                self.release_socket_inflight_for_op(user_data);
                self.ops
                    .recycle_token(token, completed_generation.wrapping_add(1));
            }
        }
    }

    fn completion_route(&mut self, token: OpToken, raw: RawCompletion) -> CompletionRoute {
        match route_checked_slot_completion(raw, self.ops.checked_slot_view(token)) {
            RoutedSlotCompletion::Waiting(_) => CompletionRoute::Waiting,
            RoutedSlotCompletion::Orphaned(_) => CompletionRoute::Orphaned,
            RoutedSlotCompletion::Missing(anomaly) => CompletionRoute::Missing(anomaly),
            RoutedSlotCompletion::Empty(anomaly) => CompletionRoute::Empty(anomaly),
            RoutedSlotCompletion::Stale(anomaly) => CompletionRoute::Stale(anomaly),
            RoutedSlotCompletion::Corrupt(anomaly) => CompletionRoute::Corrupt(anomaly),
        }
    }

    #[inline]
    pub(super) fn with_inflight_slot<R>(
        ops: &mut IocpOpRegistry,
        token: OpToken,
        f: impl FnOnce(Slot<'_, InFlightWaiting>) -> R,
    ) -> Option<R> {
        match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => Some(f(slot)),
            _ => None,
        }
    }

    fn calculate_io_result(
        &mut self,
        token: OpToken,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) -> IocpResult<usize> {
        let user_data = token.index();
        let mut io_result = if !success {
            Err(IocpError::CompletionWait.io_report(
                "iocp.driver.calculate_io_result",
                std::io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32),
            ))
        } else {
            Ok(bytes_transferred as usize)
        };

        let processed = Self::with_inflight_slot(&mut self.ops, token, |mut guard| {
            let _ = guard.with_op_mut(|iocp_op: &mut IocpOp| {
                let blocking_res = iocp_op
                    .header
                    .blocking_completion
                    .take()
                    .and_then(|completion| completion.take_result());
                if let Some(res) = blocking_res {
                    io_result = res
                        .with_ctx("outer_scope", "iocp.driver.blocking_completion")
                        .attach_note("blocking completion returned stored error");
                } else if matches!(
                    &iocp_op.payload,
                    crate::op::IocpOpPayload::Open(_)
                        | crate::op::IocpOpPayload::Close(_)
                        | crate::op::IocpOpPayload::Fsync(_)
                        | crate::op::IocpOpPayload::FsyncRaw(_)
                        | crate::op::IocpOpPayload::SyncRange(_)
                        | crate::op::IocpOpPayload::SyncRangeRaw(_)
                        | crate::op::IocpOpPayload::Fallocate(_)
                        | crate::op::IocpOpPayload::FallocateRaw(_)
                ) {
                    io_result = Err(IocpError::CompletionWait
                        .to_report()
                        .push_ctx("scope", "iocp/driver")
                        .attach_note("missing blocking result for offloaded file completion"));
                } else if let Ok(val) = io_result {
                    io_result = iocp_op
                        .on_complete(val, &self.extensions)
                        .attach_note("IOCP completion hook failed");
                }
            });
        });

        if processed.is_none() {
            debug!(
                user_data,
                "Skipping IO result calculation for non in-flight slot"
            );
            return io_result;
        }

        io_result
    }

    pub(super) fn emit_event_inner(
        ctx: EmitContext<'_>,
        ops: &mut IocpOpRegistry,
        diagnostics: &mut DriverCompletionDiagnostics,
        token: OpToken,
        io_result: IocpResult<usize>,
    ) -> Option<RecordCompletionOutcome> {
        let user_data = token.index();
        let mut should_free = false;
        let mut sidecar_to_push = None;
        let handled = Self::with_inflight_slot(ops, token, |guard| {
            let completion_res = io_result_to_event_res(&io_result);
            let mut io_detail = Some(io_result);
            let mut guard = guard.complete();

            if guard.platform_mut().is_background {
                let _ = guard.take_op();
                let _ = guard.take_completion_data();
                let _data = std::mem::take(guard.platform_mut());
                should_free = true;
            } else {
                let cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| op.completion_cleanup(io_detail.as_ref().expect("io result present")))
                    .unwrap_or_default();
                if let Some(op) = guard.op.as_mut() {
                    op.unbind_user_payload();
                }
                let (payload, detail) = guard.take_completion_data();
                sidecar_to_push = Some(CompletionSidecar {
                    token,
                    res: completion_res,
                    flags: 0,
                    payload,
                    detail: detail.or_else(|| io_detail.take()),
                    cleanup,
                });
                let _ = guard.take_op();
                let _data = std::mem::take(guard.platform_mut());
                should_free = true;
            }
        });

        if handled.is_none() {
            debug!(user_data, "Received completion for non-active slot");
        }

        if let Some(sidecar) = sidecar_to_push {
            let outcome = push_completion_shared(
                ctx.completion_events,
                ctx.completion_table,
                diagnostics,
                completion_record(sidecar),
            );
            if handled.is_some() && should_free {
                let _ = ops.remove_token(token);
            }
            return Some(outcome);
        }

        if handled.is_some() && should_free {
            let _ = ops.remove_token(token);
        }
        None
    }
}

#[inline]
fn iocp_completion_res(success: bool, error_code: Option<u32>, bytes_transferred: u32) -> i32 {
    if success {
        bytes_transferred.min(i32::MAX as u32) as i32
    } else {
        -(error_code.unwrap_or(0).min(i32::MAX as u32) as i32)
    }
}
