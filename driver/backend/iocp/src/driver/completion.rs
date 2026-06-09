use std::time::Instant;

use diagweave::prelude::*;
use tracing::{debug, error};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionCleanupGuard, CompletionEvent, CompletionToken,
    DriverCompletionDiagnostics, OpToken, RawCompletion, RecordCompletionOutcome,
    RoutedSlotCompletion, record_completion_anomaly, record_lost_completion, route_user_completion,
    run_completion_cleanup,
};
use veloq_driver_core::slot::{CheckedSlotView, InFlightWaiting, SlotRegistryExt, SlotView};

use crate::common::{completion_record, io_result_to_event_res, push_completion_shared};
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::{IocpError, IocpResult};
use crate::op::{IocpOp, IocpUserPayload, Slot};
use crate::rio::SocketInflightToken;

pub(super) struct EmitContext<'a> {
    pub(super) completion_table:
        &'a veloq_driver_core::driver::SharedCompletionTable<IocpUserPayload, IocpError>,
}

enum TimerFinish {
    WaitingCompleted,
    OrphanedDropped,
}

impl<'a> IocpDriver<'a> {
    pub(super) fn process_timers(&mut self) {
        let timer_buffer = self.timer.take_buffer();
        let mut pending_events: Vec<CompletionSidecar> = Vec::new();
        let now = Instant::now();

        let mut expired = Vec::new();
        for &token in &timer_buffer {
            match self.ops.checked_slot_view(token) {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                    if let Some(deadline) = slot.platform().timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        let timer_id = self.timer.insert(token, remain);
                        slot.platform_mut().timer_id = Some(timer_id);
                        continue;
                    }
                    expired.push(token);
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                    if let Some(deadline) = slot.platform().timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        let timer_id = self.timer.insert(token, remain);
                        slot.platform_mut().timer_id = Some(timer_id);
                        continue;
                    }
                    expired.push(token);
                }
                CheckedSlotView::Valid(SlotView::Reserved(_)) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly = CompletionAnomaly::backend_invariant_broken(
                        raw.token,
                        token.index(),
                        token.generation(),
                        veloq_driver_core::slot::SlotState::Reserved,
                    )
                    .with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                }
                CheckedSlotView::Missing {
                    index,
                    expected_generation,
                } => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly =
                        CompletionAnomaly::unknown_slot(raw.token, index, expected_generation)
                            .with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                }
                CheckedSlotView::Empty(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly = CompletionAnomaly::non_active(
                        raw.token,
                        snapshot.index,
                        token.generation(),
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                }
                CheckedSlotView::Stale(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly = CompletionAnomaly::stale(
                        raw.token,
                        snapshot.index,
                        token.generation(),
                        snapshot.generation,
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                }
                CheckedSlotView::Corrupt(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly = CompletionAnomaly::corrupt(
                        raw.token,
                        snapshot.index,
                        snapshot.generation,
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    self.emit_corrupt_completion(anomaly, "IOCP timer found corrupt slot");
                }
            }
        }
        let mut finished_timers = Vec::new();
        for token in expired {
            if let Some(finish) = Self::finish_timer_op(&mut self.ops, token, &mut pending_events) {
                finished_timers.push((token, finish));
            }
        }

        for completion in pending_events {
            let outcome = push_completion_shared(
                self.completion.table(),
                &mut self.completion_diagnostics,
                completion_record(completion),
            );
            let _ = outcome;
        }
        for (token, finish) in finished_timers {
            match finish {
                TimerFinish::WaitingCompleted => {
                    let _ = self.ops.finalize_waiting_completion(token);
                }
                TimerFinish::OrphanedDropped => {
                    let _ = self.ops.finalize_orphaned_completion(token);
                }
            }
        }
        self.timer.restore_cleared_buffer(timer_buffer);
    }

    fn finish_timer_op(
        ops: &mut IocpOpRegistry,
        token: OpToken,
        pending_events: &mut Vec<CompletionSidecar>,
    ) -> Option<TimerFinish> {
        match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let mut guard = slot.complete();
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
                Some(TimerFinish::WaitingCompleted)
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
                let mut guard = slot.complete();
                let _ = guard.take_op();
                let (payload_erased, detail) = guard.take_completion_data();
                drop(payload_erased);
                drop(detail);
                Some(TimerFinish::OrphanedDropped)
            }
            _ => None,
        }
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

        match route_user_completion(token, raw, self.ops.checked_slot_view(token)) {
            RoutedSlotCompletion::Waiting(mut slot) => {
                let io_result = Self::calculate_io_result_from_slot(
                    &self.extensions,
                    &mut slot,
                    success,
                    error_code,
                    bytes_transferred,
                );
                let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                let ctx = EmitContext {
                    completion_table: self.completion.table(),
                };
                let _ = Self::emit_event_from_slot(
                    ctx,
                    &mut self.completion_diagnostics,
                    token,
                    slot,
                    io_result,
                );
                if let Some(socket_inflight) = socket_inflight {
                    self.rio
                        .state_mut()
                        .release_socket_inflight_token(socket_inflight);
                    self.drain_deferred_socket_cleanup();
                }
                let _ = self.ops.finalize_waiting_completion(token);
            }
            RoutedSlotCompletion::Orphaned(slot) => {
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
                let socket_inflight = completed.op.as_mut().and_then(take_socket_inflight_from_op);
                let _ = completed.take_op();
                let _ = completed.take_completion_data();
                drop(completed);
                let mut cleanup = cleanup;
                let _ = run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                if let Some(socket_inflight) = socket_inflight {
                    self.rio
                        .state_mut()
                        .release_socket_inflight_token(socket_inflight);
                    self.drain_deferred_socket_cleanup();
                }
                let _ = self.ops.finalize_orphaned_completion(token);
            }
            RoutedSlotCompletion::Missing(anomaly) | RoutedSlotCompletion::Empty(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring completion for non-active slot"
                );
            }
            RoutedSlotCompletion::Stale(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring stale IOCP completion"
                );
            }
            RoutedSlotCompletion::Corrupt(anomaly) => {
                self.emit_corrupt_completion(anomaly, "IOCP completion found corrupt slot");
            }
        }
    }

    fn calculate_io_result_from_slot(
        ext: &crate::ext::Extensions,
        guard: &mut Slot<'_, InFlightWaiting>,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) -> IocpResult<usize> {
        let user_data = guard.snapshot().index;
        let mut io_result = if !success {
            Err(IocpError::CompletionWait.io_report(
                "iocp.driver.calculate_io_result_from_slot",
                std::io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32),
            ))
        } else {
            Ok(bytes_transferred as usize)
        };

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
                    .with_ctx("user_data", user_data)
                    .attach_note("missing blocking result for offloaded file completion"));
            } else if let Ok(val) = io_result {
                io_result = iocp_op
                    .on_complete(val, ext)
                    .attach_note("IOCP completion hook failed");
            }
        });

        io_result
    }

    pub(super) fn emit_event_from_slot(
        ctx: EmitContext<'_>,
        diagnostics: &mut DriverCompletionDiagnostics,
        token: OpToken,
        guard: Slot<'_, InFlightWaiting>,
        io_result: IocpResult<usize>,
    ) -> Option<RecordCompletionOutcome> {
        let mut sidecar_to_push = None;
        {
            let completion_res = io_result_to_event_res(&io_result);
            let mut io_detail = Some(io_result);
            let mut guard = guard.complete();

            if guard.platform_mut().is_background {
                let _ = guard.take_op();
                let _ = guard.take_completion_data();
                let _data = std::mem::take(guard.platform_mut());
            } else {
                let cleanup = match (guard.op.as_mut(), io_detail.as_ref()) {
                    (Some(op), Some(io_result)) => op.completion_cleanup(io_result),
                    _ => CompletionCleanupGuard::default(),
                };
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
            }
        }

        if let Some(sidecar) = sidecar_to_push {
            let outcome = push_completion_shared(
                ctx.completion_table,
                diagnostics,
                completion_record(sidecar),
            );
            return Some(outcome);
        }

        None
    }

    fn emit_corrupt_completion(&mut self, anomaly: CompletionAnomaly, note: &'static str) {
        let Some(snapshot) = anomaly.slot_snapshot else {
            record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
            return;
        };
        let token = OpToken::new(snapshot.index, snapshot.generation);
        let raw_res = anomaly.raw_result.unwrap_or(-5);
        let flags = anomaly.flags.unwrap_or(0);

        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
        error!(
            user_data = snapshot.index,
            generation = snapshot.generation,
            state = ?snapshot.state,
            has_op = snapshot.has_op,
            has_payload = snapshot.has_payload,
            raw_res,
            "IOCP completion found corrupt slot"
        );

        self.release_socket_inflight_for_op(snapshot.index);

        let lost_result = Err(IocpError::InvalidState
            .to_report()
            .push_ctx("scope", "iocp.driver.completion")
            .with_ctx("user_data", snapshot.index)
            .with_ctx("generation", snapshot.generation)
            .with_ctx("slot_state", format!("{:?}", snapshot.state))
            .with_ctx("has_op", snapshot.has_op)
            .with_ctx("has_payload", snapshot.has_payload)
            .set_error_code((-raw_res).max(1))
            .attach_note(note));

        let cleanup = self
            .ops
            .get_slot_entry_op_storage_and_entry_mut_token(token)
            .and_then(|(_, _, op, _)| {
                let cleanup = op
                    .as_mut()
                    .map(|op| op.completion_cleanup(&lost_result))
                    .unwrap_or_default();
                let _ = op.take();
                Some(cleanup)
            })
            .unwrap_or_default();

        let _ = self
            .ops
            .with_slot_storage_mut_token(token, |result, payload, _sidecar| {
                let _ = result.take();
                let _ = payload.take();
            });

        let event = CompletionEvent {
            token: anomaly.token,
            res: -5,
            flags,
        };
        let _ = record_lost_completion(
            self.completion.table(),
            &mut self.completion_diagnostics,
            event,
            anomaly,
            cleanup,
        );
        let _ = self.ops.finalize_corrupt_slot(snapshot);
        self.drain_deferred_socket_cleanup();
    }
}

#[inline]
fn take_socket_inflight_from_slot(
    slot: &mut Slot<'_, InFlightWaiting>,
) -> Option<SocketInflightToken> {
    slot.op.as_mut().and_then(take_socket_inflight_from_op)
}

#[inline]
fn take_socket_inflight_from_op(op: &mut IocpOp) -> Option<SocketInflightToken> {
    if op.header.in_flight {
        op.header.in_flight = false;
    }
    op.header.socket_inflight.take()
}

#[inline]
fn iocp_completion_res(success: bool, error_code: Option<u32>, bytes_transferred: u32) -> i32 {
    if success {
        bytes_transferred.min(i32::MAX as u32) as i32
    } else {
        -(error_code.unwrap_or(0).min(i32::MAX as u32) as i32)
    }
}
