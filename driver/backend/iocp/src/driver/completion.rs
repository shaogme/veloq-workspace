use std::time::Instant;

use diagweave::prelude::*;
use tracing::{debug, error};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionCleanupGuard,
    CompletionToken, DriverCompletionDiagnostics, OpToken, RawCompletion, RecordCompletionOutcome,
    RoutedSlotCompletion, UserCompletionEvent, record_completion_anomaly, record_lost_completion,
    route_user_completion, run_completion_cleanup, slot_view_anomaly,
};
use veloq_driver_core::slot::{CheckedSlotView, InFlightWaiting, SlotRegistryExt, SlotView};

use crate::common::{completion_record, io_result_to_event_res, push_completion_shared};
use crate::driver::polling::CompletionProgress;
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::{IocpError, IocpResult};
use crate::op::{IocpOp, IocpUserPayload, Slot};
use crate::rio::SocketInflightToken;

#[derive(Clone, Copy)]
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
                CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let snapshot = slot.snapshot();
                    let anomaly = CompletionAnomaly::backend_invariant_broken(
                        raw.token,
                        snapshot.index,
                        snapshot.generation,
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                }
                view @ (CheckedSlotView::Missing { .. }
                | CheckedSlotView::Empty(_)
                | CheckedSlotView::Stale(_)
                | CheckedSlotView::Corrupt(_)) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly = match slot_view_anomaly(CompletionBackend::Iocp, token, raw, view)
                    {
                        Ok(_) => continue,
                        Err(anomaly) => anomaly,
                    };
                    if matches!(
                        anomaly.reason,
                        CompletionAnomalyReason::OpMissing
                            | CompletionAnomalyReason::PayloadMissing
                            | CompletionAnomalyReason::SlotCorruption
                    ) {
                        self.emit_corrupt_completion(anomaly, "IOCP timer found corrupt slot");
                    } else {
                        record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                    }
                }
            }
        }
        let mut finished_timers = Vec::new();
        let emit_ctx = EmitContext {
            completion_table: self.completion.table(),
        };
        for token in expired {
            if let Some(finish) = Self::finish_timer_op(
                emit_ctx,
                &mut self.ops,
                &mut self.completion_diagnostics,
                token,
                &mut pending_events,
            ) {
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
        ctx: EmitContext<'_>,
        ops: &mut IocpOpRegistry,
        diagnostics: &mut DriverCompletionDiagnostics,
        token: OpToken,
        pending_events: &mut Vec<CompletionSidecar>,
    ) -> Option<TimerFinish> {
        match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let mut guard = slot.complete();
                let io_result: IocpResult<usize> = Ok(0);
                let snapshot = guard.snapshot();
                let cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| op.completion_cleanup(&io_result))
                    .unwrap_or_default();
                let _ = guard.take_op();
                let (payload_erased, detail) = guard.take_completion_data();
                if let Some(payload_erased) = payload_erased {
                    pending_events.push(CompletionSidecar::new(
                        UserCompletionEvent::from_parts(CompletionBackend::Iocp, token, 0, 0),
                        payload_erased,
                        detail,
                        cleanup,
                    ));
                    Some(TimerFinish::WaitingCompleted)
                } else {
                    drop(detail);
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly =
                        veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                            .with_slot_snapshot(snapshot)
                            .with_raw_completion(raw);
                    let _ = record_lost_completion(
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
                    );
                    let _ = ops.finalize_corrupt_slot(snapshot);
                    None
                }
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
                let mut guard = slot.complete();
                let io_result: IocpResult<usize> = Ok(0);
                let mut cleanup = guard
                    .op
                    .as_mut()
                    .map(|op| op.orphan_cleanup(&io_result))
                    .unwrap_or_default();
                let _ = guard.take_op();
                let (payload_erased, detail) = guard.take_completion_data();
                drop(payload_erased);
                drop(detail);
                let _ = run_completion_cleanup(diagnostics, &mut cleanup);
                Some(TimerFinish::OrphanedDropped)
            }
            _ => None,
        }
    }

    pub(super) fn process_completion(
        &mut self,
        event: UserCompletionEvent,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) -> CompletionProgress {
        let token = event.token();
        let (user_data, completed_generation) = token.parts();

        match route_user_completion(event, self.ops.checked_slot_view(token)) {
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
                let emit_outcome = Self::emit_event_from_slot(
                    ctx,
                    &mut self.completion_diagnostics,
                    token,
                    slot,
                    io_result,
                );
                let user_lost = matches!(emit_outcome, Some(RecordCompletionOutcome::RecordedLost));
                if let Some(socket_inflight) = socket_inflight {
                    self.rio
                        .state_mut()
                        .release_socket_inflight_token(socket_inflight);
                    self.drain_deferred_socket_cleanup();
                }
                let _ = self.ops.finalize_waiting_completion(token);
                CompletionProgress {
                    iocp: 1,
                    user_completed: usize::from(!user_lost),
                    user_lost: usize::from(user_lost),
                    ..CompletionProgress::default()
                }
            }
            RoutedSlotCompletion::Orphaned(slot) => {
                let (mut cleanup, socket_inflight) = {
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
                        .map(|op| op.orphan_cleanup(&io_result))
                        .unwrap_or_default();
                    let socket_inflight =
                        completed.op.as_mut().and_then(take_socket_inflight_from_op);
                    let _ = completed.take_op();
                    let _ = completed.take_completion_data();
                    (cleanup, socket_inflight)
                };
                let _ = run_completion_cleanup(&self.completion_diagnostics, &mut cleanup);
                if let Some(socket_inflight) = socket_inflight {
                    self.rio
                        .state_mut()
                        .release_socket_inflight_token(socket_inflight);
                    self.drain_deferred_socket_cleanup();
                }
                let _ = self.ops.finalize_orphaned_completion(token);
                CompletionProgress {
                    iocp: 1,
                    orphan_cleaned: 1,
                    ..CompletionProgress::default()
                }
            }
            RoutedSlotCompletion::Missing(anomaly) | RoutedSlotCompletion::Empty(anomaly) => {
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring completion for non-active slot"
                );
                CompletionProgress {
                    iocp: 1,
                    anomaly: 1,
                    ..CompletionProgress::default()
                }
            }
            RoutedSlotCompletion::Stale(anomaly) => {
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    completed_generation, "ignoring stale IOCP completion"
                );
                CompletionProgress {
                    iocp: 1,
                    anomaly: 1,
                    ..CompletionProgress::default()
                }
            }
            RoutedSlotCompletion::Corrupt(anomaly) => {
                self.emit_corrupt_completion(anomaly, "IOCP completion found corrupt slot");
                CompletionProgress {
                    iocp: 1,
                    user_lost: 1,
                    ..CompletionProgress::default()
                }
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
        let mut lost_outcome = None;
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
                if let Some(payload) = payload {
                    sidecar_to_push = Some(CompletionSidecar::new(
                        UserCompletionEvent::from_parts(
                            CompletionBackend::Iocp,
                            token,
                            completion_res,
                            0,
                        ),
                        payload,
                        detail.or_else(|| io_detail.take()),
                        cleanup,
                    ));
                    let _ = guard.take_op();
                    let _data = std::mem::take(guard.platform_mut());
                } else {
                    drop(detail);
                    let snapshot = guard.snapshot();
                    let raw = RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(token),
                        completion_res,
                        0,
                    );
                    let anomaly =
                        veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                            .with_slot_snapshot(snapshot)
                            .with_raw_completion(raw);
                    lost_outcome = Some(record_lost_completion(
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
                    ));
                    let _ = guard.take_op();
                    let _data = std::mem::take(guard.platform_mut());
                }
            }
        }

        if let Some(outcome) = lost_outcome {
            return Some(outcome);
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
            record_completion_anomaly(&self.completion_diagnostics, &anomaly);
            return;
        };
        let Ok(token) = snapshot.try_token() else {
            record_completion_anomaly(&self.completion_diagnostics, &anomaly);
            return;
        };
        let raw_res = anomaly.raw_result.unwrap_or(-5);
        let flags = anomaly.flags.unwrap_or(0);

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
            .active_slot_bundle_mut(token)
            .map(|(_, _, op, _)| {
                let cleanup = op
                    .as_mut()
                    .map(|op| {
                        if snapshot.state == veloq_driver_core::slot::SlotState::InFlightOrphaned {
                            op.orphan_cleanup(&lost_result)
                        } else {
                            op.completion_cleanup(&lost_result)
                        }
                    })
                    .unwrap_or_default();
                let _ = op.take();
                cleanup
            })
            .unwrap_or_default();

        let _ = self
            .ops
            .with_slot_storage_mut(token, |result, payload, _sidecar| {
                let _ = result.take();
                let _ = payload.take();
            });

        let event = UserCompletionEvent::from_parts(CompletionBackend::Iocp, token, -5, flags);
        let _ = record_lost_completion(
            self.completion.table(),
            &self.completion_diagnostics,
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
