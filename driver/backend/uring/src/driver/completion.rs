use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::driver::UringDriver;
use crate::error::{UringDriverResult, UringError, UringResult, uring_report_to_event_res};
use crate::op::{
    UringUserPayload,
    slot::{CheckedSlotView, SlotView, UringOpRegistryExt},
};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionCleanupGuard, CompletionDispatch,
    CompletionEvent, CompletionPacket, CompletionSidecar, CompletionToken, OpToken, RawCompletion,
    RoutedSlotCompletion, dispatch_raw_completion, drain_cancel_requests,
    record_completion_anomaly, record_lost_completion, record_user_completion,
    route_checked_slot_completion,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletionProgress {
    pub(crate) user: usize,
    pub(crate) internal: usize,
}

impl<'a> UringDriver<'a> {
    pub(crate) fn wait_internal(&mut self) -> UringResult<()> {
        drain_cancel_requests(self);
        self.flush_cancellations();
        self.flush_backlog();

        if !self.has_active_ops_internal() {
            return Ok(());
        }

        if self.ring.completion().is_empty() {
            let next_timeout = self.wheel.next_timeout();

            if let Some(duration) = next_timeout {
                let ts = io_uring::types::Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());

                let args = io_uring::types::SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {}
                    Err(e) => {
                        return Err(UringError::CompletionWait
                            .io_report("driver.wait_internal.submit_with_args", e));
                    }
                }
            } else {
                self.ring.submit_and_wait(1).map_err(|e| {
                    UringError::CompletionWait.io_report("driver.wait_internal.submit_and_wait", e)
                })?;
            }
        }

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed);
        self.last_timer_poll = now;

        let _ = self.process_completions_internal()?;
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn advance_timers(&mut self, elapsed: Duration) {
        self.wheel.advance(elapsed, &mut self.timer_buffer);

        let timer_buffer = std::mem::take(&mut self.timer_buffer);
        for token in timer_buffer {
            let sidecar = match self.ops.checked_slot_view(token) {
                CheckedSlotView::Valid(slot) => match slot {
                    SlotView::InFlightWaiting(mut slot) => {
                        slot.platform_mut().timer_id = None;
                        let mut completed = slot.complete();

                        let _ = completed.take_op();
                        let (payload, detail) = completed.take_completion_data();

                        Some(CompletionSidecar::<UringUserPayload, UringError> {
                            token,
                            res: 0,
                            flags: 0,
                            payload,
                            detail,
                            cleanup: CompletionCleanupGuard::default(),
                        })
                    }
                    _ => None,
                },
                CheckedSlotView::Missing {
                    index,
                    expected_generation,
                } => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Uring,
                        CompletionToken::user(token),
                        0,
                        0,
                    );
                    let anomaly =
                        CompletionAnomaly::unknown_slot(raw.token, index, expected_generation)
                            .with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                    None
                }
                CheckedSlotView::Empty(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Uring,
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
                    None
                }
                CheckedSlotView::Stale(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Uring,
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
                    None
                }
                CheckedSlotView::Corrupt(snapshot) => {
                    let raw = RawCompletion::new(
                        CompletionBackend::Uring,
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
                    self.emit_corrupt_completion(anomaly, "timer found corrupt slot");
                    None
                }
            };

            if let Some(sidecar) = sidecar {
                self.push_completion_event(sidecar);
                let _ = self.ops.remove_token(token);
            }
        }
    }

    pub(crate) fn poll_nonblocking_internal(&mut self) -> UringResult<()> {
        drain_cancel_requests(self);
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()?;
        let _ = self.process_completions_internal()?;

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed);
        self.last_timer_poll = now;

        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn process_completions_internal(&mut self) -> UringResult<CompletionProgress> {
        // DEFER_TASKRUN needs a GETEVENTS enter to trigger deferred task work.
        let _ = unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
        };

        let mut cqes = Vec::new();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());
            for cqe in cqe_kicker {
                cqes.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        let mut progress = CompletionProgress::default();
        for (raw_token, cqe_res, cqe_flags) in cqes {
            match dispatch_raw_completion(CompletionBackend::Uring, raw_token, cqe_res, cqe_flags) {
                CompletionDispatch::User { token, raw } => {
                    progress.user += self.handle_user_completion(token, raw);
                }
                CompletionDispatch::Waker { raw, .. } => {
                    progress.internal += 1;
                    self.handle_waker_completion(raw.res)?;
                }
                CompletionDispatch::Cancel { id, raw } => {
                    progress.internal += 1;
                    self.handle_cancel_completion(id, raw.res);
                }
                CompletionDispatch::RioWake { raw, .. } | CompletionDispatch::Unknown { raw } => {
                    let anomaly =
                        CompletionAnomaly::unknown_control(raw.token).with_raw_completion(raw);
                    record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                    debug!(
                        token = raw.token.raw(),
                        res = raw.res,
                        flags = raw.flags,
                        "unknown uring completion token"
                    );
                }
            }
        }

        Ok(progress)
    }

    fn handle_user_completion(&mut self, token: OpToken, raw: RawCompletion) -> usize {
        let (user_data, generation) = token.parts();
        match route_checked_slot_completion(raw, self.ops.checked_slot_view(token)) {
            RoutedSlotCompletion::Waiting(slot) => {
                let sidecar = complete_waiting_slot(slot, token, raw.res, raw.flags);
                self.push_completion_event(sidecar);
                let _ = self.ops.remove_token(token);
                1
            }
            RoutedSlotCompletion::Orphaned(slot) => {
                let sidecar = complete_orphaned_slot(slot, token, raw.res, raw.flags);
                self.push_completion_event(sidecar);
                let _ = self.ops.remove_token(token);
                1
            }
            RoutedSlotCompletion::Corrupt(anomaly) => {
                self.emit_corrupt_completion(anomaly, "corrupt slot");
                0
            }
            RoutedSlotCompletion::Missing(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(user_data, generation, "completion for missing slot");
                0
            }
            RoutedSlotCompletion::Empty(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation,
                    state = ?anomaly.state,
                    "completion for non-active slot"
                );
                0
            }
            RoutedSlotCompletion::Stale(anomaly) => {
                record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation,
                    actual_generation = anomaly.actual_generation,
                    state = ?anomaly.state,
                    "stale uring completion"
                );
                0
            }
        }
    }

    fn handle_cancel_completion(&mut self, cancel_id: u16, cqe_res: i32) {
        let request = self.pending_cancel_cqes.remove(&cancel_id);
        match cqe_res {
            value if value >= 0 => {
                self.completion_diagnostics.inc_cancel_ack_ok();
                trace!(
                    cancel_id,
                    ?request,
                    result = value,
                    "async cancel completed"
                );
            }
            value if value == -libc::ENOENT => {
                self.completion_diagnostics.inc_cancel_ack_not_found();
                debug!(
                    cancel_id,
                    ?request,
                    "async cancel target was already complete or absent"
                );
            }
            value => {
                self.completion_diagnostics.inc_cancel_ack_error();
                warn!(
                    cancel_id,
                    ?request,
                    result = value,
                    errno = -value,
                    "async cancel request failed"
                );
            }
        }
    }

    fn handle_waker_completion(&mut self, cqe_res: i32) -> UringResult<()> {
        if cqe_res >= 0 {
            self.completion_diagnostics.inc_waker_ok();
        } else {
            self.completion_diagnostics.inc_waker_error();
            match -cqe_res {
                libc::EAGAIN | libc::EINTR => {
                    debug!(res = cqe_res, "recoverable eventfd waker read completion");
                }
                errno => {
                    warn!(res = cqe_res, errno, "eventfd waker read failed");
                }
            }
        }

        self.is_waked.store(false, Ordering::Release);
        if let Some(token) = self.waker_token.take() {
            let _ = self.ops.remove_token(token);
        }
        if let Err(e) = self.submit_waker() {
            self.completion_diagnostics.inc_waker_rebuild();
            error!(report = ?e, "failed to resubmit waker");
            return Err(e);
        }
        if cqe_res < 0 {
            self.completion_diagnostics.inc_waker_rebuild();
        }
        self.flush_backlog();
        Ok(())
    }

    fn emit_corrupt_completion(&mut self, anomaly: CompletionAnomaly, note: &'static str) {
        let Some(snapshot) = anomaly.slot_snapshot else {
            record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
            return;
        };
        let raw_res = anomaly.raw_result.unwrap_or(-libc::EIO);
        let flags = anomaly.flags.unwrap_or(0);
        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
        error!(
            user_data = snapshot.index,
            generation = snapshot.generation,
            state = ?snapshot.state,
            has_op = snapshot.has_op,
            has_payload = snapshot.has_payload,
            raw_res,
            "uring completion found corrupt slot"
        );

        let (payload, detail) = self
            .ops
            .with_slot_storage_mut_token(
                OpToken::new(snapshot.index, snapshot.generation),
                |result, payload, _sidecar| (payload.take(), result.take()),
            )
            .unwrap_or((None, None));
        let mut cleanup = CompletionCleanupGuard::default();
        if let Some((_, _, op, _)) =
            self.ops
                .get_slot_entry_op_storage_and_entry_mut_token(OpToken::new(
                    snapshot.index,
                    snapshot.generation,
                ))
        {
            if let Some(op_ref) = op.as_mut() {
                cleanup = unsafe { (op_ref.vtable.completion_cleanup)(op_ref, raw_res) };
            }
            let _ = op.take();
        }

        let detail = detail.or_else(|| {
            Some(Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "uring.driver.completion")
                .with_ctx("user_data", snapshot.index)
                .with_ctx("generation", snapshot.generation)
                .with_ctx("slot_state", format!("{:?}", snapshot.state))
                .with_ctx("has_op", snapshot.has_op)
                .with_ctx("has_payload", snapshot.has_payload)
                .attach_note(note)))
        });
        drop(payload);
        drop(detail);

        let event = CompletionEvent {
            token: anomaly.token,
            res: -libc::EIO,
            flags,
        };
        let _ = record_lost_completion(
            &self.completion_events,
            &self.completion_table,
            &mut self.completion_diagnostics,
            event,
            anomaly,
            cleanup,
        );
        if let Some(token) = event.token.op_token() {
            let _ = self.ops.remove_token(token);
        }
    }

    pub(crate) fn push_completion_event(
        &mut self,
        sidecar: CompletionSidecar<UringUserPayload, UringError>,
    ) {
        let packet = CompletionPacket::from(sidecar);
        let _ = record_user_completion(
            &self.completion_events,
            &self.completion_table,
            &mut self.completion_diagnostics,
            packet,
        );
    }
}

fn complete_waiting_slot(
    slot: crate::op::slot::Slot<'_, veloq_driver_core::slot::InFlightWaiting>,
    token: OpToken,
    cqe_res: i32,
    cqe_flags: u32,
) -> CompletionSidecar<UringUserPayload, UringError> {
    let user_data = token.index();
    let generation = slot.entry.generation(Ordering::Acquire);
    let has_op = slot.op.is_some();
    let has_payload = slot.storage.payload.is_some();
    if !has_op || !has_payload {
        let cleanup = slot
            .op
            .as_mut()
            .map(|op| unsafe { (op.vtable.completion_cleanup)(op, cqe_res) })
            .unwrap_or_default();
        let (payload, detail) = slot
            .storage
            .with_mut(|result, payload, _sidecar| (payload.take(), result.take()));
        let _ = slot.op.take();
        return CompletionSidecar::<UringUserPayload, UringError> {
            token,
            res: -libc::EIO,
            flags: cqe_flags,
            payload,
            detail: detail.or_else(|| {
                Some(Err(UringError::InvalidState
                    .to_report()
                    .push_ctx("scope", "uring.driver.completion")
                    .with_ctx("user_data", user_data)
                    .with_ctx("generation", generation)
                    .with_ctx("has_op", has_op)
                    .with_ctx("has_payload", has_payload)
                    .attach_note(
                        "in-flight uring completion missing op or payload",
                    )))
            }),
            cleanup,
        };
    }

    let (final_res, cleanup) = {
        let Some(payload) = slot.storage.payload.as_mut() else {
            unreachable!("payload presence checked above");
        };
        let Some(op) = slot.op.as_mut() else {
            unreachable!("op presence checked above");
        };
        let final_res = unsafe { (op.vtable.on_complete)(op, payload, cqe_res) };
        let cleanup = unsafe { (op.vtable.completion_cleanup)(op, cqe_res) };
        (final_res, cleanup)
    };

    let mut completed = slot.complete();
    let res_code = driver_result_to_event_res(&final_res);

    let (payload, mut detail) = completed.take_completion_data();
    if detail.is_none()
        && let Err(err) = final_res
    {
        detail = Some(Err(err));
    }
    let _ = completed.take_op();

    CompletionSidecar::<UringUserPayload, UringError> {
        token,
        res: res_code,
        flags: cqe_flags,
        payload,
        detail,
        cleanup,
    }
}

fn complete_orphaned_slot(
    slot: crate::op::slot::Slot<'_, veloq_driver_core::slot::InFlightOrphaned>,
    token: OpToken,
    cqe_res: i32,
    cqe_flags: u32,
) -> CompletionSidecar<UringUserPayload, UringError> {
    let mut completed = slot.complete();
    let generation = completed.entry.generation(Ordering::Acquire);
    let cleanup = completed
        .op
        .as_mut()
        .map(|op| unsafe { (op.vtable.completion_cleanup)(op, cqe_res) })
        .unwrap_or_default();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();

    CompletionSidecar::<UringUserPayload, UringError> {
        token: OpToken::new(token.index(), generation),
        res: cqe_res,
        flags: cqe_flags,
        payload,
        detail,
        cleanup,
    }
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}
