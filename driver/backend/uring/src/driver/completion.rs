use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::driver::UringDriver;
use crate::error::{UringDriverResult, UringError, UringResult, uring_report_to_event_res};
use crate::op::{
    UringUserPayload,
    slot::{CheckedSlotView, SlotSnapshot, SlotView, UringOpRegistryExt},
};
use veloq_driver_core::driver::{
    CompletionControlKind, CompletionEvent, CompletionSidecar, CompletionToken,
    CompletionTokenClass, OpToken, drain_cancel_requests,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletionProgress {
    pub(crate) user: usize,
    pub(crate) internal: usize,
}

enum UringCompletionKind {
    User { token: OpToken },
    Waker,
    Cancel { id: u16 },
    Unknown { token: CompletionToken },
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
            let user_data = token.index();
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
                        })
                    }
                    _ => None,
                },
                CheckedSlotView::Missing { .. } | CheckedSlotView::Empty(_) => {
                    self.completion_diagnostics.inc_unknown_completion();
                    None
                }
                CheckedSlotView::Stale(_) => {
                    self.completion_diagnostics.inc_stale_completion();
                    None
                }
                CheckedSlotView::Corrupt(snapshot) => {
                    self.emit_corrupt_completion(snapshot, 0, 0, "timer found corrupt slot");
                    None
                }
            };

            if let Some(sidecar) = sidecar {
                self.push_completion_event(sidecar);
                self.ops.remove(user_data);
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
            match classify_completion(raw_token) {
                UringCompletionKind::User { token } => {
                    progress.user += self.handle_user_completion(token, cqe_res, cqe_flags);
                }
                UringCompletionKind::Waker => {
                    progress.internal += 1;
                    self.handle_waker_completion(cqe_res)?;
                }
                UringCompletionKind::Cancel { id } => {
                    progress.internal += 1;
                    self.handle_cancel_completion(id, cqe_res);
                }
                UringCompletionKind::Unknown { token } => {
                    self.completion_diagnostics.inc_unknown_completion();
                    debug!(
                        token = token.raw(),
                        res = cqe_res,
                        flags = cqe_flags,
                        "unknown uring completion token"
                    );
                }
            }
        }

        Ok(progress)
    }

    fn handle_user_completion(&mut self, token: OpToken, cqe_res: i32, cqe_flags: u32) -> usize {
        let (user_data, generation) = token.parts();
        match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let sidecar = complete_waiting_slot(slot, token, cqe_res, cqe_flags);
                self.push_completion_event(sidecar);
                self.ops.remove(user_data);
                1
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
                let sidecar = complete_orphaned_slot(slot, token, cqe_res, cqe_flags);
                self.push_completion_event(sidecar);
                self.ops.remove(user_data);
                1
            }
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                let snapshot = SlotSnapshot {
                    index: user_data,
                    generation,
                    state: veloq_driver_core::slot::SlotState::Reserved,
                    has_op: slot.op.is_some(),
                    has_payload: slot.storage.payload.is_some(),
                };
                drop(slot);
                self.emit_corrupt_completion(snapshot, cqe_res, cqe_flags, "reserved slot");
                0
            }
            CheckedSlotView::Missing { .. } => {
                self.completion_diagnostics.inc_unknown_completion();
                debug!(user_data, generation, "completion for missing slot");
                0
            }
            CheckedSlotView::Empty(snapshot) => {
                self.completion_diagnostics.inc_unknown_completion();
                debug!(
                    user_data,
                    generation,
                    state = ?snapshot.state,
                    "completion for non-active slot"
                );
                0
            }
            CheckedSlotView::Stale(snapshot) => {
                self.completion_diagnostics.inc_stale_completion();
                debug!(
                    user_data,
                    generation,
                    actual_generation = snapshot.generation,
                    state = ?snapshot.state,
                    "stale uring completion"
                );
                0
            }
            CheckedSlotView::Corrupt(snapshot) => {
                self.emit_corrupt_completion(snapshot, cqe_res, cqe_flags, "corrupt slot");
                0
            }
        }
    }

    fn handle_cancel_completion(&mut self, cancel_id: u16, cqe_res: i32) {
        let request = self.pending_cancel_cqes.remove(&cancel_id);
        match cqe_res {
            value if value >= 0 => {
                self.completion_diagnostics.inc_cancel_cqe_ok();
                trace!(
                    cancel_id,
                    ?request,
                    result = value,
                    "async cancel completed"
                );
            }
            value if value == -libc::ENOENT => {
                self.completion_diagnostics.inc_cancel_cqe_enoent();
                debug!(
                    cancel_id,
                    ?request,
                    "async cancel target was already complete or absent"
                );
            }
            value => {
                self.completion_diagnostics.inc_cancel_cqe_error();
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
            self.ops.remove(token.index());
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

    fn emit_corrupt_completion(
        &mut self,
        snapshot: SlotSnapshot,
        raw_res: i32,
        flags: u32,
        note: &'static str,
    ) {
        self.completion_diagnostics.inc_slot_corruption();
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
            .with_slot_storage_mut(snapshot.index, |result, payload, _sidecar| {
                (payload.take(), result.take())
            })
            .unwrap_or((None, None));
        if let Some((_, _, op, _)) = self
            .ops
            .get_slot_entry_op_storage_and_entry_mut(snapshot.index)
        {
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
        self.push_completion_event(CompletionSidecar::<UringUserPayload, UringError> {
            token: OpToken::new(snapshot.index, snapshot.generation),
            res: -libc::EIO,
            flags,
            payload,
            detail,
        });
        let _ = self
            .ops
            .recycle_if_active(snapshot.index, snapshot.generation.wrapping_add(1));
    }

    pub(crate) fn push_completion_event(
        &mut self,
        sidecar: CompletionSidecar<UringUserPayload, UringError>,
    ) {
        let event = CompletionEvent {
            token: CompletionToken::user(sidecar.token),
            res: sidecar.res,
            flags: sidecar.flags,
        };
        let outcome = self.completion_table.record_completion_with_data(
            event,
            sidecar.payload,
            sidecar.detail,
        );
        self.completion_diagnostics
            .record_completion_outcome(&outcome);
        self.completion_events.push(event);
    }
}

fn classify_completion(raw: u64) -> UringCompletionKind {
    let token = CompletionToken::from_raw(raw);
    match token.classify() {
        CompletionTokenClass::User(token) => UringCompletionKind::User { token },
        CompletionTokenClass::Control {
            kind: CompletionControlKind::Waker,
            ..
        } => UringCompletionKind::Waker,
        CompletionTokenClass::Control {
            kind: CompletionControlKind::Cancel,
            id,
        } => UringCompletionKind::Cancel { id },
        CompletionTokenClass::Control { .. } => UringCompletionKind::Unknown { token },
        CompletionTokenClass::UnknownControl { .. } => UringCompletionKind::Unknown { token },
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
        };
    }

    let final_res = {
        let Some(payload) = slot.storage.payload.as_mut() else {
            unreachable!("payload presence checked above");
        };
        let Some(op) = slot.op.as_mut() else {
            unreachable!("op presence checked above");
        };
        unsafe { (op.vtable.on_complete)(op, payload, cqe_res) }
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
    if let (Some(op), Some(payload)) = (completed.op.as_mut(), completed.storage.payload.as_mut()) {
        unsafe { (op.vtable.orphan_cleanup)(op, payload, cqe_res) };
    }
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();

    CompletionSidecar::<UringUserPayload, UringError> {
        token: OpToken::new(token.index(), generation),
        res: cqe_res,
        flags: cqe_flags,
        payload,
        detail,
    }
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}
