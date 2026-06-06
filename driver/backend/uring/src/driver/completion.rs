use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{error, trace};

use crate::driver::UringDriver;
use crate::error::{UringDriverResult, UringError, UringResult};
use crate::op::{
    UringUserPayload,
    slot::{SlotView, UringOpRegistryExt},
};
use veloq_driver_core::driver::{
    CompletionEvent, CompletionSidecar, drain_cancel_requests, encode_completion_token,
};
use veloq_driver_core::driver_report_to_event_res;

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

        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn advance_timers(&mut self, elapsed: Duration) {
        self.wheel.advance(elapsed, &mut self.timer_buffer);

        let timer_buffer = std::mem::take(&mut self.timer_buffer);
        for user_data in timer_buffer {
            let sidecar = self.ops.slot_view(user_data).and_then(|slot| match slot {
                SlotView::InFlightWaiting(mut slot) => {
                    slot.platform_mut().timer_id = None;
                    let mut completed = slot.complete();

                    let generation = completed.entry.generation(Ordering::Acquire);
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();

                    Some(CompletionSidecar::<UringUserPayload, UringError> {
                        user_data,
                        generation,
                        res: 0,
                        flags: 0,
                        payload,
                        detail,
                    })
                }
                _ => None,
            });

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
        self.process_completions_internal();

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed);
        self.last_timer_poll = now;

        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn process_completions_internal(&mut self) {
        // If we use DEFER_TASKRUN, we need to enter the kernel with GETEVENTS to trigger task runs
        // even if we don't want to wait for new events.
        // We do this unconditionally as it is safe even without DEFER_TASKRUN.
        let _ = unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
        };

        let mut needs_waker_resubmit = false;
        let mut pending_events: Vec<CompletionSidecar<UringUserPayload, UringError>> = Vec::new();

        let mut cqes = Vec::new();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());
            for cqe in cqe_kicker {
                cqes.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        for (cqe_user_data, cqe_res, cqe_flags) in cqes {
            let user_data = cqe_user_data as usize;

            if user_data == u64::MAX as usize
                || user_data == crate::driver::submission::CANCEL_USER_DATA as usize
            {
                continue;
            }

            if Some(user_data) == self.waker_token {
                needs_waker_resubmit = true;
                continue;
            }

            let sidecar = self.ops.slot_view(user_data).and_then(|slot| match slot {
                SlotView::InFlightWaiting(slot) => {
                    let res_val = cqe_res;
                    let final_res = {
                        let payload = slot.storage.payload.as_mut().unwrap();
                        let op = slot.op.as_mut().unwrap();
                        unsafe { (op.vtable.on_complete)(op, payload, res_val) }
                    };

                    let mut completed = slot.complete();
                    let generation = completed.entry.generation(Ordering::Acquire);
                    let res_code = driver_result_to_event_res(&final_res);

                    let (payload, mut detail) = completed.take_completion_data();
                    if detail.is_none()
                        && let Err(err) = final_res
                    {
                        detail = Some(Err(err));
                    }
                    let _ = completed.take_op();

                    Some(CompletionSidecar::<UringUserPayload, UringError> {
                        user_data,
                        generation,
                        res: res_code,
                        flags: cqe_flags,
                        payload,
                        detail,
                    })
                }
                SlotView::InFlightOrphaned(slot) => {
                    let generation = slot.entry.generation(Ordering::Acquire);
                    let mut completed = slot.complete();
                    let (payload, detail) = completed.take_completion_data();
                    let _ = completed.take_op();

                    Some(CompletionSidecar::<UringUserPayload, UringError> {
                        user_data,
                        generation,
                        res: cqe_res,
                        flags: cqe_flags,
                        payload,
                        detail,
                    })
                }
                _ => None,
            });

            if let Some(sidecar) = sidecar {
                pending_events.push(sidecar);
                self.ops.remove(user_data);
            }
        }

        for sidecar in pending_events {
            self.push_completion_event(sidecar);
        }

        if needs_waker_resubmit {
            self.is_waked.store(false, Ordering::Release);
            if let Some(token) = self.waker_token.take() {
                self.ops.remove(token);
            }
            if let Err(e) = self.submit_waker() {
                error!(report = ?e, "failed to resubmit waker");
            }
            self.flush_backlog();
        }
    }

    pub(crate) fn push_completion_event(
        &mut self,
        sidecar: CompletionSidecar<UringUserPayload, UringError>,
    ) {
        let token = encode_completion_token(sidecar.user_data, sidecar.generation);
        let event = CompletionEvent {
            user_data: token,
            res: sidecar.res,
            flags: sidecar.flags,
        };
        self.completion_table
            .record_completion_with_data(event, sidecar.payload, sidecar.detail);
        self.completion_events.push(event);
    }
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => driver_report_to_event_res(e),
    }
}
