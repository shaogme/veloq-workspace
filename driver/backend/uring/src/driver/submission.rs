use diagweave::report::Report;
use std::task::Poll;
use tracing::{debug, trace};

use crate::config::{RawHandle, UringRawHandle};
use crate::driver::lifecycle::UringOpState;
use crate::driver::{UringDriver, invalid_input, invalid_state, map_uring_error, unsupported};
use crate::error::{UringError, UringResult, from_io_error};
use crate::op::slot::{Slot, SlotView, UringOpRegistryExt};
use crate::op::{SubmissionStrategy, UringOp, UringUserPayload};

use veloq_driver_core::driver::registry::{AllocResult, OpHandle};
use veloq_driver_core::driver::{Driver, Outcome, SubmitBinder, SubmitStatus};
use veloq_driver_core::op::{IntoPlatformOp, Wakeup};

pub(crate) const CANCEL_USER_DATA: u64 = u64::MAX - 1;

impl<'a> UringDriver<'a> {
    pub(crate) unsafe fn submit_from_slot_raw(
        driver: *mut UringDriver,
        user_data: usize,
        slot: Slot<'_, crate::op::slot::Reserved>,
    ) -> UringResult<bool> {
        let driver = unsafe { &mut *driver };
        let mut sub_guard = slot.start_submission_with(None);
        let strategy = sub_guard
            .slot
            .as_mut()
            .ok_or_else(|| {
                invalid_state(
                    "driver.submit_from_slot_raw",
                    "submission guard slot missing",
                )
            })?
            .op_mut()
            .vtable
            .strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [0u16; 4];
                let (count, sqe) = {
                    let driver_ptr = driver as *mut UringDriver;
                    let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                        invalid_state(
                            "driver.submit_from_slot_raw",
                            "submission guard slot missing",
                        )
                    })?;
                    let payload = slot.storage.payload.as_mut().ok_or_else(|| {
                        invalid_state(
                            "driver.submit_from_slot_raw",
                            "submission guard payload missing",
                        )
                    })?;

                    let op = slot
                        .op
                        .as_mut()
                        .expect("slot in InFlight state must contain an op");
                    let vtable = op.vtable;
                    let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
                    let sqe = unsafe {
                        (vtable.make_sqe)(op, &mut *driver_ptr, user_data)
                            .map_err(|e| {
                                UringError::Submission.attach_note(format!(
                                    "driver.submit_from_slot_raw.make_sqe: {e:#}"
                                ))
                            })?
                            .user_data(user_data as u64)
                    };
                    (count, sqe)
                };

                for &chunk_id in chunks.iter().take(count) {
                    let index = chunk_id as usize;
                    let is_registered = driver.registered_chunks.get(index).map_err(|e| {
                        invalid_state(
                            "driver.submit_from_slot_raw.bitset_get",
                            format!("BitSet get failed index={index}: {e:?}"),
                        )
                    })?;

                    if !is_registered
                        && let Some(info) = driver.registrar.resolve_chunk_info(chunk_id)
                    {
                        if let Err(e) = driver.register_chunk_internal(
                            info.id,
                            info.ptr.as_ptr(),
                            info.len.get(),
                        ) {
                            if driver.registration_mode.is_strict() {
                                return Err(e.attach_note(format!(
                                    "strict mode lazy register failed: chunk_id={chunk_id}, user_data={user_data}"
                                )));
                            }
                            return Err(e);
                        }
                    } else if !is_registered {
                        driver.registration_stats.submission_missing_chunk_info = driver
                            .registration_stats
                            .submission_missing_chunk_info
                            .saturating_add(1);
                        if driver.registration_mode.is_strict() {
                            return Err(invalid_state(
                                "driver.submit_from_slot_raw.missing_chunk_info",
                                format!(
                                    "strict mode missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                                ),
                            ));
                        }
                        return Err(invalid_input(
                            "driver.submit_from_slot_raw.missing_chunk_info",
                            format!(
                                "Missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                            ),
                        ));
                    }
                }

                if driver.push_entry(sqe) {
                    let _ = sub_guard.persist();
                    trace!(user_data, "Submitted to SQ");
                    Ok(true)
                } else {
                    debug!(user_data, "SQ full");
                    Ok(false)
                }
            }
            SubmissionStrategy::SoftwareTimer => {
                let duration_opt = {
                    let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                        invalid_state(
                            "driver.submit_from_slot_raw.timer",
                            "submission guard slot missing",
                        )
                    })?;
                    let payload = slot.storage.payload.as_mut().ok_or_else(|| {
                        invalid_state(
                            "driver.submit_from_slot_raw.timer",
                            "submission guard payload missing",
                        )
                    })?;
                    let op = slot.op.as_mut().expect("timer op missing");
                    let vtable = op.vtable;
                    unsafe { (vtable.get_timeout)(op, payload) }
                };
                if let Some(duration) = duration_opt {
                    let task_id = driver.wheel.insert(user_data, duration);
                    if let Some(entry) = driver.ops.get_mut(user_data) {
                        entry.platform_data.timer_id = Some(task_id);
                    }
                    let _ = sub_guard.persist();
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(true)
                } else {
                    Err(invalid_input(
                        "driver.submit_from_slot_raw.timer_duration",
                        "Timer duration missing",
                    ))
                }
            }
            _ => Err(unsupported(
                "driver.submit_from_slot_raw.strategy",
                "Unsupported strategy for slot submission",
            )),
        }
    }

    pub(crate) fn submit_from_slot_index(&mut self, user_data: usize) -> UringResult<bool> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(invalid_state(
                    "driver.submit_from_slot_index",
                    "op missing in slot",
                ));
            }
        };
        unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) }
    }

    pub(crate) fn submit_waker(&mut self) -> UringResult<()> {
        if self.waker_token.is_some() {
            return Ok(());
        }

        let fixed_fd = match self.waker_registered_fd {
            Some(fd) => fd,
            None => {
                let fd = self.waker_fd.fd.raw().as_fd();
                let raw = RawHandle::new(UringRawHandle::for_file(fd));
                let mut fds = self.register_files_internal(vec![
                    veloq_driver_core::driver::RegisterFd::Borrowed(raw.borrow()),
                ])?;
                let fixed_fd = fds.pop().ok_or_else(|| {
                    invalid_state("driver.submit_waker", "register_files returned empty")
                })?;
                self.waker_registered_fd = Some(fixed_fd);
                fixed_fd
            }
        };
        let op = Wakeup { fd: fixed_fd };
        let (uring_op, payload) = <Wakeup as IntoPlatformOp<UringOp>>::into_kernel_and_payload(op);

        let result = self.ops.alloc(UringOpState::new());

        if let Ok(AllocResult {
            handle: OpHandle {
                index: user_data, ..
            },
        }) = result
        {
            self.waker_token = Some(user_data);
            self.slot_set_payload(user_data, UringUserPayload::Wakeup(payload));

            let driver_ptr = self as *mut UringDriver;
            let slot = self
                .ops
                .slot_reserve(user_data)
                .init_op_with(uring_op, |_| {});
            match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
                Ok(true) => {}
                Ok(false) => self.push_backlog(user_data),
                Err(e) => return Err(e),
            }
            Ok(())
        } else {
            Err(invalid_state(
                "driver.submit_waker",
                "failed to reserve waker slot",
            ))
        }
    }

    pub(crate) fn submit_to_kernel(&mut self) -> UringResult<()> {
        trace!("submit_to_kernel entered");
        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                self.ring.submit().map_err(|e| {
                    from_io_error(
                        UringError::Submission,
                        "driver.submit_to_kernel.submit.sqpoll",
                        e,
                    )
                })?;
            }
        } else {
            let n = self.ring.submission().len();
            if n > 0 {
                // We use enter with IORING_ENTER_GETEVENTS (1) to ensure tasks are triggered even with DEFER_TASKRUN.
                unsafe {
                    self.ring
                        .submitter()
                        .enter::<()>(n as u32, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
                        .map_err(|e| {
                            from_io_error(
                                UringError::Submission,
                                "driver.submit_to_kernel.enter",
                                e,
                            )
                        })?;
                }
            }
        }
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn push_entry(&mut self, entry: io_uring::squeue::Entry) -> bool {
        trace!("Pushing SQE user_data={}", entry.get_user_data());
        let mut sq = self.ring.submission();

        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        drop(sq);
        let _ = unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
        };

        let mut sq = self.ring.submission();
        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        debug!("SQ full even after flush");
        false
    }

    pub(crate) fn submit_sqe_internal(
        &mut self,
        user_data: usize,
        op: UringOp,
        op_in: &mut Option<UringOp>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (veloq_driver_core::DriverErrorReport, SubmitStatus)>> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) | None => {
                return binder.err(
                    veloq_driver_core::driver_error(
                        veloq_driver_core::DriverErrorKind::InvalidState,
                        "uring.driver.submit_sqe_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                binder.err(
                    map_uring_error(
                        e,
                        veloq_driver_core::DriverErrorKind::Submission,
                        "uring.driver.submit_sqe_internal",
                        "submit sqe",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }

    pub(crate) fn submit_timer_internal(
        &mut self,
        user_data: usize,
        op: UringOp,
        op_in: &mut Option<UringOp>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (veloq_driver_core::DriverErrorReport, SubmitStatus)>> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) | None => {
                return binder.err(
                    veloq_driver_core::driver_error(
                        veloq_driver_core::DriverErrorKind::InvalidState,
                        "uring.driver.submit_timer_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                binder.err(
                    map_uring_error(
                        e,
                        veloq_driver_core::DriverErrorKind::Submission,
                        "uring.driver.submit_timer_internal",
                        "submit timer",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }
}
