use diagweave::prelude::*;
use std::task::Poll;
use tracing::{debug, trace};

use crate::config::{RawHandle, UringRawHandle};
use crate::driver::UringDriver;
use crate::driver::lifecycle::UringOpState;
use crate::error::{UringError, UringResult};
use crate::op::slot::{Slot, SlotView, UringOpRegistryExt};
use crate::op::{SubmissionStrategy, UringOp, UringUserPayload};

use veloq_driver_core::driver::registry::{AllocResult, OpHandle};
use veloq_driver_core::driver::{
    CompletionToken, Driver, DriverSubmitResult, OpToken, SubmitStatus,
};
use veloq_driver_core::op::{IntoPlatformOp, Wakeup};

impl<'a> UringDriver<'a> {
    pub(crate) unsafe fn submit_from_slot_raw(
        driver: *mut UringDriver,
        token: OpToken,
        slot: Slot<'_, crate::op::slot::Reserved>,
    ) -> UringResult<bool> {
        let driver = unsafe { &mut *driver };
        let user_data = token.index();
        let mut sub_guard = slot.start_submission_with(None);
        let strategy = sub_guard
            .slot
            .as_mut()
            .ok_or_else(|| {
                UringError::InvalidState.report(
                    "driver.submit_from_slot_raw",
                    "submission guard slot missing",
                )
            })?
            .op_mut()
            .vtable
            .strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [veloq_buf::heap::ChunkId::ZERO; 4];
                let (count, sqe) = {
                    let driver_ptr = driver as *mut UringDriver;
                    let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw",
                            "submission guard slot missing",
                        )
                    })?;
                    let payload = slot.storage.payload.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw",
                            "submission guard payload missing",
                        )
                    })?;

                    let op = slot.op.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw",
                            "slot in submission state missing op",
                        )
                    })?;
                    let vtable = op.vtable;
                    let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
                    let completion_token = if Some(token) == driver.waker_token {
                        CompletionToken::waker(0)
                    } else {
                        CompletionToken::user(token)
                    };
                    let sqe = unsafe {
                        (vtable.make_sqe)(op, &mut *driver_ptr, user_data)
                            .attach_note("driver.submit_from_slot_raw.make_sqe")?
                            .user_data(completion_token.raw())
                    };
                    (count, sqe)
                };

                for &chunk_id in chunks.iter().take(count) {
                    let index = chunk_id.as_usize();
                    let is_registered = driver.registered_chunks.get(index).map_err(|e| {
                        UringError::InvalidState
                            .to_report()
                            .push_ctx("scope", "driver.submit_from_slot_raw.bitset_get")
                            .with_ctx("chunk_index", index)
                            .with_ctx("bitset_error", format!("{e:?}"))
                            .attach_note("BitSet get failed")
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
                                return Err(e
                                    .with_ctx("chunk_id", chunk_id.raw())
                                    .with_ctx("user_data", user_data)
                                    .attach_note("strict mode lazy register failed"));
                            }
                            return Err(e);
                        }
                    } else if !is_registered {
                        driver.registration_stats.submission_missing_chunk_info = driver
                            .registration_stats
                            .submission_missing_chunk_info
                            .saturating_add(1);
                        if driver.registration_mode.is_strict() {
                            return Err(UringError::InvalidState
                                .to_report()
                                .push_ctx("scope", "driver.submit_from_slot_raw.missing_chunk_info")
                                .with_ctx("chunk_id", chunk_id.raw())
                                .with_ctx("user_data", user_data)
                                .attach_note(
                                    "strict mode missing chunk info for lazy registration",
                                ));
                        }
                        return Err(UringError::InvalidInput
                            .to_report()
                            .push_ctx("scope", "driver.submit_from_slot_raw.missing_chunk_info")
                            .with_ctx("chunk_id", chunk_id.raw())
                            .with_ctx("user_data", user_data)
                            .attach_note("missing chunk info for lazy registration"));
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
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw.timer",
                            "submission guard slot missing",
                        )
                    })?;
                    let payload = slot.storage.payload.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw.timer",
                            "submission guard payload missing",
                        )
                    })?;
                    let op = slot.op.as_mut().ok_or_else(|| {
                        UringError::InvalidState
                            .report("driver.submit_from_slot_raw.timer", "timer slot missing op")
                    })?;
                    let vtable = op.vtable;
                    unsafe { (vtable.get_timeout)(op, payload) }
                };
                if let Some(duration) = duration_opt {
                    let task_id = driver.wheel.insert(token, duration);
                    if let Some(entry) = driver.ops.get_mut(user_data) {
                        entry.platform_data.timer_id = Some(task_id);
                    }
                    let _ = sub_guard.persist();
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(true)
                } else {
                    Err(UringError::InvalidInput.report(
                        "driver.submit_from_slot_raw.timer_duration",
                        "Timer duration missing",
                    ))
                }
            }
            _ => Err(UringError::Unsupported.report(
                "driver.submit_from_slot_raw.strategy",
                "Unsupported strategy for slot submission",
            )),
        }
    }

    pub(crate) fn submit_from_slot_token(&mut self, token: OpToken) -> UringResult<bool> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.checked_slot_view(token) {
            veloq_driver_core::slot::CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(UringError::InvalidState
                    .report("driver.submit_from_slot_index", "op missing in slot"));
            }
        };
        unsafe { Self::submit_from_slot_raw(driver_ptr, token, slot) }
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
                    UringError::InvalidState
                        .report("driver.submit_waker", "register_files returned empty")
                })?;
                self.waker_registered_fd = Some(fixed_fd);
                fixed_fd
            }
        };
        let op = Wakeup { fd: fixed_fd };
        let (uring_op, payload) = <Wakeup as IntoPlatformOp<UringOp>>::into_kernel_and_payload(op);

        let result = self.ops.alloc(UringOpState::new());

        if let Ok(AllocResult {
            handle:
                OpHandle {
                    index: user_data,
                    generation,
                },
        }) = result
        {
            let token = OpToken::new(user_data, generation);
            self.waker_token = Some(token);
            self.slot_set_payload_raw(token, UringUserPayload::Wakeup(payload));

            let driver_ptr = self as *mut UringDriver;
            let slot = self
                .ops
                .slot_reserve(user_data)
                .init_op_with(uring_op, |_| {});
            match unsafe { Self::submit_from_slot_raw(driver_ptr, token, slot) } {
                Ok(true) => {}
                Ok(false) => self.push_backlog(token),
                Err(e) => return Err(e),
            }
            Ok(())
        } else {
            Err(UringError::InvalidState
                .report("driver.submit_waker", "failed to reserve waker slot"))
        }
    }

    pub(crate) fn submit_to_kernel(&mut self) -> UringResult<()> {
        trace!("submit_to_kernel entered");
        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                self.ring.submit().map_err(|e| {
                    UringError::Submission.io_report("driver.submit_to_kernel.submit.sqpoll", e)
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
                            UringError::Submission.io_report("driver.submit_to_kernel.enter", e)
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
        token: OpToken,
        op: UringOp,
        op_in: &mut Option<UringOp>,
    ) -> DriverSubmitResult<UringError> {
        let user_data = token.index();
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.checked_slot_view(token) {
            veloq_driver_core::slot::CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            _ => {
                return DriverSubmitResult::failed(
                    UringError::InvalidState.report(
                        "uring.driver.submit_sqe_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, token, slot) } {
            Ok(true) => DriverSubmitResult::submitted(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                self.push_backlog(token);
                DriverSubmitResult::submitted(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                DriverSubmitResult::failed(
                    e.push_ctx("scope", "uring.driver.submit_sqe_internal")
                        .attach_note("submit sqe"),
                    SubmitStatus::Void,
                )
            }
        }
    }

    pub(crate) fn submit_timer_internal(
        &mut self,
        token: OpToken,
        op: UringOp,
        op_in: &mut Option<UringOp>,
    ) -> DriverSubmitResult<UringError> {
        let user_data = token.index();
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.checked_slot_view(token) {
            veloq_driver_core::slot::CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            _ => {
                return DriverSubmitResult::failed(
                    UringError::InvalidState.report(
                        "uring.driver.submit_timer_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, token, slot) } {
            Ok(true) => DriverSubmitResult::submitted(Poll::Ready(())),
            Ok(false) => {
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                self.push_backlog(token);
                DriverSubmitResult::submitted(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                DriverSubmitResult::failed(
                    e.push_ctx("scope", "uring.driver.submit_timer_internal")
                        .attach_note("submit timer"),
                    SubmitStatus::Void,
                )
            }
        }
    }
}
