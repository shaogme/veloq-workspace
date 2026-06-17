use crate::{
    config::{RawHandle, UringRawHandle},
    driver::{UringDriver, lifecycle::UringSubmissionState},
    error::{UringError, UringResult},
    op::{Reserved, Slot, SlotView, SubmissionStrategy, UringOp, UringOpRegistryExt},
};
use diagweave::prelude::*;
use io_uring::opcode;
use std::task::Poll;
use tracing::{debug, trace};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{
        CompletionToken, DriverSubmitResult, OpToken, RegisterFd, SubmitStatus, SubmitTokenContext,
    },
    slot::{CheckedSlotView, InFlightWaiting, SlotAccessError},
};

fn slot_access_report(scope: &'static str, err: SlotAccessError) -> Report<UringError> {
    UringError::InvalidState
        .to_report()
        .push_ctx("scope", scope)
        .with_ctx("slot_index", err.snapshot.index)
        .with_ctx("slot_generation", err.snapshot.generation)
        .with_ctx("slot_state", format!("{:?}", err.snapshot.state))
        .with_ctx("slot_has_op", err.snapshot.has_op)
        .with_ctx("slot_has_payload", err.snapshot.has_payload)
        .with_ctx("slot_access_action", format!("{:?}", err.action))
        .with_ctx("slot_access_reason", format!("{:?}", err.reason))
        .attach_note("slot access failed during uring submission")
}

impl<'a> UringDriver<'a> {
    pub(crate) unsafe fn submit_from_slot_raw(
        driver: *mut UringDriver,
        token: OpToken,
        slot: Slot<'_, Reserved>,
    ) -> UringResult<bool> {
        let driver = unsafe { &mut *driver };
        let user_data = token.index();
        let mut sub_guard = slot
            .start_submission_with(None)
            .map_err(|err| slot_access_report("driver.submit_from_slot_raw.start", err))?;
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
            .map_err(|err| slot_access_report("driver.submit_from_slot_raw.strategy", err))?
            .vtable
            .strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [ChunkId::ZERO; 4];
                let (count, sqe) = {
                    let driver_ptr = driver as *mut UringDriver;
                    let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_from_slot_raw",
                            "submission guard slot missing",
                        )
                    })?;
                    slot.with_op_and_payload_mut(|op, payload| {
                        let vtable = op.vtable;
                        let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
                        let completion_token = CompletionToken::user(token);
                        let sqe = unsafe {
                            (vtable.make_sqe)(
                                op,
                                payload,
                                &mut *driver_ptr,
                                SubmitTokenContext::new(token, completion_token),
                            )
                            .attach_note("driver.submit_from_slot_raw.make_sqe")?
                            .user_data(completion_token.raw())
                        };
                        Ok::<_, Report<UringError>>((count, sqe))
                    })
                    .map_err(|err| {
                        slot_access_report("driver.submit_from_slot_raw.op_payload", err)
                    })??
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

                let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                    UringError::InvalidState.report(
                        "driver.submit_from_slot_raw",
                        "submission guard slot missing",
                    )
                })?;
                if driver.push_entry(sqe) {
                    slot.platform_mut().submission_state = UringSubmissionState::KernelSubmitted;
                    let _ = sub_guard.persist();
                    trace!(user_data, "Submitted to SQ");
                    Ok(true)
                } else {
                    slot.platform_mut().submission_state = UringSubmissionState::Queued;
                    let _ = sub_guard.persist();
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
                    slot.with_op_and_payload_mut(|op, payload| {
                        let vtable = op.vtable;
                        unsafe { (vtable.get_timeout)(op, payload) }
                    })
                    .map_err(|err| {
                        slot_access_report("driver.submit_from_slot_raw.timer.op_payload", err)
                    })?
                };
                if let Some(duration) = duration_opt {
                    let task_id = driver.wheel.insert(token, duration);
                    if let Some(platform) = driver.ops.platform_mut(token) {
                        platform.timer_id = Some(task_id);
                        platform.submission_state = UringSubmissionState::Timer;
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
        }
    }

    pub(crate) fn submit_from_slot_token(&mut self, token: OpToken) -> UringResult<bool> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(UringError::InvalidState
                    .report("driver.submit_from_slot_index", "op missing in slot"));
            }
        };
        unsafe { Self::submit_from_slot_raw(driver_ptr, token, slot) }
    }

    pub(crate) unsafe fn submit_queued_from_slot_raw(
        driver: *mut UringDriver,
        token: OpToken,
        mut slot: Slot<'_, InFlightWaiting>,
    ) -> UringResult<bool> {
        let driver = unsafe { &mut *driver };
        let user_data = token.index();
        if slot.platform().submission_state != UringSubmissionState::Queued {
            return Ok(true);
        }

        let strategy = slot
            .op_mut()
            .map_err(|err| slot_access_report("driver.submit_queued_from_slot_raw.strategy", err))?
            .vtable
            .strategy;
        if strategy != SubmissionStrategy::SubmitSqe {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.submit_queued_from_slot_raw.strategy")
                .with_ctx("user_data", user_data)
                .with_ctx("strategy", format!("{strategy:?}"))
                .attach_note("queued uring backlog entry is not an SQE operation"));
        }

        let mut chunks = [ChunkId::ZERO; 4];
        let (count, sqe) = {
            let driver_ptr = driver as *mut UringDriver;
            slot.with_op_and_payload_mut(|op, payload| {
                let vtable = op.vtable;
                let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
                let completion_token = CompletionToken::user(token);
                let sqe = unsafe {
                    (vtable.make_sqe)(
                        op,
                        payload,
                        &mut *driver_ptr,
                        SubmitTokenContext::new(token, completion_token),
                    )
                    .attach_note("driver.submit_queued_from_slot_raw.make_sqe")?
                    .user_data(completion_token.raw())
                };
                Ok::<_, Report<UringError>>((count, sqe))
            })
            .map_err(|err| {
                slot_access_report("driver.submit_queued_from_slot_raw.op_payload", err)
            })??
        };

        for &chunk_id in chunks.iter().take(count) {
            let index = chunk_id.as_usize();
            let is_registered = driver.registered_chunks.get(index).map_err(|e| {
                UringError::InvalidState
                    .to_report()
                    .push_ctx("scope", "driver.submit_queued_from_slot_raw.bitset_get")
                    .with_ctx("chunk_index", index)
                    .with_ctx("bitset_error", format!("{e:?}"))
                    .attach_note("BitSet get failed")
            })?;

            if !is_registered && let Some(info) = driver.registrar.resolve_chunk_info(chunk_id) {
                if let Err(e) =
                    driver.register_chunk_internal(info.id, info.ptr.as_ptr(), info.len.get())
                {
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
                        .push_ctx(
                            "scope",
                            "driver.submit_queued_from_slot_raw.missing_chunk_info",
                        )
                        .with_ctx("chunk_id", chunk_id.raw())
                        .with_ctx("user_data", user_data)
                        .attach_note("strict mode missing chunk info for lazy registration"));
                }
                return Err(UringError::InvalidInput
                    .to_report()
                    .push_ctx(
                        "scope",
                        "driver.submit_queued_from_slot_raw.missing_chunk_info",
                    )
                    .with_ctx("chunk_id", chunk_id.raw())
                    .with_ctx("user_data", user_data)
                    .attach_note("missing chunk info for lazy registration"));
            }
        }

        if driver.push_entry(sqe) {
            slot.platform_mut().submission_state = UringSubmissionState::KernelSubmitted;
            trace!(user_data, "Submitted queued backlog entry to SQ");
            Ok(true)
        } else {
            slot.platform_mut().submission_state = UringSubmissionState::Queued;
            debug!(user_data, "SQ still full for queued backlog entry");
            Ok(false)
        }
    }

    pub(crate) fn submit_waker(&mut self) -> UringResult<()> {
        if self.waker_armed {
            return Ok(());
        }

        let fixed_fd = match self.waker_registered_fd {
            Some(fd) => fd,
            None => {
                let event_fd = self.waker_state.current();
                let fd = event_fd.fd.raw().as_fd();
                let raw = RawHandle::new(UringRawHandle::for_file(fd));
                let mut fds =
                    self.register_files_internal(vec![RegisterFd::Borrowed(raw.borrow())])?;
                let fixed_fd = fds.pop().ok_or_else(|| {
                    UringError::InvalidState
                        .report("driver.submit_waker", "register_files returned empty")
                })?;
                self.waker_registered_fd = Some(fixed_fd);
                fixed_fd
            }
        };
        let sqe = opcode::Read::new(
            io_uring::types::Fixed(fixed_fd.fixed_index()),
            self.waker_buf.as_mut_ptr(),
            self.waker_buf.len() as u32,
        )
        .build()
        .user_data(CompletionToken::waker(0).raw());

        if self.push_entry(sqe) {
            self.waker_armed = true;
            Ok(())
        } else {
            Err(UringError::Submission.report("driver.submit_waker", "failed to enqueue waker SQE"))
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
        self.flush_backlog()?;
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
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    match slot.op_mut() {
                        Ok(slot_op) => *slot_op = op,
                        Err(err) => {
                            return DriverSubmitResult::failed(
                                slot_access_report("uring.driver.submit_sqe_internal.op_mut", err),
                                SubmitStatus::Void,
                            );
                        }
                    }
                    slot
                } else {
                    match slot.init_op_with(op, |_| {}) {
                        Ok(slot) => slot,
                        Err(err) => {
                            return DriverSubmitResult::failed(
                                slot_access_report("uring.driver.submit_sqe_internal.init_op", err),
                                SubmitStatus::Void,
                            );
                        }
                    }
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
                    .active_slot_bundle_mut(token)
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
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    match slot.op_mut() {
                        Ok(slot_op) => *slot_op = op,
                        Err(err) => {
                            return DriverSubmitResult::failed(
                                slot_access_report(
                                    "uring.driver.submit_timer_internal.op_mut",
                                    err,
                                ),
                                SubmitStatus::Void,
                            );
                        }
                    }
                    slot
                } else {
                    match slot.init_op_with(op, |_| {}) {
                        Ok(slot) => slot,
                        Err(err) => {
                            return DriverSubmitResult::failed(
                                slot_access_report(
                                    "uring.driver.submit_timer_internal.init_op",
                                    err,
                                ),
                                SubmitStatus::Void,
                            );
                        }
                    }
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
                    .active_slot_bundle_mut(token)
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
