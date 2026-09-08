pub(crate) mod txn;

use self::txn::{UringSubmitTxn, slot_access_report};
use crate::{
    config::{RawHandle, UringRawHandle},
    driver::{SqeFd, UringDriver, env::SubmitEnv, lifecycle::UringSubmissionState},
    error::{UringError, UringResult},
    op::{Reserved, Slot, SlotView, SubmissionStrategy, UringOp, UringOpRegistryExt, sqe_with_fd},
};
use diagweave::prelude::*;
use io_uring::{opcode, types};
use tracing::{debug, trace};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{
        CompletionToken, DriverSubmitResult, OpToken, RegisterFd, SubmitStatus, SubmitTokenContext,
    },
    slot::{CheckedSlotView, InFlightWaiting},
};
use veloq_std::{format, task::Poll, vec};

/// Turns a reserved slot's op into an SQE (or a wheel entry) and hands it to the kernel.
///
/// Takes the driver split in two: `slot` borrows out of `UringDriver::ops`, `env` covers every
/// other field the path needs. Both halves stay live at once — `make_sqe` reads the registered
/// file table while holding the op, and the SQE push happens while the slot's platform data is
/// still being updated — which is exactly why the split is a borrow-checked projection rather
/// than a `&mut UringDriver` reconstructed from a raw pointer.
pub(crate) fn submit_from_slot(
    env: &mut SubmitEnv<'_, '_>,
    token: OpToken,
    slot: Slot<'_, Reserved>,
) -> UringResult<bool> {
    UringSubmitTxn::new(env, token, slot)?.submit()
}

/// Retries a backlogged submission whose SQE never made it into a full ring.
pub(crate) fn submit_queued_from_slot(
    env: &mut SubmitEnv<'_, '_>,
    token: OpToken,
    mut slot: Slot<'_, InFlightWaiting>,
) -> UringResult<bool> {
    let user_data = token.index();
    if slot.platform().submission_state != UringSubmissionState::Queued {
        return Ok(true);
    }

    let strategy = slot
        .op_mut()
        .map_err(|err| slot_access_report("driver.submit_queued_from_slot.strategy", err))?
        .vtable
        .strategy;
    if strategy != SubmissionStrategy::SubmitSqe {
        return UringError::InvalidState
            .push_ctx("scope", "driver.submit_queued_from_slot.strategy")
            .with_ctx("user_data", user_data)
            .with_ctx("strategy", format!("{strategy:?}"))
            .attach_note("queued uring backlog entry is not an SQE operation");
    }

    let mut chunks = [ChunkId::ZERO; 4];
    let (count, sqe) = {
        let sqe_env = env.sqe_env();
        slot.with_op_and_payload_mut(|op, payload| {
            let vtable = op.vtable;
            let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
            let completion_token = CompletionToken::user(token);
            let sqe = unsafe {
                (vtable.make_sqe)(
                    op,
                    payload,
                    &sqe_env,
                    SubmitTokenContext::new(token, completion_token),
                )
                .attach_note("driver.submit_queued_from_slot.make_sqe")?
                .user_data(completion_token.raw())
            };
            Ok::<_, Report<UringError>>((count, sqe))
        })
        .map_err(|err| slot_access_report("driver.submit_queued_from_slot.op_payload", err))??
    };

    for &chunk_id in chunks.iter().take(count) {
        env.ensure_chunk_registered(
            chunk_id,
            user_data,
            "driver.submit_queued_from_slot.ensure_chunk_registered",
        )?;
    }

    if env.push_entry(sqe) {
        slot.platform_mut().submission_state = UringSubmissionState::KernelSubmitted;
        trace!(user_data, "Submitted queued backlog entry to SQ");
        Ok(true)
    } else {
        slot.platform_mut().submission_state = UringSubmissionState::Queued;
        debug!(user_data, "SQ still full for queued backlog entry");
        Ok(false)
    }
}

impl<'a> UringDriver<'a> {
    pub(crate) fn submit_from_slot_token(&mut self, token: OpToken) -> UringResult<bool> {
        let (ops, mut env) = self.split_for_submit();
        let slot = match ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(UringError::InvalidState
                    .report("driver.submit_from_slot_index", "op missing in slot"));
            }
        };
        submit_from_slot(&mut env, token, slot)
    }

    pub(crate) fn submit_waker(&mut self) -> UringResult<()> {
        if self.waker.is_armed() {
            return Ok(());
        }

        let waker_fd = match self.waker.registered_fd() {
            Some(fd) => fd,
            None => {
                let event_fd = self.waker.state().current();
                let fd = event_fd.fd.raw().as_fd();
                let raw = RawHandle::new(UringRawHandle::for_file(fd));
                let mut fds =
                    self.register_files_internal(vec![RegisterFd::Borrowed(raw.borrow())])?;
                let waker_fd = fds.pop().ok_or_else(|| {
                    UringError::InvalidState
                        .report("driver.submit_waker", "register_files returned empty")
                })?;
                self.waker.set_registered_fd(Some(waker_fd));
                waker_fd
            }
        };
        // The eventfd is registered like any other descriptor, so it lands in the fallback area
        // once the kernel table is full (or configured away entirely).
        let sqe_fd = self
            .file_table
            .resolve(waker_fd, None, "driver.submit_waker.resolve")?;
        let buf = self.waker.buf_mut_ptr();
        let len = self.waker.buf_len() as u32;
        let sqe = sqe_with_fd!(sqe_fd, |f| opcode::Read::new(f, buf, len).build())
            .user_data(CompletionToken::waker(0).raw());

        if self.push_entry(sqe) {
            self.waker.set_armed(true);
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

    #[inline]
    pub(crate) fn push_entry(&mut self, entry: io_uring::squeue::Entry) -> bool {
        self.submit_env().push_entry(entry)
    }

    pub(crate) fn submit_sqe_internal(
        &mut self,
        token: OpToken,
        op: UringOp,
        op_in: &mut Option<UringOp>,
    ) -> DriverSubmitResult<UringError> {
        let user_data = token.index();
        let outcome = {
            let (ops, mut env) = self.split_for_submit();
            let slot = match ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => {
                    if slot.has_op() {
                        let mut slot = slot;
                        match slot.op_mut() {
                            Ok(slot_op) => *slot_op = op,
                            Err(err) => {
                                return DriverSubmitResult::failed(
                                    slot_access_report(
                                        "uring.driver.submit_sqe_internal.op_mut",
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
                                        "uring.driver.submit_sqe_internal.init_op",
                                        err,
                                    ),
                                    SubmitStatus::Void,
                                );
                            }
                        }
                    }
                }
                Ok(_) => {
                    return DriverSubmitResult::failed(
                        UringError::InvalidState.report(
                            "uring.driver.submit_sqe_internal",
                            "Op slot missing in registry",
                        ),
                        SubmitStatus::Void,
                    );
                }
                Err(report) => {
                    return DriverSubmitResult::failed(report, SubmitStatus::Void);
                }
            };

            submit_from_slot(&mut env, token, slot)
        };

        match outcome {
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
        let outcome = {
            let (ops, mut env) = self.split_for_submit();
            let slot = match ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => {
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
                Ok(_) => {
                    return DriverSubmitResult::failed(
                        UringError::InvalidState.report(
                            "uring.driver.submit_timer_internal",
                            "Op slot missing in registry",
                        ),
                        SubmitStatus::Void,
                    );
                }
                Err(report) => {
                    return DriverSubmitResult::failed(report, SubmitStatus::Void);
                }
            };

            submit_from_slot(&mut env, token, slot)
        };

        match outcome {
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
