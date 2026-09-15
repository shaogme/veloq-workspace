pub(crate) mod txn;

use self::txn::{UringSubmitTxn, slot_access_report};
use crate::{
    config::{RawHandle, UringRawHandle},
    driver::{
        SqeFd, UringDriver,
        control::{BacklogStageKind, ControlPlaneEvent, StagedEntry, transition_submission_phase},
        env::StageResult,
        env::SubmitEnv,
        lifecycle::{CancellationPhase, SubmissionPhase},
    },
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

pub(super) fn validate_resolved_chunk_count(
    count: usize,
    capacity: usize,
    scope: &'static str,
) -> UringResult<()> {
    if count <= capacity {
        return Ok(());
    }

    UringError::InvalidState
        .push_ctx("scope", scope)
        .with_ctx("resolved_chunk_count", count)
        .with_ctx("chunk_output_capacity", capacity)
        .attach_note("operation resolved more chunks than the submission scratch buffer holds")
}

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
    if slot.platform().control.submission != SubmissionPhase::Reserved {
        return Ok(true);
    }

    let strategy = slot
        .with_access_mut(|access| access.operation().get_ref().descriptor().strategy)
        .map_err(|err| slot_access_report("driver.submit_queued_from_slot.strategy", err))?;
    if strategy != SubmissionStrategy::SubmitSqe {
        return UringError::InvalidState
            .push_ctx("scope", "driver.submit_queued_from_slot.strategy")
            .with_ctx("user_data", user_data)
            .with_ctx("strategy", format!("{strategy:?}"))
            .attach_note("queued uring backlog entry is not an SQE operation");
    }

    let mut chunks = [ChunkId::ZERO; 4];
    let (count, sqe, completion_token, cleanup_hint) = {
        let sqe_env = env.sqe_env();
        slot.with_access_mut(|access| {
            let descriptor = access.operation().get_ref().descriptor();
            let count = unsafe { (descriptor.resolve_chunks)(access, token, &mut chunks) }?;
            validate_resolved_chunk_count(
                count,
                chunks.len(),
                "driver.submit_queued_from_slot.resolve_chunks",
            )?;
            let completion_token = CompletionToken::user(token);
            let sqe = unsafe {
                (descriptor.make_sqe)(
                    access,
                    &sqe_env,
                    SubmitTokenContext::new(token, completion_token),
                )
                .attach_note("driver.submit_queued_from_slot.make_sqe")?
                .user_data(completion_token.raw())
            };
            Ok::<_, Report<UringError>>((
                count,
                sqe,
                completion_token,
                descriptor.completion_cleanup_hint,
            ))
        })
        .map_err(|err| slot_access_report("driver.submit_queued_from_slot.op_payload", err))??
    };

    for &chunk_id in chunks.iter().take(count) {
        let _decision = env.ensure_chunk_registered(
            chunk_id,
            user_data,
            "driver.submit_queued_from_slot.ensure_chunk_registered",
        )?;
    }

    if env.stage_user_entry(token, sqe)? == StageResult::Staged {
        env.transition_submission_state(
            token,
            &mut slot.platform_mut().control.submission,
            SubmissionPhase::SqeStaged,
            "queued submission staged SQE",
        );
        env.register_completion_cleanup_hint(completion_token, cleanup_hint);
        trace!(user_data, "Submitted queued backlog entry to SQ");
        Ok(true)
    } else {
        env.transition_submission_state(
            token,
            &mut slot.platform_mut().control.submission,
            SubmissionPhase::Reserved,
            "queued submission remains queued after SQ full",
        );
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
        if self.control.waker.is_armed() {
            return Ok(());
        }

        let waker_fd = match self.control.waker.registered_fd() {
            Some(fd) => fd,
            None => {
                let event_fd = self.control.waker.state().current();
                let fd = event_fd.fd.raw().as_fd();
                let raw = RawHandle::new(UringRawHandle::for_file(fd));
                let mut fds =
                    self.register_files_internal(vec![RegisterFd::Borrowed(raw.borrow())])?;
                let waker_fd = fds.pop().ok_or_else(|| {
                    UringError::InvalidState
                        .report("driver.submit_waker", "register_files returned empty")
                })?;
                self.control.waker.set_registered_fd(Some(waker_fd));
                waker_fd
            }
        };
        // The eventfd is registered like any other descriptor, so it lands in the fallback area
        // once the kernel table is full (or configured away entirely).
        let sqe_fd = self
            .file_table
            .resolve(waker_fd, None, "driver.submit_waker.resolve")?;
        let buf = self.control.waker.buf_mut_ptr();
        let len = self.control.waker.buf_len() as u32;
        let sqe = sqe_with_fd!(sqe_fd, |f| opcode::Read::new(f, buf, len).build())
            .user_data(CompletionToken::waker(0).raw());

        if self.push_entry(sqe) == StageResult::Staged {
            self.control.stage_entry(StagedEntry::Waker);
            self.control.waker_stage_pending = false;
            self.control.waker.arm();
            self.control
                .record(ControlPlaneEvent::WakerArm { armed: true });
            self.control.waker.finish_rearm();
            self.control.record(ControlPlaneEvent::WakerRearmed);
            Ok(())
        } else {
            self.control.waker_stage_pending = true;
            Ok(())
        }
    }

    pub(crate) fn submit_to_kernel(&mut self) -> UringResult<()> {
        trace!("submit_to_kernel entered");
        let submitted = self.submit_staged_batch()?;
        self.mark_kernel_submissions(submitted)?;
        if self.control.waker_stage_pending {
            self.submit_waker()?;
            let submitted = self.submit_staged_batch()?;
            self.mark_kernel_submissions(submitted)?;
        }
        self.flush_backlog()?;
        Ok(())
    }

    fn submit_staged_batch(&mut self) -> UringResult<usize> {
        let to_submit = self.ring.submission().len();
        if to_submit == 0 {
            return Ok(0);
        }

        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                return self.ring.submit().map_err(|e| {
                    UringError::Submission.io_report("driver.submit_to_kernel.submit.sqpoll", e)
                });
            }
            return Ok(to_submit);
        }

        unsafe {
            self.ring
                .submitter()
                .enter::<()>(
                    to_submit as u32,
                    0,
                    1, /* IORING_ENTER_GETEVENTS */
                    None,
                )
                .map_err(|e| UringError::Submission.io_report("driver.submit_to_kernel.enter", e))
        }
    }

    fn mark_kernel_submissions(&mut self, submitted: usize) -> UringResult<()> {
        let available = self.control.staged_entry_count();
        if submitted > available {
            return Err(UringError::InvalidState
                .report(
                    "uring.submit.mark_kernel_submissions",
                    "kernel submit count exceeds staged metadata",
                )
                .with_ctx("submitted", submitted)
                .with_ctx("staged_entries", available));
        }
        let entries = self.control.mark_submitted_entries(submitted);
        let (ops, control) = (&mut self.ops, &mut self.control);
        for entry in entries {
            match entry {
                StagedEntry::User(token) => {
                    let view = ops.checked_slot_view(token)?;
                    match view {
                        CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                            transition_submission_phase(
                                &mut slot.platform_mut().control.submission,
                                token,
                                SubmissionPhase::KernelOutstanding,
                                "submit boundary handed SQE to kernel",
                                &mut control.observer,
                            );
                        }
                        CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                            transition_submission_phase(
                                &mut slot.platform_mut().control.submission,
                                token,
                                SubmissionPhase::KernelOutstanding,
                                "submit boundary handed orphaned SQE to kernel",
                                &mut control.observer,
                            );
                        }
                        _ => {
                            return Err(UringError::InvalidState
                                .report(
                                    "uring.submit.mark_kernel_submissions",
                                    "staged user token is no longer in flight",
                                )
                                .with_ctx("token", token.index()));
                        }
                    }
                }
                StagedEntry::Cancel { target, .. } => {
                    if let Ok(view) = ops.checked_slot_view(target) {
                        match view {
                            CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                                slot.platform_mut().control.cancellation =
                                    CancellationPhase::CancelOutstanding;
                            }
                            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                                slot.platform_mut().control.cancellation =
                                    CancellationPhase::CancelOutstanding;
                            }
                            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                                slot.platform_mut().control.cancellation =
                                    CancellationPhase::CancelOutstanding;
                            }
                            CheckedSlotView::Empty(_)
                            | CheckedSlotView::Missing { .. }
                            | CheckedSlotView::Stale(_) => {}
                        }
                    }
                }
                StagedEntry::Waker => {}
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn push_entry(
        &mut self,
        entry: io_uring::squeue::Entry,
    ) -> crate::driver::env::StageResult {
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
                        if let Err(err) = slot.replace_op(op) {
                            return DriverSubmitResult::failed(
                                slot_access_report(
                                    "uring.driver.submit_sqe_internal.replace_op",
                                    err,
                                ),
                                SubmitStatus::Void,
                            );
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
                    *op_in = Some(op);
                    return DriverSubmitResult::failed(
                        UringError::InvalidState.report(
                            "uring.driver.submit_sqe_internal",
                            "Op slot missing in registry",
                        ),
                        SubmitStatus::Void,
                    );
                }
                Err(report) => {
                    *op_in = Some(op);
                    return DriverSubmitResult::failed(report, SubmitStatus::Void);
                }
            };

            submit_from_slot(&mut env, token, slot)
        };

        match outcome {
            Ok(true) => DriverSubmitResult::submitted(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                match self.push_backlog(token, BacklogStageKind::Sqe) {
                    Ok(()) => DriverSubmitResult::submitted(Poll::Pending),
                    Err(report) => {
                        if let Some(op) = self
                            .ops
                            .active_slot_bundle_mut(token)
                            .and_then(|(_, _, op, _)| op.take())
                        {
                            *op_in = Some(op);
                        }
                        DriverSubmitResult::failed(report, SubmitStatus::Void)
                    }
                }
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
                        if let Err(err) = slot.replace_op(op) {
                            return DriverSubmitResult::failed(
                                slot_access_report(
                                    "uring.driver.submit_timer_internal.replace_op",
                                    err,
                                ),
                                SubmitStatus::Void,
                            );
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
                    *op_in = Some(op);
                    return DriverSubmitResult::failed(
                        UringError::InvalidState.report(
                            "uring.driver.submit_timer_internal",
                            "Op slot missing in registry",
                        ),
                        SubmitStatus::Void,
                    );
                }
                Err(report) => {
                    *op_in = Some(op);
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
                match self.push_backlog(token, BacklogStageKind::Timer) {
                    Ok(()) => DriverSubmitResult::submitted(Poll::Pending),
                    Err(report) => {
                        if let Some(op) = self
                            .ops
                            .active_slot_bundle_mut(token)
                            .and_then(|(_, _, op, _)| op.take())
                        {
                            *op_in = Some(op);
                        }
                        DriverSubmitResult::failed(report, SubmitStatus::Void)
                    }
                }
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
