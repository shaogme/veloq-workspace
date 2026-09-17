pub(crate) mod txn;

use self::txn::{UringSubmitTxn, slot_access_report};
use crate::{
    config::{RawHandle, UringRawHandle},
    driver::{
        SqeFd, UringDriver,
        control::{ControlPlaneEvent, StagedLedgerError, transition_submission_phase},
        env::StageResult,
        env::SubmitEnv,
        lifecycle::{CancellationPhase, SubmissionPhase},
    },
    error::{UringError, UringResult},
    op::{Reserved, Slot, SlotView, SubmissionStrategy, UringOp, UringOpRegistryExt, sqe_with_fd},
};
use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{
        CompletionToken, DriverSubmitResult, OpToken, RegisterFd, SubmitStatus, SubmitTokenContext,
    },
    slot::{CheckedSlotView, InFlightWaiting},
};
use veloq_io_uring::{
    opcode,
    types::{self, SubmitArgs, Timespec},
};
use veloq_std::{format, task::Poll, time::Duration, vec};

#[derive(Debug)]
pub(crate) enum SubmissionError {
    Kernel(Report<UringError>),
    ReceiptUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitBudgetSource {
    External,
    Timer,
    Probe,
}

#[derive(Debug)]
pub(crate) enum SubmissionReceipt {
    NoEntries,
    Consumed { requested: usize, consumed: usize },
    PublishedToSqpoll { published: usize },
    Rejected { error: SubmissionError },
    Unknown { error: SubmissionError },
}

#[derive(Debug)]
pub(crate) struct SubmitProgress {
    pub(crate) receipt: SubmissionReceipt,
    pub(crate) staged: usize,
    pub(crate) kernel_outstanding: usize,
    pub(crate) pending_submit: bool,
    pub(crate) timed_out: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct KernelEnterPlan {
    pub(crate) to_submit: usize,
    pub(crate) wait: Option<Duration>,
    pub(crate) wait_source: Option<WaitBudgetSource>,
    pub(crate) ready_preflight: bool,
    pub(crate) zero_timeout: bool,
}

impl KernelEnterPlan {
    pub(crate) const fn poll(to_submit: usize) -> Self {
        Self {
            to_submit,
            wait: None,
            wait_source: None,
            ready_preflight: false,
            zero_timeout: false,
        }
    }

    pub(crate) const fn poll_ready(to_submit: usize) -> Self {
        Self {
            ready_preflight: true,
            ..Self::poll(to_submit)
        }
    }

    pub(crate) const fn poll_zero_timeout(to_submit: usize) -> Self {
        Self {
            zero_timeout: true,
            ..Self::poll(to_submit)
        }
    }

    pub(crate) const fn wait(
        to_submit: usize,
        timeout: Duration,
        source: WaitBudgetSource,
    ) -> Self {
        Self {
            to_submit,
            wait: Some(timeout),
            wait_source: Some(source),
            ready_preflight: false,
            zero_timeout: false,
        }
    }
}

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
    let (count, sqe, cleanup_hint) = {
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
            Ok::<_, Report<UringError>>((count, sqe, descriptor.completion_cleanup_hint))
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

    if env.stage_user_entry(token, sqe, cleanup_hint)? == StageResult::Staged {
        env.transition_submission_state(
            token,
            &mut slot.platform_mut().control.submission,
            SubmissionPhase::SqeStaged,
            "queued submission staged SQE",
        );
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

        if self.submit_env().stage_waker_entry(sqe)? == StageResult::Staged {
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

    pub(crate) fn submit_to_kernel(
        &mut self,
        plan: KernelEnterPlan,
    ) -> UringResult<SubmitProgress> {
        trace!(
            to_submit = plan.to_submit,
            wait = ?plan.wait,
            ready_preflight = plan.ready_preflight,
            zero_timeout = plan.zero_timeout,
            "submit_to_kernel entered"
        );
        if self.submission_fail_stop {
            return Err(UringError::Submission
                .report(
                    "uring.submit_to_kernel.fail_stop",
                    "submission receipt was previously unknown",
                )
                .attach_note("driver cannot safely reuse an SQE after an unknown receipt"));
        }

        let to_submit = self.ring.submission().len();
        if to_submit != plan.to_submit {
            return Err(UringError::InvalidState
                .report(
                    "uring.submit_to_kernel.plan",
                    "submission queue changed after the kernel enter plan was built",
                )
                .with_ctx("planned", plan.to_submit)
                .with_ctx("actual", to_submit));
        }

        let unpublished = self.control.unpublished_staged_entry_count();
        if unpublished > to_submit {
            return Err(UringError::InvalidState
                .report(
                    "uring.submit_to_kernel.ledger",
                    "staged metadata exceeds entries visible in the submission queue",
                )
                .with_ctx("unpublished", unpublished)
                .with_ctx("queue_entries", to_submit));
        }
        let published_in_queue = to_submit - unpublished;
        let (receipt, timed_out) = self.submit_staged_batch(plan, to_submit, unpublished)?;
        let receipt = match receipt {
            SubmissionReceipt::Unknown { error } => {
                self.control.quarantine_unpublished_staged();
                self.submission_fail_stop = true;
                return Err(submission_error_report(error));
            }
            receipt => receipt,
        };
        let kernel_outstanding = match &receipt {
            SubmissionReceipt::NoEntries => 0,
            SubmissionReceipt::Rejected { error } => {
                let _ = error;
                0
            }
            SubmissionReceipt::Consumed {
                requested,
                consumed,
            } => self
                .control
                .mark_staged_consumed(*requested, *consumed, published_in_queue)
                .map_err(|error| staged_ledger_report("uring.submit.receipt", error))?,
            SubmissionReceipt::PublishedToSqpoll { published } => {
                if *published != unpublished {
                    return Err(UringError::InvalidState
                        .report(
                            "uring.submit.receipt.sqpoll",
                            "published receipt does not match unpublished ledger entries",
                        )
                        .with_ctx("published", *published)
                        .with_ctx("unpublished", unpublished));
                }
                self.control.mark_staged_published();
                0
            }
            SubmissionReceipt::Unknown { .. } => unreachable!("unknown receipt was returned"),
        };
        self.settle_kernel_submission_phases();

        let pending_submit = self.control.unpublished_staged_entry_count() > 0;
        Ok(SubmitProgress {
            receipt,
            staged: unpublished,
            kernel_outstanding,
            pending_submit,
            timed_out,
        })
    }

    fn settle_kernel_submission_phases(&mut self) {
        self.control.mark_kernel_cancel_intents();
        let (ops, control) = (&mut self.ops, &mut self.control);
        control.staged_entries.for_each_kernel_cancel(|_, target| {
            let Ok(view) = ops.checked_slot_view(target) else {
                return;
            };
            match view {
                CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                    slot.platform_mut().control.cancellation = CancellationPhase::CancelOutstanding;
                }
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                    slot.platform_mut().control.cancellation = CancellationPhase::CancelOutstanding;
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                    slot.platform_mut().control.cancellation = CancellationPhase::CancelOutstanding;
                }
                CheckedSlotView::Empty(_)
                | CheckedSlotView::Missing { .. }
                | CheckedSlotView::Stale(_) => {}
            }
        });
        control.staged_entries.for_each_kernel_user(|token| {
            let Ok(view) = ops.checked_slot_view(token) else {
                return;
            };
            match view {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))
                    if slot.platform().control.submission == SubmissionPhase::SqeStaged =>
                {
                    transition_submission_phase(
                        &mut slot.platform_mut().control.submission,
                        token,
                        SubmissionPhase::KernelOutstanding,
                        "submit receipt handed SQE to kernel",
                        &mut control.observer,
                    );
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot))
                    if slot.platform().control.submission == SubmissionPhase::SqeStaged =>
                {
                    transition_submission_phase(
                        &mut slot.platform_mut().control.submission,
                        token,
                        SubmissionPhase::KernelOutstanding,
                        "submit receipt handed orphaned SQE to kernel",
                        &mut control.observer,
                    );
                }
                _ => {}
            }
        });
    }

    fn submit_staged_batch(
        &mut self,
        plan: KernelEnterPlan,
        to_submit: usize,
        unpublished: usize,
    ) -> UringResult<(SubmissionReceipt, bool)> {
        if to_submit > u32::MAX as usize {
            return Ok((
                SubmissionReceipt::Rejected {
                    error: SubmissionError::ReceiptUnavailable,
                },
                false,
            ));
        }

        if self.ring.params().is_setup_sqpoll() && plan.wait.is_none() {
            if to_submit == 0 || !self.ring.submission().need_wakeup() {
                if unpublished == 0 {
                    return Ok((SubmissionReceipt::NoEntries, false));
                }
                return Ok((
                    SubmissionReceipt::PublishedToSqpoll {
                        published: unpublished,
                    },
                    false,
                ));
            }

            let consumed = self.ring.submit();
            return Ok((
                match consumed {
                    Ok(consumed) => SubmissionReceipt::Consumed {
                        requested: to_submit,
                        consumed,
                    },
                    Err(error) => SubmissionReceipt::Unknown {
                        error: SubmissionError::Kernel(
                            UringError::Submission
                                .io_report("driver.submit_to_kernel.submit.sqpoll", error),
                        ),
                    },
                },
                false,
            ));
        }

        if let Some(timeout) = plan.wait {
            let timespec = Timespec::new()
                .sec(timeout.as_secs())
                .nsec(timeout.subsec_nanos());
            let args = SubmitArgs::new().timespec(&timespec);
            match self.ring.submitter().submit_with_args(1, &args) {
                Ok(consumed) => Ok((
                    if to_submit == 0 {
                        SubmissionReceipt::NoEntries
                    } else {
                        SubmissionReceipt::Consumed {
                            requested: to_submit,
                            consumed,
                        }
                    },
                    false,
                )),
                Err(error) if error.raw_os_error() == Some(libc::ETIME) && to_submit == 0 => {
                    Ok((SubmissionReceipt::NoEntries, true))
                }
                Err(error) => Ok((
                    SubmissionReceipt::Unknown {
                        error: SubmissionError::Kernel(
                            UringError::CompletionWait
                                .io_report("driver.submit_to_kernel.wait", error),
                        ),
                    },
                    false,
                )),
            }
        } else {
            let consumed = unsafe {
                self.ring.submitter().enter::<()>(
                    to_submit as u32,
                    0,
                    1, /* IORING_ENTER_GETEVENTS */
                    None,
                )
            };
            match consumed {
                Ok(consumed) => Ok((
                    if to_submit == 0 {
                        SubmissionReceipt::NoEntries
                    } else {
                        SubmissionReceipt::Consumed {
                            requested: to_submit,
                            consumed,
                        }
                    },
                    false,
                )),
                Err(error) => Ok((
                    SubmissionReceipt::Unknown {
                        error: SubmissionError::Kernel(
                            UringError::Submission
                                .io_report("driver.submit_to_kernel.enter", error),
                        ),
                    },
                    false,
                )),
            }
        }
    }

    #[inline]
    pub(crate) fn submit_operation_internal(
        &mut self,
        token: OpToken,
        op: UringOp,
        op_in: &mut Option<UringOp>,
        strategy: SubmissionStrategy,
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
                                    "uring.driver.submit_operation_internal.replace_op",
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
                                        "uring.driver.submit_operation_internal.init_op",
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
                            "uring.driver.submit_operation_internal",
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
                if strategy != SubmissionStrategy::SubmitSqe {
                    if let Some(op) = self
                        .ops
                        .active_slot_bundle_mut(token)
                        .and_then(|(_, _, op, _)| op.take())
                    {
                        *op_in = Some(op);
                    }
                    return DriverSubmitResult::failed(
                        UringError::InvalidState
                            .report(
                                "uring.driver.submit_operation_internal",
                                "software timer unexpectedly entered the SQ backlog",
                            )
                            .attach_note("timer submissions must be wheel-only"),
                        SubmitStatus::Void,
                    );
                }
                debug!(user_data, "SQ full, pushing to backlog");
                match self.push_backlog(token) {
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
                    e.push_ctx("scope", "uring.driver.submit_operation_internal")
                        .attach_note("submit operation"),
                    SubmitStatus::Void,
                )
            }
        }
    }
}

fn staged_ledger_report(scope: &'static str, error: StagedLedgerError) -> Report<UringError> {
    UringError::InvalidState
        .report(
            scope,
            format!("staged ledger rejected submission receipt: {error:?}"),
        )
        .attach_note("kernel consumption could not be applied to the control-plane ledger")
}

fn submission_error_report(error: SubmissionError) -> Report<UringError> {
    match error {
        SubmissionError::Kernel(report) => report,
        SubmissionError::ReceiptUnavailable => UringError::Submission
            .report(
                "uring.submit.receipt",
                "kernel submission completed without an observable consumption receipt",
            )
            .attach_note("the affected SQEs were quarantined and will not be retried"),
    }
}
