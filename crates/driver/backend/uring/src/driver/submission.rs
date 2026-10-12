mod receipt;
mod txn;

use crate::{
    driver::{
        context::SubmitPort,
        env::{StageResult, SubmitEnv},
        lifecycle::SubmissionPhase,
        registration::SqeFd,
    },
    error::{UringError, UringResult},
    op::{
        CompletionCleanupHintFn, Reserved, Slot, SlotView, SubmissionStrategy, UringOp,
        UringOpRegistryExt, opcode_build, sqe_with_fd,
    },
};
use diagweave::prelude::*;
pub(crate) use receipt::{
    KernelEnterPlan, SubmissionError, SubmissionReceipt, SubmissionScope, SubmitProgress,
    WaitBudgetSource,
};
use tracing::{debug, trace};
pub(crate) use txn::{SlotAccessReporter, UringSubmitTxn};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionToken, DriverSubmitResult, OpToken, SubmitStatus, SubmitTokenContext},
    slot::{CheckedSlotView, InFlightWaiting},
};
use veloq_io_uring::{
    EnterArgs, EnterFlags, opcode, squeue,
    types::{self, SubmitArgs, Timespec},
};
use veloq_std::{format, task::Poll};

pub(crate) struct SubmissionEngine {
    fail_stop: bool,
}

pub(crate) struct OperationSubmitRequest<'op> {
    pub(crate) token: OpToken,
    pub(crate) op: UringOp,
    pub(crate) op_in: &'op mut Option<UringOp>,
    pub(crate) strategy: SubmissionStrategy,
}

pub(crate) struct ResolvedSqe {
    pub(crate) chunk_count: usize,
    pub(crate) entry: squeue::Entry,
    pub(crate) cleanup_hint: Option<CompletionCleanupHintFn>,
}

impl SubmissionEngine {
    pub(crate) const fn new() -> Self {
        Self { fail_stop: false }
    }

    pub(crate) fn enter_fail_stop(&mut self) {
        self.fail_stop = true;
    }
}

/// Turns a reserved slot's op into an SQE (or a wheel entry) and hands it to the kernel.
///
/// Takes the registry split in two: `slot` borrows out of the operation registry, `env` covers every
/// other field the path needs. Both halves stay live at once — `make_sqe` reads the registered
/// file table while holding the op, and the SQE push happens while the slot's platform data is
/// still being updated — which is exactly why the split is a borrow-checked projection rather
/// than a mutable parent object reconstructed from a raw pointer.
/// slot 与环境分别借用各自字段，避免持有 slot 时重新取得父对象的可变引用；两种访问可在借用检查下并存。
fn submit_from_slot(
    env: &mut SubmitEnv<'_, '_>,
    token: OpToken,
    slot: Slot<'_, Reserved>,
) -> UringResult<bool> {
    UringSubmitTxn::new(env, token, slot)?.submit()
}

/// Retries a backlogged submission whose SQE never made it into a full ring. 重试因 SQ ring 满而未能入队的 backlog 提交。
fn submit_queued_from_slot(
    env: &mut SubmitEnv<'_, '_>,
    token: OpToken,
    mut slot: Slot<'_, InFlightWaiting>,
) -> UringResult<bool> {
    let user_data = token.index();
    if slot.platform().submission_phase() != SubmissionPhase::Reserved {
        return Ok(true);
    }

    let strategy = slot
        .with_access_mut(|access| access.operation().get_ref().descriptor().strategy)
        .map_err(|err| {
            SlotAccessReporter::report("driver.submit_queued_from_slot.strategy", err)
        })?;
    if strategy != SubmissionStrategy::SubmitSqe {
        return UringError::InvalidState
            .push_ctx("scope", "driver.submit_queued_from_slot.strategy")
            .with_ctx("user_data", user_data)
            .with_ctx("strategy", format!("{strategy:?}"))
            .attach_note("queued uring backlog entry is not an SQE operation");
    }

    let mut chunks = [ChunkId::ZERO; 4];
    let resolved = {
        let sqe_env = env.sqe_env();
        slot.with_access_mut(|access| {
            let descriptor = access.operation().get_ref().descriptor();
            let count = unsafe { (descriptor.resolve_chunks)(access, token, &mut chunks) }?;
            SubmissionEngine::validate_resolved_chunk_count(
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
            Ok::<_, Report<UringError>>(ResolvedSqe {
                chunk_count: count,
                entry: sqe,
                cleanup_hint: descriptor.completion_cleanup_hint,
            })
        })
        .map_err(|err| {
            SlotAccessReporter::report("driver.submit_queued_from_slot.op_payload", err)
        })??
    };

    for &chunk_id in chunks.iter().take(resolved.chunk_count) {
        let _decision = env.ensure_chunk_registered(
            chunk_id,
            user_data,
            "driver.submit_queued_from_slot.ensure_chunk_registered",
        )?;
    }

    if env.stage_user_entry(token, resolved.entry, resolved.cleanup_hint)? == StageResult::Staged {
        env.transition_submission_state(
            token,
            slot.platform_mut(),
            SubmissionPhase::SqeStaged,
            "queued submission staged SQE",
        );
        trace!(user_data, "Submitted queued backlog entry to SQ");
        Ok(true)
    } else {
        env.transition_submission_state(
            token,
            slot.platform_mut(),
            SubmissionPhase::Reserved,
            "queued submission remains queued after SQ full",
        );
        debug!(user_data, "SQ still full for queued backlog entry");
        Ok(false)
    }
}

impl SubmissionEngine {
    pub(crate) fn validate_resolved_chunk_count(
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

    pub(crate) fn submit_from_slot_token(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
        token: OpToken,
    ) -> UringResult<bool> {
        let (ops, mut env) = context.split_for_submit();
        let slot = match ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(UringError::InvalidState
                    .report("driver.submit_from_slot_index", "op missing in slot"));
            }
        };
        let result = submit_from_slot(&mut env, token, slot);
        if matches!(result, Ok(true)) {
            let timer = match ops.checked_slot_view(token)? {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))
                    if slot.platform().submission_phase() == SubmissionPhase::TimerArmed =>
                {
                    slot.platform().timer_id()
                }
                _ => None,
            };
            if let Some(timer) = timer {
                ops.arm_timer(token, timer)?;
            }
        }
        result
    }

    pub(crate) fn submit_queued_from_slot_token(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
        token: OpToken,
    ) -> UringResult<bool> {
        let (ops, mut env) = context.split_for_submit();
        match ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                submit_queued_from_slot(&mut env, token, slot)
            }
            _ => Ok(true),
        }
    }

    pub(crate) fn submit_waker(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
    ) -> UringResult<()> {
        if context.waker_is_armed() {
            return Ok(());
        }

        let waker_fd = match context.waker_registered_fd() {
            Some(fd) => fd,
            None => {
                let raw = context.waker_current_raw();
                let mut fds = context.register_waker_file(raw)?;
                let waker_fd = fds.pop().ok_or_else(|| {
                    UringError::InvalidState
                        .report("driver.submit_waker", "register_files returned empty")
                })?;
                context.set_waker_registered_fd(Some(waker_fd));
                waker_fd
            }
        };
        // The eventfd is registered like any other descriptor, so it lands in the fallback area
        // once the kernel table is full (or configured away entirely).
        // eventfd 与其他描述符一样注册；内核表满或禁用时会落入回退区域。
        let sqe_fd = context.resolve_waker_file(waker_fd)?;
        let (buf, len) = context.waker_buffer();
        let sqe = opcode_build(
            "driver.submit_waker.opcode",
            sqe_with_fd!(sqe_fd, |f| unsafe { opcode::Read::new(f, buf, len) }
                .build()),
        )?
        .user_data(CompletionToken::waker(0).raw());

        let staged = {
            let (_, mut env) = context.split_for_submit();
            env.stage_waker_entry(sqe)? == StageResult::Staged
        };
        if staged {
            context.set_waker_stage_pending(false);
            context.commit_waker_stage();
            Ok(())
        } else {
            context.set_waker_stage_pending(true);
            Ok(())
        }
    }

    pub(crate) fn submit_to_kernel(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
        plan: KernelEnterPlan,
    ) -> UringResult<SubmitProgress> {
        trace!(
            to_submit = plan.to_submit(),
            wait = ?plan.timeout(),
            ready_preflight = plan.ready_preflight(),
            zero_timeout = plan.zero_timeout(),
            "submit_to_kernel entered"
        );
        if self.fail_stop {
            return Err(UringError::Submission
                .report(
                    "uring.submit_to_kernel.fail_stop",
                    "submission receipt was previously unknown",
                )
                .attach_note("driver cannot safely reuse an SQE after an unknown receipt"));
        }

        let to_submit = context.submission_len();
        if to_submit != plan.to_submit() {
            context.quarantine_unpublished_staged();
            self.fail_stop = true;
            return Err(UringError::InvalidState
                .report(
                    "uring.submit_to_kernel.plan",
                    "submission queue changed after the kernel enter plan was built",
                )
                .with_ctx("planned", plan.to_submit())
                .with_ctx("actual", to_submit));
        }

        let unpublished = context.unpublished_staged_entry_count();
        if unpublished > to_submit {
            context.quarantine_unpublished_staged();
            self.fail_stop = true;
            return Err(UringError::InvalidState
                .report(
                    "uring.submit_to_kernel.ledger",
                    "staged metadata exceeds entries visible in the submission queue",
                )
                .with_ctx("unpublished", unpublished)
                .with_ctx("queue_entries", to_submit));
        }
        let published_in_queue = to_submit - unpublished;
        let (receipt, timed_out) =
            self.submit_staged_batch(context, plan, to_submit, unpublished)?;
        if !receipt.validate(to_submit) {
            context.quarantine_unpublished_staged();
            self.fail_stop = true;
            return Err(UringError::InvalidState
                .report(
                    "uring.submit.receipt.evidence",
                    "submission receipt evidence does not describe the planned SQ",
                )
                .with_ctx("requested", receipt.requested())
                .with_ctx("published", receipt.published())
                .with_ctx("consumed", receipt.consumed())
                .with_ctx("queue_len", to_submit)
                .with_ctx("scope", format!("{:?}", receipt.scope()))
                .attach_note("ambiguous staged metadata was quarantined"));
        }
        let receipt = match receipt {
            SubmissionReceipt::Unknown { error, .. } => {
                context.quarantine_unpublished_staged();
                self.fail_stop = true;
                return Err(error.report());
            }
            receipt => receipt,
        };
        let kernel_outstanding = match &receipt {
            SubmissionReceipt::NoEntries { .. } => 0,
            SubmissionReceipt::Rejected { error, .. } => {
                let _ = error;
                0
            }
            SubmissionReceipt::ConsumedPrefix {
                requested,
                published: _,
                consumed,
                ..
            } => match context.mark_staged_consumed(*requested, *consumed, published_in_queue) {
                Ok(consumed) => consumed,
                Err(error) => {
                    context.quarantine_unpublished_staged();
                    self.fail_stop = true;
                    return Err(SubmissionError::staged_ledger_report(
                        "uring.submit.receipt",
                        error,
                    ));
                }
            },
            SubmissionReceipt::PublishedToSqPoll { published, .. } => {
                if *published < unpublished {
                    context.quarantine_unpublished_staged();
                    self.fail_stop = true;
                    return Err(UringError::InvalidState
                        .report(
                            "uring.submit.receipt.sqpoll",
                            "published receipt is shorter than unpublished ledger entries",
                        )
                        .with_ctx("published", *published)
                        .with_ctx("unpublished", unpublished));
                }
                let marked = context.mark_staged_published();
                if marked != unpublished {
                    context.quarantine_unpublished_staged();
                    self.fail_stop = true;
                    return Err(UringError::InvalidState
                        .report(
                            "uring.submit.receipt.sqpoll.ledger",
                            "SQPOLL publication did not settle the staged ledger prefix",
                        )
                        .with_ctx("marked", marked)
                        .with_ctx("unpublished", unpublished));
                }
                0
            }
            SubmissionReceipt::Unknown { .. } => unreachable!("unknown receipt was returned"),
        };
        context.settle_kernel_submission_phases();

        let pending_submit = context.unpublished_staged_entry_count() > 0;
        Ok(SubmitProgress::new(
            receipt,
            unpublished,
            kernel_outstanding,
            pending_submit,
            timed_out,
        ))
    }

    fn submit_staged_batch(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
        plan: KernelEnterPlan,
        to_submit: usize,
        unpublished: usize,
    ) -> UringResult<(SubmissionReceipt, bool)> {
        if to_submit > u32::MAX as usize {
            return Ok((
                SubmissionReceipt::Rejected {
                    requested: to_submit,
                    published: to_submit,
                    consumed: 0,
                    scope: SubmissionScope::Enter,
                    error: SubmissionError::ReceiptUnavailable,
                },
                false,
            ));
        }

        if context.submission_is_sqpoll() && plan.timeout().is_none() {
            if to_submit == 0 || !context.submission_need_wakeup() {
                if unpublished == 0 {
                    return Ok((
                        SubmissionReceipt::NoEntries {
                            requested: to_submit,
                            published: to_submit,
                            consumed: 0,
                            scope: SubmissionScope::SqPoll,
                        },
                        false,
                    ));
                }
                return Ok((
                    SubmissionReceipt::PublishedToSqPoll {
                        requested: to_submit,
                        published: to_submit,
                        consumed: 0,
                        scope: SubmissionScope::SqPoll,
                    },
                    false,
                ));
            }

            let consumed = context.kernel_submit();
            return Ok((
                match consumed {
                    Ok(receipt) => SubmissionReceipt::from_kernel_receipt(
                        receipt,
                        to_submit,
                        SubmissionScope::SqPoll,
                    ),
                    Err(error) => SubmissionReceipt::from_kernel_error(
                        error,
                        to_submit,
                        SubmissionScope::SqPoll,
                        "driver.submit_to_kernel.submit.sqpoll",
                    ),
                },
                false,
            ));
        }

        if let Some(timeout) = plan.timeout() {
            let timespec = Timespec::try_from(timeout).map_err(|error| {
                UringError::InvalidInput.io_report("driver.submit_to_kernel.timespec", error)
            })?;
            let args = SubmitArgs::new().timespec(&timespec);
            match context.kernel_submit_with_args(1, &args) {
                Ok(receipt) => Ok((
                    SubmissionReceipt::from_kernel_receipt(
                        receipt,
                        to_submit,
                        SubmissionScope::ExtendedWait,
                    ),
                    false,
                )),
                Err(error)
                    if error.error().raw_os_error() == Some(libc::ETIME) && to_submit == 0 =>
                {
                    Ok((
                        SubmissionReceipt::NoEntries {
                            requested: to_submit,
                            published: to_submit,
                            consumed: 0,
                            scope: SubmissionScope::ExtendedWait,
                        },
                        true,
                    ))
                }
                Err(error) => Ok((
                    SubmissionReceipt::from_kernel_error(
                        error,
                        to_submit,
                        SubmissionScope::ExtendedWait,
                        "driver.submit_to_kernel.wait",
                    ),
                    false,
                )),
            }
        } else {
            let args = EnterArgs::new(to_submit as u32, 0).flags(EnterFlags::GETEVENTS);
            match context.kernel_enter(args) {
                Ok(receipt) => Ok((
                    SubmissionReceipt::from_kernel_receipt(
                        receipt,
                        to_submit,
                        SubmissionScope::Enter,
                    ),
                    false,
                )),
                Err(error) => Ok((
                    SubmissionReceipt::from_kernel_error(
                        error,
                        to_submit,
                        SubmissionScope::Enter,
                        "driver.submit_to_kernel.enter",
                    ),
                    false,
                )),
            }
        }
    }

    #[inline]
    pub(crate) fn submit_operation_internal(
        &mut self,
        context: &mut SubmitPort<'_, '_, '_, '_>,
        request: OperationSubmitRequest<'_>,
    ) -> DriverSubmitResult<UringError> {
        let OperationSubmitRequest {
            token,
            op,
            op_in,
            strategy,
        } = request;
        let user_data = token.index();
        let outcome = {
            let (ops, mut env) = context.split_for_submit();
            let slot = match ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => {
                    if slot.has_op() {
                        let mut slot = slot;
                        if let Err(err) = slot.replace_op(op) {
                            return DriverSubmitResult::failed(
                                SlotAccessReporter::report(
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
                                    SlotAccessReporter::report(
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
                    if let Some(op) = context.take_operation_from_slot(token) {
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
                match Self::push_backlog(context, token) {
                    Ok(()) => DriverSubmitResult::submitted(Poll::Pending),
                    Err(report) => {
                        if let Some(op) = context.take_operation_from_slot(token) {
                            *op_in = Some(op);
                        }
                        DriverSubmitResult::failed(report, SubmitStatus::Void)
                    }
                }
            }
            Err(e) => {
                if let Some(op) = context.take_operation_from_slot(token) {
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

    pub(crate) fn push_backlog(
        context: &mut SubmitPort<'_, '_, '_, '_>,
        token: OpToken,
    ) -> UringResult<()> {
        if let Err(error) = context.push_backlog(token) {
            return Err(UringError::InvalidState
                .report(
                    "uring.backlog.push",
                    format!("backlog rejected token: {error:?}"),
                )
                .with_ctx("token", token.index())
                .with_ctx("generation", token.generation().get()));
        }
        Ok(())
    }
}
