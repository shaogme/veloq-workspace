pub(crate) mod txn;

use self::txn::{UringSubmitTxn, slot_access_report};
use crate::{
    driver::{
        context::SubmitPort,
        control::StagedLedgerError,
        env::{StageResult, SubmitEnv},
        lifecycle::SubmissionPhase,
        registration::file_table::SqeFd,
    },
    error::{UringError, UringResult},
    op::{
        Reserved, Slot, SlotView, SubmissionStrategy, UringOp, UringOpRegistryExt, opcode_build,
        sqe_with_fd,
    },
};
use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionToken, DriverSubmitResult, OpToken, SubmitStatus, SubmitTokenContext},
    slot::{CheckedSlotView, InFlightWaiting},
};
use veloq_io_uring::{
    EnterArgs, EnterFlags, SubmitError as KernelSubmitError, SubmitReceipt as KernelSubmitReceipt,
    opcode,
    types::{self, SubmitArgs, Timespec},
};
use veloq_std::{format, task::Poll, time::Duration};

pub(crate) struct SubmissionEngine {
    fail_stop: bool,
}

impl SubmissionEngine {
    pub(crate) const fn new() -> Self {
        Self { fail_stop: false }
    }

    pub(crate) fn enter_fail_stop(&mut self) {
        self.fail_stop = true;
    }
}

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

/// 内核 enter 的证据范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionScope {
    Enter,
    ExtendedWait,
    SqPoll,
}

/// 不可绕过的 submission 回执。
///
/// `requested` 是本次 enter 看到的 SQE 总数，`published` 是已经写入用户态 SQ
/// 并作为本次 enter 输入的数量，`consumed` 是已经得到内核确认消费的前缀长度。
/// `Unknown` 即使携带了错误也不能重试：对应的 staged metadata 会被隔离，保留
/// cleanup hint 以便迟到 CQE 仍能完成清理。
#[derive(Debug)]
pub(crate) enum SubmissionReceipt {
    NoEntries {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    ConsumedPrefix {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    PublishedToSqPoll {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
    },
    Rejected {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
        error: SubmissionError,
    },
    Unknown {
        requested: usize,
        published: usize,
        consumed: usize,
        scope: SubmissionScope,
        error: SubmissionError,
    },
}

impl SubmissionReceipt {
    fn requested(&self) -> usize {
        match self {
            Self::NoEntries { requested, .. }
            | Self::ConsumedPrefix { requested, .. }
            | Self::PublishedToSqPoll { requested, .. }
            | Self::Rejected { requested, .. }
            | Self::Unknown { requested, .. } => *requested,
        }
    }

    fn published(&self) -> usize {
        match self {
            Self::NoEntries { published, .. }
            | Self::ConsumedPrefix { published, .. }
            | Self::PublishedToSqPoll { published, .. }
            | Self::Rejected { published, .. }
            | Self::Unknown { published, .. } => *published,
        }
    }

    fn consumed(&self) -> usize {
        match self {
            Self::NoEntries { consumed, .. }
            | Self::ConsumedPrefix { consumed, .. }
            | Self::PublishedToSqPoll { consumed, .. }
            | Self::Rejected { consumed, .. }
            | Self::Unknown { consumed, .. } => *consumed,
        }
    }

    fn scope(&self) -> SubmissionScope {
        match self {
            Self::NoEntries { scope, .. }
            | Self::ConsumedPrefix { scope, .. }
            | Self::PublishedToSqPoll { scope, .. }
            | Self::Rejected { scope, .. }
            | Self::Unknown { scope, .. } => *scope,
        }
    }

    fn validate(&self, queue_len: usize) -> bool {
        let evidence_valid =
            self.published() <= self.requested() && self.consumed() <= self.published();
        let kind_valid = match self {
            Self::NoEntries { .. }
            | Self::PublishedToSqPoll { .. }
            | Self::Rejected { .. }
            | Self::Unknown { .. } => self.consumed() == 0,
            Self::ConsumedPrefix { .. } => true,
        };
        self.requested() == queue_len && evidence_valid && kind_valid
    }
}

#[derive(Debug)]
pub(crate) struct SubmitProgress {
    receipt: SubmissionReceipt,
    staged: usize,
    kernel_outstanding: usize,
    pending_submit: bool,
    timed_out: bool,
}

impl SubmitProgress {
    pub(crate) fn receipt(&self) -> &SubmissionReceipt {
        &self.receipt
    }

    pub(crate) const fn staged(&self) -> usize {
        self.staged
    }

    pub(crate) const fn kernel_outstanding(&self) -> usize {
        self.kernel_outstanding
    }

    pub(crate) const fn pending_submit(&self) -> bool {
        self.pending_submit
    }

    pub(crate) const fn timed_out(&self) -> bool {
        self.timed_out
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct KernelEnterPlan {
    to_submit: usize,
    wait: Option<Duration>,
    wait_source: Option<WaitBudgetSource>,
    ready_preflight: bool,
    zero_timeout: bool,
}

impl KernelEnterPlan {
    pub(crate) const fn wait_source(self) -> Option<WaitBudgetSource> {
        self.wait_source
    }

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
/// Takes the registry split in two: `slot` borrows out of the operation registry, `env` covers every
/// other field the path needs. Both halves stay live at once — `make_sqe` reads the registered
/// file table while holding the op, and the SQE push happens while the slot's platform data is
/// still being updated — which is exactly why the split is a borrow-checked projection rather
/// than a mutable parent object reconstructed from a raw pointer.
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
    if slot.platform().submission_phase() != SubmissionPhase::Reserved {
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
            to_submit = plan.to_submit,
            wait = ?plan.wait,
            ready_preflight = plan.ready_preflight,
            zero_timeout = plan.zero_timeout,
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
        if to_submit != plan.to_submit {
            context.quarantine_unpublished_staged();
            self.fail_stop = true;
            return Err(UringError::InvalidState
                .report(
                    "uring.submit_to_kernel.plan",
                    "submission queue changed after the kernel enter plan was built",
                )
                .with_ctx("planned", plan.to_submit)
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
                return Err(submission_error_report(error));
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
                    return Err(staged_ledger_report("uring.submit.receipt", error));
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
        Ok(SubmitProgress {
            receipt,
            staged: unpublished,
            kernel_outstanding,
            pending_submit,
            timed_out,
        })
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

        if context.submission_is_sqpoll() && plan.wait.is_none() {
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
                    Ok(receipt) => map_kernel_receipt(receipt, to_submit, SubmissionScope::SqPoll),
                    Err(error) => map_kernel_error(
                        error,
                        to_submit,
                        SubmissionScope::SqPoll,
                        "driver.submit_to_kernel.submit.sqpoll",
                    ),
                },
                false,
            ));
        }

        if let Some(timeout) = plan.wait {
            let timespec = Timespec::try_from(timeout).map_err(|error| {
                UringError::InvalidInput.io_report("driver.submit_to_kernel.timespec", error)
            })?;
            let args = SubmitArgs::new().timespec(&timespec);
            match context.kernel_submit_with_args(1, &args) {
                Ok(receipt) => Ok((
                    map_kernel_receipt(receipt, to_submit, SubmissionScope::ExtendedWait),
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
                    map_kernel_error(
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
                    map_kernel_receipt(receipt, to_submit, SubmissionScope::Enter),
                    false,
                )),
                Err(error) => Ok((
                    map_kernel_error(
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
        token: OpToken,
        op: UringOp,
        op_in: &mut Option<UringOp>,
        strategy: SubmissionStrategy,
    ) -> DriverSubmitResult<UringError> {
        let user_data = token.index();
        let outcome = {
            let (ops, mut env) = context.split_for_submit();
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

fn map_kernel_receipt(
    receipt: KernelSubmitReceipt,
    requested: usize,
    scope: SubmissionScope,
) -> SubmissionReceipt {
    match receipt {
        KernelSubmitReceipt::NoEntries { .. } => SubmissionReceipt::NoEntries {
            requested,
            published: requested,
            consumed: 0,
            scope,
        },
        KernelSubmitReceipt::PublishedToSqpoll { published, .. } => {
            SubmissionReceipt::PublishedToSqPoll {
                requested,
                published: published as usize,
                consumed: 0,
                scope,
            }
        }
        KernelSubmitReceipt::Submitted {
            requested: kernel_requested,
            submitted,
            ..
        } => {
            if kernel_requested == 0 {
                SubmissionReceipt::NoEntries {
                    requested: kernel_requested as usize,
                    published: kernel_requested as usize,
                    consumed: 0,
                    scope,
                }
            } else {
                SubmissionReceipt::ConsumedPrefix {
                    requested: kernel_requested as usize,
                    published: kernel_requested as usize,
                    consumed: submitted as usize,
                    scope,
                }
            }
        }
    }
}

fn map_kernel_error(
    error: KernelSubmitError,
    requested: usize,
    scope: SubmissionScope,
    report_scope: &'static str,
) -> SubmissionReceipt {
    match error {
        KernelSubmitError::Rejected { error, .. } => SubmissionReceipt::Rejected {
            requested,
            published: requested,
            consumed: 0,
            scope,
            error: SubmissionError::Kernel(UringError::Submission.io_report(report_scope, error)),
        },
        KernelSubmitError::Unknown { error, .. } => SubmissionReceipt::Unknown {
            requested,
            published: requested,
            consumed: 0,
            scope,
            error: SubmissionError::Kernel(UringError::Submission.io_report(report_scope, error)),
        },
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
