use veloq_std::{format, time::Duration};

use diagweave::prelude::*;
use veloq_io_uring::{SubmitError as KernelSubmitError, SubmitReceipt as KernelSubmitReceipt};

use crate::{driver::control::StagedLedgerError, error::UringError};

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
/// Scope of the kernel enter receipt.
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
/// Records requested, published, and confirmed-consumed SQEs; unknown consumption is quarantined and cannot be retried, while cleanup hints remain for late CQEs.
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
    pub(super) fn requested(&self) -> usize {
        match self {
            Self::NoEntries { requested, .. }
            | Self::ConsumedPrefix { requested, .. }
            | Self::PublishedToSqPoll { requested, .. }
            | Self::Rejected { requested, .. }
            | Self::Unknown { requested, .. } => *requested,
        }
    }

    pub(super) fn published(&self) -> usize {
        match self {
            Self::NoEntries { published, .. }
            | Self::ConsumedPrefix { published, .. }
            | Self::PublishedToSqPoll { published, .. }
            | Self::Rejected { published, .. }
            | Self::Unknown { published, .. } => *published,
        }
    }

    pub(super) fn consumed(&self) -> usize {
        match self {
            Self::NoEntries { consumed, .. }
            | Self::ConsumedPrefix { consumed, .. }
            | Self::PublishedToSqPoll { consumed, .. }
            | Self::Rejected { consumed, .. }
            | Self::Unknown { consumed, .. } => *consumed,
        }
    }

    pub(super) fn scope(&self) -> SubmissionScope {
        match self {
            Self::NoEntries { scope, .. }
            | Self::ConsumedPrefix { scope, .. }
            | Self::PublishedToSqPoll { scope, .. }
            | Self::Rejected { scope, .. }
            | Self::Unknown { scope, .. } => *scope,
        }
    }

    pub(super) fn validate(&self, queue_len: usize) -> bool {
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
    pub(crate) fn new(
        receipt: SubmissionReceipt,
        staged: usize,
        kernel_outstanding: usize,
        pending_submit: bool,
        timed_out: bool,
    ) -> Self {
        Self {
            receipt,
            staged,
            kernel_outstanding,
            pending_submit,
            timed_out,
        }
    }

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
    pub(super) const fn to_submit(self) -> usize {
        self.to_submit
    }

    pub(super) const fn timeout(self) -> Option<Duration> {
        self.wait
    }

    pub(super) const fn ready_preflight(self) -> bool {
        self.ready_preflight
    }

    pub(super) const fn zero_timeout(self) -> bool {
        self.zero_timeout
    }

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

impl SubmissionReceipt {
    pub(crate) fn from_kernel_receipt(
        receipt: KernelSubmitReceipt,
        requested: usize,
        scope: SubmissionScope,
    ) -> Self {
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
    pub(crate) fn from_kernel_error(
        error: KernelSubmitError,
        requested: usize,
        scope: SubmissionScope,
        report_scope: &'static str,
    ) -> Self {
        match error {
            KernelSubmitError::Rejected { error, .. } => SubmissionReceipt::Rejected {
                requested,
                published: requested,
                consumed: 0,
                scope,
                error: SubmissionError::Kernel(
                    UringError::Submission.io_report(report_scope, error),
                ),
            },
            KernelSubmitError::Unknown { error, .. } => SubmissionReceipt::Unknown {
                requested,
                published: requested,
                consumed: 0,
                scope,
                error: SubmissionError::Kernel(
                    UringError::Submission.io_report(report_scope, error),
                ),
            },
        }
    }
}

impl SubmissionError {
    pub(crate) fn staged_ledger_report(
        scope: &'static str,
        error: StagedLedgerError,
    ) -> Report<UringError> {
        UringError::InvalidState
            .report(
                scope,
                format!("staged ledger rejected submission receipt: {error:?}"),
            )
            .attach_note("kernel consumption could not be applied to the control-plane ledger")
    }
    pub(crate) fn report(self) -> Report<UringError> {
        match self {
            SubmissionError::Kernel(report) => report,
            SubmissionError::ReceiptUnavailable => UringError::Submission
                .report(
                    "uring.submit.receipt",
                    "kernel submission completed without an observable consumption receipt",
                )
                .attach_note("the affected SQEs were quarantined and will not be retried"),
        }
    }
}
