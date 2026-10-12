//! uring submission 的字段级借用投影。
//! Field-level borrow projections for the uring submission path.
//!
//! SQE 构造同时借用 slot 和提交资源；将非 slot 状态投影为窄视图，由借用检查器保证两侧互不别名。
//! SQE construction borrows both a slot and submission resources; narrow views let the borrow checker verify they do not alias.

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::{
        control::PendingCancel,
        lifecycle::{SubmissionPhase, UringOpState},
        registration::{ChunkRegistrationRequest, FileTable, FixedBufferSubmitPort},
    },
    error::UringResult,
    op::CompletionCleanupHintFn,
};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{CancelTicket, OpToken};
use veloq_io_uring::{Submitter, squeue};
use veloq_std::time::Duration;
use veloq_wheel::TimerId;

mod completion_control;
mod cqe;
mod parts;
mod submission;

pub(crate) use completion_control::{CompletionControlView, CompletionLedgerParts};
pub(crate) use cqe::CqeEnv;
pub(crate) use parts::{
    ChunkRegistrationDecision, CompletionControlParts, CompletionMetadata, ProvidedBufSqeInfo,
    SqeEnv,
};
pub(crate) use submission::{
    StageResult, SubmissionControlParts, SubmissionLedgerParts, SubmissionSidecarParts,
    SubmitControlView,
};

/// 提交路径可读取或更新的资源注册字段。
/// Resource-registration fields a submission may read or update.
pub(crate) struct SubmitResourceView<'d, 'r> {
    submitter: Submitter<'d>,
    file_table: &'d FileTable,
    registration_diagnostics: &'d UringCompletionDiagnostics,
    buffers: FixedBufferSubmitPort<'d, 'r>,
}

/// 将控制面与资源投影组合为提交事务使用的窄环境。
/// Combines control and resource projections into the narrow environment used by submission transactions.
pub(crate) struct SubmitEnv<'d, 'r> {
    control: SubmitControlView<'d>,
    resources: SubmitResourceView<'d, 'r>,
}

impl<'d, 'r> SubmitResourceView<'d, 'r> {
    pub(crate) fn from_registration_parts(
        submitter: Submitter<'d>,
        file_table: &'d FileTable,
        registration_diagnostics: &'d UringCompletionDiagnostics,
        buffers: FixedBufferSubmitPort<'d, 'r>,
    ) -> Self {
        Self {
            submitter,
            file_table,
            registration_diagnostics,
            buffers,
        }
    }

    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        self.buffers.sqe_env(self.file_table)
    }

    #[inline]
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        self.buffers.ensure_chunk_registered(
            &self.submitter,
            self.registration_diagnostics,
            ChunkRegistrationRequest {
                chunk_id,
                user_data,
                scope,
            },
        )
    }
}

pub(crate) struct SubmitParts<'d, 'r> {
    control: SubmitControlView<'d>,
    resources: SubmitResourceView<'d, 'r>,
}

impl<'d, 'r> SubmitParts<'d, 'r> {
    pub(crate) fn new(
        control: SubmitControlView<'d>,
        resources: SubmitResourceView<'d, 'r>,
    ) -> Self {
        Self { control, resources }
    }
}

pub(crate) struct SubmitEnvironmentFactory;

impl SubmitEnvironmentFactory {
    pub(crate) fn resource_view<'d, 'r>(
        submitter: Submitter<'d>,
        file_table: &'d FileTable,
        diagnostics: &'d UringCompletionDiagnostics,
        buffers: FixedBufferSubmitPort<'d, 'r>,
    ) -> SubmitResourceView<'d, 'r> {
        SubmitResourceView::from_registration_parts(submitter, file_table, diagnostics, buffers)
    }

    pub(crate) fn build<'d, 'r>(parts: SubmitParts<'d, 'r>) -> SubmitEnv<'d, 'r> {
        SubmitEnv {
            control: parts.control,
            resources: parts.resources,
        }
    }
}

impl SubmitEnv<'_, '_> {
    #[inline]
    pub(crate) fn transition_submission_state(
        &mut self,
        token: OpToken,
        state: &mut UringOpState,
        next: SubmissionPhase,
        reason: &'static str,
    ) {
        self.control
            .transition_submission_state(token, state, next, reason);
    }

    #[inline]
    pub(crate) fn cancel_timer(&mut self, token: OpToken, task_id: TimerId) {
        self.control.cancel_timer(token, task_id);
    }

    #[inline]
    pub(crate) fn record_timer_insert(&mut self, token: OpToken, task_id: TimerId) {
        self.control.record_timer_insert(token, task_id);
    }

    #[inline]
    pub(crate) fn insert_timer(
        &mut self,
        token: OpToken,
        duration: Duration,
    ) -> UringResult<TimerId> {
        self.control.insert_timer(token, duration)
    }

    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        self.resources.sqe_env()
    }

    #[inline]
    pub(crate) fn stage_user_entry(
        &mut self,
        token: OpToken,
        entry: squeue::Entry,
        cleanup_hint: Option<CompletionCleanupHintFn>,
    ) -> UringResult<StageResult> {
        self.control.stage_user_entry(token, entry, cleanup_hint)
    }

    #[inline]
    pub(crate) fn stage_cancel_entry(
        &mut self,
        ticket: CancelTicket,
        request: PendingCancel,
        entry: squeue::Entry,
    ) -> UringResult<StageResult> {
        self.control.stage_cancel_entry(ticket, request, entry)
    }

    #[inline]
    pub(crate) fn stage_waker_entry(&mut self, entry: squeue::Entry) -> UringResult<StageResult> {
        self.control.stage_waker_entry(entry)
    }

    #[inline]
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        self.resources
            .ensure_chunk_registered(chunk_id, user_data, scope)
    }
}
