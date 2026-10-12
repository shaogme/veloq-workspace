//! 驱动组件之间的窄生命周期上下文，只在单次提交或 drive round 内存在。
//! Narrow-lifetime contexts between driver components; each exists for one submission or drive round.
//!
//! 它们不持有父门面，用 field-level borrow split 限制组件依赖。
//! They do not own the parent facade and use field-level borrow splits to limit dependencies.

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::{
        completion::{CompletionBatchProgress, CompletionEngine},
        control::UringControlPlane,
        drive::{RoundBudget, WaitBudget, WaitPriority},
        lifecycle::{BacklogProgress, LifecycleEngine},
        operation::OperationLedger,
        registration::RegistrationEngine,
        submission::{KernelEnterPlan, SubmissionEngine, SubmitProgress},
    },
    error::{UringError, UringResult},
    op::UringSlotSpec,
};
use veloq_driver_core::driver::{
    CancelRequest, CancelSubmitOutcome, DriveMode, DrivePendingWork, DriverCompletionDiagnostics,
    SharedCompletionTable,
};
use veloq_io_uring::{IoUring, KernelCapabilities};
use veloq_std::{format, time::Duration};

mod backlog;
mod cancellation;
mod effects;
mod invariants;
mod ports;
pub(crate) use backlog::BacklogExecutor;
pub(crate) use cancellation::CancellationCoordinator;
pub(crate) use effects::CompletionEffectExecutor;
pub(crate) use invariants::ControlPlaneInvariantAuditor;
pub(crate) use ports::{
    CompletionContext, CompletionOperationBorrowParts, DrivePortAccess, DrivePorts,
    LifecycleContext, SubmitPort, SubmitPortParts,
};

pub(crate) struct DriveContext<'ops, 'registration, 'ring, 'a> {
    ops: &'ops mut OperationLedger,
    control: &'ops mut UringControlPlane,
    registration: &'registration mut RegistrationEngine<'a>,
    completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    ring: &'ring mut IoUring,
    kernel_capabilities: &'ops mut KernelCapabilities,
}

impl<'ops, 'registration, 'ring, 'a> DriveContext<'ops, 'registration, 'ring, 'a> {
    pub(crate) fn from_parts(parts: DriveContextParts<'ops, 'registration, 'ring, 'a>) -> Self {
        let DriveContextParts {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            kernel_capabilities,
        } = parts;
        Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            kernel_capabilities,
        }
    }

    pub(crate) fn diagnostics(&self) -> &DriverCompletionDiagnostics<UringCompletionDiagnostics> {
        self.diagnostics
    }

    pub(crate) fn next_timeout_hint(&self) -> UringResult<Option<Duration>> {
        self.control.timers().next_deadline().map_err(|error| {
            UringError::InvalidState
                .report(
                    "uring.timer.deadline",
                    "timer wheel deadline could not be queried",
                )
                .with_ctx("timer_error", format!("{error:?}"))
        })
    }

    pub(crate) fn control_has_pending_work(&self) -> bool {
        self.control.unpublished_staged_entry_count() > 0
            || self.control.pending_cancel_len() > 0
            || self.control.backlog_front().is_some()
            || self.control.timers().has_pending_expired()
    }

    pub(crate) fn pending_work(
        &mut self,
        remote_cancels: usize,
        lifecycle: &LifecycleEngine,
        completion: &CompletionEngine,
    ) -> DrivePendingWork {
        let cqes = {
            let mut completion = self.ring.completion();
            completion.sync();
            completion.len()
        };
        let control_pending = self.control_has_pending_work();
        DrivePendingWork {
            remote_cancels,
            lifecycle_actions: lifecycle.pending_action_count(),
            backlog_actions: self.control.backlog_len(),
            sqe_enters: if control_pending
                || !self.ring.submission().is_empty()
                || self.control.waker_stage_pending()
            {
                1
            } else {
                0
            },
            cqes,
            timers: self.control.timers().pending_expired_len(),
            effects: completion.pending_effects_len(),
        }
    }

    pub(crate) fn build_kernel_enter_plan(
        &mut self,
        mode: DriveMode,
    ) -> UringResult<KernelEnterPlan> {
        let to_submit = self.ring.submission().len();
        match mode {
            DriveMode::Poll => Ok(KernelEnterPlan::poll(to_submit)),
            DriveMode::Wait { timeout } => {
                let cq_ready = {
                    let mut completion = self.ring.completion();
                    completion.sync();
                    !completion.is_empty()
                };
                let user_ready = self.ops.shared.has_ready_completion();
                let waker_ready = cq_ready && !user_ready;
                let timer_deadline = self.next_timeout_hint()?;
                let priority = WaitPriority::resolve(
                    user_ready,
                    waker_ready,
                    timer_deadline.is_some_and(|duration| duration.is_zero()),
                    timeout.is_some_and(|duration| duration.is_zero()),
                    timeout.is_some(),
                );
                if matches!(
                    priority,
                    WaitPriority::UserCompletion | WaitPriority::WakerCompletion
                ) {
                    self.diagnostics.backend().inc_wait_ready_preflight();
                    return Ok(KernelEnterPlan::poll_ready(to_submit));
                }

                let budget = WaitBudget::resolve(timeout, timer_deadline, Duration::from_secs(1));
                if budget.duration().is_zero() {
                    self.diagnostics.backend().inc_wait_zero();
                    Ok(KernelEnterPlan::poll_zero_timeout(to_submit))
                } else {
                    self.diagnostics.backend().inc_wait_block();
                    Ok(KernelEnterPlan::wait(
                        to_submit,
                        budget.duration(),
                        budget.source(),
                    ))
                }
            }
        }
    }

    pub(crate) fn submit_waker(&mut self, submission: &mut SubmissionEngine) -> UringResult<()> {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = self;
        let mut context = SubmitPort::from_parts(SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        });
        submission.submit_waker(&mut context)
    }

    pub(crate) fn submit_to_kernel(
        &mut self,
        submission: &mut SubmissionEngine,
        plan: KernelEnterPlan,
    ) -> UringResult<SubmitProgress> {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = self;
        let mut context = SubmitPort::from_parts(SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        });
        submission.submit_to_kernel(&mut context, plan)
    }

    pub(crate) fn enter_submission_fail_stop(&mut self, submission: &mut SubmissionEngine) {
        submission.enter_fail_stop();
    }

    pub(crate) fn process_completion_batch(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        let mut ports = self.ports();
        DrivePortAccess::process_completion_batch(&mut ports, lifecycle, completion, budget)
    }

    pub(crate) fn cancel_operation(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        submission: &mut SubmissionEngine,
        request: CancelRequest,
    ) -> UringResult<CancelSubmitOutcome> {
        let mut ports = self.ports();
        CancellationCoordinator::cancel_operation(
            &mut ports, lifecycle, completion, submission, request,
        )
    }

    pub(crate) fn drain_cancel_requests_bounded(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let mut ports = self.ports();
        CancellationCoordinator::drain_cancel_requests_bounded(
            &mut ports, lifecycle, completion, limit,
        )
    }

    pub(crate) fn stage_pending_cancellations(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let mut ports = self.ports();
        CancellationCoordinator::stage_pending_cancellations(
            &mut ports, lifecycle, completion, limit,
        )
    }

    pub(crate) fn stage_backlog_entries(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        submission: &mut SubmissionEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<BacklogProgress> {
        let mut ports = self.ports();
        BacklogExecutor::stage_entries(&mut ports, lifecycle, submission, completion, limit)
    }

    fn ports(&mut self) -> DrivePorts<'_, '_, '_, 'a> {
        let Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            kernel_capabilities,
        } = self;
        DrivePorts {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            kernel_capabilities,
        }
    }
}

pub(crate) struct DriveContextParts<'ops, 'registration, 'ring, 'a> {
    pub(crate) ops: &'ops mut OperationLedger,
    pub(crate) control: &'ops mut UringControlPlane,
    pub(crate) registration: &'registration mut RegistrationEngine<'a>,
    pub(crate) completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    pub(crate) diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) ring: &'ring mut IoUring,
    pub(crate) kernel_capabilities: &'ops mut KernelCapabilities,
}
