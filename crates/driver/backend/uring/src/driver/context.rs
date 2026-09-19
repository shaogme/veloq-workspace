//! 驱动组件之间的窄、短生命周期上下文。
//!
//! 这些类型不持有父门面。上下文只在一次提交或一次 drive round 内存在，
//! 用来保持 field-level borrow split，并把组件之间的依赖限制在最小端口上。

use crate::{
    config::{IoFd, RawHandle, UringRawHandle},
    diagnostics::UringCompletionDiagnostics,
    driver::{
        completion::{
            CompletionBatchProgress, CompletionEngine, UringSyntheticCompletion,
            effects::{self, CompletionEffectBatch, CompletionEffectKind},
        },
        control::{
            BacklogError, ControlPlaneEvent, DeferredCancelReconcile, StagedLedgerError,
            UringControlPlane, UringWakerManager,
        },
        drive::{EffectExecutor, RoundBudget},
        env::{SubmitEnv, SubmitEnvironmentFactory, SubmitParts},
        lifecycle::{CancellationPhase, LifecycleCompletionAction, LifecycleEngine},
        operation::OperationLedger,
        registration::{
            RegistrationEngine, RegistrationPorts, file_table::SqeFd, provided_buf::ProvidedBufPort,
        },
        submission::{KernelEnterPlan, SubmissionEngine, SubmitProgress},
    },
    error::{UringError, UringResult},
    op::{CheckedSlotView, RecordPolicy, SlotView, UringOp, UringOpRegistryExt, UringSlotSpec},
};
use diagweave::prelude::*;
use tracing::trace;
use veloq_driver_core::driver::{
    AnomalyAttach, CancelRequest, CancelSubmitOutcome, CompletionAnomalyKind,
    CompletionFlowOutcome, DriveMode, DrivePendingWork, DriverCompletionDiagnostics, OpToken,
    RegisterFd, SharedCompletionTable, SyntheticCompletionSource, UserCompletionEvent,
};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_driver_core::driver::{CancelTicket, CompletionToken};
use veloq_io_uring::{
    EnterArgs, IoUring, KernelCapabilities, SubmitError as KernelSubmitError,
    SubmitReceipt as KernelSubmitReceipt, types::SubmitArgs,
};
use veloq_std::{
    format,
    time::{Duration, Instant},
    vec,
    vec::Vec,
};

use crate::driver::drive::{WaitPriority, wait_budget, wait_priority};
use crate::driver::lifecycle::{BacklogAction, BacklogProgress, SubmissionPhase};

#[cfg(any(test, feature = "test-hooks"))]
use crate::driver::control::{ControlInvariantError, ControlPlaneSnapshot, ControlTokenSnapshot};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_std::collections::HashSet;

fn remember_first_error(first_error: &mut Option<Report<UringError>>, error: Report<UringError>) {
    if first_error.is_none() {
        *first_error = Some(error);
    }
}

pub(crate) struct SubmitPort<'ops, 'registration, 'ring, 'a> {
    ops: &'ops mut OperationLedger,
    control: &'ops mut UringControlPlane,
    registration: &'registration mut RegistrationEngine<'a>,
    diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    ring: &'ring mut IoUring,
    kernel_capabilities: &'ops mut KernelCapabilities,
}

impl<'ops, 'registration, 'ring, 'a> SubmitPort<'ops, 'registration, 'ring, 'a> {
    pub(crate) fn from_parts(
        ops: &'ops mut OperationLedger,
        control: &'ops mut UringControlPlane,
        registration: &'registration mut RegistrationEngine<'a>,
        diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        ring: &'ring mut IoUring,
        kernel_capabilities: &'ops mut KernelCapabilities,
    ) -> Self {
        Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        }
    }

    pub(crate) fn split_for_submit<'context>(
        &'context mut self,
    ) -> (&'context mut OperationLedger, SubmitEnv<'context, 'a>) {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            ..
        } = self;
        let (submitter, submission_queue, completion_queue) = ring.split();
        drop(completion_queue);
        let control = control.with_submit_view(submission_queue);
        let resources = registration.build_submit_view(submitter, diagnostics.backend());
        let env = SubmitEnvironmentFactory::build(SubmitParts::new(control, resources));
        (ops, env)
    }

    pub(crate) fn take_operation_from_slot(&mut self, token: OpToken) -> Option<UringOp> {
        self.ops
            .active_slot_bundle_mut(token)
            .and_then(|(_, _, operation, _)| operation.take())
    }

    pub(crate) fn submission_len(&mut self) -> usize {
        self.ring.submission().len()
    }

    pub(crate) fn submission_is_sqpoll(&self) -> bool {
        self.ring.params().is_setup_sqpoll()
    }

    pub(crate) fn submission_need_wakeup(&mut self) -> bool {
        self.ring.submission().need_wakeup()
    }

    pub(crate) fn kernel_submit(&mut self) -> Result<KernelSubmitReceipt, KernelSubmitError> {
        self.ring.submit()
    }

    pub(crate) fn kernel_submit_with_args(
        &mut self,
        want: usize,
        args: &SubmitArgs<'_, '_>,
    ) -> Result<KernelSubmitReceipt, KernelSubmitError> {
        self.ring.submitter().submit_with_args(want, args)
    }

    pub(crate) fn kernel_enter(
        &mut self,
        args: EnterArgs<'_>,
    ) -> Result<KernelSubmitReceipt, KernelSubmitError> {
        self.ring.submitter().enter(args)
    }

    pub(crate) fn unpublished_staged_entry_count(&self) -> usize {
        self.control.unpublished_staged_entry_count()
    }

    pub(crate) fn quarantine_unpublished_staged(&mut self) -> usize {
        self.control.quarantine_unpublished_staged()
    }

    pub(crate) fn mark_staged_published(&mut self) -> usize {
        self.control.mark_staged_published()
    }

    pub(crate) fn mark_staged_consumed(
        &mut self,
        requested: usize,
        consumed: usize,
        published_in_queue: usize,
    ) -> Result<usize, StagedLedgerError> {
        self.control
            .mark_staged_consumed(requested, consumed, published_in_queue)
    }

    pub(crate) fn mark_kernel_cancel_intents(&mut self) {
        self.control.mark_kernel_cancel_intents();
    }

    pub(crate) fn push_backlog(&mut self, token: OpToken) -> Result<(), BacklogError> {
        self.control.push_backlog(token)
    }

    pub(crate) fn settle_kernel_submission_phases(&mut self) {
        self.mark_kernel_cancel_intents();
        let Self { ops, control, .. } = self;
        let mut kernel_users = Vec::new();
        control.for_each_kernel_user(|token| kernel_users.push(token));
        for token in kernel_users {
            let Ok(view) = ops.checked_slot_view(token) else {
                continue;
            };
            let staged = match view {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                    slot.platform().submission_phase() == SubmissionPhase::SqeStaged
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
                    slot.platform().submission_phase() == SubmissionPhase::SqeStaged
                }
                _ => false,
            };
            if staged {
                let _ = ops.transition_submission(
                    token,
                    SubmissionPhase::KernelOutstanding,
                    "submit receipt handed SQE to kernel",
                    control.observer_mut(),
                );
            }
        }

        let mut kernel_cancels = Vec::new();
        control.for_each_kernel_cancel(|_, target| kernel_cancels.push(target));
        for target in kernel_cancels {
            let Ok(view) = ops.checked_slot_view(target) else {
                continue;
            };
            match view {
                CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                    slot.platform_mut()
                        .set_cancellation_phase(CancellationPhase::CancelOutstanding);
                }
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                    slot.platform_mut()
                        .set_cancellation_phase(CancellationPhase::CancelOutstanding);
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                    slot.platform_mut()
                        .set_cancellation_phase(CancellationPhase::CancelOutstanding);
                }
                CheckedSlotView::Empty(_)
                | CheckedSlotView::Missing { .. }
                | CheckedSlotView::Stale(_) => {}
            }
        }
    }

    pub(crate) fn waker_is_armed(&self) -> bool {
        self.control.waker().is_armed()
    }

    pub(crate) fn waker_registered_fd(&self) -> Option<IoFd> {
        self.control.waker().registered_fd()
    }

    pub(crate) fn waker_current_raw(&self) -> RawHandle {
        let event_fd = self.control.waker().state().current();
        RawHandle::new(UringRawHandle::for_file(event_fd.raw().as_fd()))
    }

    pub(crate) fn set_waker_registered_fd(&mut self, fd: Option<IoFd>) {
        self.control.waker_mut().set_registered_fd(fd);
    }

    pub(crate) fn resolve_waker_file(&mut self, fd: IoFd) -> UringResult<SqeFd> {
        self.registration
            .resolve_file(fd, None, "driver.submit_waker.resolve")
    }

    pub(crate) fn waker_buffer(&mut self) -> (*mut u8, u32) {
        let buffer = self.control.waker_mut().buf_mut_ptr();
        let length = self.control.waker().buf_len() as u32;
        (buffer, length)
    }

    pub(crate) fn set_waker_stage_pending(&mut self, pending: bool) {
        self.control.set_waker_stage_pending(pending);
    }

    pub(crate) fn commit_waker_stage(&mut self) {
        self.control.waker_mut().arm();
        self.control
            .record(ControlPlaneEvent::WakerArm { armed: true });
        self.control.waker().finish_rearm();
        self.control.record(ControlPlaneEvent::WakerRearmed);
    }

    pub(crate) fn register_waker_file(&mut self, raw: RawHandle) -> UringResult<Vec<IoFd>> {
        let submitter = self.ring.submitter();
        self.registration.register_files_internal(
            &submitter,
            self.diagnostics.backend(),
            self.kernel_capabilities,
            vec![RegisterFd::Borrowed(raw.borrow())],
        )
    }
}

pub(crate) struct CompletionContextParts<'context> {
    operation: CompletionOperationParts<'context>,
    resources: CompletionResourceParts<'context>,
}

impl<'context> CompletionContextParts<'context> {
    pub(crate) fn split(
        self,
    ) -> (
        CompletionOperationParts<'context>,
        CompletionResourceParts<'context>,
    ) {
        (self.operation, self.resources)
    }
}

pub(crate) struct CompletionOperationParts<'context> {
    ops: &'context mut OperationLedger,
    control: &'context mut UringControlPlane,
    completion_table: &'context SharedCompletionTable<UringSlotSpec>,
    diagnostics: &'context DriverCompletionDiagnostics<UringCompletionDiagnostics>,
}

impl<'context> CompletionOperationParts<'context> {
    pub(crate) fn into_parts(
        self,
    ) -> (
        &'context mut OperationLedger,
        &'context mut UringControlPlane,
        &'context SharedCompletionTable<UringSlotSpec>,
        &'context DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    ) {
        (
            self.ops,
            self.control,
            self.completion_table,
            self.diagnostics,
        )
    }
}

pub(crate) struct CompletionResourceParts<'context> {
    ring: &'context mut IoUring,
    provided_buffers: Option<ProvidedBufPort<'context>>,
}

impl<'context> CompletionResourceParts<'context> {
    pub(crate) fn into_parts(self) -> (&'context mut IoUring, Option<ProvidedBufPort<'context>>) {
        (self.ring, self.provided_buffers)
    }
}

pub(crate) struct CompletionContext<'ops, 'registration, 'ring, 'a> {
    ops: &'ops mut OperationLedger,
    control: &'ops mut UringControlPlane,
    registration: &'registration mut RegistrationEngine<'a>,
    completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    ring: &'ring mut IoUring,
}

impl<'ops, 'registration, 'ring, 'a> CompletionContext<'ops, 'registration, 'ring, 'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        ops: &'ops mut OperationLedger,
        control: &'ops mut UringControlPlane,
        registration: &'registration mut RegistrationEngine<'a>,
        completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
        diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        ring: &'ring mut IoUring,
    ) -> Self {
        Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
        }
    }

    pub(crate) fn parts<'context>(&'context mut self) -> CompletionContextParts<'context> {
        let Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
        } = self;
        let provided_buffers = registration.provided_buffer_port();
        CompletionContextParts {
            operation: CompletionOperationParts {
                ops,
                control,
                completion_table,
                diagnostics,
            },
            resources: CompletionResourceParts {
                ring,
                provided_buffers,
            },
        }
    }
}

pub(crate) struct LifecycleContext<'ops, 'registration, 'ring, 'a> {
    ops: &'ops mut OperationLedger,
    control: &'ops mut UringControlPlane,
    registration: &'registration mut RegistrationEngine<'a>,
    diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    ring: &'ring mut IoUring,
    kernel_capabilities: &'ops mut KernelCapabilities,
}

impl<'ops, 'registration, 'ring, 'a> LifecycleContext<'ops, 'registration, 'ring, 'a> {
    pub(crate) fn from_parts(
        ops: &'ops mut OperationLedger,
        control: &'ops mut UringControlPlane,
        registration: &'registration mut RegistrationEngine<'a>,
        diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        ring: &'ring mut IoUring,
        kernel_capabilities: &'ops mut KernelCapabilities,
    ) -> Self {
        Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        }
    }

    pub(crate) fn ops(&mut self) -> &mut OperationLedger {
        self.ops
    }

    pub(crate) fn control(&mut self) -> &mut UringControlPlane {
        self.control
    }

    pub(crate) fn diagnostics(&self) -> &DriverCompletionDiagnostics<UringCompletionDiagnostics> {
        self.diagnostics
    }

    pub(crate) fn with_submit_port<R>(
        &mut self,
        operation: impl for<'context> FnOnce(&mut SubmitPort<'context, 'context, 'context, 'a>) -> R,
    ) -> R {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        } = self;
        let mut context = SubmitPort::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
        operation(&mut context)
    }
}

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        ops: &'ops mut OperationLedger,
        control: &'ops mut UringControlPlane,
        registration: &'registration mut RegistrationEngine<'a>,
        completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
        diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        ring: &'ring mut IoUring,
        kernel_capabilities: &'ops mut KernelCapabilities,
    ) -> Self {
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
                let priority = wait_priority(
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

                let budget = wait_budget(timeout, timer_deadline, Duration::from_secs(1));
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
        let mut context = SubmitPort::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
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
        let mut context = SubmitPort::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
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
        let Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            ..
        } = self;
        let flow_result = {
            let mut context = CompletionContext::from_parts(
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
            );
            completion.process_completion_batch(&mut context, budget)
        };
        let (effects, collector_exhausted) = completion.take_effects();
        budget.consume_effects(effects.len());
        let effect_result =
            EffectExecutor.execute(self, lifecycle, completion, &effects, collector_exhausted);
        completion.recycle_effects(effects);
        match (flow_result, effect_result) {
            (Ok(progress), Ok(())) => Ok(progress),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(effect_error)) => Err(effect_error),
            (Err(flow_error), Err(effect_error)) => Err(effect_error.with_diag_src_err(flow_error)),
        }
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            ..
        } = self;
        let flow_result = {
            let mut context = CompletionContext::from_parts(
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
            );
            completion.accept_synthetic_completion(&mut context, event, source, synthetic)
        };
        let (effects, collector_exhausted) = completion.take_effects();
        let effect_result =
            EffectExecutor.execute(self, lifecycle, completion, &effects, collector_exhausted);
        completion.recycle_effects(effects);
        match (flow_result, effect_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(effect_error)) => Err(effect_error),
            (Err(flow_error), Err(effect_error)) => Err(effect_error.with_diag_src_err(flow_error)),
        }
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        let Self {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
            ..
        } = self;
        let flow_result = {
            let mut context = CompletionContext::from_parts(
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
            );
            completion.accept_completion_anomaly_kind(&mut context, kind, attach)
        };
        let (effects, collector_exhausted) = completion.take_effects();
        let effect_result =
            EffectExecutor.execute(self, lifecycle, completion, &effects, collector_exhausted);
        completion.recycle_effects(effects);
        match (flow_result, effect_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(effect_error)) => Err(effect_error),
            (Err(flow_error), Err(effect_error)) => Err(effect_error.with_diag_src_err(flow_error)),
        }
    }

    /// Execute the effects emitted by completion ingress in their fixed ownership order.
    ///
    /// This is deliberately a drive-layer operation.  Completion has already settled the slot
    /// and returned the effect batch, so resource owners can be mutated without keeping a CQE
    /// or operation borrow alive.
    pub(crate) fn execute_completion_effects(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &CompletionEngine,
        effects: &CompletionEffectBatch,
        collector_exhausted: bool,
    ) -> UringResult<()> {
        let mut first_error = None;
        if effects.is_overflowed() {
            self.diagnostics.backend().inc_completion_effect_overflow();
            remember_first_error(
                &mut first_error,
                UringError::InvalidState
                    .report(
                        "uring.drive.completion_effects",
                        "completion effect batch capacity was exhausted",
                    )
                    .attach_note("completion effects were truncated; the driver entered fail-stop"),
            );
        }

        self.execute_bookkeeping_effects(
            lifecycle,
            completion,
            effects,
            collector_exhausted,
            &mut first_error,
        );
        self.execute_resource_effects(effects, &mut first_error);
        self.execute_close_effects(effects, &mut first_error);
        self.execute_waker_effects(effects, &mut first_error);
        if effects.has_backlog_kick() {
            trace!("completion batch requested a bounded backlog submit round");
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn execute_bookkeeping_effects(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &CompletionEngine,
        effects: &CompletionEffectBatch,
        collector_exhausted: bool,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects::iter(effects) {
            match effect.kind() {
                CompletionEffectKind::CancelAck {
                    cancel_ticket,
                    phase,
                } => {
                    if let Some(target) = effect.token()
                        && let Err(error) = self.with_lifecycle(lifecycle, |lifecycle, context| {
                            lifecycle.set_cancel_phase(context, target, phase);
                            Ok(())
                        })
                    {
                        remember_first_error(first_error, error);
                    }
                    if let Some(target) = effect.token() {
                        let _ = self
                            .control
                            .cancel_ticket_for_completion(cancel_ticket, target);
                    }
                }
                CompletionEffectKind::CancelReconcile {
                    cancel_ticket,
                    request,
                    raw,
                } => {
                    self.diagnostics.backend().inc_cancel_reconcile_deferred();
                    if let Err(()) =
                        self.control
                            .defer_cancel_reconcile(cancel_ticket, request, raw)
                    {
                        remember_first_error(
                            first_error,
                            UringError::InvalidState
                                .report(
                                    "uring.cancel.reconcile",
                                    "cancel reconcile ledger is full",
                                )
                                .attach_note(
                                    "the target remains quarantined because its ENOENT could not be tracked",
                                ),
                        );
                    }
                }
                _ => {}
            }
        }
        self.reconcile_deferred_cancels(lifecycle, completion, collector_exhausted, first_error);
    }

    fn execute_resource_effects(
        &mut self,
        effects: &CompletionEffectBatch,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects::iter(effects) {
            let CompletionEffectKind::UdpRearm {
                logical_receiver_generation,
            } = effect.kind()
            else {
                continue;
            };
            let Some(token) = effect.token() else {
                remember_first_error(
                    first_error,
                    UringError::InvalidState.report(
                        "uring.drive.udp_rearm",
                        "UDP rearm effect has no operation token",
                    ),
                );
                continue;
            };
            let valid = match self.ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => {
                    let generation_matches = slot
                        .with_access_mut(|access| {
                            let descriptor = access.operation().get_ref().descriptor();
                            descriptor.record_policy == RecordPolicy::UdpMultishot
                                && unsafe { (descriptor.receive_generation)(access) }
                                    == Some(logical_receiver_generation)
                        })
                        .unwrap_or(false);
                    generation_matches
                        && slot.platform().submission_phase() == SubmissionPhase::KernelOutstanding
                }
                Ok(CheckedSlotView::Valid(SlotView::Reserved(_)))
                | Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(_)))
                | Ok(CheckedSlotView::Empty(_))
                | Ok(CheckedSlotView::Missing { .. })
                | Ok(CheckedSlotView::Stale(_))
                | Err(_) => false,
            };
            if !valid {
                continue;
            }
            if let Err(report) = self.ops.transition_submission(
                token,
                SubmissionPhase::Reserved,
                "UDP receive permit became available",
                self.control.observer_mut(),
            ) {
                remember_first_error(first_error, report);
                continue;
            }
            if let Err(error) = self.control.push_backlog(token) {
                remember_first_error(
                    first_error,
                    UringError::InvalidState
                        .report("uring.drive.udp_rearm", "failed to queue UDP rearm")
                        .with_ctx("backlog_error", format!("{error:?}")),
                );
            }
        }
    }

    fn execute_close_effects(
        &mut self,
        effects: &CompletionEffectBatch,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects::iter(effects) {
            let CompletionEffectKind::CloseUnregister { ticket } = effect.kind() else {
                continue;
            };
            if effect.token() != Some(ticket.operation()) {
                remember_first_error(
                    first_error,
                    UringError::InvalidState
                        .report(
                            "uring.drive.close_effect",
                            "close ownership ticket does not match effect token",
                        )
                        .attach_note("late close completion cannot consume another operation's fd"),
                );
                continue;
            }
            let submitter = self.ring.submitter();
            let ports = RegistrationPorts::new(&submitter, self.diagnostics.backend());
            if let Err(report) = self
                .registration
                .unregister_close_owned_fd(&ports, ticket.fd())
            {
                remember_first_error(first_error, report);
            }
        }
    }

    fn execute_waker_effects(
        &mut self,
        effects: &CompletionEffectBatch,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects::iter(effects) {
            if let CompletionEffectKind::WakerRebuild { generation } = effect.kind() {
                if generation != self.control.waker().armed_generation() {
                    continue;
                }
                self.diagnostics.backend().inc_waker_rebuild();
                if let Err(report) = self
                    .rebuild_waker_fd()
                    .attach_note("failed to rebuild eventfd waker")
                {
                    remember_first_error(first_error, report);
                }
            }
        }

        for effect in effects::iter(effects) {
            if let CompletionEffectKind::WakerRearm { generation } = effect.kind()
                && self.control.waker_mut().prepare_rearm(generation)
            {
                self.control
                    .record(ControlPlaneEvent::WakerArm { armed: false });
                self.control.record(ControlPlaneEvent::WakerRearmRequested);
                self.control.set_waker_stage_pending(true);
                self.diagnostics.backend().inc_waker_rearm();
            }
        }
    }

    fn rebuild_waker_fd(&mut self) -> UringResult<()> {
        if self.registration.file_table_is_poisoned() {
            return Err(self.registration.file_table_poisoned_report(
                "driver.rebuild_waker_fd",
                self.control.waker().registered_fd(),
            ));
        }
        let new_fd = UringWakerManager::create_event_fd("driver.rebuild_waker_fd.eventfd")?;
        let raw = RawHandle::new(UringRawHandle::for_file(new_fd.raw().as_fd()));
        let registered_fd = self.control.waker().registered_fd();
        let state = self.control.waker().state();

        state.with_lock(|current_fd| {
            if self.control.waker().has_pending_notification() {
                UringWakerManager::write_event_fd(&new_fd)?;
            }

            match registered_fd {
                Some(fd @ IoFd::Registered { .. }) => {
                    let submitter = self.ring.submitter();
                    let ports = RegistrationPorts::new(&submitter, self.diagnostics.backend());
                    self.registration
                        .replace_registered_fixed_fd(&ports, fd, raw)?;
                }
                Some(IoFd::Direct(_)) => {
                    self.control
                        .waker_mut()
                        .set_registered_fd(Some(IoFd::direct(raw.raw())));
                }
                Some(IoFd::OwnedDirect { .. }) => {
                    return Err(UringError::InvalidState
                        .report(
                            "uring.rebuild_waker_fd",
                            "owned direct waker descriptors are unsupported",
                        )
                        .with_ctx("fd", format!("{registered_fd:?}")));
                }
                None => {}
            }

            *current_fd = new_fd.clone();
            Ok(())
        })
    }

    fn reconcile_deferred_cancels(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &CompletionEngine,
        collector_exhausted: bool,
        first_error: &mut Option<Report<UringError>>,
    ) {
        let timeout = completion.drive_limits().cancel_reconcile_timeout;
        let mut index = 0;
        while index < self.control.deferred_cancel_reconciles().len() {
            let entry = self.control.deferred_cancel_reconciles()[index];
            let request = entry.request();
            let active = match self.ops.checked_slot_view(request.target()) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => Some(slot.snapshot()),
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Empty(_))
                | Ok(CheckedSlotView::Missing { .. })
                | Ok(CheckedSlotView::Stale(_)) => None,
                Err(report) => {
                    remember_first_error(first_error, report);
                    index += 1;
                    continue;
                }
            };
            let watchdog_expired =
                Instant::now().saturating_duration_since(entry.since()) >= timeout;
            if !collector_exhausted && !watchdog_expired {
                index += 1;
                continue;
            }

            if active.is_none() {
                let _ = self.control.remove_deferred_cancel_reconcile(index);
                let _ = self
                    .control
                    .cancel_ticket_for_completion(entry.cancel_ticket(), request.target());
                continue;
            }
            if watchdog_expired && !collector_exhausted {
                self.diagnostics.backend().inc_cancel_reconcile_timeout();
            }
            match self.quarantine_cancel_target(lifecycle, entry) {
                Ok(()) => {
                    let _ = self.control.remove_deferred_cancel_reconcile(index);
                    let _ = self
                        .control
                        .cancel_ticket_for_completion(entry.cancel_ticket(), request.target());
                }
                Err(report) => {
                    remember_first_error(first_error, report);
                    index += 1;
                }
            }
        }
    }

    fn quarantine_cancel_target(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        entry: DeferredCancelReconcile,
    ) -> UringResult<()> {
        let request = entry.request();
        let snapshot = match self.ops.checked_slot_view(request.target())? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let snapshot = slot.snapshot();
                if !self
                    .completion_table
                    .mark_orphaned(request.target())
                    .is_applied()
                {
                    return Err(UringError::InvalidState.report(
                        "uring.cancel.reconcile.quarantine",
                        "unable to move active cancel target to orphaned state",
                    ));
                }
                snapshot
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => slot.snapshot(),
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot.snapshot(),
            CheckedSlotView::Empty(_)
            | CheckedSlotView::Missing { .. }
            | CheckedSlotView::Stale(_) => return Ok(()),
        };
        self.diagnostics.backend().inc_cancel_ack_enoent_active();
        self.control.quarantine_token(request.target());
        self.with_lifecycle(lifecycle, |lifecycle, context| {
            lifecycle.set_cancel_phase(context, request.target(), CancellationPhase::NotFound);
            Ok(())
        })?;
        Err(UringError::InvalidState
            .report(
                "uring.cancel.reconcile",
                "io_uring cancel returned ENOENT while target remained active",
            )
            .with_ctx("cancel_ticket", entry.cancel_ticket().raw())
            .with_ctx("expected_index", request.target().index())
            .with_ctx("expected_generation", request.target().generation())
            .with_ctx("actual_index", snapshot.index)
            .with_ctx("actual_generation", snapshot.generation)
            .with_ctx("slot_status", format!("{:?}", snapshot.status))
            .with_ctx("raw_cqe_res", entry.raw().res)
            .with_ctx("raw_cqe_flags", entry.raw().flags)
            .attach_note(
                "The target was moved to orphan cleanup and will not be reused until its final CQE arrives.",
            ))
    }

    pub(crate) fn apply_lifecycle_actions(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
    ) -> UringResult<()> {
        let mut actions = Vec::new();
        lifecycle.drain_completion_actions_into(&mut actions);
        let mut first_error: Option<Report<UringError>> = None;
        for action in actions.drain(..) {
            let result = match action {
                LifecycleCompletionAction::SyntheticCancel { event, mode } => self
                    .accept_synthetic_completion(
                        lifecycle,
                        completion,
                        event,
                        SyntheticCompletionSource::Cancel,
                        UringSyntheticCompletion::Cancel { mode },
                    )
                    .map(|_| ()),
                LifecycleCompletionAction::SyntheticSubmissionFailure { event, report } => self
                    .accept_synthetic_completion(
                        lifecycle,
                        completion,
                        event,
                        SyntheticCompletionSource::SubmissionFailure,
                        UringSyntheticCompletion::SubmissionFailure {
                            report: Some(report),
                        },
                    )
                    .map(|_| ()),
                LifecycleCompletionAction::Anomaly { kind, attach } => self
                    .accept_completion_anomaly_kind(lifecycle, completion, kind, attach)
                    .map(|_| ()),
            };
            if let Err(error) = result {
                first_error = Some(match first_error {
                    Some(previous) => previous.with_diag_src_err(error),
                    None => error,
                });
            }
        }
        lifecycle.restore_completion_actions(&mut actions);
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn cancel_operation(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        submission: &mut SubmissionEngine,
        request: CancelRequest,
    ) -> UringResult<CancelSubmitOutcome> {
        let result = {
            let Self {
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
                ..
            } = self;
            let mut context = LifecycleContext::from_parts(
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
            );
            lifecycle.cancel_op_internal(&mut context, request)
        };
        let actions = self.apply_lifecycle_actions(lifecycle, completion);
        let combined = match (result, actions) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        };
        if combined.is_err() {
            submission.enter_fail_stop();
        }
        combined
    }

    pub(crate) fn drain_cancel_requests_bounded(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let result = {
            let Self {
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
                ..
            } = self;
            let mut context = LifecycleContext::from_parts(
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
            );
            lifecycle.drain_cancel_requests_bounded(&mut context, limit)
        };
        let actions = self.apply_lifecycle_actions(lifecycle, completion);
        match (result, actions) {
            (Ok(count), Ok(())) => Ok(count),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        }
    }

    pub(crate) fn stage_pending_cancellations(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let result = {
            let Self {
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
                ..
            } = self;
            let mut context = LifecycleContext::from_parts(
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
            );
            lifecycle.stage_pending_cancellations(&mut context, limit)
        };
        let actions = self.apply_lifecycle_actions(lifecycle, completion);
        match (result, actions) {
            (Ok(count), Ok(())) => Ok(count),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        }
    }

    pub(crate) fn stage_backlog_entries(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        submission: &mut SubmissionEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<BacklogProgress> {
        let progress = {
            let Self {
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
                ..
            } = self;
            let mut context = LifecycleContext::from_parts(
                ops,
                control,
                registration,
                diagnostics,
                ring,
                kernel_capabilities,
            );
            lifecycle.plan_backlog_entries(&mut context, limit)?
        };

        let mut actions = Vec::new();
        lifecycle.drain_backlog_actions_into(&mut actions);
        let mut progress = progress;
        let mut stop = false;
        for action in actions.drain(..) {
            if stop {
                Self::requeue_backlog_action(self.control, action)?;
                continue;
            }
            match action {
                BacklogAction::Drop => {}
                BacklogAction::CancelQueued(token) => {
                    self.with_lifecycle(lifecycle, |lifecycle, context| {
                        lifecycle.complete_backlog_cancel(context, token)
                    })?;
                    progress.synthetic();
                }
                BacklogAction::CancelKernel(token) => {
                    match self.with_lifecycle(lifecycle, |lifecycle, context| {
                        lifecycle.cancel_op_internal(context, CancelRequest::abandon(token))
                    })? {
                        CancelSubmitOutcome::Submitted => progress.submitted(),
                        CancelSubmitOutcome::CompletedLocally => progress.synthetic(),
                        CancelSubmitOutcome::CompletionPending => progress.mark_still_full(),
                        CancelSubmitOutcome::Queued
                        | CancelSubmitOutcome::AlreadyPending { .. }
                        | CancelSubmitOutcome::Merged { .. } => progress.mark_still_full(),
                        CancelSubmitOutcome::TargetGone { .. }
                        | CancelSubmitOutcome::NoBackendHandle => {}
                    }
                }
                BacklogAction::SubmitReserved(token) => {
                    match self.submit_backlog_reserved(submission, token) {
                        Ok(true) => progress.submitted(),
                        Ok(false) => {
                            progress.mark_still_full();
                            stop = true;
                            Self::requeue_backlog(self.control, token)?;
                        }
                        Err(report) => {
                            self.with_lifecycle(lifecycle, |lifecycle, context| {
                                lifecycle
                                    .complete_backlog_submission_error(context, token, true, report)
                            })?;
                            progress.synthetic();
                        }
                    }
                }
                BacklogAction::SubmitQueued(token) => match self
                    .submit_backlog_queued(submission, token)
                {
                    Ok(true) => progress.submitted(),
                    Ok(false) => {
                        progress.mark_still_full();
                        stop = true;
                        Self::requeue_backlog(self.control, token)?;
                    }
                    Err(report) => {
                        self.with_lifecycle(lifecycle, |lifecycle, context| {
                            lifecycle
                                .complete_backlog_submission_error(context, token, false, report)
                        })?;
                        progress.synthetic();
                    }
                },
            }
        }
        lifecycle.restore_backlog_actions(&mut actions);
        self.apply_lifecycle_actions(lifecycle, completion)?;
        Ok(progress)
    }

    fn with_lifecycle<R>(
        &mut self,
        lifecycle: &mut LifecycleEngine,
        operation: impl FnOnce(
            &mut LifecycleEngine,
            &mut LifecycleContext<'_, '_, '_, '_>,
        ) -> UringResult<R>,
    ) -> UringResult<R> {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = self;
        let mut context = LifecycleContext::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
        operation(lifecycle, &mut context)
    }

    fn submit_backlog_reserved(
        &mut self,
        submission: &mut SubmissionEngine,
        token: OpToken,
    ) -> UringResult<bool> {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = self;
        let mut context = SubmitPort::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
        submission.submit_from_slot_token(&mut context, token)
    }

    fn submit_backlog_queued(
        &mut self,
        submission: &mut SubmissionEngine,
        token: OpToken,
    ) -> UringResult<bool> {
        let Self {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = self;
        let mut context = SubmitPort::from_parts(
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        );
        submission.submit_queued_from_slot_token(&mut context, token)
    }

    fn requeue_backlog(control: &mut UringControlPlane, token: OpToken) -> UringResult<()> {
        control.push_backlog(token).map_err(|error| {
            UringError::InvalidState
                .report(
                    "uring.backlog.requeue",
                    format!("backlog rejected token: {error:?}"),
                )
                .with_ctx("token", token.index())
        })
    }

    fn requeue_backlog_action(
        control: &mut UringControlPlane,
        action: BacklogAction,
    ) -> UringResult<()> {
        let token = match action {
            BacklogAction::SubmitReserved(token)
            | BacklogAction::SubmitQueued(token)
            | BacklogAction::CancelQueued(token)
            | BacklogAction::CancelKernel(token) => token,
            BacklogAction::Drop => return Ok(()),
        };
        Self::requeue_backlog(control, token)
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn control_plane_snapshot(
    ops: &mut OperationLedger,
    control: &mut UringControlPlane,
) -> UringResult<ControlPlaneSnapshot> {
    let active_tokens: Vec<OpToken> = ops.local_active_tokens().collect();
    let mut active = Vec::with_capacity(ops.active_count());
    for token in active_tokens {
        let (slot, submission_phase, timer_id) = match ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => (
                slot.snapshot(),
                slot.platform().submission_phase(),
                slot.platform().timer_id(),
            ),
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => (
                slot.snapshot(),
                slot.platform().submission_phase(),
                slot.platform().timer_id(),
            ),
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => (
                slot.snapshot(),
                slot.platform().submission_phase(),
                slot.platform().timer_id(),
            ),
            CheckedSlotView::Empty(_) => {
                return Err(UringError::InvalidState.report(
                    "uring.control_plane.snapshot",
                    "local active token has an idle slot view",
                ));
            }
            CheckedSlotView::Missing { .. } | CheckedSlotView::Stale(_) => {
                return Err(UringError::InvalidState.report(
                    "uring.control_plane.snapshot",
                    "active token disappeared while taking a control-plane snapshot",
                ));
            }
        };
        active.push(ControlTokenSnapshot::new(
            token,
            slot,
            submission_phase,
            timer_id,
            control
                .completion_cleanup_hints()
                .contains_key(&CompletionToken::user(token)),
        ));
    }

    let mut backlog_tokens: Vec<OpToken> = control
        .backlog_entries()
        .into_iter()
        .map(|entry| entry.token())
        .collect();
    backlog_tokens.sort_by_key(|token| (token.index(), token.generation().get()));
    let mut pending_cancel_targets: Vec<OpToken> = control.pending_cancel_targets();
    pending_cancel_targets.sort_by_key(|token| (token.index(), token.generation().get()));
    let mut in_flight_cancel_targets: Vec<(CancelTicket, OpToken)> =
        control.in_flight_cancel_targets();
    in_flight_cancel_targets.sort_by_key(|(ticket, _)| ticket.raw());
    let mut timer_tokens = control.timer_entries();
    timer_tokens.sort_by_key(|(task_id, _)| task_id.raw());
    let mut cleanup_hint_tokens: Vec<CompletionToken> =
        control.completion_cleanup_hints().keys().copied().collect();
    cleanup_hint_tokens.sort_by_key(|token| token.raw());
    let mut quarantined_tokens = control.quarantined_tokens();
    quarantined_tokens.sort_by_key(|token| (token.index(), token.generation().get()));

    Ok(ControlPlaneSnapshot::new(
        active,
        backlog_tokens,
        pending_cancel_targets,
        in_flight_cancel_targets,
        timer_tokens,
        cleanup_hint_tokens,
        quarantined_tokens,
    ))
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn check_control_plane_invariants(
    ops: &mut OperationLedger,
    control: &mut UringControlPlane,
) -> UringResult<()> {
    let snapshot = control_plane_snapshot(ops, control)?;
    let failure = |control: &mut UringControlPlane, error: ControlInvariantError| {
        control.record(ControlPlaneEvent::InvariantViolation(error));
        Err(UringError::InvalidState
            .report("uring.control_plane.invariant", format!("{error:?}"))
            .attach_note("control-plane batch invariant check failed"))
    };

    if snapshot.active_tokens().len() != ops.active_count() {
        return failure(
            control,
            ControlInvariantError::ActiveCountMismatch {
                registry: ops.active_count(),
                observed: snapshot.active_tokens().len(),
            },
        );
    }

    let mut seen_backlog = HashSet::default();
    for token in snapshot.backlog_tokens() {
        if !seen_backlog.insert(*token) {
            return failure(
                control,
                ControlInvariantError::BacklogDuplicate { token: *token },
            );
        }
        if !ops.is_current_active(*token) {
            return failure(
                control,
                ControlInvariantError::BacklogInactive { token: *token },
            );
        }
    }

    for active in snapshot.active_tokens() {
        if control.is_timer_quarantined(active.token()) {
            continue;
        }
        if let Some(task_id) = active.timer_id() {
            if active.submission_phase() != SubmissionPhase::TimerArmed {
                return failure(
                    control,
                    ControlInvariantError::TimerStateMismatch {
                        token: active.token(),
                        state: active.submission_phase(),
                    },
                );
            }
            if control.timer_for(active.token()) != Some(task_id) {
                return failure(
                    control,
                    ControlInvariantError::TimerSlotMismatch {
                        task_id,
                        expected: active.token(),
                        actual: control.timer_for(active.token()).and_then(|id| {
                            control
                                .timer_entries()
                                .into_iter()
                                .find_map(|(entry_id, token)| (entry_id == id).then_some(token))
                        }),
                    },
                );
            }
        } else if active.submission_phase() == SubmissionPhase::TimerArmed {
            return failure(
                control,
                ControlInvariantError::TimerStateMismatch {
                    token: active.token(),
                    state: active.submission_phase(),
                },
            );
        }

        if active.has_cleanup_hint() {
            if !matches!(
                active.submission_phase(),
                SubmissionPhase::SqeStaged | SubmissionPhase::KernelOutstanding
            ) {
                return failure(
                    control,
                    ControlInvariantError::CleanupHintNotKernelSubmitted {
                        token: active.token(),
                        state: active.submission_phase(),
                    },
                );
            }
            if !control.has_staged_kernel_token(active.token()) {
                return failure(
                    control,
                    ControlInvariantError::CleanupHintNotStaged {
                        token: active.token(),
                    },
                );
            }
        }
    }

    for cleanup_token in snapshot.cleanup_hint_tokens() {
        let Some(token) = cleanup_token.op_token() else {
            continue;
        };
        let Some(active) = snapshot
            .active_tokens()
            .iter()
            .find(|active| active.token() == token)
        else {
            return failure(
                control,
                ControlInvariantError::CleanupHintInactive { token },
            );
        };
        if !active.has_cleanup_hint() {
            return failure(
                control,
                ControlInvariantError::CleanupHintInactive { token },
            );
        }
    }

    for (task_id, token) in snapshot.timer_tokens() {
        let Some(active) = snapshot
            .active_tokens()
            .iter()
            .find(|active| active.token() == *token)
        else {
            return failure(
                control,
                ControlInvariantError::TimerSlotMismatch {
                    task_id: *task_id,
                    expected: *token,
                    actual: None,
                },
            );
        };
        if active.timer_id() != Some(*task_id) {
            return failure(
                control,
                ControlInvariantError::TimerSlotMismatch {
                    task_id: *task_id,
                    expected: *token,
                    actual: active.timer_id().and_then(|id| {
                        snapshot
                            .timer_tokens()
                            .iter()
                            .find_map(|(entry_id, entry_token)| {
                                (*entry_id == id).then_some(*entry_token)
                            })
                    }),
                },
            );
        }
    }

    for target in snapshot.pending_cancel_targets() {
        if !ops.is_current_active(*target) {
            return failure(
                control,
                ControlInvariantError::BacklogInactive { token: *target },
            );
        }
    }
    Ok(())
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn check_control_plane_invariants(
    _ops: &mut OperationLedger,
    _control: &mut UringControlPlane,
) -> UringResult<()> {
    Ok(())
}
