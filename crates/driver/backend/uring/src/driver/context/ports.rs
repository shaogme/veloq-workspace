use crate::{
    config::{IoFd, RawHandle, UringRawHandle},
    diagnostics::UringCompletionDiagnostics,
    driver::{
        completion::{CompletionBatchProgress, CompletionEngine},
        context::CompletionEffectExecutor,
        control::{BacklogError, ControlPlaneEvent, StagedLedgerError, UringControlPlane},
        drive::RoundBudget,
        env::{SubmitEnv, SubmitEnvironmentFactory, SubmitParts},
        lifecycle::{CancellationPhase, LifecycleEngine, SubmissionPhase},
        operation::OperationLedger,
        registration::{ProvidedBufPort, RegistrationEngine, SqeFd},
    },
    error::UringResult,
    op::{CheckedSlotView, SlotView, UringOp, UringOpRegistryExt, UringSlotSpec},
};
use veloq_driver_core::driver::{
    DriverCompletionDiagnostics, OpToken, RegisterFd, SharedCompletionTable,
};
use veloq_io_uring::{
    EnterArgs, IoUring, KernelCapabilities, SubmitError as KernelSubmitError,
    SubmitReceipt as KernelSubmitReceipt, types::SubmitArgs,
};
use veloq_std::{vec, vec::Vec};

pub(crate) struct SubmitPortParts<'ops, 'registration, 'ring, 'a> {
    pub(crate) ops: &'ops mut OperationLedger,
    pub(crate) control: &'ops mut UringControlPlane,
    pub(crate) registration: &'registration mut RegistrationEngine<'a>,
    pub(crate) diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) ring: &'ring mut IoUring,
    pub(crate) kernel_capabilities: &'ops mut KernelCapabilities,
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
    pub(crate) fn from_parts(parts: SubmitPortParts<'ops, 'registration, 'ring, 'a>) -> Self {
        let SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        } = parts;
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

pub(crate) struct CompletionContextInput<'ops, 'registration, 'ring, 'a> {
    pub(crate) ops: &'ops mut OperationLedger,
    pub(crate) control: &'ops mut UringControlPlane,
    pub(crate) registration: &'registration mut RegistrationEngine<'a>,
    pub(crate) completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    pub(crate) diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) ring: &'ring mut IoUring,
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

pub(crate) struct CompletionOperationBorrowParts<'context> {
    pub(crate) ops: &'context mut OperationLedger,
    pub(crate) control: &'context mut UringControlPlane,
    pub(crate) completion_table: &'context SharedCompletionTable<UringSlotSpec>,
    pub(crate) diagnostics: &'context DriverCompletionDiagnostics<UringCompletionDiagnostics>,
}

impl<'context> CompletionOperationParts<'context> {
    pub(crate) fn into_parts(self) -> CompletionOperationBorrowParts<'context> {
        CompletionOperationBorrowParts {
            ops: self.ops,
            control: self.control,
            completion_table: self.completion_table,
            diagnostics: self.diagnostics,
        }
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
    pub(crate) fn from_parts(
        parts: CompletionContextInput<'ops, 'registration, 'ring, 'a>,
    ) -> Self {
        let CompletionContextInput {
            ops,
            control,
            registration,
            completion_table,
            diagnostics,
            ring,
        } = parts;
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
    pub(crate) fn from_parts(parts: SubmitPortParts<'ops, 'registration, 'ring, 'a>) -> Self {
        let SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        } = parts;
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
        let mut context = SubmitPort::from_parts(SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        });
        operation(&mut context)
    }
}

pub(crate) struct DrivePorts<'ops, 'registration, 'ring, 'a> {
    pub(crate) ops: &'ops mut OperationLedger,
    pub(crate) control: &'ops mut UringControlPlane,
    pub(crate) registration: &'registration mut RegistrationEngine<'a>,
    pub(crate) completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    pub(crate) diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) ring: &'ring mut IoUring,
    pub(crate) kernel_capabilities: &'ops mut KernelCapabilities,
}

pub(crate) struct DrivePortAccess;

impl DrivePortAccess {
    pub(crate) fn with_lifecycle<R>(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        operation: impl FnOnce(
            &mut LifecycleEngine,
            &mut LifecycleContext<'_, '_, '_, '_>,
        ) -> UringResult<R>,
    ) -> UringResult<R> {
        let DrivePorts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = ports;
        let parts = SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        };
        let mut context = LifecycleContext::from_parts(parts);
        operation(lifecycle, &mut context)
    }

    pub(crate) fn process_completion_batch(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        let flow_result = {
            let DrivePorts {
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
                ..
            } = ports;
            let parts = CompletionContextInput {
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
            };
            let mut context = CompletionContext::from_parts(parts);
            completion.process_completion_batch(&mut context, budget)
        };
        let (effects, collector_exhausted) = completion.take_effects();
        budget.consume_effects(effects.len());
        let effect_result = CompletionEffectExecutor::execute(
            ports,
            lifecycle,
            completion,
            &effects,
            collector_exhausted,
        );
        completion.recycle_effects(effects);
        match (flow_result, effect_result) {
            (Ok(progress), Ok(())) => Ok(progress),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(effect_error)) => Err(effect_error),
            (Err(flow_error), Err(effect_error)) => Err(effect_error.with_diag_src_err(flow_error)),
        }
    }

    pub(crate) fn accept_completion<R>(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        operation: impl FnOnce(
            &mut CompletionEngine,
            &mut CompletionContext<'_, '_, '_, '_>,
        ) -> UringResult<R>,
    ) -> UringResult<R> {
        let flow_result = {
            let DrivePorts {
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
                ..
            } = ports;
            let parts = CompletionContextInput {
                ops,
                control,
                registration,
                completion_table,
                diagnostics,
                ring,
            };
            let mut context = CompletionContext::from_parts(parts);
            operation(completion, &mut context)
        };
        let (effects, collector_exhausted) = completion.take_effects();
        let effect_result = CompletionEffectExecutor::execute(
            ports,
            lifecycle,
            completion,
            &effects,
            collector_exhausted,
        );
        completion.recycle_effects(effects);
        match (flow_result, effect_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(effect_error)) => Err(effect_error),
            (Err(flow_error), Err(effect_error)) => Err(effect_error.with_diag_src_err(flow_error)),
        }
    }
}
