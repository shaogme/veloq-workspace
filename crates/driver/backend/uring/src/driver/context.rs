//! 驱动组件之间的窄生命周期上下文，只在单次提交或 drive round 内存在。
//! Narrow-lifetime contexts between driver components; each exists for one submission or drive round.
//!
//! 它们不持有父门面，用 field-level borrow split 限制组件依赖。
//! They do not own the parent facade and use field-level borrow splits to limit dependencies.

mod backlog;
mod cancellation;
mod effects;
mod ports;

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
use veloq_driver_core::{
    CancelRequest, CancelSubmitOutcome, DriveMode, DrivePendingWork, DriverCompletionDiagnostics,
    SharedCompletionTable,
};
use veloq_io_uring::{IoUring, KernelCapabilities};
use veloq_std::{format, time::Duration};

pub(crate) use backlog::BacklogExecutor;
pub(crate) use cancellation::CancellationCoordinator;
pub(crate) use effects::CompletionEffectExecutor;
pub(crate) use invariants::ControlPlaneInvariantAuditor;
pub(crate) use ports::{
    CompletionContext, CompletionOperationBorrowParts, DrivePortAccess, DrivePorts,
    LifecycleContext, SubmitPort, SubmitPortParts,
};

pub(crate) struct DriveContextParts<'ops, 'registration, 'ring, 'a> {
    pub(crate) ops: &'ops mut OperationLedger,
    pub(crate) control: &'ops mut UringControlPlane,
    pub(crate) registration: &'registration mut RegistrationEngine<'a>,
    pub(crate) completion_table: &'ops SharedCompletionTable<UringSlotSpec>,
    pub(crate) diagnostics: &'ops DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) ring: &'ring mut IoUring,
    pub(crate) kernel_capabilities: &'ops mut KernelCapabilities,
}

mod invariants {
    use crate::{
        driver::{control::UringControlPlane, operation::OperationLedger},
        error::UringResult,
    };

    #[cfg(any(test, feature = "test-hooks"))]
    use crate::{
        driver::{
            control::{
                ControlInvariantError, ControlPlaneEvent, ControlPlaneSnapshot,
                ControlPlaneSnapshotParts, ControlTokenSnapshot,
            },
            lifecycle::SubmissionPhase,
        },
        error::UringError,
    };

    #[cfg(any(test, feature = "test-hooks"))]
    use veloq_driver_core::{
        CancelTicket, CheckedSlotView, CompletionToken, OpToken,
        SlotRegistryExt as UringOpRegistryExt, SlotSnapshot, SlotView,
    };

    #[cfg(any(test, feature = "test-hooks"))]
    use veloq_std::{collections::HashSet, format, vec::Vec};

    #[cfg(any(test, feature = "test-hooks"))]
    use veloq_wheel::TimerId;

    pub(crate) struct ControlPlaneInvariantAuditor;

    #[cfg(any(test, feature = "test-hooks"))]
    struct ActiveSlotFacts {
        snapshot: SlotSnapshot,
        submission_phase: SubmissionPhase,
        timer_id: Option<TimerId>,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl ControlPlaneInvariantAuditor {
        pub(crate) fn snapshot(
            ops: &mut OperationLedger,
            control: &mut UringControlPlane,
        ) -> UringResult<ControlPlaneSnapshot> {
            let active_tokens: Vec<OpToken> = ops.local_active_tokens().collect();
            let mut active = Vec::with_capacity(ops.active_count());
            for token in active_tokens {
                let facts = match ops.checked_slot_view(token)? {
                    CheckedSlotView::Valid(SlotView::Reserved(slot)) => ActiveSlotFacts {
                        snapshot: slot.snapshot(),
                        submission_phase: slot.platform().submission_phase(),
                        timer_id: slot.platform().timer_id(),
                    },
                    CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => ActiveSlotFacts {
                        snapshot: slot.snapshot(),
                        submission_phase: slot.platform().submission_phase(),
                        timer_id: slot.platform().timer_id(),
                    },
                    CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => ActiveSlotFacts {
                        snapshot: slot.snapshot(),
                        submission_phase: slot.platform().submission_phase(),
                        timer_id: slot.platform().timer_id(),
                    },
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
                    facts.snapshot,
                    facts.submission_phase,
                    facts.timer_id,
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

            Ok(ControlPlaneSnapshot::new(ControlPlaneSnapshotParts {
                active_tokens: active,
                backlog_tokens,
                pending_cancel_targets,
                in_flight_cancel_targets,
                timer_tokens,
                cleanup_hint_tokens,
                quarantined_tokens,
            }))
        }

        pub(crate) fn check(
            ops: &mut OperationLedger,
            control: &mut UringControlPlane,
        ) -> UringResult<()> {
            let snapshot = Self::snapshot(ops, control)?;
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
                                    control.timer_entries().into_iter().find_map(
                                        |(entry_id, token)| (entry_id == id).then_some(token),
                                    )
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
                                snapshot.timer_tokens().iter().find_map(
                                    |(entry_id, entry_token)| {
                                        (*entry_id == id).then_some(*entry_token)
                                    },
                                )
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
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    impl ControlPlaneInvariantAuditor {
        pub(crate) fn check(
            _ops: &mut OperationLedger,
            _control: &mut UringControlPlane,
        ) -> UringResult<()> {
            Ok(())
        }
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
        DrivePendingWork::default()
            .with_remote_cancels(remote_cancels)
            .with_lifecycle_actions(lifecycle.pending_action_count())
            .with_backlog_actions(self.control.backlog_len())
            .with_sqe_enters(
                if control_pending
                    || !self.ring.submission().is_empty()
                    || self.control.waker_stage_pending()
                {
                    1
                } else {
                    0
                },
            )
            .with_cqes(cqes)
            .with_timers(self.control.timers().pending_expired_len())
            .with_effects(completion.pending_effects_len())
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
                let user_ready = self.ops.shared_slots().has_ready_completion();
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
