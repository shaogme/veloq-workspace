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
    op::{CheckedSlotView, SlotView, UringOpRegistryExt},
};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_driver_core::{
    driver::{CancelTicket, CompletionToken, OpToken},
    slot::SlotSnapshot,
};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_std::{collections::HashSet, vec::Vec};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_std::format;

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
