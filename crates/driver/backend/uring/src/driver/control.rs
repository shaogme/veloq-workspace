pub(crate) mod cancellation;
pub(crate) mod plane;
pub(crate) mod timer;
pub(crate) mod waker;

pub(crate) use cancellation::{
    CancelIntentError, CancelRequestDisposition, PendingCancel, UringCancelManager,
};
pub(crate) use plane::DeferredCancelReconcile;
pub(crate) use plane::{
    StagedEntry, StagedLedger, StagedLedgerError, UringControlEffectKind, UringControlPlane,
    UringPostCompletionEffects,
};
pub(crate) use timer::{ExpiredBatch, UringTimerWheel};
pub(crate) use waker::UringWakerManager;

use crate::driver::lifecycle::SubmissionPhase;
use veloq_driver_core::driver::{CancelTicket, CompletionToken, OpToken};

#[cfg(any(test, feature = "test-hooks"))]
use veloq_driver_core::slot::SlotSnapshot;

#[cfg(any(test, feature = "test-hooks"))]
use veloq_std::vec::Vec;
use veloq_wheel::TaskId;

/// A single observed control-plane transition.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlTransition {
    pub(crate) token: OpToken,
    pub(crate) from: SubmissionPhase,
    pub(crate) to: SubmissionPhase,
    pub(crate) reason: &'static str,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlInvariantError {
    BacklogDuplicate {
        token: OpToken,
    },
    BacklogPopMissing {
        token: OpToken,
    },
    BacklogInactive {
        token: OpToken,
    },
    ActiveCountMismatch {
        registry: usize,
        observed: usize,
    },
    TimerDuplicate {
        task_id: TaskId,
        token: OpToken,
    },
    TimerCancelMissing {
        task_id: TaskId,
        token: OpToken,
    },
    TimerExpireMissing {
        token: OpToken,
    },
    TimerSlotMismatch {
        task_id: TaskId,
        expected: OpToken,
        actual: Option<OpToken>,
    },
    TimerStateMismatch {
        token: OpToken,
        state: SubmissionPhase,
    },
    CleanupHintInactive {
        token: OpToken,
    },
    CleanupHintDuplicate {
        token: OpToken,
    },
    CleanupHintNotKernelSubmitted {
        token: OpToken,
        state: SubmissionPhase,
    },
    CleanupHintNotStaged {
        token: OpToken,
    },
    CleanupHintRemoveMissing {
        token: CompletionToken,
    },
    CancelTicketDuplicate {
        ticket: CancelTicket,
        target: OpToken,
    },
    CancelTicketTargetMismatch {
        ticket: CancelTicket,
        expected: OpToken,
        actual: OpToken,
    },
    CancelTicketRemoveMissing {
        ticket: CancelTicket,
        target: OpToken,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlPlaneEvent {
    SubmissionTransition(ControlTransition),
    BacklogPush(OpToken),
    BacklogPop(OpToken),
    BacklogRemove(OpToken),
    CancelInFlightInsert {
        ticket: CancelTicket,
        target: OpToken,
    },
    CancelInFlightRemove {
        ticket: CancelTicket,
        target: OpToken,
    },
    TimerInsert {
        task_id: TaskId,
        token: OpToken,
    },
    TimerCancel {
        task_id: TaskId,
        token: OpToken,
    },
    #[cfg(any(test, feature = "test-hooks"))]
    TimerExpire {
        task_id: Option<TaskId>,
        token: OpToken,
    },
    WakerArm {
        armed: bool,
    },
    WakerRearmRequested,
    WakerRearmed,
    CleanupHintInsert(CompletionToken),
    CleanupHintRemove(CompletionToken),
    #[cfg(any(test, feature = "test-hooks"))]
    CompletionRecord {
        token: OpToken,
        final_completion: bool,
    },
    #[cfg(any(test, feature = "test-hooks"))]
    CompletionFinalize(OpToken),
    #[cfg(any(test, feature = "test-hooks"))]
    InvariantViolation(ControlInvariantError),
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlTokenSnapshot {
    pub(crate) token: OpToken,
    pub(crate) slot: SlotSnapshot,
    pub(crate) submission_phase: SubmissionPhase,
    pub(crate) timer_id: Option<TaskId>,
    pub(crate) has_cleanup_hint: bool,
}

/// Point-in-time state used by invariant checks and test hooks.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneSnapshot {
    pub(crate) active_tokens: Vec<ControlTokenSnapshot>,
    pub(crate) backlog_tokens: Vec<OpToken>,
    pub(crate) pending_cancel_targets: Vec<OpToken>,
    pub(crate) in_flight_cancel_targets: Vec<(CancelTicket, OpToken)>,
    pub(crate) timer_tokens: Vec<(TaskId, OpToken)>,
    pub(crate) cleanup_hint_tokens: Vec<CompletionToken>,
    pub(crate) quarantined_tokens: Vec<OpToken>,
}

/// Records protocol events only in test/debug builds.
///
/// The type remains present in release builds so the instrumentation call sites stay identical,
/// but the release representation is zero-sized and `record` is a no-op after optimization.
#[derive(Default)]
pub(crate) struct ControlPlaneObserver {
    #[cfg(any(test, feature = "test-hooks"))]
    events: Vec<ControlPlaneEvent>,
    #[cfg(any(test, feature = "test-hooks"))]
    backlog: veloq_std::collections::HashSet<OpToken>,
    #[cfg(any(test, feature = "test-hooks"))]
    timers: veloq_std::collections::HashMap<TaskId, OpToken>,
    #[cfg(any(test, feature = "test-hooks"))]
    staged_kernel_tokens: veloq_std::collections::HashSet<OpToken>,
    #[cfg(any(test, feature = "test-hooks"))]
    cancel_in_flight: veloq_std::collections::HashMap<CancelTicket, OpToken>,
}

impl ControlPlaneObserver {
    #[inline]
    pub(crate) fn record(&mut self, event: ControlPlaneEvent) {
        #[cfg(not(any(test, feature = "test-hooks")))]
        let _ = event;
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let violation = self.apply(event);
            self.events.push(event);
            if let Some(violation) = violation {
                self.events
                    .push(ControlPlaneEvent::InvariantViolation(violation));
                debug_assert!(false, "control-plane invariant violated: {violation:?}");
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn apply(&mut self, event: ControlPlaneEvent) -> Option<ControlInvariantError> {
        match event {
            ControlPlaneEvent::BacklogPush(token) => {
                if self.backlog.insert(token) {
                    None
                } else {
                    Some(ControlInvariantError::BacklogDuplicate { token })
                }
            }
            ControlPlaneEvent::BacklogPop(token) | ControlPlaneEvent::BacklogRemove(token) => self
                .backlog
                .remove(&token)
                .then_some(())
                .ok_or(ControlInvariantError::BacklogPopMissing { token })
                .err(),
            ControlPlaneEvent::TimerInsert { task_id, token } => {
                if self.timers.insert(task_id, token).is_none() {
                    None
                } else {
                    Some(ControlInvariantError::TimerDuplicate { task_id, token })
                }
            }
            ControlPlaneEvent::TimerCancel { task_id, token } => {
                match self.timers.remove(&task_id) {
                    Some(actual) if actual == token => None,
                    Some(actual) => Some(ControlInvariantError::TimerSlotMismatch {
                        task_id,
                        expected: token,
                        actual: Some(actual),
                    }),
                    None => Some(ControlInvariantError::TimerCancelMissing { task_id, token }),
                }
            }
            ControlPlaneEvent::TimerExpire { task_id, token } => {
                let Some(task_id) = task_id else {
                    return Some(ControlInvariantError::TimerExpireMissing { token });
                };
                match self.timers.remove(&task_id) {
                    Some(actual) if actual == token => None,
                    Some(actual) => Some(ControlInvariantError::TimerSlotMismatch {
                        task_id,
                        expected: token,
                        actual: Some(actual),
                    }),
                    None => Some(ControlInvariantError::TimerExpireMissing { token }),
                }
            }
            ControlPlaneEvent::CancelInFlightInsert { ticket, target } => {
                match self.cancel_in_flight.entry(ticket) {
                    veloq_std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(target);
                        None
                    }
                    veloq_std::collections::hash_map::Entry::Occupied(_) => {
                        Some(ControlInvariantError::CancelTicketDuplicate { ticket, target })
                    }
                }
            }
            ControlPlaneEvent::CancelInFlightRemove { ticket, target } => {
                match self.cancel_in_flight.remove(&ticket) {
                    Some(actual) if actual == target => None,
                    Some(actual) => Some(ControlInvariantError::CancelTicketTargetMismatch {
                        ticket,
                        expected: target,
                        actual,
                    }),
                    None => {
                        Some(ControlInvariantError::CancelTicketRemoveMissing { ticket, target })
                    }
                }
            }
            ControlPlaneEvent::CleanupHintInsert(token) => {
                let token = token.op_token()?;
                if self.staged_kernel_tokens.insert(token) {
                    None
                } else {
                    Some(ControlInvariantError::CleanupHintDuplicate { token })
                }
            }
            ControlPlaneEvent::CleanupHintRemove(token) => {
                let op_token = token.op_token()?;
                if self.staged_kernel_tokens.remove(&op_token) {
                    None
                } else {
                    Some(ControlInvariantError::CleanupHintRemoveMissing { token })
                }
            }
            ControlPlaneEvent::InvariantViolation(_)
            | ControlPlaneEvent::SubmissionTransition(_)
            | ControlPlaneEvent::WakerArm { .. }
            | ControlPlaneEvent::WakerRearmRequested
            | ControlPlaneEvent::WakerRearmed
            | ControlPlaneEvent::CompletionRecord { .. }
            | ControlPlaneEvent::CompletionFinalize(_) => None,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn take_events(&mut self) -> Vec<ControlPlaneEvent> {
        veloq_std::mem::take(&mut self.events)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn timer_for(&self, token: OpToken) -> Option<TaskId> {
        self.timers
            .iter()
            .find_map(|(task_id, active)| (*active == token).then_some(*task_id))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn timer_entries(&self) -> Vec<(TaskId, OpToken)> {
        self.timers
            .iter()
            .map(|(task_id, token)| (*task_id, *token))
            .collect()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn has_staged_kernel_token(&self, token: OpToken) -> bool {
        self.staged_kernel_tokens.contains(&token)
    }
}

/// Writes a backend submission phase and records the exact write site in the observer.
#[inline]
pub(crate) fn transition_submission_phase(
    phase: &mut SubmissionPhase,
    token: OpToken,
    next: SubmissionPhase,
    reason: &'static str,
    observer: &mut ControlPlaneObserver,
) {
    let from = *phase;
    *phase = next;
    observer.record(ControlPlaneEvent::SubmissionTransition(ControlTransition {
        token,
        from,
        to: next,
        reason,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_driver_core::slot::Generation;
    use veloq_std::time::Duration;
    use veloq_wheel::{Wheel, WheelConfig};

    fn token() -> OpToken {
        OpToken::from_registry_parts(3, Generation::new(7)).expect("test token")
    }

    fn other_token() -> OpToken {
        OpToken::from_registry_parts(4, Generation::new(8)).expect("second test token")
    }

    #[test]
    fn records_submission_and_backlog_protocol_events() {
        let token = token();
        let mut observer = ControlPlaneObserver::default();
        let mut state = SubmissionPhase::Reserved;

        transition_submission_phase(
            &mut state,
            token,
            SubmissionPhase::SqeStaged,
            "test staged",
            &mut observer,
        );
        transition_submission_phase(
            &mut state,
            token,
            SubmissionPhase::KernelOutstanding,
            "test submit boundary",
            &mut observer,
        );
        observer.record(ControlPlaneEvent::BacklogPush(token));
        observer.record(ControlPlaneEvent::BacklogPop(token));

        let events = observer.take_events();
        assert_eq!(events.len(), 4);
        assert!(matches!(
            events[0],
            ControlPlaneEvent::SubmissionTransition(ControlTransition {
                token: event_token,
                from: SubmissionPhase::Reserved,
                to: SubmissionPhase::SqeStaged,
                reason: "test staged",
            }) if event_token == token
        ));
        assert!(matches!(
            events[1],
            ControlPlaneEvent::SubmissionTransition(ControlTransition {
                from: SubmissionPhase::SqeStaged,
                to: SubmissionPhase::KernelOutstanding,
                reason: "test submit boundary",
                ..
            })
        ));
        assert_eq!(events[2], ControlPlaneEvent::BacklogPush(token));
        assert_eq!(events[3], ControlPlaneEvent::BacklogPop(token));
    }

    #[test]
    fn tracks_cleanup_hint_staging_until_final_removal() {
        let token = token();
        let completion_token = CompletionToken::user(token);
        let mut observer = ControlPlaneObserver::default();

        observer.record(ControlPlaneEvent::CleanupHintInsert(completion_token));
        assert!(observer.has_staged_kernel_token(token));
        observer.record(ControlPlaneEvent::CleanupHintRemove(completion_token));
        assert!(!observer.has_staged_kernel_token(token));

        let events = observer.take_events();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0],
            ControlPlaneEvent::CleanupHintInsert(completion_token)
        );
        assert_eq!(
            events[1],
            ControlPlaneEvent::CleanupHintRemove(completion_token)
        );
    }

    #[test]
    fn records_round_one_control_plane_sequences() {
        let token = token();
        let expire_token = other_token();
        let cancel_ticket = CancelTicket::try_new(1).expect("test ticket");
        let mut wheel = Wheel::new(WheelConfig::default());
        let task_id = wheel.insert(token, Duration::from_secs(1));
        let expire_task_id = wheel.insert(expire_token, Duration::from_secs(1));
        let mut observer = ControlPlaneObserver::default();
        let mut state = SubmissionPhase::Reserved;

        // Normal submission and SQ-full retry.
        transition_submission_phase(
            &mut state,
            token,
            SubmissionPhase::SqeStaged,
            "test staged SQE",
            &mut observer,
        );
        transition_submission_phase(
            &mut state,
            token,
            SubmissionPhase::Reserved,
            "test SQ full",
            &mut observer,
        );
        observer.record(ControlPlaneEvent::BacklogPush(token));
        observer.record(ControlPlaneEvent::BacklogPop(token));
        transition_submission_phase(
            &mut state,
            token,
            SubmissionPhase::KernelOutstanding,
            "test queued retry",
            &mut observer,
        );

        // Queued cancellation and timer cancellation.
        observer.record(ControlPlaneEvent::TimerInsert { task_id, token });
        observer.record(ControlPlaneEvent::TimerCancel { task_id, token });
        observer.record(ControlPlaneEvent::TimerInsert {
            task_id: expire_task_id,
            token: expire_token,
        });
        observer.record(ControlPlaneEvent::TimerExpire {
            task_id: Some(expire_task_id),
            token: expire_token,
        });

        // A multishot orphan produces one non-final record and one finalization.
        observer.record(ControlPlaneEvent::CompletionRecord {
            token,
            final_completion: false,
        });
        observer.record(ControlPlaneEvent::CompletionRecord {
            token,
            final_completion: true,
        });
        observer.record(ControlPlaneEvent::CompletionFinalize(token));

        // Waker processing leaves REARM and then arms a single replacement read.
        observer.record(ControlPlaneEvent::WakerArm { armed: false });
        observer.record(ControlPlaneEvent::WakerRearmRequested);
        observer.record(ControlPlaneEvent::WakerArm { armed: true });
        observer.record(ControlPlaneEvent::WakerRearmed);

        // Async cancel bookkeeping is paired around the cancel CQE.
        observer.record(ControlPlaneEvent::CancelInFlightInsert {
            ticket: cancel_ticket,
            target: token,
        });
        observer.record(ControlPlaneEvent::CancelInFlightRemove {
            ticket: cancel_ticket,
            target: token,
        });

        let events = observer.take_events();
        assert_eq!(events.len(), 18);
        assert!(matches!(
            events[0],
            ControlPlaneEvent::SubmissionTransition(ControlTransition {
                from: SubmissionPhase::Reserved,
                to: SubmissionPhase::SqeStaged,
                ..
            })
        ));
        assert_eq!(
            events[9],
            ControlPlaneEvent::CompletionRecord {
                token,
                final_completion: false,
            }
        );
        assert_eq!(events[12], ControlPlaneEvent::WakerArm { armed: false });
        assert_eq!(
            events[16],
            ControlPlaneEvent::CancelInFlightInsert {
                ticket: cancel_ticket,
                target: token,
            }
        );
    }
}
