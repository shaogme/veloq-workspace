use crate::{
    config::IoFd,
    driver::{
        PendingCancel, UringCancelManager, UringTimerWheel, UringWakerManager,
        control::{ControlPlaneEvent, ControlPlaneObserver},
        lifecycle::CancellationPhase,
    },
    op::CompletionCleanupHintFn,
};
use veloq_driver_core::{
    driver::{CancelCompletionId, CompletionToken, OpToken, RawCompletion},
    slot::Generation,
};
use veloq_std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    vec::Vec,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BacklogStageKind {
    Sqe,
    Timer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogEntry {
    pub(crate) token: OpToken,
    pub(crate) kind: BacklogStageKind,
}

/// Ordered retry queue with an O(1) membership marker.
///
/// Removing a cancelled entry only removes its marker. The stale queue node is discarded when it
/// reaches the front, so cancellation does not scan the queue and cannot accidentally remove a
/// later generation of the same slot.
pub(crate) struct SubmissionBacklog {
    queue: VecDeque<OpToken>,
    membership: HashMap<OpToken, BacklogEntry>,
}

impl SubmissionBacklog {
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            membership: HashMap::default(),
        }
    }

    pub(crate) fn push(&mut self, token: OpToken, kind: BacklogStageKind) -> Result<(), OpToken> {
        if self.membership.contains_key(&token) {
            return Err(token);
        }
        self.membership.insert(token, BacklogEntry { token, kind });
        self.queue.push_back(token);
        Ok(())
    }

    pub(crate) fn front(&mut self) -> Option<BacklogEntry> {
        while let Some(token) = self.queue.front().copied() {
            if let Some(entry) = self.membership.get(&token).copied() {
                return Some(entry);
            }
            let _ = self.queue.pop_front();
        }
        None
    }

    pub(crate) fn pop_front(&mut self) -> Option<BacklogEntry> {
        while let Some(token) = self.queue.pop_front() {
            if let Some(entry) = self.membership.remove(&token) {
                return Some(entry);
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, token: OpToken) -> bool {
        self.membership.remove(&token).is_some()
    }

    pub(crate) fn contains(&self, token: OpToken) -> bool {
        self.membership.contains_key(&token)
    }

    pub(crate) fn entries(&self) -> Vec<BacklogEntry> {
        self.membership.values().copied().collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedEntry {
    User(OpToken),
    Cancel {
        id: CancelCompletionId,
        target: OpToken,
    },
    Waker,
}

/// A typed post-completion action owned by the uring control plane.
///
/// The identity is deliberately carried with the action instead of being reconstructed by the
/// executor. This makes a stale completion unable to apply an effect to a recycled slot and gives
/// tests a deterministic sequence to assert against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UringControlEffect {
    pub(crate) sequence: u64,
    pub(crate) token: Option<OpToken>,
    pub(crate) generation: Option<Generation>,
    pub(crate) kind: UringControlEffectKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UringControlEffectKind {
    CancelAck {
        cancel_id: CancelCompletionId,
        phase: CancellationPhase,
    },
    CancelReconcile {
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CloseUnregister {
        fd: IoFd,
    },
    WakerRebuild {
        generation: u64,
    },
    WakerRearm {
        generation: u64,
    },
    BacklogKick,
}

/// Deferred effects produced while routing one completion or one CQ batch.
///
/// Completion routing only appends to this queue. The driver drains it after the core has
/// finished routing the current completion or the complete CQ batch, which keeps control-plane
/// side effects out of the slot borrow and lets cancel ENOENT be reconciled against all user CQEs
/// in the batch.
#[derive(Default)]
pub(crate) struct UringPostCompletionEffects {
    effects: VecDeque<UringControlEffect>,
    next_sequence: u64,
}

impl UringPostCompletionEffects {
    #[inline]
    pub(crate) fn push(&mut self, effect: UringControlEffect) {
        let duplicate = match effect.kind {
            UringControlEffectKind::WakerRearm { generation }
            | UringControlEffectKind::WakerRebuild { generation } => {
                self.effects
                    .iter()
                    .any(|existing| match (existing.kind, effect.kind) {
                        (
                            UringControlEffectKind::WakerRearm {
                                generation: existing_generation,
                            },
                            UringControlEffectKind::WakerRearm { .. },
                        )
                        | (
                            UringControlEffectKind::WakerRebuild {
                                generation: existing_generation,
                            },
                            UringControlEffectKind::WakerRebuild { .. },
                        ) => existing_generation == generation,
                        _ => false,
                    })
            }
            UringControlEffectKind::BacklogKick => self
                .effects
                .iter()
                .any(|existing| matches!(existing.kind, UringControlEffectKind::BacklogKick)),
            UringControlEffectKind::CancelAck { .. }
            | UringControlEffectKind::CancelReconcile { .. }
            | UringControlEffectKind::CloseUnregister { .. } => false,
        };
        if !duplicate {
            self.effects.push_back(effect);
        }
    }

    #[inline]
    pub(crate) fn append(
        &mut self,
        token: Option<OpToken>,
        generation: Option<Generation>,
        kind: UringControlEffectKind,
    ) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.push(UringControlEffect {
            sequence,
            token,
            generation,
            kind,
        });
    }

    #[inline]
    pub(crate) fn extend(&mut self, other: Self) {
        for mut effect in other.effects {
            effect.sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.wrapping_add(1);
            self.push(effect);
        }
    }

    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<UringControlEffect> {
        self.effects.pop_front()
    }

    #[cfg(test)]
    pub(crate) fn effects(&self) -> &VecDeque<UringControlEffect> {
        &self.effects
    }
}

/// The single owner of uring's backend control-plane state.
pub(crate) struct UringControlPlane {
    pub(crate) backlog: SubmissionBacklog,
    pub(crate) staged_entries: VecDeque<StagedEntry>,
    pub(crate) cancellations: UringCancelManager,
    pub(crate) waker: UringWakerManager,
    pub(crate) timers: UringTimerWheel,
    pub(crate) completion_cleanup_hints: HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    pub(crate) observer: ControlPlaneObserver,
    pub(crate) post: UringPostCompletionEffects,
    pub(crate) quarantined_timers: HashSet<OpToken>,
    pub(crate) quarantined_tokens: HashSet<OpToken>,
    pub(crate) waker_stage_pending: bool,
    #[cfg(feature = "test-hooks")]
    pub(crate) push_entry_failure: bool,
}

impl UringControlPlane {
    pub(crate) fn new(waker: UringWakerManager) -> Self {
        Self {
            backlog: SubmissionBacklog::new(),
            staged_entries: VecDeque::new(),
            cancellations: UringCancelManager::new(),
            waker,
            timers: UringTimerWheel::new(),
            completion_cleanup_hints: HashMap::default(),
            observer: ControlPlaneObserver::default(),
            post: UringPostCompletionEffects::default(),
            quarantined_timers: HashSet::default(),
            quarantined_tokens: HashSet::default(),
            waker_stage_pending: false,
            #[cfg(feature = "test-hooks")]
            push_entry_failure: false,
        }
    }

    #[inline]
    pub(crate) fn record(&mut self, event: ControlPlaneEvent) {
        self.observer.record(event);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn take_events(&mut self) -> veloq_std::vec::Vec<ControlPlaneEvent> {
        self.observer.take_events()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn record_completion_observation(&mut self, token: OpToken, final_completion: bool) {
        self.observer.record(ControlPlaneEvent::CompletionRecord {
            token,
            final_completion,
        });
        if final_completion {
            self.observer
                .record(ControlPlaneEvent::CompletionFinalize(token));
        }
    }

    #[inline]
    pub(crate) fn take_post_effects(&mut self) -> UringPostCompletionEffects {
        mem::take(&mut self.post)
    }

    #[inline]
    pub(crate) fn stage_entry(&mut self, entry: StagedEntry) {
        self.staged_entries.push_back(entry);
    }

    #[inline]
    pub(crate) fn staged_entry_count(&self) -> usize {
        self.staged_entries.len()
    }

    pub(crate) fn mark_submitted_entries(&mut self, count: usize) -> Vec<StagedEntry> {
        let count = count.min(self.staged_entries.len());
        self.staged_entries.drain(..count).collect()
    }

    #[inline]
    pub(crate) fn quarantine_timer(&mut self, token: OpToken) {
        self.quarantined_timers.insert(token);
    }

    #[inline]
    pub(crate) fn is_timer_quarantined(&self, token: OpToken) -> bool {
        self.quarantined_timers.contains(&token)
    }

    #[inline]
    pub(crate) fn quarantine_token(&mut self, token: OpToken) {
        self.quarantined_tokens.insert(token);
    }

    #[inline]
    pub(crate) fn release_quarantined_token(&mut self, token: OpToken) {
        self.quarantined_tokens.remove(&token);
    }

    #[inline]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn quarantined_tokens(&self) -> Vec<OpToken> {
        self.quarantined_tokens.iter().copied().collect()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn timer_for(&self, token: OpToken) -> Option<veloq_wheel::TaskId> {
        self.observer.timer_for(token)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn timer_entries(&self) -> veloq_std::vec::Vec<(veloq_wheel::TaskId, OpToken)> {
        self.observer.timer_entries()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn has_staged_kernel_token(&self, token: OpToken) -> bool {
        self.observer.has_staged_kernel_token(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_driver_core::slot::Generation;

    fn token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, Generation::new(generation)).expect("test token")
    }

    #[test]
    fn backlog_removal_keeps_fifo_and_uses_membership() {
        let first = token(1, 1);
        let second = token(2, 1);
        let mut backlog = SubmissionBacklog::new();

        assert!(backlog.push(first, BacklogStageKind::Sqe).is_ok());
        assert!(backlog.push(second, BacklogStageKind::Timer).is_ok());
        assert!(backlog.push(first, BacklogStageKind::Sqe).is_err());
        assert!(backlog.remove(first));
        assert!(!backlog.remove(first));
        assert_eq!(
            backlog.front(),
            Some(BacklogEntry {
                token: second,
                kind: BacklogStageKind::Timer,
            })
        );
        assert_eq!(backlog.pop_front().map(|entry| entry.token), Some(second));
        assert!(backlog.front().is_none());
    }

    #[test]
    fn staged_entries_preserve_submit_order() {
        let first = token(3, 2);
        let second = token(4, 2);
        let mut plane = UringControlPlane::new(UringWakerManager::new().expect("test eventfd"));
        plane.stage_entry(StagedEntry::User(first));
        plane.stage_entry(StagedEntry::Waker);
        plane.stage_entry(StagedEntry::Cancel {
            id: CancelCompletionId::new(7),
            target: second,
        });

        assert_eq!(plane.staged_entry_count(), 3);
        assert_eq!(
            plane.mark_submitted_entries(2),
            veloq_std::vec![StagedEntry::User(first), StagedEntry::Waker]
        );
        assert_eq!(plane.staged_entry_count(), 1);
        assert_eq!(
            plane.mark_submitted_entries(1),
            veloq_std::vec![StagedEntry::Cancel {
                id: CancelCompletionId::new(7),
                target: second,
            }]
        );
    }

    #[test]
    fn post_effects_are_typed_and_waker_generation_is_deduplicated() {
        let token = token(5, 3);
        let mut effects = UringPostCompletionEffects::default();
        effects.push(UringControlEffect {
            sequence: 1,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 9 },
        });
        effects.push(UringControlEffect {
            sequence: 2,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRearm { generation: 9 },
        });
        effects.push(UringControlEffect {
            sequence: 3,
            token: Some(token),
            generation: Some(token.generation()),
            kind: UringControlEffectKind::WakerRebuild { generation: 9 },
        });

        assert_eq!(effects.effects().len(), 2);
        assert!(matches!(
            effects.effects()[0].kind,
            UringControlEffectKind::WakerRearm { generation: 9 }
        ));
        assert_eq!(effects.effects()[0].sequence, 1);
        assert_eq!(effects.effects()[0].generation, Some(token.generation()));
    }

    #[test]
    fn batch_effects_receive_a_deterministic_sequence() {
        let token = token(6, 4);
        let mut first = UringPostCompletionEffects::default();
        first.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::BacklogKick,
        );
        let mut second = UringPostCompletionEffects::default();
        second.append(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::CancelAck {
                cancel_id: CancelCompletionId::new(2),
                phase: CancellationPhase::Acked,
            },
        );

        first.extend(second);

        assert_eq!(first.effects()[0].sequence, 0);
        assert_eq!(first.effects()[1].sequence, 1);
    }
}
