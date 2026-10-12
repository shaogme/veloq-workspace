use crate::driver::{
    control::PendingCancel, lifecycle::CancellationPhase, registration::OwnedFdOwnershipTicket,
};
use veloq_driver_core::{
    driver::{CancelTicket, OpToken, RawCompletion},
    slot::Generation,
};
use veloq_std::{time::Instant, vec::Vec};

/// A typed post-completion action owned by the uring control plane.
///
/// The identity is deliberately carried with the action instead of being reconstructed by the
/// executor. This makes a stale completion unable to apply an effect to a recycled slot and gives
/// tests a deterministic sequence to assert against.
/// effect 自带身份信息，避免过期完成作用于已复用的 slot，并让测试能断言稳定顺序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UringControlEffect {
    sequence: u64,
    token: Option<OpToken>,
    generation: Option<Generation>,
    kind: UringControlEffectKind,
}

impl UringControlEffect {
    #[cfg(test)]
    pub(crate) const fn new(
        sequence: u64,
        token: Option<OpToken>,
        generation: Option<Generation>,
        kind: UringControlEffectKind,
    ) -> Self {
        Self {
            sequence,
            token,
            generation,
            kind,
        }
    }

    pub(crate) const fn kind(self) -> UringControlEffectKind {
        self.kind
    }

    #[cfg(test)]
    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }

    #[cfg(test)]
    pub(crate) const fn generation(self) -> Option<Generation> {
        self.generation
    }

    pub(crate) const fn token(self) -> Option<OpToken> {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UringControlEffectKind {
    CancelAck {
        cancel_ticket: CancelTicket,
        phase: CancellationPhase,
    },
    CancelReconcile {
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CloseUnregister {
        ticket: OwnedFdOwnershipTicket,
    },
    WakerRebuild {
        generation: u64,
    },
    WakerRearm {
        generation: u64,
    },
    UdpRearm {
        logical_receiver_generation: u32,
    },
    BacklogKick,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeferredCancelReconcile {
    cancel_ticket: CancelTicket,
    request: PendingCancel,
    raw: RawCompletion,
    since: Instant,
}

impl DeferredCancelReconcile {
    pub(crate) const fn new(
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
        since: Instant,
    ) -> Self {
        Self {
            cancel_ticket,
            request,
            raw,
            since,
        }
    }

    pub(crate) const fn cancel_ticket(self) -> CancelTicket {
        self.cancel_ticket
    }

    pub(crate) const fn request(self) -> PendingCancel {
        self.request
    }

    pub(crate) const fn raw(self) -> RawCompletion {
        self.raw
    }

    pub(crate) const fn since(self) -> Instant {
        self.since
    }
}

/// Deferred effects produced while routing one completion or one CQ batch.
///
/// Completion routing only appends to this queue. The driver drains it after the core has
/// finished routing the current completion or the complete CQ batch, which keeps control-plane
/// side effects out of the slot borrow and lets cancel ENOENT be reconciled against all user CQEs
/// in the batch.
/// 完成路由只入队；驱动在路由结束后统一执行，使副作用避开 slot 借用，并可用整批用户 CQE 对账 cancel ENOENT。
pub(crate) struct UringPostCompletionEffects {
    effects: Vec<UringControlEffect>,
    capacity: usize,
    next_sequence: u64,
    waker_rebuild_generation: Option<u64>,
    waker_rebuild_index: Option<usize>,
    waker_rearm_generation: Option<u64>,
    waker_rearm_index: Option<usize>,
    backlog_kick: bool,
    overflowed: bool,
}

impl UringPostCompletionEffects {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            effects: Vec::with_capacity(capacity),
            capacity,
            next_sequence: 0,
            waker_rebuild_generation: None,
            waker_rebuild_index: None,
            waker_rearm_generation: None,
            waker_rearm_index: None,
            backlog_kick: false,
            overflowed: false,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, effect: UringControlEffect) {
        let replace_index = match effect.kind {
            UringControlEffectKind::WakerRearm { generation } => {
                if self.waker_rearm_generation == Some(generation) {
                    return;
                }
                self.waker_rearm_index
            }
            UringControlEffectKind::WakerRebuild { generation } => {
                if self.waker_rebuild_generation == Some(generation) {
                    return;
                }
                self.waker_rebuild_index
            }
            UringControlEffectKind::BacklogKick => {
                if self.backlog_kick {
                    return;
                }
                None
            }
            UringControlEffectKind::CancelAck { .. }
            | UringControlEffectKind::CancelReconcile { .. }
            | UringControlEffectKind::CloseUnregister { .. }
            | UringControlEffectKind::UdpRearm { .. } => None,
        };
        if let Some(index) = replace_index {
            self.effects[index] = effect;
            match effect.kind {
                UringControlEffectKind::WakerRearm { generation } => {
                    self.waker_rearm_generation = Some(generation);
                }
                UringControlEffectKind::WakerRebuild { generation } => {
                    self.waker_rebuild_generation = Some(generation);
                }
                UringControlEffectKind::BacklogKick
                | UringControlEffectKind::CancelAck { .. }
                | UringControlEffectKind::CancelReconcile { .. }
                | UringControlEffectKind::CloseUnregister { .. }
                | UringControlEffectKind::UdpRearm { .. } => {}
            }
            return;
        }
        if self.effects.len() >= self.capacity {
            self.overflowed = true;
            return;
        }
        let index = self.effects.len();
        match effect.kind {
            UringControlEffectKind::WakerRearm { generation } => {
                self.waker_rearm_generation = Some(generation);
                self.waker_rearm_index = Some(index);
            }
            UringControlEffectKind::WakerRebuild { generation } => {
                self.waker_rebuild_generation = Some(generation);
                self.waker_rebuild_index = Some(index);
            }
            UringControlEffectKind::BacklogKick => self.backlog_kick = true,
            UringControlEffectKind::CancelAck { .. }
            | UringControlEffectKind::CancelReconcile { .. }
            | UringControlEffectKind::CloseUnregister { .. }
            | UringControlEffectKind::UdpRearm { .. } => {}
        }
        self.effects.push(effect);
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

    #[cfg(test)]
    #[inline]
    pub(crate) fn extend(&mut self, other: Self) {
        for mut effect in other.effects {
            effect.sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.wrapping_add(1);
            self.push(effect);
        }
        self.overflowed |= other.overflowed;
    }

    #[inline]
    pub(crate) fn drain_into(&mut self, destination: &mut Self) {
        for mut effect in self.effects.drain(..) {
            effect.sequence = destination.next_sequence;
            destination.next_sequence = destination.next_sequence.wrapping_add(1);
            destination.push(effect);
        }
        destination.overflowed |= self.overflowed;
        self.waker_rebuild_generation = None;
        self.waker_rebuild_index = None;
        self.waker_rearm_generation = None;
        self.waker_rearm_index = None;
        self.backlog_kick = false;
        self.overflowed = false;
        self.next_sequence = 0;
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &UringControlEffect> {
        self.effects.iter()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.effects.len()
    }

    #[inline]
    pub(crate) fn is_overflowed(&self) -> bool {
        self.overflowed
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.effects.clear();
        self.waker_rebuild_generation = None;
        self.waker_rebuild_index = None;
        self.waker_rearm_generation = None;
        self.waker_rearm_index = None;
        self.backlog_kick = false;
        self.overflowed = false;
        self.next_sequence = 0;
    }

    #[inline]
    pub(crate) fn has_backlog_kick(&self) -> bool {
        self.backlog_kick
    }

    #[cfg(test)]
    pub(crate) fn effects(&self) -> &[UringControlEffect] {
        &self.effects
    }
}

impl Default for UringPostCompletionEffects {
    fn default() -> Self {
        Self::with_capacity(64)
    }
}
