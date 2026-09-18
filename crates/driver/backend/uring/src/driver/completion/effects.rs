//! Completion effects produced by ingress settlement.
//!
//! Completion routing may describe a resource or control-plane side effect, but it must not
//! execute that side effect while a CQE is being settled.  The drive coordinator owns the
//! executor and applies these effects after the completion borrow has ended.

use crate::driver::control::{
    UringControlEffect, UringControlEffectKind, UringPostCompletionEffects,
};

/// A generation-bound action emitted by completion ingress.
pub(crate) type CompletionEffect = UringControlEffect;

/// The kind of side effect requested by completion ingress.
pub(crate) type CompletionEffectKind = UringControlEffectKind;

/// Reusable bounded effect batch for one completion or CQE collection.
pub(crate) type CompletionEffectBatch = UringPostCompletionEffects;

#[inline]
pub(crate) fn iter(batch: &CompletionEffectBatch) -> impl Iterator<Item = &CompletionEffect> {
    batch.iter()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::lifecycle::CancellationPhase;
    use veloq_driver_core::driver::{CancelTicket, OpToken};
    use veloq_driver_core::slot::Generation;
    use veloq_std::vec::Vec;

    fn token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, Generation::new(generation)).expect("test token")
    }

    #[test]
    fn effect_batch_preserves_generation_bound_order() {
        let token = token(2, 7);
        let ticket = CancelTicket::try_new(3).expect("test cancel ticket");
        let mut batch = CompletionEffectBatch::with_capacity(4);
        batch.append(
            Some(token),
            Some(token.generation()),
            CompletionEffectKind::CancelAck {
                cancel_ticket: ticket,
                phase: CancellationPhase::Acked,
            },
        );
        batch.append(
            Some(token),
            Some(token.generation()),
            CompletionEffectKind::BacklogKick,
        );

        let effects: Vec<_> = iter(&batch).collect();
        assert_eq!(effects.len(), 2);
        assert_eq!(effects[0].token(), Some(token));
        assert!(matches!(
            effects[0].kind(),
            CompletionEffectKind::CancelAck { .. }
        ));
        assert!(matches!(
            effects[1].kind(),
            CompletionEffectKind::BacklogKick
        ));
    }
}
