use veloq_driver_core::driver::{
    CancelMode, CancelRequest, CancelTicket, CancelTicketError, OpToken, RemoteCancelSender,
};
use veloq_std::{collections::HashMap, sync::mpsc, vec, vec::Vec};

#[cfg(test)]
use veloq_std::collections::hash_map::Entry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingCancel {
    pub(crate) target: OpToken,
    pub(crate) mode: CancelMode,
}

impl PendingCancel {
    #[inline]
    pub(crate) const fn new(request: CancelRequest) -> Self {
        Self {
            target: request.target,
            mode: request.mode,
        }
    }

    #[inline]
    pub(crate) const fn user_parts(self) -> (usize, veloq_driver_core::slot::Generation) {
        self.target.parts()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelRequestDisposition {
    New { ticket: CancelTicket },
    AlreadyPending { ticket: CancelTicket },
    Merged { ticket: CancelTicket },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelIntentError {
    Capacity,
    GenerationMismatch,
    InvalidPhase,
    TicketExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelIntentPhase {
    Pending,
    Staged,
    Outstanding,
}

#[derive(Debug, Clone, Copy)]
struct CancelIntent {
    request: PendingCancel,
    ticket: CancelTicket,
    phase: CancelIntentPhase,
    prev: Option<usize>,
    next: Option<usize>,
}

pub(crate) struct UringCancelManager {
    /// One intent per target slot. The token generation prevents an old intent from matching a
    /// replacement operation in the same slot.
    intents: Vec<Option<CancelIntent>>,
    pending_head: Option<usize>,
    pending_tail: Option<usize>,
    pending_len: usize,
    capacity: usize,
    pending_cancel_cqes: HashMap<CancelTicket, PendingCancel>,
    next_cancel_ticket: u64,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
}

impl UringCancelManager {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            intents: vec![None; capacity],
            pending_head: None,
            pending_tail: None,
            pending_len: 0,
            capacity,
            pending_cancel_cqes: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            next_cancel_ticket: 1,
            remote_cancel_sender: sender,
            remote_cancel_receiver: receiver,
        }
    }

    #[inline]
    pub(crate) fn remote_sender(&self) -> RemoteCancelSender {
        self.remote_cancel_sender.clone()
    }

    #[inline]
    pub(crate) fn try_recv_remote(&mut self) -> Option<CancelRequest> {
        self.remote_cancel_receiver.try_recv().ok()
    }

    #[inline]
    pub(crate) fn request(
        &mut self,
        request: PendingCancel,
    ) -> Result<CancelRequestDisposition, CancelIntentError> {
        let index = request.target.index();
        if index >= self.intents.len() {
            return Err(CancelIntentError::Capacity);
        }
        if let Some(intent) = self.intents[index].as_mut() {
            if intent.request.target != request.target {
                return Err(CancelIntentError::GenerationMismatch);
            }
            if request.mode == CancelMode::Abandon && intent.request.mode != CancelMode::Abandon {
                intent.request.mode = CancelMode::Abandon;
                if let Some(sidecar) = self.pending_cancel_cqes.get_mut(&intent.ticket) {
                    sidecar.mode = CancelMode::Abandon;
                }
                return Ok(CancelRequestDisposition::Merged {
                    ticket: intent.ticket,
                });
            }
            return Ok(CancelRequestDisposition::AlreadyPending {
                ticket: intent.ticket,
            });
        }

        let ticket = self
            .allocate_cancel_ticket()
            .map_err(|_| CancelIntentError::TicketExhausted)?;
        self.intents[index] = Some(CancelIntent {
            request,
            ticket,
            phase: CancelIntentPhase::Pending,
            prev: self.pending_tail,
            next: None,
        });
        if let Some(previous) = self.pending_tail {
            self.intents[previous]
                .as_mut()
                .expect("pending tail must contain an intent")
                .next = Some(index);
        } else {
            self.pending_head = Some(index);
        }
        self.pending_tail = Some(index);
        self.pending_len += 1;
        Ok(CancelRequestDisposition::New { ticket })
    }

    #[inline]
    pub(crate) fn front_pending(&self) -> Option<&PendingCancel> {
        self.pending_head
            .and_then(|index| self.intents[index].as_ref().map(|intent| &intent.request))
    }

    #[inline]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending_len
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub(crate) fn ticket_for(&self, target: OpToken) -> Option<CancelTicket> {
        self.intents
            .get(target.index())
            .and_then(Option::as_ref)
            .filter(|intent| intent.request.target == target)
            .map(|intent| intent.ticket)
    }

    pub(crate) fn remove_pending_target(&mut self, target: OpToken) -> Option<PendingCancel> {
        let index = target.index();
        let intent = self.intents.get(index).and_then(Option::as_ref).copied()?;
        if intent.request.target != target || intent.phase != CancelIntentPhase::Pending {
            return None;
        }
        self.remove_intent(index).map(|intent| intent.request)
    }

    pub(crate) fn mark_staged(
        &mut self,
        ticket: CancelTicket,
        target: OpToken,
    ) -> Result<(), CancelIntentError> {
        let index = target.index();
        let Some(intent) = self.intents.get(index).and_then(Option::as_ref).copied() else {
            return Err(CancelIntentError::Capacity);
        };
        if intent.request.target != target || intent.ticket != ticket {
            return Err(CancelIntentError::GenerationMismatch);
        }
        if intent.phase != CancelIntentPhase::Pending {
            return Err(CancelIntentError::InvalidPhase);
        }
        self.unlink_pending(index);
        self.intents[index]
            .as_mut()
            .expect("unlinked cancel intent must remain present")
            .phase = CancelIntentPhase::Staged;
        Ok(())
    }

    pub(crate) fn mark_outstanding(&mut self, ticket: CancelTicket, target: OpToken) {
        if let Some(intent) = self
            .intents
            .get_mut(target.index())
            .and_then(Option::as_mut)
            && intent.request.target == target
            && intent.ticket == ticket
        {
            intent.phase = CancelIntentPhase::Outstanding;
        }
    }

    #[inline]
    pub(crate) fn allocate_cancel_ticket(&mut self) -> Result<CancelTicket, CancelTicketError> {
        let raw = self.next_cancel_ticket;
        let ticket = CancelTicket::try_new(raw).map_err(|_| CancelTicketError::Exhausted)?;
        let next = raw.checked_add(1).ok_or(CancelTicketError::Exhausted)?;
        self.next_cancel_ticket = next;
        Ok(ticket)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn insert_in_flight(
        &mut self,
        ticket: CancelTicket,
        pending: PendingCancel,
    ) -> Result<(), PendingCancel> {
        if self.pending_cancel_cqes.len() >= self.capacity
            && !self.pending_cancel_cqes.contains_key(&ticket)
        {
            return Err(pending);
        }
        match self.pending_cancel_cqes.entry(ticket) {
            Entry::Vacant(entry) => {
                entry.insert(pending);
                Ok(())
            }
            Entry::Occupied(_) => Err(pending),
        }
    }

    #[inline]
    pub(crate) fn in_flight_mut(&mut self) -> &mut HashMap<CancelTicket, PendingCancel> {
        &mut self.pending_cancel_cqes
    }

    #[inline]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.pending_cancel_cqes.len()
    }

    #[inline]
    pub(crate) fn clear_in_flight(&mut self) {
        self.pending_cancel_cqes.clear();
    }

    /// Ends the control-plane lifetime of a cancel intent. The CQE sidecar may already have been
    /// removed by completion routing, so this method is intentionally idempotent.
    pub(crate) fn finish_ticket(
        &mut self,
        ticket: CancelTicket,
        target: OpToken,
    ) -> Option<PendingCancel> {
        let index = target.index();
        let intent = self.intents.get(index).and_then(Option::as_ref)?;
        if intent.ticket != ticket || intent.request.target != target {
            return None;
        }
        self.remove_intent(index).map(|intent| intent.request)
    }

    pub(crate) fn cancel_request_failed(&mut self, ticket: CancelTicket, target: OpToken) {
        let index = target.index();
        if self
            .intents
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|intent| intent.ticket == ticket && intent.request.target == target)
        {
            let _ = self.remove_intent(index);
        }
        let _ = self.pending_cancel_cqes.remove(&ticket);
    }

    fn unlink_pending(&mut self, index: usize) {
        let intent = self.intents[index].expect("unlinking pending cancel requires an intent");
        if let Some(previous) = intent.prev {
            self.intents[previous]
                .as_mut()
                .expect("pending previous must contain an intent")
                .next = intent.next;
        } else {
            self.pending_head = intent.next;
        }
        if let Some(next) = intent.next {
            self.intents[next]
                .as_mut()
                .expect("pending next must contain an intent")
                .prev = intent.prev;
        } else {
            self.pending_tail = intent.prev;
        }
        self.pending_len -= 1;
        self.intents[index]
            .as_mut()
            .expect("pending intent must remain present")
            .prev = None;
        self.intents[index]
            .as_mut()
            .expect("pending intent must remain present")
            .next = None;
    }

    fn remove_intent(&mut self, index: usize) -> Option<CancelIntent> {
        let intent = self.intents.get(index).and_then(Option::as_ref).copied()?;
        if intent.phase == CancelIntentPhase::Pending {
            self.unlink_pending(index);
        }
        self.intents[index].take()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn pending_targets(&self) -> impl Iterator<Item = OpToken> + '_ {
        self.intents
            .iter()
            .flatten()
            .filter(|intent| intent.phase == CancelIntentPhase::Pending)
            .map(|intent| intent.request.target)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn in_flight_targets(&self) -> impl Iterator<Item = (CancelTicket, OpToken)> + '_ {
        self.pending_cancel_cqes
            .iter()
            .map(|(ticket, request)| (*ticket, request.target))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_driver_core::slot::Generation;

    fn pending() -> PendingCancel {
        PendingCancel {
            target: OpToken::from_registry_parts(1, Generation::new(1)).expect("test token"),
            mode: CancelMode::Abandon,
        }
    }

    fn request(index: usize, generation: u32, mode: CancelMode) -> PendingCancel {
        PendingCancel {
            target: OpToken::from_registry_parts(index, Generation::new(generation))
                .expect("test token"),
            mode,
        }
    }

    #[test]
    fn allocator_is_monotonic_and_ignores_in_flight_map() {
        let mut manager = UringCancelManager::with_capacity(4);
        let first = manager.allocate_cancel_ticket().expect("first ticket");
        manager
            .insert_in_flight(first, pending())
            .expect("unique ticket");

        let next = manager.allocate_cancel_ticket().expect("second ticket");
        assert_eq!(next.raw(), first.raw() + 1);
    }

    #[test]
    fn allocator_never_reuses_a_ticket_removed_from_the_sidecar() {
        let mut manager = UringCancelManager::with_capacity(4);
        let first = manager.allocate_cancel_ticket().expect("first ticket");
        manager
            .insert_in_flight(first, pending())
            .expect("unique ticket");
        assert!(manager.in_flight_mut().remove(&first).is_some());

        let next = manager.allocate_cancel_ticket().expect("next ticket");
        assert_ne!(next, first);
        assert_eq!(next.raw(), first.raw() + 1);
    }

    #[test]
    fn allocator_reports_exhaustion_without_wrapping() {
        let mut manager = UringCancelManager::with_capacity(4);
        manager.next_cancel_ticket = CancelTicket::MAX_RAW;

        let last = manager.allocate_cancel_ticket().expect("maximum ticket");
        assert_eq!(last.raw(), CancelTicket::MAX_RAW);
        assert_eq!(
            manager.allocate_cancel_ticket(),
            Err(CancelTicketError::Exhausted)
        );
        assert_eq!(manager.next_cancel_ticket, CancelTicket::MAX_RAW + 1);
    }

    #[test]
    fn cancel_intents_merge_by_generation_and_keep_ticket_lifetime() {
        let mut manager = UringCancelManager::with_capacity(2);
        let target = request(0, 1, CancelMode::UserVisible);
        let stale = request(0, 2, CancelMode::Abandon);
        let other = request(1, 1, CancelMode::UserVisible);
        let disposition = manager.request(target).expect("first intent");
        let ticket = match disposition {
            CancelRequestDisposition::New { ticket } => ticket,
            other => panic!("unexpected first disposition: {other:?}"),
        };

        assert_eq!(
            manager.request(target),
            Ok(CancelRequestDisposition::AlreadyPending { ticket })
        );
        assert_eq!(
            manager.request(stale),
            Err(CancelIntentError::GenerationMismatch)
        );
        assert_eq!(
            manager.request(PendingCancel {
                target: target.target,
                mode: CancelMode::Abandon,
            }),
            Ok(CancelRequestDisposition::Merged { ticket })
        );
        assert_eq!(
            manager.front_pending().map(|request| request.mode),
            Some(CancelMode::Abandon)
        );

        let other_ticket = match manager.request(other).expect("second intent") {
            CancelRequestDisposition::New { ticket } => ticket,
            disposition => panic!("unexpected second disposition: {disposition:?}"),
        };
        assert_eq!(manager.pending_len(), 2);

        manager
            .mark_staged(ticket, target.target)
            .expect("pending intent should become staged");
        assert_eq!(manager.pending_len(), 1);
        assert_eq!(
            manager.front_pending().map(|request| request.target),
            Some(other.target)
        );
        assert_eq!(
            manager.mark_staged(ticket, target.target),
            Err(CancelIntentError::InvalidPhase)
        );
        assert_eq!(
            manager.finish_ticket(ticket, target.target),
            Some(PendingCancel {
                target: target.target,
                mode: CancelMode::Abandon,
            })
        );
        assert_eq!(manager.ticket_for(target.target), None);
        assert_eq!(manager.ticket_for(other.target), Some(other_ticket));
    }

    #[test]
    fn cancel_intent_capacity_is_indexed_and_sidecar_merge_updates_mode() {
        let mut manager = UringCancelManager::with_capacity(2);
        let first = request(0, 1, CancelMode::UserVisible);
        let first_ticket = match manager.request(first).expect("first intent") {
            CancelRequestDisposition::New { ticket } => ticket,
            disposition => panic!("unexpected disposition: {disposition:?}"),
        };
        manager
            .insert_in_flight(first_ticket, first)
            .expect("sidecar insert");

        assert_eq!(
            manager.request(request(2, 1, CancelMode::Abandon)),
            Err(CancelIntentError::Capacity)
        );
        assert_eq!(
            manager.request(request(0, 1, CancelMode::Abandon)),
            Ok(CancelRequestDisposition::Merged {
                ticket: first_ticket
            })
        );
        assert_eq!(
            manager.in_flight_targets().collect::<Vec<_>>(),
            vec![(first_ticket, first.target)]
        );
        assert_eq!(
            manager
                .in_flight_mut()
                .get(&first_ticket)
                .map(|request| request.mode),
            Some(CancelMode::Abandon)
        );
        assert_eq!(manager.pending_len(), 1);
    }
}
