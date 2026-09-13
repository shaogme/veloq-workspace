use veloq_driver_core::driver::{
    CancelMode, CancelRequest, CancelTicket, CancelTicketError, OpToken, RemoteCancelSender,
};
use veloq_std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    sync::mpsc,
};

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

pub(crate) struct UringCancelManager {
    pending_cancellations: VecDeque<PendingCancel>,
    pending_cancel_cqes: HashMap<CancelTicket, PendingCancel>,
    next_cancel_ticket: u64,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
}

impl UringCancelManager {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            pending_cancellations: VecDeque::new(),
            pending_cancel_cqes: HashMap::default(),
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
    pub(crate) fn push_pending(&mut self, request: PendingCancel) {
        self.pending_cancellations.push_back(request);
    }

    #[inline]
    pub(crate) fn pop_pending(&mut self) -> Option<PendingCancel> {
        self.pending_cancellations.pop_front()
    }

    #[inline]
    pub(crate) fn front_pending(&self) -> Option<&PendingCancel> {
        self.pending_cancellations.front()
    }

    #[inline]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending_cancellations.len()
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
    pub(crate) fn insert_in_flight(
        &mut self,
        ticket: CancelTicket,
        pending: PendingCancel,
    ) -> Result<(), PendingCancel> {
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

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn pending_targets(&self) -> impl Iterator<Item = OpToken> + '_ {
        self.pending_cancellations
            .iter()
            .map(|request| request.target)
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

    #[test]
    fn allocator_is_monotonic_and_ignores_in_flight_map() {
        let mut manager = UringCancelManager::new();
        let first = manager.allocate_cancel_ticket().expect("first ticket");
        manager
            .insert_in_flight(first, pending())
            .expect("unique ticket");

        let next = manager.allocate_cancel_ticket().expect("second ticket");
        assert_eq!(next.raw(), first.raw() + 1);
    }

    #[test]
    fn allocator_never_reuses_a_ticket_removed_from_the_sidecar() {
        let mut manager = UringCancelManager::new();
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
        let mut manager = UringCancelManager::new();
        manager.next_cancel_ticket = CancelTicket::MAX_RAW;

        let last = manager.allocate_cancel_ticket().expect("maximum ticket");
        assert_eq!(last.raw(), CancelTicket::MAX_RAW);
        assert_eq!(
            manager.allocate_cancel_ticket(),
            Err(CancelTicketError::Exhausted)
        );
        assert_eq!(manager.next_cancel_ticket, CancelTicket::MAX_RAW + 1);
    }
}
