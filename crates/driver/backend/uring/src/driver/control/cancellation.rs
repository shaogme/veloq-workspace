use veloq_driver_core::driver::{
    CancelCompletionId, CancelMode, CancelRequest, OpToken, RemoteCancelSender,
};
use veloq_std::{
    collections::{HashMap, VecDeque},
    sync::mpsc,
};

#[derive(Debug, Clone, Copy)]
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
    pending_cancel_cqes: HashMap<CancelCompletionId, PendingCancel>,
    next_cancel_id: u16,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
}

impl UringCancelManager {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            pending_cancellations: VecDeque::new(),
            pending_cancel_cqes: HashMap::default(),
            next_cancel_id: 1,
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
    pub(crate) fn allocate_cancel_id(&mut self) -> CancelCompletionId {
        let raw = self.next_cancel_id;
        self.next_cancel_id = self.next_cancel_id.wrapping_add(1);
        if self.next_cancel_id == 0 {
            self.next_cancel_id = 1;
        }
        CancelCompletionId::new(raw)
    }

    #[inline]
    pub(crate) fn insert_in_flight(&mut self, id: CancelCompletionId, pending: PendingCancel) {
        self.pending_cancel_cqes.insert(id, pending);
    }

    #[inline]
    pub(crate) fn in_flight_mut(&mut self) -> &mut HashMap<CancelCompletionId, PendingCancel> {
        &mut self.pending_cancel_cqes
    }
}
