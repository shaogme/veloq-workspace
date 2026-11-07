use core::slice::from_raw_parts;
use veloq_buf::FixedBuf;
use veloq_std::{boxed::Box, collections::VecDeque, net::SocketAddr};

use super::{
    ReceivePumpError,
    config::ReceivePumpConfig,
    datagram::DatagramQueue,
    types::{PendingCompletion, ReceiveRequest, ReceiveSlotState},
};

/// A fixed receive slot. Its buffer never moves while the pump exists.
pub struct ReceiveSlot {
    slot_id: u32,
    generation: u32,
    data_buf: FixedBuf,
    state: ReceiveSlotState,
    completed_len: usize,
    completed_addr: Option<SocketAddr>,
    current_request: Option<ReceiveRequest>,
}

impl ReceiveSlot {
    pub fn new(slot_id: u32, data_buf: FixedBuf) -> Self {
        Self {
            slot_id,
            generation: 0,
            data_buf,
            state: ReceiveSlotState::Free,
            completed_len: 0,
            completed_addr: None,
            current_request: None,
        }
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn state(&self) -> ReceiveSlotState {
        self.state
    }

    pub fn data_buf(&self) -> &FixedBuf {
        &self.data_buf
    }

    pub fn data_buf_mut(&mut self) -> &mut FixedBuf {
        &mut self.data_buf
    }

    pub fn completed_len(&self) -> Option<usize> {
        matches!(
            self.state,
            ReceiveSlotState::CompletedPending
                | ReceiveSlotState::LeaseHeld
                | ReceiveSlotState::SubmitPending
        )
        .then_some(self.completed_len)
    }

    /// View bytes written by the kernel while the slot is no longer in flight.
    ///
    /// The RIO receive path writes directly into the fixed buffer and reports the byte count
    /// separately, so the buffer's logical length is intentionally not changed here.
    pub fn completed_data(&self) -> Option<&[u8]> {
        let len = self.completed_len()?;
        Some(unsafe { from_raw_parts(self.data_buf.as_ptr(), len) })
    }

    pub fn current_request(&self) -> Option<ReceiveRequest> {
        self.current_request
    }

    pub(super) fn set_state(&mut self, state: ReceiveSlotState) {
        self.state = state;
    }

    pub(super) fn set_generation(&mut self, generation: u32) {
        self.generation = generation;
    }

    pub(super) fn set_completed_len(&mut self, len: usize) {
        self.completed_len = len;
    }

    pub(super) fn set_completed_addr(&mut self, addr: Option<SocketAddr>) {
        self.completed_addr = addr;
    }

    pub(super) fn set_current_request(&mut self, request: Option<ReceiveRequest>) {
        self.current_request = request;
    }
}

pub(super) struct ReceivePumpQueueParts {
    pub(super) slots: Box<[ReceiveSlot]>,
    pub(super) pending: VecDeque<PendingCompletion>,
    pub(super) held: VecDeque<PendingCompletion>,
    pub(super) datagrams: DatagramQueue,
}

pub(super) struct PumpQueues;

impl PumpQueues {
    pub(super) fn allocate(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        multishot: bool,
    ) -> Result<ReceivePumpQueueParts, ReceivePumpError> {
        config.validate(if multishot {
            config.kernel_capacity().get()
        } else {
            slots.len()
        })?;
        if (!multishot && slots.len() != config.kernel_capacity().get())
            || (multishot && !slots.is_empty())
        {
            return Err(ReceivePumpError::ReceiveConfigInvalid);
        }
        for (index, slot) in slots.iter().enumerate() {
            if slot.data_buf().capacity() < config.datagram_capacity().get()
                || slots[..index]
                    .iter()
                    .any(|previous| previous.slot_id() == slot.slot_id())
            {
                return Err(ReceivePumpError::ReceiveConfigInvalid);
            }
        }

        let pending_capacity = config.kernel_capacity().get();
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(pending_capacity)
            .map_err(|_| ReceivePumpError::ReceiveConfigInvalid)?;
        let mut held = VecDeque::new();
        held.try_reserve_exact(pending_capacity)
            .map_err(|_| ReceivePumpError::ReceiveConfigInvalid)?;
        let datagrams = DatagramQueue::new(pending_capacity, multishot)?;
        Ok(ReceivePumpQueueParts {
            slots,
            pending,
            held,
            datagrams,
        })
    }
}
