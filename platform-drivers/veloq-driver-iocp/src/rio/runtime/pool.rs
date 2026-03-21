//! UDP receive-pool implementation for RIO.
//!
//! This module provides a pool of pre-registered buffers for efficient UDP reception.
//! The core datapath logic is located in the `datapath` submodule.

pub(crate) mod datapath;

use crate::net::addr::SockAddrStorage;
use crate::rio::core::submit_ops::RioBufferId;
use std::collections::VecDeque;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Networking::WinSock::RIORESULT;

pub(crate) use datapath::UdpPoolManager;

pub(crate) const UDP_RECV_POOL_MIN_CREDITS: usize = 2;
pub(crate) const UDP_RECV_POOL_INITIAL_CREDITS: usize = 4;
pub(crate) const UDP_RECV_POOL_MAX_CREDITS: usize = 16;
pub(crate) const UDP_RECV_POOL_QUEUE_CAP: usize = 256;

pub(crate) const POOL_CTX_TAG: usize = 1;

pub(crate) struct UdpRecvDatagram {
    pub(crate) buf: FixedBuf,
    pub(crate) addr: SockAddrStorage,
    pub(crate) addr_len: i32,
}

pub(crate) struct UdpMailbox {
    pub(crate) queue: VecDeque<UdpRecvDatagram>,
    pub(crate) waiters: VecDeque<UdpWaiter>,
}

impl UdpMailbox {
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::with_capacity(UDP_RECV_POOL_QUEUE_CAP),
            waiters: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpWaiterKind {
    Stream,
    Recv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UdpWaiter {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) kind: UdpWaiterKind,
}

pub(crate) struct UdpRecvPoolSlot {
    pub(crate) buf: FixedBuf,
    pub(crate) addr: Box<SockAddrStorage>,
    pub(crate) addr_buf_id: RioBufferId,
    pub(crate) in_flight: bool,
    pub(crate) stop_requested: bool,
}

pub(crate) struct UdpRecvPool {
    pub(crate) slots: Vec<UdpRecvPoolSlot>,
    pub(crate) spare_bufs: VecDeque<FixedBuf>,
    pub(crate) min_credits: usize,
    pub(crate) max_credits: usize,
    pub(crate) target_credits: usize,
    pub(crate) idle_hits: u32,
    pub(crate) state: UdpPoolState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpPoolState {
    Running,
    Draining,
    Closed,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PoolCompletionEvent {
    SlotMissing,
    DrainingAck,
    ReceivedNoDatagram,
    DatagramQueued { resubmit: bool },
}

#[derive(Default, Debug, Clone, Copy)]
pub(super) struct CompletionActions {
    pub(super) resubmit_slot: Option<usize>,
    pub(super) dispatch_waiters: bool,
    pub(super) rebalance_pool: bool,
}

impl UdpRecvPool {
    pub(super) fn update_state(
        &mut self,
        mailbox: &mut UdpMailbox,
        slot_idx: usize,
        res: &RIORESULT,
    ) -> PoolCompletionEvent {
        let Some(slot) = self.slots.get_mut(slot_idx) else {
            return PoolCompletionEvent::SlotMissing;
        };

        slot.in_flight = false;
        let stopping = slot.stop_requested;
        slot.stop_requested = false;

        if !matches!(self.state, UdpPoolState::Running) {
            return PoolCompletionEvent::DrainingAck;
        }

        if !(res.Status == 0 && res.BytesTransferred > 0) {
            return PoolCompletionEvent::ReceivedNoDatagram;
        }

        if mailbox.queue.len() >= UDP_RECV_POOL_QUEUE_CAP {
            let _ = mailbox.queue.pop_front();
        }

        let replacement_buf = self.spare_bufs.pop_front().or_else(|| {
            std::num::NonZeroUsize::new(slot.buf.capacity())
                .and_then(|cap| FixedBuf::alloc_heap(cap).ok())
        });

        if let Some(new_buf) = replacement_buf {
            let mut old_buf = std::mem::replace(&mut slot.buf, new_buf);
            old_buf.set_len(res.BytesTransferred as usize);

            mailbox.queue.push_back(UdpRecvDatagram {
                buf: old_buf,
                addr: *slot.addr,
                addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
            });

            return PoolCompletionEvent::DatagramQueued {
                resubmit: !stopping && slot_idx < self.target_credits,
            };
        }

        PoolCompletionEvent::ReceivedNoDatagram
    }

    pub(super) fn plan_actions(event: PoolCompletionEvent, slot_idx: usize) -> CompletionActions {
        match event {
            PoolCompletionEvent::DatagramQueued { resubmit } => CompletionActions {
                resubmit_slot: resubmit.then_some(slot_idx),
                dispatch_waiters: true,
                rebalance_pool: true,
            },
            PoolCompletionEvent::ReceivedNoDatagram => CompletionActions {
                dispatch_waiters: true,
                rebalance_pool: true,
                ..CompletionActions::default()
            },
            _ => CompletionActions::default(),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct UdpRecvPoolDebugStats {
    pub(crate) min_credits: usize,
    pub(crate) max_credits: usize,
    pub(crate) target_credits: usize,
    pub(crate) waiters_len: usize,
}
