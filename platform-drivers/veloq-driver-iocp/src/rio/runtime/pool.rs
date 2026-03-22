//! UDP receive-pool implementation for RIO.
//!
//! This module provides a pool of pre-registered buffers for efficient UDP reception.
//! The core datapath logic is located in the `datapath submodule.

pub(crate) mod datapath;

use crate::net::addr::SockAddrStorage;
use crate::rio::core::submit_ops::RioBufferId;
use slotmap::{SlotMap, new_key_type};
use std::collections::VecDeque;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Networking::WinSock::{RIORESULT, WSAEMSGSIZE};

new_key_type! {
    pub(crate) struct SlotKey;
}

pub(crate) use datapath::UdpPoolManager;

pub(crate) const UDP_RECV_POOL_MIN_CREDITS: usize = 2;
pub(crate) const UDP_RECV_POOL_INITIAL_CREDITS: usize = 4;
pub(crate) const UDP_RECV_POOL_MAX_CREDITS: usize = 16;
pub(crate) const UDP_RECV_POOL_QUEUE_CAP: usize = 256;
pub(crate) const UDP_RECV_POOL_CHUNK_SIZE: usize = 8192;
pub(crate) const UDP_RECV_POOL_SLAB_CHUNKS: usize = 512;

pub(crate) const POOL_CTX_TAG: usize = 1;

pub(crate) struct UdpPoolPacket {
    pub(crate) buf: FixedBuf,
    pub(crate) addr: SockAddrStorage,
    pub(crate) addr_len: i32,
}

pub(crate) struct UdpBufferSlab {
    pub(crate) _backing: FixedBuf,
    pub(crate) rio_id: RioBufferId,
    pub(crate) chunk_size: usize,
    pub(crate) chunks: Vec<FixedBuf>,
    pub(crate) free_indices: VecDeque<u32>,
}

impl UdpBufferSlab {
    #[inline]
    pub(crate) fn chunk_offset(&self, idx: u32) -> u32 {
        (idx as usize)
            .saturating_mul(self.chunk_size)
            .min(u32::MAX as usize) as u32
    }

    #[inline]
    pub(crate) fn chunk_capacity(&self) -> usize {
        self.chunk_size
    }

    #[inline]
    pub(crate) fn chunk_view(&self, idx: u32, len: usize) -> Option<FixedBuf> {
        let chunk = self.chunks.get(idx as usize)?;
        if len > chunk.capacity() {
            return None;
        }
        Some(chunk.slice(0..len))
    }
}

pub(crate) struct UdpMailbox {
    pub(crate) queue: VecDeque<UdpPoolPacket>,
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
    pub(crate) current_idx: u32,
    pub(crate) addr: Box<SockAddrStorage>,
    pub(crate) addr_buf_id: RioBufferId,
    pub(crate) in_flight: bool,
    pub(crate) stop_requested: bool,
}

pub(crate) struct UdpRecvPool {
    pub(crate) slots: SlotMap<SlotKey, UdpRecvPoolSlot>,
    pub(crate) slab: Option<UdpBufferSlab>,
    pub(crate) min_credits: usize,
    pub(crate) max_credits: usize,
    pub(crate) target_credits: usize,
    pub(crate) idle_hits: u32,
    pub(crate) state: UdpPoolState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpPoolState {
    Uninitialized,
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
    pub(super) resubmit_slot: Option<SlotKey>,
    pub(super) dispatch_waiters: bool,
    pub(super) rebalance_pool: bool,
}

impl UdpRecvPool {
    #[inline]
    pub(super) fn is_datagram_completion(res: &RIORESULT) -> bool {
        (res.Status == 0 || res.Status == WSAEMSGSIZE as i32) && res.BytesTransferred > 0
    }

    pub(crate) fn uninit() -> Self {
        Self {
            slots: SlotMap::with_key(),
            slab: None,
            min_credits: 0,
            max_credits: 0,
            target_credits: 0,
            idle_hits: 0,
            state: UdpPoolState::Uninitialized,
        }
    }

    pub(super) fn update_state(
        &mut self,
        mailbox: &mut UdpMailbox,
        slot_key: SlotKey,
        res: &RIORESULT,
    ) -> PoolCompletionEvent {
        let Some(slot) = self.slots.get_mut(slot_key) else {
            return PoolCompletionEvent::SlotMissing;
        };

        slot.in_flight = false;
        let stopping = slot.stop_requested;
        slot.stop_requested = false;

        if !matches!(self.state, UdpPoolState::Running) {
            return PoolCompletionEvent::DrainingAck;
        }

        if !Self::is_datagram_completion(res) {
            return PoolCompletionEvent::ReceivedNoDatagram;
        }

        if mailbox.queue.len() >= UDP_RECV_POOL_QUEUE_CAP {
            let _ = mailbox.queue.pop_front();
        }

        let Some(slab) = self.slab.as_mut() else {
            return PoolCompletionEvent::ReceivedNoDatagram;
        };
        let bytes = res.BytesTransferred as usize;
        if bytes > slab.chunk_capacity() {
            return PoolCompletionEvent::ReceivedNoDatagram;
        }
        let Some(next_idx) = slab.free_indices.pop_front() else {
            return PoolCompletionEvent::ReceivedNoDatagram;
        };
        let completed_idx = std::mem::replace(&mut slot.current_idx, next_idx);
        if let Some(buf) = slab.chunk_view(completed_idx, bytes) {
            mailbox.queue.push_back(UdpPoolPacket {
                buf,
                addr: *slot.addr,
                addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
            });
            return PoolCompletionEvent::DatagramQueued {
                resubmit: !stopping && self.slots.len() <= self.target_credits,
            };
        }

        PoolCompletionEvent::ReceivedNoDatagram
    }

    pub(super) fn plan_actions(event: PoolCompletionEvent, slot_key: SlotKey) -> CompletionActions {
        match event {
            PoolCompletionEvent::DatagramQueued { resubmit } => CompletionActions {
                resubmit_slot: resubmit.then_some(slot_key),
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
