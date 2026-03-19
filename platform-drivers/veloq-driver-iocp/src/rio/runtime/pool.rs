//! UDP receive-pool implementation for RIO.
//!
//! This module provides a pool of pre-registered buffers for efficient UDP reception.
//! The core datapath logic is located in the `datapath` submodule.

pub(crate) mod datapath;

use crate::net::addr::SockAddrStorage;
use crate::rio::core::submit_ops::RioBufferId;
use std::collections::VecDeque;
use veloq_buf::FixedBuf;

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

pub(crate) struct UdpRecvPoolSlot {
    pub(crate) buf: FixedBuf,
    pub(crate) addr: Box<SockAddrStorage>,
    pub(crate) addr_buf_id: RioBufferId,
    pub(crate) in_flight: bool,
    pub(crate) stop_requested: bool,
}

pub(crate) struct UdpRecvPool {
    pub(crate) slots: Vec<UdpRecvPoolSlot>,
    pub(crate) queue: VecDeque<UdpRecvDatagram>,
    pub(crate) waiters: VecDeque<(usize, u32)>,
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

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct UdpRecvPoolDebugStats {
    pub(crate) min_credits: usize,
    pub(crate) max_credits: usize,
    pub(crate) target_credits: usize,
    pub(crate) waiters_len: usize,
}
