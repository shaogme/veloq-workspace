use super::RioDispatch;
use super::registry::RIO_INVALID_BUFFERID;
use crate::SockAddrStorage;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::op::RecvFromPayload;
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOp, IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::{OverlappedEntry, STATE_COMPLETED};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::{ERROR_OPERATION_ABORTED, HANDLE};
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_RQ, RIORESULT, WSAGetLastError,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

pub const UDP_POOL_USER_DATA: usize = usize::MAX - 2;
const UDP_RECV_POOL_MIN_CREDITS: usize = 2;
const UDP_RECV_POOL_INITIAL_CREDITS: usize = 4;
const UDP_RECV_POOL_MAX_CREDITS: usize = 16;
const UDP_RECV_POOL_BUF_SIZE: usize = 65_536;
pub const UDP_RECV_POOL_QUEUE_CAP: usize = 256;

pub const POOL_CTX_TAG: usize = 1;

pub struct UdpRecvDatagram {
    pub data: Vec<u8>,
    pub addr: SockAddrStorage,
    pub addr_len: i32,
}

pub struct UdpRecvPoolSlot {
    pub data: Box<[u8]>,
    pub addr: Box<SockAddrStorage>,
    pub data_buf_id: RIO_BUFFERID,
    pub addr_buf_id: RIO_BUFFERID,
    pub in_flight: bool,
    pub stop_requested: bool,
}

pub struct UdpRecvPool {
    pub rq: RIO_RQ,
    pub slots: Vec<UdpRecvPoolSlot>,
    pub queue: VecDeque<UdpRecvDatagram>,
    pub waiters: VecDeque<(usize, u32)>,
    pub min_credits: usize,
    pub max_credits: usize,
    pub target_credits: usize,
    pub idle_hits: u32,
    pub shutting_down: bool,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct UdpRecvPoolDebugStats {
    pub min_credits: usize,
    pub max_credits: usize,
    pub target_credits: usize,
    pub slots_len: usize,
    pub in_flight: usize,
    pub waiters_len: usize,
    pub queue_len: usize,
    pub idle_hits: u32,
}

pub struct UdpPoolManager {
    pub(crate) udp_recv_pools: FxHashMap<HANDLE, UdpRecvPool>,
    pub(crate) udp_ctx_map: FxHashMap<u32, (HANDLE, usize)>,
    pub(crate) udp_next_ctx: u32,
}

impl UdpPoolManager {
    pub fn new() -> Self {
        Self {
            udp_recv_pools: FxHashMap::default(),
            udp_ctx_map: FxHashMap::default(),
            udp_next_ctx: 1,
        }
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    #[inline]
    pub fn encode_pool_context(token: u32) -> *const std::ffi::c_void {
        (((token as usize) << 1) | POOL_CTX_TAG) as *const std::ffi::c_void
    }

    fn alloc_udp_ctx_token(&mut self, handle: HANDLE, slot_idx: usize) -> u32 {
        loop {
            let token = self.udp_next_ctx;
            self.udp_next_ctx = self.udp_next_ctx.wrapping_add(1);
            if token == 0 {
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(e) = self.udp_ctx_map.entry(token) {
                e.insert((handle, slot_idx));
                return token;
            }
        }
    }

    fn create_udp_pool_slot(&self, dispatch: &RioDispatch) -> io::Result<UdpRecvPoolSlot> {
        let mut data = vec![0u8; UDP_RECV_POOL_BUF_SIZE].into_boxed_slice();
        let mut addr = Box::new(SockAddrStorage::default());

        let data_buf_id =
            unsafe { (dispatch.register_buffer)(data.as_mut_ptr(), data.len() as u32) };
        if data_buf_id == RIO_INVALID_BUFFERID {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIORegisterBuffer failed for UDP recv pool data buffer",
            ));
        }

        let addr_buf_id = unsafe {
            (dispatch.register_buffer)(
                (&mut *addr as *mut SockAddrStorage).cast::<u8>(),
                std::mem::size_of::<SockAddrStorage>() as u32,
            )
        };
        if addr_buf_id == RIO_INVALID_BUFFERID {
            unsafe { (dispatch.deregister_buffer)(data_buf_id) };
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIORegisterBuffer failed for UDP recv pool addr buffer",
            ));
        }

        Ok(UdpRecvPoolSlot {
            data,
            addr,
            data_buf_id,
            addr_buf_id,
            in_flight: false,
            stop_requested: false,
        })
    }

    fn deregister_udp_pool_slot(&self, slot: UdpRecvPoolSlot, dispatch: &RioDispatch) {
        if slot.data_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (dispatch.deregister_buffer)(slot.data_buf_id) };
        }
        if slot.addr_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (dispatch.deregister_buffer)(slot.addr_buf_id) };
        }
    }

    pub fn submit_udp_pool_slot(
        &mut self,
        handle: HANDLE,
        slot_idx: usize,
        dispatch: &RioDispatch,
    ) -> io::Result<usize> {
        let rq = {
            let pool = self
                .udp_recv_pools
                .get(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
            if pool.shutting_down {
                return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
            }
            pool.rq
        };

        let token = self.alloc_udp_ctx_token(handle, slot_idx);
        let pool = self
            .udp_recv_pools
            .get_mut(&handle)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
        let slot = pool
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool slot missing"))?;
        slot.in_flight = true;
        slot.stop_requested = false;

        let data_buf = RIO_BUF {
            BufferId: slot.data_buf_id,
            Offset: 0,
            Length: UDP_RECV_POOL_BUF_SIZE as u32,
        };
        let addr_buf = RIO_BUF {
            BufferId: slot.addr_buf_id,
            Offset: 0,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };
        let recv_ex_fn = dispatch.receive_ex;
        let req_ctx = Self::encode_pool_context(token);

        let ret = unsafe {
            recv_ex_fn(
                rq,
                &data_buf,
                1,
                std::ptr::null(),
                &addr_buf,
                std::ptr::null(),
                std::ptr::null(),
                0,
                req_ctx,
            )
        };
        if ret == 0 {
            self.udp_ctx_map.remove(&token);
            if let Some(pool) = self.udp_recv_pools.get_mut(&handle)
                && let Some(slot) = pool.slots.get_mut(slot_idx)
            {
                slot.in_flight = false;
                slot.stop_requested = false;
            }
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceiveEx submit failed for UDP recv pool: handle=0x{:x}, slot_idx={}, rq=0x{:x}",
                    handle as usize, slot_idx, rq as usize
                ),
            ));
        }
        Ok(1)
    }

    pub fn grow_udp_pool_to(
        &mut self,
        handle: HANDLE,
        target: usize,
        dispatch: &RioDispatch,
    ) -> io::Result<usize> {
        let mut submissions = 0;
        loop {
            let (current, shutting_down) = {
                let pool = self
                    .udp_recv_pools
                    .get(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                (pool.slots.len(), pool.shutting_down)
            };
            if shutting_down {
                return Ok(submissions);
            }
            if current >= target {
                return Ok(submissions);
            }

            let slot = self.create_udp_pool_slot(dispatch)?;
            {
                let pool = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                pool.slots.push(slot);
            }
            let idx = current;
            match self.submit_udp_pool_slot(handle, idx, dispatch) {
                Ok(n) => submissions += n,
                Err(e) => {
                    let popped = self
                        .udp_recv_pools
                        .get_mut(&handle)
                        .and_then(|pool| pool.slots.pop());
                    if let Some(slot) = popped {
                        self.deregister_udp_pool_slot(slot, dispatch);
                    }
                    return Err(e);
                }
            }
        }
    }

    pub fn trim_udp_pool_tail(&mut self, handle: HANDLE, dispatch: &RioDispatch) {
        loop {
            let maybe_slot = {
                let Some(pool) = self.udp_recv_pools.get_mut(&handle) else {
                    return;
                };
                if !pool.shutting_down && pool.slots.len() <= pool.target_credits {
                    return;
                }
                if pool.slots.last().is_some_and(|slot| slot.in_flight) {
                    return;
                }
                pool.slots.pop()
            };
            if let Some(slot) = maybe_slot {
                self.deregister_udp_pool_slot(slot, dispatch);
            } else {
                return;
            }
        }
    }

    pub fn rebalance_udp_pool(
        &mut self,
        handle: HANDLE,
        dispatch: &RioDispatch,
    ) -> io::Result<usize> {
        if self
            .udp_recv_pools
            .get(&handle)
            .is_some_and(|pool| pool.shutting_down)
        {
            self.trim_udp_pool_tail(handle, dispatch);
            return Ok(0);
        }

        let desired = {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !pool.waiters.is_empty() {
                let bump = pool.waiters.len().clamp(1, 4);
                pool.target_credits = (pool.target_credits + bump).min(pool.max_credits);
                pool.idle_hits = 0;
            } else if pool.queue.is_empty() {
                pool.idle_hits = pool.idle_hits.saturating_add(1);
                if pool.idle_hits >= 64 && pool.target_credits > pool.min_credits {
                    pool.target_credits -= 1;
                    pool.idle_hits = 0;
                }
            } else {
                pool.idle_hits = 0;
            }

            pool.target_credits
        };

        let submissions = self.grow_udp_pool_to(handle, desired, dispatch)?;
        self.trim_udp_pool_tail(handle, dispatch);
        Ok(submissions)
    }

    pub fn ensure_udp_recv_pool(
        &mut self,
        handle: HANDLE,
        rq: RIO_RQ,
        dispatch: &RioDispatch,
    ) -> io::Result<usize> {
        if self.udp_recv_pools.contains_key(&handle) {
            return Ok(0);
        }
        let min = UDP_RECV_POOL_MIN_CREDITS;
        let max = UDP_RECV_POOL_MAX_CREDITS.max(min);
        let initial = UDP_RECV_POOL_INITIAL_CREDITS.clamp(min, max);

        self.udp_recv_pools.insert(
            handle,
            UdpRecvPool {
                rq,
                slots: Vec::with_capacity(max),
                queue: VecDeque::with_capacity(initial),
                waiters: VecDeque::new(),
                min_credits: min,
                max_credits: max,
                target_credits: initial,
                idle_hits: 0,
                shutting_down: false,
            },
        );

        self.grow_udp_pool_to(handle, initial, dispatch)
    }

    fn deliver_udp_datagram_to_waiter(
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        user_data: usize,
        expected_generation: u32,
        datagram: UdpRecvDatagram,
    ) -> bool {
        if user_data >= ops.local.len() {
            return false;
        }
        let op = &mut ops.local[user_data];
        let slot = &ops.shared.slots[user_data];
        if op.platform_data.generation != expected_generation {
            return false;
        }
        if !matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
            return false;
        }

        let Some(iocp_op) = (unsafe { &mut *slot.op.get() }).as_mut() else {
            return false;
        };

        let payload: &mut RecvFromPayload = unsafe { &mut *iocp_op.payload.recv_from };
        let copy_len = std::cmp::min(payload.op.buf.capacity(), datagram.data.len());
        payload.op.buf.as_slice_mut()[..copy_len].copy_from_slice(&datagram.data[..copy_len]);
        payload.op.buf.set_len(copy_len);
        payload.addr = datagram.addr;
        payload.addr_len = datagram.addr_len;

        op.platform_data.rio_pool_waiting = false;
        op.platform_data.lifecycle = OpLifecycle::Completed(Ok(copy_len));
        unsafe { *slot.result.get() = Some(Ok(copy_len)) };
        slot.state.store(STATE_COMPLETED, Ordering::Release);
        slot.waker.wake();
        true
    }

    pub fn dispatch_udp_waiters_for_handle(
        &mut self,
        handle: HANDLE,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
    ) {
        loop {
            let (waiter, datagram) = {
                let Some(pool) = self.udp_recv_pools.get_mut(&handle) else {
                    return;
                };
                let Some(waiter) = pool.waiters.pop_front() else {
                    return;
                };
                let Some(datagram) = pool.queue.pop_front() else {
                    pool.waiters.push_front(waiter);
                    return;
                };
                (waiter, datagram)
            };

            let (user_data, generation) = waiter;
            if !Self::deliver_udp_datagram_to_waiter(ops, user_data, generation, datagram)
                && let Some(pool) = self.udp_recv_pools.get_mut(&handle)
            {
                if pool.queue.len() > UDP_RECV_POOL_QUEUE_CAP {
                    let _ = pool.queue.pop_front();
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_submit_recv_from_pooled(
        &mut self,
        handle: HANDLE,
        rq: RIO_RQ,
        user_data: usize,
        generation: u32,
        buf: &mut FixedBuf,
        addr: &mut SockAddrStorage,
        addr_len: &mut i32,
        overlapped: *mut OVERLAPPED,
        dispatch: &RioDispatch,
    ) -> io::Result<(SubmissionResult, usize)> {
        let mut total_submissions = self.ensure_udp_recv_pool(handle, rq, dispatch)?;
        {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if pool.shutting_down {
                return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
            }

            if let Some(datagram) = pool.queue.pop_front() {
                let copy_len = std::cmp::min(buf.capacity(), datagram.data.len());
                buf.as_slice_mut()[..copy_len].copy_from_slice(&datagram.data[..copy_len]);
                buf.set_len(copy_len);
                *addr = datagram.addr;
                *addr_len = datagram.addr_len;

                let entry = overlapped as *mut OverlappedEntry;
                unsafe {
                    (*entry).blocking_result = Some(Ok(copy_len));
                }
                return Ok((SubmissionResult::PostToQueue, total_submissions));
            }

            pool.waiters.push_back((user_data, generation));
        }
        total_submissions += self.rebalance_udp_pool(handle, dispatch)?;
        Ok((SubmissionResult::Pending, total_submissions))
    }

    pub fn cancel_udp_recv_waiter(
        &mut self,
        handle: HANDLE,
        user_data: usize,
        generation: u32,
        dispatch: &RioDispatch,
    ) {
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.waiters.retain(|&(ud, waiter_generation)| {
                !(ud == user_data && waiter_generation == generation)
            });
        }
        let _ = self.rebalance_udp_pool(handle, dispatch);
    }

    pub fn ack_udp_pool_completion(
        &mut self,
        completion_generation: u32,
    ) -> Option<(HANDLE, usize)> {
        self.udp_ctx_map.remove(&completion_generation)
    }

    /// Returns `(drained_handle, submissions)` if the pool for that handle was fully drained and
    /// removed, so the caller can also clean up the RQ mapping in the registry.
    pub fn handle_completion(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        res: &RIORESULT,
        completion_generation: u32,
        dispatch: &RioDispatch,
    ) -> (Option<HANDLE>, usize) {
        let mut submissions = 0;
        let Some((handle, slot_idx)) = self.ack_udp_pool_completion(completion_generation) else {
            return (None, 0);
        };

        let mut should_resubmit = false;
        let mut should_dispatch = false;
        let mut should_rebalance = false;

        if let Some(pool) = self.udp_recv_pools.get_mut(&handle)
            && let Some(slot) = pool.slots.get_mut(slot_idx)
        {
            slot.in_flight = false;
            let stopping = slot.stop_requested;
            slot.stop_requested = false;

            if !pool.shutting_down {
                if res.Status == 0 && res.BytesTransferred > 0 {
                    if pool.queue.len() >= UDP_RECV_POOL_QUEUE_CAP {
                        let _ = pool.queue.pop_front();
                    }
                    pool.queue.push_back(UdpRecvDatagram {
                        data: slot.data[..(res.BytesTransferred as usize)].to_vec(),
                        addr: *slot.addr,
                        addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
                    });
                }
                should_resubmit = !stopping && slot_idx < pool.target_credits;
                should_dispatch = true;
                should_rebalance = true;
            }
        }

        if should_resubmit {
            if let Ok(n) = self.submit_udp_pool_slot(handle, slot_idx, dispatch) {
                submissions += n;
            }
        }
        if should_dispatch {
            self.dispatch_udp_waiters_for_handle(handle, ops);
        }
        if should_rebalance {
            self.trim_udp_pool_tail(handle, dispatch);
            if let Ok(n) = self.rebalance_udp_pool(handle, dispatch) {
                submissions += n;
            }
        }
        if self.cleanup_shutdown_udp_pool_if_drained(handle, dispatch) {
            (Some(handle), submissions)
        } else {
            (None, submissions)
        }
    }

    pub fn begin_udp_pool_shutdown(&mut self) {
        for pool in self.udp_recv_pools.values_mut() {
            pool.shutting_down = true;
            pool.target_credits = 0;
            pool.queue.clear();
            pool.waiters.clear();
            for slot in &mut pool.slots {
                if slot.in_flight {
                    slot.stop_requested = true;
                }
            }
        }
    }

    pub fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE, dispatch: &RioDispatch) {
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.shutting_down = true;
            pool.target_credits = 0;
            pool.queue.clear();
            pool.waiters.clear();
            for slot in &mut pool.slots {
                if slot.in_flight {
                    slot.stop_requested = true;
                }
            }
        }
        self.cleanup_shutdown_udp_pool_if_drained(handle, dispatch);
    }

    /// Returns `true` if the pool was fully drained and removed.
    pub fn cleanup_shutdown_udp_pool_if_drained(
        &mut self,
        handle: HANDLE,
        dispatch: &RioDispatch,
    ) -> bool {
        let drained = self.udp_recv_pools.get(&handle).is_some_and(|pool| {
            pool.shutting_down && pool.slots.iter().all(|slot| !slot.in_flight)
        });
        if !drained {
            return false;
        }

        self.udp_ctx_map.retain(|_, (h, _)| *h != handle);
        if let Some(pool) = self.udp_recv_pools.remove(&handle) {
            for slot in pool.slots {
                self.deregister_udp_pool_slot(slot, dispatch);
            }
        }
        true
    }

    /// Hard-timeout fallback for `Drop`: for any pool that still has in-flight
    /// slots after drain timed out, keep the memory alive via `std::mem::forget`
    /// instead of freeing while the kernel may still be touching it.
    /// Already-landed slots are deregistered normally.
    pub fn forget_in_flight_and_deregister_rest(&mut self, dispatch: &RioDispatch) {
        for (_handle, pool) in std::mem::take(&mut self.udp_recv_pools) {
            for slot in pool.slots {
                if slot.in_flight {
                    // Kernel may still be writing to this buffer – leak it.
                    std::mem::forget(slot);
                    continue;
                }
                self.deregister_udp_pool_slot(slot, dispatch);
            }
        }
    }

    #[cfg(test)]
    pub fn udp_pool_debug_stats(&self, handle: HANDLE) -> Option<UdpRecvPoolDebugStats> {
        self.udp_recv_pools
            .get(&handle)
            .map(|pool| UdpRecvPoolDebugStats {
                min_credits: pool.min_credits,
                max_credits: pool.max_credits,
                target_credits: pool.target_credits,
                slots_len: pool.slots.len(),
                in_flight: pool.slots.iter().filter(|s| s.in_flight).count(),
                waiters_len: pool.waiters.len(),
                queue_len: pool.queue.len(),
                idle_hits: pool.idle_hits,
            })
    }
}
