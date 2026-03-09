use super::registry::RIO_INVALID_BUFFERID;
use super::{RioContext, RioDispatch};
use crate::SockAddrStorage;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOp, IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::STATE_COMPLETED;
use crate::op::{IoFd, UdpRecvStream};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::{ERROR_OPERATION_ABORTED, HANDLE};
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_RQ, RIORESULT, WSAGetLastError,
};

pub const UDP_POOL_USER_DATA: usize = usize::MAX - 2;
const UDP_RECV_POOL_MIN_CREDITS: usize = 2;
const UDP_RECV_POOL_INITIAL_CREDITS: usize = 4;
const UDP_RECV_POOL_MAX_CREDITS: usize = 16;
pub const UDP_RECV_POOL_QUEUE_CAP: usize = 256;

pub const POOL_CTX_TAG: usize = 1;

pub struct UdpRecvDatagram {
    pub buf: FixedBuf,
    pub addr: SockAddrStorage,
    pub addr_len: i32,
}

pub struct UdpRecvPoolSlot {
    pub buf: FixedBuf,
    pub addr: Box<SockAddrStorage>,
    pub addr_buf_id: RIO_BUFFERID,
    pub in_flight: bool,
    pub stop_requested: bool,
}

pub struct UdpRecvPool {
    pub rq: RIO_RQ,
    pub slots: Vec<UdpRecvPoolSlot>,
    pub queue: VecDeque<UdpRecvDatagram>,
    pub waiters: VecDeque<(usize, u32)>,
    pub spare_bufs: VecDeque<FixedBuf>,
    pub min_credits: usize,
    pub max_credits: usize,
    pub target_credits: usize,
    pub idle_hits: u32,
    pub state: UdpPoolState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpPoolState {
    Running,
    Draining,
    Closed,
}

#[derive(Debug, Clone, Copy)]
enum PoolCompletionEvent {
    PoolMissing,
    SlotMissing,
    DrainingAck,
    ReceivedNoDatagram,
    DatagramQueued { resubmit: bool },
}

#[derive(Default, Debug, Clone, Copy)]
struct CompletionActions {
    resubmit_slot: Option<usize>,
    dispatch_waiters: bool,
    rebalance_pool: bool,
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

    fn create_udp_pool_slot(
        &self,
        buf: FixedBuf,
        dispatch: &RioDispatch,
    ) -> io::Result<UdpRecvPoolSlot> {
        let mut addr = Box::new(SockAddrStorage::default());

        let addr_buf_id = unsafe {
            (dispatch.register_buffer)(
                (&mut *addr as *mut SockAddrStorage).cast::<u8>(),
                std::mem::size_of::<SockAddrStorage>() as u32,
            )
        };
        if addr_buf_id == RIO_INVALID_BUFFERID {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIORegisterBuffer failed for UDP recv pool addr buffer",
            ));
        }

        Ok(UdpRecvPoolSlot {
            buf,
            addr,
            addr_buf_id,
            in_flight: false,
            stop_requested: false,
        })
    }

    #[inline]
    fn begin_draining(pool: &mut UdpRecvPool) {
        if matches!(pool.state, UdpPoolState::Running) {
            pool.state = UdpPoolState::Draining;
        }
        pool.target_credits = 0;
        pool.queue.clear();
        pool.waiters.clear();
        pool.spare_bufs.clear();
        for slot in &mut pool.slots {
            if slot.in_flight {
                slot.stop_requested = true;
            }
        }
    }

    fn deregister_udp_pool_slot(&self, slot: UdpRecvPoolSlot, dispatch: &RioDispatch) {
        // If this slot currently uses a lazily-registered heap buffer, deregister it
        // before dropping the FixedBuf memory.
        // Note: callers that need this must use `deregister_udp_pool_slot_with_registry`.
        if slot.addr_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (dispatch.deregister_buffer)(slot.addr_buf_id) };
        }
    }

    fn deregister_udp_pool_slot_with_registry(&self, slot: UdpRecvPoolSlot, ctx: &mut RioContext) {
        ctx.registry
            .deregister_heap_buffer_for_buf(&slot.buf, ctx.env);
        self.deregister_udp_pool_slot(slot, ctx.env.dispatch);
    }

    pub fn submit_udp_pool_slot(
        &mut self,
        target: (HANDLE, usize),
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (handle, slot_idx) = target;
        let rq = {
            let pool = self
                .udp_recv_pools
                .get(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
            if !matches!(pool.state, UdpPoolState::Running) {
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

        let (data_buf_id, offset) = ctx.registry.resolve_buffer_id(&slot.buf, ctx.env)?;

        slot.in_flight = true;
        slot.stop_requested = false;

        let data_buf = RIO_BUF {
            BufferId: data_buf_id,
            Offset: offset,
            Length: slot.buf.capacity() as u32,
        };
        let addr_buf = RIO_BUF {
            BufferId: slot.addr_buf_id,
            Offset: 0,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };
        let recv_ex_fn = ctx.env.dispatch.receive_ex;
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
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let mut submissions = 0;
        loop {
            let (current, state) = {
                let pool = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                (pool.slots.len(), pool.state)
            };
            if !matches!(state, UdpPoolState::Running) {
                return Ok(submissions);
            }
            if current >= target {
                return Ok(submissions);
            }

            let slot_buf = {
                let pool = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                match pool.spare_bufs.pop_front() {
                    Some(b) => b,
                    None => return Ok(submissions),
                }
            };

            let slot = self.create_udp_pool_slot(slot_buf, ctx.env.dispatch)?;
            {
                let pool = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                pool.slots.push(slot);
            }

            let idx = current;
            match self.submit_udp_pool_slot((handle, idx), ctx) {
                Ok(n) => submissions += n,
                Err(e) => {
                    let (popped_slot, is_running) = {
                        let mut pool = self.udp_recv_pools.get_mut(&handle);
                        (
                            pool.as_mut().and_then(|p| p.slots.pop()),
                            pool.as_ref()
                                .is_some_and(|p| matches!(p.state, UdpPoolState::Running)),
                        )
                    };

                    if let Some(s) = popped_slot {
                        let UdpRecvPoolSlot { buf, .. } = s;
                        if s.addr_buf_id != RIO_INVALID_BUFFERID {
                            unsafe { (ctx.env.dispatch.deregister_buffer)(s.addr_buf_id) };
                        }
                        if is_running
                            && let Some(pool) = self.udp_recv_pools.get_mut(&handle)
                        {
                            pool.spare_bufs.push_front(buf);
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    pub fn trim_udp_pool_tail(&mut self, handle: HANDLE, ctx: &mut RioContext) {
        loop {
            let maybe_slot = {
                let Some(pool) = self.udp_recv_pools.get_mut(&handle) else {
                    return;
                };
                if matches!(pool.state, UdpPoolState::Running) && pool.slots.len() <= pool.target_credits {
                    return;
                }
                if pool.slots.last().is_some_and(|slot| slot.in_flight) {
                    return;
                }
                pool.slots.pop()
            };
            if let Some(slot) = maybe_slot {
                self.deregister_udp_pool_slot_with_registry(slot, ctx);
            } else {
                return;
            }
        }
    }

    pub fn rebalance_udp_pool(
        &mut self,
        handle: HANDLE,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (desired, state) = {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !matches!(pool.state, UdpPoolState::Running) {
                (0, pool.state)
            } else {
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
                (pool.target_credits, pool.state)
            }
        };

        if !matches!(state, UdpPoolState::Running) {
            self.trim_udp_pool_tail(handle, ctx);
            return Ok(0);
        }

        let submissions = self.grow_udp_pool_to(handle, desired, ctx)?;
        self.trim_udp_pool_tail(handle, ctx);
        Ok(submissions)
    }

    pub fn ensure_udp_recv_pool(
        &mut self,
        target: (IoFd, HANDLE),
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (fd, handle) = target;
        if self.udp_recv_pools.contains_key(&handle) {
            return Ok(0);
        }
        let rq = ctx.registry.ensure_rq((handle, fd), ctx.env)?;
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
                spare_bufs: VecDeque::with_capacity(initial),
                min_credits: min,
                max_credits: max,
                target_credits: initial,
                idle_hits: 0,
                state: UdpPoolState::Running,
            },
        );

        self.grow_udp_pool_to(handle, initial, ctx)
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

        let stream_op: &mut UdpRecvStream = unsafe { &mut iocp_op.payload.udp_recv_stream };
        let datagram_len = datagram.buf.len();

        let addr = unsafe {
            let s = std::slice::from_raw_parts(
                &datagram.addr as *const _ as *const u8,
                datagram.addr_len as usize,
            );
            crate::to_socket_addr(s).unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap())
        };

        stream_op.result = Some(crate::op::UdpRecvDatagram {
            buf: datagram.buf,
            addr,
        });

        op.platform_data.rio_pool_waiting = false;
        op.platform_data.lifecycle = OpLifecycle::Completed(Ok(datagram_len));
        unsafe { *slot.result.get() = Some(Ok(datagram_len)) };
        slot.state.store(STATE_COMPLETED, Ordering::Release);
        slot.waker.wake();
        true
    }

    fn into_op_udp_datagram(datagram: UdpRecvDatagram) -> crate::op::UdpRecvDatagram {
        let addr = unsafe {
            let s = std::slice::from_raw_parts(
                &datagram.addr as *const _ as *const u8,
                datagram.addr_len as usize,
            );
            crate::to_socket_addr(s).unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap())
        };

        crate::op::UdpRecvDatagram {
            buf: datagram.buf,
            addr,
        }
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
                && pool.queue.len() > UDP_RECV_POOL_QUEUE_CAP
            {
                let _ = pool.queue.pop_front();
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_submit_udp_recv_stream_pooled(
        &mut self,
        target: (IoFd, HANDLE),
        stream_op: &mut crate::op::UdpRecvStream,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) -> io::Result<(SubmissionResult, usize)> {
        let (fd, handle) = target;
        let (user_data, generation) = uid;
        let mut total_submissions = self.ensure_udp_recv_pool((fd, handle), ctx)?;
        {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !matches!(pool.state, UdpPoolState::Running) {
                return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
            }

            if let Some(datagram) = pool.queue.pop_front() {
                stream_op.result = Some(Self::into_op_udp_datagram(datagram));
                total_submissions += self.rebalance_udp_pool(handle, ctx)?;
                return Ok((SubmissionResult::PostToQueue, total_submissions));
            }

            pool.waiters.push_back((user_data, generation));
        }
        total_submissions += self.rebalance_udp_pool(handle, ctx)?;
        Ok((SubmissionResult::Pending, total_submissions))
    }

    pub fn try_refill_udp_pool(
        &mut self,
        target: (IoFd, HANDLE),
        buf: FixedBuf,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (fd, handle) = target;
        let mut total_submissions = self.ensure_udp_recv_pool((fd, handle), ctx)?;
        let pool = self.udp_recv_pools.get_mut(&handle).unwrap();

        pool.spare_bufs.push_back(buf);
        total_submissions += self.rebalance_udp_pool(handle, ctx)?;
        Ok(total_submissions)
    }

    pub fn cancel_udp_recv_waiter(
        &mut self,
        handle: HANDLE,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) {
        let (user_data, generation) = uid;
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.waiters.retain(|&(ud, waiter_generation)| {
                !(ud == user_data && waiter_generation == generation)
            });
        }
        let _ = self.rebalance_udp_pool(handle, ctx);
    }

    pub fn ack_udp_pool_completion(
        &mut self,
        completion_generation: u32,
    ) -> Option<(HANDLE, usize)> {
        self.udp_ctx_map.remove(&completion_generation)
    }

    fn ack_pool_completion_event(
        &mut self,
        res: &RIORESULT,
        completion_generation: u32,
    ) -> Option<(HANDLE, usize, PoolCompletionEvent)> {
        let (handle, slot_idx) = self.ack_udp_pool_completion(completion_generation)?;
        let event = match self.udp_recv_pools.get_mut(&handle) {
            Some(pool) => Self::transition_pool_on_completion(pool, slot_idx, res),
            None => PoolCompletionEvent::PoolMissing,
        };
        Some((handle, slot_idx, event))
    }

    fn transition_pool_on_completion(
        pool: &mut UdpRecvPool,
        slot_idx: usize,
        res: &RIORESULT,
    ) -> PoolCompletionEvent {
        let Some(slot) = pool.slots.get_mut(slot_idx) else {
            return PoolCompletionEvent::SlotMissing;
        };

        slot.in_flight = false;
        let stopping = slot.stop_requested;
        slot.stop_requested = false;

        if !matches!(pool.state, UdpPoolState::Running) {
            return PoolCompletionEvent::DrainingAck;
        }

        if !(res.Status == 0 && res.BytesTransferred > 0) {
            return PoolCompletionEvent::ReceivedNoDatagram;
        }

        if pool.queue.len() >= UDP_RECV_POOL_QUEUE_CAP {
            let _ = pool.queue.pop_front();
        }

        let replacement_buf = pool.spare_bufs.pop_front().or_else(|| {
            std::num::NonZeroUsize::new(slot.buf.capacity())
                .and_then(|cap| FixedBuf::alloc_heap(cap).ok())
        });

        if let Some(new_buf) = replacement_buf {
            let mut old_buf = std::mem::replace(&mut slot.buf, new_buf);
            old_buf.set_len(res.BytesTransferred as usize);

            pool.queue.push_back(UdpRecvDatagram {
                buf: old_buf,
                addr: *slot.addr,
                addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
            });

            return PoolCompletionEvent::DatagramQueued {
                resubmit: !stopping && slot_idx < pool.target_credits,
            };
        }

        PoolCompletionEvent::ReceivedNoDatagram
    }

    fn plan_completion_actions(event: PoolCompletionEvent, slot_idx: usize) -> CompletionActions {
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
            PoolCompletionEvent::PoolMissing
            | PoolCompletionEvent::SlotMissing
            | PoolCompletionEvent::DrainingAck => CompletionActions::default(),
        }
    }

    pub fn handle_completion(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        comp_info: (&RIORESULT, u32),
        ctx: &mut RioContext,
    ) -> (Option<HANDLE>, usize) {
        let (res, completion_generation) = comp_info;
        let mut submissions = 0;
        let Some((handle, slot_idx, event)) =
            self.ack_pool_completion_event(res, completion_generation)
        else {
            return (None, 0);
        };

        let actions = Self::plan_completion_actions(event, slot_idx);

        if let Some(idx) = actions.resubmit_slot
            && let Ok(n) = self.submit_udp_pool_slot((handle, idx), ctx)
        {
            submissions += n;
        }
        if actions.dispatch_waiters {
            self.dispatch_udp_waiters_for_handle(handle, ops);
        }
        if actions.rebalance_pool {
            self.trim_udp_pool_tail(handle, ctx);
            if let Ok(n) = self.rebalance_udp_pool(handle, ctx) {
                submissions += n;
            }
        }
        if self.cleanup_shutdown_udp_pool_if_drained(handle, ctx) {
            (Some(handle), submissions)
        } else {
            (None, submissions)
        }
    }

    pub fn handle_completion_drain_only(
        &mut self,
        comp_info: (&RIORESULT, u32),
        ctx: &mut RioContext,
    ) -> Option<HANDLE> {
        let (res, completion_generation) = comp_info;
        let (handle, _slot_idx, _event) = self.ack_pool_completion_event(res, completion_generation)?;
        self.cleanup_shutdown_udp_pool_if_drained(handle, ctx)
            .then_some(handle)
    }

    pub fn begin_udp_pool_shutdown(&mut self) {
        for pool in self.udp_recv_pools.values_mut() {
            Self::begin_draining(pool);
        }
    }

    pub fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE, ctx: &mut RioContext) {
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            Self::begin_draining(pool);
        }
        self.cleanup_shutdown_udp_pool_if_drained(handle, ctx);
    }

    pub fn cleanup_shutdown_udp_pool_if_drained(
        &mut self,
        handle: HANDLE,
        ctx: &mut RioContext,
    ) -> bool {
        let drained = self.udp_recv_pools.get(&handle).is_some_and(|pool| {
            !matches!(pool.state, UdpPoolState::Running)
                && pool.slots.iter().all(|slot| !slot.in_flight)
        });
        if !drained {
            return false;
        }

        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.state = UdpPoolState::Closed;
        }
        self.udp_ctx_map.retain(|_, (h, _)| *h != handle);
        if let Some(pool) = self.udp_recv_pools.remove(&handle) {
            for slot in pool.slots {
                self.deregister_udp_pool_slot_with_registry(slot, ctx);
            }
        }
        true
    }

    pub fn forget_in_flight_and_deregister_rest(&mut self, ctx: &mut RioContext) {
        for (_handle, pool) in std::mem::take(&mut self.udp_recv_pools) {
            for slot in pool.slots {
                if slot.in_flight {
                    std::mem::forget(slot);
                    continue;
                }
                self.deregister_udp_pool_slot_with_registry(slot, ctx);
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
