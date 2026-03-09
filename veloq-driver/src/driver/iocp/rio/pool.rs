use super::registry::RIO_INVALID_BUFFERID;
use super::{RioContext, RioDispatch};
use crate::SockAddrStorage;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOp, IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::STATE_COMPLETED;
use crate::op::UdpRecvStream;
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_RQ, RIORESULT, WSAGetLastError,
};

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
    pub(crate) pool: Option<UdpRecvPool>,
    pub(crate) udp_ctx_map: FxHashMap<u32, usize>,
    pub(crate) udp_next_ctx: u32,
}

impl UdpPoolManager {
    pub fn new() -> Self {
        Self {
            pool: None,
            udp_ctx_map: FxHashMap::default(),
            udp_next_ctx: 1,
        }
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    #[inline]
    pub fn encode_pool_context(actor_id: u32, token: u32) -> *const std::ffi::c_void {
        (((actor_id as usize) << 33) | ((token as usize) << 1) | POOL_CTX_TAG)
            as *const std::ffi::c_void
    }

    fn alloc_udp_ctx_token(&mut self, slot_idx: usize) -> u32 {
        loop {
            let token = self.udp_next_ctx;
            self.udp_next_ctx = self.udp_next_ctx.wrapping_add(1);
            if token == 0 {
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(e) = self.udp_ctx_map.entry(token) {
                e.insert(slot_idx);
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
        if slot.addr_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (dispatch.deregister_buffer)(slot.addr_buf_id) };
        }
    }

    fn deregister_udp_pool_slot_with_registry(&self, slot: UdpRecvPoolSlot, ctx: &mut RioContext) {
        ctx.registry
            .deregister_heap_buffer_for_buf(&slot.buf, ctx.env);
        self.deregister_udp_pool_slot(slot, ctx.env.dispatch);
    }

    fn submit_udp_pool_slot(
        &mut self,
        target: (usize, u32),
        rq: RIO_RQ,
        actor_id: u32,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (slot_idx, completion_token) = target;
        let Some(pool) = self.pool.as_mut() else {
            return Err(io_msg(IocpErrorContext::Rio, "UDP recv pool missing"));
        };
        if !matches!(pool.state, UdpPoolState::Running) {
            return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
        }

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
        let req_ctx = Self::encode_pool_context(actor_id, completion_token);

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
            self.udp_ctx_map.remove(&completion_token);
            if let Some(pool) = self.pool.as_mut()
                && let Some(slot) = pool.slots.get_mut(slot_idx)
            {
                slot.in_flight = false;
                slot.stop_requested = false;
            }
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceiveEx submit failed for UDP recv pool: slot_idx={}, rq=0x{:x}",
                    slot_idx, rq as usize
                ),
            ));
        }
        Ok(1)
    }

    fn grow_udp_pool_to(
        &mut self,
        target: usize,
        rq: RIO_RQ,
        actor_id: u32,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let mut submissions = 0;
        loop {
            let (current, state) = {
                let pool = self
                    .pool
                    .as_mut()
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
                    .pool
                    .as_mut()
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                match pool.spare_bufs.pop_front() {
                    Some(b) => b,
                    None => return Ok(submissions),
                }
            };

            let slot = self.create_udp_pool_slot(slot_buf, ctx.env.dispatch)?;
            {
                let pool = self
                    .pool
                    .as_mut()
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                pool.slots.push(slot);
            }

            let idx = current;
            let token = self.alloc_udp_ctx_token(idx);
            match self.submit_udp_pool_slot((idx, token), rq, actor_id, ctx) {
                Ok(n) => submissions += n,
                Err(e) => {
                    let (popped_slot, is_running) = {
                        let mut pool = self.pool.as_mut();
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
                        if is_running && let Some(pool) = self.pool.as_mut() {
                            pool.spare_bufs.push_front(buf);
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    fn trim_udp_pool_tail(&mut self, ctx: &mut RioContext) {
        loop {
            let maybe_slot = {
                let Some(pool) = self.pool.as_mut() else {
                    return;
                };
                if matches!(pool.state, UdpPoolState::Running)
                    && pool.slots.len() <= pool.target_credits
                {
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
        rq: RIO_RQ,
        actor_id: u32,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (desired, state) = {
            let pool = self
                .pool
                .as_mut()
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
            self.trim_udp_pool_tail(ctx);
            return Ok(0);
        }

        let submissions = self.grow_udp_pool_to(desired, rq, actor_id, ctx)?;
        self.trim_udp_pool_tail(ctx);
        Ok(submissions)
    }

    fn ensure_udp_recv_pool(
        &mut self,
        rq: RIO_RQ,
        actor_id: u32,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        if self.pool.is_some() {
            return Ok(0);
        }

        let min = UDP_RECV_POOL_MIN_CREDITS;
        let max = UDP_RECV_POOL_MAX_CREDITS.max(min);
        let initial = UDP_RECV_POOL_INITIAL_CREDITS.clamp(min, max);

        self.pool = Some(UdpRecvPool {
            slots: Vec::with_capacity(max),
            queue: VecDeque::with_capacity(initial),
            waiters: VecDeque::new(),
            spare_bufs: VecDeque::with_capacity(initial),
            min_credits: min,
            max_credits: max,
            target_credits: initial,
            idle_hits: 0,
            state: UdpPoolState::Running,
        });

        self.grow_udp_pool_to(initial, rq, actor_id, ctx)
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

    fn dispatch_udp_waiters(&mut self, ops: &mut OpRegistry<IocpOp, IocpOpState>) {
        loop {
            let (waiter, datagram) = {
                let Some(pool) = self.pool.as_mut() else {
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
                && let Some(pool) = self.pool.as_mut()
                && pool.queue.len() > UDP_RECV_POOL_QUEUE_CAP
            {
                let _ = pool.queue.pop_front();
            }
        }
    }

    pub fn try_submit_udp_recv_stream_pooled(
        &mut self,
        rq: RIO_RQ,
        actor_id: u32,
        stream_op: &mut crate::op::UdpRecvStream,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) -> io::Result<(SubmissionResult, usize)> {
        let (user_data, generation) = uid;
        let mut total_submissions = self.ensure_udp_recv_pool(rq, actor_id, ctx)?;
        {
            let pool = self
                .pool
                .as_mut()
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !matches!(pool.state, UdpPoolState::Running) {
                return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
            }

            if let Some(datagram) = pool.queue.pop_front() {
                stream_op.result = Some(Self::into_op_udp_datagram(datagram));
                total_submissions += self.rebalance_udp_pool(rq, actor_id, ctx)?;
                return Ok((SubmissionResult::PostToQueue, total_submissions));
            }

            pool.waiters.push_back((user_data, generation));
        }
        total_submissions += self.rebalance_udp_pool(rq, actor_id, ctx)?;
        Ok((SubmissionResult::Pending, total_submissions))
    }

    pub fn try_refill_udp_pool(
        &mut self,
        rq: RIO_RQ,
        actor_id: u32,
        buf: FixedBuf,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let mut total_submissions = self.ensure_udp_recv_pool(rq, actor_id, ctx)?;
        let pool = self.pool.as_mut().unwrap();

        pool.spare_bufs.push_back(buf);
        total_submissions += self.rebalance_udp_pool(rq, actor_id, ctx)?;
        Ok(total_submissions)
    }

    pub fn cancel_udp_recv_waiter(
        &mut self,
        uid: (usize, u32),
        rq: RIO_RQ,
        actor_id: u32,
        ctx: &mut RioContext,
    ) {
        let (user_data, generation) = uid;
        if let Some(pool) = self.pool.as_mut() {
            pool.waiters.retain(|&(ud, waiter_generation)| {
                !(ud == user_data && waiter_generation == generation)
            });
        }
        let _ = self.rebalance_udp_pool(rq, actor_id, ctx);
    }

    pub fn ack_udp_pool_completion(&mut self, completion_generation: u32) -> Option<usize> {
        self.udp_ctx_map.remove(&completion_generation)
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
        rq: RIO_RQ,
        actor_id: u32,
        slot_idx: usize,
        res: &RIORESULT,
        ctx: &mut RioContext,
    ) -> usize {
        let Some(pool) = self.pool.as_mut() else {
            return 0;
        };
        let event = Self::transition_pool_on_completion(pool, slot_idx, res);

        let mut submissions = 0;
        let actions = Self::plan_completion_actions(event, slot_idx);

        if let Some(idx) = actions.resubmit_slot {
            let token = self.alloc_udp_ctx_token(idx);
            if let Ok(n) = self.submit_udp_pool_slot((idx, token), rq, actor_id, ctx) {
                submissions += n;
            }
        }
        if actions.dispatch_waiters {
            self.dispatch_udp_waiters(ops);
        }
        if actions.rebalance_pool {
            self.trim_udp_pool_tail(ctx);
            if let Ok(n) = self.rebalance_udp_pool(rq, actor_id, ctx) {
                submissions += n;
            }
        }
        submissions
    }

    pub fn handle_completion_drain_only(&mut self) {
        let _ = self.pool.as_mut().map(|_| PoolCompletionEvent::PoolMissing);
    }

    pub fn begin_udp_pool_shutdown(&mut self) {
        if let Some(pool) = self.pool.as_mut() {
            Self::begin_draining(pool);
        }
    }

    pub fn cleanup_shutdown_udp_pool_if_drained(&mut self, ctx: &mut RioContext) -> bool {
        let drained = self.pool.as_ref().is_some_and(|pool| {
            !matches!(pool.state, UdpPoolState::Running)
                && pool.slots.iter().all(|slot| !slot.in_flight)
        });
        if !drained {
            return false;
        }

        if let Some(pool) = self.pool.as_mut() {
            pool.state = UdpPoolState::Closed;
        }
        if let Some(pool) = self.pool.take() {
            for slot in pool.slots {
                self.deregister_udp_pool_slot_with_registry(slot, ctx);
            }
        }
        self.udp_ctx_map.clear();
        true
    }

    pub fn forget_in_flight_and_deregister_rest(&mut self, ctx: &mut RioContext) {
        if let Some(pool) = self.pool.take() {
            for slot in pool.slots {
                if slot.in_flight {
                    std::mem::forget(slot);
                    continue;
                }
                self.deregister_udp_pool_slot_with_registry(slot, ctx);
            }
        }
        self.udp_ctx_map.clear();
    }

    #[cfg(test)]
    pub fn udp_pool_debug_stats(&self) -> Option<UdpRecvPoolDebugStats> {
        self.pool.as_ref().map(|pool| UdpRecvPoolDebugStats {
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
