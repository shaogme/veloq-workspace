use super::{
    POOL_CTX_TAG, PoolCompletionEvent, UDP_RECV_POOL_QUEUE_CAP, UdpPoolState, UdpRecvDatagram,
    UdpRecvPool, UdpRecvPoolSlot,
};
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::net::addr::{SockAddrStorage, to_socket_addr};
use crate::ops::IocpOpPayload;
use crate::ops::slot::{InFlight, Slot};
use crate::ops::submit::SubmissionResult;
use crate::rio::core::submit_ops::{RioDispatch, RioExConfig, RioProvider, RioRq};
use crate::rio::{RioCompletionContext, RioContext};
use rustc_hash::FxHashMap;
use std::collections::{VecDeque, hash_map};
use std::io;
use veloq_buf::FixedBuf;
use veloq_driver_core::driver::{CompletionEvent, encode_completion_token};
use veloq_driver_core::op::{UdpRecvDatagram as OpUdpRecvDatagram, UdpRecvStream};

use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::Networking::WinSock::{RIO_BUF, RIORESULT, WSAGetLastError};

pub(crate) struct UdpPoolManager {
    pub(crate) pool: Option<UdpRecvPool>,
    pub(crate) udp_ctx_map: FxHashMap<u32, usize>,
    pub(crate) udp_next_ctx: u32,
}

impl UdpPoolManager {
    pub(crate) fn new() -> Self {
        Self {
            pool: None,
            udp_ctx_map: FxHashMap::default(),
            udp_next_ctx: 1,
        }
    }

    fn last_wsa_error() -> io::Error {
        // SAFETY: WSAGetLastError is a thread-safe getter with no side effects.
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    #[inline]
    pub(crate) fn encode_pool_context(actor_id: u32, token: u32) -> *const std::ffi::c_void {
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
            if let hash_map::Entry::Vacant(e) = self.udp_ctx_map.entry(token) {
                e.insert(slot_idx);
                return token;
            }
        }
    }

    fn create_pool_slot(
        &self,
        buf: FixedBuf,
        dispatch: &RioDispatch,
    ) -> io::Result<UdpRecvPoolSlot> {
        let mut addr = Box::new(SockAddrStorage::default());

        let addr_buf_id = dispatch
            .register_buffer(
                (&mut *addr as *mut SockAddrStorage).cast::<u8>(),
                std::mem::size_of::<SockAddrStorage>() as u32,
            )
            .map_err(|e| {
                io_error(
                    IocpErrorContext::Rio,
                    Self::last_wsa_error(),
                    format!("RIORegisterBuffer failed for UDP recv pool addr buffer: {e}"),
                )
            })?;

        Ok(UdpRecvPoolSlot {
            buf,
            addr,
            addr_buf_id,
            in_flight: false,
            stop_requested: false,
        })
    }

    #[inline]
    pub(super) fn begin_draining(pool: &mut UdpRecvPool) {
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

    fn deregister_slot(&self, slot: UdpRecvPoolSlot, dispatch: &RioDispatch) {
        if !slot.addr_buf_id.is_invalid() {
            dispatch.deregister_buffer(slot.addr_buf_id);
        }
    }

    fn free_slot(&self, slot: UdpRecvPoolSlot, ctx: &mut RioContext) {
        ctx.registry.deregister_heap_buf(&slot.buf, ctx.env);
        self.deregister_slot(slot, ctx.env.dispatch);
    }

    fn submit_pool_slot(
        &mut self,
        target: (usize, u32),
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let (slot_idx, completion_token) = target;
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

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
            BufferId: data_buf_id.0,
            Offset: offset,
            Length: slot.buf.capacity() as u32,
        };
        let addr_buf = RIO_BUF {
            BufferId: slot.addr_buf_id.0,
            Offset: 0,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };

        if let Err(e) = ctx.env.dispatch.receive_ex(RioExConfig {
            rq: ctx.rq,
            data_buf: &data_buf,
            data_buf_count: 1,
            local_addr: std::ptr::null(),
            remote_addr: &addr_buf,
            control_buf: std::ptr::null(),
            flags_buf: std::ptr::null(),
            flags: 0,
            context: Self::encode_pool_context(ctx.actor_id, completion_token),
        }) {
            self.on_recv_submit_fail(slot_idx, completion_token, e, ctx.rq)
        } else {
            Ok(1)
        }
    }

    fn on_recv_submit_fail(
        &mut self,
        slot_idx: usize,
        token: u32,
        e: io::Error,
        rq: RioRq,
    ) -> io::Result<usize> {
        self.udp_ctx_map.remove(&token);
        if let Some(pool) = self.pool.as_mut()
            && let Some(slot) = pool.slots.get_mut(slot_idx)
        {
            slot.in_flight = false;
            slot.stop_requested = false;
        }
        Err(io_error(
            IocpErrorContext::Rio,
            Self::last_wsa_error(),
            format!(
                "RIOReceiveEx submit failed: slot={}, rq=0x{:x}, error={e}",
                slot_idx, rq.0 as usize
            ),
        ))
    }

    fn grow_pool_to(&mut self, target: usize, ctx: &mut RioContext) -> io::Result<usize> {
        let mut submissions = 0;
        while self.should_grow(target) {
            let slot_buf = match self.pool.as_mut() {
                Some(p) => match p.spare_bufs.pop_front() {
                    Some(b) => b,
                    None => break,
                },
                None => break,
            };

            let slot = self.create_pool_slot(slot_buf, ctx.env.dispatch)?;
            let idx = if let Some(p) = self.pool.as_mut() {
                p.slots.push(slot);
                p.slots.len() - 1
            } else {
                return Ok(submissions);
            };

            let token = self.alloc_udp_ctx_token(idx);
            if let Err(e) = self.submit_pool_slot((idx, token), ctx) {
                self.undo_failed_growth(idx, ctx);
                return Err(e);
            }
            submissions += 1;
        }
        Ok(submissions)
    }

    fn should_grow(&self, target: usize) -> bool {
        if let Some(pool) = &self.pool {
            pool.state == UdpPoolState::Running && pool.slots.len() < target
        } else {
            false
        }
    }

    fn undo_failed_growth(&mut self, idx: usize, ctx: &mut RioContext) {
        let (popped_slot, is_running) = {
            let mut pool = self.pool.as_mut();
            (
                pool.as_mut().and_then(|p| {
                    if p.slots.len() > idx {
                        Some(p.slots.remove(idx))
                    } else {
                        None
                    }
                }),
                pool.as_ref()
                    .is_some_and(|p| matches!(p.state, UdpPoolState::Running)),
            )
        };

        if let Some(s) = popped_slot {
            let buf = s.buf;
            if !s.addr_buf_id.is_invalid() {
                ctx.env.dispatch.deregister_buffer(s.addr_buf_id);
            }
            if is_running && let Some(pool) = self.pool.as_mut() {
                pool.spare_bufs.push_front(buf);
            }
        }
    }

    fn trim_pool_tail(&mut self, ctx: &mut RioContext) {
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
                self.free_slot(slot, ctx);
            } else {
                return;
            }
        }
    }

    pub(crate) fn rebalance_udp_pool(&mut self, ctx: &mut RioContext) -> io::Result<usize> {
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
            self.trim_pool_tail(ctx);
            return Ok(0);
        }

        let submissions = self.grow_pool_to(desired, ctx)?;
        self.trim_pool_tail(ctx);
        Ok(submissions)
    }

    fn ensure_pool(&mut self, ctx: &mut RioContext) -> io::Result<usize> {
        use super::{
            UDP_RECV_POOL_INITIAL_CREDITS, UDP_RECV_POOL_MAX_CREDITS, UDP_RECV_POOL_MIN_CREDITS,
        };

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

        self.grow_pool_to(initial, ctx)
    }

    fn deliver_to_waiter(
        comp: &mut RioCompletionContext<'_>,
        uid: (usize, u32),
        datagram: &mut Option<UdpRecvDatagram>,
    ) -> bool {
        let (user_data, generation) = uid;
        let ops = &mut comp.ops;
        if user_data >= ops.local.len() {
            return false;
        }
        let (slot, op, slot_op, storage) =
            match ops.get_slot_entry_op_storage_and_entry_mut(user_data) {
                Some(v) => v,
                None => return false,
            };
        if op.platform_data.generation != generation || !Slot::<InFlight>::is_in_flight_entry(slot)
        {
            return false;
        }

        let mut guard = Slot::<InFlight>::as_inflight_entry(slot, slot_op, storage, user_data);
        let d = match datagram.as_ref() {
            Some(d) => d,
            None => return false,
        };
        let d_len = d.buf.len();

        let stream_op = Self::get_stream_op_mut(&mut guard);
        if let Some(stream_op) = stream_op {
            let Some(owned_d) = datagram.take() else {
                return false;
            };
            stream_op.result = Some(Self::into_op_datagram(owned_d));
            op.platform_data.rio_pool_waiting = false;

            let event = CompletionEvent {
                user_data: encode_completion_token(user_data, generation),
                res: d_len.min(i32::MAX as usize) as i32,
                flags: 0,
            };

            let mut guard = guard.complete();
            let (payload, detail) = guard.take_completion_data();
            comp.table
                .record_completion_with_data(event, payload, detail);
            comp.events.push(event);
            let _ = guard.take_op();
            let _ = std::mem::take(&mut op.platform_data);
            comp.ops.shared.push_free(user_data);
            true
        } else {
            false
        }
    }

    fn get_stream_op_mut<'a>(
        guard: &'a mut Slot<'_, InFlight>,
    ) -> Option<&'a mut UdpRecvStream<crate::RawHandle>> {
        guard
            .with_op_mut(|iocp_op| {
                if let IocpOpPayload::UdpRecvStream(ref mut kernel) = iocp_op.payload {
                    // SAFETY: `kernel.user` is a live `NonNull` while the op is in-flight.
                    Some(unsafe { kernel.user.as_mut() })
                } else {
                    None
                }
            })
            .flatten()
    }

    fn into_op_datagram(datagram: UdpRecvDatagram) -> OpUdpRecvDatagram {
        // SAFETY: addr and addr_len are provided by RIO completion.
        let addr = unsafe {
            let s = std::slice::from_raw_parts(
                &datagram.addr as *const _ as *const u8,
                datagram.addr_len as usize,
            );
            to_socket_addr(s).map_err(|_| ()).unwrap_or_else(|_| {
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::UNSPECIFIED,
                    0,
                ))
            })
        };

        OpUdpRecvDatagram {
            buf: datagram.buf,
            addr,
        }
    }

    pub(super) fn dispatch_waiters(&mut self, comp: &mut RioCompletionContext<'_>) {
        loop {
            let (waiter, mut datagram) = {
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
                (waiter, Some(datagram))
            };

            if !Self::deliver_to_waiter(comp, waiter, &mut datagram)
                && let Some(pool) = self.pool.as_mut()
                && let Some(returned_datagram) = datagram
            {
                pool.queue.push_front(returned_datagram);
                if pool.queue.len() > UDP_RECV_POOL_QUEUE_CAP {
                    let _ = pool.queue.pop_back();
                }
            }
        }
    }

    pub(crate) fn try_submit_pool_recv(
        &mut self,
        stream_op: &mut UdpRecvStream<crate::RawHandle>,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) -> io::Result<(SubmissionResult, usize)> {
        let mut total_submissions = self.ensure_pool(ctx)?;
        {
            let pool = self
                .pool
                .as_mut()
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !matches!(pool.state, UdpPoolState::Running) {
                return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
            }

            if let Some(datagram) = pool.queue.pop_front() {
                stream_op.result = Some(Self::into_op_datagram(datagram));
                total_submissions += self.rebalance_udp_pool(ctx)?;
                return Ok((SubmissionResult::PostToQueue, total_submissions));
            }

            pool.waiters.push_back(uid);
        }
        total_submissions += self.rebalance_udp_pool(ctx)?;
        Ok((SubmissionResult::Pending, total_submissions))
    }

    pub(crate) fn try_refill_pool(
        &mut self,
        buf: FixedBuf,
        ctx: &mut RioContext,
    ) -> io::Result<usize> {
        let mut total_submissions = self.ensure_pool(ctx)?;
        let Some(pool) = self.pool.as_mut() else {
            return Ok(total_submissions);
        };

        pool.spare_bufs.push_back(buf);
        total_submissions += self.rebalance_udp_pool(ctx)?;
        Ok(total_submissions)
    }

    pub(crate) fn cancel_waiter(&mut self, uid: (usize, u32), ctx: &mut RioContext) {
        let (user_data, generation) = uid;
        if let Some(pool) = self.pool.as_mut() {
            pool.waiters.retain(|&(ud, waiter_generation)| {
                !(ud == user_data && waiter_generation == generation)
            });
        }
        let _ = self.rebalance_udp_pool(ctx);
    }

    pub(crate) fn ack_pool_done(&mut self, completion_generation: u32) -> Option<usize> {
        self.udp_ctx_map.remove(&completion_generation)
    }

    pub(super) fn update_pool_state(
        pool: &mut UdpRecvPool,
        slot_idx: usize,
        res: &RIORESULT,
    ) -> PoolCompletionEvent {
        pool.update_state(slot_idx, res)
    }

    pub(crate) fn handle_completion(
        &mut self,
        completion: (usize, &RIORESULT),
        comp: &mut RioCompletionContext<'_>,
        ctx: &mut RioContext,
    ) -> usize {
        let (slot_idx, res) = completion;
        let Some(pool) = self.pool.as_mut() else {
            return 0;
        };
        let event = Self::update_pool_state(pool, slot_idx, res);

        let mut submissions = 0;
        let actions = UdpRecvPool::plan_actions(event, slot_idx);

        if let Some(idx) = actions.resubmit_slot {
            let token = self.alloc_udp_ctx_token(idx);
            if let Ok(n) = self.submit_pool_slot((idx, token), ctx) {
                submissions += n;
            }
        }
        if actions.dispatch_waiters {
            self.dispatch_waiters(comp);
        }
        if actions.rebalance_pool {
            self.trim_pool_tail(ctx);
            if let Ok(n) = self.rebalance_udp_pool(ctx) {
                submissions += n;
            }
        }
        submissions
    }

    pub(crate) fn handle_drain_comp(&mut self) {
        // Pure side effect to satisfy RIO requirements
    }

    pub(crate) fn shutdown_pool(&mut self) {
        if let Some(pool) = self.pool.as_mut() {
            Self::begin_draining(pool);
        }
    }

    pub(crate) fn cleanup_drained_pool(&mut self, ctx: &mut RioContext) -> bool {
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
                self.free_slot(slot, ctx);
            }
        }
        self.udp_ctx_map.clear();
        true
    }

    pub(crate) fn forget_and_cleanup(&mut self, ctx: &mut RioContext) {
        if let Some(pool) = self.pool.take() {
            for slot in pool.slots {
                if slot.in_flight {
                    std::mem::forget(slot);
                    continue;
                }
                self.free_slot(slot, ctx);
            }
        }
        self.udp_ctx_map.clear();
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(&self) -> Option<super::UdpRecvPoolDebugStats> {
        self.pool.as_ref().map(|pool| super::UdpRecvPoolDebugStats {
            min_credits: pool.min_credits,
            max_credits: pool.max_credits,
            target_credits: pool.target_credits,
            waiters_len: pool.waiters.len(),
        })
    }
}
