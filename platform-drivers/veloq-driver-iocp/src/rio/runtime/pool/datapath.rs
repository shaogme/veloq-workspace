use super::{
    POOL_CTX_TAG, UDP_RECV_POOL_QUEUE_CAP, UdpPoolState, UdpRecvDatagram, UdpRecvPool,
    UdpRecvPoolSlot,
};
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::net::addr::{SockAddrStorage, to_socket_addr};
use crate::ops::IocpOpPayload;
use crate::ops::slot::{InFlight, Slot};
use crate::ops::submit::SubmissionResult;
use crate::rio::core::submit_ops::{RioDispatch, RioExConfig, RioProvider};
use crate::rio::{RioCompletionContext, RioContext};
use rustc_hash::FxHashMap;
use std::collections::{VecDeque, hash_map};
use std::io;
use veloq_buf::FixedBuf;
use veloq_driver_core::driver::{CompletionEvent, encode_completion_token};
use veloq_driver_core::op::UdpRecvDatagram as OpUdpRecvDatagram;

use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::Networking::WinSock::{RIO_BUF, RIORESULT, WSAGetLastError};

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
        let actor_id = ctx.actor_id;
        let rq = ctx.rq;
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
            BufferId: data_buf_id.0,
            Offset: offset,
            Length: slot.buf.capacity() as u32,
        };
        let addr_buf = RIO_BUF {
            BufferId: slot.addr_buf_id.0,
            Offset: 0,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };
        let req_ctx = Self::encode_pool_context(actor_id, completion_token);

        if let Err(e) = ctx.env.dispatch.receive_ex(RioExConfig {
            rq,
            data_buf: &data_buf,
            data_buf_count: 1,
            local_addr: std::ptr::null(),
            remote_addr: &addr_buf,
            control_buf: std::ptr::null(),
            flags_buf: std::ptr::null(),
            flags: 0,
            context: req_ctx,
        }) {
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
                    "RIOReceiveEx submit failed for UDP recv pool: slot_idx={}, rq=0x{:x}, error={e}",
                    slot_idx, rq.0 as usize
                ),
            ));
        }
        Ok(1)
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
            let idx = self
                .pool
                .as_mut()
                .map(|p| {
                    p.slots.push(slot);
                    p.slots.len() - 1
                })
                .expect("pool existence verified by should_grow");

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
        if self.pool.is_some() {
            return Ok(0);
        }

        use super::{
            UDP_RECV_POOL_INITIAL_CREDITS, UDP_RECV_POOL_MAX_CREDITS, UDP_RECV_POOL_MIN_CREDITS,
        };
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
        let (user_data, expected_generation) = uid;
        let ops = &mut comp.ops;
        if user_data >= ops.local.len() {
            return false;
        }
        let (slot, op, storage) = match ops.get_slot_entry_storage_and_entry_mut(user_data) {
            Some(v) => v,
            None => return false,
        };
        if op.platform_data.generation != expected_generation {
            return false;
        }
        if !Slot::<InFlight>::is_in_flight_entry(slot) {
            return false;
        }

        let mut guard = Slot::<InFlight>::as_inflight_entry(slot, storage, user_data);
        let d = datagram
            .as_ref()
            .expect("datagram must exist when deliver_to_waiter is called");
        let d_len = d.buf.len();

        // SAFETY: op_mut_unchecked is safe because we've verified the slot is InFlight
        // and its generation matches.
        let stream_op = unsafe {
            guard
                .op_mut_unchecked(|iocp_op| {
                    if let IocpOpPayload::UdpRecvStream(ref mut kernel) = iocp_op.payload {
                        Some(kernel.user.as_mut())
                    } else {
                        None
                    }
                })
                .flatten()
        };

        if let Some(stream_op) = stream_op {
            let owned_d = datagram.take().expect("datagram must be taken only once");
            // SAFETY: addr and addr_len are provided by RIO completion and are guaranteed
            // to be valid SockAddrStorage data.
            let addr = unsafe {
                let s = std::slice::from_raw_parts(
                    &owned_d.addr as *const _ as *const u8,
                    owned_d.addr_len as usize,
                );
                to_socket_addr(s).unwrap_or_else(|_| "0.0.0.0:0".parse().expect("static parse"))
            };

            stream_op.result = Some(OpUdpRecvDatagram {
                buf: owned_d.buf,
                addr,
            });

            op.platform_data.rio_pool_waiting = false;
            let event = CompletionEvent {
                user_data: encode_completion_token(user_data, expected_generation),
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

    fn into_op_datagram(datagram: UdpRecvDatagram) -> OpUdpRecvDatagram {
        // SAFETY: addr and addr_len are provided by RIO completion.
        let addr = unsafe {
            let s = std::slice::from_raw_parts(
                &datagram.addr as *const _ as *const u8,
                datagram.addr_len as usize,
            );
            to_socket_addr(s).unwrap_or_else(|_| "0.0.0.0:0".parse().expect("static parse"))
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

    pub(crate) fn try_submit_pooled_recv(
        &mut self,
        stream_op: &mut veloq_driver_core::op::UdpRecvStream<crate::RawHandle>,
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
        let pool = self
            .pool
            .as_mut()
            .expect("ensure_pool guarantees existence");

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
        let actions = Self::plan_actions(event, slot_idx);

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

    pub(crate) fn handle_drain_completion(&mut self) {
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
