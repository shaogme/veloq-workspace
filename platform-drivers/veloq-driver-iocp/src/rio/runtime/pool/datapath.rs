use super::{
    SlotKey, UDP_RECV_POOL_CHUNK_SIZE, UDP_RECV_POOL_INITIAL_CREDITS, UDP_RECV_POOL_MAX_CREDITS,
    UDP_RECV_POOL_MIN_CREDITS, UDP_RECV_POOL_QUEUE_CAP, UDP_RECV_POOL_SLAB_CHUNKS, UdpBufferSlab,
    UdpMailbox, UdpPoolPacket, UdpPoolState, UdpRecvPool, UdpRecvPoolSlot, UdpWaiter,
    UdpWaiterKind,
};
use crate::net::addr::{SockAddrStorage, to_socket_addr};
use crate::ops::IocpOpPayload;
use crate::ops::slot::Slot;
use crate::ops::submit::SubmissionResult;
use crate::rio::core::submit_ops::{RioDispatch, RioExConfig, RioProvider, RioRq};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioCompletionContext, RioContext, RioState};
use error_stack::ResultExt;
use rustc_hash::FxHashMap;
use slotmap::SlotMap;
use std::collections::{VecDeque, hash_map};
use veloq_buf::FixedBuf;
use veloq_driver_core::driver::{CompletionEvent, encode_completion_token};
use veloq_driver_core::op::{
    UdpRecv as OpUdpRecv, UdpRecvPacket as OpUdpRecvPacket, UdpRecvStream,
};
use veloq_driver_core::slot::{InFlightWaiting, SlotRegistryExt, SlotView};

use windows_sys::Win32::Networking::WinSock::{RIO_BUF, RIORESULT};

enum FastDeliverPayload {
    Recv { idx: u32, len: usize },
    Stream { idx: u32, len: usize },
}

pub(crate) struct TokenRegistry {
    pub(crate) map: FxHashMap<u32, SlotKey>,
    pub(crate) next_ctx: u32,
}

impl TokenRegistry {
    pub(crate) fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            next_ctx: 1,
        }
    }

    pub(crate) fn alloc(&mut self, slot_key: SlotKey) -> u32 {
        loop {
            let token = self.next_ctx;
            self.next_ctx = self.next_ctx.wrapping_add(1);
            if token == 0 {
                continue;
            }
            if let hash_map::Entry::Vacant(e) = self.map.entry(token) {
                e.insert(slot_key);
                return token;
            }
        }
    }

    pub(crate) fn reclaim(&mut self, token: u32) {
        self.map.remove(&token);
    }
}

pub(crate) struct UdpPoolManager {
    pub(crate) pool: UdpRecvPool,
    pub(crate) registry: TokenRegistry,
}

impl UdpPoolManager {
    pub(crate) fn new() -> Self {
        Self {
            pool: UdpRecvPool::uninit(),
            registry: TokenRegistry::new(),
        }
    }

    #[inline]
    pub(crate) fn encode_pool_context(
        actor_key: crate::rio::ActorKey,
        token: u32,
    ) -> *const std::ffi::c_void {
        RioState::encode_pool_req_ctx(actor_key, token)
    }

    pub(crate) fn ensure_pool(
        &mut self,
        ctx: &mut RioContext,
        requested_chunk_size: usize,
    ) -> RioResult<usize> {
        if self.pool.state != UdpPoolState::Uninitialized {
            return Ok(0);
        }

        let min = UDP_RECV_POOL_MIN_CREDITS;
        let max = UDP_RECV_POOL_MAX_CREDITS.max(min);
        let initial = UDP_RECV_POOL_INITIAL_CREDITS.clamp(min, max);

        let chunk_size = requested_chunk_size.max(UDP_RECV_POOL_CHUNK_SIZE).max(1);
        self.pool.slots = SlotMap::with_capacity_and_key(max);
        self.pool.slab = Some(Self::init_slab(
            ctx,
            chunk_size,
            UDP_RECV_POOL_SLAB_CHUNKS,
        )?);
        self.pool.min_credits = min;
        self.pool.max_credits = max;
        self.pool.target_credits = initial;
        self.pool.idle_hits = 0;
        self.pool.state = UdpPoolState::Running;

        self.pool.grow_to(initial, ctx, &mut self.registry)
    }

    fn init_slab(
        ctx: &mut RioContext,
        chunk_size: usize,
        chunk_count: usize,
    ) -> RioResult<UdpBufferSlab> {
        let total = chunk_size
            .checked_mul(chunk_count)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("UDP slab size overflow")?;
        let total_nz = std::num::NonZeroUsize::new(total)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("UDP slab total size must be > 0")?;
        let backing = FixedBuf::alloc_heap(total_nz)
            .map_err(|e| error_stack::Report::new(RioError::Internal).attach(e.to_string()))?;

        let rio_id = ctx
            .env
            .dispatch
            .register_buffer(backing.as_ptr(), backing.capacity() as u32)
            .attach("RIORegisterBuffer failed for UDP slab backing")?;

        let mut chunks = Vec::with_capacity(chunk_count);
        for idx in 0..chunk_count {
            let start = idx * chunk_size;
            let end = start + chunk_size;
            chunks.push(backing.slice(start..end));
        }

        let mut free_indices = VecDeque::with_capacity(chunk_count);
        for idx in 0..chunk_count {
            free_indices.push_back(idx as u32);
        }

        Ok(UdpBufferSlab {
            _backing: backing,
            rio_id,
            chunk_size,
            chunks,
            free_indices,
        })
    }

    pub(crate) fn try_submit_pool_recv(
        &mut self,
        mailbox: &mut UdpMailbox,
        stream_op: &mut UdpRecvStream<crate::RawHandle>,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) -> RioResult<(SubmissionResult, usize)> {
        let requested_chunk_size = stream_op
            .buf
            .as_ref()
            .map(|buf| buf.len().max(buf.capacity()))
            .unwrap_or(UDP_RECV_POOL_CHUNK_SIZE);
        let total_submissions = self.ensure_pool(ctx, requested_chunk_size)?;
        let (res, subs) =
            self.pool
                .try_submit_recv(mailbox, stream_op, uid, ctx, &mut self.registry)?;
        Ok((res, total_submissions + subs))
    }

    pub(crate) fn try_submit_pool_recv_recv(
        &mut self,
        mailbox: &mut UdpMailbox,
        recv_op: &mut OpUdpRecv<crate::RawHandle>,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) -> RioResult<(SubmissionResult, usize, Option<usize>)> {
        let requested_chunk_size = recv_op
            .buf
            .len()
            .saturating_sub(recv_op.buf_offset)
            .max(1);
        let total_submissions = self.ensure_pool(ctx, requested_chunk_size)?;
        let (res, subs, copied) =
            self.pool
                .try_submit_recv_recv(mailbox, recv_op, uid, ctx, &mut self.registry)?;
        Ok((res, total_submissions + subs, copied))
    }

    pub(crate) fn cancel_waiter(
        &mut self,
        mailbox: &mut UdpMailbox,
        uid: (usize, u32),
        ctx: &mut RioContext,
    ) {
        self.pool
            .cancel_waiter(mailbox, uid, ctx, &mut self.registry);
    }

    pub(crate) fn ack_pool_done(&mut self, completion_generation: u32) -> Option<SlotKey> {
        self.registry.map.remove(&completion_generation)
    }

    pub(crate) fn handle_completion(
        &mut self,
        mailbox: &mut UdpMailbox,
        completion: (SlotKey, &RIORESULT),
        comp: &mut RioCompletionContext<'_>,
        ctx: &mut RioContext,
    ) -> RioResult<usize> {
        self.pool
            .handle_completion(mailbox, completion, comp, ctx, &mut self.registry)
    }

    #[cfg(test)]
    pub(crate) fn rebalance_udp_pool(
        &mut self,
        mailbox: &UdpMailbox,
        ctx: &mut RioContext,
    ) -> RioResult<usize> {
        self.pool.rebalance(mailbox, ctx, &mut self.registry)
    }

    pub(crate) fn shutdown_pool(&mut self, _mailbox: &UdpMailbox) {
        self.pool.begin_draining();
    }

    pub(crate) fn cleanup_drained_pool(&mut self, ctx: &mut RioContext) -> bool {
        self.pool.cleanup_drained(ctx)
    }

    pub(crate) fn forget_and_cleanup(
        &mut self,
        mailbox: &UdpMailbox,
        ctx: &mut RioContext,
    ) -> bool {
        self.registry.map.clear();
        self.shutdown_pool(mailbox);
        self.cleanup_drained_pool(ctx)
    }

    pub(crate) fn handle_drain_comp(&mut self) {
        self.pool.handle_drain_comp();
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(
        &self,
        mailbox: &UdpMailbox,
    ) -> Option<super::UdpRecvPoolDebugStats> {
        if self.pool.state == UdpPoolState::Uninitialized {
            return None;
        }
        Some(super::UdpRecvPoolDebugStats {
            min_credits: self.pool.min_credits,
            max_credits: self.pool.max_credits,
            target_credits: self.pool.target_credits,
            waiters_len: mailbox.waiters.len(),
        })
    }
}

impl UdpRecvPool {
    fn create_slot(&self, current_idx: u32, dispatch: &RioDispatch) -> RioResult<UdpRecvPoolSlot> {
        let mut addr = Box::new(SockAddrStorage::default());

        let addr_buf_id = dispatch
            .register_buffer(
                (&mut *addr as *mut SockAddrStorage).cast::<u8>(),
                std::mem::size_of::<SockAddrStorage>() as u32,
            )
            .attach("RIORegisterBuffer failed for UDP recv pool addr buffer")?;

        Ok(UdpRecvPoolSlot {
            current_idx,
            addr,
            addr_buf_id,
            in_flight: false,
            stop_requested: false,
        })
    }

    pub(crate) fn begin_draining(&mut self) {
        if matches!(self.state, UdpPoolState::Running) {
            self.state = UdpPoolState::Draining;
        }
        self.target_credits = 0;
        if let Some(slab) = self.slab.as_mut() {
            slab.free_indices.clear();
        }
        for (_, slot) in &mut self.slots {
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
        self.deregister_slot(slot, ctx.env.dispatch);
    }

    fn submit_slot(
        &mut self,
        target: (SlotKey, u32),
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<usize> {
        let (slot_key, completion_token) = target;

        if !matches!(self.state, UdpPoolState::Running) {
            return Err(error_stack::Report::new(RioError::Internal))
                .attach("RIO pool not running during submit_slot");
        }

        let slot = self
            .slots
            .get_mut(slot_key)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("UDP recv pool slot missing")?;
        let slab = self
            .slab
            .as_ref()
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("UDP slab missing during submit_slot")?;

        slot.in_flight = true;
        slot.stop_requested = false;

        let data_buf = RIO_BUF {
            BufferId: slab.rio_id.0,
            Offset: slab.chunk_offset(slot.current_idx),
            Length: slab.chunk_capacity() as u32,
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
            context: UdpPoolManager::encode_pool_context(ctx.actor_key, completion_token),
        }) {
            self.on_submit_fail(slot_key, completion_token, e, ctx.rq, registry)
        } else {
            Ok(1)
        }
    }

    fn on_submit_fail(
        &mut self,
        slot_key: SlotKey,
        token: u32,
        e: error_stack::Report<RioError>,
        rq: RioRq,
        registry: &mut TokenRegistry,
    ) -> RioResult<usize> {
        registry.reclaim(token);
        if let Some(slot) = self.slots.get_mut(slot_key) {
            slot.in_flight = false;
            slot.stop_requested = false;
        }
        Err(e).attach(format!(
            "RIOReceiveEx submit failed: key={:?}, rq=0x{:x}",
            slot_key, rq.0 as usize
        ))
    }

    pub(crate) fn grow_to(
        &mut self,
        target: usize,
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<usize> {
        let mut submissions = 0;
        while self.state == UdpPoolState::Running && self.slots.len() < target {
            let idx = match self.slab.as_mut().and_then(|s| s.free_indices.pop_front()) {
                Some(i) => i,
                None => break,
            };

            let slot = match self.create_slot(idx, ctx.env.dispatch) {
                Ok(s) => s,
                Err(e) => {
                    if let Some(slab) = self.slab.as_mut() {
                        slab.free_indices.push_front(idx);
                    }
                    return Err(e);
                }
            };
            let key = self.slots.insert(slot);
            let token = registry.alloc(key);
            if let Err(e) = self.submit_slot((key, token), ctx, registry) {
                self.undo_failed_growth(key, ctx);
                return Err(e);
            }
            submissions += 1;
        }
        Ok(submissions)
    }

    fn undo_failed_growth(&mut self, key: SlotKey, ctx: &mut RioContext) {
        let popped_slot = self.slots.remove(key);

        if let Some(s) = popped_slot {
            if !s.addr_buf_id.is_invalid() {
                ctx.env.dispatch.deregister_buffer(s.addr_buf_id);
            }
            if self.state == UdpPoolState::Running
                && let Some(slab) = self.slab.as_mut()
            {
                slab.free_indices.push_front(s.current_idx);
            }
        }
    }

    fn trim_tail(&mut self, ctx: &mut RioContext) {
        loop {
            if matches!(self.state, UdpPoolState::Running)
                && self.slots.len() <= self.target_credits
            {
                return;
            }

            let target_key = self
                .slots
                .iter()
                .find(|(_, slot)| !slot.in_flight)
                .map(|(key, _)| key);

            if let Some(key) = target_key {
                if let Some(slot) = self.slots.remove(key) {
                    self.free_slot(slot, ctx);
                }
            } else {
                return;
            }
        }
    }

    pub(crate) fn rebalance(
        &mut self,
        mailbox: &UdpMailbox,
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<usize> {
        let (desired, is_running) = if !matches!(self.state, UdpPoolState::Running) {
            (0, false)
        } else {
            if !mailbox.waiters.is_empty() {
                let bump = mailbox.waiters.len().clamp(1, 4);
                self.target_credits = (self.target_credits + bump).min(self.max_credits);
                self.idle_hits = 0;
            } else if mailbox.queue.is_empty() {
                self.idle_hits = self.idle_hits.saturating_add(1);
                if self.idle_hits >= 64 && self.target_credits > self.min_credits {
                    self.target_credits -= 1;
                    self.idle_hits = 0;
                }
            } else {
                self.idle_hits = 0;
            }
            (self.target_credits, true)
        };

        if !is_running {
            self.trim_tail(ctx);
            return Ok(0);
        }

        let submissions = self.grow_to(desired, ctx, registry)?;
        self.trim_tail(ctx);
        Ok(submissions)
    }

    fn maybe_prime_credit(
        &mut self,
        mailbox: &UdpMailbox,
        _preferred_capacity: usize,
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<()> {
        if !matches!(self.state, UdpPoolState::Running) {
            return Ok(());
        }
        if mailbox.waiters.is_empty() || !mailbox.queue.is_empty() {
            return Ok(());
        }
        if !self.slots.is_empty() {
            return Ok(());
        }
        let _ = self.rebalance(mailbox, ctx, registry)?;
        Ok(())
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        mailbox: &mut UdpMailbox,
        stream_op: &mut UdpRecvStream<crate::RawHandle>,
        uid: (usize, u32),
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<(SubmissionResult, usize)> {
        match self.state {
            UdpPoolState::Running => {}
            UdpPoolState::Uninitialized => {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("UDP recv pool uninitialized");
            }
            _ => {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("RIO pool operation aborted during try_submit_recv");
            }
        }

        let mut total_submissions = 0;

        if let Some(datagram) = mailbox.queue.pop_front() {
            stream_op.result = Some(UdpPoolManager::into_op_datagram(datagram));
            total_submissions += self.rebalance(mailbox, ctx, registry)?;
            return Ok((SubmissionResult::PostToQueue, total_submissions));
        }

        mailbox.waiters.push_back(UdpWaiter {
            user_data: uid.0,
            generation: uid.1,
            kind: UdpWaiterKind::Stream,
        });

        let preferred_capacity = stream_op
            .buf
            .as_ref()
            .map(FixedBuf::capacity)
            .unwrap_or(2048);
        self.maybe_prime_credit(mailbox, preferred_capacity, ctx, registry)?;
        total_submissions += self.rebalance(mailbox, ctx, registry)?;
        Ok((SubmissionResult::Pending, total_submissions))
    }

    pub(crate) fn try_submit_recv_recv(
        &mut self,
        mailbox: &mut UdpMailbox,
        recv_op: &mut OpUdpRecv<crate::RawHandle>,
        uid: (usize, u32),
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<(SubmissionResult, usize, Option<usize>)> {
        match self.state {
            UdpPoolState::Running => {}
            UdpPoolState::Uninitialized => {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("UDP recv pool uninitialized");
            }
            _ => {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("RIO pool operation aborted during try_submit_recv_recv");
            }
        }

        let mut total_submissions = 0;

        if let Some(datagram) = mailbox.queue.pop_front() {
            let copied = UdpPoolManager::copy_data_to_recv_op(recv_op, datagram.buf.as_slice());
            total_submissions += self.rebalance(mailbox, ctx, registry)?;
            return Ok((
                SubmissionResult::PostToQueue,
                total_submissions,
                Some(copied),
            ));
        }

        mailbox.waiters.push_back(UdpWaiter {
            user_data: uid.0,
            generation: uid.1,
            kind: UdpWaiterKind::Recv,
        });

        self.maybe_prime_credit(mailbox, recv_op.buf.capacity(), ctx, registry)?;
        total_submissions += self.rebalance(mailbox, ctx, registry)?;
        Ok((SubmissionResult::Pending, total_submissions, None))
    }

    pub(crate) fn cancel_waiter(
        &mut self,
        mailbox: &mut UdpMailbox,
        uid: (usize, u32),
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) {
        let (user_data, generation) = uid;
        mailbox
            .waiters
            .retain(|w| !(w.user_data == user_data && w.generation == generation));
        let _ = self.rebalance(mailbox, ctx, registry);
    }

    pub(crate) fn handle_completion(
        &mut self,
        mailbox: &mut UdpMailbox,
        completion: (SlotKey, &RIORESULT),
        comp: &mut RioCompletionContext<'_>,
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<usize> {
        let (slot_key, res) = completion;

        if Self::is_datagram_completion(res)
            && let Some(waiter) = mailbox.waiters.pop_front()
            && let Some(n) =
                self.try_fast_deliver(mailbox, waiter, (slot_key, res), comp, ctx, registry)?
        {
            return Ok(n);
        }

        let event = self.update_state(mailbox, slot_key, res);
        let actions = Self::plan_actions(event, slot_key);

        let mut submissions = 0;
        if let Some(key) = actions.resubmit_slot {
            let token = registry.alloc(key);
            submissions += self.submit_slot((key, token), ctx, registry)?;
        }

        if actions.dispatch_waiters {
            UdpPoolManager::dispatch_waiters_static(self, mailbox, comp);
        }

        if actions.rebalance_pool {
            submissions += self.rebalance(mailbox, ctx, registry)?;
        }

        Ok(submissions)
    }

    fn try_fast_deliver(
        &mut self,
        mailbox: &mut UdpMailbox,
        waiter: UdpWaiter,
        completion: (SlotKey, &RIORESULT),
        comp: &mut RioCompletionContext<'_>,
        ctx: &mut RioContext,
        registry: &mut TokenRegistry,
    ) -> RioResult<Option<usize>> {
        let (slot_key, res) = completion;
        let slots_len = self.slots.len();
        let slot = self.slots.get_mut(slot_key).ok_or_else(|| {
            error_stack::Report::new(RioError::Internal)
                .attach("UDP recv pool slot missing in try_fast_deliver")
        })?;
        let slab = self.slab.as_mut().ok_or_else(|| {
            error_stack::Report::new(RioError::Internal)
                .attach("UDP slab missing in try_fast_deliver")
        })?;

        let bytes = res.BytesTransferred as usize;
        if bytes > slab.chunk_capacity() {
            mailbox.waiters.push_front(waiter);
            return Err(error_stack::Report::new(RioError::Internal)).attach(format!(
                "UDP recv completion exceeded slot capacity in try_fast_deliver: bytes={}, cap={}",
                bytes,
                slab.chunk_capacity()
            ));
        }

        let Some(next_idx) = slab.free_indices.pop_front() else {
            mailbox.waiters.push_front(waiter);
            return Ok(None);
        };
        let completed_idx = std::mem::replace(&mut slot.current_idx, next_idx);

        let mut delivered = false;
        let mut payload = match waiter.kind {
            UdpWaiterKind::Recv => Some(FastDeliverPayload::Recv {
                idx: completed_idx,
                len: bytes,
            }),
            UdpWaiterKind::Stream => Some(FastDeliverPayload::Stream {
                idx: completed_idx,
                len: bytes,
            }),
        };

        if let Some(payload) = payload.take() {
            match payload {
                FastDeliverPayload::Recv { idx, len } => {
                    let Some(buf) = slab.chunk_view(idx, len) else {
                        mailbox.waiters.push_front(waiter);
                        slab.free_indices.push_front(slot.current_idx);
                        slot.current_idx = idx;
                        return Ok(None);
                    };
                    if UdpPoolManager::deliver_to_recv_waiter_raw(comp, waiter, buf.as_slice()) {
                        slot.in_flight = false;
                        delivered = true;
                    }
                }
                FastDeliverPayload::Stream { idx, len } => {
                    let Some(mut buf) = slab.chunk_view(idx, len) else {
                        mailbox.waiters.push_front(waiter);
                        slab.free_indices.push_front(slot.current_idx);
                        slot.current_idx = idx;
                        return Ok(None);
                    };
                    buf.set_len(len);
                    let addr = UdpPoolManager::parse_rio_address(
                        &slot.addr,
                        std::mem::size_of::<SockAddrStorage>() as i32,
                    );
                    match UdpPoolManager::deliver_to_stream_waiter_raw(comp, waiter, buf, addr) {
                        Ok(()) => {
                            slot.in_flight = false;
                            delivered = true;
                        }
                        Err(_returned_buf) => {
                            slab.free_indices.push_front(slot.current_idx);
                            slot.current_idx = idx;
                            slot.in_flight = false;
                            delivered = false;
                        }
                    }
                }
            }
        }

        if delivered {
            let resubmit = !slot.stop_requested && slots_len <= self.target_credits;
            slot.stop_requested = false;

            let mut submissions = 0;
            if resubmit {
                let token = registry.alloc(slot_key);
                submissions += self.submit_slot((slot_key, token), ctx, registry)?;
            }
            Ok(Some(submissions))
        } else {
            mailbox.waiters.push_front(waiter);
            Ok(None)
        }
    }

    pub(crate) fn handle_drain_comp(&mut self) {
        if self.state == UdpPoolState::Draining && self.slots.iter().all(|s| !s.1.in_flight) {
            self.state = UdpPoolState::Closed;
        }
    }

    pub(crate) fn cleanup_drained(&mut self, ctx: &mut RioContext) -> bool {
        if self.state == UdpPoolState::Closed {
            let slots: Vec<UdpRecvPoolSlot> = self.slots.drain().map(|(_, s)| s).collect();
            for slot in slots {
                self.free_slot(slot, ctx);
            }
            if let Some(slab) = self.slab.take()
                && !slab.rio_id.is_invalid()
            {
                ctx.env.dispatch.deregister_buffer(slab.rio_id);
            }
            return true;
        }
        false
    }
}

impl UdpPoolManager {
    fn copy_data_to_recv_op(recv_op: &mut OpUdpRecv<crate::RawHandle>, src: &[u8]) -> usize {
        let start = recv_op.buf_offset.min(recv_op.buf.len());
        let dst_len = recv_op.buf.len().saturating_sub(start);
        let copied = dst_len.min(src.len());
        if copied > 0 {
            recv_op.buf.as_slice_mut()[start..start + copied].copy_from_slice(&src[..copied]);
        }
        copied
    }

    fn parse_rio_address(addr: &SockAddrStorage, len: i32) -> std::net::SocketAddr {
        let s = unsafe { std::slice::from_raw_parts(addr as *const _ as *const u8, len as usize) };
        to_socket_addr(s).map_err(|_| ()).unwrap_or_else(|_| {
            std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::UNSPECIFIED,
                0,
            ))
        })
    }

    fn into_op_datagram(datagram: UdpPoolPacket) -> OpUdpRecvPacket {
        let addr = Self::parse_rio_address(&datagram.addr, datagram.addr_len);
        OpUdpRecvPacket {
            buf: datagram.buf,
            addr,
        }
    }

    fn deliver_to_stream_waiter_raw(
        comp: &mut RioCompletionContext<'_>,
        waiter: UdpWaiter,
        buf: FixedBuf,
        addr: std::net::SocketAddr,
    ) -> Result<(), FixedBuf> {
        let user_data = waiter.user_data;
        let generation = waiter.generation;
        let ops = &mut comp.ops;
        if user_data >= ops.local.len() {
            return Err(buf);
        }
        let Some(SlotView::InFlightWaiting(mut slot)) = ops.slot_view(user_data) else {
            return Err(buf);
        };
        if slot.platform_mut().generation != generation {
            return Err(buf);
        }

        let mut guard = slot;
        let d_len = buf.len();

        let stream_op = Self::get_stream_op_mut(&mut guard);
        if let Some(stream_op) = stream_op {
            stream_op.result = Some(OpUdpRecvPacket { buf, addr });
            guard.platform_mut().rio_pool_waiting = false;

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
            let _ = std::mem::take(guard.platform_mut());
            comp.ops.shared.push_free(user_data);
            Ok(())
        } else {
            Err(buf)
        }
    }

    fn deliver_to_recv_waiter_raw(
        comp: &mut RioCompletionContext<'_>,
        waiter: UdpWaiter,
        data: &[u8],
    ) -> bool {
        let user_data = waiter.user_data;
        let generation = waiter.generation;
        let ops = &mut comp.ops;
        if user_data >= ops.local.len() {
            return false;
        }
        let Some(SlotView::InFlightWaiting(mut slot)) = ops.slot_view(user_data) else {
            return false;
        };
        if slot.platform_mut().generation != generation {
            return false;
        }

        let copied_opt = slot.with_op_mut(|iocp_op| {
            if let IocpOpPayload::UdpRecv(ref mut kernel) = iocp_op.payload {
                // SAFETY: `kernel.user` is valid while the op is in-flight.
                let recv_op = unsafe { kernel.user.as_mut() };
                Some(Self::copy_data_to_recv_op(recv_op, data))
            } else {
                None
            }
        });
        let Some(copied) = copied_opt.flatten() else {
            return false;
        };

        slot.platform_mut().rio_pool_waiting = false;
        let event = CompletionEvent {
            user_data: encode_completion_token(user_data, generation),
            res: copied.min(i32::MAX as usize) as i32,
            flags: 0,
        };

        let mut completed = slot.complete();
        let (payload, detail) = completed.take_completion_data();
        comp.table
            .record_completion_with_data(event, payload, detail);
        comp.events.push(event);
        let _ = completed.take_op();
        let _ = std::mem::take(completed.platform_mut());
        comp.ops.shared.push_free(user_data);
        true
    }

    fn get_stream_op_mut<'a>(
        guard: &'a mut Slot<'_, InFlightWaiting>,
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

    fn deliver_to_stream_waiter(
        comp: &mut RioCompletionContext<'_>,
        waiter: UdpWaiter,
        datagram: &mut Option<UdpPoolPacket>,
    ) -> bool {
        let Some(d) = datagram.take() else {
            return false;
        };
        let addr = Self::parse_rio_address(&d.addr, d.addr_len);
        match Self::deliver_to_stream_waiter_raw(comp, waiter, d.buf, addr) {
            Ok(()) => true,
            Err(buf) => {
                *datagram = Some(UdpPoolPacket {
                    buf,
                    addr: d.addr,
                    addr_len: d.addr_len,
                });
                false
            }
        }
    }

    fn deliver_to_recv_waiter(
        comp: &mut RioCompletionContext<'_>,
        waiter: UdpWaiter,
        datagram: &mut Option<UdpPoolPacket>,
    ) -> bool {
        let data = match datagram.as_ref() {
            Some(d) => d.buf.as_slice(),
            None => return false,
        };
        if Self::deliver_to_recv_waiter_raw(comp, waiter, data) {
            let _ = datagram.take();
            true
        } else {
            false
        }
    }

    // pub(crate) fn dispatch_waiters(...) is moved to dispatch_waiters_static

    fn dispatch_waiters_static(
        _pool: &mut UdpRecvPool,
        mailbox: &mut UdpMailbox,
        comp: &mut RioCompletionContext<'_>,
    ) {
        loop {
            let (waiter, mut datagram) = {
                let Some(waiter) = mailbox.waiters.pop_front() else {
                    return;
                };
                let Some(datagram) = mailbox.queue.pop_front() else {
                    mailbox.waiters.push_front(waiter);
                    return;
                };
                (waiter, Some(datagram))
            };

            let delivered = match waiter.kind {
                UdpWaiterKind::Stream => {
                    Self::deliver_to_stream_waiter(comp, waiter, &mut datagram)
                }
                UdpWaiterKind::Recv => Self::deliver_to_recv_waiter(comp, waiter, &mut datagram),
            };

            if !delivered && let Some(returned_datagram) = datagram {
                mailbox.queue.push_front(returned_datagram);
                if mailbox.queue.len() > UDP_RECV_POOL_QUEUE_CAP {
                    let _ = mailbox.queue.pop_back();
                }
            }
        }
    }
}
