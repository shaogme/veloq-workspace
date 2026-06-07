//! Buffer and request-queue registration state for the RIO backend.
//!
//! `RioRegistry` owns all registration metadata required to submit operations:
//! - chunk registrations for pre-registered slab regions,
//! - lazy heap-buffer registrations with bounded cache behavior,
//! - a dedicated registered address scratch buffer for RIO address operations,
//! - deferred deregistration queues for safe teardown sequencing.
//!
//! This module is focused on resource identity and lifetime bookkeeping; it
//! intentionally avoids actor scheduling or completion routing policy.

use crate::IoFd;
use crate::config::BorrowedRawHandle;
use crate::net::addr::SockAddrStorage;
use crate::rio::RioEnv;
use crate::rio::core::submit_ops::{RioBufferId, RioProvider, RioRq, RioRqConfig};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use std::time::{Duration, Instant};
use veloq_buf::{FixedBuf, PoolKind};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RioRegistrationStats {
    pub(crate) chunk_register_attempts: u64,
    pub(crate) chunk_register_success: u64,
    pub(crate) chunk_register_failures: u64,
    pub(crate) chunk_register_skipped_recent_failure: u64,
    pub(crate) heap_register_attempts: u64,
    pub(crate) heap_register_success: u64,
    pub(crate) heap_register_failures: u64,
    pub(crate) heap_register_skipped_recent_failure: u64,
}

pub(crate) struct RioRegistry {
    pub(crate) chunk_registry: Vec<RioBufferId>,
    addr_slots: Box<[SockAddrStorage]>,
    addr_slot_in_use: Vec<bool>,
    addr_free_slots: Vec<usize>,
    addr_buffer_id: RioBufferId,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RioBufferId
    pub(crate) heap_rio_bufs: FxHashMap<(usize, usize, u64), RioBufferId>,
    pub(crate) pending_deregistrations: Vec<RioBufferId>,
    pub(crate) rq_depth: u32,
    pub(crate) registration_stats: RioRegistrationStats,
    chunk_register_failures_recent: FxHashMap<u16, Instant>,
    heap_register_failures_recent: FxHashMap<(usize, usize, u64), Instant>,
}

#[derive(Clone, Copy)]
pub(crate) struct RioAddrReservation {
    pub(crate) slot: usize,
    pub(crate) rio_buf: RIO_BUF,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioSubmissionKind {
    Recv,
    Send,
}

impl RioSubmissionKind {
    pub(crate) fn data_len(
        self,
        buf: &FixedBuf,
        buf_offset: usize,
        operation: &'static str,
    ) -> RioResult<u32> {
        let bound = self.bound(buf);
        if buf_offset > bound {
            return RioError::InvalidInput
                .with_ctx("rio_operation", operation)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("buffer_bound", bound)
                .with_ctx("buffer_bound_kind", self.bound_name())
                .attach_note(self.offset_exceeded_note());
        }

        let len = bound - buf_offset;
        u32::try_from(len).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("rio_operation", operation)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO data buffer length exceeds u32")
        })
    }

    fn bound(self, buf: &FixedBuf) -> usize {
        match self {
            Self::Recv => buf.capacity(),
            Self::Send => buf.len(),
        }
    }

    fn bound_name(self) -> &'static str {
        match self {
            Self::Recv => "capacity",
            Self::Send => "length",
        }
    }

    fn offset_exceeded_note(self) -> &'static str {
        match self {
            Self::Recv => "RIO recv buffer offset exceeds buffer capacity",
            Self::Send => "RIO send buffer offset exceeds buffer length",
        }
    }
}

impl RioRegistry {
    pub(crate) fn new(rq_depth: u32, addr_capacity: usize) -> Self {
        let addr_capacity = addr_capacity.max(1);
        let mut addr_free_slots = Vec::with_capacity(addr_capacity);
        for slot in (0..addr_capacity).rev() {
            addr_free_slots.push(slot);
        }

        Self {
            chunk_registry: Vec::new(),
            addr_slots: vec![SockAddrStorage::default(); addr_capacity].into_boxed_slice(),
            addr_slot_in_use: vec![false; addr_capacity],
            addr_free_slots,
            addr_buffer_id: RioBufferId::INVALID,
            heap_rio_bufs: FxHashMap::default(),
            pending_deregistrations: Vec::new(),
            rq_depth,
            registration_stats: RioRegistrationStats::default(),
            chunk_register_failures_recent: FxHashMap::default(),
            heap_register_failures_recent: FxHashMap::default(),
        }
    }

    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, usize)> {
        let info = buf.resolve_region_info();

        if info.pool_kind == PoolKind::Heap {
            return self.resolve_heap_id(buf, info.offset, env);
        }

        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if !id.is_invalid() => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = env.registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(
                info.id,
                (chunk_info.ptr.as_ptr(), chunk_info.len.get()),
                env,
            )?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        match buffer_id {
            Some(id) => Ok((id, info.offset)),
            None => RioError::Internal
                .with_ctx("chunk_id", info.id as usize)
                .attach_note("RIO chunk not registered"),
        }
    }

    pub(crate) fn prepare_submission(
        &mut self,
        buf: &FixedBuf,
        buf_offset: usize,
        len: u32,
        env: RioEnv<'_>,
    ) -> RioResult<RIO_BUF> {
        let buf_offset_u32 = u32::try_from(buf_offset).map_err(|_| {
            RioError::InvalidInput
                .to_report()
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO buffer offset exceeds u32")
        })?;
        let (buffer_id, offset) = self.resolve_buffer_id(buf, env)?;
        let offset = Self::checked_rio_buffer_offset(offset, buf_offset_u32, len, buf)?;
        let rio_buf = RIO_BUF {
            BufferId: buffer_id.0,
            Offset: offset,
            Length: len,
        };
        Ok(rio_buf)
    }

    fn checked_rio_buffer_offset(
        base_offset: usize,
        buf_offset: u32,
        len: u32,
        buf: &FixedBuf,
    ) -> RioResult<u32> {
        let base_offset_u32 = u32::try_from(base_offset).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("rio_base_offset", base_offset)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO registered buffer base offset exceeds u32")
        })?;
        base_offset_u32.checked_add(buf_offset).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("rio_base_offset", base_offset)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO buffer offset addition overflow")
        })
    }

    pub(crate) fn register_chunk(
        &mut self,
        id: u16,
        mem: (*const u8, usize),
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        if let Some(last_fail) = self.chunk_register_failures_recent.get(&id)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats
                .chunk_register_skipped_recent_failure = self
                .registration_stats
                .chunk_register_skipped_recent_failure
                .saturating_add(1);
            return RioError::ResourceExhaustion
                .with_ctx("chunk_id", id as usize)
                .attach_note("RIO chunk registration skipped due to recent failure");
        }

        let (ptr, len) = mem;
        let id_idx = id as usize;

        if id_idx >= self.chunk_registry.len() {
            self.chunk_registry.resize(id_idx + 1, RioBufferId::INVALID);
        }

        if let Some(existing) = self.chunk_registry.get(id_idx).copied()
            && !existing.is_invalid()
        {
            self.pending_deregistrations.push(existing);
        }

        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);

        let buf_id = match env.dispatch.register_buffer(ptr, len as u32) {
            Ok(id) => id,
            Err(e) => {
                self.registration_stats.chunk_register_failures = self
                    .registration_stats
                    .chunk_register_failures
                    .saturating_add(1);
                self.chunk_register_failures_recent
                    .insert(id, Instant::now());
                return Err(e)
                    .with_ctx("chunk_id", id as usize)
                    .with_ctx("buffer_length", len)
                    .attach_note("RIORegisterBuffer failed for chunk");
            }
        };

        self.chunk_registry[id_idx] = buf_id;
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);
        Ok(())
    }

    pub(crate) fn create_rq(
        &mut self,
        target: (BorrowedRawHandle<'_>, IoFd),
        env: RioEnv<'_>,
    ) -> RioResult<RioRq> {
        let (handle, fd) = target;

        let max_outstanding_recvs = self.rq_depth;
        let max_outstanding_sends = self.rq_depth;

        env.dispatch
            .create_rq(RioRqConfig {
                socket: handle.raw().as_socket(),
                max_outstanding_recvs,
                max_receive_data_buffers: 1,
                max_outstanding_sends,
                max_send_data_buffers: 1,
                recv_cq: env.cq,
                send_cq: env.cq,
                context: std::ptr::null(),
            })
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("socket_raw", handle.raw().as_handle() as usize)
            .with_ctx("rq_depth", self.rq_depth)
            .with_ctx("max_outstanding_recvs", max_outstanding_recvs)
            .with_ctx("max_outstanding_sends", max_outstanding_sends)
            .with_ctx("max_receive_data_buffers", 1_u32)
            .with_ctx("max_send_data_buffers", 1_u32)
            .attach_note("RIOCreateRequestQueue failed")
    }

    pub(crate) fn flush_deregs(&mut self, env: RioEnv<'_>) {
        if self.pending_deregistrations.is_empty() {
            return;
        }
        for id in self.pending_deregistrations.drain(..) {
            env.dispatch.deregister_buffer(id);
        }
    }

    pub(crate) fn cleanup_deregister(&mut self, env: RioEnv<'_>) {
        use std::collections::HashSet;
        let mut deregistered = HashSet::new();

        for id in self.chunk_registry.iter().copied() {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }
        for id in self.pending_deregistrations.drain(..) {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }
        let addr_buffer_id = std::mem::replace(&mut self.addr_buffer_id, RioBufferId::INVALID);
        if !addr_buffer_id.is_invalid() && deregistered.insert(addr_buffer_id.0 as usize) {
            env.dispatch.deregister_buffer(addr_buffer_id);
        }
        for id in self.heap_rio_bufs.values().copied() {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }

        self.chunk_registry.clear();
        self.reset_addr_slots();
        self.heap_rio_bufs.clear();
        self.chunk_register_failures_recent.clear();
        self.heap_register_failures_recent.clear();
    }

    fn resolve_heap_id(
        &mut self,
        buf: &FixedBuf,
        offset: usize,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, usize)> {
        let key = (
            buf.as_ptr() as usize,
            buf.capacity(),
            buf.resolve_region_info().cookie,
        );
        if let Some(&id) = self.heap_rio_bufs.get(&key) {
            return Ok((id, offset));
        }

        if let Some(last_fail) = self.heap_register_failures_recent.get(&key)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats.heap_register_skipped_recent_failure = self
                .registration_stats
                .heap_register_skipped_recent_failure
                .saturating_add(1);
            return RioError::ResourceExhaustion
                .with_ctx("registration_mode", env.registration_mode.as_str())
                .with_ctx("buffer_ptr", key.0)
                .with_ctx("buffer_capacity", key.1)
                .with_ctx("buffer_cookie", key.2)
                .attach_note("RIO heap registration skipped due to recent failure");
        }

        if self.heap_rio_bufs.len() >= 1024 {
            for id in self.heap_rio_bufs.values().copied() {
                self.pending_deregistrations.push(id);
            }
            self.heap_rio_bufs.clear();
        }

        let id = self.register_heap_raw(buf, key, env)?;
        Ok((id, offset))
    }

    fn register_heap_raw(
        &mut self,
        buf: &FixedBuf,
        key: (usize, usize, u64),
        env: RioEnv<'_>,
    ) -> RioResult<RioBufferId> {
        self.registration_stats.heap_register_attempts = self
            .registration_stats
            .heap_register_attempts
            .saturating_add(1);

        let id = match env
            .dispatch
            .register_buffer(buf.as_ptr(), buf.capacity() as u32)
        {
            Ok(id) => id,
            Err(e) => {
                self.registration_stats.heap_register_failures = self
                    .registration_stats
                    .heap_register_failures
                    .saturating_add(1);
                self.heap_register_failures_recent
                    .insert(key, Instant::now());
                return Err(e)
                    .with_ctx("registration_mode", env.registration_mode.as_str())
                    .with_ctx("buffer_ptr", key.0)
                    .with_ctx("buffer_capacity", key.1)
                    .with_ctx("buffer_cookie", key.2)
                    .attach_note("RIORegisterBuffer failed for heap buffer");
            }
        };

        self.heap_rio_bufs.insert(key, id);
        self.heap_register_failures_recent.remove(&key);
        self.registration_stats.heap_register_success = self
            .registration_stats
            .heap_register_success
            .saturating_add(1);
        Ok(id)
    }

    pub(crate) fn prepare_send_addr(
        &mut self,
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        env: RioEnv<'_>,
    ) -> RioResult<RioAddrReservation> {
        let rio_addr_len = Self::validate_send_addr(addr_ptr, addr_len)?;
        let reservation = self.allocate_addr_slot(env)?;
        let dst = (&mut self.addr_slots[reservation.slot] as *mut SockAddrStorage).cast::<u8>();
        let copy_len = (addr_len as usize).min(rio_addr_len as usize);
        // SAFETY: `dst` points at an owned scratch slot, and `addr_ptr` was
        // validated as non-null with at least `copy_len` readable bytes.
        unsafe {
            std::ptr::write_bytes(dst, 0, std::mem::size_of::<SockAddrStorage>());
            std::ptr::copy_nonoverlapping(addr_ptr.cast::<u8>(), dst, copy_len);
        }
        Ok(RioAddrReservation {
            rio_buf: RIO_BUF {
                Length: rio_addr_len,
                ..reservation.rio_buf
            },
            ..reservation
        })
    }

    pub(crate) fn prepare_recv_addr(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        let reservation = self.allocate_addr_slot(env)?;
        let dst = (&mut self.addr_slots[reservation.slot] as *mut SockAddrStorage).cast::<u8>();
        // SAFETY: `dst` points at an owned scratch slot.
        unsafe {
            std::ptr::write_bytes(dst, 0, std::mem::size_of::<SockAddrStorage>());
        }
        Ok(reservation)
    }

    pub(crate) fn copy_addr_slot_to(
        &self,
        slot: usize,
        dst: *mut SockAddrStorage,
    ) -> RioResult<()> {
        if dst.is_null() {
            return RioError::Internal
                .attach_note("RIO recv_from completion missing output address");
        }
        let Some(src) = self.addr_slots.get(slot) else {
            return RioError::Internal
                .with_ctx("addr_slot", slot)
                .attach_note("RIO address slot out of bounds");
        };
        // SAFETY: `src` is a live scratch slot and `dst` points at the op payload.
        unsafe {
            std::ptr::copy_nonoverlapping(src as *const SockAddrStorage, dst, 1);
        }
        Ok(())
    }

    pub(crate) fn free_addr_slot(&mut self, slot: Option<usize>) {
        let Some(slot) = slot else {
            return;
        };
        if let Some(in_use) = self.addr_slot_in_use.get_mut(slot)
            && *in_use
        {
            *in_use = false;
            self.addr_free_slots.push(slot);
        }
    }

    fn allocate_addr_slot(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        let buffer_id = self.ensure_addr_buffer_registered(env)?;
        let Some(slot) = self.addr_free_slots.pop() else {
            return RioError::ResourceExhaustion
                .with_ctx("addr_capacity", self.addr_slots.len())
                .attach_note("RIO address scratch buffer exhausted");
        };
        self.addr_slot_in_use[slot] = true;
        let offset = Self::addr_slot_offset(slot)?;
        Ok(RioAddrReservation {
            slot,
            rio_buf: RIO_BUF {
                BufferId: buffer_id.0,
                Offset: offset,
                Length: std::mem::size_of::<SOCKADDR_INET>() as u32,
            },
        })
    }

    fn ensure_addr_buffer_registered(&mut self, env: RioEnv<'_>) -> RioResult<RioBufferId> {
        if !self.addr_buffer_id.is_invalid() {
            return Ok(self.addr_buffer_id);
        }

        let len = std::mem::size_of_val(&*self.addr_slots);
        let len_u32 = u32::try_from(len).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("addr_buffer_length", len)
                .attach_note("RIO address scratch buffer too large")
        })?;
        let id = env
            .dispatch
            .register_buffer(self.addr_slots.as_ptr().cast::<u8>(), len_u32)
            .with_ctx("buffer_length", len)
            .attach_note("RIORegisterBuffer failed for address scratch buffer")?;
        self.addr_buffer_id = id;
        Ok(id)
    }

    fn validate_send_addr(addr_ptr: *const std::ffi::c_void, addr_len: i32) -> RioResult<u32> {
        if addr_ptr.is_null() {
            return RioError::Internal.attach_note("RIO send_to received null address");
        }
        if addr_len < 0 {
            return RioError::Internal
                .with_ctx("address_len", addr_len)
                .attach_note("RIO send_to invalid negative address length");
        }
        // SAFETY: addr_ptr is checked for null, and sa_family is a standard field in SOCKADDR.
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return RioError::Internal
                    .with_ctx("address_family", family)
                    .attach_note("RIO unsupported address family");
            }
        };
        if (addr_len as usize) < min_len {
            return RioError::Internal
                .with_ctx("address_len", addr_len)
                .with_ctx("min_address_len", min_len)
                .attach_note("RIO send_to invalid address length");
        }

        Ok(std::mem::size_of::<SOCKADDR_INET>() as u32)
    }

    fn addr_slot_offset(slot: usize) -> RioResult<u32> {
        let offset = slot
            .checked_mul(std::mem::size_of::<SockAddrStorage>())
            .ok_or(RioError::ResourceExhaustion)
            .attach_note("RIO address slot offset overflow")?;
        u32::try_from(offset).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("addr_slot", slot)
                .with_ctx("addr_slot_offset", offset)
                .attach_note("RIO address slot offset exceeds u32")
        })
    }

    fn reset_addr_slots(&mut self) {
        self.addr_free_slots.clear();
        for (slot, in_use) in self.addr_slot_in_use.iter_mut().enumerate().rev() {
            *in_use = false;
            self.addr_free_slots.push(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn fixed_buf(capacity: usize, len: usize) -> FixedBuf {
        let mut buf = FixedBuf::alloc_heap(NonZeroUsize::new(capacity).expect("non-zero capacity"))
            .expect("heap buffer allocation failed");
        buf.set_len(len);
        buf
    }

    #[test]
    fn rio_submission_len_allows_exact_boundaries() {
        let buf = fixed_buf(8, 4);

        assert_eq!(
            RioSubmissionKind::Recv
                .data_len(&buf, buf.capacity(), "recv")
                .expect("recv boundary should be allowed"),
            0
        );
        assert_eq!(
            RioSubmissionKind::Send
                .data_len(&buf, buf.len(), "send")
                .expect("send boundary should be allowed"),
            0
        );
    }

    #[test]
    fn rio_submission_len_rejects_out_of_bounds_offsets_for_all_entries() {
        let buf = fixed_buf(8, 4);
        let cases = [
            (RioSubmissionKind::Recv, "recv", buf.capacity() + 1),
            (RioSubmissionKind::Send, "send", buf.len() + 1),
            (RioSubmissionKind::Recv, "udp_recv", buf.capacity() + 1),
            (RioSubmissionKind::Send, "udp_send", buf.len() + 1),
            (RioSubmissionKind::Send, "send_to", buf.len() + 1),
            (RioSubmissionKind::Recv, "udp_recv_from", buf.capacity() + 1),
        ];

        for (kind, operation, offset) in cases {
            let err = kind
                .data_len(&buf, offset, operation)
                .expect_err("out-of-bounds RIO offset should fail");
            assert_eq!(*err.inner(), RioError::InvalidInput);
        }
    }

    #[test]
    fn rio_buffer_offset_rejects_u32_overflow() {
        let buf = fixed_buf(8, 4);

        let err = RioRegistry::checked_rio_buffer_offset((u32::MAX as usize) + 1, 0, 0, &buf)
            .expect_err("base offset above u32 should fail");
        assert_eq!(*err.inner(), RioError::ResourceExhaustion);

        let err = RioRegistry::checked_rio_buffer_offset(u32::MAX as usize, 1, 0, &buf)
            .expect_err("combined RIO offset should overflow");
        assert_eq!(*err.inner(), RioError::ResourceExhaustion);
    }
}
