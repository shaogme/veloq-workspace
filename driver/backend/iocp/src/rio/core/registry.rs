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
const HEAP_REGISTRATION_CACHE_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RioHeapBufferKey {
    ptr: usize,
    cap: usize,
    cookie: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioHeapLeaseToken {
    key: RioHeapBufferKey,
    id: RioBufferId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RioChunkRegistrationKey {
    id: u16,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioChunkLeaseToken {
    key: RioChunkRegistrationKey,
    id: RioBufferId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioBufferLeaseToken {
    Chunk(RioChunkLeaseToken),
    Heap(RioHeapLeaseToken),
}

#[derive(Clone, Copy)]
pub(crate) struct RioPreparedBuffer {
    pub(crate) rio_buf: RIO_BUF,
    pub(crate) lease: Option<RioBufferLeaseToken>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RioBufferRegistration {
    id: RioBufferId,
    active_refs: usize,
    retired: bool,
}

impl RioBufferRegistration {
    fn new(id: RioBufferId) -> Self {
        Self {
            id,
            active_refs: 0,
            retired: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RioChunkRegistration {
    generation: u64,
    registration: RioBufferRegistration,
}

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
    pub(crate) chunk_registry: Vec<Option<RioChunkRegistration>>,
    retired_chunk_registrations: FxHashMap<RioChunkRegistrationKey, RioBufferRegistration>,
    addr_slots: Box<[SockAddrStorage]>,
    addr_slot_in_use: Vec<bool>,
    addr_free_slots: Vec<usize>,
    addr_buffer_id: RioBufferId,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RIO buffer registration.
    pub(crate) heap_rio_bufs: FxHashMap<RioHeapBufferKey, RioBufferRegistration>,
    pub(crate) pending_deregistrations: Vec<RioBufferId>,
    pub(crate) rq_depth: u32,
    pub(crate) registration_stats: RioRegistrationStats,
    chunk_register_failures_recent: FxHashMap<u16, Instant>,
    heap_register_failures_recent: FxHashMap<RioHeapBufferKey, Instant>,
    next_registration_generation: u64,
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
            retired_chunk_registrations: FxHashMap::default(),
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
            next_registration_generation: 0,
        }
    }

    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, usize, Option<RioBufferLeaseToken>)> {
        let info = buf.resolve_region_info();

        if info.pool_kind == PoolKind::Heap {
            return self.resolve_heap_id(buf, info.offset, env);
        }

        let mut lease = self.current_chunk_lease(info.id);

        if lease.is_none()
            && let Some(chunk_info) = env.registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(
                info.id,
                (chunk_info.ptr.as_ptr(), chunk_info.len.get()),
                env,
            )?;
            lease = self.current_chunk_lease(info.id);
        }

        match lease {
            Some(RioBufferLeaseToken::Chunk(lease)) => Ok((
                lease.id,
                info.offset,
                Some(RioBufferLeaseToken::Chunk(lease)),
            )),
            Some(RioBufferLeaseToken::Heap(_)) => {
                debug_assert!(false, "resolved heap lease from chunk registry");
                RioError::Internal
                    .with_ctx("chunk_id", info.id as usize)
                    .attach_note("RIO chunk registration resolved to heap lease")
            }
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
    ) -> RioResult<RioPreparedBuffer> {
        let buf_offset_u32 = u32::try_from(buf_offset).map_err(|_| {
            RioError::InvalidInput
                .to_report()
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO buffer offset exceeds u32")
        })?;
        let (buffer_id, offset, lease) = self.resolve_buffer_id(buf, env)?;
        let offset = Self::checked_rio_buffer_offset(offset, buf_offset_u32, len, buf)?;
        let rio_buf = RIO_BUF {
            BufferId: buffer_id.0,
            Offset: offset,
            Length: len,
        };
        Ok(RioPreparedBuffer { rio_buf, lease })
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
            self.chunk_registry.resize(id_idx + 1, None);
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

        let generation = self.next_registration_generation();
        let previous = self.chunk_registry[id_idx].replace(RioChunkRegistration {
            generation,
            registration: RioBufferRegistration::new(buf_id),
        });
        if let Some(previous) = previous {
            let key = RioChunkRegistrationKey {
                id,
                generation: previous.generation,
            };
            self.retire_chunk_registration(key, previous.registration, env);
        }
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);
        Ok(())
    }

    fn next_registration_generation(&mut self) -> u64 {
        self.next_registration_generation = self.next_registration_generation.wrapping_add(1);
        if self.next_registration_generation == 0 {
            self.next_registration_generation = 1;
        }
        self.next_registration_generation
    }

    fn current_chunk_lease(&self, id: u16) -> Option<RioBufferLeaseToken> {
        let entry = self.chunk_registry.get(id as usize)?.as_ref()?;
        Some(RioBufferLeaseToken::Chunk(RioChunkLeaseToken {
            key: RioChunkRegistrationKey {
                id,
                generation: entry.generation,
            },
            id: entry.registration.id,
        }))
    }

    fn retire_chunk_registration(
        &mut self,
        key: RioChunkRegistrationKey,
        mut registration: RioBufferRegistration,
        env: RioEnv<'_>,
    ) {
        if registration.active_refs == 0 {
            env.dispatch.deregister_buffer(registration.id);
            return;
        }
        registration.retired = true;
        self.retired_chunk_registrations.insert(key, registration);
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

        for id in self
            .chunk_registry
            .iter()
            .filter_map(|entry| entry.as_ref().map(|entry| entry.registration.id))
        {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }
        for id in self
            .retired_chunk_registrations
            .values()
            .map(|entry| entry.id)
        {
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
        for id in self.heap_rio_bufs.values().map(|entry| entry.id) {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }

        self.chunk_registry.clear();
        self.retired_chunk_registrations.clear();
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
    ) -> RioResult<(RioBufferId, usize, Option<RioBufferLeaseToken>)> {
        let key = RioHeapBufferKey {
            ptr: buf.as_ptr() as usize,
            cap: buf.capacity(),
            cookie: buf.resolve_region_info().cookie,
        };
        if let Some(entry) = self.heap_rio_bufs.get(&key) {
            let lease = RioHeapLeaseToken { key, id: entry.id };
            return Ok((entry.id, offset, Some(RioBufferLeaseToken::Heap(lease))));
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
                .with_ctx("buffer_ptr", key.ptr)
                .with_ctx("buffer_capacity", key.cap)
                .with_ctx("buffer_cookie", key.cookie)
                .attach_note("RIO heap registration skipped due to recent failure");
        }

        self.retire_heap_cache_for_insert(env);

        let id = self.register_heap_raw(buf, key, env)?;
        let lease = RioHeapLeaseToken { key, id };
        Ok((id, offset, Some(RioBufferLeaseToken::Heap(lease))))
    }

    fn register_heap_raw(
        &mut self,
        buf: &FixedBuf,
        key: RioHeapBufferKey,
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
                    .with_ctx("buffer_ptr", key.ptr)
                    .with_ctx("buffer_capacity", key.cap)
                    .with_ctx("buffer_cookie", key.cookie)
                    .attach_note("RIORegisterBuffer failed for heap buffer");
            }
        };

        self.heap_rio_bufs
            .insert(key, RioBufferRegistration::new(id));
        self.heap_register_failures_recent.remove(&key);
        self.registration_stats.heap_register_success = self
            .registration_stats
            .heap_register_success
            .saturating_add(1);
        Ok(id)
    }

    pub(crate) fn commit_buffer_lease(&mut self, lease: Option<RioBufferLeaseToken>) {
        let Some(lease) = lease else {
            return;
        };
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.commit_chunk_lease(lease),
            RioBufferLeaseToken::Heap(lease) => self.commit_heap_lease(lease),
        }
    }

    fn commit_chunk_lease(&mut self, lease: RioChunkLeaseToken) {
        if let Some(entry) = self.current_chunk_registration_mut(lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "committed stale RIO chunk lease");
                return;
            }
            entry.active_refs = entry.active_refs.saturating_add(1);
            return;
        }

        let Some(entry) = self.retired_chunk_registrations.get_mut(&lease.key) else {
            debug_assert!(false, "committed unknown RIO chunk lease");
            return;
        };
        if entry.id != lease.id {
            debug_assert!(false, "committed stale RIO chunk lease");
            return;
        }
        entry.active_refs = entry.active_refs.saturating_add(1);
    }

    fn commit_heap_lease(&mut self, lease: RioHeapLeaseToken) {
        let Some(entry) = self.heap_rio_bufs.get_mut(&lease.key) else {
            debug_assert!(false, "committed unknown RIO heap lease");
            return;
        };
        if entry.id != lease.id {
            debug_assert!(false, "committed stale RIO heap lease");
            return;
        }
        entry.active_refs = entry.active_refs.saturating_add(1);
    }

    pub(crate) fn release_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
        env: RioEnv<'_>,
    ) {
        if let Some(id) = self.release_buffer_lease_inner(lease) {
            env.dispatch.deregister_buffer(id);
        }
    }

    pub(crate) fn release_buffer_lease_deferred(&mut self, lease: Option<RioBufferLeaseToken>) {
        if let Some(id) = self.release_buffer_lease_inner(lease) {
            self.pending_deregistrations.push(id);
        }
    }

    fn release_buffer_lease_inner(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> Option<RioBufferId> {
        let Some(lease) = lease else {
            return None;
        };
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.release_chunk_lease_inner(lease),
            RioBufferLeaseToken::Heap(lease) => self.release_heap_lease_inner(lease),
        }
    }

    fn release_chunk_lease_inner(&mut self, lease: RioChunkLeaseToken) -> Option<RioBufferId> {
        let remove_current;
        if let Some(entry) = self.current_chunk_registration_mut(lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO chunk lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO chunk lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            remove_current = entry.active_refs == 0 && entry.retired;
        } else if let Some(entry) = self.retired_chunk_registrations.get_mut(&lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO chunk lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO chunk lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            if entry.active_refs == 0 && entry.retired {
                return self
                    .retired_chunk_registrations
                    .remove(&lease.key)
                    .map(|entry| entry.id);
            }
            return None;
        } else {
            debug_assert!(false, "released unknown RIO chunk lease");
            return None;
        }

        if remove_current {
            return self
                .chunk_registry
                .get_mut(lease.key.id as usize)
                .and_then(Option::take)
                .map(|entry| entry.registration.id);
        }
        None
    }

    fn release_heap_lease_inner(&mut self, lease: RioHeapLeaseToken) -> Option<RioBufferId> {
        let mut remove = false;
        if let Some(entry) = self.heap_rio_bufs.get_mut(&lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO heap lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO heap lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            remove = entry.active_refs == 0 && entry.retired;
        }

        if remove {
            return self.heap_rio_bufs.remove(&lease.key).map(|entry| entry.id);
        }
        None
    }

    fn current_chunk_registration_mut(
        &mut self,
        key: RioChunkRegistrationKey,
    ) -> Option<&mut RioBufferRegistration> {
        self.chunk_registry
            .get_mut(key.id as usize)?
            .as_mut()
            .filter(|entry| entry.generation == key.generation)
            .map(|entry| &mut entry.registration)
    }

    fn retire_heap_cache_for_insert(&mut self, env: RioEnv<'_>) {
        if self.heap_rio_bufs.len() < HEAP_REGISTRATION_CACHE_LIMIT {
            return;
        }

        let mut idle_keys = Vec::new();
        for (key, entry) in &mut self.heap_rio_bufs {
            if entry.active_refs == 0 {
                idle_keys.push(*key);
            } else {
                entry.retired = true;
            }
        }

        for key in idle_keys {
            if let Some(entry) = self.heap_rio_bufs.remove(&key) {
                env.dispatch.deregister_buffer(entry.id);
            }
        }
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
            return RioError::InvalidInput.attach_note("RIO send_to received null address");
        }
        if addr_len < 0 {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .attach_note("RIO send_to invalid negative address length");
        }
        if (addr_len as usize) < std::mem::size_of::<SOCKADDR>() {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .with_ctx("min_address_len", std::mem::size_of::<SOCKADDR>())
                .attach_note("RIO send_to address too short for SOCKADDR");
        }
        // SAFETY: addr_ptr is non-null and at least SOCKADDR-sized; read_unaligned avoids
        // imposing alignment requirements on future raw-pointer callers.
        let family = unsafe {
            std::ptr::addr_of!((*(addr_ptr as *const SOCKADDR)).sa_family).read_unaligned()
        };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return RioError::InvalidInput
                    .with_ctx("address_family", family)
                    .attach_note("RIO unsupported address family");
            }
        };
        if (addr_len as usize) < min_len {
            return RioError::InvalidInput
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
    use crate::BufferRegistrationMode;
    use crate::rio::core::submit_ops::{RioCq, RioDispatch};
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use windows_sys::Win32::Networking::WinSock::{
        RIO_BUFFERID, RIO_CQ, RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT,
    };

    static NEXT_REGISTER_ID: AtomicUsize = AtomicUsize::new(100);
    static REGISTER_FAILS: AtomicBool = AtomicBool::new(false);
    static DISPATCH_TEST_LOCK: Mutex<()> = Mutex::new(());
    static DEREGISTERED_IDS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    fn fixed_buf(capacity: usize, len: usize) -> FixedBuf {
        let mut buf = FixedBuf::alloc_heap(NonZeroUsize::new(capacity).expect("non-zero capacity"))
            .expect("heap buffer allocation failed");
        buf.set_len(len);
        buf
    }

    fn reset_dispatch_state() {
        NEXT_REGISTER_ID.store(100, Ordering::SeqCst);
        REGISTER_FAILS.store(false, Ordering::SeqCst);
        DEREGISTERED_IDS.lock().expect("deregister mutex").clear();
    }

    fn lock_dispatch_state() -> std::sync::MutexGuard<'static, ()> {
        DISPATCH_TEST_LOCK.lock().expect("dispatch test mutex")
    }

    fn deregistered_ids() -> Vec<usize> {
        DEREGISTERED_IDS.lock().expect("deregister mutex").clone()
    }

    unsafe extern "system" fn test_create_cq(
        _entries: u32,
        _notification: *const RIO_NOTIFICATION_COMPLETION,
    ) -> RIO_CQ {
        1 as _
    }

    unsafe extern "system" fn test_create_rq(
        _socket: usize,
        _max_outstanding_recvs: u32,
        _max_receive_data_buffers: u32,
        _max_outstanding_sends: u32,
        _max_send_data_buffers: u32,
        _recv_cq: RIO_CQ,
        _send_cq: RIO_CQ,
        _context: *const std::ffi::c_void,
    ) -> RIO_RQ {
        1 as _
    }

    unsafe extern "system" fn test_register_buffer(_ptr: *const u8, _len: u32) -> RIO_BUFFERID {
        if REGISTER_FAILS.swap(false, Ordering::SeqCst) {
            return 0 as _;
        }
        NEXT_REGISTER_ID.fetch_add(1, Ordering::SeqCst) as _
    }

    unsafe extern "system" fn test_deregister_buffer(id: RIO_BUFFERID) {
        DEREGISTERED_IDS
            .lock()
            .expect("deregister mutex")
            .push(id as usize);
    }

    unsafe extern "system" fn test_dequeue(
        _cq: RIO_CQ,
        _results: *mut RIORESULT,
        _count: u32,
    ) -> u32 {
        0
    }

    unsafe extern "system" fn test_notify(_cq: RIO_CQ) -> i32 {
        0
    }

    unsafe extern "system" fn test_close_cq(_cq: RIO_CQ) {}

    unsafe extern "system" fn test_receive(
        _rq: RIO_RQ,
        _buf: *const RIO_BUF,
        _num_bufs: u32,
        _flags: u32,
        _context: *const std::ffi::c_void,
    ) -> i32 {
        0
    }

    unsafe extern "system" fn test_send(
        _rq: RIO_RQ,
        _buf: *const RIO_BUF,
        _num_bufs: u32,
        _flags: u32,
        _context: *const std::ffi::c_void,
    ) -> i32 {
        0
    }

    unsafe extern "system" fn test_send_ex(
        _rq: RIO_RQ,
        _data_buf: *const RIO_BUF,
        _data_buf_count: u32,
        _local_addr: *const RIO_BUF,
        _remote_addr: *const RIO_BUF,
        _control_buf: *const RIO_BUF,
        _flags_buf: *const RIO_BUF,
        _flags: u32,
        _context: *const std::ffi::c_void,
    ) -> i32 {
        0
    }

    unsafe extern "system" fn test_receive_ex(
        _rq: RIO_RQ,
        _data_buf: *const RIO_BUF,
        _data_buf_count: u32,
        _local_addr: *const RIO_BUF,
        _remote_addr: *const RIO_BUF,
        _control_buf: *const RIO_BUF,
        _flags_buf: *const RIO_BUF,
        _flags: u32,
        _context: *const std::ffi::c_void,
    ) -> i32 {
        0
    }

    fn test_dispatch() -> RioDispatch {
        RioDispatch {
            create_cq: test_create_cq,
            create_rq: test_create_rq,
            register_buffer: test_register_buffer,
            deregister_buffer: test_deregister_buffer,
            dequeue: test_dequeue,
            notify: test_notify,
            close_cq: test_close_cq,
            receive: test_receive,
            send: test_send,
            send_ex: test_send_ex,
            receive_ex: test_receive_ex,
        }
    }

    fn test_env(dispatch: &RioDispatch) -> RioEnv<'_> {
        RioEnv {
            registrar: &veloq_buf::NoopRegistrar,
            dispatch,
            cq: RioCq::INVALID,
            registration_mode: BufferRegistrationMode::Strict,
        }
    }

    #[test]
    fn rio_chunk_retired_registration_waits_for_last_lease() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let chunk_id = 3;
        let key = RioChunkRegistrationKey {
            id: chunk_id,
            generation: 1,
        };
        registry.chunk_registry.resize(chunk_id as usize + 1, None);
        registry.chunk_registry[chunk_id as usize] = Some(RioChunkRegistration {
            generation: key.generation,
            registration: RioBufferRegistration::new(RioBufferId(41 as _)),
        });
        let lease = registry.current_chunk_lease(chunk_id);

        registry.commit_buffer_lease(lease);
        let previous = registry.chunk_registry[chunk_id as usize]
            .take()
            .expect("chunk registration");
        registry.retire_chunk_registration(key, previous.registration, env);

        assert!(deregistered_ids().is_empty());
        assert!(registry.retired_chunk_registrations.contains_key(&key));

        registry.release_buffer_lease(lease, env);

        assert_eq!(deregistered_ids(), vec![41]);
        assert!(!registry.retired_chunk_registrations.contains_key(&key));
    }

    #[test]
    fn rio_heap_retired_registration_deregisters_on_release() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let key = RioHeapBufferKey {
            ptr: 1,
            cap: 8,
            cookie: 13,
        };
        let lease = Some(RioBufferLeaseToken::Heap(RioHeapLeaseToken {
            key,
            id: RioBufferId(77 as _),
        }));
        registry
            .heap_rio_bufs
            .insert(key, RioBufferRegistration::new(RioBufferId(77 as _)));

        registry.commit_buffer_lease(lease);
        registry
            .heap_rio_bufs
            .get_mut(&key)
            .expect("heap registration")
            .retired = true;
        registry.release_buffer_lease(lease, env);

        assert_eq!(deregistered_ids(), vec![77]);
        assert!(!registry.heap_rio_bufs.contains_key(&key));
    }

    #[test]
    fn rio_chunk_register_failure_keeps_existing_registration_current() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let chunk_id = 2;
        registry.chunk_registry.resize(chunk_id as usize + 1, None);
        registry.chunk_registry[chunk_id as usize] = Some(RioChunkRegistration {
            generation: 1,
            registration: RioBufferRegistration::new(RioBufferId(55 as _)),
        });
        let byte = 0_u8;
        REGISTER_FAILS.store(true, Ordering::SeqCst);

        registry
            .register_chunk(chunk_id, (&byte as *const u8, 1), env)
            .expect_err("failed registration should be reported");

        let current = registry.chunk_registry[chunk_id as usize]
            .expect("existing chunk registration should remain current");
        assert_eq!(current.registration.id, RioBufferId(55 as _));
        assert!(registry.pending_deregistrations.is_empty());
        assert!(deregistered_ids().is_empty());
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
    fn rio_send_addr_validation_rejects_short_sockaddr_before_family_read() {
        let bytes = [0_u8; 1];
        let err = RioRegistry::validate_send_addr(bytes.as_ptr().cast(), bytes.len() as i32)
            .expect_err("short sockaddr should fail before reading sa_family");

        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_rejects_invalid_lengths_and_families() {
        // SAFETY: SOCKADDR is a plain WinSock address header and all-zero bytes are valid here.
        let mut sockaddr: SOCKADDR = unsafe { std::mem::zeroed() };
        let err = RioRegistry::validate_send_addr((&sockaddr as *const SOCKADDR).cast(), -1)
            .expect_err("negative sockaddr length should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.sa_family = AF_INET6;
        let err = RioRegistry::validate_send_addr(
            (&sockaddr as *const SOCKADDR).cast(),
            std::mem::size_of::<SOCKADDR>() as i32,
        )
        .expect_err("IPv6 sockaddr shorter than SOCKADDR_IN6 should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.sa_family = 0x7fff;
        let err = RioRegistry::validate_send_addr(
            (&sockaddr as *const SOCKADDR).cast(),
            std::mem::size_of::<SOCKADDR>() as i32,
        )
        .expect_err("unsupported sockaddr family should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_accepts_ipv4_and_ipv6_lengths() {
        // SAFETY: SOCKADDR_IN is POD; the test fills the family field explicitly.
        let mut ipv4: SOCKADDR_IN = unsafe { std::mem::zeroed() };
        ipv4.sin_family = AF_INET;
        let rio_len = RioRegistry::validate_send_addr(
            (&ipv4 as *const SOCKADDR_IN).cast(),
            std::mem::size_of::<SOCKADDR_IN>() as i32,
        )
        .expect("valid IPv4 sockaddr should pass");
        assert_eq!(rio_len, std::mem::size_of::<SOCKADDR_INET>() as u32);

        // SAFETY: SOCKADDR_IN6 is POD; the test fills the family field explicitly.
        let mut ipv6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
        ipv6.sin6_family = AF_INET6;
        let rio_len = RioRegistry::validate_send_addr(
            (&ipv6 as *const SOCKADDR_IN6).cast(),
            std::mem::size_of::<SOCKADDR_IN6>() as i32,
        )
        .expect("valid IPv6 sockaddr should pass");
        assert_eq!(rio_len, std::mem::size_of::<SOCKADDR_INET>() as u32);
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
