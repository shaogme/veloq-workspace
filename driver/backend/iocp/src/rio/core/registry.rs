//! Buffer and request-queue registration state for the RIO backend.
//!
//! `RioRegistry` owns all registration metadata required to submit operations:
//! - chunk registrations for pre-registered slab regions,
//! - lazy heap-buffer registrations with bounded cache behavior,
//! - slab-page registrations for address buffers used by `RIOSendEx`,
//! - deferred deregistration queues for safe teardown sequencing.
//!
//! This module is focused on resource identity and lifetime bookkeeping; it
//! intentionally avoids actor scheduling or completion routing policy.

use crate::IoFd;
use crate::config::BorrowedRawHandle;
use crate::rio::RioEnv;
use crate::rio::core::submit_ops::{RioBufferId, RioProvider, RioRq, RioRqConfig};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use std::time::{Duration, Instant};
use veloq_buf::{FixedBuf, PoolKind};
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

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
    /// RIO Registration for Slab Pages (for Address Buffers)
    /// Maps PageIndex -> (RioBufferId, BaseAddress, Length)
    pub(crate) slab_rio_pages: Vec<Option<(RioBufferId, usize, usize)>>,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RioBufferId
    pub(crate) heap_rio_bufs: FxHashMap<(usize, usize, u64), RioBufferId>,
    pub(crate) pending_deregistrations: Vec<RioBufferId>,
    pub(crate) rq_depth: u32,
    pub(crate) registration_stats: RioRegistrationStats,
    chunk_register_failures_recent: FxHashMap<u16, Instant>,
    heap_register_failures_recent: FxHashMap<(usize, usize, u64), Instant>,
}

impl RioRegistry {
    pub(crate) fn new(rq_depth: u32) -> Self {
        Self {
            chunk_registry: Vec::new(),
            slab_rio_pages: Vec::new(),
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
    ) -> RioResult<(RioBufferId, u32)> {
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
            Some(id) => Ok((id, info.offset as u32)),
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
        let (buffer_id, offset) = self.resolve_buffer_id(buf, env)?;
        let rio_buf = RIO_BUF {
            BufferId: buffer_id.0,
            Offset: offset + buf_offset as u32,
            Length: len,
        };
        Ok(rio_buf)
    }

    pub(crate) fn resize_rqs(&mut self, _size: usize) {}

    pub(crate) fn clear_registered_rq(&mut self, _idx: usize) {}

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

    pub(crate) fn ensure_page_reg(
        &mut self,
        page_idx: usize,
        resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = resolver(page_idx) {
                let id = env
                    .dispatch
                    .register_buffer(ptr, len as u32)
                    .with_ctx("page_idx", page_idx)
                    .with_ctx("buffer_length", len)
                    .attach_note("RIORegisterBuffer failed for slab page")?;
                self.slab_rio_pages[page_idx] = Some((id, ptr as usize, len));
            } else {
                return RioError::Internal
                    .with_ctx("page_idx", page_idx)
                    .attach_note("RIO slab page not found in registry");
            }
        }
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
        for (id, _, _) in self.slab_rio_pages.iter().flatten().copied() {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }
        for id in self.heap_rio_bufs.values().copied() {
            if !id.is_invalid() && deregistered.insert(id.0 as usize) {
                env.dispatch.deregister_buffer(id);
            }
        }

        self.chunk_registry.clear();
        self.slab_rio_pages.clear();
        self.heap_rio_bufs.clear();
        self.chunk_register_failures_recent.clear();
        self.heap_register_failures_recent.clear();
    }

    fn resolve_heap_id(
        &mut self,
        buf: &FixedBuf,
        offset: usize,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, u32)> {
        let key = (
            buf.as_ptr() as usize,
            buf.capacity(),
            buf.resolve_region_info().cookie,
        );
        if let Some(&id) = self.heap_rio_bufs.get(&key) {
            return Ok((id, offset as u32));
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
        Ok((id, offset as u32))
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
}
