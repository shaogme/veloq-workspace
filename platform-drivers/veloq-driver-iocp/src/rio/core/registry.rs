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
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::rio::RioEnv;
use crate::rio::core::submit_ops::{RioBufferId, RioProvider, RioRq, RioRqConfig};
use rustc_hash::FxHashMap;
use std::io;
use std::time::{Duration, Instant};
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_BUF, WSAGetLastError};

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

    fn last_wsa_error() -> io::Error {
        // SAFETY: WSAGetLastError is a simple getter with no side effects.
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> io::Result<(RioBufferId, u32)> {
        let info = buf.resolve_region_info();

        if info.id == u16::MAX {
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
            None => Err(io_msg(
                IocpErrorContext::Rio,
                format!("RIO chunk not registered: chunk_id={}", info.id),
            )),
        }
    }

    pub(crate) fn prepare_submission(
        &mut self,
        buf: &FixedBuf,
        buf_offset: usize,
        len: u32,
        env: RioEnv<'_>,
    ) -> io::Result<RIO_BUF> {
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
    ) -> io::Result<()> {
        if let Some(last_fail) = self.chunk_register_failures_recent.get(&id)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats
                .chunk_register_skipped_recent_failure = self
                .registration_stats
                .chunk_register_skipped_recent_failure
                .saturating_add(1);
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!("RIO chunk registration skipped due to recent failure: chunk_id={id}"),
            ));
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
            Err(e) => return Err(self.on_chunk_reg_fail(id, len, e)),
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
    ) -> io::Result<()> {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = resolver(page_idx) {
                let id = match env.dispatch.register_buffer(ptr, len as u32) {
                    Ok(id) => id,
                    Err(e) => {
                        return Err(io_error(
                            IocpErrorContext::Rio,
                            Self::last_wsa_error(),
                            format!(
                                "RIORegisterBuffer failed for slab page: page_idx={page_idx}, len={len}, original_error={e}"
                            ),
                        ));
                    }
                };
                self.slab_rio_pages[page_idx] = Some((id, ptr as usize, len));
            } else {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO slab page not found in registry: page_idx={page_idx}"),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn create_rq(
        &mut self,
        target: (HANDLE, IoFd),
        env: RioEnv<'_>,
    ) -> io::Result<RioRq> {
        let (handle, fd) = target;

        let max_outstanding_recvs = self.rq_depth;
        let max_outstanding_sends = self.rq_depth;

        env.dispatch.create_rq(RioRqConfig {
            socket: handle as usize,
            max_outstanding_recvs,
            max_receive_data_buffers: 1,
            max_outstanding_sends,
            max_send_data_buffers: 1,
            recv_cq: env.cq,
            send_cq: env.cq,
            context: std::ptr::null(),
        }).map_err(|e| {
            io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOCreateRequestQueue failed: fd={fd:?}, handle={handle:?}, rq_depth={}, original_error={e}",
                    self.rq_depth
                ),
            )
        })
    }

    pub(crate) fn deregister_heap_buf(&mut self, buf: &FixedBuf, _env: RioEnv<'_>) {
        let info = buf.resolve_region_info();
        if info.id != u16::MAX {
            return;
        }
        let key = (buf.as_ptr() as usize, buf.capacity(), info.cookie);
        if let Some(id) = self.heap_rio_bufs.remove(&key)
            && !id.is_invalid()
        {
            self.pending_deregistrations.push(id);
        }
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
    ) -> io::Result<(RioBufferId, u32)> {
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
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO heap registration skipped due to recent failure (mode={:?}): ptr=0x{:x}, cap={}, cookie={}",
                    env.registration_mode, key.0, key.1, key.2
                ),
            ));
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
    ) -> io::Result<RioBufferId> {
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
                return Err(io_error(
                    IocpErrorContext::Rio,
                    Self::last_wsa_error(),
                    format!(
                        "RIORegisterBuffer failed for heap buffer (mode={:?}): ptr=0x{:x}, cap={}, cookie={}, original_error={e}",
                        env.registration_mode, key.0, key.1, key.2
                    ),
                ));
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

    fn on_chunk_reg_fail(&mut self, id: u16, len: usize, e: std::io::Error) -> std::io::Error {
        self.registration_stats.chunk_register_failures = self
            .registration_stats
            .chunk_register_failures
            .saturating_add(1);
        self.chunk_register_failures_recent
            .insert(id, Instant::now());
        io_error(
            IocpErrorContext::Rio,
            Self::last_wsa_error(),
            format!("RIORegisterBuffer failed: chunk_id={id}, len={len}, original_error={e}"),
        )
    }
}
