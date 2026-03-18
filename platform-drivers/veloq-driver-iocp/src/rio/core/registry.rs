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
use rustc_hash::FxHashMap;
use std::io;
use std::time::{Duration, Instant};
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_BUF, RIO_BUFFERID, RIO_RQ, WSAGetLastError};

pub(crate) const RIO_INVALID_BUFFERID: RIO_BUFFERID = 0 as RIO_BUFFERID;
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
    pub(crate) chunk_registry: Vec<RIO_BUFFERID>,
    /// RIO Registration for Slab Pages (for Address Buffers)
    /// Maps PageIndex -> (RIO_BUFFERID, BaseAddress, Length)
    pub(crate) slab_rio_pages: Vec<Option<(RIO_BUFFERID, usize, usize)>>,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RIO_BUFFERID
    pub(crate) heap_rio_bufs: FxHashMap<(usize, usize, u64), RIO_BUFFERID>,
    pub(crate) pending_deregistrations: Vec<RIO_BUFFERID>,
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
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> io::Result<(RIO_BUFFERID, u32)> {
        let info = buf.resolve_region_info();

        // Heap-allocated buffers use sentinel id=u16::MAX (no pre-registration).
        if info.id == u16::MAX {
            let key = (buf.as_ptr() as usize, buf.capacity(), info.cookie);
            if let Some(&id) = self.heap_rio_bufs.get(&key) {
                return Ok((id, info.offset as u32));
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
                        "RIO heap registration skipped due to recent failure: ptr=0x{:x}, cap={}, cookie={}",
                        key.0, key.1, key.2
                    ),
                ));
            }

            // Simple eviction to prevent unbounded growth of registered heap buffers.
            // Note: RIO_BUFFERIDs are a limited kernel resource.
            if self.heap_rio_bufs.len() >= 1024 {
                // We clear and deregister everything.
                // UNRESOLVED: This is only 100% safe if no heap-based IO is pended.
                // However, the cookie already prevents the dangerous "wrong buffer mapping" crash.
                for id in self.heap_rio_bufs.values().copied() {
                    unsafe { (env.dispatch.deregister_buffer)(id) };
                }
                self.heap_rio_bufs.clear();
            }

            self.registration_stats.heap_register_attempts = self
                .registration_stats
                .heap_register_attempts
                .saturating_add(1);
            let id = unsafe { (env.dispatch.register_buffer)(buf.as_ptr(), buf.capacity() as u32) };
            if id == RIO_INVALID_BUFFERID {
                self.registration_stats.heap_register_failures = self
                    .registration_stats
                    .heap_register_failures
                    .saturating_add(1);
                self.heap_register_failures_recent
                    .insert(key, Instant::now());
                let err = io_error(
                    IocpErrorContext::Rio,
                    Self::last_wsa_error(),
                    format!(
                        "RIORegisterBuffer failed for heap buffer: ptr=0x{:x}, cap={}, cookie={}",
                        key.0, key.1, key.2
                    ),
                );
                if env.registration_mode.is_strict() {
                    panic!(
                        "strict registration mode: RIO heap registration failed: ptr=0x{:x}, cap={}, cookie={}, error={}",
                        key.0, key.1, key.2, err
                    );
                }
                return Err(err);
            }

            self.heap_rio_bufs.insert(key, id);
            self.heap_register_failures_recent.remove(&key);
            self.registration_stats.heap_register_success = self
                .registration_stats
                .heap_register_success
                .saturating_add(1);
            return Ok((id, info.offset as u32));
        }

        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = env.registrar.resolve_chunk_info(info.id)
        {
            if let Err(e) = self.register_chunk(
                info.id,
                (chunk_info.ptr.as_ptr(), chunk_info.len.get()),
                env,
            ) {
                if env.registration_mode.is_strict() {
                    panic!(
                        "strict registration mode: RIO lazy chunk registration failed: chunk_id={}, error={}",
                        info.id, e
                    );
                }
                return Err(e);
            }
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        match buffer_id {
            Some(id) => Ok((id, info.offset as u32)),
            None => {
                if env.registration_mode.is_strict() {
                    panic!(
                        "strict registration mode: RIO chunk not registered and chunk info unavailable: chunk_id={}",
                        info.id
                    );
                }
                Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO chunk not registered: chunk_id={}", info.id),
                ))
            }
        }
    }

    pub(crate) fn prepare_data_submission(
        &mut self,
        buf: &FixedBuf,
        len: u32,
        env: RioEnv<'_>,
    ) -> io::Result<RIO_BUF> {
        let (buffer_id, offset) = self.resolve_buffer_id(buf, env)?;
        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset,
            Length: len,
        };
        Ok(rio_buf)
    }

    pub(crate) fn resize_registered_rqs(&mut self, _size: usize) {}

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
        let reg_fn = env.dispatch.register_buffer;
        let id_idx = id as usize;

        if id_idx >= self.chunk_registry.len() {
            self.chunk_registry.resize(id_idx + 1, RIO_INVALID_BUFFERID);
        }

        if let Some(existing) = self.chunk_registry.get(id_idx).copied()
            && existing != RIO_INVALID_BUFFERID
        {
            self.pending_deregistrations.push(existing);
        }

        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);
        let buf_id = unsafe { reg_fn(ptr, len as u32) };
        if buf_id == RIO_INVALID_BUFFERID {
            self.registration_stats.chunk_register_failures = self
                .registration_stats
                .chunk_register_failures
                .saturating_add(1);
            self.chunk_register_failures_recent
                .insert(id, Instant::now());
            let err = io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIORegisterBuffer failed: chunk_id={id}, len={len}"),
            );
            if env.registration_mode.is_strict() {
                panic!(
                    "strict registration mode: RIO chunk register syscall failed: chunk_id={}, len={}, error={}",
                    id, len, err
                );
            }
            return Err(err);
        }

        self.chunk_registry[id_idx] = buf_id;
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);
        Ok(())
    }

    pub(crate) fn ensure_slab_page_registration(
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
                let reg_fn = env.dispatch.register_buffer;
                let id = unsafe { reg_fn(ptr, len as u32) };
                if id == RIO_INVALID_BUFFERID {
                    return Err(io_error(
                        IocpErrorContext::Rio,
                        Self::last_wsa_error(),
                        format!(
                            "RIORegisterBuffer failed for slab page: page_idx={page_idx}, len={len}"
                        ),
                    ));
                }
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
    ) -> io::Result<RIO_RQ> {
        let (handle, fd) = target;
        let create_fn = env.dispatch.create_rq;

        let max_outstanding_recvs = self.rq_depth;
        let max_outstanding_sends = self.rq_depth;

        let rq = unsafe {
            create_fn(
                handle as usize,
                max_outstanding_recvs,
                1,
                max_outstanding_sends,
                1,
                env.cq,
                env.cq,
                std::ptr::null_mut(),
            )
        };

        if rq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOCreateRequestQueue failed: fd={fd:?}, handle={handle:?}, rq_depth={}",
                    self.rq_depth
                ),
            ));
        }
        Ok(rq)
    }

    pub(crate) fn deregister_heap_buffer_for_buf(&mut self, buf: &FixedBuf, env: RioEnv<'_>) {
        let info = buf.resolve_region_info();
        if info.id != u16::MAX {
            return;
        }
        let key = (buf.as_ptr() as usize, buf.capacity(), info.cookie);
        if let Some(id) = self.heap_rio_bufs.remove(&key)
            && id != RIO_INVALID_BUFFERID
        {
            unsafe { (env.dispatch.deregister_buffer)(id) };
        }
    }

    pub(crate) fn cleanup_deregister(&mut self, env: RioEnv<'_>) {
        use std::collections::HashSet;
        let mut deregistered = HashSet::new();

        for id in self.chunk_registry.iter().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (env.dispatch.deregister_buffer)(id) };
            }
        }
        for id in self.pending_deregistrations.drain(..) {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (env.dispatch.deregister_buffer)(id) };
            }
        }
        for (id, _, _) in self.slab_rio_pages.iter().flatten().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (env.dispatch.deregister_buffer)(id) };
            }
        }
        for id in self.heap_rio_bufs.values().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (env.dispatch.deregister_buffer)(id) };
            }
        }

        self.chunk_registry.clear();
        self.slab_rio_pages.clear();
        self.heap_rio_bufs.clear();
        self.chunk_register_failures_recent.clear();
        self.heap_register_failures_recent.clear();
    }
}
