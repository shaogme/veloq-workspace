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

mod addr;
mod buffer;

pub(crate) use addr::RioAddrReservation;
pub(crate) use buffer::{
    RioBufferLeaseToken, RioBufferRegistration, RioChunkRegistration, RioChunkRegistrationKey,
    RioHeapBufferKey,
};

use crate::{
    IoFd,
    config::BorrowedRawHandle,
    net::addr::SockAddrStorage,
    rio::{
        RioEnv,
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use std::{
    ptr,
    time::{Duration, Instant},
};
use veloq_buf::FixedBuf;
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

use super::{
    RioCompletedRequestContext, RioCompletionKind, RioOpRequestInit, RioPreparedRequestContext,
    RioRequestContextDecode, RioRequestContextId,
    submit_ops::{RioBufferId, RioProvider, RioRq, RioRqConfig},
};

pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);
pub(crate) const HEAP_REGISTRATION_CACHE_LIMIT: usize = 1024;

#[derive(Clone, Copy)]
pub(crate) struct RioPreparedBuffer {
    pub(crate) rio_buf: RIO_BUF,
    pub(crate) lease: Option<RioBufferLeaseToken>,
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
    pub(crate) retired_chunk_registrations:
        FxHashMap<RioChunkRegistrationKey, RioBufferRegistration>,
    pub(crate) addr_slots: Box<[SockAddrStorage]>,
    pub(crate) addr_slot_in_use: Vec<bool>,
    pub(crate) addr_free_slots: Vec<usize>,
    pub(crate) addr_buffer_id: RioBufferId,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RIO buffer registration.
    pub(crate) heap_rio_bufs: FxHashMap<RioHeapBufferKey, RioBufferRegistration>,
    pub(crate) pending_deregistrations: Vec<RioBufferId>,
    pub(crate) rq_depth: u32,
    pub(crate) registration_stats: RioRegistrationStats,
    pub(crate) chunk_register_failures_recent: FxHashMap<veloq_buf::heap::ChunkId, Instant>,
    pub(crate) heap_register_failures_recent: FxHashMap<RioHeapBufferKey, Instant>,
    pub(crate) next_registration_generation: u64,
    request_contexts: Vec<RioRequestContextSlot>,
    request_context_free: Vec<usize>,
}

#[derive(Default)]
struct RioRequestContextSlot {
    generation: u32,
    init: Option<RioOpRequestInit>,
    in_use: bool,
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
            request_contexts: Vec::new(),
            request_context_free: Vec::new(),
        }
    }

    pub(crate) fn alloc_request_context(
        &mut self,
        init: RioOpRequestInit,
    ) -> RioPreparedRequestContext {
        let index = self.request_context_free.pop().unwrap_or_else(|| {
            self.request_contexts.push(RioRequestContextSlot::default());
            self.request_contexts.len() - 1
        });
        let slot = &mut self.request_contexts[index];
        debug_assert!(!slot.in_use, "reusing active RIO request context slot");
        let mut generation = slot.generation.wrapping_add(1);
        if generation == 0 {
            generation = 1;
        }
        slot.generation = generation;
        slot.init = Some(init);
        slot.in_use = true;
        RioPreparedRequestContext::new(RioRequestContextId::new(index, generation))
    }

    pub(crate) fn take_prepared_request_init(
        &mut self,
        context: RioPreparedRequestContext,
    ) -> Option<RioOpRequestInit> {
        self.take_request_context_init(context.id())
    }

    #[cfg(test)]
    pub(crate) fn decode_request_context(&mut self, raw: u64) -> Option<RioCompletionKind> {
        match self.decode_request_context_checked(raw) {
            RioRequestContextDecode::Valid(kind) => Some(kind),
            RioRequestContextDecode::Malformed { .. }
            | RioRequestContextDecode::Missing { .. }
            | RioRequestContextDecode::Stale { .. } => None,
        }
    }

    pub(crate) fn decode_request_context_checked(&mut self, raw: u64) -> RioRequestContextDecode {
        let Some(id) = RioRequestContextId::from_raw(raw) else {
            return RioRequestContextDecode::Malformed { raw };
        };
        let Some(slot) = self.request_contexts.get_mut(id.index()) else {
            return RioRequestContextDecode::Missing { id };
        };
        if !slot.in_use {
            return RioRequestContextDecode::Missing { id };
        }
        if slot.generation != id.generation() {
            return RioRequestContextDecode::Stale {
                id,
                actual_generation: slot.generation,
            };
        }
        let Some(init) = slot.init.take() else {
            slot.in_use = false;
            self.request_context_free.push(id.index());
            return RioRequestContextDecode::Missing { id };
        };
        slot.in_use = false;
        self.request_context_free.push(id.index());
        RioRequestContextDecode::Valid(RioCompletionKind::Op {
            init,
            context: RioCompletedRequestContext::new(),
        })
    }

    fn take_request_context_init(&mut self, id: RioRequestContextId) -> Option<RioOpRequestInit> {
        let slot = self.request_contexts.get_mut(id.index())?;
        if !slot.in_use || slot.generation != id.generation() {
            return None;
        }
        let init = slot.init.take()?;
        slot.in_use = false;
        self.request_context_free.push(id.index());
        Some(init)
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

    pub(crate) fn checked_rio_buffer_offset(
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
                context: ptr::null(),
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
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::super::submit_ops::{RioCq, RioDispatch};
    use super::*;
    use crate::BufferRegistrationMode;
    use std::ffi::c_void;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use veloq_buf::NoopRegistrar;
    use windows_sys::Win32::Networking::WinSock::{
        RIO_BUFFERID, RIO_CQ, RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT,
    };

    pub(crate) static NEXT_REGISTER_ID: AtomicUsize = AtomicUsize::new(100);
    pub(crate) static REGISTER_FAILS: AtomicBool = AtomicBool::new(false);
    pub(crate) static DISPATCH_TEST_LOCK: Mutex<()> = Mutex::new(());
    pub(crate) static DEREGISTERED_IDS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    pub(crate) fn fixed_buf(capacity: usize, len: usize) -> FixedBuf {
        let mut buf = FixedBuf::alloc_heap(NonZeroUsize::new(capacity).expect("non-zero capacity"))
            .expect("heap buffer allocation failed");
        buf.set_len(len);
        buf
    }

    pub(crate) fn reset_dispatch_state() {
        NEXT_REGISTER_ID.store(100, SeqCst);
        REGISTER_FAILS.store(false, SeqCst);
        DEREGISTERED_IDS.lock().expect("deregister mutex").clear();
    }

    pub(crate) fn lock_dispatch_state() -> std::sync::MutexGuard<'static, ()> {
        DISPATCH_TEST_LOCK.lock().expect("dispatch test mutex")
    }

    pub(crate) fn deregistered_ids() -> Vec<usize> {
        DEREGISTERED_IDS.lock().expect("deregister mutex").clone()
    }

    pub(crate) unsafe extern "system" fn test_create_cq(
        _entries: u32,
        _notification: *const RIO_NOTIFICATION_COMPLETION,
    ) -> RIO_CQ {
        1 as _
    }

    pub(crate) unsafe extern "system" fn test_create_rq(
        _socket: usize,
        _max_outstanding_recvs: u32,
        _max_receive_data_buffers: u32,
        _max_outstanding_sends: u32,
        _max_send_data_buffers: u32,
        _recv_cq: RIO_CQ,
        _send_cq: RIO_CQ,
        _context: *const c_void,
    ) -> RIO_RQ {
        1 as _
    }

    pub(crate) unsafe extern "system" fn test_register_buffer(
        _ptr: *const u8,
        _len: u32,
    ) -> RIO_BUFFERID {
        if REGISTER_FAILS.swap(false, SeqCst) {
            return 0 as _;
        }
        NEXT_REGISTER_ID.fetch_add(1, SeqCst) as _
    }

    pub(crate) unsafe extern "system" fn test_deregister_buffer(id: RIO_BUFFERID) {
        DEREGISTERED_IDS
            .lock()
            .expect("deregister mutex")
            .push(id as usize);
    }

    pub(crate) unsafe extern "system" fn test_dequeue(
        _cq: RIO_CQ,
        _results: *mut RIORESULT,
        _count: u32,
    ) -> u32 {
        0
    }

    pub(crate) unsafe extern "system" fn test_notify(_cq: RIO_CQ) -> i32 {
        0
    }

    pub(crate) unsafe extern "system" fn test_close_cq(_cq: RIO_CQ) {}

    pub(crate) unsafe extern "system" fn test_receive(
        _rq: RIO_RQ,
        _buf: *const RIO_BUF,
        _num_bufs: u32,
        _flags: u32,
        _context: *const c_void,
    ) -> i32 {
        0
    }

    pub(crate) unsafe extern "system" fn test_send(
        _rq: RIO_RQ,
        _buf: *const RIO_BUF,
        _num_bufs: u32,
        _flags: u32,
        _context: *const c_void,
    ) -> i32 {
        0
    }

    pub(crate) unsafe extern "system" fn test_send_ex(
        _rq: RIO_RQ,
        _data_buf: *const RIO_BUF,
        _data_buf_count: u32,
        _local_addr: *const RIO_BUF,
        _remote_addr: *const RIO_BUF,
        _control_buf: *const RIO_BUF,
        _flags_buf: *const RIO_BUF,
        _flags: u32,
        _context: *const c_void,
    ) -> i32 {
        0
    }

    pub(crate) unsafe extern "system" fn test_receive_ex(
        _rq: RIO_RQ,
        _data_buf: *const RIO_BUF,
        _data_buf_count: u32,
        _local_addr: *const RIO_BUF,
        _remote_addr: *const RIO_BUF,
        _control_buf: *const RIO_BUF,
        _flags_buf: *const RIO_BUF,
        _flags: u32,
        _context: *const c_void,
    ) -> i32 {
        0
    }

    pub(crate) fn test_dispatch() -> RioDispatch {
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

    pub(crate) fn test_env(dispatch: &RioDispatch) -> RioEnv<'_> {
        RioEnv {
            registrar: &NoopRegistrar,
            dispatch,
            cq: RioCq::INVALID,
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::fixed_buf;
    use super::*;

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
