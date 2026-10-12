//! Buffer and request-queue registration state for RIO. / RIO 缓冲区与请求队列注册状态。
//!
//! `RioRegistry` owns the metadata needed to submit operations and release resources safely.
//! `RioRegistry` 持有提交操作及安全释放资源所需的元数据。
//!
//! Resource identity and lifetime stay here; actor scheduling and completion routing live elsewhere.
//! 本模块管理资源身份与生命周期；actor 调度和完成路由由其他模块负责。

mod addr;
mod buffer;

use self::{addr::RioAddressRegistry, buffer::RioBufferRegistry};
pub(crate) use addr::RioAddrReservation;
pub(crate) use buffer::{ResolvedRioBuffer, RioBufferLeaseToken};

#[cfg(test)]
pub(super) use test_helpers::RioRegistryTestSupport;

use crate::{
    IoFd,
    config::BorrowedRawHandle,
    net::addr::SockAddrStorage,
    rio::{
        core::RioEnv,
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_buf::{FixedBuf, heap::ChunkId};
use veloq_driver_core::slot::Generation;
use veloq_std::{
    boxed::Box, collections::HashSet, ptr, string::ToString, time::Duration, vec::Vec,
};
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

use super::{
    RioCompletedRequestContext, RioCompletionKind, RioOpRequestInit, RioPreparedRequestContext,
    RioRequestContextDecode, RioRequestContextId,
    submit_ops::{RioProvider, RioRq, RioRqConfig},
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
    addresses: RioAddressRegistry,
    buffers: RioBufferRegistry,
    rq_depth: u32,
    request_contexts: Vec<RioRequestContextSlot>,
    request_context_free: Vec<usize>,
}

#[derive(Default)]
struct RioRequestContextSlot {
    generation: Generation,
    init: Option<RioOpRequestInit>,
    in_use: bool,
}

impl RioRegistry {
    pub(crate) fn new(rq_depth: u32, addr_capacity: usize) -> Self {
        Self {
            addresses: RioAddressRegistry::new(addr_capacity),
            buffers: RioBufferRegistry::new(),
            rq_depth,
            request_contexts: Vec::new(),
            request_context_free: Vec::new(),
        }
    }

    pub(crate) fn prepare_send_addr(
        &mut self,
        addr: &SockAddrStorage,
        addr_len: i32,
        env: RioEnv<'_>,
    ) -> RioResult<RioAddrReservation> {
        self.addresses.prepare_send_addr(addr, addr_len, env)
    }

    pub(crate) fn prepare_recv_addr(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        self.addresses.prepare_recv_addr(env)
    }

    pub(crate) fn copy_addr_slot_to(
        &self,
        reservation: RioAddrReservation,
        dst: &mut SockAddrStorage,
    ) -> RioResult<()> {
        self.addresses.copy_addr_slot_to(reservation, dst)
    }

    pub(crate) fn free_addr_reservation(
        &mut self,
        reservation: Option<RioAddrReservation>,
    ) -> RioResult<()> {
        self.addresses.free_addr_reservation(reservation)
    }

    #[cfg(test)]
    pub(crate) fn validate_send_addr(addr: &SockAddrStorage, addr_len: i32) -> RioResult<u32> {
        RioAddressRegistry::validate_send_addr(addr, addr_len)
    }

    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> RioResult<ResolvedRioBuffer> {
        self.buffers.resolve_buffer_id(buf, env)
    }

    pub(crate) fn register_buffer_backend(
        &mut self,
        id: ChunkId,
        mem: (*const u8, usize),
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        self.buffers.register_buffer_backend(id, mem, env)
    }

    pub(crate) fn acquire_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> RioResult<()> {
        self.buffers.acquire_buffer_lease(lease)
    }

    pub(crate) fn release_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        self.buffers.release_buffer_lease(lease, env)
    }

    pub(crate) fn release_buffer_lease_deferred(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> RioResult<()> {
        self.buffers.release_buffer_lease_deferred(lease)
    }

    pub(crate) const fn rq_depth(&self) -> u32 {
        self.rq_depth
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn chunk_register_attempts(&self) -> u64 {
        self.buffers.chunk_register_attempts()
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
        // Zero is the unallocated sentinel, so wraparound skips it. / 0 表示未分配，回绕时跳过该哨兵。
        let mut generation = slot.generation.next();
        if generation == Generation::ZERO {
            generation = generation.next();
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
            RioRequestContextDecode::Valid(kind) => Some(*kind),
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
        RioRequestContextDecode::Valid(Box::new(RioCompletionKind::Op {
            init,
            context: RioCompletedRequestContext::new(),
        }))
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
        let resolved = self.resolve_buffer_id(buf, env)?;
        let offset = Self::checked_rio_buffer_offset(resolved.offset, buf_offset_u32, len, buf)?;
        let rio_buf = RIO_BUF {
            BufferId: resolved.id.raw(),
            Offset: offset,
            Length: len,
        };
        Ok(RioPreparedBuffer {
            rio_buf,
            lease: resolved.lease,
        })
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
            .with_ctx("fd", fd.to_string())
            .with_ctx("socket_raw", handle.raw().as_handle() as usize)
            .with_ctx("rq_depth", self.rq_depth)
            .with_ctx("max_outstanding_recvs", max_outstanding_recvs)
            .with_ctx("max_outstanding_sends", max_outstanding_sends)
            .with_ctx("max_receive_data_buffers", 1_u32)
            .with_ctx("max_send_data_buffers", 1_u32)
            .attach_note("RIOCreateRequestQueue failed")
    }

    pub(crate) fn flush_deregs(&mut self, env: RioEnv<'_>) {
        self.buffers.flush_deregs(env);
    }

    pub(crate) fn cleanup_deregister(&mut self, env: RioEnv<'_>) {
        let active_addr_slots = self.addresses.active_slot_count();
        if active_addr_slots != 0
            || self.request_contexts.iter().any(|slot| slot.in_use)
            || self.has_active_buffer_leases()
        {
            tracing::error!(
                active_addr_slots,
                active_request_contexts = self
                    .request_contexts
                    .iter()
                    .filter(|slot| slot.in_use)
                    .count(),
                "refusing to deregister RIO buffers while leases or request contexts are active"
            );
            return;
        }
        let mut deregistered = HashSet::default();
        self.buffers.deregister_all(env, &mut deregistered);
        self.addresses.deregister_buffer(env, &mut deregistered);
        self.addresses.reset_addr_slots();
    }

    fn has_active_buffer_leases(&self) -> bool {
        self.buffers.has_active_leases()
    }

    #[cfg(test)]
    pub(super) fn addr_free_slot_count(&self) -> usize {
        self.addresses.free_slot_count()
    }

    #[cfg(test)]
    pub(super) fn addr_slot_in_use(&self, slot: usize) -> bool {
        self.addresses.slot_in_use(slot)
    }

    #[cfg(test)]
    pub(super) fn heap_registration_count(&self) -> usize {
        self.buffers.heap_registration_count()
    }
}

#[cfg(test)]
mod test_helpers {
    use super::super::submit_ops::{RioCq, RioDispatch, RioDispatchFunctions};
    use crate::BufferRegistrationMode;
    use crate::rio::RioEnv;
    use veloq_buf::{FixedBuf, NoopRegistrar};
    use veloq_std::{
        ffi::c_void,
        num::NonZeroUsize,
        sync::{
            UnpoisonedMutex, UnpoisonedMutexGuard,
            atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        },
        vec::Vec,
    };
    use windows_sys::Win32::Networking::WinSock::{
        RIO_BUF, RIO_BUFFERID, RIO_CQ, RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT,
    };

    static NEXT_REGISTER_ID: AtomicUsize = AtomicUsize::new(100);
    static REGISTER_FAILS: AtomicBool = AtomicBool::new(false);
    static RECEIVE_EX_CALLS: AtomicUsize = AtomicUsize::new(0);
    static RECEIVE_EX_DATA_COUNT: AtomicUsize = AtomicUsize::new(0);
    static RECEIVE_EX_REMOTE_ADDR: AtomicUsize = AtomicUsize::new(0);
    static RECEIVE_EX_FLAGS: AtomicUsize = AtomicUsize::new(0);
    static DISPATCH_TEST_LOCK: UnpoisonedMutex<()> = UnpoisonedMutex::new(());
    static DEREGISTERED_IDS: UnpoisonedMutex<Vec<usize>> = UnpoisonedMutex::new(Vec::new());

    pub(crate) struct RioRegistryTestSupport;

    impl RioRegistryTestSupport {
        pub(crate) fn fixed_buf(capacity: usize, len: usize) -> FixedBuf {
            FixedBuf::alloc_heap(NonZeroUsize::new(capacity).expect("non-zero capacity"), len)
                .expect("heap buffer allocation failed")
        }

        pub(crate) fn reset_dispatch_state() {
            NEXT_REGISTER_ID.store(100, SeqCst);
            REGISTER_FAILS.store(false, SeqCst);
            RECEIVE_EX_CALLS.store(0, SeqCst);
            RECEIVE_EX_DATA_COUNT.store(0, SeqCst);
            RECEIVE_EX_REMOTE_ADDR.store(0, SeqCst);
            RECEIVE_EX_FLAGS.store(0, SeqCst);
            DEREGISTERED_IDS.lock().clear();
        }

        pub(crate) fn lock_dispatch_state() -> UnpoisonedMutexGuard<'static, ()> {
            DISPATCH_TEST_LOCK.lock()
        }

        pub(crate) fn deregistered_ids() -> Vec<usize> {
            DEREGISTERED_IDS.lock().clone()
        }

        pub(crate) fn receive_ex_calls() -> usize {
            RECEIVE_EX_CALLS.load(SeqCst)
        }

        pub(crate) fn fail_next_register() {
            REGISTER_FAILS.store(true, SeqCst);
        }

        pub(crate) fn receive_ex_data_count() -> usize {
            RECEIVE_EX_DATA_COUNT.load(SeqCst)
        }

        pub(crate) fn receive_ex_remote_addr() -> usize {
            RECEIVE_EX_REMOTE_ADDR.load(SeqCst)
        }

        pub(crate) fn receive_ex_flags() -> usize {
            RECEIVE_EX_FLAGS.load(SeqCst)
        }

        pub(crate) fn next_register_id() -> usize {
            NEXT_REGISTER_ID.load(SeqCst)
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
            DEREGISTERED_IDS.lock().push(id as usize);
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
            RECEIVE_EX_CALLS.fetch_add(1, SeqCst);
            RECEIVE_EX_DATA_COUNT.store(_data_buf_count as usize, SeqCst);
            RECEIVE_EX_REMOTE_ADDR.store((!_remote_addr.is_null()) as usize, SeqCst);
            RECEIVE_EX_FLAGS.store(_flags as usize, SeqCst);
            1
        }

        pub(crate) fn test_dispatch() -> RioDispatch {
            RioDispatch::from_functions(RioDispatchFunctions {
                create_cq: Self::test_create_cq,
                create_rq: Self::test_create_rq,
                register_buffer: Self::test_register_buffer,
                deregister_buffer: Self::test_deregister_buffer,
                dequeue: Self::test_dequeue,
                notify: Self::test_notify,
                close_cq: Self::test_close_cq,
                receive: Self::test_receive,
                send: Self::test_send,
                send_ex: Self::test_send_ex,
                receive_ex: Self::test_receive_ex,
            })
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
}

#[cfg(test)]
mod tests {
    use super::super::submit_ops::RioSubmissionKind;
    use super::{RioError, RioRegistry, test_helpers::RioRegistryTestSupport};

    struct SubmissionCase {
        kind: RioSubmissionKind,
        operation: &'static str,
        offset: usize,
    }

    #[test]
    fn rio_submission_len_allows_exact_boundaries() {
        let buf = RioRegistryTestSupport::fixed_buf(8, 4);

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
        let buf = RioRegistryTestSupport::fixed_buf(8, 4);
        let cases = [
            SubmissionCase {
                kind: RioSubmissionKind::Recv,
                operation: "recv",
                offset: buf.capacity() + 1,
            },
            SubmissionCase {
                kind: RioSubmissionKind::Send,
                operation: "send",
                offset: buf.len() + 1,
            },
            SubmissionCase {
                kind: RioSubmissionKind::Send,
                operation: "udp_send",
                offset: buf.len() + 1,
            },
            SubmissionCase {
                kind: RioSubmissionKind::Send,
                operation: "send_to",
                offset: buf.len() + 1,
            },
            SubmissionCase {
                kind: RioSubmissionKind::Recv,
                operation: "udp_recv_multi",
                offset: buf.capacity() + 1,
            },
        ];

        for case in cases {
            let err = case
                .kind
                .data_len(&buf, case.offset, case.operation)
                .expect_err("out-of-bounds RIO offset should fail");
            assert_eq!(*err.inner(), RioError::InvalidInput);
        }
    }

    #[test]
    fn rio_buffer_offset_rejects_u32_overflow() {
        let buf = RioRegistryTestSupport::fixed_buf(8, 4);

        let err = RioRegistry::checked_rio_buffer_offset((u32::MAX as usize) + 1, 0, 0, &buf)
            .expect_err("base offset above u32 should fail");
        assert_eq!(*err.inner(), RioError::ResourceExhaustion);

        let err = RioRegistry::checked_rio_buffer_offset(u32::MAX as usize, 1, 0, &buf)
            .expect_err("combined RIO offset should overflow");
        assert_eq!(*err.inner(), RioError::ResourceExhaustion);
    }
}
