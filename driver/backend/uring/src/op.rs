//! io_uring Platform-Specific Operation Definitions

use crate::config::UringRawHandle;
use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::{OwnedRawHandle, RawHandle};
use io_uring::squeue;
use std::marker::PhantomData;
use std::time::Duration;
use veloq_driver_core::driver::CompletionCleanupGuard;
use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::driver::SubmitTokenContext;
use veloq_driver_core::op::{IntoPlatformOp, OpCompletion, OpKind};

mod payload;
pub(crate) mod slot;
mod spec;
mod submit;

use spec::{UringOpErasure, UringOpSpec};

pub(crate) use payload::UringOpPayload;
pub(crate) use payload::{
    Accept, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, Open, ReadFixed,
    ReadRaw, Recv, SendTo, SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecv,
    UdpRecvFrom, UdpSend, Wakeup, WriteFixed, WriteRaw,
};

// ============================================================================
// VTable Definition
// ============================================================================

pub(crate) type MakeSqeFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    driver: &mut UringDriver,
    token: SubmitTokenContext,
) -> DriverResult<squeue::Entry>;
pub(crate) type OnCompleteFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize>;
pub(crate) type CompletionCleanupFn =
    unsafe fn(op: &mut UringKernelOp, result: i32) -> CompletionCleanupGuard;
pub(crate) type GetTimeoutFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload) -> Option<Duration>;
pub(crate) type ResolveChunksFn = unsafe fn(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    chunks: &mut [veloq_buf::heap::ChunkId],
) -> usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
    /// Only for background operations (e.g. Close)
    BackgroundOnly,
}

pub(crate) struct OpVTable {
    pub(crate) make_sqe: MakeSqeFn,
    pub(crate) on_complete: OnCompleteFn,
    pub(crate) completion_cleanup: CompletionCleanupFn,
    pub(crate) strategy: SubmissionStrategy,
    pub(crate) get_timeout: GetTimeoutFn,
    pub(crate) resolve_chunks: ResolveChunksFn,
}

// ============================================================================
// UringKernelOp Struct & Payload (Type-Erased)
// ============================================================================

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: &'static OpVTable,

    /// Type-erased payload (kernel-side data)
    pub(crate) payload: UringOpPayload,
}

impl PlatformOp for UringKernelOp {}

pub type UringOp = UringKernelOp;

// ============================================================================
// User Payload & Erasure Glue
// ============================================================================

pub enum UringUserPayload {
    ReadFixed(ReadFixed),
    ReadRaw(ReadRaw),
    WriteFixed(WriteFixed),
    WriteRaw(WriteRaw),
    Recv(Recv),
    OpSend(OpSend),
    UdpRecv(UdpRecv),
    UdpSend(UdpSend),
    Connect(Connect),
    UdpConnect(UdpConnect),
    Close(Close),
    Fsync(Fsync),
    FsyncRaw(FsyncRaw),
    SyncFileRange(SyncFileRange),
    SyncFileRangeRaw(SyncFileRangeRaw),
    Fallocate(Fallocate),
    FallocateRaw(FallocateRaw),
    Accept(Accept),
    SendTo(SendTo),
    UdpRecvFrom(UdpRecvFrom),
    Open(Open),
    Wakeup(Wakeup),
    Timeout(Timeout),
}

unsafe impl Send for UringUserPayload {}

fn kernel_ref<T>(_user: &T) -> payload::KernelRef<T> {
    payload::KernelRef {
        marker: PhantomData,
    }
}

fn zeroed_sockaddr_storage() -> libc::sockaddr_storage {
    // C socket storage is intentionally zero-initialized before make_sqe fills it.
    unsafe { std::mem::zeroed() }
}

fn zeroed_msghdr() -> libc::msghdr {
    // msghdr pointer fields are populated immediately before submission.
    unsafe { std::mem::zeroed() }
}

macro_rules! impl_uring_op_erasure {
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl UringOpErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> UringOpPayload {
                UringOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(payload: &UringOpPayload) -> Option<&Self::KernelPayload> {
                match payload {
                    UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(
                payload: &mut UringOpPayload,
            ) -> Option<&mut Self::KernelPayload> {
                match payload {
                    UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> UringUserPayload {
                UringUserPayload::$user_variant(payload)
            }

            fn try_user_payload(payload: UringUserPayload) -> DriverResult<Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(veloq_driver_core::op::payload_projection_mismatch_report::<
                        UringError,
                    >(stringify!($OpType), "UringUserPayload")),
                }
            }

            fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn user_payload_mut(payload: &mut UringUserPayload) -> Option<&mut Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static OpVTable {
                static TABLE: OpVTable = OpVTable {
                    make_sqe: spec::make_sqe_shim::<$OpType>,
                    on_complete: spec::on_complete_shim::<$OpType>,
                    completion_cleanup: spec::completion_cleanup_shim::<$OpType>,
                    strategy: <$OpType as UringOpSpec>::STRATEGY,
                    get_timeout: spec::get_timeout_shim::<$OpType>,
                    resolve_chunks: spec::resolve_chunks_shim::<$OpType>,
                };
                &TABLE
            }
        }

        impl IntoPlatformOp<UringOp> for $OpType {
            type UserPayload = $OpType;
            type ErasedPayload = UringUserPayload;
            type Output = $OpType;
            type Completion = $completion;
            type DriverCompletion = usize;
            type Error = UringError;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::UserPayload) {
                let kernel_payload = <$OpType as UringOpSpec>::new_kernel_payload(&self);
                let op = UringKernelOp {
                    vtable: <$OpType as UringOpErasure>::vtable(),
                    payload: <$OpType as UringOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::UserPayload) -> UringUserPayload {
                <$OpType as UringOpErasure>::erase_user_payload(payload)
            }

            fn try_payload_from_erased(
                payload: UringUserPayload,
            ) -> DriverResult<Self::UserPayload> {
                <$OpType as UringOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::UserPayload,
                res: DriverResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let completion = <$OpType as UringOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }
    };
}

// ============================================================================
// Op Specs
// ============================================================================

impl UringOpSpec for ReadFixed {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_read_fixed(kernel, payload, driver, token) }
    }

    fn resolve_chunks(
        kernel: &Self::KernelPayload,
        payload: &Self,
        chunks: &mut [veloq_buf::heap::ChunkId],
    ) -> usize {
        submit::resolve_chunks_read_fixed(kernel, payload, chunks)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for ReadRaw {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_read_raw(kernel, payload, driver, token) }
    }

    fn resolve_chunks(
        kernel: &Self::KernelPayload,
        payload: &Self,
        chunks: &mut [veloq_buf::heap::ChunkId],
    ) -> usize {
        submit::resolve_chunks_read_raw(kernel, payload, chunks)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for WriteFixed {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_write_fixed(kernel, payload, driver, token) }
    }

    fn resolve_chunks(
        kernel: &Self::KernelPayload,
        payload: &Self,
        chunks: &mut [veloq_buf::heap::ChunkId],
    ) -> usize {
        submit::resolve_chunks_write_fixed(kernel, payload, chunks)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for WriteRaw {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_write_raw(kernel, payload, driver, token) }
    }

    fn resolve_chunks(
        kernel: &Self::KernelPayload,
        payload: &Self,
        chunks: &mut [veloq_buf::heap::ChunkId],
    ) -> usize {
        submit::resolve_chunks_write_raw(kernel, payload, chunks)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Recv {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Recv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_recv(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for OpSend {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Send;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_send(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpRecv {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_recv(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpSend {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpSend;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_send(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Connect {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Connect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_connect(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpConnect {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpConnect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_connect(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Close {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Close;
    const STRATEGY: SubmissionStrategy = SubmissionStrategy::BackgroundOnly;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_close(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Fsync {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_fsync(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for FsyncRaw {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_fsync_raw(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for SyncFileRange {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_sync_range(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for SyncFileRangeRaw {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_sync_range_raw(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Fallocate {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_fallocate(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for FallocateRaw {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_fallocate_raw(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Accept {
    type KernelPayload = payload::AcceptPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Accept;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::AcceptPayload {}
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_accept(kernel, payload, driver, token) }
    }

    unsafe fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> DriverResult<usize> {
        unsafe { submit::on_complete_accept(kernel, payload, result) }
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(raw as i32)))
        })
    }
}

impl UringOpSpec for SendTo {
    type KernelPayload = payload::SendToPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SendTo;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::SendToPayload {
            msg_name: zeroed_sockaddr_storage(),
            msg_namelen: 0,
            iovec: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
        }
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_send_to(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpRecvFrom {
    type KernelPayload = payload::UdpRecvFromPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecvFrom;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::UdpRecvFromPayload {
            msg_name: zeroed_sockaddr_storage(),
            iovec: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
        }
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_recv_from(kernel, payload, driver, token) }
    }

    unsafe fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> DriverResult<usize> {
        unsafe { submit::on_complete_udp_recv_from(kernel, payload, result) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Open {
    type KernelPayload = payload::OpenPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Open;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::OpenPayload {}
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_open(kernel, payload, driver, token) }
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw as i32)))
        })
    }
}

impl UringOpSpec for Wakeup {
    type KernelPayload = payload::WakeupPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::WakeupPayload { buf: [0; 8] }
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_wakeup(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Timeout {
    type KernelPayload = payload::TimeoutPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Timeout;
    const STRATEGY: SubmissionStrategy = SubmissionStrategy::SoftwareTimer;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::TimeoutPayload { ts: [0; 2] }
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_timeout(kernel, payload, driver, token) }
    }

    fn get_timeout(_kernel: &Self::KernelPayload, payload: &Self) -> Option<Duration> {
        Some(payload.duration)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl_uring_op_erasure!(ReadFixed, ReadFixed, Read, usize);
impl_uring_op_erasure!(ReadRaw, ReadRaw, ReadRaw, usize);
impl_uring_op_erasure!(WriteFixed, WriteFixed, Write, usize);
impl_uring_op_erasure!(WriteRaw, WriteRaw, WriteRaw, usize);
impl_uring_op_erasure!(Recv, Recv, Recv, usize);
impl_uring_op_erasure!(OpSend, OpSend, Send, usize);
impl_uring_op_erasure!(UdpRecv, UdpRecv, UdpRecv, usize);
impl_uring_op_erasure!(UdpSend, UdpSend, UdpSend, usize);
impl_uring_op_erasure!(Connect, Connect, Connect, usize);
impl_uring_op_erasure!(UdpConnect, UdpConnect, UdpConnect, usize);
impl_uring_op_erasure!(Close, Close, Close, usize);
impl_uring_op_erasure!(Fsync, Fsync, Fsync, usize);
impl_uring_op_erasure!(FsyncRaw, FsyncRaw, FsyncRaw, usize);
impl_uring_op_erasure!(SyncFileRange, SyncFileRange, SyncRange, usize);
impl_uring_op_erasure!(SyncFileRangeRaw, SyncFileRangeRaw, SyncRangeRaw, usize);
impl_uring_op_erasure!(Fallocate, Fallocate, Fallocate, usize);
impl_uring_op_erasure!(FallocateRaw, FallocateRaw, FallocateRaw, usize);
impl_uring_op_erasure!(Accept, Accept, Accept, OwnedRawHandle);
impl_uring_op_erasure!(SendTo, SendTo, SendTo, usize);
impl_uring_op_erasure!(UdpRecvFrom, UdpRecvFrom, UdpRecvFrom, usize);
impl_uring_op_erasure!(Open, Open, Open, OwnedRawHandle);
impl_uring_op_erasure!(Wakeup, Wakeup, Wakeup, usize);
impl_uring_op_erasure!(Timeout, Timeout, Timeout, usize);
