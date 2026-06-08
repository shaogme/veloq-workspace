mod file;
mod net;

use crate::OwnedRawHandle;
use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{
    Accept, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, OpVTable, Open,
    ReadFixed, ReadRaw, Recv, SendTo, SubmissionStrategy, SyncFileRange, SyncFileRangeRaw, Timeout,
    UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, UringKernelOp, UringOpPayload, UringUserPayload,
    Wakeup, WriteFixed, WriteRaw, payload, submit,
};
use io_uring::squeue;
use std::time::Duration;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{CompletionCleanupGuard, SubmitTokenContext};
use veloq_driver_core::op::OpKind;

pub(crate) trait UringOpSpec: Sized + Send + 'static {
    type KernelPayload;
    type Completion;

    const PAYLOAD_KIND: OpKind;
    const STRATEGY: SubmissionStrategy = SubmissionStrategy::SubmitSqe;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload;

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry>;

    unsafe fn on_complete(
        _kernel: &mut Self::KernelPayload,
        _payload: &mut Self,
        result: i32,
    ) -> DriverResult<usize> {
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(UringError::CompletionWait
                .report(
                    "uring.op.spec.on_complete_default",
                    "kernel completion returned error",
                )
                .set_error_code(-result))
        }
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        _result: i32,
    ) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }

    fn get_timeout(_kernel: &Self::KernelPayload, _payload: &Self) -> Option<Duration> {
        None
    }

    fn resolve_chunks(
        _kernel: &Self::KernelPayload,
        _payload: &Self,
        _chunks: &mut [ChunkId],
    ) -> usize {
        0
    }

    fn map_completion(payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion>;
}

pub(crate) trait UringOpErasure: UringOpSpec {
    fn erase_kernel_payload(payload: Self::KernelPayload) -> UringOpPayload;
    fn kernel_payload_ref(payload: &UringOpPayload) -> Option<&Self::KernelPayload>;
    fn kernel_payload_mut(payload: &mut UringOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self) -> UringUserPayload;
    fn try_user_payload(payload: UringUserPayload) -> DriverResult<Self>;
    fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self>;
    fn user_payload_mut(payload: &mut UringUserPayload) -> Option<&mut Self>;

    fn vtable() -> &'static OpVTable;
}

pub(crate) unsafe fn make_sqe_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    driver: &mut UringDriver,
    token: SubmitTokenContext,
) -> DriverResult<squeue::Entry>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.make_sqe", "kernel payload mismatch")
    })?;
    let user = S::user_payload_mut(payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.make_sqe", "user payload mismatch")
    })?;
    unsafe { S::make_sqe(kernel, user, driver, token) }
}

pub(crate) unsafe fn on_complete_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.on_complete", "kernel payload mismatch")
    })?;
    let user = S::user_payload_mut(payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.on_complete", "user payload mismatch")
    })?;
    unsafe { S::on_complete(kernel, user, result) }
}

pub(crate) unsafe fn completion_cleanup_shim<S>(
    op: &mut UringKernelOp,
    result: i32,
) -> CompletionCleanupGuard
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_mut(&mut op.payload) else {
        return CompletionCleanupGuard::default();
    };
    S::completion_cleanup(kernel, result)
}

pub(crate) unsafe fn get_timeout_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
) -> Option<Duration>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_ref(&op.payload)?;
    let user = S::user_payload_ref(payload)?;
    S::get_timeout(kernel, user)
}

pub(crate) unsafe fn resolve_chunks_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_ref(&op.payload) else {
        return 0;
    };
    let Some(user) = S::user_payload_ref(payload) else {
        return 0;
    };
    S::resolve_chunks(kernel, user, chunks)
}

macro_rules! impl_uring_op_erasure {
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl crate::op::spec::UringOpErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> crate::op::UringOpPayload {
                crate::op::UringOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(
                payload: &crate::op::UringOpPayload,
            ) -> Option<&Self::KernelPayload> {
                match payload {
                    crate::op::UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(
                payload: &mut crate::op::UringOpPayload,
            ) -> Option<&mut Self::KernelPayload> {
                match payload {
                    crate::op::UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> crate::op::UringUserPayload {
                crate::op::UringUserPayload::$user_variant(payload)
            }

            fn try_user_payload(
                payload: crate::op::UringUserPayload,
            ) -> crate::error::UringDriverResult<Self> {
                match payload {
                    crate::op::UringUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(veloq_driver_core::op::payload_projection_mismatch_report::<
                        crate::error::UringError,
                    >(stringify!($OpType), "UringUserPayload")),
                }
            }

            fn user_payload_ref(payload: &crate::op::UringUserPayload) -> Option<&Self> {
                match payload {
                    crate::op::UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn user_payload_mut(payload: &mut crate::op::UringUserPayload) -> Option<&mut Self> {
                match payload {
                    crate::op::UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static crate::op::OpVTable {
                static TABLE: crate::op::OpVTable = crate::op::OpVTable {
                    make_sqe: crate::op::spec::make_sqe_shim::<$OpType>,
                    on_complete: crate::op::spec::on_complete_shim::<$OpType>,
                    completion_cleanup: crate::op::spec::completion_cleanup_shim::<$OpType>,
                    strategy: <$OpType as crate::op::spec::UringOpSpec>::STRATEGY,
                    get_timeout: crate::op::spec::get_timeout_shim::<$OpType>,
                    resolve_chunks: crate::op::spec::resolve_chunks_shim::<$OpType>,
                };
                &TABLE
            }
        }

        impl veloq_driver_core::op::IntoPlatformOp<crate::op::UringOp> for $OpType {
            type UserPayload = $OpType;
            type ErasedPayload = crate::op::UringUserPayload;
            type Output = $OpType;
            type Completion = $completion;
            type DriverCompletion = usize;
            type Error = crate::error::UringError;

            const PAYLOAD_KIND: veloq_driver_core::op::OpKind =
                <$OpType as crate::op::spec::UringOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (crate::op::UringKernelOp, Self::UserPayload) {
                let kernel_payload =
                    <$OpType as crate::op::spec::UringOpSpec>::new_kernel_payload(&self);
                let op = crate::op::UringKernelOp {
                    vtable: <$OpType as crate::op::spec::UringOpErasure>::vtable(),
                    payload: <$OpType as crate::op::spec::UringOpErasure>::erase_kernel_payload(
                        kernel_payload,
                    ),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::UserPayload) -> crate::op::UringUserPayload {
                <$OpType as crate::op::spec::UringOpErasure>::erase_user_payload(payload)
            }

            fn try_payload_from_erased(
                payload: crate::op::UringUserPayload,
            ) -> crate::error::UringDriverResult<Self::UserPayload> {
                <$OpType as crate::op::spec::UringOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::UserPayload,
                res: crate::error::UringDriverResult<usize>,
            ) -> veloq_driver_core::op::OpCompletion<
                Self::Output,
                crate::error::UringError,
                Self::Completion,
            > {
                let completion =
                    <$OpType as crate::op::spec::UringOpSpec>::map_completion(&payload, res);
                veloq_driver_core::op::OpCompletion::new(completion, payload)
            }
        }
    };
}

impl UringOpSpec for Wakeup {
    type KernelPayload = payload::WakeupPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::WakeupPayload::new()
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
        payload::TimeoutPayload::new()
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
