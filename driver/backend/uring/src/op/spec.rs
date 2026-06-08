use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{OpVTable, SubmissionStrategy, UringKernelOp, UringOpPayload, UringUserPayload};
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
