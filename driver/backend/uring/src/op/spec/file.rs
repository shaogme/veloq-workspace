use crate::config::UringRawHandle;
use crate::driver::UringDriver;
use crate::error::UringDriverResult as DriverResult;
use crate::op::{
    Close, Fallocate, FallocateRaw, Fsync, FsyncRaw, Open, ReadFixed, ReadRaw, SubmissionStrategy,
    SyncFileRange, SyncFileRangeRaw, WriteFixed, WriteRaw, payload, submit,
};
use crate::{OwnedRawHandle, RawHandle};
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{CompletionCleanupGuard, SubmitTokenContext};
use veloq_driver_core::op::OpKind;

use super::UringOpSpec;

impl UringOpSpec for ReadFixed {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
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
        chunks: &mut [ChunkId],
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
        payload::kernel_ref(user)
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
        chunks: &mut [ChunkId],
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
        payload::kernel_ref(user)
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
        chunks: &mut [ChunkId],
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
        payload::kernel_ref(user)
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
        chunks: &mut [ChunkId],
    ) -> usize {
        submit::resolve_chunks_write_raw(kernel, payload, chunks)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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
        payload::kernel_ref(user)
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

impl UringOpSpec for Open {
    type KernelPayload = payload::OpenPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Open;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::OpenPayload::new()
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
