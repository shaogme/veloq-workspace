use crate::{
    driver::SqeEnv,
    error::UringResult,
    op::{
        Close, CompletionCleanupHintFn, Fallocate, FallocateRaw, Fsync, FsyncRaw, Open, ReadFixed,
        ReadRaw, SyncFileRange, SyncFileRangeRaw, WriteFixed, WriteRaw, payload, submit,
    },
};
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{CompletionCleanupGuard, SubmitTokenContext};

use super::{UringOpSpec, make_sqe_adapter};

impl_uring_forwarding_spec!(
    ReadFixed,
    submit::make_sqe_read_fixed,
    submit::resolve_chunks_read_fixed
);
impl_uring_forwarding_spec!(
    ReadRaw,
    submit::make_sqe_read_raw,
    submit::resolve_chunks_read_raw
);
impl_uring_forwarding_spec!(
    WriteFixed,
    submit::make_sqe_write_fixed,
    submit::resolve_chunks_write_fixed
);
impl_uring_forwarding_spec!(
    WriteRaw,
    submit::make_sqe_write_raw,
    submit::resolve_chunks_write_raw
);
impl_uring_forwarding_spec!(Close, submit::make_sqe_close);
impl_uring_forwarding_spec!(Fsync, submit::make_sqe_fsync);
impl_uring_forwarding_spec!(FsyncRaw, submit::make_sqe_fsync_raw);
impl_uring_forwarding_spec!(SyncFileRange, submit::make_sqe_sync_range);
impl_uring_forwarding_spec!(SyncFileRangeRaw, submit::make_sqe_sync_range_raw);
impl_uring_forwarding_spec!(Fallocate, submit::make_sqe_fallocate);
impl_uring_forwarding_spec!(FallocateRaw, submit::make_sqe_fallocate_raw);

impl UringOpSpec for Open {
    type KernelPayload = payload::OpenPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::OpenPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_open)
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    const COMPLETION_CLEANUP_HINT: Option<CompletionCleanupHintFn> =
        Some(submit::completion_cleanup_close_raw_fd);
}
