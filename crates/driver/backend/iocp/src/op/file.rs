use core::marker::PhantomData;

use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RawHandle},
    error::IocpResult,
    op::{
        Close, Fallocate, FallocateRaw, Fsync, FsyncRaw, IocpOpSpec, KernelRef, Open, OpenPayload,
        OverlappedEntry, ReadFixed, ReadRaw, SubmitContext, SyncFileRange, SyncFileRangeRaw,
        WriteFixed, WriteRaw,
        submit::{BlockingSubmissionOps, FileSubmissionOps, SubmissionOps, SubmissionResult},
    },
};

use veloq_driver_core::{CompletionCleanupGuard, OpKind};

pub(super) struct FileOpSpec<T>(PhantomData<T>);

impl IocpOpSpec for FileOpSpec<ReadFixed> {
    type User = ReadFixed;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        FileSubmissionOps::submit_read_fixed(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_read_fixed(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<ReadRaw> {
    type User = ReadRaw;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        FileSubmissionOps::submit_read_raw(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<WriteFixed> {
    type User = WriteFixed;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        FileSubmissionOps::submit_write_fixed(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_write_fixed(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<WriteRaw> {
    type User = WriteRaw;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        FileSubmissionOps::submit_write_raw(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<Close> {
    type User = Close;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Close;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_close(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_close(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<Fsync> {
    type User = Fsync;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_fsync(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_fsync(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<FsyncRaw> {
    type User = FsyncRaw;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_fsync_raw(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<SyncFileRange> {
    type User = SyncFileRange;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_sync_range(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_sync_range(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<SyncFileRangeRaw> {
    type User = SyncFileRangeRaw;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_sync_range_raw(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<Fallocate> {
    type User = Fallocate;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_fallocate(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_fallocate(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<FallocateRaw> {
    type User = FallocateRaw;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_fallocate_raw(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FileOpSpec<Open> {
    type User = Open;
    type KernelPayload = OpenPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Open;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        OpenPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        BlockingSubmissionOps::submit_open(header, payload, ctx)
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        BlockingSubmissionOps::completion_cleanup_close_file(result)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_file(raw as _)))
        })
    }
}
