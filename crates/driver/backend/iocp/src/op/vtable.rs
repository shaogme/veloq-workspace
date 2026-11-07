use veloq_std::ptr::NonNull;

use crate::{
    config::IoFd,
    error::IocpResult,
    ext::Extensions,
    op::{
        IocpKernelOp, IocpOpPayload, IocpUserPayload, OverlappedEntry, SubmissionResult,
        SubmitContext,
    },
};
use veloq_driver_core::{CompletionCleanupGuard, CompletionContinuation, OpKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IocpRecordKind {
    AcceptedSocket,
    ProvidedBuf,
    UdpRecvPacket,
}

pub(crate) struct IocpMultiShotVTable {
    pub(crate) record_kind: IocpRecordKind,
    pub(crate) encode_accepted_socket: fn() -> IocpUserPayload,
    pub(crate) rearm: fn(&mut IocpKernelOp, &mut SubmitContext) -> IocpResult<SubmissionResult>,
    pub(crate) continuation: fn(&IocpResult<usize>) -> CompletionContinuation,
}

pub(crate) struct OpVTable {
    pub(crate) kind: OpKind,
    pub(crate) multishot_ops: Option<&'static IocpMultiShotVTable>,
    pub(crate) submit: fn(&mut IocpKernelOp, &mut SubmitContext) -> IocpResult<SubmissionResult>,
    pub(crate) on_complete:
        unsafe fn(&mut IocpKernelOp, result: usize, ext: &Extensions) -> IocpResult<usize>,
    pub(crate) completion_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) orphan_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) get_fd: unsafe fn(&IocpKernelOp) -> Option<IoFd>,
    pub(crate) bind_user_payload: fn(&mut IocpKernelOp, &mut IocpUserPayload) -> IocpResult<()>,
    pub(crate) unbind_user_payload: fn(&mut IocpKernelOp),
}

pub(crate) trait PayloadBinding<T> {
    fn bind(&mut self, user: NonNull<T>);

    fn clear(&mut self);
}

pub(crate) trait IocpOpSpec: Sized + Send + 'static {
    type User: Send + 'static;
    type KernelPayload: PayloadBinding<Self::User>;
    type Completion;

    const PAYLOAD_KIND: OpKind;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload;

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult>;

    unsafe fn on_complete(
        _header: &mut OverlappedEntry,
        _payload: &mut Self::KernelPayload,
        result: usize,
        _ext: &Extensions,
    ) -> IocpResult<usize> {
        Ok(result)
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        _result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }

    fn orphan_cleanup(
        payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        Self::completion_cleanup(payload, result)
    }

    unsafe fn get_fd(_payload: &Self::KernelPayload) -> Option<IoFd> {
        None
    }

    fn map_completion(payload: &Self::User, res: IocpResult<usize>)
    -> IocpResult<Self::Completion>;
}

pub(crate) trait IocpMultiShotSpec: Sized + Send + 'static {
    type User: Send + 'static;
    type KernelPayload: PayloadBinding<Self::User>;
    type RecordPayload: Send;
    type Completion: Send;

    const PAYLOAD_KIND: OpKind;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload;

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult>;

    unsafe fn on_complete(
        _header: &mut OverlappedEntry,
        _payload: &mut Self::KernelPayload,
        result: usize,
        _ext: &Extensions,
    ) -> IocpResult<usize> {
        Ok(result)
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        _result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }

    fn orphan_cleanup(
        payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        Self::completion_cleanup(payload, result)
    }

    unsafe fn get_fd(_payload: &Self::KernelPayload) -> Option<IoFd> {
        None
    }

    fn map_completion(
        payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion>;
}

pub(crate) trait IocpOpErasure: IocpOpSpec {
    fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload;

    fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload>;

    fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self::User) -> IocpUserPayload;

    fn try_user_payload(payload: IocpUserPayload) -> IocpResult<Self::User>;

    fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self::User>;

    fn vtable() -> &'static OpVTable;
}

pub(crate) trait IocpMultiShotErasure: IocpMultiShotSpec {
    fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload;

    fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload>;

    fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self::User) -> IocpUserPayload;

    fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self::User>;

    fn try_record_payload(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload>;

    fn vtable() -> &'static OpVTable;
}
