use std::ptr::NonNull;

use crate::config::IoFd;
use crate::error::{IocpDriverResult as DriverResult, IocpError, IocpResult};
use crate::ext::Extensions;
use crate::op::{
    IocpKernelOp, IocpOpPayload, IocpUserPayload, OpVTable, OverlappedEntry, SubmissionResult,
    SubmitContext,
};
use diagweave::prelude::*;
use veloq_driver_core::driver::CompletionCleanupGuard;
use veloq_driver_core::op::OpKind;

pub(crate) trait PayloadBinding<T> {
    fn bind(&mut self, user: NonNull<T>);
    fn clear(&mut self);
}

pub(crate) trait IocpOpSpec: Sized + Send + 'static {
    type KernelPayload: PayloadBinding<Self>;
    type Completion;

    const PAYLOAD_KIND: OpKind;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload;

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

    fn map_completion(payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion>;
}

pub(crate) trait IocpOpErasure: IocpOpSpec {
    fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload;
    fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload>;
    fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self) -> IocpUserPayload;
    fn try_user_payload(payload: IocpUserPayload) -> DriverResult<Self>;
    fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self>;

    fn vtable() -> &'static OpVTable;
}

pub(crate) fn submit_shim<S>(
    op: &mut IocpKernelOp,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult>
where
    S: IocpOpErasure,
{
    let payload = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        IocpError::InvalidState
            .to_report()
            .with_ctx("op_type", std::any::type_name::<S>())
            .attach_note("variant mismatch in IocpKernelOp dispatch")
    })?;
    S::submit(&mut op.header, payload, ctx)
}

pub(crate) unsafe fn on_complete_shim<S>(
    op: &mut IocpKernelOp,
    result: usize,
    ext: &Extensions,
) -> IocpResult<usize>
where
    S: IocpOpErasure,
{
    let payload = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        IocpError::InvalidState
            .to_report()
            .with_ctx("op_type", std::any::type_name::<S>())
            .attach_note("variant mismatch in IocpKernelOp on_complete")
    })?;
    unsafe { S::on_complete(&mut op.header, payload, result, ext) }
}

pub(crate) unsafe fn completion_cleanup_shim<S>(
    op: &mut IocpKernelOp,
    result: &IocpResult<usize>,
) -> CompletionCleanupGuard
where
    S: IocpOpErasure,
{
    let Some(payload) = S::kernel_payload_mut(&mut op.payload) else {
        return CompletionCleanupGuard::default();
    };
    S::completion_cleanup(payload, result)
}

pub(crate) unsafe fn orphan_cleanup_shim<S>(
    op: &mut IocpKernelOp,
    result: &IocpResult<usize>,
) -> CompletionCleanupGuard
where
    S: IocpOpErasure,
{
    let Some(payload) = S::kernel_payload_mut(&mut op.payload) else {
        return CompletionCleanupGuard::default();
    };
    S::orphan_cleanup(payload, result)
}

pub(crate) unsafe fn get_fd_shim<S>(op: &IocpKernelOp) -> Option<IoFd>
where
    S: IocpOpErasure,
{
    let payload = S::kernel_payload_ref(&op.payload)?;
    unsafe { S::get_fd(payload) }
}

pub(crate) fn bind_user_payload_shim<S>(
    op: &mut IocpKernelOp,
    erased: &mut IocpUserPayload,
) -> IocpResult<()>
where
    S: IocpOpErasure,
{
    let payload = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        IocpError::InvalidState
            .to_report()
            .with_ctx("op_type", std::any::type_name::<S>())
            .attach_note("variant mismatch while binding IOCP kernel payload")
    })?;
    let user = S::user_payload_mut(erased).ok_or_else(|| {
        IocpError::InvalidState
            .to_report()
            .with_ctx("op_type", std::any::type_name::<S>())
            .attach_note("variant mismatch while binding IOCP user payload")
    })?;
    payload.bind(NonNull::from(user));
    Ok(())
}

pub(crate) fn unbind_user_payload_shim<S>(op: &mut IocpKernelOp)
where
    S: IocpOpErasure,
{
    if let Some(payload) = S::kernel_payload_mut(&mut op.payload) {
        payload.clear();
    }
}
