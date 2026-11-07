use veloq_std::{any::type_name, ptr::NonNull};

use crate::{
    config::IoFd,
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::{
        AcceptedSocket, IocpKernelOp, IocpMultiShotErasure, IocpOpErasure, IocpUserPayload,
        PayloadBinding, SubmissionResult, SubmitContext,
    },
};
use diagweave::prelude::*;
use veloq_driver_core::{CompletionCleanupGuard, CompletionContinuation};

pub(super) struct IocpSpecDispatch;

impl IocpSpecDispatch {
    pub(super) fn encode_accepted_socket() -> IocpUserPayload {
        IocpUserPayload::AcceptedSocket(AcceptedSocket {})
    }

    pub(super) fn final_continuation(_result: &IocpResult<usize>) -> CompletionContinuation {
        CompletionContinuation::Final
    }

    pub(super) fn more_on_success(result: &IocpResult<usize>) -> CompletionContinuation {
        if result.is_ok() {
            CompletionContinuation::More
        } else {
            CompletionContinuation::Final
        }
    }
}

impl IocpSpecDispatch {
    pub(super) fn submit_shim<S>(
        op: &mut IocpKernelOp,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult>
    where
        S: IocpOpErasure,
    {
        let (header, erased_payload) = op.header_payload_mut();
        let payload = S::kernel_payload_mut(erased_payload).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch in IocpKernelOp dispatch")
        })?;
        S::submit(header, payload, ctx)
    }

    pub(super) fn submit_multishot_shim<S>(
        op: &mut IocpKernelOp,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult>
    where
        S: IocpMultiShotErasure,
    {
        let (header, erased_payload) = op.header_payload_mut();
        let payload = S::kernel_payload_mut(erased_payload).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch in IOCP multishot submit dispatch")
        })?;
        S::submit(header, payload, ctx)
    }

    pub(super) unsafe fn on_complete_shim<S>(
        op: &mut IocpKernelOp,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize>
    where
        S: IocpOpErasure,
    {
        let (header, erased_payload) = op.header_payload_mut();
        let payload = S::kernel_payload_mut(erased_payload).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch in IocpKernelOp on_complete")
        })?;
        unsafe { S::on_complete(header, payload, result, ext) }
    }

    pub(super) unsafe fn on_complete_multishot_shim<S>(
        op: &mut IocpKernelOp,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize>
    where
        S: IocpMultiShotErasure,
    {
        let (header, erased_payload) = op.header_payload_mut();
        let payload = S::kernel_payload_mut(erased_payload).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch in IOCP multishot on_complete")
        })?;
        unsafe { S::on_complete(header, payload, result, ext) }
    }

    pub(super) unsafe fn completion_cleanup_shim<S>(
        op: &mut IocpKernelOp,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard
    where
        S: IocpOpErasure,
    {
        let Some(payload) = S::kernel_payload_mut(op.payload_mut()) else {
            return CompletionCleanupGuard::default();
        };
        S::completion_cleanup(payload, result)
    }

    pub(super) unsafe fn completion_cleanup_multishot_shim<S>(
        op: &mut IocpKernelOp,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard
    where
        S: IocpMultiShotErasure,
    {
        let Some(payload) = S::kernel_payload_mut(op.payload_mut()) else {
            return CompletionCleanupGuard::default();
        };
        S::completion_cleanup(payload, result)
    }

    pub(super) unsafe fn orphan_cleanup_shim<S>(
        op: &mut IocpKernelOp,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard
    where
        S: IocpOpErasure,
    {
        let Some(payload) = S::kernel_payload_mut(op.payload_mut()) else {
            return CompletionCleanupGuard::default();
        };
        S::orphan_cleanup(payload, result)
    }

    pub(super) unsafe fn orphan_cleanup_multishot_shim<S>(
        op: &mut IocpKernelOp,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard
    where
        S: IocpMultiShotErasure,
    {
        let Some(payload) = S::kernel_payload_mut(op.payload_mut()) else {
            return CompletionCleanupGuard::default();
        };
        S::orphan_cleanup(payload, result)
    }

    pub(super) unsafe fn get_fd_shim<S>(op: &IocpKernelOp) -> Option<IoFd>
    where
        S: IocpOpErasure,
    {
        let payload = S::kernel_payload_ref(op.payload())?;
        unsafe { S::get_fd(payload) }
    }

    pub(super) unsafe fn get_fd_multishot_shim<S>(op: &IocpKernelOp) -> Option<IoFd>
    where
        S: IocpMultiShotErasure,
    {
        let payload = S::kernel_payload_ref(op.payload())?;
        unsafe { S::get_fd(payload) }
    }

    pub(super) fn bind_user_payload_shim<S>(
        op: &mut IocpKernelOp,
        erased: &mut IocpUserPayload,
    ) -> IocpResult<()>
    where
        S: IocpOpErasure,
    {
        let payload = S::kernel_payload_mut(op.payload_mut()).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch while binding IOCP kernel payload")
        })?;
        let user = S::user_payload_mut(erased).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch while binding IOCP user payload")
        })?;
        payload.bind(NonNull::from(user));
        Ok(())
    }

    pub(super) fn bind_multishot_user_payload_shim<S>(
        op: &mut IocpKernelOp,
        erased: &mut IocpUserPayload,
    ) -> IocpResult<()>
    where
        S: IocpMultiShotErasure,
    {
        let payload = S::kernel_payload_mut(op.payload_mut()).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch while binding IOCP multishot kernel payload")
        })?;
        let user = S::user_payload_mut(erased).ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("op_type", type_name::<S>())
                .attach_note("variant mismatch while binding IOCP multishot user payload")
        })?;
        payload.bind(NonNull::from(user));
        Ok(())
    }

    pub(super) fn unbind_user_payload_shim<S>(op: &mut IocpKernelOp)
    where
        S: IocpOpErasure,
    {
        if let Some(payload) = S::kernel_payload_mut(op.payload_mut()) {
            payload.clear();
        }
    }

    pub(super) fn unbind_multishot_user_payload_shim<S>(op: &mut IocpKernelOp)
    where
        S: IocpMultiShotErasure,
    {
        if let Some(payload) = S::kernel_payload_mut(op.payload_mut()) {
            payload.clear();
        }
    }
}
