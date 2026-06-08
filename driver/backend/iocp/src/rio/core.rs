//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::config::{BorrowedRawHandle, IoFd, SocketKey};
use crate::error::{IocpError, iocp_report_to_event_res};
use crate::op::submit::SubmissionResult;
use crate::rio::RioEnv;
use crate::rio::core::registry::{
    RioAddrReservation, RioBufferLeaseToken, RioPreparedBuffer, RioSubmissionKind,
};
use crate::rio::core::submit_ops::{RioKernel, RioRq};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioState, SocketInflightToken};
use diagweave::prelude::*;
use std::ffi::c_void;
use std::ptr::NonNull;
use veloq_driver_core::driver::OpToken;
use windows_sys::Win32::Networking::WinSock::{RIO_BUF, SOCKADDR_INET};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioOpKind {
    Recv,
    Send,
    SendTo,
    RecvFrom,
}

impl RioOpKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Recv => "recv",
            Self::Send => "send",
            Self::SendTo => "send_to",
            Self::RecvFrom => "recv_from",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RioRequestDiagnostics {
    pub(crate) rq_raw: usize,
    pub(crate) data_buffer_id: usize,
    pub(crate) data_buffer_offset: u32,
    pub(crate) data_buffer_length: u32,
    pub(crate) addr_buffer_id: usize,
    pub(crate) addr_buffer_offset: u32,
    pub(crate) addr_buffer_length: u32,
}

impl RioRequestDiagnostics {
    fn new(rq: RioRq, data_buf: &RIO_BUF, addr: Option<&RioAddrReservation>) -> Self {
        let (addr_buffer_id, addr_buffer_offset, addr_buffer_length) = addr
            .map(|addr| {
                (
                    addr.rio_buf.BufferId as usize,
                    addr.rio_buf.Offset,
                    addr.rio_buf.Length,
                )
            })
            .unwrap_or((0, 0, 0));
        Self {
            rq_raw: rq.0 as usize,
            data_buffer_id: data_buf.BufferId as usize,
            data_buffer_offset: data_buf.Offset,
            data_buffer_length: data_buf.Length,
            addr_buffer_id,
            addr_buffer_offset,
            addr_buffer_length,
        }
    }
}

pub(crate) struct RioOpRequestInit {
    pub(crate) token: OpToken,
    pub(crate) socket_inflight: SocketInflightToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) buffer_lease: Option<RioBufferLeaseToken>,
    pub(crate) diagnostics: RioRequestDiagnostics,
}

impl RioOpRequestInit {
    #[inline]
    pub(crate) fn socket_key(&self) -> SocketKey {
        self.socket_inflight.socket_key()
    }
}

pub(crate) enum RioCompletionKind {
    Op {
        init: RioOpRequestInit,
        context: RioCompletedRequestContext,
    },
}

#[repr(C)]
pub(crate) struct RioOpRequestContext {
    pub(crate) init: Option<RioOpRequestInit>,
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct RioRequestContextHandle(NonNull<RioOpRequestContext>);

pub(crate) struct RioPreparedRequestContext {
    handle: RioRequestContextHandle,
}

#[derive(Clone, Copy)]
pub(crate) struct RioSubmittedRequestContext {
    handle: RioRequestContextHandle,
}

pub(crate) struct RioCompletedRequestContext {
    handle: RioRequestContextHandle,
}

impl RioRequestContextHandle {
    fn new(ctx: RioOpRequestContext) -> Self {
        let ptr = Box::into_raw(Box::new(ctx));
        // SAFETY: Box::into_raw never returns a null pointer.
        Self(unsafe { NonNull::new_unchecked(ptr) })
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.0.as_ptr().cast::<c_void>()
    }

    #[inline]
    fn from_request_context(ctx: u64) -> Option<Self> {
        NonNull::new(ctx as usize as *mut RioOpRequestContext).map(Self)
    }

    #[inline]
    fn as_ref(&self) -> &RioOpRequestContext {
        // SAFETY: all handles are created from a live Box<RioOpRequestContext>
        // and are only decoded once the kernel returns the request context.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    fn as_mut(&mut self) -> &mut RioOpRequestContext {
        // SAFETY: the typed owner has unique access while extracting the init
        // before freeing the request context.
        unsafe { self.0.as_mut() }
    }

    #[inline]
    fn free(self) {
        // SAFETY: the typed owner calling this method is responsible for owning
        // the allocation represented by the handle.
        unsafe { drop(Box::from_raw(self.0.as_ptr())) };
    }
}

impl RioPreparedRequestContext {
    #[inline]
    pub(crate) fn new(init: RioOpRequestInit) -> Self {
        Self {
            handle: RioRequestContextHandle::new(RioOpRequestContext { init: Some(init) }),
        }
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.handle.as_request_context()
    }

    #[inline]
    fn into_submitted(self) -> RioSubmittedRequestContext {
        let this = std::mem::ManuallyDrop::new(self);
        RioSubmittedRequestContext {
            handle: this.handle,
        }
    }

    #[inline]
    fn init(&self) -> Option<&RioOpRequestInit> {
        self.handle.as_ref().init.as_ref()
    }

    #[inline]
    fn take_init(&mut self) -> Option<RioOpRequestInit> {
        self.handle.as_mut().init.take()
    }
}

impl Drop for RioPreparedRequestContext {
    fn drop(&mut self) {
        debug_assert!(
            self.handle.as_ref().init.is_none(),
            "dropping prepared RIO request context without releasing socket inflight token"
        );
        self.handle.free();
    }
}

impl RioSubmittedRequestContext {
    #[inline]
    fn as_request_context(&self) -> *const c_void {
        self.handle.as_request_context()
    }
}

impl RioCompletedRequestContext {
    #[inline]
    fn from_request_context(ctx: u64) -> Option<Self> {
        RioRequestContextHandle::from_request_context(ctx).map(|handle| Self { handle })
    }

    #[inline]
    fn take_init(&mut self) -> Option<RioOpRequestInit> {
        self.handle.as_mut().init.take()
    }
}

impl Drop for RioCompletedRequestContext {
    fn drop(&mut self) {
        self.handle.free();
    }
}

pub(crate) struct RioSubmissionSpec {
    pub(crate) token: OpToken,
    pub(crate) socket_inflight: SocketInflightToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) rq: RioRq,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
}

pub(crate) struct RioPreparedRequest {
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) rq: RioRq,
    context: Option<RioPreparedRequestContext>,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
    pub(crate) diagnostics: RioRequestDiagnostics,
    pub(crate) outstanding_snapshot: usize,
}

pub(crate) struct RioSubmitErrorContext<'a> {
    pub(crate) scope: &'static str,
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) note: &'static str,
}

#[derive(Clone, Copy)]
pub(crate) enum RioAddressPolicy {
    None,
    SendTo {
        addr_ptr: *const c_void,
        addr_len: i32,
    },
    RecvFrom {
        addr_ptr: *mut c_void,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct RioSubmitPlan<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) buffer_kind: RioSubmissionKind,
    pub(crate) buffer: &'a veloq_buf::FixedBuf,
    pub(crate) buffer_offset: usize,
    pub(crate) operation: &'static str,
    pub(crate) address: RioAddressPolicy,
    pub(crate) dispatch_error: RioError,
    pub(crate) dispatch_note: &'static str,
    pub(crate) submit_scope: &'static str,
    pub(crate) submit_note: &'static str,
}

pub(crate) struct RioSubmissionLease<'a> {
    state: &'a mut RioState,
    request: RioPreparedRequest,
    submitted: bool,
    buffer_ref_acquired: bool,
}

impl RioPreparedRequest {
    #[inline]
    fn init(&self) -> &RioOpRequestInit {
        self.context
            .as_ref()
            .and_then(RioPreparedRequestContext::init)
            .expect("RIO prepared request init missing")
    }

    #[inline]
    fn take_init(&mut self) -> Option<RioOpRequestInit> {
        self.context
            .as_mut()
            .and_then(RioPreparedRequestContext::take_init)
    }

    #[inline]
    pub(crate) fn socket_key(&self) -> SocketKey {
        self.init().socket_key()
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.context
            .as_ref()
            .expect("RIO prepared request context missing")
            .as_request_context()
    }

    #[inline]
    fn mark_submitted(&mut self) -> RioSubmittedRequestContext {
        self.context
            .take()
            .expect("RIO prepared request context already submitted")
            .into_submitted()
    }

    pub(crate) fn attach_submit_error(
        &self,
        error: Report<RioError>,
        ctx: RioSubmitErrorContext<'_>,
    ) -> Report<RioError> {
        let diagnostics = self.diagnostics;
        let socket_key = self.socket_key();
        error
            .push_ctx("scope", ctx.scope)
            .with_ctx("fd_fixed_index", ctx.fd.fixed_index())
            .with_ctx("fd_generation", ctx.fd.generation())
            .with_ctx("handle_raw", ctx.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("user_data", ctx.token.index())
            .with_ctx("generation", ctx.token.generation())
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx(
                "addr_slot",
                self.addr.map(|addr| addr.slot).unwrap_or(usize::MAX),
            )
            .with_ctx("rq_raw", diagnostics.rq_raw)
            .with_ctx("data_buffer_id", diagnostics.data_buffer_id)
            .with_ctx("data_buffer_offset", diagnostics.data_buffer_offset)
            .with_ctx("data_buffer_length", diagnostics.data_buffer_length)
            .with_ctx("addr_buffer_id", diagnostics.addr_buffer_id)
            .with_ctx("addr_buffer_offset", diagnostics.addr_buffer_offset)
            .with_ctx("addr_buffer_length", diagnostics.addr_buffer_length)
            .with_ctx("outstanding_count", self.outstanding_snapshot)
            .attach_note(ctx.note)
    }
}

impl<'a> RioSubmitPlan<'a> {
    #[inline]
    fn submit_error_context(&self) -> RioSubmitErrorContext<'a> {
        RioSubmitErrorContext {
            scope: self.submit_scope,
            fd: self.fd,
            handle: self.handle,
            token: self.token,
            note: self.submit_note,
        }
    }
}

impl<'a> RioSubmissionLease<'a> {
    pub(crate) fn submit_with(
        mut self,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        self.acquire_buffer_ref()?;
        submit(&self.state.kernel, &self.request)?;
        self.commit_submitted();
        Ok(SubmissionResult::Pending)
    }

    fn acquire_buffer_ref(&mut self) -> RioResult<()> {
        if self.buffer_ref_acquired {
            return Ok(());
        }
        self.state
            .registry
            .acquire_buffer_lease(self.request.data_buf.lease)
            .push_ctx("scope", "rio.core.submission.acquire_buffer_lease")
            .with_ctx("rio_op_kind", self.request.op_kind.as_str())
            .with_ctx("rio_request_id", self.request.request_id)
            .with_ctx("data_buffer_id", self.request.diagnostics.data_buffer_id)
            .with_ctx(
                "data_buffer_offset",
                self.request.diagnostics.data_buffer_offset,
            )
            .with_ctx(
                "data_buffer_length",
                self.request.diagnostics.data_buffer_length,
            )
            .attach_note("failed to acquire RIO buffer lease before kernel submit")?;
        self.buffer_ref_acquired = true;
        Ok(())
    }

    fn commit_submitted(&mut self) {
        if self.submitted {
            return;
        }
        let submitted_context = self.request.mark_submitted();
        debug_assert!(!submitted_context.as_request_context().is_null());
        self.state.outstanding_count += 1;
        self.submitted = true;
    }

    fn rollback_buffer_ref(&mut self) {
        if !self.buffer_ref_acquired {
            return;
        }

        let release = if let Some(env) = self
            .state
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.state.registration_mode)
        {
            self.state
                .registry
                .release_buffer_lease(self.request.data_buf.lease, env)
        } else {
            self.state
                .registry
                .release_buffer_lease_deferred(self.request.data_buf.lease)
        };

        match release {
            Ok(()) => {
                self.buffer_ref_acquired = false;
            }
            Err(error) => {
                tracing::error!(
                    report = ?error,
                    rio_op_kind = self.request.op_kind.as_str(),
                    rio_request_id = self.request.request_id,
                    "failed to roll back unsubmitted RIO buffer lease"
                );
            }
        }
    }
}

impl Drop for RioSubmissionLease<'_> {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        self.rollback_buffer_ref();
        self.state
            .registry
            .free_addr_slot(self.request.addr.map(|addr| addr.slot));
        let socket_inflight = self.request.take_init().map(|init| init.socket_inflight);
        debug_assert!(
            socket_inflight.is_some(),
            "unsubmitted RIO request missing socket inflight token"
        );
        if let Some(socket_inflight) = socket_inflight {
            self.state.release_socket_inflight_token(socket_inflight);
        }
    }
}

#[inline]
pub(crate) fn rio_result_to_event_res(res: &crate::error::IocpDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}

impl RioState {
    #[inline]
    pub(crate) fn encode_req_ctx(init: RioOpRequestInit) -> RioPreparedRequestContext {
        RioPreparedRequestContext::new(init)
    }

    pub(crate) fn submit_rio(
        &mut self,
        plan: RioSubmitPlan<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        let buf_len = plan
            .buffer_kind
            .data_len(plan.buffer, plan.buffer_offset, plan.operation)?;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or(plan.dispatch_error)
            .attach_note(plan.dispatch_note)?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let outstanding_snapshot = self.outstanding_count;
        let rq = {
            let actor = self
                .ensure_actor((plan.fd, plan.handle), env)
                .push_ctx("scope", "rio.core.submit_plan.ensure_actor")
                .with_ctx("fd_fixed_index", plan.fd.fixed_index())
                .with_ctx("fd_generation", plan.fd.generation())
                .with_ctx("handle_raw", plan.handle.raw().as_handle() as usize)
                .with_ctx("socket_raw", plan.handle.raw().as_handle() as usize)
                .with_ctx("user_data", plan.token.index())
                .with_ctx("generation", plan.token.generation())
                .with_ctx("rio_op_kind", plan.op_kind.as_str())
                .with_ctx("rio_operation", plan.operation)
                .with_ctx("outstanding_count", outstanding_snapshot)
                .attach_note("failed to ensure RIO actor")?;
            actor.rq
        };
        let data_buf =
            self.registry
                .prepare_submission(plan.buffer, plan.buffer_offset, buf_len, env)?;
        let addr = self.prepare_submit_address(plan.address, env)?;
        let socket_key = plan.handle.raw().actor_key();
        let socket_inflight = match self.try_acquire_socket_inflight_token(socket_key) {
            Ok(token) => token,
            Err(error) => {
                self.registry.free_addr_slot(addr.map(|addr| addr.slot));
                return Err(error
                    .push_ctx("scope", "rio.core.submit_plan.acquire_socket_inflight")
                    .with_ctx("fd_fixed_index", plan.fd.fixed_index())
                    .with_ctx("fd_generation", plan.fd.generation())
                    .with_ctx("handle_raw", plan.handle.raw().as_handle() as usize)
                    .with_ctx("socket_raw", socket_key.as_handle() as usize)
                    .with_ctx("user_data", plan.token.index())
                    .with_ctx("generation", plan.token.generation())
                    .with_ctx("rio_op_kind", plan.op_kind.as_str())
                    .with_ctx("rio_operation", plan.operation)
                    .attach_note("failed to acquire socket inflight slot for RIO submission"));
            }
        };
        let error_context = plan.submit_error_context();
        self.prepare_submission_lease(RioSubmissionSpec {
            token: plan.token,
            socket_inflight,
            op_kind: plan.op_kind,
            rq,
            data_buf,
            addr,
        })
        .submit_with(|kernel, request| {
            submit(kernel, request)
                .map_err(|error| request.attach_submit_error(error, error_context))
        })
    }

    fn prepare_submit_address(
        &mut self,
        policy: RioAddressPolicy,
        env: RioEnv<'_>,
    ) -> RioResult<Option<RioAddrReservation>> {
        match policy {
            RioAddressPolicy::None => Ok(None),
            RioAddressPolicy::SendTo { addr_ptr, addr_len } => self
                .registry
                .prepare_send_addr(addr_ptr, addr_len, env)
                .map(Some),
            RioAddressPolicy::RecvFrom { addr_ptr } => {
                if addr_ptr.is_null() {
                    return RioError::Internal
                        .attach_note("RIO recv_from received null address buffer");
                }
                let mut addr = self.registry.prepare_recv_addr(env)?;
                addr.rio_buf.Length = std::mem::size_of::<SOCKADDR_INET>() as u32;
                Ok(Some(addr))
            }
        }
    }

    #[inline]
    pub(crate) fn prepare_submission_lease(
        &mut self,
        spec: RioSubmissionSpec,
    ) -> RioSubmissionLease<'_> {
        let diagnostics =
            RioRequestDiagnostics::new(spec.rq, &spec.data_buf.rio_buf, spec.addr.as_ref());
        let request_id = self.next_request_id();
        let context = Self::encode_req_ctx(RioOpRequestInit {
            token: spec.token,
            socket_inflight: spec.socket_inflight,
            op_kind: spec.op_kind,
            request_id,
            addr_slot: spec.addr.map(|addr| addr.slot),
            buffer_lease: spec.data_buf.lease,
            diagnostics,
        });
        let request = RioPreparedRequest {
            op_kind: spec.op_kind,
            request_id,
            rq: spec.rq,
            context: Some(context),
            data_buf: spec.data_buf,
            addr: spec.addr,
            diagnostics,
            outstanding_snapshot: self.outstanding_count,
        };
        RioSubmissionLease {
            state: self,
            request,
            submitted: false,
            buffer_ref_acquired: false,
        }
    }

    #[inline]
    pub(crate) fn decode_req_ctx(ctx: u64) -> Option<RioCompletionKind> {
        let mut context = RioCompletedRequestContext::from_request_context(ctx)?;
        let init = context.take_init()?;
        Some(RioCompletionKind::Op { init, context })
    }

    #[inline]
    fn next_request_id(&mut self) -> u64 {
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }
        self.next_request_id
    }

    #[inline]
    pub(crate) fn last_wsa_error_code() -> i32 {
        // SAFETY: WSAGetLastError is safe to call.
        unsafe { windows_sys::Win32::Networking::WinSock::WSAGetLastError() }
    }

    pub(crate) fn last_wsa_report(context: RioError, scope: &'static str) -> Report<RioError> {
        let code = Self::last_wsa_error_code() as u32;
        context
            .to_report()
            .push_ctx("scope", scope)
            .set_error_code(code)
            .attach_note(
                IocpError::Internal
                    .to_report()
                    .push_ctx("scope", scope)
                    .set_error_code(code as i32)
                    .attach_note("winsock error"),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IocpHandle;

    fn test_req_init(addr_slot: Option<usize>) -> RioOpRequestInit {
        let socket_key = IocpHandle::for_socket(std::ptr::null_mut());
        RioOpRequestInit {
            token: OpToken::new(11, 17),
            socket_inflight: SocketInflightToken::new(socket_key),
            op_kind: RioOpKind::Recv,
            request_id: 23,
            addr_slot,
            buffer_lease: None,
            diagnostics: RioRequestDiagnostics::default(),
        }
    }

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let context = RioState::encode_req_ctx(test_req_init(None));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = RioState::decode_req_ctx(raw);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                init: RioOpRequestInit {
                    token,
                    op_kind: RioOpKind::Recv,
                    request_id: 23,
                    addr_slot: None,
                    ..
                },
                ..
            }) if token == OpToken::new(11, 17)));
    }

    #[test]
    fn op_ctx_with_addr_roundtrip_decode_and_free() {
        let context = RioState::encode_req_ctx(test_req_init(Some(3)));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = RioState::decode_req_ctx(raw);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                init: RioOpRequestInit {
                    token,
                    op_kind: RioOpKind::Recv,
                    request_id: 23,
                    addr_slot: Some(3),
                    ..
                },
                ..
            }) if token == OpToken::new(11, 17)));
    }

    #[test]
    fn rio_result_translation_behaviour() {
        assert_eq!(rio_result_to_event_res(&Ok(5)), 5);
        assert_eq!(
            rio_result_to_event_res(&Ok((i32::MAX as usize) + 10)),
            i32::MAX
        );
        let err = IocpError::Internal
            .to_report()
            .push_ctx("scope", "rio.core.tests")
            .set_error_code(10022)
            .attach_note("invalid argument");
        assert_eq!(rio_result_to_event_res(&Err(err)), -10022);
    }

    #[test]
    fn decode_zero_context_is_noop() {
        assert!(RioState::decode_req_ctx(0).is_none());
    }
}
