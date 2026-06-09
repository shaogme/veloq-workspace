//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::config::{BorrowedRawHandle, IoFd, SocketKey};
use crate::error::{IocpError, iocp_report_to_event_res};
use crate::op::SubmissionResult;
use crate::rio::RioEnv;
use crate::rio::core::registry::{
    RioAddrReservation, RioBufferLeaseToken, RioPreparedBuffer, RioRegistry, RioSubmissionKind,
};
use crate::rio::core::submit_ops::{RioDispatch, RioKernel, RioRq};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioState, SocketInflightToken, SocketLifecycleState};
use diagweave::prelude::*;
use std::ffi::c_void;
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

pub(crate) enum RioCompletionKind {
    Op {
        init: RioOpRequestInit,
        context: RioCompletedRequestContext,
    },
}

pub(crate) enum RioRequestContextDecode {
    Valid(RioCompletionKind),
    Malformed {
        raw: u64,
    },
    Missing {
        id: RioRequestContextId,
    },
    Stale {
        id: RioRequestContextId,
        actual_generation: u32,
    },
}

const RIO_REQUEST_CONTEXT_MAGIC: u64 = 0xA7;
const RIO_REQUEST_CONTEXT_MAGIC_SHIFT: u32 = 56;
const RIO_REQUEST_CONTEXT_INDEX_SHIFT: u32 = 32;
const RIO_REQUEST_CONTEXT_INDEX_MASK: u64 = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioRequestContextId {
    index: usize,
    generation: u32,
}

impl RioRequestContextId {
    #[inline]
    pub(crate) fn new(index: usize, generation: u32) -> Self {
        assert!(
            index <= RIO_REQUEST_CONTEXT_INDEX_MASK as usize,
            "RIO request context index exceeds encodable range"
        );
        Self { index, generation }
    }

    #[inline]
    pub(crate) const fn index(self) -> usize {
        self.index
    }

    #[inline]
    pub(crate) const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub(crate) fn raw(self) -> u64 {
        (RIO_REQUEST_CONTEXT_MAGIC << RIO_REQUEST_CONTEXT_MAGIC_SHIFT)
            | ((self.index as u64) << RIO_REQUEST_CONTEXT_INDEX_SHIFT)
            | self.generation as u64
    }

    #[inline]
    pub(crate) fn from_raw(raw: u64) -> Option<Self> {
        let magic = raw >> RIO_REQUEST_CONTEXT_MAGIC_SHIFT;
        if magic != RIO_REQUEST_CONTEXT_MAGIC {
            return None;
        }
        let index =
            ((raw >> RIO_REQUEST_CONTEXT_INDEX_SHIFT) & RIO_REQUEST_CONTEXT_INDEX_MASK) as usize;
        let generation = raw as u32;
        Some(Self { index, generation })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioPreparedRequestContext {
    id: RioRequestContextId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioSubmittedRequestContext {
    id: RioRequestContextId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioCompletedRequestContext;

impl RioPreparedRequestContext {
    #[inline]
    pub(crate) fn new(id: RioRequestContextId) -> Self {
        Self { id }
    }

    #[inline]
    pub(crate) const fn id(self) -> RioRequestContextId {
        self.id
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.id.raw() as usize as *const c_void
    }

    #[inline]
    fn into_submitted(self) -> RioSubmittedRequestContext {
        RioSubmittedRequestContext { id: self.id }
    }
}

impl RioSubmittedRequestContext {
    #[inline]
    fn as_request_context(&self) -> *const c_void {
        self.id.raw() as usize as *const c_void
    }
}

impl RioCompletedRequestContext {
    #[inline]
    pub(crate) fn new() -> Self {
        Self
    }
}

pub(crate) struct RioPreparedRequest {
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) rq: RioRq,
    context: Option<RioPreparedRequestContext>,
    pub(crate) token: OpToken,
    pub(crate) socket_key: SocketKey,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
    pub(crate) diagnostics: RioRequestDiagnostics,
    pub(crate) outstanding_snapshot: usize,
}

pub(crate) struct RioSubmitErrorContext<'a> {
    pub(crate) scope: &'static str,
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
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

struct RioSubmitTxn<'a> {
    state: &'a mut RioState,
    plan: RioSubmitPlan<'a>,
    registrar: &'a dyn veloq_buf::BufferRegistrar,
    rq: Option<RioRq>,
    socket_inflight: Option<SocketInflightToken>,
    data_buf: Option<RioPreparedBuffer>,
    addr: Option<RioAddrReservation>,
    request: Option<RioPreparedRequest>,
    buffer_ref_acquired: bool,
    submitted: bool,
    outstanding_snapshot: usize,
}

impl RioPreparedRequest {
    #[inline]
    fn take_init(&mut self, registry: &mut RioRegistry) -> Option<RioOpRequestInit> {
        let context = self.context.take()?;
        registry.take_prepared_request_init(context)
    }

    #[inline]
    pub(crate) fn socket_key(&self) -> SocketKey {
        self.socket_key
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
            .with_ctx("user_data", self.token.index())
            .with_ctx("generation", self.token.generation())
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx("addr_slot", self.addr_slot.unwrap_or(usize::MAX))
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
            note: self.submit_note,
        }
    }
}

impl<'a> RioSubmitTxn<'a> {
    fn new(
        state: &'a mut RioState,
        plan: RioSubmitPlan<'a>,
        registrar: &'a dyn veloq_buf::BufferRegistrar,
    ) -> Self {
        let outstanding_snapshot = state.outstanding_count;
        Self {
            state,
            plan,
            registrar,
            rq: None,
            socket_inflight: None,
            data_buf: None,
            addr: None,
            request: None,
            buffer_ref_acquired: false,
            submitted: false,
            outstanding_snapshot,
        }
    }

    fn check_socket_accepting(self) -> RioResult<Self> {
        let socket_key = self.socket_key();
        if self.state.submissions_closed {
            let error = RioError::InvalidInput
                .to_report()
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("outstanding_count", self.state.outstanding_count)
                .attach_note("RIO runtime is shutting down; rejecting socket submission");
            return Err(self.attach_stage_error(
                error,
                "rio.core.submit_txn.check_socket_accepting",
                "failed to enter RIO submission transaction",
            ));
        }

        if let Some(runtime) = self.state.socket_runtime.get(&socket_key)
            && runtime.lifecycle == SocketLifecycleState::Closing
        {
            let error = RioError::InvalidInput
                .to_report()
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("socket_lifecycle", "closing")
                .with_ctx("socket_inflight", runtime.inflight)
                .attach_note("socket is closing; rejecting new socket submission");
            return Err(self.attach_stage_error(
                error,
                "rio.core.submit_txn.check_socket_accepting",
                "failed to enter RIO submission transaction",
            ));
        }

        Ok(self)
    }

    fn ensure_actor(mut self) -> RioResult<Self> {
        let dispatch = self.dispatch_for_stage(
            "rio.core.submit_txn.ensure_actor",
            "failed to load RIO dispatch while ensuring actor",
        )?;
        let env = self.env(&dispatch);
        let actor = match self
            .state
            .ensure_actor((self.plan.fd, self.plan.handle), env)
        {
            Ok(actor) => actor,
            Err(error) => {
                return Err(self.attach_stage_error(
                    error,
                    "rio.core.submit_txn.ensure_actor",
                    "failed to ensure RIO actor",
                ));
            }
        };
        self.rq = Some(actor.rq);
        Ok(self)
    }

    fn acquire_socket(mut self) -> RioResult<Self> {
        let socket_key = self.socket_key();
        let socket_inflight = match self.state.try_acquire_socket_inflight_token(socket_key) {
            Ok(token) => token,
            Err(error) => {
                return Err(self.attach_stage_error(
                    error,
                    "rio.core.submit_txn.acquire_socket",
                    "failed to acquire socket inflight slot for RIO submission",
                ));
            }
        };
        self.socket_inflight = Some(socket_inflight);
        Ok(self)
    }

    fn prepare_buffer(mut self) -> RioResult<Self> {
        let buf_len = self
            .plan
            .buffer_kind
            .data_len(
                self.plan.buffer,
                self.plan.buffer_offset,
                self.plan.operation,
            )
            .map_err(|error| {
                self.attach_stage_error(
                    error,
                    "rio.core.submit_txn.prepare_buffer",
                    "failed to compute RIO submission buffer length",
                )
            })?;
        let dispatch = self.dispatch_for_stage(
            "rio.core.submit_txn.prepare_buffer",
            "failed to load RIO dispatch while preparing buffer",
        )?;
        let env = self.env(&dispatch);
        let data_buf = match self.state.registry.prepare_submission(
            self.plan.buffer,
            self.plan.buffer_offset,
            buf_len,
            env,
        ) {
            Ok(data_buf) => data_buf,
            Err(error) => {
                return Err(self.attach_stage_error(
                    error,
                    "rio.core.submit_txn.prepare_buffer",
                    "failed to prepare RIO data buffer",
                ));
            }
        };
        self.data_buf = Some(data_buf);
        Ok(self)
    }

    fn prepare_address(mut self) -> RioResult<Self> {
        let dispatch = self.dispatch_for_stage(
            "rio.core.submit_txn.prepare_address",
            "failed to load RIO dispatch while preparing address",
        )?;
        let env = self.env(&dispatch);
        let addr = match self.state.prepare_submit_address(self.plan.address, env) {
            Ok(addr) => addr,
            Err(error) => {
                return Err(self.attach_stage_error(
                    error,
                    "rio.core.submit_txn.prepare_address",
                    "failed to prepare RIO address buffer",
                ));
            }
        };
        self.addr = addr;
        Ok(self)
    }

    fn encode_context(mut self) -> RioResult<Self> {
        let rq = self.rq.expect("RIO submit transaction missing actor RQ");
        let socket_inflight = self
            .socket_inflight
            .expect("RIO submit transaction missing socket inflight token");
        let data_buf = self
            .data_buf
            .expect("RIO submit transaction missing prepared data buffer");
        let addr = self.addr;
        let diagnostics = RioRequestDiagnostics::new(rq, &data_buf.rio_buf, addr.as_ref());
        let request_id = self.state.next_request_id();
        let socket_key = socket_inflight.socket_key();
        let addr_slot = addr.map(|addr| addr.slot);
        let context = self.state.encode_req_ctx(RioOpRequestInit {
            token: self.plan.token,
            socket_inflight,
            op_kind: self.plan.op_kind,
            request_id,
            addr_slot,
            buffer_lease: data_buf.lease,
            diagnostics,
        });
        self.request = Some(RioPreparedRequest {
            op_kind: self.plan.op_kind,
            request_id,
            rq,
            context: Some(context),
            token: self.plan.token,
            socket_key,
            addr_slot,
            data_buf,
            addr,
            diagnostics,
            outstanding_snapshot: self.outstanding_snapshot,
        });
        Ok(self)
    }

    fn submit_kernel(
        mut self,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<Self> {
        self.acquire_buffer_ref()?;
        let error_context = self.plan.submit_error_context();
        let submit_result = {
            let request = self
                .request
                .as_ref()
                .expect("RIO submit transaction missing encoded request");
            submit(&self.state.kernel, request)
                .map_err(|error| request.attach_submit_error(error, error_context))
        };

        match submit_result {
            Ok(()) => Ok(self),
            Err(error) => Err(self.attach_stage_error(
                error,
                "rio.core.submit_txn.submit_kernel",
                "failed to submit RIO request to kernel",
            )),
        }
    }

    fn commit(mut self) -> RioResult<SubmissionResult> {
        let request = self
            .request
            .as_mut()
            .expect("RIO submit transaction missing committed request");
        let submitted_context = request.mark_submitted();
        debug_assert!(!submitted_context.as_request_context().is_null());
        self.state.outstanding_count += 1;
        self.submitted = true;
        Ok(SubmissionResult::Pending)
    }

    fn acquire_buffer_ref(&mut self) -> RioResult<()> {
        if self.buffer_ref_acquired {
            return Ok(());
        }
        let request = self
            .request
            .as_ref()
            .expect("RIO submit transaction missing request before buffer ref acquire");
        match self
            .state
            .registry
            .acquire_buffer_lease(request.data_buf.lease)
        {
            Ok(()) => {
                self.buffer_ref_acquired = true;
                Ok(())
            }
            Err(error) => Err(self.attach_stage_error(
                error
                    .with_ctx("data_buffer_id", request.diagnostics.data_buffer_id)
                    .with_ctx("data_buffer_offset", request.diagnostics.data_buffer_offset)
                    .with_ctx("data_buffer_length", request.diagnostics.data_buffer_length),
                "rio.core.submit_txn.acquire_buffer_ref",
                "failed to acquire RIO buffer lease before kernel submit",
            )),
        }
    }

    fn rollback_buffer_ref(&mut self) {
        if !self.buffer_ref_acquired {
            return;
        }

        let Some(data_buf) = self.data_buf else {
            tracing::error!("unsubmitted RIO buffer ref missing prepared buffer");
            return;
        };

        let release = if let Some(dispatch) = self.state.kernel.dispatch {
            let env = self.env(&dispatch);
            self.state
                .registry
                .release_buffer_lease(data_buf.lease, env)
        } else {
            self.state
                .registry
                .release_buffer_lease_deferred(data_buf.lease)
        };

        match release {
            Ok(()) => {
                self.buffer_ref_acquired = false;
            }
            Err(error) => {
                tracing::error!(
                    report = ?error,
                    rio_op_kind = self.plan.op_kind.as_str(),
                    rio_request_id = self
                        .request
                        .as_ref()
                        .map_or(0, |request| request.request_id),
                    "failed to roll back unsubmitted RIO buffer lease"
                );
            }
        }
    }

    fn rollback_context(&mut self) {
        let Some(request) = self.request.as_mut() else {
            return;
        };
        let init = request.take_init(&mut self.state.registry);
        debug_assert!(
            init.is_some(),
            "unsubmitted RIO request missing prepared request context"
        );
    }

    fn rollback_address(&mut self) {
        self.state
            .registry
            .free_addr_slot(self.addr.take().map(|addr| addr.slot));
    }

    fn rollback_socket(&mut self) {
        if let Some(socket_inflight) = self.socket_inflight.take() {
            self.state.release_socket_inflight_token(socket_inflight);
        }
    }

    #[inline]
    fn socket_key(&self) -> SocketKey {
        self.plan.handle.raw().actor_key()
    }

    #[inline]
    fn env<'d>(&self, dispatch: &'d RioDispatch) -> RioEnv<'d>
    where
        'a: 'd,
    {
        RioEnv {
            registrar: self.registrar,
            dispatch,
            cq: self.state.kernel.cq,
            registration_mode: self.state.registration_mode,
        }
    }

    fn dispatch_for_stage(
        &self,
        scope: &'static str,
        note: &'static str,
    ) -> RioResult<RioDispatch> {
        match self
            .state
            .kernel
            .dispatch
            .ok_or(self.plan.dispatch_error)
            .attach_note(self.plan.dispatch_note)
        {
            Ok(dispatch) => Ok(dispatch),
            Err(error) => Err(self.attach_stage_error(error, scope, note)),
        }
    }

    fn attach_stage_error(
        &self,
        error: Report<RioError>,
        scope: &'static str,
        note: &'static str,
    ) -> Report<RioError> {
        let socket_key = self.socket_key();
        let mut report = error
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", self.plan.fd.fixed_index())
            .with_ctx("fd_generation", self.plan.fd.generation())
            .with_ctx("handle_raw", self.plan.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("user_data", self.plan.token.index())
            .with_ctx("generation", self.plan.token.generation())
            .with_ctx("rio_op_kind", self.plan.op_kind.as_str())
            .with_ctx("rio_operation", self.plan.operation)
            .with_ctx("addr_slot", self.addr.map_or(usize::MAX, |addr| addr.slot))
            .with_ctx("outstanding_count", self.outstanding_snapshot)
            .attach_note(note);

        if let Some(rq) = self.rq {
            report = report.with_ctx("rq_raw", rq.0 as usize);
        }
        if let Some(request) = self.request.as_ref() {
            report = report.with_ctx("rio_request_id", request.request_id);
        }
        if let Some(diagnostics) = self.diagnostics_snapshot() {
            report = report
                .with_ctx("data_buffer_id", diagnostics.data_buffer_id)
                .with_ctx("data_buffer_offset", diagnostics.data_buffer_offset)
                .with_ctx("data_buffer_length", diagnostics.data_buffer_length)
                .with_ctx("addr_buffer_id", diagnostics.addr_buffer_id)
                .with_ctx("addr_buffer_offset", diagnostics.addr_buffer_offset)
                .with_ctx("addr_buffer_length", diagnostics.addr_buffer_length);
        }

        report
    }

    fn diagnostics_snapshot(&self) -> Option<RioRequestDiagnostics> {
        let rq = self.rq?;
        let data_buf = self.data_buf.as_ref()?;
        Some(RioRequestDiagnostics::new(
            rq,
            &data_buf.rio_buf,
            self.addr.as_ref(),
        ))
    }
}

impl Drop for RioSubmitTxn<'_> {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        self.rollback_buffer_ref();
        self.rollback_context();
        self.rollback_address();
        self.rollback_socket();
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
    pub(crate) fn encode_req_ctx(&mut self, init: RioOpRequestInit) -> RioPreparedRequestContext {
        self.registry.alloc_request_context(init)
    }

    pub(crate) fn submit_rio(
        &mut self,
        plan: RioSubmitPlan<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        RioSubmitTxn::new(self, plan, registrar)
            .check_socket_accepting()?
            .ensure_actor()?
            .acquire_socket()?
            .prepare_buffer()?
            .prepare_address()?
            .encode_context()?
            .submit_kernel(submit)?
            .commit()
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
    pub(crate) fn decode_req_ctx_checked(&mut self, ctx: u64) -> RioRequestContextDecode {
        self.registry.decode_request_context_checked(ctx)
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
    use crate::BufferRegistrationMode;
    use crate::config::{IocpHandle, RawHandle};
    use crate::net::addr::SockAddrStorage;
    use crate::rio::core::registry::{RioRegistry, test_helpers};
    use std::cell::Cell;
    use std::sync::atomic::Ordering;

    fn test_state_with_dispatch(addr_capacity: usize) -> RioState {
        let mut kernel = RioKernel::noop();
        kernel.dispatch = Some(test_helpers::test_dispatch());
        RioState {
            kernel,
            registry: RioRegistry::new(32, addr_capacity),
            registration_mode: BufferRegistrationMode::Strict,
            submissions_closed: false,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: rustc_hash::FxHashMap::default(),
            socket_runtime: rustc_hash::FxHashMap::default(),
            outstanding_count: 0,
            next_request_id: 0,
            deferred_payloads: Vec::new(),
            diagnostics: crate::driver::IocpDriverCompletionDiagnostics::default(),
        }
    }

    fn test_plan<'a>(
        handle: crate::config::BorrowedRawHandle<'a>,
        buffer: &'a veloq_buf::FixedBuf,
        address: RioAddressPolicy,
    ) -> RioSubmitPlan<'a> {
        RioSubmitPlan {
            fd: IoFd::fixed_with_generation(7, 9),
            handle,
            token: OpToken::from_registry_parts(11, 17).expect("test token should be encodable"),
            op_kind: RioOpKind::RecvFrom,
            buffer_kind: RioSubmissionKind::Recv,
            buffer,
            buffer_offset: 0,
            operation: "test_recv_from",
            address,
            dispatch_error: RioError::Internal,
            dispatch_note: "test dispatch missing",
            submit_scope: "rio.core.tests.submit",
            submit_note: "test submit failed",
        }
    }

    fn test_req_init(addr_slot: Option<usize>) -> RioOpRequestInit {
        let socket_key = IocpHandle::for_socket(std::ptr::null_mut());
        RioOpRequestInit {
            token: OpToken::from_registry_parts(11, 17).expect("test token should be encodable"),
            socket_inflight: SocketInflightToken::new(socket_key),
            op_kind: RioOpKind::Recv,
            request_id: 23,
            addr_slot,
            buffer_lease: None,
            diagnostics: RioRequestDiagnostics::default(),
        }
    }

    #[test]
    fn submit_txn_rejects_closing_socket_before_buffer_or_address_prepare() {
        let _guard = test_helpers::lock_dispatch_state();
        test_helpers::reset_dispatch_state();
        let mut state = test_state_with_dispatch(4);
        let socket = IocpHandle::for_socket(1 as _);
        state.mark_socket_registered(socket);
        assert!(state.begin_socket_cleanup(socket));

        let raw = RawHandle::new(socket);
        let buf = test_helpers::fixed_buf(64, 16);
        let mut addr = SockAddrStorage::default();
        let plan = test_plan(
            raw.borrow(),
            &buf,
            RioAddressPolicy::RecvFrom {
                addr_ptr: (&mut addr as *mut SockAddrStorage).cast(),
            },
        );

        let err = match state.submit_rio(plan, &veloq_buf::NoopRegistrar, |_kernel, _request| {
            panic!("closing socket should fail before kernel submit")
        }) {
            Ok(_) => panic!("closing socket should reject submission transaction"),
            Err(error) => error,
        };

        assert_eq!(*err.inner(), RioError::InvalidInput);
        assert_eq!(state.outstanding_count, 0);
        assert_eq!(state.socket_runtime.get(&socket).unwrap().inflight, 0);
        assert!(state.actors.is_empty());
        assert!(state.actor_by_handle.is_empty());
        assert_eq!(state.registry.addr_free_slots.len(), 4);
        assert!(
            state
                .registry
                .addr_slot_in_use
                .iter()
                .all(|in_use| !*in_use)
        );
        assert!(state.registry.heap_rio_bufs.is_empty());
        assert_eq!(test_helpers::NEXT_REGISTER_ID.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn submit_txn_rolls_back_resources_after_kernel_submit_failure() {
        let _guard = test_helpers::lock_dispatch_state();
        test_helpers::reset_dispatch_state();
        let mut state = test_state_with_dispatch(4);
        let socket = IocpHandle::for_socket(2 as _);
        state.mark_socket_registered(socket);

        let raw = RawHandle::new(socket);
        let buf = test_helpers::fixed_buf(64, 16);
        let mut addr = SockAddrStorage::default();
        let request_context = Cell::new(0_u64);
        let plan = test_plan(
            raw.borrow(),
            &buf,
            RioAddressPolicy::RecvFrom {
                addr_ptr: (&mut addr as *mut SockAddrStorage).cast(),
            },
        );

        let err = match state.submit_rio(plan, &veloq_buf::NoopRegistrar, |_kernel, request| {
            request_context.set(request.as_request_context() as usize as u64);
            Err(RioError::Datapath
                .to_report()
                .attach_note("synthetic submit failure"))
        }) {
            Ok(_) => panic!("kernel submit failure should roll back transaction"),
            Err(error) => error,
        };

        assert_eq!(*err.inner(), RioError::Datapath);
        assert_ne!(request_context.get(), 0);
        assert!(
            state
                .registry
                .decode_request_context(request_context.get())
                .is_none()
        );
        assert_eq!(state.outstanding_count, 0);
        assert_eq!(state.socket_runtime.get(&socket).unwrap().inflight, 0);
        assert_eq!(state.registry.addr_free_slots.len(), 4);
        assert!(
            state
                .registry
                .addr_slot_in_use
                .iter()
                .all(|in_use| !*in_use)
        );
        assert!(
            state
                .registry
                .heap_rio_bufs
                .values()
                .all(|entry| entry.active_refs == 0)
        );
    }

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let mut registry = RioRegistry::new(32, 1);
        let context = registry.alloc_request_context(test_req_init(None));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = registry.decode_request_context(raw);
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
            }) if token
                == OpToken::from_registry_parts(11, 17)
                    .expect("test token should be encodable")));
    }

    #[test]
    fn op_ctx_with_addr_roundtrip_decode_and_free() {
        let mut registry = RioRegistry::new(32, 1);
        let context = registry.alloc_request_context(test_req_init(Some(3)));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = registry.decode_request_context(raw);
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
            }) if token
                == OpToken::from_registry_parts(11, 17)
                    .expect("test token should be encodable")));
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
        let mut registry = RioRegistry::new(32, 1);
        assert!(registry.decode_request_context(0).is_none());
    }

    #[test]
    fn decode_unknown_context_does_not_deref_raw_pointer() {
        let mut registry = RioRegistry::new(32, 1);
        assert!(
            registry
                .decode_request_context(0xa700_0002_0000_0001)
                .is_none()
        );
    }
}
