use super::registry::{RioAddrReservation, RioPreparedBuffer};
use super::request::{
    RioAddressPolicy, RioOpRequestInit, RioPreparedRequest, RioRequestDiagnostics, RioSubmitPlan,
};
use super::submit_ops::{RioDispatch, RioKernel, RioRq};
use crate::config::SocketKey;
use crate::op::SubmissionResult;
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioEnv, RioState, SocketInflightToken, SocketLifecycleState};
use diagweave::prelude::*;
use windows_sys::Win32::Networking::WinSock::SOCKADDR_INET;

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
        let rq = self.rq.ok_or_else(|| {
            self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.encode_context",
                "RIO submit transaction missing actor RQ",
            )
        })?;
        let socket_inflight = self.socket_inflight.ok_or_else(|| {
            self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.encode_context",
                "RIO submit transaction missing socket inflight token",
            )
        })?;
        let data_buf = self.data_buf.ok_or_else(|| {
            self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.encode_context",
                "RIO submit transaction missing prepared data buffer",
            )
        })?;
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
        let request = if let Some(ref request) = self.request {
            request
        } else {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.submit_kernel",
                "RIO submit transaction missing encoded request",
            ));
        };

        match submit(&self.state.kernel, request) {
            Ok(()) => Ok(self),
            Err(error) => Err(self.attach_stage_error(
                request.attach_submit_error(error, error_context),
                "rio.core.submit_txn.submit_kernel",
                "failed to submit RIO request to kernel",
            )),
        }
    }

    fn commit(mut self) -> RioResult<SubmissionResult> {
        let request = if let Some(ref mut request) = self.request {
            request
        } else {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.commit",
                "RIO submit transaction missing committed request",
            ));
        };
        let submitted_context = request.mark_submitted();
        if submitted_context.as_request_context().is_null() {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.commit",
                "RIO submitted request context is null",
            ));
        }
        self.state.outstanding_count += 1;
        self.submitted = true;
        Ok(SubmissionResult::Pending)
    }

    fn acquire_buffer_ref(&mut self) -> RioResult<()> {
        if self.buffer_ref_acquired {
            return Ok(());
        }
        let request = if let Some(ref request) = self.request {
            request
        } else {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.acquire_buffer_ref",
                "RIO submit transaction missing request before buffer ref acquire",
            ));
        };
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

    fn rollback_buffer_ref(&mut self) -> RioResult<()> {
        if !self.buffer_ref_acquired {
            return Ok(());
        }

        let data_buf = self.data_buf.ok_or_else(|| {
            self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.rollback_buffer_ref",
                "unsubmitted RIO buffer ref missing prepared buffer",
            )
        })?;

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
                Ok(())
            }
            Err(error) => Err(self.attach_stage_error(
                error,
                "rio.core.submit_txn.rollback_buffer_ref",
                "failed to roll back unsubmitted RIO buffer lease",
            )),
        }
    }

    fn rollback_context(&mut self) -> RioResult<()> {
        let Some(request) = self.request.as_mut() else {
            return Ok(());
        };
        let init = request.take_init(&mut self.state.registry);
        if init.is_none() {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.rollback_context",
                "unsubmitted RIO request missing prepared request context during rollback",
            ));
        }
        Ok(())
    }

    fn rollback_address(&mut self) {
        self.state
            .registry
            .free_addr_slot(self.addr.take().map(|addr| addr.slot));
    }

    fn rollback_socket(&mut self) -> RioResult<()> {
        if let Some(socket_inflight) = self.socket_inflight.take() {
            self.state.release_socket_inflight_token(socket_inflight)?;
        }
        Ok(())
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
        if let Err(error) = self.rollback_buffer_ref() {
            tracing::error!(
                report = ?error,
                rio_op_kind = self.plan.op_kind.as_str(),
                rio_request_id = self
                    .request
                    .as_ref()
                    .map(|request| request.request_id),
                "failed to roll back unsubmitted RIO buffer reference"
            );
        }
        if let Err(error) = self.rollback_context() {
            tracing::error!(
                report = ?error,
                rio_op_kind = self.plan.op_kind.as_str(),
                rio_request_id = self
                    .request
                    .as_ref()
                    .map(|request| request.request_id),
                "failed to roll back unsubmitted RIO request context"
            );
        }
        self.rollback_address();
        if let Err(error) = self.rollback_socket() {
            tracing::error!(
                report = ?error,
                rio_op_kind = self.plan.op_kind.as_str(),
                rio_request_id = self
                    .request
                    .as_ref()
                    .map(|request| request.request_id),
                "failed to roll back unsubmitted RIO socket inflight token"
            );
        }
    }
}

impl RioState {
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
    fn next_request_id(&mut self) -> u64 {
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }
        self.next_request_id
    }
}

#[cfg(test)]
mod tests {
    use super::super::registry::{RioRegistry, RioSubmissionKind, test_helpers};
    use super::*;
    use crate::BufferRegistrationMode;
    use crate::config::{IoFd, IocpHandle, RawHandle};
    use crate::net::addr::SockAddrStorage;
    use std::cell::Cell;
    use std::sync::atomic::Ordering;
    use veloq_driver_core::driver::OpToken;

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
            op_kind: super::super::RioOpKind::RecvFrom,
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
}
