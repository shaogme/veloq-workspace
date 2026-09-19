use super::{
    registry::{RioAddrReservation, RioPreparedBuffer},
    request::{
        RioAddressPolicy, RioOpRequestInit, RioPreparedRequest, RioRequestDiagnostics,
        RioSubmitPlan,
    },
    submit_ops::{RioDispatch, RioKernel, RioRq},
};
use crate::{
    config::SocketKey,
    op::SubmissionResult,
    rio::{
        RioEnv, RioState, SocketInflightToken, SocketLifecycleState,
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_std::string::ToString;

struct RioSubmitTxn<'a> {
    state: &'a mut RioState,
    plan: RioSubmitPlan<'a>,
    registrar: &'a dyn BufferRegistrar,
    rq: Option<RioRq>,
    socket_inflight: Option<SocketInflightToken>,
    data_buf: Option<RioPreparedBuffer>,
    addr: Option<RioAddrReservation>,
    request: Option<RioPreparedRequest>,
    buffer_ref_acquired: bool,
    submitted: bool,
    outstanding_snapshot: usize,
}

pub(crate) struct RioSubmittedRequest {
    pub(crate) request_id: u64,
    pub(crate) result: SubmissionResult,
}

impl<'a> RioSubmitTxn<'a> {
    fn new(
        state: &'a mut RioState,
        plan: RioSubmitPlan<'a>,
        registrar: &'a dyn BufferRegistrar,
    ) -> Self {
        let outstanding_snapshot = state.rio_outstanding_count;
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
                .with_ctx("rio_outstanding_count", self.state.rio_outstanding_count)
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
        let socket_inflight = self.socket_inflight.as_ref().ok_or_else(|| {
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
        let diagnostics = RioRequestDiagnostics::with_receive_slot(
            rq,
            &data_buf.rio_buf,
            addr.as_ref(),
            self.plan.receive_slot_id,
            self.plan.receive_slot_generation,
        );
        let request_id = socket_inflight.request_id();
        let socket_key = socket_inflight.socket_key();
        let addr_slot = addr.map(|addr| addr.slot);
        let addr_generation = addr.map(|addr| addr.generation);
        let socket_inflight = self
            .socket_inflight
            .take()
            .expect("RIO submit transaction socket inflight token disappeared");
        let context = self.state.encode_req_ctx(RioOpRequestInit {
            token: self.plan.token,
            socket_inflight,
            op_kind: self.plan.op_kind,
            request_id,
            addr_slot,
            addr_generation,
            addr,
            buffer_lease: data_buf.lease,
            receive_slot_id: self.plan.receive_slot_id,
            receive_slot_generation: self.plan.receive_slot_generation,
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
            addr_generation,
            data_buf,
            addr,
            receive_slot_id: self.plan.receive_slot_id,
            receive_slot_generation: self.plan.receive_slot_generation,
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

    fn commit(mut self) -> RioResult<RioSubmittedRequest> {
        let request = if let Some(ref mut request) = self.request {
            request
        } else {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.commit",
                "RIO submit transaction missing committed request",
            ));
        };
        if request.as_request_context().is_null() {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.commit",
                "RIO submitted request context is null",
            ));
        }
        let Some(next_outstanding) = self.state.rio_outstanding_count.checked_add(1) else {
            return Err(self.attach_stage_error(
                RioError::ResourceExhaustion.to_report(),
                "rio.core.submit_txn.commit",
                "RIO outstanding request count overflow",
            ));
        };
        self.state
            .record_request_submitted(request.op_kind, request.socket_key)?;
        let _submitted_context = request.mark_submitted();
        self.state.rio_outstanding_count = next_outstanding;
        self.submitted = true;
        Ok(RioSubmittedRequest {
            request_id: request.request_id,
            result: SubmissionResult::Pending,
        })
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
        let Some(init) = request.take_init(&mut self.state.registry) else {
            return Err(self.attach_stage_error(
                RioError::Internal.to_report(),
                "rio.core.submit_txn.rollback_context",
                "unsubmitted RIO request missing prepared request context during rollback",
            ));
        };
        self.state
            .release_socket_inflight_token(init.socket_inflight)?;
        Ok(())
    }

    fn rollback_address(&mut self) {
        if let Err(error) = self.state.registry.free_addr_reservation(self.addr.take()) {
            tracing::error!(report = ?error, "failed to roll back RIO address lease");
        }
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
            .with_ctx("fd", self.plan.fd.to_string())
            .with_ctx("handle_raw", self.plan.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("user_data", self.plan.token.index())
            .with_ctx("generation", self.plan.token.generation())
            .with_ctx("rio_op_kind", self.plan.op_kind.as_str())
            .with_ctx("rio_operation", self.plan.operation)
            .with_ctx("addr_slot", self.addr.map_or(usize::MAX, |addr| addr.slot))
            .with_ctx("rio_outstanding_count", self.outstanding_snapshot)
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
        registrar: &dyn BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        self.submit_rio_with_request(plan, registrar, submit)
            .map(|submitted| submitted.result)
    }

    pub(crate) fn submit_rio_with_request(
        &mut self,
        plan: RioSubmitPlan<'_>,
        registrar: &dyn BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<RioSubmittedRequest> {
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
            RioAddressPolicy::RecvMulti => self.registry.prepare_recv_addr(env).map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            RioRegistry, RioSubmissionKind,
            registry::test_helpers::{self, NEXT_REGISTER_ID},
        },
        *,
    };
    use crate::{
        BufferRegistrationMode,
        config::{BorrowedRawHandle, IoFd, IocpHandle, RawHandle},
        driver::IocpDriverCompletionDiagnostics,
        rio::core::{RioCompletionKind, RioOpKind, RioRequestContextDecode},
    };
    use slotmap::SlotMap;
    use veloq_buf::{FixedBuf, NoopRegistrar};
    use veloq_driver_core::{driver::OpToken, slot::Generation};
    use veloq_std::{cell::Cell, collections::FastHashMap, sync::atomic::Ordering, vec::Vec};

    fn test_state_with_dispatch(addr_capacity: usize) -> RioState {
        let mut kernel = RioKernel::noop();
        kernel.dispatch = Some(test_helpers::test_dispatch());
        RioState {
            kernel,
            registry: RioRegistry::new(32, addr_capacity),
            registration_mode: BufferRegistrationMode::Strict,
            submissions_closed: false,
            actors: SlotMap::with_key(),
            actor_by_handle: FastHashMap::default(),
            socket_runtime: FastHashMap::default(),
            rio_outstanding_count: 0,
            next_request_id: 0,
            deferred_kernel_ops: Vec::new(),
            deferred_payloads: Vec::new(),
            diagnostics: IocpDriverCompletionDiagnostics::default(),
            cq_armed: true,
        }
    }

    fn test_plan<'a>(
        handle: BorrowedRawHandle<'a>,
        buffer: &'a FixedBuf,
        address: RioAddressPolicy,
    ) -> RioSubmitPlan<'a> {
        RioSubmitPlan {
            fd: IoFd::fixed_with_generation(7, 9),
            handle,
            token: OpToken::from_registry_parts(11, Generation::new(17))
                .expect("test token should be encodable"),
            op_kind: RioOpKind::UdpRecvMulti,
            buffer_kind: RioSubmissionKind::Recv,
            buffer,
            buffer_offset: 0,
            operation: "test_recv_multi",
            address,
            dispatch_error: RioError::Internal,
            dispatch_note: "test dispatch missing",
            submit_scope: "rio.core.tests.submit",
            submit_note: "test submit failed",
            receive_slot_id: None,
            receive_slot_generation: None,
        }
    }

    fn multi_plan<'a>(
        handle: BorrowedRawHandle<'a>,
        buffer: &'a FixedBuf,
        slot_id: u32,
        slot_generation: u32,
    ) -> RioSubmitPlan<'a> {
        let mut plan = test_plan(handle, buffer, RioAddressPolicy::RecvMulti);
        plan.op_kind = RioOpKind::UdpRecvMulti;
        plan.operation = "test_recv_multi";
        plan.receive_slot_id = Some(slot_id);
        plan.receive_slot_generation = Some(slot_generation);
        plan
    }

    fn release_request_init(state: &mut RioState, init: RioOpRequestInit) {
        let dispatch = test_helpers::test_dispatch();
        let env = test_helpers::test_env(&dispatch);
        state
            .registry
            .free_addr_reservation(init.addr)
            .expect("address lease should release");
        state
            .registry
            .release_buffer_lease(init.buffer_lease, env)
            .expect("buffer lease should release");
        state
            .release_request_inflight(init.op_kind, init.socket_inflight)
            .expect("socket inflight token should release");
        state.rio_outstanding_count -= 1;
    }

    #[test]
    fn multishot_submit_keeps_request_identity_and_rio_receive_ex_shape() {
        let _guard = test_helpers::lock_dispatch_state();
        test_helpers::reset_dispatch_state();
        let mut state = test_state_with_dispatch(2);
        let socket = IocpHandle::for_socket(3 as _);
        state.mark_socket_registered(socket);
        let raw = RawHandle::new(socket);
        let first_buf = test_helpers::fixed_buf(64, 0);
        let second_buf = test_helpers::fixed_buf(64, 0);
        let first_context = Cell::new(0_u64);
        let second_context = Cell::new(0_u64);

        let first = state
            .submit_rio_with_request(
                multi_plan(raw.borrow(), &first_buf, 7, 11),
                &NoopRegistrar,
                |kernel, request| {
                    first_context.set(request.as_request_context() as usize as u64);
                    let addr = request.addr.as_ref().expect("multishot address lease");
                    kernel.submit_receive_multi(
                        request.rq,
                        &request.data_buf.rio_buf,
                        &addr.rio_buf,
                        request.as_request_context(),
                    )
                },
            )
            .expect("first multishot submit should succeed");
        let second = state
            .submit_rio_with_request(
                multi_plan(raw.borrow(), &second_buf, 8, 3),
                &NoopRegistrar,
                |kernel, request| {
                    second_context.set(request.as_request_context() as usize as u64);
                    let addr = request.addr.as_ref().expect("multishot address lease");
                    kernel.submit_receive_multi(
                        request.rq,
                        &request.data_buf.rio_buf,
                        &addr.rio_buf,
                        request.as_request_context(),
                    )
                },
            )
            .expect("second multishot submit should succeed");

        assert_ne!(first.request_id, second.request_id);
        assert_eq!(state.rio_outstanding_count, 2);
        assert_eq!(state.registry.addr_free_slots.len(), 0);
        assert_eq!(test_helpers::RECEIVE_EX_CALLS.load(Ordering::SeqCst), 2);
        assert_eq!(
            test_helpers::RECEIVE_EX_DATA_COUNT.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            test_helpers::RECEIVE_EX_REMOTE_ADDR.load(Ordering::SeqCst),
            1
        );
        assert_eq!(test_helpers::RECEIVE_EX_FLAGS.load(Ordering::SeqCst), 0);

        for (raw_context, slot_id, generation) in [
            (first_context.get(), 7_u32, 11_u32),
            (second_context.get(), 8_u32, 3_u32),
        ] {
            let init = match state.decode_req_ctx_checked(raw_context) {
                RioRequestContextDecode::Valid(kind) => {
                    let RioCompletionKind::Op { init, .. } = *kind;
                    init
                }
                _ => panic!("multishot request context should decode"),
            };
            assert_eq!(init.token.index(), 11);
            assert_eq!(init.op_kind, RioOpKind::UdpRecvMulti);
            assert_eq!(init.receive_slot_id, Some(slot_id));
            assert_eq!(init.receive_slot_generation, Some(generation));
            assert_eq!(init.diagnostics.receive_slot_id, Some(slot_id));
            assert_eq!(init.diagnostics.receive_slot_generation, Some(generation));
            release_request_init(&mut state, init);
        }
        assert_eq!(state.rio_outstanding_count, 0);
        assert_eq!(state.registry.addr_free_slots.len(), 2);
    }

    #[test]
    fn submit_txn_rejects_closing_socket_before_buffer_or_address_prepare() {
        let _guard = test_helpers::lock_dispatch_state();
        test_helpers::reset_dispatch_state();
        let mut state = test_state_with_dispatch(4);
        let socket = IocpHandle::for_socket(1 as _);
        state.mark_socket_registered(socket);
        assert!(state.begin_socket_cleanup(socket).unwrap());

        let raw = RawHandle::new(socket);
        let buf = test_helpers::fixed_buf(64, 16);
        let plan = test_plan(raw.borrow(), &buf, RioAddressPolicy::RecvMulti);

        let err = match state.submit_rio(plan, &NoopRegistrar, |_kernel, _request| {
            panic!("closing socket should fail before kernel submit")
        }) {
            Ok(_) => panic!("closing socket should reject submission transaction"),
            Err(error) => error,
        };

        assert_eq!(*err.inner(), RioError::InvalidInput);
        assert_eq!(state.rio_outstanding_count, 0);
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
        assert_eq!(NEXT_REGISTER_ID.load(Ordering::SeqCst), 100);
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
        let request_context = Cell::new(0_u64);
        let plan = test_plan(raw.borrow(), &buf, RioAddressPolicy::RecvMulti);

        let err = match state.submit_rio(plan, &NoopRegistrar, |_kernel, request| {
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
        assert_eq!(state.rio_outstanding_count, 0);
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
