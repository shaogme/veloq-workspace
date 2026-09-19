//! Kernel-facing RIO dispatch table and submission primitives.
//!
//! This module encapsulates:
//! - CQ creation/notification lifecycle,
//! - minimal wrappers for `RIOReceive`, `RIOSend`, and `RIOSendEx`,
//! - `RioState` constructors and basic registration entry points.
//!
//! It forms the low-level boundary between high-level runtime orchestration and
//! Windows RIO APIs, keeping unsafe calls and pointer setup in one place.

mod dispatch;
pub(crate) use dispatch::*;

use crate::{
    BufferRegistrationMode,
    config::BorrowedRawHandle,
    driver::IocpDriverCompletionDiagnostics,
    ext::Extensions,
    op::SubmissionResult,
    rio::{
        RioState, RioTarget,
        core::{
            RioAddressPolicy, RioOpKind, RioRegistry, RioSubmissionKind, RioSubmitPlan,
            RioSubmittedRequest,
        },
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_buf::{BufferRegistrar, FixedBuf, NoopRegistrar, heap::ChunkId};
use veloq_driver_core::driver::BufferRegistrationStatus;
use veloq_driver_core::platform::receive_pump::{
    ReceivePumpError, ReceivePumpState, ReceiveReplacementReservation,
};
use veloq_std::{collections::FastHashMap, string::ToString, vec::Vec};

impl RioState {
    pub(crate) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
        diagnostics: IocpDriverCompletionDiagnostics,
    ) -> RioResult<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;

        // Keep per-socket RQ depth conservative so that multi-socket warmup
        // does not exhaust RIO request-queue resources too early.
        let rq_depth = entries.clamp(32, 64);

        Ok(Self {
            kernel,
            registry: RioRegistry::new(rq_depth, entries as usize),
            registration_mode,
            submissions_closed: false,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: FastHashMap::default(),
            socket_runtime: FastHashMap::default(),
            rio_outstanding_count: 0,
            next_request_id: 0,
            deferred_kernel_ops: Vec::new(),
            deferred_payloads: Vec::new(),
            diagnostics,
            cq_armed: true,
        })
    }

    pub(crate) fn register_buffer_backend(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> RioResult<BufferRegistrationStatus> {
        let Some(env) = self.kernel.env(&NoopRegistrar, self.registration_mode) else {
            return Err(RioError::NotSupported
                .to_report()
                .attach_note("RIO buffer registration is unavailable without a dispatch table"));
        };
        self.registry
            .register_buffer_backend(id, (ptr, len), env)
            .map(|_| BufferRegistrationStatus::Registered)
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        target: RioTarget<'_>,
        buf: &mut FixedBuf,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_internal(RioOpKind::Recv, target, buf, registrar)
    }

    pub(crate) fn try_submit_recv_provided(
        &mut self,
        target: RioTarget<'_>,
        buf: &mut FixedBuf,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_internal(RioOpKind::RecvProvided, target, buf, registrar)
    }

    fn try_submit_recv_internal(
        &mut self,
        op_kind: RioOpKind,
        target: RioTarget<'_>,
        buf: &mut FixedBuf,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind,
                buffer_kind: RioSubmissionKind::Recv,
                buffer: buf,
                buffer_offset: buf_offset,
                operation,
                address: RioAddressPolicy::None,
                dispatch_error: RioError::NotSupported,
                dispatch_note: "RIO not supported or dispatch table missing",
                submit_scope: "rio.core.submit_ops.try_submit_recv_internal",
                submit_note: "RIOReceive submit failed",
                receive_slot_id: None,
                receive_slot_generation: None,
            },
            registrar,
            |kernel, request| {
                kernel.submit_receive(
                    request.rq,
                    &request.data_buf.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: RioTarget<'_>,
        buf: &FixedBuf,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::Send,
                buffer_kind: RioSubmissionKind::Send,
                buffer: buf,
                buffer_offset: buf_offset,
                operation,
                address: RioAddressPolicy::None,
                dispatch_error: RioError::NotSupported,
                dispatch_note: "RIO not supported or dispatch table missing",
                submit_scope: "rio.core.submit_ops.try_submit_send",
                submit_note: "RIOSend submit failed",
                receive_slot_id: None,
                receive_slot_generation: None,
            },
            registrar,
            |kernel, request| {
                kernel.submit_send(
                    request.rq,
                    &request.data_buf.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_recv_multi_slot(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        buf: &FixedBuf,
        slot_id: u32,
        slot_generation: u32,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<RioSubmittedRequest> {
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        let address = match op_kind {
            RioOpKind::TcpRecvMulti => RioAddressPolicy::None,
            RioOpKind::UdpRecvMulti => RioAddressPolicy::RecvMulti,
            _ => {
                return Err(RioError::InvalidInput
                    .to_report()
                    .attach_note("invalid RIO receive multishot operation kind"));
            }
        };
        self.submit_rio_with_request(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind,
                buffer_kind: RioSubmissionKind::Recv,
                buffer: buf,
                buffer_offset: buf_offset,
                operation,
                address,
                dispatch_error: RioError::NotSupported,
                dispatch_note: "RIO not supported or dispatch table missing",
                submit_scope: "rio.core.submit_ops.try_submit_recv_multi_slot",
                submit_note: "RIO receive multishot submit failed",
                receive_slot_id: Some(slot_id),
                receive_slot_generation: Some(slot_generation),
            },
            registrar,
            |kernel, request| match op_kind {
                RioOpKind::TcpRecvMulti => kernel.submit_receive(
                    request.rq,
                    &request.data_buf.rio_buf,
                    request.as_request_context(),
                ),
                RioOpKind::UdpRecvMulti => {
                    let Some(addr) = request.addr.as_ref() else {
                        return RioError::Internal
                            .attach_note("RIO UDP recv_multi missing prepared address");
                    };
                    kernel.submit_receive_multi(
                        request.rq,
                        &request.data_buf.rio_buf,
                        &addr.rio_buf,
                        request.as_request_context(),
                    )
                }
                _ => Err(RioError::InvalidInput
                    .to_report()
                    .attach_note("invalid RIO receive multishot operation kind")),
            },
        )
    }

    pub(crate) fn try_submit_recv_multi_replacement(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        pump: &mut ReceivePumpState,
        reservation: ReceiveReplacementReservation,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<RioSubmittedRequest> {
        let Some(slot) = pump.slot(reservation.slot_id()) else {
            return Err(receive_pump_error(ReceivePumpError::ReceiveContextCorrupt));
        };
        self.try_submit_recv_multi_slot(
            target,
            op_kind,
            slot.data_buf(),
            reservation.slot_id(),
            reservation.slot_generation(),
            registrar,
        )
    }

    pub(crate) fn try_submit_recv_multi_initial(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        pump: &mut ReceivePumpState,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<()> {
        pump.start().map_err(receive_pump_error)?;
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        while let Some(submission) = pump.next_submission() {
            let Some(slot) = pump.slot(submission.slot_id) else {
                return Err(receive_pump_error(ReceivePumpError::ReceiveContextCorrupt));
            };
            let result = self.try_submit_recv_multi_slot(
                RioTarget {
                    fd,
                    handle,
                    token,
                    buf_offset,
                    operation,
                },
                op_kind,
                slot.data_buf(),
                submission.slot_id,
                submission.slot_generation,
                registrar,
            );
            match result {
                Ok(submitted) => {
                    pump.submit_initial(submission, submitted.request_id)
                        .map_err(receive_pump_error)?;
                }
                Err(error) => {
                    let _ = pump.fail_initial_submit(submission);
                    if pump.actual_in_flight() == 0 {
                        return Err(error);
                    }

                    // The logical operation owns every request submitted before this failure.
                    // Keep the driver slot in flight and let the normal RIO completion/reaper
                    // path cancel and drain those requests before releasing the persistent pump.
                    let socket_key = handle.raw().actor_key();
                    self.activate_receive_pump(socket_key, token);
                    if let Err(cancel_error) =
                        self.request_socket_receive_shutdown_for_cleanup(socket_key)
                    {
                        tracing::error!(
                            report = ?cancel_error,
                            socket_raw = socket_key.as_handle() as usize,
                            "failed to request cancellation after partial RIO receive startup"
                        );
                    }
                    return Ok(());
                }
            }
        }
        if !pump.is_ready() {
            return Err(RioError::Internal
                .to_report()
                .attach_note("RIO receive pump initial arm did not reach target depth"));
        }
        self.activate_receive_pump(handle.raw().actor_key(), token);
        Ok(())
    }
}

fn receive_pump_error(error: ReceivePumpError) -> diagweave::Report<RioError> {
    RioError::InvalidInput
        .to_report()
        .with_ctx("receive_pump_error", error.to_string())
        .attach_note("RIO receive pump state transition failed")
}
