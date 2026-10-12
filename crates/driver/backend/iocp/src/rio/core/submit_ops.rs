//! Kernel-facing RIO submission primitives. / RIO 内核提交基础类型与调用入口。
//!
//! This module defines operation inputs and delegates Windows calls to `dispatch`.
//! 本模块定义操作输入，并将 Windows 调用委托给 `dispatch`。

use super::{
    RioAddressPolicy, RioCore, RioError, RioOpKind, RioPreparedRequest, RioResult, RioSubmitPlan,
    RioSubmitTxn, RioSubmittedRequest,
};
use crate::{
    IoFd, config::BorrowedRawHandle, net::addr::SockAddrStorage, op::SubmissionResult,
    rio::RioRuntime,
};
use diagweave::prelude::*;
use veloq_buf::{BufferRegistrar, FixedBuf};
use veloq_driver_core::driver::OpToken;
use veloq_driver_core::platform::receive_pump::{
    ReceivePumpError, ReceivePumpState, ReceiveReplacementReservation,
};
use veloq_std::string::ToString;

mod dispatch;
pub(crate) use dispatch::{
    RioBufferId, RioCq, RioDispatch, RioKernel, RioProvider, RioRq, RioRqConfig,
};

#[cfg(test)]
pub(crate) use dispatch::RioDispatchFunctions;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioSubmissionKind {
    Recv,
    Send,
}

impl RioSubmissionKind {
    pub(crate) fn data_len(
        self,
        buf: &FixedBuf,
        buf_offset: usize,
        operation: &'static str,
    ) -> RioResult<u32> {
        let bound = self.bound(buf);
        if buf_offset > bound {
            return RioError::InvalidInput
                .with_ctx("rio_operation", operation)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("buffer_bound", bound)
                .with_ctx("buffer_bound_kind", self.bound_name())
                .attach_note(self.offset_exceeded_note());
        }

        let len = bound - buf_offset;
        u32::try_from(len).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("rio_operation", operation)
                .with_ctx("buffer_offset", buf_offset)
                .with_ctx("buffer_length", buf.len())
                .with_ctx("buffer_capacity", buf.capacity())
                .with_ctx("submission_length", len)
                .attach_note("RIO data buffer length exceeds u32")
        })
    }

    fn bound(self, buf: &FixedBuf) -> usize {
        match self {
            Self::Recv => buf.capacity(),
            Self::Send => buf.len(),
        }
    }

    fn bound_name(self) -> &'static str {
        match self {
            Self::Recv => "capacity",
            Self::Send => "length",
        }
    }

    fn offset_exceeded_note(self) -> &'static str {
        match self {
            Self::Recv => "RIO recv buffer offset exceeds buffer capacity",
            Self::Send => "RIO send buffer offset exceeds buffer length",
        }
    }
}

pub(crate) struct RioTarget<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) buf_offset: usize,
    pub(crate) operation: &'static str,
}

pub(crate) struct RioSendToArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) buf: &'a FixedBuf,
    pub(crate) addr: &'a SockAddrStorage,
    pub(crate) addr_len: i32,
    pub(crate) token: OpToken,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioReceiveSlotSubmit<'a> {
    pub(crate) target: RioTarget<'a>,
    pub(crate) op_kind: RioOpKind,
    pub(crate) buf: &'a FixedBuf,
    pub(crate) slot_id: u32,
    pub(crate) slot_generation: u32,
    pub(crate) registrar: &'a dyn BufferRegistrar,
}

pub(crate) struct RioReceiveReplacementArgs<'a> {
    pub(crate) target: RioTarget<'a>,
    pub(crate) op_kind: RioOpKind,
    pub(crate) pump: &'a mut ReceivePumpState,
    pub(crate) reservation: ReceiveReplacementReservation,
    pub(crate) registrar: &'a dyn BufferRegistrar,
}

pub(crate) struct RioSubmitOperations<'a> {
    core: &'a mut RioCore,
    runtime: &'a mut RioRuntime,
}

impl<'a> RioSubmitOperations<'a> {
    pub(crate) fn new(core: &'a mut RioCore, runtime: &'a mut RioRuntime) -> Self {
        Self { core, runtime }
    }

    fn submit_rio(
        &mut self,
        plan: RioSubmitPlan<'_>,
        registrar: &dyn BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        self.submit_rio_with_request(plan, registrar, submit)
            .map(|submitted| submitted.result)
    }

    fn submit_rio_with_request(
        &mut self,
        plan: RioSubmitPlan<'_>,
        registrar: &dyn BufferRegistrar,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<RioSubmittedRequest> {
        RioSubmitTxn::execute(self.core, self.runtime, plan, registrar, submit)
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr,
            addr_len,
            token,
            buf_offset,
            ..
        } = args;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::SendTo,
                buffer_kind: RioSubmissionKind::Send,
                buffer: buf,
                buffer_offset: buf_offset,
                operation: "send_to",
                address: RioAddressPolicy::SendTo { addr, addr_len },
                dispatch_error: RioError::Internal,
                dispatch_note: "lost RIO context",
                submit_scope: "rio.runtime.try_submit_send_to_internal",
                submit_note: "RIOSendEx submit failed",
                receive_slot_id: None,
                receive_slot_generation: None,
            },
            registrar,
            |kernel, request| {
                let Some(addr) = request.addr() else {
                    return RioError::Internal.attach_note("RIO send_to missing prepared address");
                };
                kernel.submit_send_ex(
                    request.rq(),
                    &request.data_buf().rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            },
        )
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
                    request.rq(),
                    &request.data_buf().rio_buf,
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
                    request.rq(),
                    &request.data_buf().rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_recv_multi_slot(
        &mut self,
        input: RioReceiveSlotSubmit<'_>,
    ) -> RioResult<RioSubmittedRequest> {
        let RioReceiveSlotSubmit {
            target,
            op_kind,
            buf,
            slot_id,
            slot_generation,
            registrar,
        } = input;
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        let address = Self::receive_address_policy(op_kind)?;
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
            |kernel, request| Self::submit_receive_multi(kernel, op_kind, request),
        )
    }

    pub(crate) fn try_submit_recv_multi_replacement(
        &mut self,
        args: RioReceiveReplacementArgs<'_>,
    ) -> RioResult<RioSubmittedRequest> {
        let RioReceiveReplacementArgs {
            target,
            op_kind,
            pump,
            reservation,
            registrar,
        } = args;
        let Some(slot) = pump.slot(reservation.slot_id()) else {
            return Err(Self::receive_pump_error(
                ReceivePumpError::ReceiveContextCorrupt,
            ));
        };
        self.try_submit_recv_multi_slot(RioReceiveSlotSubmit {
            target,
            op_kind,
            buf: slot.data_buf(),
            slot_id: reservation.slot_id(),
            slot_generation: reservation.slot_generation(),
            registrar,
        })
    }

    pub(crate) fn try_submit_recv_multi_initial(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        pump: &mut ReceivePumpState,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<()> {
        pump.start().map_err(Self::receive_pump_error)?;
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        while let Some(submission) = pump.next_submission() {
            let Some(slot) = pump.slot(submission.slot_id) else {
                return Err(Self::receive_pump_error(
                    ReceivePumpError::ReceiveContextCorrupt,
                ));
            };
            let result = self.try_submit_recv_multi_slot(RioReceiveSlotSubmit {
                target: RioTarget {
                    fd,
                    handle,
                    token,
                    buf_offset,
                    operation,
                },
                op_kind,
                buf: slot.data_buf(),
                slot_id: submission.slot_id,
                slot_generation: submission.slot_generation,
                registrar,
            });
            match result {
                Ok(submitted) => {
                    pump.submit_initial(submission, submitted.request_id)
                        .map_err(Self::receive_pump_error)?;
                }
                Err(error) => {
                    let _ = pump.fail_initial_submit(submission);
                    if pump.actual_in_flight() == 0 {
                        return Err(error);
                    }

                    // Earlier requests retain their slots until completion-driven drain. / 先前已提交的请求仍持有槽位，直到完成队列驱动 drain。
                    let socket_key = handle.raw().actor_key();
                    self.runtime.activate_receive_pump(socket_key, token);
                    if let Err(cancel_error) = self
                        .runtime
                        .request_socket_receive_shutdown_for_cleanup(socket_key)
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
        self.runtime
            .activate_receive_pump(handle.raw().actor_key(), token);
        Ok(())
    }

    fn receive_address_policy(op_kind: RioOpKind) -> RioResult<RioAddressPolicy<'static>> {
        match op_kind {
            RioOpKind::TcpRecvMulti => Ok(RioAddressPolicy::None),
            RioOpKind::UdpRecvMulti => Ok(RioAddressPolicy::RecvMulti),
            _ => Err(RioError::InvalidInput
                .to_report()
                .attach_note("invalid RIO receive multishot operation kind")),
        }
    }

    fn submit_receive_multi(
        kernel: &RioKernel,
        op_kind: RioOpKind,
        request: &RioPreparedRequest,
    ) -> RioResult<()> {
        match op_kind {
            RioOpKind::TcpRecvMulti => kernel.submit_receive(
                request.rq(),
                &request.data_buf().rio_buf,
                request.as_request_context(),
            ),
            RioOpKind::UdpRecvMulti => {
                let Some(addr) = request.addr() else {
                    return RioError::Internal
                        .attach_note("RIO UDP recv_multi missing prepared address");
                };
                kernel.submit_receive_multi(
                    request.rq(),
                    &request.data_buf().rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            }
            _ => Err(RioError::InvalidInput
                .to_report()
                .attach_note("invalid RIO receive multishot operation kind")),
        }
    }

    fn receive_pump_error(error: ReceivePumpError) -> Report<RioError> {
        RioError::InvalidInput
            .to_report()
            .with_ctx("receive_pump_error", error.to_string())
            .attach_note("RIO receive pump state transition failed")
    }
}
