//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::op::SubmissionResult;
use crate::rio::core::registry::RioSubmissionKind;
use crate::rio::core::{RioAddressPolicy, RioOpKind, RioSubmitPlan};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioState, SocketLifecycleState, SocketRuntimeState};
use diagweave::prelude::*;
use veloq_driver_core::op::UdpRecvFrom;

pub(crate) struct RioTarget<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) buf_offset: usize,
    pub(crate) operation: &'static str,
}

pub(crate) struct RioSendToArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) buf: &'a veloq_buf::FixedBuf,
    pub(crate) addr_ptr: *const std::ffi::c_void,
    pub(crate) addr_len: i32,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioUdpRecvFromArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) recv_from_op: &'a mut UdpRecvFrom,
    pub(crate) addr_ptr: *mut std::ffi::c_void,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
}

impl RioState {
    #[inline]
    fn socket_runtime_mut(&mut self, actor_key: SocketKey) -> &mut SocketRuntimeState {
        self.socket_runtime.entry(actor_key).or_default()
    }

    #[inline]
    pub(crate) fn mark_socket_registered(&mut self, actor_key: SocketKey) {
        let state = self.socket_runtime_mut(actor_key);
        state.lifecycle = SocketLifecycleState::Open;
    }

    #[inline]
    pub(crate) fn begin_socket_cleanup(&mut self, actor_key: SocketKey) -> bool {
        let state = self.socket_runtime_mut(actor_key);
        state.lifecycle = SocketLifecycleState::Closing;
        state.inflight == 0
    }

    #[inline]
    pub(crate) fn try_acquire_socket_inflight(&mut self, actor_key: SocketKey) -> bool {
        let state = self.socket_runtime_mut(actor_key);
        if state.lifecycle == SocketLifecycleState::Closing {
            return false;
        }
        state.inflight = state.inflight.saturating_add(1);
        true
    }

    #[inline]
    pub(crate) fn acquire_socket_kernel_inflight(&mut self, actor_key: SocketKey) {
        let state = self.socket_runtime_mut(actor_key);
        state.inflight = state.inflight.saturating_add(1);
    }

    #[inline]
    pub(crate) fn release_socket_inflight(&mut self, actor_key: SocketKey) {
        if let Some(state) = self.socket_runtime.get_mut(&actor_key)
            && state.inflight > 0
        {
            state.inflight -= 1;
        }
    }

    #[inline]
    pub(crate) fn socket_ready_for_cleanup(&self, actor_key: SocketKey) -> bool {
        self.socket_runtime.get(&actor_key).is_none_or(|state| {
            state.lifecycle == SocketLifecycleState::Closing && state.inflight == 0
        })
    }

    #[inline]
    pub(crate) fn forget_socket_runtime(&mut self, actor_key: SocketKey) {
        self.socket_runtime.remove(&actor_key);
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_send_to_internal(args, registrar)
    }

    fn try_submit_send_to_internal(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            user_data,
            generation,
            buf_offset,
            ..
        } = args;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                user_data,
                generation,
                op_kind: RioOpKind::SendTo,
                buffer_kind: RioSubmissionKind::Send,
                buffer: buf,
                buffer_offset: buf_offset,
                operation: "send_to",
                address: RioAddressPolicy::SendTo { addr_ptr, addr_len },
                dispatch_error: RioError::Internal,
                dispatch_note: "lost RIO context",
                submit_scope: "rio.runtime.try_submit_send_to_internal",
                submit_note: "RIOSendEx submit failed",
            },
            registrar,
            |kernel, request| {
                let Some(addr) = request.addr.as_ref() else {
                    return RioError::Internal.attach_note("RIO send_to missing prepared address");
                };
                kernel.submit_send_ex(
                    request.rq,
                    &request.data_buf.rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_recv_from(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_from_internal(args, registrar)
    }

    fn try_submit_recv_from_internal(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioUdpRecvFromArgs {
            fd,
            handle,
            recv_from_op,
            addr_ptr,
            user_data,
            generation,
        } = args;
        let buf_offset = recv_from_op.buf_offset;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                user_data,
                generation,
                op_kind: RioOpKind::RecvFrom,
                buffer_kind: RioSubmissionKind::Recv,
                buffer: &recv_from_op.buf,
                buffer_offset: buf_offset,
                operation: "udp_recv_from",
                address: RioAddressPolicy::RecvFrom { addr_ptr },
                dispatch_error: RioError::Internal,
                dispatch_note: "lost RIO context",
                submit_scope: "rio.runtime.try_submit_recv_from_internal",
                submit_note: "RIOReceiveEx submit failed",
            },
            registrar,
            |kernel, request| {
                let Some(addr) = request.addr.as_ref() else {
                    return RioError::Internal
                        .attach_note("RIO recv_from missing prepared address");
                };
                kernel.submit_receive_ex(
                    request.rq,
                    &request.data_buf.rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }
}
