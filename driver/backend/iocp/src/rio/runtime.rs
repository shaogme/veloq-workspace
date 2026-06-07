//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::op::SubmissionResult;
use crate::rio::core::registry::RioSubmissionKind;
use crate::rio::core::submit_ops::{RioExConfig, RioProvider};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioEnv, RioState, SocketLifecycleState, SocketRuntimeState};
use diagweave::prelude::*;
use veloq_driver_core::op::UdpRecvFrom;
use windows_sys::Win32::Networking::WinSock::SOCKADDR_INET;

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
        let buf_len = RioSubmissionKind::Send.data_len(buf, buf_offset, "send_to")?;

        let dispatch = self
            .kernel
            .dispatch
            .ok_or(RioError::Internal)
            .attach_note("lost RIO context")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = {
            let actor = self.ensure_actor((fd, handle), env)?;
            actor.rq
        };
        let data_buf = self
            .registry
            .prepare_submission(buf, buf_offset, buf_len, env)?;
        let addr = self.registry.prepare_send_addr(addr_ptr, addr_len, env)?;
        let request_context =
            Self::encode_req_ctx_with_addr(user_data, generation, Some(addr.slot));

        if let Err(e) = self
            .kernel
            .submit_send_ex(rq, &data_buf, &addr.rio_buf, request_context)
        {
            Self::free_op_req_ctx(request_context as u64);
            self.registry.free_addr_slot(Some(addr.slot));
            return Err(e
                .push_ctx("scope", "rio.runtime.try_submit_send_to_internal")
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("user_data", user_data)
                .with_ctx("generation", generation)
                .with_ctx("addr_slot", addr.slot)
                .with_ctx("rq_raw", rq.0 as usize)
                .with_ctx("data_buffer_id", data_buf.BufferId as usize)
                .with_ctx("data_buffer_offset", data_buf.Offset)
                .with_ctx("data_buffer_length", data_buf.Length)
                .with_ctx("addr_buffer_id", addr.rio_buf.BufferId as usize)
                .with_ctx("addr_buffer_offset", addr.rio_buf.Offset)
                .with_ctx("addr_buffer_length", addr.rio_buf.Length)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIOSendEx submit failed"));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
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
        let buf_len =
            RioSubmissionKind::Recv.data_len(&recv_from_op.buf, buf_offset, "udp_recv_from")?;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or(RioError::Internal)
            .attach_note("lost RIO context")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = {
            let actor = self.ensure_actor((fd, handle), env)?;
            actor.rq
        };
        let data_buf =
            self.registry
                .prepare_submission(&recv_from_op.buf, buf_offset, buf_len, env)?;
        if addr_ptr.is_null() {
            return RioError::Internal.attach_note("RIO recv_from received null address buffer");
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>() as u32;
        let mut addr = self.registry.prepare_recv_addr(env)?;
        addr.rio_buf.Length = rio_addr_len;
        let request_context =
            Self::encode_req_ctx_with_addr(user_data, generation, Some(addr.slot));

        let submit_res = dispatch.receive_ex(RioExConfig {
            rq,
            data_buf: &data_buf,
            data_buf_count: 1,
            local_addr: std::ptr::null(),
            remote_addr: &addr.rio_buf,
            control_buf: std::ptr::null(),
            flags_buf: std::ptr::null(),
            flags: 0,
            context: request_context,
        });

        if let Err(e) = submit_res {
            Self::free_op_req_ctx(request_context as u64);
            self.registry.free_addr_slot(Some(addr.slot));
            return Err(e
                .push_ctx("scope", "rio.runtime.try_submit_recv_from_internal")
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("user_data", user_data)
                .with_ctx("generation", generation)
                .with_ctx("addr_slot", addr.slot)
                .with_ctx("rq_raw", rq.0 as usize)
                .with_ctx("data_buffer_id", data_buf.BufferId as usize)
                .with_ctx("data_buffer_offset", data_buf.Offset)
                .with_ctx("data_buffer_length", data_buf.Length)
                .with_ctx("addr_buffer_id", addr.rio_buf.BufferId as usize)
                .with_ctx("addr_buffer_offset", addr.rio_buf.Offset)
                .with_ctx("addr_buffer_length", addr.rio_buf.Length)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIOReceiveEx submit failed"));
        };

        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
