//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::op::SubmissionResult;
use crate::rio::core::submit_ops::{RioExConfig, RioProvider};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioEnv, RioState, SocketLifecycleState, SocketRuntimeMode, SocketRuntimeState};
use diagweave::report::ResultReportExt;
use veloq_driver_core::op::UdpRecvFrom;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

pub(crate) struct RioTarget<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioSendToArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) buf: &'a veloq_buf::FixedBuf,
    pub(crate) addr_ptr: *const std::ffi::c_void,
    pub(crate) addr_len: i32,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) page_idx: usize,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioUdpRecvFromArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) recv_from_op: &'a mut UdpRecvFrom,
    pub(crate) addr_ptr: *mut std::ffi::c_void,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) page_idx: usize,
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

    #[inline]
    pub(crate) fn is_iocp_fallback(&self, actor_key: SocketKey) -> bool {
        self.socket_runtime
            .get(&actor_key)
            .is_some_and(|state| state.mode == SocketRuntimeMode::IocpFallback)
    }

    fn validate_rio_addr(
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        base_addr: usize,
        slab_len: usize,
        page_idx: usize,
    ) -> RioResult<(u32, usize)> {
        if addr_ptr.is_null() {
            return Err(diagweave::report::Report::new(RioError::Internal))
                .attach_note("RIO send_to received null address");
        }
        // SAFETY: addr_ptr is checked for null, and sa_family is a standard field in SOCKADDR.
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return Err(diagweave::report::Report::new(RioError::Internal))
                    .attach_note(format!("RIO unsupported family: {family}"));
            }
        };
        if (addr_len as usize) < min_len {
            return Err(diagweave::report::Report::new(RioError::Internal))
                .attach_note("RIO send_to invalid address length");
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>();
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr.saturating_add(rio_addr_len) > slab_end {
            return Err(diagweave::report::Report::new(RioError::Internal))
                .attach_note(format!("RIO address outside slab: page={page_idx}"));
        }

        Ok((rio_addr_len as u32, (addr_addr - base_addr) as u32 as usize))
    }

    fn validate_rio_addr_output(
        addr_ptr: *mut std::ffi::c_void,
        rio_addr_len: u32,
        base_addr: usize,
        slab_len: usize,
        page_idx: usize,
    ) -> RioResult<usize> {
        if addr_ptr.is_null() {
            return Err(diagweave::report::Report::new(RioError::Internal))
                .attach_note("RIO recv_from received null address buffer");
        }
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr.saturating_add(rio_addr_len as usize) > slab_end {
            return Err(diagweave::report::Report::new(RioError::Internal)).attach_note(format!(
                "RIO recv_from address outside slab: page={page_idx}"
            ));
        }

        Ok((addr_addr - base_addr) as u32 as usize)
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_send_to_internal(args, registrar, slab_resolver)
    }

    fn try_submit_send_to_internal(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> RioResult<SubmissionResult> {
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            user_data,
            generation,
            page_idx,
            buf_offset,
            ..
        } = args;

        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
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
        let socket_key = handle.raw().actor_key();
        if self.is_iocp_fallback(socket_key) {
            return Err(diagweave::report::Report::new(RioError::NotSupported))
                .attach_note("Socket is marked for IOCP fallback");
        }
        let data_buf = self.registry.prepare_submission(
            buf,
            buf_offset,
            (buf.len().saturating_sub(buf_offset)) as u32,
            env,
        )?;
        self.registry
            .ensure_page_reg(page_idx, slab_resolver, env)?;

        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx]
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
            .attach_note("missing slab page")?;

        let (rio_addr_len, addr_offset) =
            Self::validate_rio_addr(addr_ptr, addr_len, base_addr, slab_len, page_idx)?;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id.0,
            Offset: addr_offset as u32,
            Length: rio_addr_len,
        };
        let request_context = Self::encode_req_ctx(user_data, generation);

        if let Err(e) = self
            .kernel
            .submit_send_ex(rq, &data_buf, &addr_buf, request_context)
        {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e.attach_note(format!("RIOSendEx failed for fd={fd:?}")));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_recv_from(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_from_internal(args, registrar, slab_resolver)
    }

    fn try_submit_recv_from_internal(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> RioResult<SubmissionResult> {
        let RioUdpRecvFromArgs {
            fd,
            handle,
            recv_from_op,
            addr_ptr,
            user_data,
            generation,
            page_idx,
        } = args;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
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
        let socket_key = handle.raw().actor_key();
        if self.is_iocp_fallback(socket_key) {
            return Err(diagweave::report::Report::new(RioError::NotSupported))
                .attach_note("Socket is marked for IOCP fallback");
        }
        let buf_offset = recv_from_op.buf_offset;
        let buf_len = recv_from_op.buf.capacity().saturating_sub(buf_offset) as u32;
        let data_buf =
            self.registry
                .prepare_submission(&recv_from_op.buf, buf_offset, buf_len, env)?;
        self.registry
            .ensure_page_reg(page_idx, slab_resolver, env)?;

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>() as u32;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx]
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
            .attach_note("missing slab page")?;
        let addr_offset =
            Self::validate_rio_addr_output(addr_ptr, rio_addr_len, base_addr, slab_len, page_idx)?;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id.0,
            Offset: addr_offset as u32,
            Length: rio_addr_len,
        };
        let request_context = Self::encode_req_ctx(user_data, generation);

        if let Err(e) = dispatch.receive_ex(RioExConfig {
            rq,
            data_buf: &data_buf,
            data_buf_count: 1,
            local_addr: std::ptr::null(),
            remote_addr: &addr_buf,
            control_buf: std::ptr::null(),
            flags_buf: std::ptr::null(),
            flags: 0,
            context: request_context,
        }) {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e.attach_note(format!(
                "RIOReceiveEx failed for fd={fd:?}, user_data={user_data}, generation={generation}, page_idx={page_idx}"
            )));
        }

        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
