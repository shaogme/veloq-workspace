//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;
pub(crate) mod pool;

use crate::IoFd;
use crate::ops::SubmissionResult;
use crate::rio::error::{RioError, RioReportExt, RioResult};
use crate::rio::{RioEnv, RioState};
use error_stack::ResultExt;
use std::io;
use veloq_driver_core::op::{UdpRecv, UdpRecvStream};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

pub(crate) struct RioTarget {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioSendToArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) buf: &'a veloq_buf::FixedBuf,
    pub(crate) addr_ptr: *const std::ffi::c_void,
    pub(crate) addr_len: i32,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) page_idx: usize,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioUdpStreamArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) stream_op: &'a mut UdpRecvStream<crate::config::RawHandle>,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
}

pub(crate) struct RioUdpRecvArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) recv_op: &'a mut UdpRecv<crate::config::RawHandle>,
    pub(crate) sidecar: &'a mut crate::ops::OverlappedEntry,
}

impl RioState {
    #[inline]
    pub(crate) fn is_iocp_fallback(&self, handle: HANDLE) -> bool {
        self.udp_iocp_fallback_handles.contains(&handle)
    }

    #[inline]
    fn should_demote_socket(err: &io::Error) -> bool {
        err.raw_os_error() == Some(10055)
            || err.to_string().contains("os_error=10055")
            || err.to_string().contains("os error 10055")
    }

    #[inline]
    pub(crate) fn maybe_mark_iocp_fallback(&mut self, handle: HANDLE, err: &io::Error) {
        if Self::should_demote_socket(err) {
            self.udp_iocp_fallback_handles.insert(handle);
        }
    }

    #[inline]
    pub(crate) fn clear_iocp_fallback(&mut self, handle: HANDLE) {
        self.udp_iocp_fallback_handles.remove(&handle);
    }

    fn validate_rio_addr(
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        base_addr: usize,
        slab_len: usize,
        page_idx: usize,
    ) -> RioResult<(u32, usize)> {
        if addr_ptr.is_null() {
            return Err(error_stack::Report::new(RioError::Internal))
                .attach("RIO send_to received null address");
        }
        // SAFETY: addr_ptr is checked for null, and sa_family is a standard field in SOCKADDR.
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach(format!("RIO unsupported family: {family}"));
            }
        };
        if (addr_len as usize) < min_len {
            return Err(error_stack::Report::new(RioError::Internal))
                .attach("RIO send_to invalid address length");
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>();
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr.saturating_add(rio_addr_len) > slab_end {
            return Err(error_stack::Report::new(RioError::Internal))
                .attach(format!("RIO address outside slab: page={page_idx}"));
        }

        Ok((rio_addr_len as u32, (addr_addr - base_addr) as u32 as usize))
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<SubmissionResult> {
        self.try_submit_send_to_internal(args, registrar, slab_resolver)
            .map_err(|e| e.to_io_error("RIOSendEx submission failed"))
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
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("lost RIO context")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let data_buf = self.registry.prepare_submission(
            buf,
            buf_offset,
            (buf.len().saturating_sub(buf_offset)) as u32,
            env,
        )?;
        self.registry
            .ensure_page_reg(page_idx, slab_resolver, env)?;

        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx]
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("missing slab page")?;

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
            return Err(e).attach(format!("RIOSendEx failed for fd={fd:?}"));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_pool_recv(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        self.try_submit_pool_recv_internal(args, registrar)
            .map_err(|e| e.to_io_error("RIO pool recv submission failed"))
    }

    fn try_submit_pool_recv_internal(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioUdpStreamArgs {
            fd,
            handle,
            stream_op,
            user_data,
            generation,
        } = args;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("lost RIO context")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let key = self
            .actor_by_handle
            .get(&handle)
            .copied()
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("actor not found")?;
        let actor = self
            .actors
            .get_mut(key)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("actor not found")?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
        let (res, pool_submissions) = pool_manager
            .try_submit_pool_recv(udp_mailbox, stream_op, (user_data, generation), &mut ctx)
            .map_err(|e| io::Error::other(e.to_string()))
            .change_context(RioError::Internal)
            .attach("pool submission failed")?;

        self.outstanding_count += pool_submissions;
        Ok(res)
    }

    pub(crate) fn try_submit_pool_recv_for_recv(
        &mut self,
        args: RioUdpRecvArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        self.try_submit_pool_recv_for_recv_internal(args, registrar)
            .map_err(|e| e.to_io_error("RIO pool recv for recv submission failed"))
    }

    fn try_submit_pool_recv_for_recv_internal(
        &mut self,
        args: RioUdpRecvArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioUdpRecvArgs {
            fd,
            handle,
            recv_op,
            sidecar,
        } = args;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("lost RIO context")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let key = self
            .actor_by_handle
            .get(&handle)
            .copied()
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("actor not found")?;
        let actor = self
            .actors
            .get_mut(key)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("actor not found")?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
        let user_data = sidecar.user_data;
        let generation = sidecar.generation;
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
        let (res, pool_submissions, immediate_copied) = pool_manager
            .try_submit_pool_recv_recv(udp_mailbox, recv_op, (user_data, generation), &mut ctx)
            .map_err(|e| io::Error::other(e.to_string()))
            .change_context(RioError::Internal)
            .attach("pool submission failed")?;

        if let Some(copied) = immediate_copied {
            sidecar.blocking_result = Some(Ok(copied));
        }
        self.outstanding_count += pool_submissions;
        Ok(res)
    }
}
