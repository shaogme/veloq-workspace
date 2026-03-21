//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;
pub(crate) mod pool;

use crate::IoFd;
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::ops::SubmissionResult;
use crate::rio::{RioEnv, RioState};
use std::io;
use veloq_buf::FixedBuf;
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
    fn validate_rio_addr(
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        base_addr: usize,
        slab_len: usize,
        page_idx: usize,
    ) -> io::Result<(u32, usize)> {
        if addr_ptr.is_null() {
            return Err(io_msg(
                IocpErrorContext::Rio,
                "RIO send_to received null address",
            ));
        }
        // SAFETY: addr_ptr is checked for null, and sa_family is a standard field in SOCKADDR.
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO unsupported family: {family}"),
                ));
            }
        };
        if (addr_len as usize) < min_len {
            return Err(io_msg(
                IocpErrorContext::Rio,
                "RIO send_to invalid address length",
            ));
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>();
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr.saturating_add(rio_addr_len) > slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!("RIO address outside slab: page={page_idx}"),
            ));
        }

        Ok((rio_addr_len as u32, (addr_addr - base_addr) as u32 as usize))
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<SubmissionResult> {
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
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "lost RIO context"))?;
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
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "missing slab page"))?;

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
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOSendEx failed for fd={fd:?}: {e}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_pool_recv(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
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
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "lost RIO context"))?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let actor = self
            .actors
            .get_mut(&handle)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "actor not found"))?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
        let (res, pool_submissions) = pool_manager.try_submit_pool_recv(
            udp_mailbox,
            stream_op,
            (user_data, generation),
            &mut ctx,
        )?;
        self.outstanding_count += pool_submissions;
        if matches!(res, SubmissionResult::Pending) {
            self.outstanding_count += 1;
        }
        Ok(res)
    }

    pub(crate) fn try_submit_pool_recv_for_recv(
        &mut self,
        args: RioUdpRecvArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let RioUdpRecvArgs {
            fd,
            handle,
            recv_op,
            sidecar,
        } = args;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "lost RIO context"))?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let actor = self
            .actors
            .get_mut(&handle)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "actor not found"))?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
        let user_data = sidecar.user_data;
        let generation = sidecar.generation;
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
        let (res, pool_submissions, immediate_copied) = pool_manager.try_submit_pool_recv_recv(
            udp_mailbox,
            recv_op,
            (user_data, generation),
            &mut ctx,
        )?;
        if let Some(copied) = immediate_copied {
            sidecar.blocking_result = Some(Ok(copied));
        }
        self.outstanding_count += pool_submissions;
        if matches!(res, SubmissionResult::Pending) {
            self.outstanding_count += 1;
        }
        Ok(res)
    }

    pub(crate) fn try_refill_udp_pool(
        &mut self,
        target: (IoFd, HANDLE),
        buf: FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let dispatch = self
            .kernel
            .dispatch
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "lost RIO context"))?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let (fd, handle) = target;
        let _ = self.ensure_actor((fd, handle), env)?;
        let actor = self
            .actors
            .get_mut(&handle)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "actor missing"))?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &actor.udp_mailbox);
        let pool_submissions = pool_manager.try_refill_pool(udp_mailbox, buf, &mut ctx)?;
        self.outstanding_count += pool_submissions;
        Ok(())
    }
}
