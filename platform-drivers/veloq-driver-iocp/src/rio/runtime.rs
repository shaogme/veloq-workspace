//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;
pub(crate) mod pool;

use crate::IoFd;
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::rio::{RioEnv, RioState};
use std::io;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

pub(crate) struct RioTarget {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
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
}

pub(crate) struct RioUdpStreamArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: HANDLE,
    pub(crate) stream_op: &'a mut veloq_driver_core::op::UdpRecvStream<crate::config::RawHandle>,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
}

impl RioState {
    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<crate::ops::submit::SubmissionResult> {
        use crate::ops::submit::SubmissionResult;
        use windows_sys::Win32::Networking::WinSock::{
            AF_INET, AF_INET6, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
        };

        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            user_data,
            generation,
            page_idx,
        } = args;

        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let data_buf = self
            .registry
            .prepare_submission(buf, buf.len() as u32, env)?;
        self.registry
            .ensure_page_registration(page_idx, slab_resolver, env)?;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx].unwrap();

        if addr_ptr.is_null() {
            return Err(io_msg(
                IocpErrorContext::Rio,
                "RIO send_to received null remote address pointer",
            ));
        }
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_addr_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO send_to unsupported address family: family={family}"),
                ));
            }
        };
        if (addr_len as usize) < min_addr_len {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to invalid address length: addr_len={}, min_required={}, family={}",
                    addr_len, min_addr_len, family
                ),
            ));
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>();
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr >= slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to address pointer is outside registered slab: page_idx={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}, slab_end=0x{:x}",
                    page_idx, addr_addr, base_addr, slab_len, slab_end
                ),
            ));
        }
        let addr_end = addr_addr.saturating_add(rio_addr_len);
        if addr_end > slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to address range exceeds registered slab: page_idx={}, addr_ptr=0x{:x}, addr_len={}, addr_end=0x{:x}, slab_end=0x{:x}",
                    page_idx, addr_addr, rio_addr_len, addr_end, slab_end
                ),
            ));
        }

        let addr_offset = (addr_addr - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id.0,
            Offset: addr_offset,
            Length: rio_addr_len as u32,
        };
        let request_context = Self::encode_request_context(user_data, generation);

        if let Err(e) = self
            .kernel
            .submit_send_ex(rq, &data_buf, &addr_buf, request_context)
        {
            Self::free_op_request_context(request_context as u64);
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOSendEx submission failed: fd={fd:?}, handle={handle:?}, page_idx={}, rq=0x{:x}, data_buf_id=0x{:x}, data_off={}, data_len={}, addr_buf_id=0x{:x}, addr_off={}, addr_len={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}, original_error={e}",
                    page_idx,
                    rq.0 as usize,
                    data_buf.BufferId as usize,
                    data_buf.Offset,
                    data_buf.Length,
                    addr_buf.BufferId as usize,
                    addr_buf.Offset,
                    addr_buf.Length,
                    addr_addr,
                    base_addr,
                    slab_len
                ),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_udp_recv_stream_pooled(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::ops::submit::SubmissionResult> {
        use crate::ops::submit::SubmissionResult;

        let RioUdpStreamArgs {
            fd,
            handle,
            stream_op,
            user_data,
            generation,
        } = args;
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let actor = self.actors.get_mut(&handle).expect("actor exists");
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
        let (res, pool_submissions) = actor.pool_manager.try_submit_udp_recv_stream_pooled(
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

    pub(crate) fn try_refill_udp_pool(
        &mut self,
        target: (IoFd, HANDLE),
        buf: FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(());
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let (fd, handle) = target;
        let _ = self.ensure_actor((fd, handle), env)?;
        let actor = self.actors.get_mut(&handle).expect("actor exists");
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
        let pool_submissions = actor.pool_manager.try_refill_udp_pool(buf, &mut ctx)?;
        self.outstanding_count += pool_submissions;
        Ok(())
    }
}
