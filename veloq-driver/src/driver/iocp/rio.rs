pub mod pool;
pub mod registry;

use crate::driver::iocp::IocpOp;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::{IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::{OverlappedEntry, STATE_COMPLETED, STATE_CONSUMED};
use crate::op::IoFd;
use std::io;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ, RIO_IOCP_COMPLETION,
    RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT, SOCKET_ERROR, WSAGetLastError,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

use self::pool::{POOL_CTX_TAG, UDP_POOL_USER_DATA, UdpPoolManager};
use self::registry::RioRegistry;

#[derive(Clone, Copy)]
pub struct RioEnv<'a> {
    pub registrar: &'a dyn veloq_buf::BufferRegistrar,
    pub dispatch: &'a RioDispatch,
}

pub struct RioContext<'a> {
    pub registry: &'a mut RioRegistry,
    pub env: RioEnv<'a>,
}

pub struct RioSendToArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub buf: &'a veloq_buf::FixedBuf,
    pub addr_ptr: *const std::ffi::c_void,
    pub addr_len: i32,
    pub overlapped: *mut OVERLAPPED,
    pub page_idx: usize,
}

pub struct RioRecvFromArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub buf: &'a mut veloq_buf::FixedBuf,
    pub addr_ptr: *const std::ffi::c_void,
    pub len_ptr: *const i32,
    pub overlapped: *mut OVERLAPPED,
    pub page_idx: usize,
}

pub struct RioUdpStreamArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub stream_op: &'a mut crate::op::UdpRecvStream,
    pub user_data: usize,
    pub generation: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct RioDispatch {
    pub create_cq: unsafe extern "system" fn(u32, *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ,
    pub create_rq: unsafe extern "system" fn(
        usize,
        u32,
        u32,
        u32,
        u32,
        RIO_CQ,
        RIO_CQ,
        *const std::ffi::c_void,
    ) -> RIO_RQ,
    pub register_buffer: unsafe extern "system" fn(*const u8, u32) -> RIO_BUFFERID,
    pub deregister_buffer: unsafe extern "system" fn(RIO_BUFFERID),
    pub dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    pub notify: unsafe extern "system" fn(RIO_CQ) -> i32,
    pub close_cq: unsafe extern "system" fn(RIO_CQ),
    pub receive:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    pub send:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    pub send_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const std::ffi::c_void,
    ) -> i32,
    pub receive_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const std::ffi::c_void,
    ) -> i32,
}

pub struct RioState {
    pub(crate) cq: RIO_CQ,
    pub(crate) _notify_overlapped: Box<OVERLAPPED>,
    pub(crate) registry: RioRegistry,
    pub(crate) pool_manager: UdpPoolManager,
    pub(crate) dispatch: RioDispatch,
    pub(crate) outstanding_count: usize,
}

impl RioState {
    #[inline]
    fn encode_request_context(overlapped: *mut OVERLAPPED) -> *const std::ffi::c_void {
        overlapped as *const std::ffi::c_void
    }

    #[inline]
    fn decode_request_context(ctx: u64) -> Option<(usize, u32)> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) == POOL_CTX_TAG {
            let token = (raw >> 1) as u32;
            if token == 0 {
                return None;
            }
            return Some((UDP_POOL_USER_DATA, token));
        }
        let entry = ctx as usize as *const OverlappedEntry;
        let user_data = unsafe { (*entry).user_data };
        let generation = unsafe { (*entry).generation };
        Some((user_data, generation))
    }

    #[inline]
    fn decode_pool_context(ctx: u64) -> Option<u32> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) != POOL_CTX_TAG {
            return None;
        }
        let token = (raw >> 1) as u32;
        if token == 0 {
            return None;
        }
        Some(token)
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Self> {
        let table = &ext.rio_table;

        let dispatch = RioDispatch {
            create_cq: table.RIOCreateCompletionQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCreateCompletionQueue function pointer missing",
                )
            })?,
            create_rq: table.RIOCreateRequestQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCreateRequestQueue function pointer missing",
                )
            })?,
            register_buffer: table.RIORegisterBuffer.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIORegisterBuffer function pointer missing",
                )
            })?,
            deregister_buffer: table.RIODeregisterBuffer.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIODeregisterBuffer function pointer missing",
                )
            })?,
            dequeue: table.RIODequeueCompletion.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIODequeueCompletion function pointer missing",
                )
            })?,
            notify: table.RIONotify.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIONotify function pointer missing")
            })?,
            close_cq: table.RIOCloseCompletionQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCloseCompletionQueue function pointer missing",
                )
            })?,
            receive: table.RIOReceive.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIOReceive function pointer missing")
            })?,
            send: table
                .RIOSend
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSend function pointer missing"))?,
            send_ex: table.RIOSendEx.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIOSendEx function pointer missing")
            })?,
            receive_ex: table.RIOReceiveEx.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOReceiveEx function pointer missing",
                )
            })?,
        };

        const RIO_EVENT_KEY: usize = usize::MAX - 1;

        let mut notify_overlapped = Box::new(unsafe { std::mem::zeroed::<OVERLAPPED>() });
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port,
                    CompletionKey: RIO_EVENT_KEY as *mut std::ffi::c_void,
                    Overlapped: (&mut *notify_overlapped as *mut OVERLAPPED).cast(),
                },
            },
        };

        let queue_size = entries.max(128);
        let cq = unsafe { (dispatch.create_cq)(queue_size, &notification as *const _) };

        if cq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOCreateCompletionQueue failed: entries={entries}, queue_size={queue_size}"
                ),
            ));
        }

        let notify_ret = unsafe { (dispatch.notify)(cq) };
        if notify_ret == SOCKET_ERROR {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIONotify failed after CQ creation",
            ));
        }

        let rq_depth = entries.clamp(32, 256);

        Ok(Self {
            cq,
            _notify_overlapped: notify_overlapped,
            registry: RioRegistry::new(rq_depth),
            pool_manager: UdpPoolManager::new(),
            dispatch,
            outstanding_count: 0,
        })
    }

    pub fn resize_registered_rqs(&mut self, size: usize) {
        self.registry.resize_registered_rqs(size);
    }

    pub fn clear_registered_rq(&mut self, idx: usize) {
        self.registry.clear_registered_rq(idx);
    }

    pub fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        let env = RioEnv {
            registrar: &veloq_buf::NoopRegistrar,
            dispatch: &self.dispatch,
        };
        self.registry.register_chunk(id, (ptr, len), env)
    }

    pub fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE) {
        let env = RioEnv {
            registrar: &veloq_buf::NoopRegistrar,
            dispatch: &self.dispatch,
        };
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        self.pool_manager
            .begin_udp_pool_shutdown_for_handle(handle, &mut ctx);
        // If pool was fully drained, also remove the stale RQ mapping.
        if !self.pool_manager.udp_recv_pools.contains_key(&handle) {
            self.registry.rio_rqs.remove(&handle);
        }
    }

    pub fn cancel_udp_recv_waiter(
        &mut self,
        handle: HANDLE,
        uid: (usize, u32),
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        self.pool_manager
            .cancel_udp_recv_waiter(handle, uid, &mut ctx);
    }

    pub fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<usize> {
        let mut completed_count = 0;
        let dequeue_fn = self.dispatch.dequeue;
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };

        loop {
            let count =
                unsafe { dequeue_fn(self.cq, results.as_mut_ptr(), MAX_RIO_RESULTS as u32) };

            if count == RIO_CORRUPT_CQ {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    "RIO completion queue is corrupt (RIO_CORRUPT_CQ)",
                ));
            }
            if count == 0 {
                break;
            }

            for res in results.iter().take(count as usize) {
                let Some((user_data, completion_generation)) =
                    Self::decode_request_context(res.RequestContext)
                else {
                    continue;
                };

                if user_data == UDP_POOL_USER_DATA {
                    let env = RioEnv {
                        registrar,
                        dispatch: &self.dispatch,
                    };
                    let mut ctx = RioContext {
                        registry: &mut self.registry,
                        env,
                    };
                    let (drained_handle, pool_submissions) = self.pool_manager.handle_completion(
                        ops,
                        (res, completion_generation),
                        &mut ctx,
                    );
                    if let Some(h) = drained_handle {
                        self.registry.rio_rqs.remove(&h);
                    }
                    self.outstanding_count -= 1;
                    self.outstanding_count += pool_submissions;
                    completed_count += 1;
                    continue;
                }

                if user_data < ops.local.len() {
                    let op = &mut ops.local[user_data];
                    let slot = &ops.shared.slots[user_data];
                    if op.platform_data.generation != completion_generation {
                        continue;
                    }

                    if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                        let result = if res.Status == 0 {
                            Ok(res.BytesTransferred as usize)
                        } else {
                            Err(io::Error::from_raw_os_error(res.Status))
                        };
                        op.platform_data.lifecycle = OpLifecycle::Completed(result);

                        let result_for_slot = if res.Status == 0 {
                            Ok(res.BytesTransferred as usize)
                        } else {
                            Err(io::Error::from_raw_os_error(res.Status))
                        };
                        unsafe { *slot.result.get() = Some(result_for_slot) };
                        slot.state.store(STATE_COMPLETED, Ordering::Release);
                        slot.waker.wake();
                    } else if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                        if op.platform_data.rio_needs_drain {
                            op.platform_data.rio_drained = true;
                            if slot.state.load(Ordering::Acquire) == STATE_CONSUMED {
                                let _ = std::mem::take(&mut op.platform_data);
                                ops.free_indices.push(user_data);
                            }
                        } else {
                            let _ = std::mem::take(&mut op.platform_data);
                            ops.free_indices.push(user_data);
                        }
                    }
                    self.outstanding_count -= 1;
                    completed_count += 1;
                }
            }

            if count < MAX_RIO_RESULTS as u32 {
                break;
            }
        }

        let notify_fn = self.dispatch.notify;
        let ret = unsafe { notify_fn(self.cq) };
        if ret == SOCKET_ERROR {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIONotify failed when rearming CQ",
            ));
        }
        Ok(completed_count)
    }

    pub fn try_submit_recv(
        &mut self,
        target: (IoFd, HANDLE, *mut OVERLAPPED),
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, overlapped) = target;
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let (buffer_id, offset) = self.registry.resolve_buffer_id(buf, env)?;
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset,
            Length: buf.capacity() as u32,
        };
        let request_context = Self::encode_request_context(overlapped);
        let ret = unsafe { (self.dispatch.receive)(rq, &rio_buf, 1, 0, request_context) };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOReceive submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_send(
        &mut self,
        target: (IoFd, HANDLE, *mut OVERLAPPED),
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, overlapped) = target;
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let (buffer_id, offset) = self.registry.resolve_buffer_id(buf, env)?;
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset,
            Length: buf.len() as u32,
        };
        let request_context = Self::encode_request_context(overlapped);
        let ret = unsafe { (self.dispatch.send)(rq, &rio_buf, 1, 0, request_context) };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOSend submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        use windows_sys::Win32::Networking::WinSock::{
            AF_INET, AF_INET6, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
        };
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            overlapped,
            page_idx,
        } = args;

        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let (buffer_id, data_offset) = self.registry.resolve_buffer_id(buf, env)?;
        self.registry
            .ensure_slab_page_registration(page_idx, slab_resolver, env)?;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx].unwrap();
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: data_offset,
            Length: buf.len() as u32,
        };

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
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: rio_addr_len as u32,
        };
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe {
            (self.dispatch.send_ex)(
                rq,
                &data_buf,
                1,
                std::ptr::null(),
                &addr_buf,
                std::ptr::null(),
                std::ptr::null(),
                0,
                request_context,
            )
        };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOSendEx submission failed: fd={fd:?}, handle={handle:?}, page_idx={}, rq=0x{:x}, data_buf_id=0x{:x}, data_off={}, data_len={}, addr_buf_id=0x{:x}, addr_off={}, addr_len={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}",
                    page_idx,
                    rq as usize,
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

    pub fn try_submit_recv_from(
        &mut self,
        args: RioRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let RioRecvFromArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            len_ptr: _len_ptr,
            overlapped,
            page_idx,
        } = args;
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let (buffer_id, data_offset) = self.registry.resolve_buffer_id(buf, env)?;
        self.registry
            .ensure_slab_page_registration(page_idx, slab_resolver, env)?;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx].unwrap();
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: data_offset,
            Length: buf.capacity() as u32,
        };

        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        let addr_len = std::mem::size_of::<crate::SockAddrStorage>();
        if addr_addr < base_addr || addr_addr >= slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO recv_from address pointer is outside registered slab: page_idx={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}, slab_end=0x{:x}",
                    page_idx, addr_addr, base_addr, slab_len, slab_end
                ),
            ));
        }
        let addr_end = addr_addr.saturating_add(addr_len);
        if addr_end > slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO recv_from address range exceeds registered slab: page_idx={}, addr_ptr=0x{:x}, addr_len={}, addr_end=0x{:x}, slab_end=0x{:x}",
                    page_idx, addr_addr, addr_len, addr_end, slab_end
                ),
            ));
        }

        let addr_offset = (addr_addr - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: addr_len as u32,
        };
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe {
            (self.dispatch.receive_ex)(
                rq,
                &data_buf,
                1,
                std::ptr::null(),
                &addr_buf,
                std::ptr::null(),
                std::ptr::null(),
                0,
                request_context,
            )
        };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceiveEx submission failed: fd={fd:?}, handle={handle:?}, page_idx={}, rq=0x{:x}, data_buf_id=0x{:x}, data_off={}, data_len={}, addr_buf_id=0x{:x}, addr_off={}, addr_len={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}",
                    page_idx,
                    rq as usize,
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

    pub fn try_submit_udp_recv_stream_pooled(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let RioUdpStreamArgs {
            fd,
            handle,
            stream_op,
            user_data,
            generation,
        } = args;
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        let (res, pool_submissions) = self.pool_manager.try_submit_udp_recv_stream_pooled(
            (handle, rq),
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

    pub fn try_refill_udp_pool(
        &mut self,
        target: (IoFd, HANDLE),
        buf: FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let (fd, handle) = target;
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let rq = self.registry.ensure_rq((handle, fd), self.cq, env)?;
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        let pool_submissions =
            self.pool_manager
                .try_refill_udp_pool((handle, rq), buf, &mut ctx)?;
        self.outstanding_count += pool_submissions;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(
        &self,
        handle: HANDLE,
    ) -> Option<pool::UdpRecvPoolDebugStats> {
        self.pool_manager.udp_pool_debug_stats(handle)
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        handle: HANDLE,
        ticks: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let env = RioEnv {
            registrar,
            dispatch: &self.dispatch,
        };
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        for _ in 0..ticks {
            self.pool_manager.rebalance_udp_pool(handle, &mut ctx)?;
        }
        Ok(())
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        // Explicit UDP pool shutdown protocol:
        // 1) forbid new submissions; 2) mark in-flight slots as stop-requested;
        // 3) drain CQ until all slot acknowledgements arrive; 4) release buffers/CQ.
        self.pool_manager.begin_udp_pool_shutdown();

        // Consolidate drain: Wait for all outstanding RIO requests (Pool + Standard) to finish.
        // This ensures the kernel is no longer touching any registered buffers or pool slots.
        let start = std::time::Instant::now();
        while self.outstanding_count > 0 {
            if start.elapsed() >= std::time::Duration::from_secs(5) {
                tracing::warn!(
                    outstanding = self.outstanding_count,
                    "RioState::drop: Timeout waiting for outstanding RIO requests"
                );
                break;
            }

            const MAX_RESULTS: usize = 128;
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { std::mem::zeroed() };
            let count = unsafe {
                (self.dispatch.dequeue)(self.cq, results.as_mut_ptr(), MAX_RESULTS as u32)
            };

            if count == RIO_CORRUPT_CQ || count == 0 {
                std::thread::yield_now();
            } else {
                for res in results.iter().take(count as usize) {
                    if let Some(completion_generation) =
                        Self::decode_pool_context(res.RequestContext)
                        && let Some((handle, slot_idx)) = self
                            .pool_manager
                            .ack_udp_pool_completion(completion_generation)
                        && let Some(pool) = self.pool_manager.udp_recv_pools.get_mut(&handle)
                        && let Some(slot) = pool.slots.get_mut(slot_idx)
                    {
                        slot.in_flight = false;
                        slot.stop_requested = false;
                    }
                    self.outstanding_count -= 1;
                }
            }
        }

        self.pool_manager.udp_ctx_map.clear();
        let env = RioEnv {
            registrar: &veloq_buf::NoopRegistrar,
            dispatch: &self.dispatch,
        };
        let mut ctx = RioContext {
            registry: &mut self.registry,
            env,
        };
        self.pool_manager
            .forget_in_flight_and_deregister_rest(&mut ctx);
        self.registry.cleanup_deregister(env);
        if self.cq != 0 {
            unsafe { (self.dispatch.close_cq)(self.cq) };
        }
    }
}
