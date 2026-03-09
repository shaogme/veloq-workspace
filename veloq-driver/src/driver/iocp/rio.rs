use crate::SockAddrStorage;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::IocpOp;
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::OverlappedEntry;
use crate::driver::slot::{STATE_COMPLETED, STATE_CONSUMED};
use crate::op::IoFd;
use rustc_hash::{FxHashMap, FxHashSet};
use std::io;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ, RIO_IOCP_COMPLETION,
    RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6,
    SOCKADDR_INET, SOCKET_ERROR, WSAGetLastError,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

// Define constants that might be missing or different in windows-sys
const RIO_INVALID_BUFFERID: RIO_BUFFERID = 0 as RIO_BUFFERID;

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
    pub(crate) chunk_registry: Vec<RIO_BUFFERID>,
    // RIO Request Queues per socket (raw handle)
    pub(crate) rio_rqs: FxHashMap<HANDLE, RIO_RQ>,
    // RIO Request Queues for registered files (O(1) lookup)
    pub(crate) registered_rio_rqs: Vec<Option<RIO_RQ>>,
    // RIO Registration for Slab Pages (for Address Buffers)
    // Maps PageIndex -> (RIO_BUFFERID, BaseAddress, Length)
    pub(crate) slab_rio_pages: Vec<Option<(RIO_BUFFERID, usize, usize)>>,
    pub(crate) rq_depth: u32,
    pub(crate) dispatch: RioDispatch,
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
        let entry = ctx as usize as *const OverlappedEntry;
        let user_data = unsafe { (*entry).user_data };
        let generation = unsafe { (*entry).generation };
        Some((user_data, generation))
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Self> {
        let table = &ext.rio_table;

        // Construct dispatch table, failing if any required function is missing
        let dispatch = RioDispatch {
            create_cq: table
                .RIOCreateCompletionQueue
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIOCreateCompletionQueue function pointer missing",
                    )
                })?,
            create_rq: table
                .RIOCreateRequestQueue
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIOCreateRequestQueue function pointer missing",
                    )
                })?,
            register_buffer: table
                .RIORegisterBuffer
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIORegisterBuffer function pointer missing",
                    )
                })?,
            deregister_buffer: table
                .RIODeregisterBuffer
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIODeregisterBuffer function pointer missing",
                    )
                })?,
            dequeue: table
                .RIODequeueCompletion
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIODequeueCompletion function pointer missing",
                    )
                })?,
            notify: table
                .RIONotify
                .ok_or_else(|| {
                    io_msg(IocpErrorContext::Rio, "RIONotify function pointer missing")
                })?,
            close_cq: table
                .RIOCloseCompletionQueue
                .ok_or_else(|| {
                    io_msg(
                        IocpErrorContext::Rio,
                        "RIOCloseCompletionQueue function pointer missing",
                    )
                })?,
            receive: table
                .RIOReceive
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOReceive function pointer missing"))?,
            send: table
                .RIOSend
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSend function pointer missing"))?,
            send_ex: table
                .RIOSendEx
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSendEx function pointer missing"))?,
            receive_ex: table
                .RIOReceiveEx
                .ok_or_else(|| {
                    io_msg(IocpErrorContext::Rio, "RIOReceiveEx function pointer missing")
                })?,
        };

        // RIO_EVENT_KEY is defined in iocp.rs as usize::MAX - 1
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
                format!("RIOCreateCompletionQueue failed: entries={entries}, queue_size={queue_size}"),
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

        Ok(Self {
            cq,
            _notify_overlapped: notify_overlapped,
            chunk_registry: Vec::new(),
            rio_rqs: FxHashMap::default(),
            registered_rio_rqs: Vec::new(),
            slab_rio_pages: Vec::new(),
            rq_depth: entries.clamp(32, 256),
            dispatch,
        })
    }

    pub fn resize_registered_rqs(&mut self, size: usize) {
        if size > self.registered_rio_rqs.len() {
            self.registered_rio_rqs.resize(size, None);
        }
    }

    pub fn clear_registered_rq(&mut self, idx: usize) {
        if idx < self.registered_rio_rqs.len() {
            self.registered_rio_rqs[idx] = None;
        }
    }

    pub fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        let reg_fn = self.dispatch.register_buffer;
        let id_idx = id as usize;

        if id_idx >= self.chunk_registry.len() {
            self.chunk_registry.resize(id_idx + 1, RIO_INVALID_BUFFERID);
        }

        // Check if already registered? For now assuming simple update or overwrite.
        // Note: RIO buffers need to be deregistered? implementation specific.
        // Here we assume new registration.

        if let Some(existing) = self.chunk_registry.get(id_idx).copied()
            && existing != RIO_INVALID_BUFFERID
        {
            unsafe { (self.dispatch.deregister_buffer)(existing) };
        }

        let buf_id = unsafe { reg_fn(ptr, len as u32) };
        if buf_id == RIO_INVALID_BUFFERID {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIORegisterBuffer failed: chunk_id={id}, len={len}"),
            ));
        }

        self.chunk_registry[id_idx] = buf_id;
        Ok(())
    }

    pub fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
    ) -> io::Result<()> {
        let dequeue_fn = self.dispatch.dequeue;

        // Stack buffer for completions
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

            let ops_local = &mut ops.local;
            let ops_shared = &ops.shared;

            for res in results.iter().take(count as usize) {
                let Some((user_data, completion_generation)) =
                    Self::decode_request_context(res.RequestContext)
                else {
                    continue;
                };

                if user_data < ops_local.len() {
                    let op = &mut ops_local[user_data];
                    let slot = &ops_shared.slots[user_data];
                    if op.platform_data.generation != completion_generation {
                        // Stale RIO completion for a previously recycled slot.
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
                        // Two-phase reclaim for cancelled RIO ops:
                        // 1) cancel path may have already completed user future;
                        // 2) this late CQE only marks drain complete;
                        // 3) recycle slot only after future consumed the completion.
                        if op.platform_data.rio_needs_drain {
                            op.platform_data.rio_drained = true;
                            let slot_state = slot.state.load(Ordering::Acquire);
                            if slot_state == STATE_CONSUMED {
                                let _ = std::mem::take(&mut op.platform_data);
                                ops.free_indices.push(user_data);
                            }
                        } else {
                            let _ = std::mem::take(&mut op.platform_data);
                            ops.free_indices.push(user_data);
                        }
                    }
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
        Ok(())
    }

    // Check if slab page is registered, register if not (lazy)
    pub fn ensure_slab_page_registration(
        &mut self,
        page_idx: usize,
        resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<()> {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = resolver(page_idx) {
                let reg_fn = self.dispatch.register_buffer;
                let id = unsafe { reg_fn(ptr, len as u32) };
                if id == RIO_INVALID_BUFFERID {
                    return Err(io_error(
                        IocpErrorContext::Rio,
                        Self::last_wsa_error(),
                        format!("RIORegisterBuffer failed for slab page: page_idx={page_idx}, len={len}"),
                    ));
                }
                self.slab_rio_pages[page_idx] = Some((id, ptr as usize, len));
            } else {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO slab page not found in registry: page_idx={page_idx}"),
                ));
            }
        }
        Ok(())
    }

    fn ensure_rq(&mut self, handle: HANDLE, fd: IoFd) -> io::Result<RIO_RQ> {
        // fast path for registered files
        if let IoFd::Fixed(idx) = fd {
            let idx = idx as usize;
            if let Some(Some(rq)) = self.registered_rio_rqs.get(idx) {
                return Ok(*rq);
            }
        } else {
            // Fallback for raw handles
            if let Some(&rq) = self.rio_rqs.get(&handle) {
                return Ok(rq);
            }
        }

        let create_fn = self.dispatch.create_rq;

        let max_outstanding_recvs = self.rq_depth;
        let max_outstanding_sends = self.rq_depth;

        let rq = unsafe {
            create_fn(
                handle as usize, // Corrected cast handle: HANDLE (*mut c_void) -> usize
                max_outstanding_recvs,
                1,
                max_outstanding_sends,
                1,
                self.cq,
                self.cq,
                std::ptr::null_mut(),
            )
        };

        if rq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOCreateRequestQueue failed: fd={fd:?}, handle={handle:?}, rq_depth={}", self.rq_depth),
            ));
        }

        if let IoFd::Fixed(idx) = fd {
            let idx = idx as usize;
            if idx < self.registered_rio_rqs.len() {
                self.registered_rio_rqs[idx] = Some(rq);
            }
        } else {
            self.rio_rqs.insert(handle, rq);
        }
        Ok(rq)
    }

    pub fn try_submit_recv(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &mut FixedBuf,
        overlapped: *mut OVERLAPPED,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let info = buf.resolve_region_info();
        // Check chunk registry
        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(info.id, chunk_info.ptr.as_ptr(), chunk_info.len.get())?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        let buffer_id = match buffer_id {
            Some(id) => id,
            None => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO chunk not registered for recv: chunk_id={}", info.id),
                ));
            }
        };

        // Now self.registered_bufs borrow has ended
        let rq = self.ensure_rq(handle, fd)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: info.offset as u32,
            Length: buf.capacity() as u32,
        };

        let recv_fn = self.dispatch.receive;
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe { recv_fn(rq, &rio_buf, 1, 0, request_context) };

        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOReceive submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_send(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &FixedBuf,
        overlapped: *mut OVERLAPPED,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let info = buf.resolve_region_info();
        // Check chunk registry
        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(info.id, chunk_info.ptr.as_ptr(), chunk_info.len.get())?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        let buffer_id = match buffer_id {
            Some(id) => id,
            None => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO chunk not registered for send: chunk_id={}", info.id),
                ));
            }
        };

        let rq = self.ensure_rq(handle, fd)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: info.offset as u32,
            Length: buf.len() as u32,
        };

        let send_fn = self.dispatch.send;
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe { send_fn(rq, &rio_buf, 1, 0, request_context) };

        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOSend submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        Ok(SubmissionResult::Pending)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_submit_send_to(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &FixedBuf,
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        overlapped: *mut OVERLAPPED,
        page_idx: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<SubmissionResult> {
        let info = buf.resolve_region_info();
        // Check chunk registry
        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(info.id, chunk_info.ptr.as_ptr(), chunk_info.len.get())?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        let buffer_id = match buffer_id {
            Some(id) => id,
            None => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO chunk not registered for send_to: chunk_id={}", info.id),
                ));
            }
        };

        // Lazy register slab page
        self.ensure_slab_page_registration(page_idx, slab_resolver)?;

        // Values are now guaranteed to be present if ensure_slab_page_registration succeeded
        let (addr_buf_id, base_addr, slab_len) = self.slab_rio_pages[page_idx].unwrap();

        let rq = self.ensure_rq(handle, fd)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: info.offset as u32,
            Length: buf.len() as u32,
        };

        if addr_ptr.is_null() {
            return Err(io_msg(
                IocpErrorContext::Rio,
                "RIO send_to received null remote address pointer",
            ));
        }
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_addr_len = if family == AF_INET {
            std::mem::size_of::<SOCKADDR_IN>() as usize
        } else if family == AF_INET6 {
            std::mem::size_of::<SOCKADDR_IN6>() as usize
        } else {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!("RIO send_to unsupported address family: family={family}"),
            ));
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

        // RIO address buffers are consumed as SOCKADDR_INET shape.
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

        let send_ex_fn = self.dispatch.send_ex;
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe {
            send_ex_fn(
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
        Ok(SubmissionResult::Pending)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_submit_recv_from(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &mut FixedBuf,
        addr_ptr: *const std::ffi::c_void,
        _len_ptr: *const i32,
        overlapped: *mut OVERLAPPED,
        page_idx: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<SubmissionResult> {
        let info = buf.resolve_region_info();
        // Check chunk registry
        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(info.id, chunk_info.ptr.as_ptr(), chunk_info.len.get())?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        let buffer_id = match buffer_id {
            Some(id) => id,
            None => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO chunk not registered for recv_from: chunk_id={}", info.id),
                ));
            }
        };

        // Lazy register slab page
        self.ensure_slab_page_registration(page_idx, slab_resolver)?;

        // Values are now guaranteed to be present
        let (addr_buf_id, base_addr, slab_len) = self.slab_rio_pages[page_idx].unwrap();

        let rq = self.ensure_rq(handle, fd)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: info.offset as u32,
            Length: buf.capacity() as u32,
        };

        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        let addr_len = std::mem::size_of::<SockAddrStorage>();
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
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };

        let recv_ex_fn = self.dispatch.receive_ex;
        let request_context = Self::encode_request_context(overlapped);

        let ret = unsafe {
            recv_ex_fn(
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
        Ok(SubmissionResult::Pending)
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        let mut deregistered = FxHashSet::default();
        for id in self.chunk_registry.iter().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (self.dispatch.deregister_buffer)(id) };
            }
        }
        for (id, _, _) in self.slab_rio_pages.iter().flatten().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (self.dispatch.deregister_buffer)(id) };
            }
        }

        if self.cq != 0 {
            unsafe { (self.dispatch.close_cq)(self.cq) };
        }
    }
}
