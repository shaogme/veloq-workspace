use crate::SockAddrStorage;
use crate::driver::iocp::IocpOp;
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::op::IoFd;
use rustc_hash::FxHashMap;
use std::io;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ, RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT,
};

// Define constants that might be missing or different in windows-sys
const RIO_INVALID_BUFFERID: RIO_BUFFERID = 0 as RIO_BUFFERID;
const RIO_NOTIFICATION_COMPLETION_TYPE_IOCP: u32 = 1;

#[derive(Debug, Clone, Copy)]
pub struct RioBufferInfo {
    pub(crate) id: RIO_BUFFERID,
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
    pub dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    pub notify: unsafe extern "system" fn(RIO_CQ) -> i32,
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
    pub(crate) registered_bufs: Vec<RioBufferInfo>,
    // RIO Request Queues per socket (raw handle)
    pub(crate) rio_rqs: FxHashMap<HANDLE, RIO_RQ>,
    // RIO Request Queues for registered files (O(1) lookup)
    pub(crate) registered_rio_rqs: Vec<Option<RIO_RQ>>,
    // RIO Registration for Slab Pages (for Address Buffers)
    // Maps PageIndex -> (RIO_BUFFERID, BaseAddress)
    pub(crate) slab_rio_pages: Vec<Option<(RIO_BUFFERID, usize)>>,
    pub(crate) dispatch: RioDispatch,
}

impl RioState {
    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Option<Self>> {
        let table = match &ext.rio_table {
            Some(t) => t,
            None => return Ok(None),
        };

        // Construct dispatch table, failing if any required function is missing
        let dispatch = RioDispatch {
            create_cq: table.RIOCreateCompletionQueue.ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "RIOCreateCompletionQueue missing")
            })?,
            create_rq: table.RIOCreateRequestQueue.ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "RIOCreateRequestQueue missing")
            })?,
            register_buffer: table
                .RIORegisterBuffer
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIORegisterBuffer missing"))?,
            dequeue: table.RIODequeueCompletion.ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "RIODequeueCompletion missing")
            })?,
            notify: table
                .RIONotify
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIONotify missing"))?,
            receive: table
                .RIOReceive
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIOReceive missing"))?,
            send: table
                .RIOSend
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIOSend missing"))?,
            send_ex: table
                .RIOSendEx
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIOSendEx missing"))?,
            receive_ex: table
                .RIOReceiveEx
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "RIOReceiveEx missing"))?,
        };

        if let Some(create_fn) = Some(dispatch.create_cq) {
            // RIO_EVENT_KEY is defined in iocp.rs as usize::MAX - 1
            const RIO_EVENT_KEY: usize = usize::MAX - 1;

            let notification = RIO_NOTIFICATION_COMPLETION {
                Type: RIO_NOTIFICATION_COMPLETION_TYPE_IOCP as i32,
                Anonymous: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0 {
                    Iocp:
                        windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0_1 {
                            IocpHandle: port,
                            CompletionKey: RIO_EVENT_KEY as *mut std::ffi::c_void,
                            Overlapped: std::ptr::null_mut(),
                        },
                },
            };

            let queue_size = entries.max(1024);
            let cq = unsafe { create_fn(queue_size, &notification as *const _) };

            if cq == 0 {
                return Ok(None);
            }

            Ok(Some(Self {
                cq,
                registered_bufs: Vec::new(),
                rio_rqs: FxHashMap::default(),
                registered_rio_rqs: Vec::new(),
                slab_rio_pages: Vec::new(),
                dispatch,
            }))
        } else {
            Ok(None)
        }
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

    pub fn register_buffers(&mut self, regions: &[veloq_buf::BufferRegion]) -> io::Result<()> {
        let reg_fn = self.dispatch.register_buffer;

        self.registered_bufs.clear();
        self.registered_bufs.reserve(regions.len());

        for region in regions {
            let len = region.len();
            let id = unsafe { reg_fn(region.as_ptr() as *const u8, len as u32) };

            if id == RIO_INVALID_BUFFERID {
                return Err(io::Error::last_os_error());
            }

            self.registered_bufs.push(RioBufferInfo { id });
        }
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
                return Err(io::Error::from_raw_os_error(
                    windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE as i32,
                ));
            }

            if count == 0 {
                break;
            }

            for i in 0..count as usize {
                let res = &results[i];
                let user_data = res.RequestContext as usize;

                if ops.contains(user_data) {
                    let op = &mut ops[user_data];

                    if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                        let result = if res.Status == 0 {
                            Ok(res.BytesTransferred as usize)
                        } else {
                            Err(io::Error::from_raw_os_error(res.Status as i32))
                        };

                        op.platform_data.lifecycle = OpLifecycle::Completed(result);
                        if let Some(waker) = op.waker.take() {
                            waker.wake();
                        }
                    } else if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                        // If cancelled, we can now remove it
                        ops.remove(user_data);
                    }
                }
            }

            if count < MAX_RIO_RESULTS as u32 {
                break;
            }
        }

        let notify_fn = self.dispatch.notify;
        let ret = unsafe { notify_fn(self.cq) };
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(ret as i32));
        }
        Ok(())
    }

    // Check if slab page is registered, register if not
    pub fn ensure_slab_page_registration(
        &mut self,
        page_idx: usize,
        ops: &OpRegistry<IocpOp, IocpOpState>,
    ) {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = ops.get_page_slice(page_idx) {
                let reg_fn = self.dispatch.register_buffer;
                let id = unsafe { reg_fn(ptr, len as u32) };
                if id != RIO_INVALID_BUFFERID {
                    self.slab_rio_pages[page_idx] = Some((id, ptr as usize));
                }
            }
        }
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

        // Queue sizes
        const MAX_OUTSTANDING_RECVS: u32 = 1024;
        const MAX_OUTSTANDING_SENDS: u32 = 1024;

        let rq = unsafe {
            create_fn(
                handle as usize, // Corrected cast handle: HANDLE (*mut c_void) -> usize
                MAX_OUTSTANDING_RECVS,
                1,
                MAX_OUTSTANDING_SENDS,
                1,
                self.cq,
                self.cq,
                std::ptr::null_mut(),
            )
        };

        if rq == 0 {
            return Err(io::Error::last_os_error());
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
        user_data: usize,
    ) -> io::Result<Option<SubmissionResult>> {
        if buf.buf_index().is_some() {
            let (idx, offset) = buf.resolve_region_info();
            // Solve borrow checker issue: extraction of Copy data (id)
            let buffer_id = if let Some(info) = self.registered_bufs.get(idx) {
                info.id
            } else {
                return Ok(None);
            };

            // Now self.registered_bufs borrow has ended
            let rq = self.ensure_rq(handle, fd)?;

            let rio_buf = RIO_BUF {
                BufferId: buffer_id,
                Offset: offset as u32,
                Length: buf.capacity() as u32,
            };

            let recv_fn = self.dispatch.receive;
            let request_context = user_data as *mut std::ffi::c_void;

            let ret = unsafe { recv_fn(rq, &rio_buf, 1, 0, request_context) };

            if ret == 0 {
                return Err(io::Error::last_os_error());
            }
            return Ok(Some(SubmissionResult::Pending));
        }
        Ok(None)
    }

    pub fn try_submit_send(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &FixedBuf,
        user_data: usize,
    ) -> io::Result<Option<SubmissionResult>> {
        if buf.buf_index().is_some() {
            let (idx, offset) = buf.resolve_region_info();
            // Solve borrow checker issue
            let buffer_id = if let Some(info) = self.registered_bufs.get(idx) {
                info.id
            } else {
                return Ok(None);
            };

            let rq = self.ensure_rq(handle, fd)?;

            let rio_buf = RIO_BUF {
                BufferId: buffer_id,
                Offset: offset as u32,
                Length: buf.len() as u32,
            };

            let send_fn = self.dispatch.send;
            let request_context = user_data as *mut std::ffi::c_void;

            let ret = unsafe { send_fn(rq, &rio_buf, 1, 0, request_context) };

            if ret == 0 {
                return Err(io::Error::last_os_error());
            }
            return Ok(Some(SubmissionResult::Pending));
        }
        Ok(None)
    }

    pub fn try_submit_send_to(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &FixedBuf,
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        user_data: usize,
        // Removed `ops` here. Caller must ensure slab registration.
    ) -> io::Result<Option<SubmissionResult>> {
        if buf.buf_index().is_none() {
            return Ok(None);
        }

        let (idx, offset) = buf.resolve_region_info();
        let buffer_id = match self.registered_bufs.get(idx) {
            Some(i) => i.id,
            None => {
                eprintln!("RIO: Buffer index {} not found for send_to", idx);
                return Ok(None);
            }
        };

        // Use the constant from OpRegistry to ensure we match the slab implementation
        const PAGE_SHIFT: usize = OpRegistry::<IocpOp, IocpOpState>::PAGE_SHIFT;
        let page_idx = user_data >> PAGE_SHIFT;

        // Copy values out to avoid holding borrow on self.slab_rio_pages while calling ensure_rq
        let (addr_buf_id, base_addr) = if let Some(Some(entry)) = self.slab_rio_pages.get(page_idx)
        {
            *entry
        } else {
            eprintln!("RIO: Slab page not registered for send_to");
            return Ok(None);
        };

        let rq = self.ensure_rq(handle, fd)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset as u32,
            Length: buf.len() as u32,
        };

        let addr_offset = (addr_ptr as usize - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: addr_len as u32,
        };

        let send_ex_fn = self.dispatch.send_ex;
        let request_context = user_data as *mut std::ffi::c_void;

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
            return Err(io::Error::last_os_error());
        }
        Ok(Some(SubmissionResult::Pending))
    }

    pub fn try_submit_recv_from(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &mut FixedBuf,
        addr_ptr: *const std::ffi::c_void,
        len_ptr: *const i32,
        user_data: usize,
        // Removed `ops`
    ) -> io::Result<Option<SubmissionResult>> {
        if buf.buf_index().is_none() {
            return Ok(None);
        }

        let (idx, offset) = buf.resolve_region_info();
        let buffer_id = match self.registered_bufs.get(idx) {
            Some(i) => i.id,
            None => {
                eprintln!("RIO: Buffer index {} not found for recv_from", idx);
                return Ok(None);
            }
        };

        const PAGE_SHIFT: usize = OpRegistry::<IocpOp, IocpOpState>::PAGE_SHIFT;
        let page_idx = user_data >> PAGE_SHIFT;

        // Copy values out to avoid holding borrow on self
        let (addr_buf_id, base_addr) = if let Some(Some(entry)) = self.slab_rio_pages.get(page_idx)
        {
            *entry
        } else {
            eprintln!("RIO: Slab page not registered for recv_from");
            return Ok(None);
        };

        let rq = self.ensure_rq(handle, fd)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset as u32,
            Length: buf.capacity() as u32,
        };

        let addr_offset = (addr_ptr as usize - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };

        let len_offset = (len_ptr as usize - base_addr) as u32;
        let len_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: len_offset,
            Length: 4,
        };

        let recv_ex_fn = self.dispatch.receive_ex;
        let request_context = user_data as *mut std::ffi::c_void;

        let ret = unsafe {
            recv_ex_fn(
                rq,
                &data_buf,
                1,
                std::ptr::null(),
                &addr_buf,
                &len_buf,
                std::ptr::null(),
                0,
                request_context,
            )
        };

        if ret == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Some(SubmissionResult::Pending))
    }
}
