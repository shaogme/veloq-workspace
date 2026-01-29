use crate::io::buffer::FixedBuf;
use crate::io::driver::iocp::IocpOp;
use crate::io::driver::iocp::ext::Extensions;
use crate::io::driver::iocp::submit::SubmissionResult;
use crate::io::driver::iocp::{IocpOpState, OpLifecycle};
use crate::io::driver::op_registry::OpRegistry;
use crate::io::op::IoFd;
use crate::io::socket::SockAddrStorage;
use rustc_hash::FxHashMap;
use std::io;
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
}

impl RioState {
    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Option<Self>> {
        let table = match &ext.rio_table {
            Some(t) => t,
            None => return Ok(None),
        };

        if let Some(create_fn) = table.RIOCreateCompletionQueue {
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

    pub fn register_buffers(
        &mut self,
        regions: &[crate::io::buffer::BufferRegion],
        ext: &Extensions,
    ) -> io::Result<()> {
        let reg_fn = match &ext.rio_table {
            Some(table) => table.RIORegisterBuffer,
            None => return Ok(()),
        };

        if let Some(reg_fn) = reg_fn {
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
        }
        Ok(())
    }

    pub fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        ext: &Extensions,
    ) -> io::Result<()> {
        let dequeue_fn = ext
            .rio_table
            .as_ref()
            .and_then(|t| t.RIODequeueCompletion)
            .ok_or(io::Error::new(io::ErrorKind::Other, "RIO not initialized"))?;

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

        let notify_fn = ext.rio_table.as_ref().unwrap().RIONotify.unwrap();
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
        ext: &Extensions,
    ) {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = ops.get_page_slice(page_idx) {
                if let Some(table) = &ext.rio_table
                    && let Some(reg_fn) = table.RIORegisterBuffer
                {
                    let id = unsafe { reg_fn(ptr, len as u32) };
                    if id != RIO_INVALID_BUFFERID {
                        self.slab_rio_pages[page_idx] = Some((id, ptr as usize));
                    }
                }
            }
        }
    }

    fn ensure_rq(&mut self, handle: HANDLE, fd: IoFd, ext: &Extensions) -> io::Result<RIO_RQ> {
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

        let table = ext.rio_table.as_ref().ok_or(io::Error::new(
            io::ErrorKind::Unsupported,
            "RIO not initialized",
        ))?;

        let create_fn = table.RIOCreateRequestQueue.unwrap();

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
        ext: &Extensions,
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
            let rq = self.ensure_rq(handle, fd, ext)?;

            let rio_buf = RIO_BUF {
                BufferId: buffer_id,
                Offset: offset as u32,
                Length: buf.capacity() as u32,
            };

            let table = ext.rio_table.as_ref().unwrap();
            let recv_fn = table.RIOReceive.unwrap();
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
        ext: &Extensions,
    ) -> io::Result<Option<SubmissionResult>> {
        if buf.buf_index().is_some() {
            let (idx, offset) = buf.resolve_region_info();
            // Solve borrow checker issue
            let buffer_id = if let Some(info) = self.registered_bufs.get(idx) {
                info.id
            } else {
                return Ok(None);
            };

            let rq = self.ensure_rq(handle, fd, ext)?;

            let rio_buf = RIO_BUF {
                BufferId: buffer_id,
                Offset: offset as u32,
                Length: buf.len() as u32,
            };

            let table = ext.rio_table.as_ref().unwrap();
            let send_fn = table.RIOSend.unwrap();
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
        ext: &Extensions,
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

        let rq = self.ensure_rq(handle, fd, ext)?;

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

        let table = ext.rio_table.as_ref().unwrap();
        let send_ex_fn = table.RIOSendEx.unwrap();
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
        ext: &Extensions,
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

        let rq = self.ensure_rq(handle, fd, ext)?;

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

        let table = ext.rio_table.as_ref().unwrap();
        let recv_ex_fn = table.RIOReceiveEx.unwrap();
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
