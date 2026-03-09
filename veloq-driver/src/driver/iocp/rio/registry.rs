use super::RioDispatch;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::op::IoFd;
use rustc_hash::FxHashMap;
use std::io;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_BUFFERID, RIO_CQ, RIO_RQ, WSAGetLastError};

pub const RIO_INVALID_BUFFERID: RIO_BUFFERID = 0 as RIO_BUFFERID;

pub struct RioRegistry {
    pub(crate) chunk_registry: Vec<RIO_BUFFERID>,
    /// RIO Request Queues per socket (raw handle)
    pub(crate) rio_rqs: FxHashMap<HANDLE, RIO_RQ>,
    /// RIO Request Queues for registered files (O(1) lookup)
    pub(crate) registered_rio_rqs: Vec<Option<RIO_RQ>>,
    /// RIO Registration for Slab Pages (for Address Buffers)
    /// Maps PageIndex -> (RIO_BUFFERID, BaseAddress, Length)
    pub(crate) slab_rio_pages: Vec<Option<(RIO_BUFFERID, usize, usize)>>,
    /// Heap-buffer lazy registrations: (ptr, cap, cookie) -> RIO_BUFFERID
    pub(crate) heap_rio_bufs: FxHashMap<(usize, usize, u64), RIO_BUFFERID>,
    pub(crate) rq_depth: u32,
}

impl RioRegistry {
    pub fn new(rq_depth: u32) -> Self {
        Self {
            chunk_registry: Vec::new(),
            rio_rqs: FxHashMap::default(),
            registered_rio_rqs: Vec::new(),
            slab_rio_pages: Vec::new(),
            heap_rio_bufs: FxHashMap::default(),
            rq_depth,
        }
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
        dispatch: &RioDispatch,
    ) -> io::Result<(RIO_BUFFERID, u32)> {
        let info = buf.resolve_region_info();

        // Heap-allocated buffers use sentinel id=u16::MAX (no pre-registration).
        if info.id == u16::MAX {
            let key = (buf.as_ptr() as usize, buf.capacity(), info.cookie);
            if let Some(&id) = self.heap_rio_bufs.get(&key) {
                return Ok((id, info.offset as u32));
            }

            // Simple eviction to prevent unbounded growth of registered heap buffers.
            // Note: RIO_BUFFERIDs are a limited kernel resource.
            if self.heap_rio_bufs.len() >= 1024 {
                // We clear and deregister everything.
                // UNRESOLVED: This is only 100% safe if no heap-based IO is pended.
                // However, the cookie already prevents the dangerous "wrong buffer mapping" crash.
                for id in self.heap_rio_bufs.values().copied() {
                    unsafe { (dispatch.deregister_buffer)(id) };
                }
                self.heap_rio_bufs.clear();
            }

            let id = unsafe { (dispatch.register_buffer)(buf.as_ptr(), buf.capacity() as u32) };
            if id == RIO_INVALID_BUFFERID {
                return Err(io_error(
                    IocpErrorContext::Rio,
                    Self::last_wsa_error(),
                    format!(
                        "RIORegisterBuffer failed for heap buffer: ptr=0x{:x}, cap={}, cookie={}",
                        key.0, key.1, key.2
                    ),
                ));
            }

            self.heap_rio_bufs.insert(key, id);
            return Ok((id, info.offset as u32));
        }

        let mut buffer_id = match self.chunk_registry.get(info.id as usize) {
            Some(&id) if id != RIO_INVALID_BUFFERID => Some(id),
            _ => None,
        };

        if buffer_id.is_none()
            && let Some(chunk_info) = registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(
                info.id,
                chunk_info.ptr.as_ptr(),
                chunk_info.len.get(),
                dispatch,
            )?;
            buffer_id = Some(self.chunk_registry[info.id as usize]);
        }

        match buffer_id {
            Some(id) => Ok((id, info.offset as u32)),
            None => Err(io_msg(
                IocpErrorContext::Rio,
                format!("RIO chunk not registered: chunk_id={}", info.id),
            )),
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

    pub fn register_chunk(
        &mut self,
        id: u16,
        ptr: *const u8,
        len: usize,
        dispatch: &RioDispatch,
    ) -> io::Result<()> {
        let reg_fn = dispatch.register_buffer;
        let id_idx = id as usize;

        if id_idx >= self.chunk_registry.len() {
            self.chunk_registry.resize(id_idx + 1, RIO_INVALID_BUFFERID);
        }

        if let Some(existing) = self.chunk_registry.get(id_idx).copied()
            && existing != RIO_INVALID_BUFFERID
        {
            unsafe { (dispatch.deregister_buffer)(existing) };
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

    pub fn ensure_slab_page_registration(
        &mut self,
        page_idx: usize,
        resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
        dispatch: &RioDispatch,
    ) -> io::Result<()> {
        if page_idx >= self.slab_rio_pages.len() {
            self.slab_rio_pages.resize(page_idx + 1, None);
        }

        if self.slab_rio_pages[page_idx].is_none() {
            if let Some((ptr, len)) = resolver(page_idx) {
                let reg_fn = dispatch.register_buffer;
                let id = unsafe { reg_fn(ptr, len as u32) };
                if id == RIO_INVALID_BUFFERID {
                    return Err(io_error(
                        IocpErrorContext::Rio,
                        Self::last_wsa_error(),
                        format!(
                            "RIORegisterBuffer failed for slab page: page_idx={page_idx}, len={len}"
                        ),
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

    pub fn ensure_rq(
        &mut self,
        handle: HANDLE,
        fd: IoFd,
        cq: RIO_CQ,
        dispatch: &RioDispatch,
    ) -> io::Result<RIO_RQ> {
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

        let create_fn = dispatch.create_rq;

        let max_outstanding_recvs = self.rq_depth;
        let max_outstanding_sends = self.rq_depth;

        let rq = unsafe {
            create_fn(
                handle as usize,
                max_outstanding_recvs,
                1,
                max_outstanding_sends,
                1,
                cq,
                cq,
                std::ptr::null_mut(),
            )
        };

        if rq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOCreateRequestQueue failed: fd={fd:?}, handle={handle:?}, rq_depth={}",
                    self.rq_depth
                ),
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

    pub fn cleanup_deregister(&mut self, dispatch: &RioDispatch) {
        use std::collections::HashSet;
        let mut deregistered = HashSet::new();

        for id in self.chunk_registry.iter().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (dispatch.deregister_buffer)(id) };
            }
        }
        for (id, _, _) in self.slab_rio_pages.iter().flatten().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (dispatch.deregister_buffer)(id) };
            }
        }
        for id in self.heap_rio_bufs.values().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (dispatch.deregister_buffer)(id) };
            }
        }

        self.chunk_registry.clear();
        self.slab_rio_pages.clear();
        self.heap_rio_bufs.clear();
        self.rio_rqs.clear();
        self.registered_rio_rqs.clear();
    }
}
