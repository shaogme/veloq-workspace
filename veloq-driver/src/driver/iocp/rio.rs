use crate::SockAddrStorage;
use crate::driver::iocp::IocpOp;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::op::RecvFromPayload;
use crate::driver::iocp::submit::SubmissionResult;
use crate::driver::iocp::{IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::OverlappedEntry;
use crate::driver::slot::{STATE_COMPLETED, STATE_CONSUMED};
use crate::op::IoFd;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};
use tracing::warn;
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
const UDP_POOL_USER_DATA: usize = usize::MAX - 2;
const UDP_RECV_POOL_MIN_CREDITS: usize = 2;
const UDP_RECV_POOL_INITIAL_CREDITS: usize = 4;
const UDP_RECV_POOL_MAX_CREDITS: usize = 16;
const UDP_RECV_POOL_BUF_SIZE: usize = 65_536;
const UDP_RECV_POOL_QUEUE_CAP: usize = 256;
const UDP_POOL_DRAIN_SOFT_TIMEOUT: Duration = Duration::from_secs(5);
const UDP_POOL_DRAIN_HARD_TIMEOUT: Duration = Duration::from_secs(30);
const UDP_POOL_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(1);

struct UdpRecvDatagram {
    data: Vec<u8>,
    addr: SockAddrStorage,
    addr_len: i32,
}

struct UdpRecvPoolSlot {
    data: Box<[u8]>,
    addr: Box<SockAddrStorage>,
    data_buf_id: RIO_BUFFERID,
    addr_buf_id: RIO_BUFFERID,
    in_flight: bool,
    stop_requested: bool,
}

struct UdpRecvPool {
    rq: RIO_RQ,
    slots: Vec<UdpRecvPoolSlot>,
    queue: VecDeque<UdpRecvDatagram>,
    waiters: VecDeque<(usize, u32)>,
    min_credits: usize,
    max_credits: usize,
    target_credits: usize,
    idle_hits: u32,
    shutting_down: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct UdpRecvPoolDebugStats {
    pub min_credits: usize,
    pub max_credits: usize,
    pub target_credits: usize,
    pub slots_len: usize,
    pub in_flight: usize,
    pub waiters_len: usize,
    pub queue_len: usize,
    pub idle_hits: u32,
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
    pub(crate) chunk_registry: Vec<RIO_BUFFERID>,
    // RIO Request Queues per socket (raw handle)
    pub(crate) rio_rqs: FxHashMap<HANDLE, RIO_RQ>,
    // RIO Request Queues for registered files (O(1) lookup)
    pub(crate) registered_rio_rqs: Vec<Option<RIO_RQ>>,
    // RIO Registration for Slab Pages (for Address Buffers)
    // Maps PageIndex -> (RIO_BUFFERID, BaseAddress, Length)
    pub(crate) slab_rio_pages: Vec<Option<(RIO_BUFFERID, usize, usize)>>,
    // Heap-buffer lazy registrations: (ptr, cap) -> RIO_BUFFERID
    pub(crate) heap_rio_bufs: FxHashMap<(usize, usize), RIO_BUFFERID>,
    udp_recv_pools: FxHashMap<HANDLE, UdpRecvPool>,
    udp_ctx_map: FxHashMap<u32, (HANDLE, usize)>,
    udp_next_ctx: u32,
    pub(crate) rq_depth: u32,
    pub(crate) dispatch: RioDispatch,
}

impl RioState {
    const POOL_CTX_TAG: usize = 1;

    #[inline]
    fn encode_request_context(overlapped: *mut OVERLAPPED) -> *const std::ffi::c_void {
        overlapped as *const std::ffi::c_void
    }

    #[inline]
    fn encode_pool_context(token: u32) -> *const std::ffi::c_void {
        (((token as usize) << 1) | Self::POOL_CTX_TAG) as *const std::ffi::c_void
    }

    #[inline]
    fn decode_request_context(ctx: u64) -> Option<(usize, u32)> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & Self::POOL_CTX_TAG) == Self::POOL_CTX_TAG {
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

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(&self, handle: HANDLE) -> Option<UdpRecvPoolDebugStats> {
        self.udp_recv_pools
            .get(&handle)
            .map(|pool| UdpRecvPoolDebugStats {
                min_credits: pool.min_credits,
                max_credits: pool.max_credits,
                target_credits: pool.target_credits,
                slots_len: pool.slots.len(),
                in_flight: pool.slots.iter().filter(|s| s.in_flight).count(),
                waiters_len: pool.waiters.len(),
                queue_len: pool.queue.len(),
                idle_hits: pool.idle_hits,
            })
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        handle: HANDLE,
        ticks: usize,
    ) -> io::Result<()> {
        for _ in 0..ticks {
            self.rebalance_udp_pool(handle)?;
        }
        Ok(())
    }

    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Self> {
        let table = &ext.rio_table;

        // Construct dispatch table, failing if any required function is missing
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

        Ok(Self {
            cq,
            _notify_overlapped: notify_overlapped,
            chunk_registry: Vec::new(),
            rio_rqs: FxHashMap::default(),
            registered_rio_rqs: Vec::new(),
            slab_rio_pages: Vec::new(),
            heap_rio_bufs: FxHashMap::default(),
            udp_recv_pools: FxHashMap::default(),
            udp_ctx_map: FxHashMap::default(),
            udp_next_ctx: 1,
            rq_depth: entries.clamp(32, 256),
            dispatch,
        })
    }

    fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<(RIO_BUFFERID, u32)> {
        let info = buf.resolve_region_info();

        // Heap-allocated buffers use sentinel id=u16::MAX (no pre-registration).
        if info.id == u16::MAX {
            let key = (buf.as_ptr() as usize, buf.capacity());
            if let Some(&id) = self.heap_rio_bufs.get(&key) {
                return Ok((id, info.offset as u32));
            }

            let id =
                unsafe { (self.dispatch.register_buffer)(buf.as_ptr(), buf.capacity() as u32) };
            if id == RIO_INVALID_BUFFERID {
                return Err(io_error(
                    IocpErrorContext::Rio,
                    Self::last_wsa_error(),
                    format!(
                        "RIORegisterBuffer failed for heap buffer: ptr=0x{:x}, cap={}",
                        key.0, key.1
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
            self.register_chunk(info.id, chunk_info.ptr.as_ptr(), chunk_info.len.get())?;
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

    fn alloc_udp_ctx_token(&mut self, handle: HANDLE, slot_idx: usize) -> u32 {
        loop {
            let token = self.udp_next_ctx;
            self.udp_next_ctx = self.udp_next_ctx.wrapping_add(1);
            if token == 0 {
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(e) = self.udp_ctx_map.entry(token) {
                e.insert((handle, slot_idx));
                return token;
            }
        }
    }

    fn create_udp_pool_slot(&self) -> io::Result<UdpRecvPoolSlot> {
        let mut data = vec![0u8; UDP_RECV_POOL_BUF_SIZE].into_boxed_slice();
        let mut addr = Box::new(SockAddrStorage::default());

        let data_buf_id =
            unsafe { (self.dispatch.register_buffer)(data.as_mut_ptr(), data.len() as u32) };
        if data_buf_id == RIO_INVALID_BUFFERID {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIORegisterBuffer failed for UDP recv pool data buffer",
            ));
        }

        let addr_buf_id = unsafe {
            (self.dispatch.register_buffer)(
                (&mut *addr as *mut SockAddrStorage).cast::<u8>(),
                std::mem::size_of::<SockAddrStorage>() as u32,
            )
        };
        if addr_buf_id == RIO_INVALID_BUFFERID {
            unsafe { (self.dispatch.deregister_buffer)(data_buf_id) };
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                "RIORegisterBuffer failed for UDP recv pool addr buffer",
            ));
        }

        Ok(UdpRecvPoolSlot {
            data,
            addr,
            data_buf_id,
            addr_buf_id,
            in_flight: false,
            stop_requested: false,
        })
    }

    fn deregister_udp_pool_slot(&self, slot: UdpRecvPoolSlot) {
        if slot.data_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (self.dispatch.deregister_buffer)(slot.data_buf_id) };
        }
        if slot.addr_buf_id != RIO_INVALID_BUFFERID {
            unsafe { (self.dispatch.deregister_buffer)(slot.addr_buf_id) };
        }
    }

    fn submit_udp_pool_slot(&mut self, handle: HANDLE, slot_idx: usize) -> io::Result<()> {
        let rq = {
            let pool = self
                .udp_recv_pools
                .get(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
            if pool.shutting_down {
                return Err(io::Error::from_raw_os_error(
                    windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                ));
            }
            pool.rq
        };

        let token = self.alloc_udp_ctx_token(handle, slot_idx);
        let pool = self
            .udp_recv_pools
            .get_mut(&handle)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
        let slot = pool
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool slot missing"))?;
        slot.in_flight = true;
        slot.stop_requested = false;

        let data_buf = RIO_BUF {
            BufferId: slot.data_buf_id,
            Offset: 0,
            Length: UDP_RECV_POOL_BUF_SIZE as u32,
        };
        let addr_buf = RIO_BUF {
            BufferId: slot.addr_buf_id,
            Offset: 0,
            Length: std::mem::size_of::<SockAddrStorage>() as u32,
        };
        let recv_ex_fn = self.dispatch.receive_ex;
        let req_ctx = Self::encode_pool_context(token);

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
                req_ctx,
            )
        };
        if ret == 0 {
            self.udp_ctx_map.remove(&token);
            if let Some(pool) = self.udp_recv_pools.get_mut(&handle)
                && let Some(slot) = pool.slots.get_mut(slot_idx)
            {
                slot.in_flight = false;
                slot.stop_requested = false;
            }
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceiveEx submit failed for UDP recv pool: handle=0x{:x}, slot_idx={}, rq=0x{:x}",
                    handle as usize, slot_idx, rq as usize
                ),
            ));
        }
        Ok(())
    }

    fn grow_udp_pool_to(&mut self, handle: HANDLE, target: usize) -> io::Result<()> {
        loop {
            let (current, shutting_down) = {
                let pool = self
                    .udp_recv_pools
                    .get(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                (pool.slots.len(), pool.shutting_down)
            };
            if shutting_down {
                return Ok(());
            }
            if current >= target {
                return Ok(());
            }

            let slot = self.create_udp_pool_slot()?;
            {
                let pool = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;
                pool.slots.push(slot);
            }
            let idx = current;
            if let Err(e) = self.submit_udp_pool_slot(handle, idx) {
                let popped = self
                    .udp_recv_pools
                    .get_mut(&handle)
                    .and_then(|pool| pool.slots.pop());
                if let Some(slot) = popped {
                    self.deregister_udp_pool_slot(slot);
                }
                return Err(e);
            }
        }
    }

    fn trim_udp_pool_tail(&mut self, handle: HANDLE) {
        loop {
            let maybe_slot = {
                let Some(pool) = self.udp_recv_pools.get_mut(&handle) else {
                    return;
                };
                if !pool.shutting_down && pool.slots.len() <= pool.target_credits {
                    return;
                }
                if pool.slots.last().is_some_and(|slot| slot.in_flight) {
                    return;
                }
                pool.slots.pop()
            };
            if let Some(slot) = maybe_slot {
                self.deregister_udp_pool_slot(slot);
            } else {
                return;
            }
        }
    }

    fn rebalance_udp_pool(&mut self, handle: HANDLE) -> io::Result<()> {
        if self
            .udp_recv_pools
            .get(&handle)
            .is_some_and(|pool| pool.shutting_down)
        {
            self.trim_udp_pool_tail(handle);
            return Ok(());
        }

        let desired = {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if !pool.waiters.is_empty() {
                let bump = pool.waiters.len().clamp(1, 4);
                pool.target_credits = (pool.target_credits + bump).min(pool.max_credits);
                pool.idle_hits = 0;
            } else if pool.queue.is_empty() {
                pool.idle_hits = pool.idle_hits.saturating_add(1);
                if pool.idle_hits >= 64 && pool.target_credits > pool.min_credits {
                    pool.target_credits -= 1;
                    pool.idle_hits = 0;
                }
            } else {
                pool.idle_hits = 0;
            }

            pool.target_credits
        };

        self.grow_udp_pool_to(handle, desired)?;
        self.trim_udp_pool_tail(handle);
        Ok(())
    }

    fn ensure_udp_recv_pool(&mut self, fd: IoFd, handle: HANDLE) -> io::Result<()> {
        if self.udp_recv_pools.contains_key(&handle) {
            return Ok(());
        }
        let rq = self.ensure_rq(handle, fd)?;
        let min = UDP_RECV_POOL_MIN_CREDITS;
        let max = UDP_RECV_POOL_MAX_CREDITS.max(min);
        let initial = UDP_RECV_POOL_INITIAL_CREDITS.clamp(min, max);

        self.udp_recv_pools.insert(
            handle,
            UdpRecvPool {
                rq,
                slots: Vec::with_capacity(max),
                queue: VecDeque::with_capacity(initial),
                waiters: VecDeque::new(),
                min_credits: min,
                max_credits: max,
                target_credits: initial,
                idle_hits: 0,
                shutting_down: false,
            },
        );

        self.grow_udp_pool_to(handle, initial)?;
        Ok(())
    }

    fn deliver_udp_datagram_to_waiter(
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        user_data: usize,
        expected_generation: u32,
        datagram: UdpRecvDatagram,
    ) -> bool {
        if user_data >= ops.local.len() {
            return false;
        }
        let op = &mut ops.local[user_data];
        let slot = &ops.shared.slots[user_data];
        if op.platform_data.generation != expected_generation {
            return false;
        }
        if !matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
            return false;
        }

        let Some(iocp_op) = (unsafe { &mut *slot.op.get() }).as_mut() else {
            return false;
        };

        let payload: &mut RecvFromPayload = unsafe { &mut *iocp_op.payload.recv_from };
        let copy_len = std::cmp::min(payload.op.buf.capacity(), datagram.data.len());
        payload.op.buf.as_slice_mut()[..copy_len].copy_from_slice(&datagram.data[..copy_len]);
        payload.op.buf.set_len(copy_len);
        payload.addr = datagram.addr;
        payload.addr_len = datagram.addr_len;

        op.platform_data.rio_pool_waiting = false;
        op.platform_data.lifecycle = OpLifecycle::Completed(Ok(copy_len));
        unsafe { *slot.result.get() = Some(Ok(copy_len)) };
        slot.state.store(STATE_COMPLETED, Ordering::Release);
        slot.waker.wake();
        true
    }

    fn dispatch_udp_waiters_for_handle(
        &mut self,
        handle: HANDLE,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
    ) {
        loop {
            let (waiter, datagram) = {
                let Some(pool) = self.udp_recv_pools.get_mut(&handle) else {
                    return;
                };
                let Some(waiter) = pool.waiters.pop_front() else {
                    return;
                };
                let Some(datagram) = pool.queue.pop_front() else {
                    pool.waiters.push_front(waiter);
                    return;
                };
                (waiter, datagram)
            };

            let (user_data, generation) = waiter;
            if !Self::deliver_udp_datagram_to_waiter(ops, user_data, generation, datagram)
                && let Some(pool) = self.udp_recv_pools.get_mut(&handle)
            {
                // stale waiter dropped; continue draining.
                if pool.queue.len() > UDP_RECV_POOL_QUEUE_CAP {
                    let _ = pool.queue.pop_front();
                }
            }
        }
    }

    pub fn try_submit_recv_from_pooled(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        user_data: usize,
        generation: u32,
        buf: &mut FixedBuf,
        addr: &mut SockAddrStorage,
        addr_len: &mut i32,
        overlapped: *mut OVERLAPPED,
    ) -> io::Result<SubmissionResult> {
        self.ensure_udp_recv_pool(fd, handle)?;
        {
            let pool = self
                .udp_recv_pools
                .get_mut(&handle)
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "UDP recv pool missing"))?;

            if pool.shutting_down {
                return Err(io::Error::from_raw_os_error(
                    windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                ));
            }

            if let Some(datagram) = pool.queue.pop_front() {
                let copy_len = std::cmp::min(buf.capacity(), datagram.data.len());
                buf.as_slice_mut()[..copy_len].copy_from_slice(&datagram.data[..copy_len]);
                buf.set_len(copy_len);
                *addr = datagram.addr;
                *addr_len = datagram.addr_len;

                let entry = overlapped as *mut OverlappedEntry;
                unsafe {
                    (*entry).blocking_result = Some(Ok(copy_len));
                }
                return Ok(SubmissionResult::PostToQueue);
            }

            pool.waiters.push_back((user_data, generation));
        }
        self.rebalance_udp_pool(handle)?;
        Ok(SubmissionResult::Pending)
    }

    pub fn cancel_udp_recv_waiter(&mut self, handle: HANDLE, user_data: usize, generation: u32) {
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.waiters.retain(|&(ud, waiter_generation)| {
                !(ud == user_data && waiter_generation == generation)
            });
        }
        let _ = self.rebalance_udp_pool(handle);
    }

    fn count_udp_pool_in_flight(&self) -> usize {
        self.udp_recv_pools
            .values()
            .map(|pool| pool.slots.iter().filter(|s| s.in_flight).count())
            .sum()
    }

    fn ack_udp_pool_completion(&mut self, completion_generation: u32) -> bool {
        let Some((handle, slot_idx)) = self.udp_ctx_map.remove(&completion_generation) else {
            return false;
        };
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle)
            && let Some(slot) = pool.slots.get_mut(slot_idx)
        {
            slot.in_flight = false;
            slot.stop_requested = false;
            return true;
        }
        false
    }

    fn begin_udp_pool_shutdown(&mut self) {
        for pool in self.udp_recv_pools.values_mut() {
            pool.shutting_down = true;
            pool.target_credits = 0;
            pool.queue.clear();
            pool.waiters.clear();
            for slot in &mut pool.slots {
                if slot.in_flight {
                    slot.stop_requested = true;
                }
            }
        }
    }

    pub(crate) fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE) {
        if let Some(pool) = self.udp_recv_pools.get_mut(&handle) {
            pool.shutting_down = true;
            pool.target_credits = 0;
            pool.queue.clear();
            pool.waiters.clear();
            for slot in &mut pool.slots {
                if slot.in_flight {
                    slot.stop_requested = true;
                }
            }
        }
        self.cleanup_shutdown_udp_pool_if_drained(handle);
    }

    fn cleanup_shutdown_udp_pool_if_drained(&mut self, handle: HANDLE) {
        let drained = self.udp_recv_pools.get(&handle).is_some_and(|pool| {
            pool.shutting_down && pool.slots.iter().all(|slot| !slot.in_flight)
        });
        if !drained {
            return;
        }

        self.udp_ctx_map.retain(|_, (h, _)| *h != handle);
        if let Some(pool) = self.udp_recv_pools.remove(&handle) {
            for slot in pool.slots {
                self.deregister_udp_pool_slot(slot);
            }
        }
        self.rio_rqs.remove(&handle);
    }

    fn drain_udp_pool_shutdown_acks(&mut self) {
        if self.udp_recv_pools.is_empty() {
            return;
        }

        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let start = Instant::now();
        let mut soft_logged = false;

        while self.count_udp_pool_in_flight() > 0 {
            if !soft_logged && start.elapsed() >= UDP_POOL_DRAIN_SOFT_TIMEOUT {
                soft_logged = true;
                warn!(
                    in_flight = self.count_udp_pool_in_flight(),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "UDP pool shutdown drain is taking longer than expected"
                );
            }
            if start.elapsed() >= UDP_POOL_DRAIN_HARD_TIMEOUT {
                warn!(
                    in_flight = self.count_udp_pool_in_flight(),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "UDP pool shutdown drain timed out before all acks arrived"
                );
                break;
            }

            let count = unsafe {
                (self.dispatch.dequeue)(self.cq, results.as_mut_ptr(), MAX_RIO_RESULTS as u32)
            };

            if count == RIO_CORRUPT_CQ {
                break;
            }
            if count == 0 {
                thread::sleep(UDP_POOL_DRAIN_POLL_INTERVAL);
                continue;
            }

            for res in results.iter().take(count as usize) {
                let Some((user_data, completion_generation)) =
                    Self::decode_request_context(res.RequestContext)
                else {
                    continue;
                };
                if user_data == UDP_POOL_USER_DATA {
                    let _ = self.ack_udp_pool_completion(completion_generation);
                }
            }
        }
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

            for res in results.iter().take(count as usize) {
                let Some((user_data, completion_generation)) =
                    Self::decode_request_context(res.RequestContext)
                else {
                    continue;
                };

                if user_data == UDP_POOL_USER_DATA {
                    let Some((handle, slot_idx)) = self.udp_ctx_map.remove(&completion_generation)
                    else {
                        continue;
                    };

                    let mut should_resubmit = false;
                    let mut should_dispatch = false;
                    let mut should_rebalance = false;
                    if let Some(pool) = self.udp_recv_pools.get_mut(&handle)
                        && let Some(slot) = pool.slots.get_mut(slot_idx)
                    {
                        slot.in_flight = false;
                        let stopping = slot.stop_requested;
                        slot.stop_requested = false;

                        if !pool.shutting_down {
                            if res.Status == 0 && res.BytesTransferred > 0 {
                                if pool.queue.len() >= UDP_RECV_POOL_QUEUE_CAP {
                                    let _ = pool.queue.pop_front();
                                }
                                pool.queue.push_back(UdpRecvDatagram {
                                    data: slot.data[..(res.BytesTransferred as usize)].to_vec(),
                                    addr: *slot.addr,
                                    addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
                                });
                            }
                            should_resubmit = !stopping && slot_idx < pool.target_credits;
                            should_dispatch = true;
                            should_rebalance = true;
                        }
                    }

                    if should_resubmit {
                        let _ = self.submit_udp_pool_slot(handle, slot_idx);
                    }
                    if should_dispatch {
                        self.dispatch_udp_waiters_for_handle(handle, ops);
                    }
                    if should_rebalance {
                        self.trim_udp_pool_tail(handle);
                        let _ = self.rebalance_udp_pool(handle);
                    }
                    self.cleanup_shutdown_udp_pool_if_drained(handle);
                    continue;
                }

                if user_data < ops.local.len() {
                    let op = &mut ops.local[user_data];
                    let slot = &ops.shared.slots[user_data];
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

    pub fn try_submit_recv(
        &mut self,
        fd: IoFd,
        handle: HANDLE,
        buf: &mut FixedBuf,
        overlapped: *mut OVERLAPPED,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let (buffer_id, offset) = self.resolve_buffer_id(buf, registrar)?;

        // Now self.registered_bufs borrow has ended
        let rq = self.ensure_rq(handle, fd)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset,
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
        let (buffer_id, offset) = self.resolve_buffer_id(buf, registrar)?;

        let rq = self.ensure_rq(handle, fd)?;

        let rio_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: offset,
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
        let (buffer_id, data_offset) = self.resolve_buffer_id(buf, registrar)?;

        // Lazy register slab page
        self.ensure_slab_page_registration(page_idx, slab_resolver)?;

        // Values are now guaranteed to be present if ensure_slab_page_registration succeeded
        let (addr_buf_id, base_addr, slab_len) = self.slab_rio_pages[page_idx].unwrap();

        let rq = self.ensure_rq(handle, fd)?;

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
        let (buffer_id, data_offset) = self.resolve_buffer_id(buf, registrar)?;

        // Lazy register slab page
        self.ensure_slab_page_registration(page_idx, slab_resolver)?;

        // Values are now guaranteed to be present
        let (addr_buf_id, base_addr, slab_len) = self.slab_rio_pages[page_idx].unwrap();

        let rq = self.ensure_rq(handle, fd)?;

        let data_buf = RIO_BUF {
            BufferId: buffer_id,
            Offset: data_offset,
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
        // Explicit UDP pool shutdown protocol:
        // 1) forbid new submissions; 2) mark in-flight slots as stop-requested;
        // 3) drain CQ until all slot acknowledgements arrive; 4) release buffers/CQ.
        self.begin_udp_pool_shutdown();
        self.drain_udp_pool_shutdown_acks();
        self.udp_ctx_map.clear();

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
        for id in self.heap_rio_bufs.values().copied() {
            if id != RIO_INVALID_BUFFERID && deregistered.insert(id as usize) {
                unsafe { (self.dispatch.deregister_buffer)(id) };
            }
        }
        for (_handle, pool) in std::mem::take(&mut self.udp_recv_pools) {
            for slot in pool.slots {
                if slot.in_flight {
                    // Hard-timeout fallback: keep memory alive instead of freeing while kernel may still touch it.
                    std::mem::forget(slot);
                    continue;
                }
                if slot.data_buf_id != RIO_INVALID_BUFFERID
                    && deregistered.insert(slot.data_buf_id as usize)
                {
                    unsafe { (self.dispatch.deregister_buffer)(slot.data_buf_id) };
                }
                if slot.addr_buf_id != RIO_INVALID_BUFFERID
                    && deregistered.insert(slot.addr_buf_id as usize)
                {
                    unsafe { (self.dispatch.deregister_buffer)(slot.addr_buf_id) };
                }
            }
        }

        if self.cq != 0 {
            unsafe { (self.dispatch.close_cq)(self.cq) };
        }
    }
}
