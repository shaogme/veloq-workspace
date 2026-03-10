mod error;
mod ext;
mod inner;
mod op;
mod rio;
mod submit;
#[cfg(test)]
mod tests;

pub use inner::{CloseMode, IocpDriver, IocpOpState, OpLifecycle};
use op::IocpOp;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::task::Poll;
use submit::SubmissionResult;
use tracing::{debug, trace};
use veloq_driver_core::driver::{
    CompletionSidecar, Driver, Outcome, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
    SubmitBinder,
};
use veloq_driver_core::op_registry::OpEntry;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP, IPPROTO_UDP, SOCK_DGRAM, SOCK_STREAM, SOCKADDR,
    SOCKADDR_IN, SOCKADDR_IN6, WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO, WSADATA, WSASocketW,
    WSAStartup, bind, closesocket, getsockname, listen,
};
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferRegistrationMode {
    #[default]
    Strict,
    Compatible,
}

impl BufferRegistrationMode {
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

#[derive(Debug, Clone)]
pub struct IocpConfig {
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<IocpConfig> for IocpConfig {
    fn as_ref(&self) -> &IocpConfig {
        self
    }
}

impl Default for IocpConfig {
    fn default() -> Self {
        Self {
            entries: NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl IocpConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct RawHandle {
    pub handle: HANDLE,
}

unsafe impl Send for RawHandle {}
unsafe impl Sync for RawHandle {}

impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        Self {
            handle: handle as HANDLE,
        }
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.handle as usize
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { std::mem::zeroed() })
    }
}

pub type IoFd = veloq_driver_core::IoFd<RawHandle>;

pub struct Socket {
    handle: RawHandle,
}

impl Socket {
    fn new(af: u16, ty: i32, protocol: i32) -> std::io::Result<Self> {
        let s = unsafe {
            WSASocketW(
                af as i32,
                ty,
                protocol,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
            )
        };
        if s == INVALID_SOCKET {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { handle: s.into() })
    }

    pub fn new_tcp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_STREAM, IPPROTO_TCP)
    }

    pub fn new_tcp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_STREAM, IPPROTO_TCP)
    }

    pub fn new_udp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    }

    pub fn new_udp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)
    }

    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        let (raw_addr, raw_addr_len) = socket_addr_trans(addr);
        let ret = unsafe {
            bind(
                self.handle.into(),
                raw_addr.as_ptr() as *const SOCKADDR,
                raw_addr_len,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> std::io::Result<()> {
        let ret = unsafe { listen(self.handle.into(), backlog) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn into_raw(self) -> RawHandle {
        let h = self.handle;
        std::mem::forget(self);
        h
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        let mut buf = [0u8; 128];
        let mut len = 128_i32;
        let ret = unsafe {
            getsockname(
                self.handle.into(),
                buf.as_mut_ptr() as *mut SOCKADDR,
                &mut len,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        to_socket_addr(&buf[..len as usize])
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { closesocket(self.handle.into()) };
    }
}

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static INIT_WINSOCK: unsafe extern "C" fn() = {
    unsafe extern "C" fn init() {
        unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            let _ = WSAStartup(0x0202, &mut data);
        }
    }
    init
};

#[inline]
fn slot_overlapped_ptr(
    slot: &veloq_driver_core::slot::SlotEntry<IocpOp, op::OverlappedEntry>,
) -> *mut windows_sys::Win32::System::IO::OVERLAPPED {
    unsafe { &mut (*slot.sidecar.get()).inner as *mut _ }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for IocpDriver {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio_state
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}

impl Driver for IocpDriver {
    type Op = IocpOp;
    type Handle = RawHandle;
    type Sidecar = op::OverlappedEntry;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        // OpRegistry::alloc handles internal vectors and free list management autonomously.
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::new())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "OpRegistry is full",
                ));
            }
        };
        trace!(user_data, generation, "Reserved op slot");
        Ok((user_data, generation))
    }

    fn slot_table(
        &self,
    ) -> std::sync::Arc<veloq_driver_core::slot::SlotTable<Self::Op, Self::Sidecar>> {
        self.ops.shared.clone()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        if self.shutting_down {
            return binder.err(io::Error::from_raw_os_error(
                windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
            ));
        }
        trace!(user_data, "Submitting op");

        let slots_per_page = self.ops.local.len();
        // On Windows, the slab is currently a single contiguous block (page 0).
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).unwrap();
        let slab_resolver = move |idx| {
            if idx == 0 {
                Some((slab_ptr, slab_len))
            } else {
                None
            }
        };
        let mut deferred_event: Option<CompletionSidecar> = None;

        // Scope for initial submission
        {
            let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
                Some(pair) => pair,
                None => panic!("Op not found"),
            };

            let op = op_in.take().expect("submit called with empty Option");
            unsafe { *slot.op.get() = Some(op) };

            let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
            op_ref.header.user_data = user_data;
            let generation = slot.generation.load(Ordering::Acquire);
            op_ref.header.generation = generation;
            op_entry.platform_data.generation = generation;

            // Use the overlapped pointer from the slot.
            // This is safe because:
            // 1. The Slot is pinned in memory (part of Arc<SlotTable>).
            // 2. OverlappedEntry is #[repr(C)], so we can recover the user_data from the pointer.
            unsafe {
                let sidecar = &mut *slot.sidecar.get();
                sidecar.user_data = user_data;
                sidecar.generation = generation;
                sidecar.blocking_result = None;
            }
            let overlapped_ptr = slot_overlapped_ptr(slot);

            let mut ctx = crate::op::SubmitContext {
                port: self.port.handle,
                overlapped: overlapped_ptr,
                ext: &self.extensions,
                registered_files: &self.registered_files,
                registrar: self.registrar.as_ref(),
                rio: &mut self.rio_state,
                slots_per_page,
                slab_resolver: &slab_resolver,
            };

            let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };
            let is_rio_pool_waiting = unsafe {
                std::ptr::eq(
                    op_ref.vtable.as_ref().submit as *const (),
                    crate::submit::submit_udp_recv_stream as *const (),
                )
            };

            match result {
                Ok(SubmissionResult::Pending) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = is_rio_pool_waiting;
                }
                Ok(SubmissionResult::PostToQueue) => {
                    let posted = unsafe {
                        PostQueuedCompletionStatus(
                            self.port.handle,
                            0,
                            user_data,
                            std::ptr::null_mut(),
                        )
                    };
                    if posted == 0 {
                        let op = unsafe { (*slot.op.get()).take().unwrap() };
                        *op_in = Some(op);
                        self.ops.remove(user_data);
                        return binder.err(io::Error::last_os_error());
                    } else {
                        op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                        op_entry.platform_data.rio_pool_waiting = false;
                    }
                }
                Ok(SubmissionResult::Offload(task)) => {
                    use veloq_blocking::get_blocking_pool;
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = false;
                    if get_blocking_pool().execute(task).is_err() {
                        let err = io::Error::other("Thread pool overloaded");
                        unsafe {
                            *slot.result.get() =
                                Some(Err(io::Error::new(err.kind(), err.to_string())));
                        }
                        op_entry.platform_data.lifecycle = OpLifecycle::Completed;
                        let generation = slot.generation.load(Ordering::Acquire);
                        let _ = unsafe { (*slot.op.get()).take() };
                        let payload = unsafe { (*slot.payload.get()).take() };
                        let detail = unsafe { (*slot.result.get()).take() };
                        deferred_event = Some(CompletionSidecar {
                            user_data,
                            generation,
                            res: -err.raw_os_error().unwrap_or(1).abs(),
                            flags: 0,
                            payload,
                            detail,
                        });
                    }
                }
                Ok(SubmissionResult::Timer(duration)) => {
                    let timeout = self.wheel.insert(user_data, duration);
                    op_entry.platform_data.timer_id = Some(timeout);
                    op_entry.platform_data.timer_deadline =
                        Some(std::time::Instant::now() + duration);
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = false;
                }
                Err(e) => {
                    let op = unsafe { (*slot.op.get()).take().unwrap() };
                    *op_in = Some(op);
                    self.ops.remove(user_data);
                    return binder.err(e);
                }
            }
        } // End of submission scope

        if let Some(deferred) = deferred_event {
            let user_data = deferred.user_data;
            self.push_completion_event(deferred);
            self.ops.remove(user_data);
        }
        binder.ok(Poll::Ready(()))
    }

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::from_raw_os_error(
                windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
            ));
        }
        let (user_data, _) = self.reserve_op()?;
        let mut submit_error: Option<io::Error> = None;

        let slots_per_page = self.ops.local.len();
        // Pre-fetch slab info to avoid borrow conflicts
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).unwrap();
        let slab_resolver = move |idx| {
            if idx == 0 {
                Some((slab_ptr, slab_len))
            } else {
                None
            }
        };

        let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
            Some(pair) => pair,
            None => panic!("Op not found after reserve"),
        };

        unsafe { *slot.op.get() = Some(op) };

        let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
        op_ref.header.user_data = user_data;
        let generation = slot.generation.load(Ordering::Acquire);
        op_ref.header.generation = generation;
        op_entry.platform_data.generation = generation;

        op_entry.platform_data.is_background = true;
        unsafe {
            let sidecar = &mut *slot.sidecar.get();
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
        }
        let overlapped_ptr = slot_overlapped_ptr(slot);

        let mut ctx = crate::op::SubmitContext {
            port: self.port.handle,
            overlapped: overlapped_ptr,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            registrar: self.registrar.as_ref(),
            rio: &mut self.rio_state,
            slots_per_page,
            slab_resolver: &slab_resolver,
        };

        let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };

        match result {
            Ok(SubmissionResult::Offload(task)) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                use veloq_blocking::get_blocking_pool;
                if get_blocking_pool().execute(task).is_err() {
                    let _ = std::mem::take(&mut op_entry.platform_data);
                    self.ops.shared.push_free(user_data);
                    return Err(io::Error::other("Thread pool overloaded"));
                }
            }
            Ok(_) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
            }
            Err(e) => {
                debug!(error = ?e, user_data, "Background submit failed");
                let _ = unsafe { (*slot.op.get()).take() };
                submit_error = Some(e);
            }
        }

        if let Some(e) = submit_error {
            self.ops.remove(user_data);
            return Err(e);
        }
        Ok(())
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        self.get_completion(u32::MAX)
    }

    fn process_completions(&mut self) {
        let _ = self.get_completion(0);
    }

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable {
        self.completion_table.clone()
    }

    fn wait_and_drain_completions(
        &mut self,
        out: &mut Vec<veloq_driver_core::driver::CompletionEvent>,
    ) -> io::Result<usize> {
        self.get_completion(u32::MAX)?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        IocpDriver::register_chunk(self, id, ptr, len)
    }

    fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn wake(&mut self) -> io::Result<()> {
        IocpDriver::wake(self)
    }

    fn inner_handle(&self) -> RawHandle {
        RawHandle {
            handle: self.port.handle as _,
        }
    }

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        IocpDriver::create_waker(self)
    }

    fn driver_id(&self) -> usize {
        self.port.handle as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}

pub fn to_socket_addr(buf: &[u8]) -> std::io::Result<SocketAddr> {
    if buf.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid address length",
        ));
    }
    let family = unsafe { *(buf.as_ptr() as *const u16) };
    match family {
        AF_INET => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN) };
            let s_addr = unsafe { sin.sin_addr.S_un.S_addr };
            let ip = Ipv4Addr::from(u32::from_be(s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN6>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin6 = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN6) };
            let addr_bytes = unsafe { sin6.sin6_addr.u.Byte };
            let ip = Ipv6Addr::from(addr_bytes);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported address family",
        )),
    }
}

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, i32) {
    let mut storage = SockAddrStorage::default();
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin_ptr = &mut storage.0 as *mut _ as *mut SOCKADDR_IN;
            unsafe {
                (*sin_ptr).sin_family = AF_INET;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<SOCKADDR_IN>() as i32
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage.0 as *mut _ as *mut SOCKADDR_IN6;
            unsafe {
                (*sin6_ptr).sin6_family = AF_INET6;
                (*sin6_ptr).sin6_port = a.port().to_be();
                (*sin6_ptr).sin6_addr = std::mem::transmute::<
                    [u8; 16],
                    windows_sys::Win32::Networking::WinSock::IN6_ADDR,
                >(a.ip().octets());
                (*sin6_ptr).sin6_flowinfo = a.flowinfo();
                (*sin6_ptr).Anonymous.sin6_scope_id = a.scope_id();
                std::mem::size_of::<SOCKADDR_IN6>() as i32
            }
        }
    };
    (storage, len)
}

fn socket_addr_trans(addr: SocketAddr) -> (Vec<u8>, i32) {
    match addr {
        SocketAddr::V4(a) => {
            let mut sin: SOCKADDR_IN = unsafe { std::mem::zeroed() };
            sin.sin_family = AF_INET;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());

            let ptr = &sin as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(a) => {
            let mut sin6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = AF_INET6;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr = unsafe {
                std::mem::transmute::<[u8; 16], windows_sys::Win32::Networking::WinSock::IN6_ADDR>(
                    a.ip().octets(),
                )
            };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.Anonymous.sin6_scope_id = a.scope_id();

            let ptr = &sin6 as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN6>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN6>() as i32)
        }
    }
}
