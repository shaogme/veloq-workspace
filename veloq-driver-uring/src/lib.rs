use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::Poll;

use libc::{sockaddr_in, sockaddr_in6, socklen_t};
use tracing::{debug, trace};

mod inner;
mod op;
mod submit;

pub use inner::{UringDriver, UringOpState};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct RawHandle {
    pub fd: i32,
}

impl From<i32> for RawHandle {
    fn from(fd: i32) -> Self {
        Self { fd }
    }
}

impl From<usize> for RawHandle {
    fn from(fd: usize) -> Self {
        Self { fd: fd as i32 }
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.fd as usize
    }
}
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub libc::sockaddr_storage);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { std::mem::zeroed() })
    }
}

pub type IoFd = veloq_driver_core::IoFd<RawHandle>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Interrupt,
    Polling(NonZeroU32),
}

#[derive(Debug, Clone)]
pub struct UringConfig {
    pub mode: IoMode,
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<UringConfig> for UringConfig {
    fn as_ref(&self) -> &UringConfig {
        self
    }
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            entries: NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
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

pub fn to_socket_addr(buf: &[u8]) -> std::io::Result<SocketAddr> {
    if buf.len() < std::mem::size_of::<libc::sa_family_t>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid address length",
        ));
    }
    let family = unsafe { *(buf.as_ptr() as *const libc::sa_family_t) } as i32;
    match family {
        libc::AF_INET => {
            if buf.len() < std::mem::size_of::<sockaddr_in>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin = unsafe { &*(buf.as_ptr() as *const sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if buf.len() < std::mem::size_of::<sockaddr_in6>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin6 = unsafe { &*(buf.as_ptr() as *const sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported address family",
        )),
    }
}

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, socklen_t) {
    let mut storage = SockAddrStorage::default();
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin_ptr = &mut storage.0 as *mut _ as *mut sockaddr_in;
            unsafe {
                (*sin_ptr).sin_family = libc::AF_INET as _;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<sockaddr_in>() as socklen_t
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage.0 as *mut _ as *mut sockaddr_in6;
            unsafe {
                (*sin6_ptr).sin6_family = libc::AF_INET6 as _;
                (*sin6_ptr).sin6_port = a.port().to_be();
                (*sin6_ptr).sin6_addr.s6_addr = a.ip().octets();
                (*sin6_ptr).sin6_flowinfo = a.flowinfo();
                (*sin6_ptr).sin6_scope_id = a.scope_id();
                std::mem::size_of::<sockaddr_in6>() as socklen_t
            }
        }
    };
    (storage, len)
}

use inner::UringWaker;
use op::UringOp;
use veloq_driver_core::driver::{
    Driver, Outcome, RemoteWaker, SharedCompletionQueue, SharedCompletionTable, SubmitBinder,
};
use veloq_driver_core::op_registry::{OpEntry, OpHandle};

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for UringDriver {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.registration_stats.chunk_register_attempts
    }
}

impl UringDriver {
    fn submit_sqe(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        {
            let slot = &self.ops.shared.slots[user_data];
            unsafe {
                *slot.op.get() = Some(op);
            }
        }

        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = inner::OpLifecycle::Pending;
                }
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                let slot = &self.ops.shared.slots[user_data];
                let op = unsafe { (*slot.op.get()).take().unwrap() };
                *op_in = Some(op);
                binder.err(e)
            }
        }
    }

    fn submit_timer(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        {
            let slot = &self.ops.shared.slots[user_data];
            unsafe {
                *slot.op.get() = Some(op);
            }
        }

        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = inner::OpLifecycle::Pending;
                }
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                let slot = &self.ops.shared.slots[user_data];
                let op = unsafe { (*slot.op.get()).take().unwrap() };
                *op_in = Some(op);
                binder.err(e)
            }
        }
    }
}

impl Driver for UringDriver {
    type Op = UringOp;
    type Handle = RawHandle;
    type Sidecar = ();

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
            Ok(OpHandle {
                index: id,
                generation,
            }) => {
                trace!(id, generation, "Reserved op slot");
                Ok((id, generation))
            }
            Err(_) => Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "OpRegistry full",
            )),
        }
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
        let op = op_in.take().expect("submit called with empty Option");
        let strategy = unsafe { op.vtable.as_ref().strategy };
        if strategy == op::SubmissionStrategy::BackgroundOnly {
            *op_in = Some(op);
            return binder.err(io::Error::new(
                io::ErrorKind::Unsupported,
                "background op cannot be submitted normally",
            ));
        }

        match strategy {
            op::SubmissionStrategy::BackgroundOnly => unreachable!(),
            op::SubmissionStrategy::SubmitSqe => self.submit_sqe(user_data, op, op_in, binder),
            op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer(user_data, op, op_in, binder)
            }
        }
    }

    fn submit_background(&mut self, mut op: Self::Op) -> io::Result<()> {
        let strategy = unsafe { op.vtable.as_ref().strategy };
        if strategy == op::SubmissionStrategy::BackgroundOnly {
            let sqe = unsafe {
                (op.vtable.as_ref().make_sqe)(&mut op, self).user_data(inner::BACKGROUND_USER_DATA)
            };

            if !self.push_entry(sqe) {
                return Err(io::Error::other("sq full"));
            }
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "background op only supports BackgroundOnly strategy",
            ))
        }
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()
    }

    fn wait(&mut self) -> io::Result<()> {
        UringDriver::wait(self)?;
        Ok(())
    }

    fn process_completions(&mut self) {
        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
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
        UringDriver::wait(self)?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        UringDriver::register_chunk(self, id, ptr, len)
    }

    fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
        let fds: Vec<i32> = files.iter().map(|h| h.fd).collect();
        self.ring.submitter().register_files(&fds)?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            fixed_fds.push(IoFd::Fixed(i as u32));
        }
        Ok(fixed_fds)
    }

    fn unregister_files(&mut self, _files: Vec<IoFd>) -> io::Result<()> {
        self.ring.submitter().unregister_files()
    }

    fn wake(&mut self) -> io::Result<()> {
        let buf = 1u64.to_ne_bytes();
        let ret = unsafe { libc::write(self.waker_fd.fd, buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    fn inner_handle(&self) -> RawHandle {
        use std::os::unix::io::AsRawFd;
        RawHandle {
            fd: self.ring.as_raw_fd(),
        }
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        Arc::new(UringWaker {
            fd: self.waker_fd.clone(),
            is_waked: self.is_waked.clone(),
        })
    }

    fn driver_id(&self) -> usize {
        self.waker_fd.fd as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}
