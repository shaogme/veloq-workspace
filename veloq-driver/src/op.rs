//! # IO Operation Abstraction Layer
//!
//! This module defines platform-agnostic operation structures and traits.
//! All types here are completely cross-platform with no conditional compilation.
//!
//! Platform-specific implementations reside in:
//! - `io/driver/uring/op.rs` for Linux io_uring
//! - `io/driver/iocp/op.rs` for Windows IOCP

use std::rc::Rc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use std::cell::RefCell;
use tracing::trace;
use veloq_buf::FixedBuf;

use crate::RawHandle;
use crate::SockAddrStorage;
use crate::driver::{
    Driver, PlatformDriver, SharedCompletionTable, SubmitBinder, encode_completion_token,
    event_res_to_io,
};

/// Represents the source of an IO operation: either a raw handle or a registered index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoFd {
    /// A raw system handle (fd on Unix, HANDLE on Windows).
    Raw(RawHandle),
    /// A registered index for pre-registered file descriptors.
    Fixed(u32),
}

impl IoFd {
    /// Returns the raw handle if this is a Raw variant.
    pub fn raw(&self) -> Option<RawHandle> {
        match self {
            Self::Raw(fd) => Some(*fd),
            Self::Fixed(_) => None,
        }
    }
}

impl From<RawHandle> for IoFd {
    fn from(handle: RawHandle) -> Self {
        Self::Raw(handle)
    }
}

// ============================================================================
// OpResult
// ============================================================================

/// The result of an IO operation.
///
/// Since operations execute asynchronously and are detached from the submitter's lifetime,
/// it is possible (though rare) for the operation slot to be recycled if the `Future`
/// is polled after the driver has reclaimed the slot (Generation Mismatch).
/// In such cases, the ownership of the resource `T` is lost.
#[derive(Debug)]
pub enum OpResult<T> {
    /// Operation completed (successfully or with IO error).
    /// Returns the result of the operation and the original resource.
    Completed(std::io::Result<usize>, T),
    /// Operation failed because the submitter/driver slot was recycled (Generation Mismatch).
    /// The resource `T` is lost (polled too late, driver reset slot).
    Lost(std::io::Error),
}

impl<T> OpResult<T> {
    /// Unwraps the result, assuming the operation completed (panics if Lost).
    pub fn unwrap(self) -> (usize, T) {
        match self {
            OpResult::Completed(Ok(res), data) => (res, data),
            OpResult::Completed(Err(e), _) => panic!("OpResult::Completed(Err({}))", e),
            OpResult::Lost(e) => panic!("OpResult::Lost({})", e),
        }
    }

    /// Returns the result and the resource implementation (if available).
    pub fn into_inner(self) -> (std::io::Result<usize>, Option<T>) {
        match self {
            OpResult::Completed(res, data) => (res, Some(data)),
            OpResult::Lost(err) => (Err(err), None),
        }
    }
}

// ============================================================================
// Core Traits
// ============================================================================

/// Trait for managing the lifecycle of an operation.
/// Handles pre-allocation, construction, and output conversion.
pub trait OpLifecycle: Sized {
    /// Type for any pre-allocated resources needed before creating the op.
    type PreAlloc;
    /// The final output type after the operation completes.
    type Output;

    /// Pre-allocate any resources needed (e.g., accept socket on Windows).
    fn pre_alloc(fd: RawHandle) -> std::io::Result<Self::PreAlloc>;

    /// Construct the operation from a raw handle and pre-allocated resources.
    fn into_op(fd: RawHandle, pre: Self::PreAlloc) -> Self;

    /// Convert the completed operation result to the final output type.
    fn into_output(self, res: std::io::Result<usize>) -> std::io::Result<Self::Output>;

    /// Helper: Pre-allocate and construct the operation in one step.
    fn prepare_op(fd: RawHandle) -> std::io::Result<Self> {
        let pre = Self::pre_alloc(fd)?;
        Ok(Self::into_op(fd, pre))
    }
}

/// Trait to convert a user-facing operation to a platform-specific driver operation.
pub trait IntoPlatformOp<D: Driver>: Sized + std::marker::Send {
    /// User payload detached from kernel op.
    type UserPayload: std::marker::Send + 'static;

    /// Split into kernel-facing op and user payload.
    fn into_kernel_and_payload(self) -> (D::Op, Self::UserPayload);

    /// Rebuild the user operation from payload.
    fn from_user_payload(payload: Self::UserPayload) -> Self;

    /// Compatibility helper for transitional callsites.
    #[inline]
    fn from_kernel_and_payload(op: D::Op, payload: Self::UserPayload) -> Self {
        drop(op);
        Self::from_user_payload(payload)
    }

    /// Compatibility helper for legacy callsites.
    #[inline]
    fn into_platform_op(self) -> D::Op
    where
        Self::UserPayload: Default,
    {
        self.into_kernel_and_payload().0
    }

    /// Compatibility helper for legacy callsites.
    #[inline]
    fn from_platform_op(op: D::Op) -> Self
    where
        Self::UserPayload: Default,
    {
        drop(op);
        Self::from_user_payload(Default::default())
    }
}

// ============================================================================
// Op (Generic Data Carrier)
// ============================================================================

/// A generic wrapper for IO operation data.
///
/// This struct represents the "intent" of an operation, holding only the data
/// required to perform the IO (e.g., buffers, file descriptors, flags).
/// It is decoupled from the execution backend (Driver).
pub struct Op<T> {
    pub data: T,
}

impl<T> Op<T> {
    /// Create a new operation intent with the given data.
    pub fn new(data: T) -> Self {
        Self { data }
    }

    /// Submit this operation manually to a specific driver instance.
    /// The operation is submitted synchronously, but completion is awaited asynchronously via the returned future.
    pub fn submit_detached<D>(self, driver: &mut D) -> DetachedOp<T, D>
    where
        T: IntoPlatformOp<D> + std::marker::Send + 'static,
        D: Driver,
    {
        let data = self.data;
        trace!("Submitting detached op");

        // Try reserve first
        match driver.reserve_op() {
            Ok((user_data, generation)) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                let mut payload = Some(payload);
                let mut op_platform = Some(kernel_op);
                let token = encode_completion_token(user_data, generation);
                let completion_table = driver.completion_table();

                if let Err(e) = driver
                    .submit(user_data, &mut op_platform, SubmitBinder::new())
                    .into_inner()
                {
                    trace!("Submit failed: {}", e);
                    // Submit failed synchronously.
                    // If the platform op is returned, propagate immediate failure with payload.
                    // Otherwise, fall back to slot-monitoring path and let generation check resolve.
                    if let Some(op) = op_platform.take() {
                        let payload = payload
                            .take()
                            .expect("Payload missing while recovering submit failure");
                        drop(op);
                        DetachedOp {
                            completion_table: None,
                            token: 0,
                            payload: None,
                            immediate_failure: Some((e, T::from_user_payload(payload))),
                            _phantom: std::marker::PhantomData,
                        }
                    } else {
                        DetachedOp {
                            completion_table: Some(completion_table),
                            token,
                            payload,
                            immediate_failure: None,
                            _phantom: std::marker::PhantomData,
                        }
                    }
                } else {
                    DetachedOp {
                        completion_table: Some(completion_table),
                        token,
                        payload,
                        immediate_failure: None,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
            Err(e) => {
                // Reservation failed (e.g. full).
                // Return DetachedOp with immediate failure.
                DetachedOp {
                    completion_table: None,
                    token: 0,
                    payload: None,
                    immediate_failure: Some((e, data)),
                    _phantom: std::marker::PhantomData,
                }
            }
        }
    }

    /// Submit this operation to a local IO driver.
    /// Returns a `LocalOp` future that resolves when the operation completes.
    pub fn submit_local(self, driver: Rc<RefCell<PlatformDriver>>) -> LocalOp<T>
    where
        T: IntoPlatformOp<PlatformDriver> + 'static,
    {
        LocalOp::new(self.data, driver)
    }
}

// ============================================================================
// DetachedOp (Future Implementation for Shared/Send Ops)
// ============================================================================

/// A Future representing a detached operation.
/// It polls a shared completion event queue by token.
pub struct DetachedOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D>,
{
    completion_table: Option<SharedCompletionTable>,
    token: u64,
    payload: Option<T::UserPayload>,
    immediate_failure: Option<(std::io::Error, T)>,
    _phantom: std::marker::PhantomData<T>,
}

// DetachedOp is Send/Sync if the Op data is Send and the Driver Op is Send (implied by SlotTable<Op> bound).
unsafe impl<T: IntoPlatformOp<D> + std::marker::Send, D: Driver> std::marker::Send
    for DetachedOp<T, D>
{
}
unsafe impl<T: IntoPlatformOp<D> + std::marker::Send, D: Driver> std::marker::Sync
    for DetachedOp<T, D>
{
}

impl<T, D> Future for DetachedOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D>,
{
    type Output = OpResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((e, data)) = this.immediate_failure.take() {
            return Poll::Ready(OpResult::Completed(Err(e), data));
        }

        let table = this
            .completion_table
            .as_ref()
            .expect("DetachedOp missing completion_table but no immediate_failure");
        if let Some(event) = table.try_take(this.token) {
            let payload = this
                .payload
                .take()
                .expect("DetachedOp payload missing on completion");
            let data = T::from_user_payload(payload);
            return Poll::Ready(OpResult::Completed(event_res_to_io(event.res), data));
        }

        if let Some(table) = this.completion_table.as_ref() {
            table.register_waker(this.token, cx.waker());
        }

        Poll::Pending
    }
}

// ============================================================================
// LocalOp (Future Implementation)
// ============================================================================

enum State {
    Defined,
    Submitted,
    Completed,
}

/// A Future wrapper for asynchronous IO operations executed locally.
pub struct LocalOp<T: IntoPlatformOp<PlatformDriver> + 'static> {
    state: State,
    data: Option<T>,
    payload: Option<T::UserPayload>,
    driver: Rc<RefCell<PlatformDriver>>,
    user_data: usize,
    token: u64,
}

impl<T: IntoPlatformOp<PlatformDriver> + 'static> LocalOp<T> {
    /// Create a new local operation future.
    pub fn new(data: T, driver: Rc<RefCell<PlatformDriver>>) -> Self {
        Self {
            state: State::Defined,
            data: Some(data),
            payload: None,
            driver,
            user_data: 0,
            token: 0,
        }
    }
}

impl<T: IntoPlatformOp<PlatformDriver> + 'static> Future for LocalOp<T> {
    type Output = OpResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = unsafe { self.get_unchecked_mut() };

        if let State::Defined = op.state {
            let mut driver = op.driver.borrow_mut();

            // Submit to driver
            let data = op.data.take().expect("Op started without data");
            let (driver_op, payload) = data.into_kernel_and_payload();
            op.payload = Some(payload);

            // reserve_op now returns generation, but we ignore it for LocalOp
            // because LocalOp lifetime is tied to the driver via Rc/RefCell.
            let (user_data, generation) = match driver.reserve_op() {
                Ok(v) => v,
                Err(e) => {
                    // Failed to reserve
                    let payload = op
                        .payload
                        .take()
                        .expect("Payload missing on reserve failure");
                    drop(driver_op);
                    return Poll::Ready(OpResult::Completed(Err(e), T::from_user_payload(payload)));
                }
            };
            op.user_data = user_data;
            op.token = encode_completion_token(user_data, generation);

            // Submit to driver.
            let mut driver_op_opt = Some(driver_op);
            if let Err(e) = driver
                .submit(user_data, &mut driver_op_opt, SubmitBinder::new())
                .into_inner()
            {
                if let Some(val) = driver_op_opt.take() {
                    drop(val);
                }
                let payload = op
                    .payload
                    .take()
                    .expect("Payload missing while recovering submit failure");
                let data = T::from_user_payload(payload);
                return Poll::Ready(OpResult::Completed(Err(e), data));
            }

            op.state = State::Submitted;
        }

        if let State::Submitted = op.state {
            let mut driver = op.driver.borrow_mut();
            if let Some(event) = driver.try_take_completion(op.token) {
                op.state = State::Completed;
                let payload = op.payload.take().expect("Payload missing on completion");
                let data = T::from_user_payload(payload);
                Poll::Ready(OpResult::Completed(event_res_to_io(event.res), data))
            } else {
                driver.register_completion_waker(op.token, cx.waker());
                Poll::Pending
            }
        } else {
            panic!("Polled after completion");
        }
    }
}

impl<T: IntoPlatformOp<PlatformDriver> + 'static> Drop for LocalOp<T> {
    fn drop(&mut self) {
        if let State::Submitted = self.state {
            // LocalOp being dropped while submitted means we must cancel it.
            self.driver.borrow_mut().cancel_op(self.user_data);
        }
    }
}

// ============================================================================
// OpSubmitter Trait
// ============================================================================

pub trait OpSubmitter: Clone + std::marker::Send + Sync + 'static {
    type Future<T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static>: Future<
        Output = OpResult<T>,
    >;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static;

    fn from_current_context() -> std::io::Result<Self>;
}

// ============================================================================
// LocalSubmitter
// ============================================================================

#[derive(Clone, Copy)]
pub struct LocalSubmitter;

impl OpSubmitter for LocalSubmitter {
    type Future<T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static> = LocalOp<T>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> LocalOp<T>
    where
        T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static,
    {
        trace!("Submitting local op");
        op.submit_local(driver)
    }

    fn from_current_context() -> std::io::Result<Self> {
        Ok(Self)
    }
}

// ============================================================================
// DetachedSubmitter
// ============================================================================

#[derive(Clone, Copy)]
pub struct DetachedSubmitter;

impl DetachedSubmitter {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self)
    }
}

impl OpSubmitter for DetachedSubmitter {
    type Future<T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static> =
        DetachedOp<T, PlatformDriver>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<PlatformDriver> + std::marker::Send + 'static,
    {
        op.submit_detached(&mut *driver.borrow_mut())
    }

    fn from_current_context() -> std::io::Result<Self> {
        Self::new()
    }
}

// ============================================================================
// Cross-Platform Operation Structures
// ============================================================================

/// Read from a file descriptor at a specific offset using a fixed buffer.
pub struct ReadFixed {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub offset: u64,
}

/// Write to a file descriptor at a specific offset using a fixed buffer.
pub struct WriteFixed {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub offset: u64,
}

/// Receive data from a socket into a fixed buffer.
pub struct Recv {
    pub fd: IoFd,
    pub buf: FixedBuf,
}

/// Send data from a fixed buffer to a socket.
pub struct Send {
    pub fd: IoFd,
    pub buf: FixedBuf,
}

/// Connect a socket to a remote address.
pub struct Connect {
    pub fd: IoFd,
    /// Raw address bytes (sockaddr representation), boxed to reduce struct size.
    pub addr: SockAddrStorage,
    pub addr_len: u32,
}

/// Open a file.
/// Path representation is platform-agnostic (raw bytes).
#[derive(Debug)]
pub struct Open {
    /// Path stored in a fixed buffer.
    /// - Unix: UTF-8 encoded, null-terminated.
    /// - Windows: UTF-16 encoded, null-terminated (stored as bytes).
    pub path: FixedBuf,
    pub flags: i32,
    pub mode: u32,
}

/// Close a file descriptor or handle.
pub struct Close {
    pub fd: IoFd,
}

/// Flush file buffers to disk.
pub struct Fsync {
    pub fd: IoFd,
    /// If true, only sync data (not metadata).
    pub datasync: bool,
}

/// Timeout operation (platform-specific timing).
pub struct Timeout {
    pub duration: std::time::Duration,
}

/// Wake up the event loop.
pub struct Wakeup {
    pub fd: IoFd,
}

/// Accept a new connection on a listening socket.
/// Result includes the new socket handle and remote address.
pub struct Accept {
    pub fd: IoFd,
    /// Buffer for storing the remote address.
    /// On Windows, we parse the result from the AcceptEx output buffer, so we don't need this storage.
    #[cfg(unix)]
    pub addr: SockAddrStorage,
    /// Length of the address buffer.
    #[cfg(unix)]
    pub addr_len: u32,
    /// Parsed remote address (populated after completion).
    pub remote_addr: Option<std::net::SocketAddr>,
    /// Pre-allocated accept socket (Windows only, required for AcceptEx).
    #[cfg(windows)]
    pub accept_socket: RawHandle,
}

/// Send data to a specific address (UDP).
pub struct SendTo {
    pub fd: IoFd,
    pub buf: FixedBuf,
    /// Target address.
    pub addr: std::net::SocketAddr,
}

/// Sync file range.
pub struct SyncFileRange {
    pub fd: IoFd,
    pub offset: u64,
    pub nbytes: u64,
    pub flags: u32,
}

/// Pre-allocate file space.
pub struct Fallocate {
    pub fd: IoFd,
    pub mode: i32,
    pub offset: u64,
    pub len: u64,
}

/// Receive data as UDP datagram stream.
pub struct UdpRecvStream {
    pub fd: IoFd,
    /// Unix io_uring path uses this provided buffer; Windows can leave it as None.
    pub buf: Option<FixedBuf>,
    /// Unix io_uring path: source address parsed from recvmsg.
    pub addr: Option<std::net::SocketAddr>,
    /// Windows RIO path: resulting datagram, populated on completion.
    pub result: Option<UdpRecvDatagram>,
}

/// A received UDP datagram.
pub struct UdpRecvDatagram {
    pub buf: FixedBuf,
    pub addr: std::net::SocketAddr,
}

/// Provide a buffer to the driver's internal RIO UDP pool.
pub struct UdpRefill {
    pub fd: IoFd,
    pub buf: Option<FixedBuf>,
}

// ============================================================================
// OpLifecycle Implementations
// ============================================================================

impl OpLifecycle for Accept {
    /// On Windows: pre-created accept socket handle
    /// On Unix: unit (no pre-allocation needed)
    #[cfg(unix)]
    type PreAlloc = ();
    #[cfg(windows)]
    type PreAlloc = RawHandle;

    type Output = (RawHandle, std::net::SocketAddr);

    #[cfg(windows)]
    fn pre_alloc(fd: RawHandle) -> std::io::Result<Self::PreAlloc> {
        // On Windows, we need to pre-create a socket for AcceptEx
        use crate::Socket;

        // Determine the address family of the listener to create a matching accept socket.
        // We temporarily wrap the raw handle to use helper methods.
        let listener = unsafe { Socket::from_raw(RawHandle::from(fd.handle)) };
        let addr_res = listener.local_addr();
        // IMPORTANT: Release ownership back to raw handle so the listener isn't closed when dropped
        let _ = listener.into_raw();

        let addr = addr_res?;

        let socket = if addr.is_ipv4() {
            Socket::new_tcp_v4()?
        } else {
            Socket::new_tcp_v6()?
        };

        Ok(socket.into_raw())
    }

    #[cfg(unix)]
    fn pre_alloc(_fd: RawHandle) -> std::io::Result<Self::PreAlloc> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn into_op(fd: RawHandle, pre: Self::PreAlloc) -> Self {
        // Use stack/inline storage
        #[cfg(unix)]
        let addr_len = std::mem::size_of::<SockAddrStorage>() as u32;

        #[cfg(unix)]
        {
            Self {
                fd: IoFd::Raw(fd),
                addr: unsafe { std::mem::zeroed() },
                addr_len,
                remote_addr: None,
            }
        }
        #[cfg(windows)]
        {
            Self {
                fd: IoFd::Raw(fd),
                remote_addr: None,
                accept_socket: pre,
            }
        }
    }

    fn into_output(self, res: std::io::Result<usize>) -> std::io::Result<Self::Output> {
        #[cfg(unix)]
        {
            let fd = res?.into();
            use crate::to_socket_addr;
            let addr = if let Some(a) = self.remote_addr {
                a
            } else {
                unsafe {
                    let s = std::slice::from_raw_parts(
                        &self.addr as *const _ as *const u8,
                        self.addr_len as usize,
                    );
                    to_socket_addr(s).unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap())
                }
            };
            Ok((fd, addr))
        }
        #[cfg(windows)]
        {
            res?;
            // On Windows, the accept_socket was pre-allocated and is the new connection
            // Helper to provide a default address if parsing failed or was irrelevant, preventing panic
            let addr = self
                .remote_addr
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
            Ok((self.accept_socket, addr))
        }
    }
}
