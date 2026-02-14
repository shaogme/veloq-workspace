//! # IO Operation Abstraction Layer
//!
//! This module defines platform-agnostic operation structures and traits.
//! All types here are completely cross-platform with no conditional compilation.
//!
//! Platform-specific implementations reside in:
//! - `io/driver/uring/op.rs` for Linux io_uring
//! - `io/driver/iocp/op.rs` for Windows IOCP

use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use std::cell::RefCell;
use tracing::trace;
use veloq_buf::FixedBuf;

use crate::driver::slot::{SlotTable, STATE_COMPLETED, STATE_CONSUMED};
use crate::driver::{Driver, PlatformDriver};
use crate::RawHandle;
use crate::SockAddrStorage;

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
    /// Convert this operation into the platform driver's operation type.
    fn into_platform_op(self) -> D::Op;

    /// Convert from the platform driver's operation type back to this type.
    fn from_platform_op(op: D::Op) -> Self;
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
                let op_platform = data.into_platform_op();
                let table = driver.slot_table();

                if let Err((e, _op)) = driver.submit(user_data, op_platform) {
                    trace!("Submit failed: {}", e);
                    driver.cancel_op(user_data);
                    // Note: We proceeded with reservation but submit failed.
                    // The slot is now pending cancellation.
                    // We return a DetachedOp that monitors this slot.
                    // The cancel_op should eventually trigger completion with error.
                    DetachedOp {
                        table: Some(table),
                        index: user_data,
                        expected_gen: generation,
                        immediate_failure: None,
                        _phantom: std::marker::PhantomData,
                    }
                } else {
                    DetachedOp {
                        table: Some(table),
                        index: user_data,
                        expected_gen: generation,
                        immediate_failure: None,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
            Err(e) => {
                // Reservation failed (e.g. full).
                // Return DetachedOp with immediate failure.
                DetachedOp {
                    table: None, // No table needed
                    index: 0,    // Dummy
                    expected_gen: 0,
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
/// It holds a reference to the Shared Slot Table and polls the slot directly.
pub struct DetachedOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D>,
{
    table: Option<Arc<SlotTable<D::Op>>>,
    index: usize,
    expected_gen: u32,
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
            .table
            .as_ref()
            .expect("DetachedOp missing table but no immediate_failure");
        let slot = &table.slots[this.index];

        // 1. Generation check: Ensure slot hasn't been recycled for a new operation.
        let generation = slot.generation.load(Ordering::Acquire);
        if generation != this.expected_gen {
            return Poll::Ready(OpResult::Lost(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Op slot recycled (generation mismatch)",
            )));
        }

        // 2. Check for completion state
        let state = slot.state.load(Ordering::Acquire);
        if state == STATE_COMPLETED {
            // Completed. Extract result and op.
            let res = unsafe {
                (*slot.result.get())
                    .take()
                    .expect("Result missing in COMPLETED slot")
            };
            let op_platform = unsafe {
                (*slot.op.get())
                    .take()
                    .expect("Op missing in COMPLETED slot")
            };

            // Convert platform op back to user op
            let data = T::from_platform_op(op_platform);

            // Mark slot as CONSUMED so it can be reclaimed
            slot.state.store(STATE_CONSUMED, Ordering::Release);

            // NOTE: The Slot index remains "occupied" in the Registry until someone frees it.
            // In the detached model, we need a mechanism to recycle the index.
            // We push the index to the "remote free queue" which sits in the SlotTable.
            table.remote_free_queue.push(this.index);

            Poll::Ready(OpResult::Completed(res, data))
        } else {
            // 3. Register Waker
            slot.waker.register(cx.waker());

            // Double check state
            let state = slot.state.load(Ordering::Acquire);
            if state == STATE_COMPLETED {
                let res = unsafe { (*slot.result.get()).take().expect("Result missing") };
                let op_platform = unsafe { (*slot.op.get()).take().expect("Op missing") };
                let data = T::from_platform_op(op_platform);
                slot.state.store(STATE_CONSUMED, Ordering::Release);
                table.remote_free_queue.push(this.index);
                Poll::Ready(OpResult::Completed(res, data))
            } else {
                Poll::Pending
            }
        }
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
    driver: Rc<RefCell<PlatformDriver>>,
    user_data: usize,
}

impl<T: IntoPlatformOp<PlatformDriver> + 'static> LocalOp<T> {
    /// Create a new local operation future.
    pub fn new(data: T, driver: Rc<RefCell<PlatformDriver>>) -> Self {
        Self {
            state: State::Defined,
            data: Some(data),
            driver,
            user_data: 0,
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
            let driver_op = data.into_platform_op();

            // reserve_op now returns generation, but we ignore it for LocalOp
            // because LocalOp lifetime is tied to the driver via Rc/RefCell.
            let (user_data, _generation) = match driver.reserve_op() {
                Ok(v) => v,
                Err(e) => {
                    // Failed to reserve
                    return Poll::Ready(OpResult::Completed(
                        Err(e),
                        T::from_platform_op(driver_op),
                    ));
                }
            };
            op.user_data = user_data;

            // Submit to driver.
            if let Err((e, val)) = driver.submit(user_data, driver_op) {
                let data = T::from_platform_op(val);
                return Poll::Ready(OpResult::Completed(Err(e), data));
            }

            op.state = State::Submitted;
        }

        if let State::Submitted = op.state {
            let mut driver = op.driver.borrow_mut();

            match driver.poll_op(op.user_data, cx) {
                Poll::Ready((res, driver_op)) => {
                    op.state = State::Completed;
                    let data = T::from_platform_op(driver_op);
                    Poll::Ready(OpResult::Completed(res, data))
                }
                Poll::Pending => Poll::Pending,
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
// DetachedSubmitter
// ============================================================================

#[derive(Clone, Copy)]
pub struct DetachedSubmitter;

impl DetachedSubmitter {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self)
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

/// Receive data and source address (UDP).
pub struct RecvFrom {
    pub fd: IoFd,
    pub buf: FixedBuf,
    /// Source address (populated after completion).
    pub addr: Option<std::net::SocketAddr>,
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

#[cfg(windows)]
/// Receive data using Windows Registered I/O (RIO).
pub struct RioRecv {
    pub fd: IoFd,
    pub buf: FixedBuf,
}

#[cfg(windows)]
/// Send data using Windows Registered I/O (RIO).
pub struct RioSend {
    pub fd: IoFd,
    pub buf: FixedBuf,
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
