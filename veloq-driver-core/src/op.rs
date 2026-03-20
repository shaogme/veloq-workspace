//! # IO Operation Abstraction Layer
//!
//! This module defines platform-agnostic operation structures and traits.
//! All types here are completely cross-platform with no conditional compilation.
//!
//! Platform-specific implementations reside in:
//! - `io/driver/uring/op.rs` for Linux io_uring
//! - `io/driver/iocp/op.rs` for Windows IOCP

use std::rc::Rc;
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use std::cell::RefCell;
use tracing::trace;
use veloq_buf::FixedBuf;

use crate::driver::{
    CompletionRecord, Driver, PlatformOp, PollRecordResult, RemoteWaker, SharedCompletionTable,
    SubmitBinder, SubmitStatus, encode_completion_token, event_res_to_io,
};
use crate::slot::DetachedCancelTable;
use crate::{Handle, IoFd, SockAddr};

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    ReadFixed = 1,
    WriteFixed = 2,
    Recv = 3,
    Send = 4,
    Connect = 5,
    Close = 6,
    Fsync = 7,
    SyncFileRange = 8,
    Fallocate = 9,
    Accept = 10,
    SendTo = 11,
    UdpRecvStream = 12,
    UdpRefill = 13,
    Open = 14,
    Wakeup = 15,
    Timeout = 16,
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
    /// Driver-defined raw handle token type.
    type Handle: Handle;

    /// Pre-allocate any resources needed (e.g., accept socket on Windows).
    fn pre_alloc(fd: Self::Handle) -> std::io::Result<Self::PreAlloc>;

    /// Construct the operation from a raw handle and pre-allocated resources.
    fn into_op(fd: Self::Handle, pre: Self::PreAlloc) -> Self;

    /// Convert the completed operation result to the final output type.
    fn into_output(self, res: std::io::Result<usize>) -> std::io::Result<Self::Output>;

    /// Helper: Pre-allocate and construct the operation in one step.
    fn prepare_op(fd: Self::Handle) -> std::io::Result<Self> {
        let pre = Self::pre_alloc(fd)?;
        Ok(Self::into_op(fd, pre))
    }
}

/// Trait to convert a user-facing operation to a platform-specific driver operation.
pub trait IntoPlatformOp<O: PlatformOp>: Sized + std::marker::Send {
    /// User payload detached from kernel op.
    type UserPayload: std::marker::Send + 'static;
    const PAYLOAD_KIND: OpKind;

    /// Split into kernel-facing op and user payload.
    fn into_kernel_and_payload(self) -> (O, Self::UserPayload);

    /// Rebuild the user operation from payload.
    fn from_user_payload(payload: Self::UserPayload) -> Self;

    fn payload_into_erased(payload: Self::UserPayload) -> crate::slot::ErasedPayload;

    /// Rebuilds payload from a raw pointer previously produced by `payload_into_erased`.
    ///
    /// # Safety
    /// `ptr` must originate from the matching `Self::payload_into_erased` implementation
    /// of the same concrete operation type and must not have been consumed before.
    unsafe fn payload_from_raw(ptr: *mut ()) -> Self::UserPayload;

    /// Compatibility helper for transitional callsites.
    #[inline]
    fn from_kernel_and_payload(op: O, payload: Self::UserPayload) -> Self {
        drop(op);
        Self::from_user_payload(payload)
    }

    /// Compatibility helper for legacy callsites.
    #[inline]
    fn into_platform_op(self) -> O
    where
        Self::UserPayload: Default,
    {
        self.into_kernel_and_payload().0
    }

    /// Compatibility helper for legacy callsites.
    #[inline]
    fn from_platform_op(op: O) -> Self
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
    pub fn submit_detached<D>(self, driver: &mut D) -> DetachedOp<T, D::Op>
    where
        T: IntoPlatformOp<D::Op> + std::marker::Send + 'static,
        D: Driver,
    {
        let data = self.data;
        trace!("Submitting detached op");

        // Try reserve first
        match driver.reserve_op() {
            Ok((user_data, generation)) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                let mut op_platform = Some(kernel_op);
                let token = encode_completion_token(user_data, generation);
                let completion_table = driver.completion_table();
                let cancel_signal = driver.detached_cancel_table();
                let cancel_waker = driver.create_waker();
                driver.slot_set_payload(user_data, T::payload_into_erased(payload));

                let result = driver
                    .submit(user_data, &mut op_platform, SubmitBinder::new())
                    .into_inner();

                match result {
                    Ok(_) => {
                        completion_table.mark_waiting(token);
                        DetachedOp {
                            completion_table: Some(completion_table),
                            cancel_signal: Some(cancel_signal),
                            cancel_waker: Some(cancel_waker),
                            token,
                            immediate_failure: None,
                            _phantom: std::marker::PhantomData,
                        }
                    }
                    Err((e, status)) => {
                        trace!("Submit failed synchronously: {} (status={:?})", e, status);
                        if status == SubmitStatus::Void {
                            let payload_any = driver
                                .slot_take_payload(user_data)
                                .expect("Payload missing while recovering submit failure");
                            if payload_any.kind != T::PAYLOAD_KIND as u16 {
                                panic!("DetachedOp payload kind mismatch on submit recovery");
                            }
                            let payload = unsafe { T::payload_from_raw(payload_any.leak_ptr()) };
                            if let Some(op) = op_platform.take() {
                                drop(op);
                            }
                            DetachedOp {
                                completion_table: None,
                                cancel_signal: None,
                                cancel_waker: None,
                                token: 0,
                                immediate_failure: Some((e, T::from_user_payload(payload))),
                                _phantom: std::marker::PhantomData,
                            }
                        } else {
                            // status == InFlight: driver guaranteed an asynchronous result
                            completion_table.mark_waiting(token);
                            DetachedOp {
                                completion_table: Some(completion_table),
                                cancel_signal: Some(cancel_signal),
                                cancel_waker: Some(cancel_waker),
                                token,
                                immediate_failure: None,
                                _phantom: std::marker::PhantomData,
                            }
                        }
                    }
                }
            }
            Err(e) => {
                // Reservation failed (e.g. full).
                // Return DetachedOp with immediate failure.
                DetachedOp {
                    completion_table: None,
                    cancel_signal: None,
                    cancel_waker: None,
                    token: 0,
                    immediate_failure: Some((e, data)),
                    _phantom: std::marker::PhantomData,
                }
            }
        }
    }

    /// Submit this operation to a local IO driver.
    /// Returns a `LocalOp` future that resolves when the operation completes.
    pub fn submit_local<D>(self, driver: Rc<RefCell<D>>) -> LocalOp<T, D>
    where
        T: IntoPlatformOp<D::Op> + 'static,
        D: Driver,
    {
        LocalOp::new(self.data, driver)
    }
}

// ============================================================================
// DetachedOp (Future Implementation for Shared/Send Ops)
// ============================================================================

/// A Future representing a detached operation.
/// It polls a shared completion event queue by token.
pub struct DetachedOp<T, O>
where
    O: PlatformOp,
    T: IntoPlatformOp<O>,
{
    completion_table: Option<SharedCompletionTable>,
    cancel_signal: Option<std::sync::Arc<DetachedCancelTable>>,
    cancel_waker: Option<Arc<dyn RemoteWaker>>,
    token: u64,
    immediate_failure: Option<(std::io::Error, T)>,
    _phantom: std::marker::PhantomData<(T, fn() -> O)>,
}

// DetachedOp is Send/Sync if the Op data is Send and the Driver Op is Send (implied by SlotTable<Op> bound).
unsafe impl<T: IntoPlatformOp<O> + std::marker::Send, O: PlatformOp> std::marker::Send
    for DetachedOp<T, O>
{
}

impl<T, O> Drop for DetachedOp<T, O>
where
    O: PlatformOp,
    T: IntoPlatformOp<O>,
{
    fn drop(&mut self) {
        if let Some(table) = self.completion_table.as_ref() {
            table.mark_orphaned(self.token);
        }
        if let Some(cancel_signal) = self.cancel_signal.as_ref() {
            cancel_signal.request_cancel(self.token);
        }
        if let Some(cancel_waker) = self.cancel_waker.as_ref() {
            if let Err(e) = cancel_waker.wake() {
                trace!("DetachedOp cancel wake failed: {}", e);
            }
        }
    }
}

impl<T, O> Future for DetachedOp<T, O>
where
    O: PlatformOp,
    T: IntoPlatformOp<O>,
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
        match table.try_take_record(this.token) {
            PollRecordResult::Ready(record) => {
                let CompletionRecord {
                    event,
                    payload: payload_any,
                    detail,
                } = record;
                let Some(payload_any) = payload_any else {
                    return Poll::Ready(OpResult::Lost(std::io::Error::other(
                        "operation payload lost: completion sidecar missing",
                    )));
                };
                if payload_any.kind != T::PAYLOAD_KIND as u16 {
                    return Poll::Ready(OpResult::Lost(std::io::Error::other(
                        "operation payload lost: kind mismatch",
                    )));
                }
                let payload = unsafe { T::payload_from_raw(payload_any.leak_ptr()) };
                let data = T::from_user_payload(payload);
                let res = detail.unwrap_or_else(|| event_res_to_io(event.res));
                return Poll::Ready(OpResult::Completed(res, data));
            }
            PollRecordResult::Stale => {
                return Poll::Ready(OpResult::Lost(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "operation lost: slot recycled (generation mismatch)",
                )));
            }
            PollRecordResult::Pending => {}
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
pub struct LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op> + 'static,
{
    state: State,
    data: Option<T>,
    driver: Rc<RefCell<D>>,
    user_data: usize,
    token: u64,
}

impl<T, D> LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op> + 'static,
{
    /// Create a new local operation future.
    pub fn new(data: T, driver: Rc<RefCell<D>>) -> Self {
        Self {
            state: State::Defined,
            data: Some(data),
            driver,
            user_data: 0,
            token: 0,
        }
    }
}

impl<T, D> Future for LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op> + 'static,
{
    type Output = OpResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = unsafe { self.get_unchecked_mut() };

        if let State::Defined = op.state {
            let mut driver = op.driver.borrow_mut();

            // Submit to driver
            let data = op.data.take().expect("Op started without data");
            let (driver_op, payload) = data.into_kernel_and_payload();

            // reserve_op now returns generation, but we ignore it for LocalOp
            // because LocalOp lifetime is tied to the driver via Rc/RefCell.
            let (user_data, generation) = match driver.reserve_op() {
                Ok(v) => v,
                Err(e) => {
                    // Failed to reserve
                    drop(driver_op);
                    return Poll::Ready(OpResult::Completed(Err(e), T::from_user_payload(payload)));
                }
            };
            op.user_data = user_data;
            op.token = encode_completion_token(user_data, generation);
            driver.slot_set_payload(user_data, T::payload_into_erased(payload));

            // Submit to driver.
            let mut driver_op_opt = Some(driver_op);
            let result = driver
                .submit(user_data, &mut driver_op_opt, SubmitBinder::new())
                .into_inner();

            match result {
                Ok(_) => {
                    op.state = State::Submitted;
                }
                Err((e, status)) => {
                    if status == SubmitStatus::Void {
                        if let Some(val) = driver_op_opt.take() {
                            drop(val);
                        }
                        let payload_any = driver
                            .slot_take_payload(user_data)
                            .expect("Payload missing while recovering submit failure");
                        if payload_any.kind != T::PAYLOAD_KIND as u16 {
                            panic!("LocalOp payload kind mismatch on submit recovery");
                        }
                        let payload = unsafe { T::payload_from_raw(payload_any.leak_ptr()) };
                        let data = T::from_user_payload(payload);
                        return Poll::Ready(OpResult::Completed(Err(e), data));
                    } else {
                        // status == InFlight: driver guaranteed an asynchronous result
                        op.state = State::Submitted;
                    }
                }
            }

            op.state = State::Submitted;
        }

        if let State::Submitted = op.state {
            let mut driver = op.driver.borrow_mut();
            match driver.try_take_completion_record(op.token) {
                PollRecordResult::Ready(record) => {
                    op.state = State::Completed;
                    let CompletionRecord {
                        event,
                        payload: payload_any,
                        detail,
                    } = record;
                    let Some(payload_any) = payload_any else {
                        return Poll::Ready(OpResult::Lost(std::io::Error::other(
                            "operation payload lost: completion sidecar missing",
                        )));
                    };
                    if payload_any.kind != T::PAYLOAD_KIND as u16 {
                        return Poll::Ready(OpResult::Lost(std::io::Error::other(
                            "operation payload lost: kind mismatch",
                        )));
                    }
                    let payload = unsafe { T::payload_from_raw(payload_any.leak_ptr()) };
                    let data = T::from_user_payload(payload);
                    let res = detail.unwrap_or_else(|| event_res_to_io(event.res));
                    Poll::Ready(OpResult::Completed(res, data))
                }
                PollRecordResult::Stale => {
                    op.state = State::Completed;
                    Poll::Ready(OpResult::Lost(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "operation lost: slot recycled (generation mismatch)",
                    )))
                }
                PollRecordResult::Pending => {
                    driver.register_completion_waker(op.token, cx.waker());
                    Poll::Pending
                }
            }
        } else {
            panic!("Polled after completion");
        }
    }
}

impl<T, D> Drop for LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op> + 'static,
{
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

pub trait OpSubmitter<D: Driver>: Clone + std::marker::Send + Sync + 'static {
    type Future<T: IntoPlatformOp<D::Op> + std::marker::Send + 'static>: Future<
        Output = OpResult<T>,
    >;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<D::Op> + std::marker::Send + 'static;

    fn from_current_context() -> std::io::Result<Self>;
}

// ============================================================================
// LocalSubmitter
// ============================================================================

#[derive(Clone, Copy)]
pub struct LocalSubmitter;

impl<D: Driver> OpSubmitter<D> for LocalSubmitter {
    type Future<T: IntoPlatformOp<D::Op> + std::marker::Send + 'static> = LocalOp<T, D>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> LocalOp<T, D>
    where
        T: IntoPlatformOp<D::Op> + std::marker::Send + 'static,
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

impl<D: Driver> OpSubmitter<D> for DetachedSubmitter {
    type Future<T: IntoPlatformOp<D::Op> + std::marker::Send + 'static> = DetachedOp<T, D::Op>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<D::Op> + std::marker::Send + 'static,
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
pub struct ReadFixed<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Write to a file descriptor at a specific offset using a fixed buffer.
pub struct WriteFixed<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Receive data from a socket into a fixed buffer.
pub struct Recv<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Send data from a fixed buffer to a socket.
pub struct Send<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Connect a socket to a remote address.
pub struct Connect<H: Handle, A: SockAddr> {
    pub fd: IoFd<H>,
    /// Raw address bytes (sockaddr representation), boxed to reduce struct size.
    pub addr: A,
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
pub struct Close<H: Handle> {
    pub fd: IoFd<H>,
}

/// Flush file buffers to disk.
pub struct Fsync<H: Handle> {
    pub fd: IoFd<H>,
    /// If true, only sync data (not metadata).
    pub datasync: bool,
}

/// Timeout operation (platform-specific timing).
pub struct Timeout {
    pub duration: std::time::Duration,
}

/// Wake up the event loop.
pub struct Wakeup<H: Handle> {
    pub fd: IoFd<H>,
}

/// Accept a new connection on a listening socket.
/// Result includes the new socket handle and remote address.
pub struct Accept<H: Handle, A: SockAddr> {
    pub fd: IoFd<H>,
    /// Buffer for storing the remote address.
    /// On Windows, we parse the result from the AcceptEx output buffer, so we don't need this storage.
    pub addr: A,
    /// Length of the address buffer.
    pub addr_len: u32,
    /// Parsed remote address (populated after completion).
    pub remote_addr: Option<std::net::SocketAddr>,
}

/// Send data to a specific address (UDP).
pub struct SendTo<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
    /// Target address.
    pub addr: std::net::SocketAddr,
}

/// Sync file range.
pub struct SyncFileRange<H: Handle> {
    pub fd: IoFd<H>,
    pub offset: u64,
    pub nbytes: u64,
    pub flags: u32,
}

/// Pre-allocate file space.
pub struct Fallocate<H: Handle> {
    pub fd: IoFd<H>,
    pub mode: i32,
    pub offset: u64,
    pub len: u64,
}

/// Receive data as UDP datagram stream.
pub struct UdpRecvStream<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct UdpRefill<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: Option<FixedBuf>,
}

// ============================================================================
// OpLifecycle Implementations
// ============================================================================

impl<H: Handle + From<usize>, A: SockAddr> OpLifecycle for Accept<H, A> {
    type PreAlloc = ();
    type Output = (H, std::net::SocketAddr);
    type Handle = H;

    fn pre_alloc(_fd: H) -> std::io::Result<Self::PreAlloc> {
        Ok(())
    }

    fn into_op(fd: H, _pre: Self::PreAlloc) -> Self {
        // Use stack/inline storage
        let addr_len = std::mem::size_of::<A>() as u32;

        Self {
            fd: IoFd::Raw(fd),
            addr: A::default(),
            addr_len,
            remote_addr: None,
        }
    }

    fn into_output(self, res: std::io::Result<usize>) -> std::io::Result<Self::Output> {
        let fd = res?.into();
        let addr = self.remote_addr.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "driver must populate Accept::remote_addr before completion",
            )
        })?;
        Ok((fd, addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::CompletionTable;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct DummyOp;

    #[derive(Clone, Copy)]
    struct DummyPayload;

    struct DummyPlatformOp;

    impl crate::driver::PlatformOp for DummyPlatformOp {}

    struct CountingWaker {
        wakes: Arc<AtomicUsize>,
    }

    impl RemoteWaker for CountingWaker {
        fn wake(&self) -> std::io::Result<()> {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl IntoPlatformOp<DummyPlatformOp> for DummyOp {
        type UserPayload = DummyPayload;
        const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

        fn into_kernel_and_payload(self) -> (DummyPlatformOp, Self::UserPayload) {
            (DummyPlatformOp, DummyPayload)
        }

        fn from_user_payload(_: Self::UserPayload) -> Self {
            Self
        }

        fn payload_into_erased(_: Self::UserPayload) -> crate::slot::ErasedPayload {
            unsafe fn drop_payload(ptr: *mut ()) {
                let _ = unsafe { Box::from_raw(ptr as *mut ()) };
            }

            let ptr = Box::into_raw(Box::new(())) as *mut ();
            crate::slot::ErasedPayload {
                ptr,
                kind: 1,
                drop_fn: drop_payload,
            }
        }

        unsafe fn payload_from_raw(ptr: *mut ()) -> Self::UserPayload {
            let _ = unsafe { Box::from_raw(ptr as *mut ()) };
            DummyPayload
        }
    }

    #[test]
    fn detached_op_drop_triggers_cancel_wake() {
        let completion_table = Arc::new(CompletionTable::new(1));
        let cancel_signal = Arc::new(DetachedCancelTable::new(1));
        let wake_count = Arc::new(AtomicUsize::new(0));
        let cancel_waker: Arc<dyn RemoteWaker> = Arc::new(CountingWaker {
            wakes: wake_count.clone(),
        });

        let op = DetachedOp::<DummyOp, DummyPlatformOp> {
            completion_table: Some(completion_table),
            cancel_signal: Some(cancel_signal),
            cancel_waker: Some(cancel_waker),
            token: encode_completion_token(0, 1),
            immediate_failure: None,
            _phantom: std::marker::PhantomData,
        };

        drop(op);

        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    }
}
