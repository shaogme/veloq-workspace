// use crate::buffer::FixedBuf;

pub(crate) mod op_registry;
pub(crate) mod slot;
use std::io;
use std::task::{Context, Poll};

/// Platform-specific operation trait
pub trait PlatformOp: 'static {}

pub trait Driver: 'static {
    /// Platform-specific operation type
    type Op: PlatformOp;

    /// Register a new operation. Returns the user_data key and expected generation.
    fn reserve_op(&mut self) -> io::Result<(usize, u32)>;

    /// Get the shared slot table if available.
    fn slot_table(&self) -> std::sync::Arc<slot::SlotTable<Self::Op>>;

    /// Submit an operation to the driver.
    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>>;

    /// Poll for operation completion.
    fn poll_op(
        &mut self,
        user_data: usize,
        cx: &mut Context<'_>,
        binder: PollBinder,
    ) -> Outcome<Poll<io::Result<usize>>>;

    /// Submit queued operations to the kernel.
    fn submit_queue(&mut self) -> io::Result<()>;

    /// Wait for completions.
    fn wait(&mut self) -> io::Result<()>;

    /// Process the completion queue.
    fn process_completions(&mut self);

    /// Cancel an operation.
    fn cancel_op(&mut self, user_data: usize);

    /// Register a memory chunk with the driver.
    /// `id` is the ChunkID (0..MAX_CHUNKS).
    /// `ptr` and `len` define the memory region.
    /// This allows incremental registration without stopping the world.
    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()>;

    /// Register a set of file descriptors/handles.
    /// Returns a list of `IoFd` that can be used in subsequent operations.
    fn register_files(&mut self, files: &[crate::RawHandle]) -> io::Result<Vec<crate::op::IoFd>>;

    /// Unregister a set of file descriptors/handles.
    fn unregister_files(&mut self, files: Vec<crate::op::IoFd>) -> io::Result<()>;

    /// Submit a fire-and-forget operation (e.g. Close).
    /// The driver takes ownership of resources and ensures cleanup.
    fn submit_background(&mut self, op: Self::Op) -> io::Result<()>;

    /// Wake up the driver from blocking wait.
    fn wake(&mut self) -> io::Result<()>;

    /// Get the low-level driver handle (RawFd on Linux, HANDLE on Windows).
    /// Used for direct mesh communication (e.g. MSG_RING).
    fn inner_handle(&self) -> crate::RawHandle;

    /// Create a thread-safe waker.
    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker>;

    /// Get the unique identifier of the driver.
    fn driver_id(&self) -> usize;

    /// Set the buffer registrar for lazy registration support.
    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>);
}

pub trait RemoteWaker: Send + Sync {
    fn wake(&self) -> io::Result<()>;
}

/// A trait for processing detached completion logic.
pub trait DetachedCompleter: Send {
    fn complete(self: Box<Self>, res: io::Result<usize>);
}

// Platform-specific driver implementations

/// A wrapper for driver method return values that enforces resource state management.
#[must_use]
pub struct Outcome<T>(T);

impl<T> Outcome<T> {
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Binder for `submit` operation.
pub struct SubmitBinder;

impl SubmitBinder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Finish submission with success. The Op is assumed to be held by the driver.
    #[inline]
    pub fn ok(self, poll: Poll<()>) -> Outcome<io::Result<Poll<()>>> {
        Outcome(Ok(poll))
    }

    /// Finish submission with failure.
    #[inline]
    pub fn err(self, err: io::Error) -> Outcome<io::Result<Poll<()>>> {
        Outcome(Err(err))
    }
}

/// Binder for `poll_op` operation.
pub struct PollBinder;

impl PollBinder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Op is ready.
    #[inline]
    pub fn ready(self, res: io::Result<usize>) -> Outcome<Poll<io::Result<usize>>> {
        Outcome(Poll::Ready(res))
    }

    /// Op is still pending, it remains owned by the driver.
    #[inline]
    pub fn pending(self) -> Outcome<Poll<io::Result<usize>>> {
        Outcome(Poll::Pending)
    }
}

#[cfg(target_os = "linux")]
pub(crate) mod uring;

#[cfg(target_os = "linux")]
pub use uring::UringDriver as PlatformDriver;

#[cfg(target_os = "windows")]
pub(crate) mod iocp;

#[cfg(target_os = "windows")]
pub use iocp::CloseMode;
#[cfg(target_os = "windows")]
pub use iocp::IocpDriver as PlatformDriver;
