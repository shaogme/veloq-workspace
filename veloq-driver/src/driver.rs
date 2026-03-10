// use crate::buffer::FixedBuf;

pub(crate) mod op_registry;
pub(crate) mod slot;
use crossbeam_queue::SegQueue;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::task::Poll;
use std::task::Waker;
use veloq_atomic_waker::AtomicWaker;

/// Platform-specific operation trait
pub trait PlatformOp: 'static {}

/// Unified completion event produced by platform drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEvent {
    /// Encoded completion token (generation + slot index).
    pub user_data: u64,
    /// Completion result code. Non-negative for success, negative for error.
    pub res: i32,
    /// Platform-specific completion flags.
    pub flags: u32,
}

pub type SharedCompletionQueue = Arc<SegQueue<CompletionEvent>>;
pub type SharedCompletionTable = Arc<CompletionTable>;
pub struct CompletionCell {
    expected_generation: AtomicU32,
    ready_generation: AtomicU32,
    ready: AtomicBool,
    res: AtomicI32,
    flags: AtomicU32,
    waker: AtomicWaker,
}

impl CompletionCell {
    fn new() -> Self {
        Self {
            expected_generation: AtomicU32::new(0),
            ready_generation: AtomicU32::new(0),
            ready: AtomicBool::new(false),
            res: AtomicI32::new(0),
            flags: AtomicU32::new(0),
            waker: AtomicWaker::new(),
        }
    }
}

pub struct CompletionTable {
    cells: Box<[CompletionCell]>,
}

impl CompletionTable {
    pub fn new(capacity: usize) -> Self {
        let mut cells = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            cells.push(CompletionCell::new());
        }
        Self {
            cells: cells.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn record_completion(&self, event: CompletionEvent) {
        let (idx, generation) = decode_completion_token(event.user_data);
        if idx >= self.cells.len() {
            return;
        }
        let cell = &self.cells[idx];
        cell.res.store(event.res, Ordering::Release);
        cell.flags.store(event.flags, Ordering::Release);
        cell.ready_generation.store(generation, Ordering::Release);
        cell.ready.store(true, Ordering::Release);
        if cell.expected_generation.load(Ordering::Acquire) == generation {
            cell.waker.wake();
        }
    }

    #[inline]
    pub fn try_take(&self, token: u64) -> Option<CompletionEvent> {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.cells.len() {
            return None;
        }
        let cell = &self.cells[idx];
        if !cell.ready.load(Ordering::Acquire) {
            return None;
        }
        if cell.ready_generation.load(Ordering::Acquire) != generation {
            return None;
        }
        if cell
            .ready
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        Some(CompletionEvent {
            user_data: token,
            res: cell.res.load(Ordering::Acquire),
            flags: cell.flags.load(Ordering::Acquire),
        })
    }

    #[inline]
    pub fn register_waker(&self, token: u64, waker: &Waker) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.cells.len() {
            return;
        }
        let cell = &self.cells[idx];
        cell.expected_generation
            .store(generation, Ordering::Release);
        cell.waker.register(waker);
        if cell.ready.load(Ordering::Acquire)
            && cell.ready_generation.load(Ordering::Acquire) == generation
        {
            waker.wake_by_ref();
        }
    }
}

#[inline]
pub fn encode_completion_token(index: usize, generation: u32) -> u64 {
    ((generation as u64) << 32) | (index as u32 as u64)
}

#[inline]
pub fn decode_completion_token(token: u64) -> (usize, u32) {
    let index = (token & 0xffff_ffff) as usize;
    let generation = (token >> 32) as u32;
    (index, generation)
}

#[inline]
pub fn event_res_to_io(res: i32) -> io::Result<usize> {
    if res >= 0 {
        Ok(res as usize)
    } else {
        Err(io::Error::from_raw_os_error(-res))
    }
}

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

    /// Submit queued operations to the kernel.
    fn submit_queue(&mut self) -> io::Result<()>;

    /// Wait for completions.
    fn wait(&mut self) -> io::Result<()>;

    /// Process the completion queue.
    fn process_completions(&mut self);

    /// Shared completion queue for event-stream consumption.
    fn completion_queue(&self) -> SharedCompletionQueue;

    /// Shared completion table for token-targeted consumption.
    fn completion_table(&self) -> SharedCompletionTable;

    /// Pop one completion event if available.
    fn try_pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completion_queue().pop()
    }

    /// Drain completion events into `out`, returning drained count.
    fn drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> usize {
        let mut drained = 0;
        let queue = self.completion_queue();
        while let Some(ev) = queue.pop() {
            out.push(ev);
            drained += 1;
        }
        drained
    }

    /// Wait for completions and drain events into `out`.
    fn wait_and_drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> io::Result<usize> {
        self.wait()?;
        Ok(self.drain_completions(out))
    }

    /// Try take completion for a specific token.
    fn try_take_completion(&mut self, token: u64) -> Option<CompletionEvent> {
        self.completion_table().try_take(token)
    }

    /// Register/replace a waiter for token.
    fn register_completion_waker(&mut self, token: u64, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    /// Store detached payload inside driver slot state.
    fn store_detached_payload<T: Send + 'static>(
        &mut self,
        user_data: usize,
        generation: u32,
        payload: T,
    ) {
        let slots = self.slot_table();
        let slot = &slots.slots[user_data];
        if slot.generation.load(Ordering::Acquire) == generation {
            unsafe {
                *slot.detached_payload.get() = Some(slot::DetachedPayload::new(payload));
            }
        }
    }

    /// Take detached payload from driver slot state by token.
    fn take_detached_payload<T: Send + 'static>(&mut self, token: u64) -> Option<T> {
        let (user_data, generation) = decode_completion_token(token);
        self.take_detached_payload_from_slot::<T>(user_data, generation)
    }

    /// Take detached payload from a slot if generation/type match.
    fn take_detached_payload_from_slot<T: Send + 'static>(
        &mut self,
        user_data: usize,
        generation: u32,
    ) -> Option<T> {
        let slots = self.slot_table();
        let slot = &slots.slots[user_data];
        if slot.generation.load(Ordering::Acquire) != generation {
            return None;
        }
        let payload = unsafe { (*slot.detached_payload.get()).take() }?;
        payload.try_take::<T>().ok()
    }
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
#[derive(Default)]
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
