use crate::handle::{IoFd, RawHandle};
use crate::slot;
use crossbeam_queue::SegQueue;
use std::cell::UnsafeCell;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::task::Poll;
use std::task::Waker;
use veloq_atomic_waker::AtomicWaker;

/// Platform-specific operation trait
pub trait PlatformOp: 'static {}

pub struct CompletionSidecar {
    pub user_data: usize,
    pub generation: u32,
    pub res: i32,
    pub flags: u32,
    pub payload: Option<slot::ErasedPayload>,
    pub detail: Option<io::Result<usize>>,
}

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
    payload: UnsafeCell<Option<slot::ErasedPayload>>,
    detail: UnsafeCell<Option<io::Result<usize>>>,
    waker: AtomicWaker,
}

unsafe impl Sync for CompletionCell {}

impl CompletionCell {
    fn new() -> Self {
        Self {
            expected_generation: AtomicU32::new(0),
            ready_generation: AtomicU32::new(0),
            ready: AtomicBool::new(false),
            res: AtomicI32::new(0),
            flags: AtomicU32::new(0),
            payload: UnsafeCell::new(None),
            detail: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
        }
    }
}

pub struct CompletionRecord {
    pub event: CompletionEvent,
    pub payload: Option<slot::ErasedPayload>,
    pub detail: Option<io::Result<usize>>,
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
    pub fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        payload: Option<slot::ErasedPayload>,
        detail: Option<io::Result<usize>>,
    ) {
        let (idx, generation) = decode_completion_token(event.user_data);
        if idx >= self.cells.len() {
            return;
        }
        let cell = &self.cells[idx];
        if cell.ready.swap(false, Ordering::AcqRel) {
            unsafe {
                let _ = (*cell.payload.get()).take();
                let _ = (*cell.detail.get()).take();
            }
        }
        unsafe {
            *cell.payload.get() = payload;
            *cell.detail.get() = detail;
        }
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
        self.try_take_record(token).map(|record| record.event)
    }

    #[inline]
    pub fn try_take_record(&self, token: u64) -> Option<CompletionRecord> {
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
        let payload = unsafe { (*cell.payload.get()).take() };
        let detail = unsafe { (*cell.detail.get()).take() };
        Some(CompletionRecord {
            event: CompletionEvent {
                user_data: token,
                res: cell.res.load(Ordering::Acquire),
                flags: cell.flags.load(Ordering::Acquire),
            },
            payload,
            detail,
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
    type Op: PlatformOp;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)>;

    fn slot_table(&self) -> std::sync::Arc<slot::SlotTable<Self::Op>>;

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>>;

    fn submit_queue(&mut self) -> io::Result<()>;

    fn wait(&mut self) -> io::Result<()>;

    fn process_completions(&mut self);

    fn completion_queue(&self) -> SharedCompletionQueue;

    fn completion_table(&self) -> SharedCompletionTable;

    fn try_pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completion_queue().pop()
    }

    fn drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> usize {
        let mut drained = 0;
        let queue = self.completion_queue();
        while let Some(ev) = queue.pop() {
            out.push(ev);
            drained += 1;
        }
        drained
    }

    fn wait_and_drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> io::Result<usize> {
        self.wait()?;
        Ok(self.drain_completions(out))
    }

    fn try_take_completion(&mut self, token: u64) -> Option<CompletionEvent> {
        self.completion_table().try_take(token)
    }

    fn try_take_completion_record(&mut self, token: u64) -> Option<CompletionRecord> {
        self.completion_table().try_take_record(token)
    }

    fn register_completion_waker(&mut self, token: u64, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    fn cancel_op(&mut self, user_data: usize);

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()>;

    fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>>;

    fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()>;

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()>;

    fn wake(&mut self) -> io::Result<()>;

    fn inner_handle(&self) -> RawHandle;

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker>;

    fn driver_id(&self) -> usize;

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>);
}

pub trait RemoteWaker: Send + Sync {
    fn wake(&self) -> io::Result<()>;
}

#[must_use]
pub struct Outcome<T>(T);

impl<T> Outcome<T> {
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

#[derive(Default)]
pub struct SubmitBinder;

impl SubmitBinder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    #[inline]
    pub fn ok(self, poll: Poll<()>) -> Outcome<io::Result<Poll<()>>> {
        Outcome(Ok(poll))
    }

    #[inline]
    pub fn err(self, err: io::Error) -> Outcome<io::Result<Poll<()>>> {
        Outcome(Err(err))
    }
}

pub mod test_hooks {
    pub trait DriverTestHooks {
        fn debug_chunk_register_attempts(&self) -> u64;
    }
}
