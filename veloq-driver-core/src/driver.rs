use crate::slot;
use crate::{Handle, IoFd, SlotSidecar};
use crossbeam_queue::SegQueue;

use veloq_shim::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use veloq_shim::cell::UnsafeCell;
use veloq_shim::hint;

use std::io;
use std::sync::Arc;

use std::task::Poll;
use std::task::Waker;
use veloq_atomic_waker::AtomicWaker;

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

/// Platform-specific operation trait
const STATE_GEN_SHIFT: u32 = 32;

#[inline]
fn pack_state_gen(state: u8, generation: u32) -> u64 {
    ((state as u64) << STATE_GEN_SHIFT) | (generation as u64)
}

#[inline]
fn unpack_state_gen(val: u64) -> (u8, u32) {
    ((val >> STATE_GEN_SHIFT) as u8, (val & 0xffff_ffff) as u32)
}

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
    state_gen: AtomicU64,
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
            state_gen: AtomicU64::new(pack_state_gen(CELL_STATE_IDLE, u32::MAX)),
            res: AtomicI32::new(0),
            flags: AtomicU32::new(0),
            payload: UnsafeCell::new(None),
            detail: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
        }
    }

    /// # Safety
    ///
    /// Caller must ensure exclusive access to completion payload/detail for this cell.
    #[inline]
    unsafe fn with_data_unchecked<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Option<slot::ErasedPayload>, &mut Option<io::Result<usize>>) -> R,
    {
        // SAFETY: Caller guarantees exclusive mutable access to these fields.
        unsafe {
            self.payload
                .with_mut(|payload| self.detail.with_mut(|detail| f(payload, detail)))
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

        loop {
            let current = cell.state_gen.load(Ordering::Acquire);
            let (state, cell_gen) = unpack_state_gen(current);

            // Stale completion check.
            if state != CELL_STATE_IDLE && generation < cell_gen {
                // If it's a very old generation, just discard it.
                return;
            }

            match state {
                CELL_STATE_IDLE | CELL_STATE_READY | CELL_STATE_WAITING => {
                    if cell
                        .state_gen
                        .compare_exchange(
                            current,
                            pack_state_gen(CELL_STATE_BUSY, generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if state == CELL_STATE_READY {
                            unsafe {
                                cell.with_data_unchecked(|payload_cell, detail_cell| {
                                    let _ = payload_cell.take();
                                    let _ = detail_cell.take();
                                });
                            }
                        }
                        break;
                    }
                }
                CELL_STATE_ORPHANED => {
                    if cell_gen == generation {
                        if cell
                            .state_gen
                            .compare_exchange(
                                current,
                                pack_state_gen(CELL_STATE_IDLE, generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            // Abandoned by consumer, drop incoming data
                            drop(payload);
                            drop(detail);
                            return;
                        }
                    } else {
                        // Stale completion for a slot that was orphaned but now maybe reused
                        // (though reused would have changed gen).
                        return;
                    }
                }
                CELL_STATE_BUSY => {
                    hint::spin_loop();
                }
                _ => unreachable!(),
            }
        }

        unsafe {
            cell.with_data_unchecked(|payload_cell, detail_cell| {
                *payload_cell = payload;
                *detail_cell = detail;
            });
        }
        cell.res.store(event.res, Ordering::Release);
        cell.flags.store(event.flags, Ordering::Release);

        cell.state_gen.store(
            pack_state_gen(CELL_STATE_READY, generation),
            Ordering::Release,
        );

        cell.waker.wake();
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

        let current = cell.state_gen.load(Ordering::Acquire);
        let (state, cell_gen) = unpack_state_gen(current);

        if state != CELL_STATE_READY || cell_gen != generation {
            return None;
        }

        if cell
            .state_gen
            .compare_exchange(
                current,
                pack_state_gen(CELL_STATE_IDLE, generation),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return None;
        }

        let (payload, detail) = unsafe {
            cell.with_data_unchecked(|payload_cell, detail_cell| {
                (payload_cell.take(), detail_cell.take())
            })
        };
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

        loop {
            let current = cell.state_gen.load(Ordering::Acquire);
            let (state, cell_gen) = unpack_state_gen(current);

            // Register waker first to avoid missing a race.
            cell.waker.register(waker);

            if cell_gen != generation {
                // Try to initial/update generation while keeping state if possible.
                // This is needed for LocalOp which doesn't call mark_waiting.
                if cell
                    .state_gen
                    .compare_exchange(
                        current,
                        pack_state_gen(state, generation),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    continue;
                }
            }

            // Check for fast completion.
            let current_after = cell.state_gen.load(Ordering::Acquire);
            let (state_after, gen_after) = unpack_state_gen(current_after);
            if state_after == CELL_STATE_READY && gen_after == generation {
                waker.wake_by_ref();
            }
            return;
        }
    }

    #[inline]
    pub fn mark_waiting(&self, token: u64) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.cells.len() {
            return;
        }
        let cell = &self.cells[idx];

        loop {
            let current = cell.state_gen.load(Ordering::Acquire);
            let (state, cell_generation) = unpack_state_gen(current);

            if state == CELL_STATE_READY && cell_generation == generation {
                // Fast completion happened, leave as READY.
                return;
            }

            match state {
                CELL_STATE_IDLE | CELL_STATE_ORPHANED | CELL_STATE_WAITING => {
                    if cell
                        .state_gen
                        .compare_exchange(
                            current,
                            pack_state_gen(CELL_STATE_WAITING, generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                CELL_STATE_READY => {
                    // Stale data or different operation, clear and wait.
                    if cell
                        .state_gen
                        .compare_exchange(
                            current,
                            pack_state_gen(CELL_STATE_WAITING, generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        unsafe {
                            cell.with_data_unchecked(|payload, detail| {
                                let _ = payload.take();
                                let _ = detail.take();
                            });
                        }
                        return;
                    }
                }
                CELL_STATE_BUSY => hint::spin_loop(),
                _ => unreachable!(),
            }
        }
    }

    #[inline]
    pub fn mark_orphaned(&self, token: u64) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.cells.len() {
            return;
        }
        let cell = &self.cells[idx];

        loop {
            let current = cell.state_gen.load(Ordering::Acquire);
            let (state, cell_gen) = unpack_state_gen(current);

            match state {
                CELL_STATE_WAITING => {
                    if cell_gen != generation {
                        return;
                    }
                    if cell
                        .state_gen
                        .compare_exchange(
                            current,
                            pack_state_gen(CELL_STATE_ORPHANED, generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                CELL_STATE_READY => {
                    if cell_gen == generation {
                        if cell
                            .state_gen
                            .compare_exchange(
                                current,
                                pack_state_gen(CELL_STATE_IDLE, generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            unsafe {
                                cell.with_data_unchecked(|payload_cell, detail_cell| {
                                    let _ = payload_cell.take();
                                    let _ = detail_cell.take();
                                });
                            }
                            return;
                        }
                    } else {
                        // Stale READY record, ignore.
                        return;
                    }
                }
                CELL_STATE_BUSY => {
                    hint::spin_loop();
                }
                _ => return,
            }
        }
    }

    #[inline]
    pub fn debug_get_state(&self, idx: usize) -> u8 {
        let current = self.cells[idx].state_gen.load(Ordering::Acquire);
        unpack_state_gen(current).0
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
    type Handle: Handle;
    type Sidecar: SlotSidecar;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)>;

    fn slot_table(&self) -> std::sync::Arc<slot::SlotTable<Self::Op, Self::Sidecar>>;

    fn slot_set_payload(&mut self, user_data: usize, payload: slot::ErasedPayload);

    fn slot_take_payload(&mut self, user_data: usize) -> Option<slot::ErasedPayload>;

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

    fn register_files(&mut self, files: &[Self::Handle]) -> io::Result<Vec<IoFd<Self::Handle>>>;

    fn unregister_files(&mut self, files: Vec<IoFd<Self::Handle>>) -> io::Result<()>;

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()>;

    fn wake(&mut self) -> io::Result<()>;

    fn inner_handle(&self) -> Self::Handle;

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

#[cfg(feature = "test-hooks")]
pub mod test_hooks {
    pub trait DriverTestHooks {
        fn debug_chunk_register_attempts(&self) -> u64;
    }
}
