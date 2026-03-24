use crate::slot;
use crate::slot::is_runnable_state;
use crate::{BorrowedRawHandle, IoFd, OwnedRawHandle, RawHandleMeta, SlotSidecar};
use crossbeam_queue::SegQueue;

use veloq_shim::atomic::Ordering;

use std::io;
use std::sync::Arc;

use std::task::Poll;
use std::task::Waker;

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

pub trait PlatformOp: 'static {}

pub enum RegisterFd<'a, H: RawHandleMeta> {
    Borrowed(BorrowedRawHandle<'a, H>),
    Owned(OwnedRawHandle<H>),
}

pub trait CompletionValue: Send + 'static {
    fn from_event_res(res: i32) -> io::Result<Self>
    where
        Self: Sized;
}

impl CompletionValue for usize {
    #[inline]
    fn from_event_res(res: i32) -> io::Result<Self> {
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        }
    }
}

pub struct CompletionSidecar<R = usize> {
    pub user_data: usize,
    pub generation: u32,
    pub res: i32,
    pub flags: u32,
    pub payload: Option<slot::ErasedPayload>,
    pub detail: Option<io::Result<R>>,
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
pub type SharedCompletionTable<R = usize> = Arc<dyn CompletionAccess<R>>;

pub struct CompletionRecord<R = usize> {
    pub event: CompletionEvent,
    pub payload: Option<slot::ErasedPayload>,
    pub detail: Option<io::Result<R>>,
}

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<R>),
    /// Operation is still in flight.
    Pending,
    /// Operation lost because the slot has been recycled for a newer generation.
    Stale,
}

pub trait CompletionAccess<R = usize>: Send + Sync {
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        payload: Option<slot::ErasedPayload>,
        detail: Option<io::Result<R>>,
    );

    fn try_take_record(&self, token: u64) -> PollRecordResult<R>;

    #[inline]
    fn try_take(&self, token: u64) -> PollRecordResult<R> {
        self.try_take_record(token)
    }

    fn register_waker(&self, token: u64, waker: &Waker);

    fn mark_waiting(&self, token: u64);

    fn mark_orphaned(&self, token: u64);

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;
}

impl<Op: PlatformOp, S: SlotSidecar, R: Send + 'static> CompletionAccess<R>
    for slot::SlotTable<Op, S, R>
{
    #[inline]
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        mut payload: Option<slot::ErasedPayload>,
        mut detail: Option<io::Result<R>>,
    ) {
        let (idx, generation) = decode_completion_token(event.user_data);
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];
        let ready_from = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            if generation < cell_gen {
                return;
            }
            if generation > cell_gen && state != slot::SlotState::Idle {
                return;
            }

            match state {
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightReady
                | slot::SlotState::InFlightWaiting
                | slot::SlotState::ReservedValue => {
                    if cell
                        .core_state
                        .compare_exchange(current, current, Ordering::Acquire, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    if state == slot::SlotState::InFlightReady {
                        cell.completion_with_data(|payload_cell, detail_cell| {
                            let _ = payload_cell.take();
                            let _ = detail_cell.take();
                        });
                    }
                    break current;
                }
                slot::SlotState::InFlightOrphaned => {
                    if cell_gen == generation {
                        if cell
                            .core_state
                            .compare_exchange(
                                current,
                                current
                                    .with_state(slot::SlotState::Idle)
                                    .with_generation(generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            // Abandoned by consumer, drop incoming data
                            let _ = payload.take();
                            let _ = detail.take();
                            return;
                        }
                    } else {
                        // generation > cell_gen but state is ORPHANED?
                        // Strict rule says reject.
                        return;
                    }
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        cell.completion_with_data(|payload_cell, detail_cell| {
            *payload_cell = payload.take();
            *detail_cell = detail.take();
        });
        cell.completion_res.store(event.res, Ordering::Release);
        cell.completion_flags.store(event.flags, Ordering::Release);

        match cell.core_state.compare_exchange(
            ready_from,
            ready_from
                .with_state(slot::SlotState::InFlightReady)
                .with_generation(generation),
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(next) => {
                let next_gen = next.generation();
                let next_state = next.state();
                if next_gen == generation
                    && next_state == slot::SlotState::InFlightWaiting
                    && cell
                        .core_state
                        .compare_exchange(
                            next,
                            next.with_state(slot::SlotState::InFlightReady)
                                .with_generation(generation),
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    cell.completion_waker.wake();
                    return;
                }

                cell.completion_with_data(|payload_cell, detail_cell| {
                    let _ = payload_cell.take();
                    let _ = detail_cell.take();
                });
                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() == generation
                    && cur.state() == slot::SlotState::InFlightOrphaned
                {
                    let _ = cell.core_state.compare_exchange(
                        cur,
                        cur.with_state(slot::SlotState::Idle)
                            .with_generation(generation),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                return;
            }
        }

        cell.completion_waker.wake();
    }

    #[inline]
    fn try_take_record(&self, token: u64) -> PollRecordResult<R> {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.slots.len() {
            return PollRecordResult::Pending;
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        // If the cell's generation is strictly greater than ours, we are stale.
        if cell_gen > generation {
            return PollRecordResult::Stale;
        }

        if state != slot::SlotState::InFlightReady || cell_gen != generation {
            return PollRecordResult::Pending;
        }

        if cell
            .core_state
            .compare_exchange(
                current,
                current
                    .with_state(slot::SlotState::Idle)
                    .with_generation(generation),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return PollRecordResult::Pending;
        }

        let (payload, detail) = cell.completion_with_data(|payload_cell, detail_cell| {
            (payload_cell.take(), detail_cell.take())
        });
        PollRecordResult::Ready(CompletionRecord {
            event: CompletionEvent {
                user_data: token,
                res: cell.completion_res.load(Ordering::Acquire),
                flags: cell.completion_flags.load(Ordering::Acquire),
            },
            payload,
            detail,
        })
    }

    #[inline]
    fn register_waker(&self, token: u64, waker: &Waker) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            // Register waker first to avoid missing a race.
            cell.completion_waker.register(waker);

            if cell_gen > generation {
                // Already stale, no point in waiting or updating.
                return;
            }

            if cell_gen < generation {
                if state == slot::SlotState::Idle {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightWaiting)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        // Successfully initialized to new generation WAITING state.
                        // Re-check for fast completion.
                        let current_after = cell.load_core_state(Ordering::Acquire);
                        let state_after = current_after.state();
                        let gen_after = current_after.generation();
                        if state_after == slot::SlotState::InFlightReady && gen_after == generation
                        {
                            waker.wake_by_ref();
                        }
                        return;
                    } else {
                        continue;
                    }
                } else {
                    // Slot not yet IDLE, cannot upgrade.
                    return;
                }
            }

            // cell_gen == generation path.
            // Check for fast completion.
            let current_after = cell.load_core_state(Ordering::Acquire);
            let state_after = current_after.state();
            let gen_after = current_after.generation();
            if state_after == slot::SlotState::InFlightReady && gen_after == generation {
                waker.wake_by_ref();
            }
            return;
        }
    }

    #[inline]
    fn mark_waiting(&self, token: u64) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_generation = current.generation();

            if cell_generation > generation {
                // Stale request, slot already repurposed for a newer op.
                return;
            }

            if cell_generation < generation {
                if state == slot::SlotState::Idle {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightWaiting)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                } else {
                    // Cannot upgrade yet.
                    return;
                }
            } else {
                // cell_generation == generation
                if state == slot::SlotState::InFlightReady {
                    // Fast completion happened, leave as READY.
                    return;
                }

                match state {
                    slot::SlotState::Idle
                    | slot::SlotState::InFlightOrphaned
                    | slot::SlotState::InFlightWaiting
                    | slot::SlotState::ReservedValue => {
                        if cell
                            .core_state
                            .compare_exchange(
                                current,
                                current
                                    .with_state(slot::SlotState::InFlightWaiting)
                                    .with_generation(generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            return;
                        }
                    }
                    slot::SlotState::Finalizing => {
                        return;
                    }
                    slot::SlotState::Reserved | slot::SlotState::InFlightReady => return,
                }
            }
        }
    }

    #[inline]
    fn mark_orphaned(&self, token: u64) {
        let (idx, generation) = decode_completion_token(token);
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            match state {
                slot::SlotState::InFlightWaiting => {
                    if cell_gen != generation {
                        return;
                    }
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightOrphaned)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                slot::SlotState::InFlightReady => {
                    if cell_gen == generation {
                        if cell
                            .core_state
                            .compare_exchange(
                                current,
                                current
                                    .with_state(slot::SlotState::Idle)
                                    .with_generation(generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            cell.completion_with_data(|payload_cell, detail_cell| {
                                let _ = payload_cell.take();
                                let _ = detail_cell.take();
                            });
                            return;
                        }
                    } else {
                        // Stale READY record, ignore.
                        return;
                    }
                }
                slot::SlotState::Finalizing => continue,
                _ => return,
            }
        }
    }

    #[inline]
    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8 {
        let current = self.slots[idx].load_core_state(Ordering::Acquire);
        match current.state() {
            slot::SlotState::Idle => CELL_STATE_IDLE,
            slot::SlotState::InFlightWaiting => CELL_STATE_WAITING,
            slot::SlotState::InFlightReady => CELL_STATE_READY,
            slot::SlotState::InFlightOrphaned => CELL_STATE_ORPHANED,
            slot::SlotState::Finalizing => CELL_STATE_BUSY,
            slot::SlotState::Reserved => CELL_STATE_IDLE,
            slot::SlotState::ReservedValue => CELL_STATE_IDLE,
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
pub fn event_res_to_io<R: CompletionValue>(res: i32) -> io::Result<R> {
    R::from_event_res(res)
}

pub trait Driver: 'static {
    type Op: PlatformOp;
    type Raw: RawHandleMeta;
    type Sidecar: SlotSidecar;
    type Completion: CompletionValue;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)>;

    fn slot_table(
        &self,
    ) -> std::sync::Arc<slot::SlotTable<Self::Op, Self::Sidecar, Self::Completion>>;

    fn detached_cancel_table(&self) -> std::sync::Arc<slot::DetachedCancelTable>;

    fn drain_cancel_requests(&mut self) {
        let shared = self.slot_table();
        let cancel_table = self.detached_cancel_table();
        let word_count = cancel_table.cancel_word_count();
        for word_idx in 0..word_count {
            let mut bits = cancel_table.take_cancel_word(word_idx);
            while bits != 0 {
                let bit_idx = bits.trailing_zeros() as usize;
                bits &= bits - 1;

                let user_data = word_idx * 64 + bit_idx;
                let Some((generation, state)) = shared.slot_snapshot(user_data) else {
                    continue;
                };
                if cancel_table.cancel_generation(user_data) == generation as u64
                    && is_runnable_state(state)
                {
                    self.cancel_op(user_data);
                }
            }
        }
    }

    fn slot_set_payload(&mut self, user_data: usize, payload: slot::ErasedPayload);

    fn slot_take_payload(&mut self, user_data: usize) -> Option<slot::ErasedPayload>;

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (io::Error, SubmitStatus)>>;

    fn submit_queue(&mut self) -> io::Result<()>;

    fn wait(&mut self) -> io::Result<()>;

    fn process_completions(&mut self);

    fn completion_queue(&self) -> SharedCompletionQueue;

    fn completion_table(&self) -> SharedCompletionTable<Self::Completion>;

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

    fn try_take_completion(&mut self, token: u64) -> PollRecordResult<Self::Completion> {
        self.completion_table().try_take(token)
    }

    fn try_take_completion_record(&mut self, token: u64) -> PollRecordResult<Self::Completion> {
        self.completion_table().try_take_record(token)
    }

    fn register_completion_waker(&mut self, token: u64, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    fn cancel_op(&mut self, user_data: usize);

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()>;

    fn register_files<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, Self::Raw>>,
    ) -> io::Result<Vec<IoFd>>;

    fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()>;

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()>;

    fn wake(&mut self) -> io::Result<()>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitStatus {
    /// Operation successfully submitted or queued. It *will* eventually produce
    /// a completion result in the `CompletionTable`.
    InFlight,
    /// Operation failed synchronously and no completion result will be produced.
    Void,
}

#[derive(Default)]
pub struct SubmitBinder;

impl SubmitBinder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    #[inline]
    pub fn ok(self, poll: Poll<()>) -> Outcome<Result<Poll<()>, (io::Error, SubmitStatus)>> {
        Outcome(Ok(poll))
    }

    #[inline]
    pub fn err(
        self,
        err: io::Error,
        status: SubmitStatus,
    ) -> Outcome<Result<Poll<()>, (io::Error, SubmitStatus)>> {
        Outcome(Err((err, status)))
    }
}

#[cfg(feature = "test-hooks")]
pub mod test_hooks {
    pub trait DriverTestHooks {
        fn debug_chunk_register_attempts(&self) -> u64;
    }
}
