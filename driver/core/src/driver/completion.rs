use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::slot;
use crate::{DriverErrorKind, DriverResult, driver_os_error};
use crossbeam_queue::SegQueue;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

pub trait CompletionValue: Send {
    fn from_event_res(res: i32) -> DriverResult<Self>
    where
        Self: Sized;
}

impl CompletionValue for usize {
    #[inline]
    fn from_event_res(res: i32) -> DriverResult<Self> {
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(driver_os_error(
                DriverErrorKind::System,
                "driver-core/completion",
                -res,
                "completion reported OS error",
            ))
        }
    }
}

pub struct CompletionSidecar<UP, R = usize> {
    pub user_data: usize,
    pub generation: u32,
    pub res: i32,
    pub flags: u32,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R>>,
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
pub type SharedCompletionTable<UP, R = usize> = Arc<dyn CompletionAccess<UP, R>>;

pub struct CompletionRecord<UP, R = usize> {
    pub event: CompletionEvent,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R>>,
}

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<UP, R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<UP, R>),
    /// Operation is still in flight.
    Pending,
    /// Operation lost because the slot has been recycled for a newer generation.
    Stale,
}

pub trait CompletionAccess<UP, R = usize>: Send + Sync {
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        payload: Option<UP>,
        detail: Option<DriverResult<R>>,
    );

    fn try_take_record(&self, token: u64) -> PollRecordResult<UP, R>;

    #[inline]
    fn try_take(&self, token: u64) -> PollRecordResult<UP, R> {
        self.try_take_record(token)
    }

    fn register_waker(&self, token: u64, waker: &Waker);

    fn mark_waiting(&self, token: u64);

    fn mark_orphaned(&self, token: u64);

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;
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
pub fn event_res_to_result<R: CompletionValue>(res: i32) -> DriverResult<R> {
    R::from_event_res(res)
}

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

impl<Op: PlatformOp, UP: Send, S: SlotSidecar, R: Send> CompletionAccess<UP, R>
    for slot::SlotTable<Op, UP, S, R>
{
    #[inline]
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        mut payload: Option<UP>,
        mut detail: Option<DriverResult<R>>,
    ) {
        let (idx, generation) = decode_completion_token(event.user_data);
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];
        let should_note_ready;

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
                    should_note_ready = state != slot::SlotState::InFlightReady;
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
                                    .with_generation(generation.wrapping_add(1)),
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
                        return;
                    }
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        if should_note_ready {
            self.note_ready_completion();
        }
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

                // If we reached here, someone else either:
                // 1. already set it to InFlightReady (which is fine, we just discard our duplicate data)
                // 2. recycled the slot (generation mismatch)
                cell.completion_with_data(|payload_cell, detail_cell| {
                    let _ = payload_cell.take();
                    let _ = detail_cell.take();
                });

                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() == generation
                    && cur.state() == slot::SlotState::InFlightOrphaned
                {
                    if should_note_ready {
                        self.clear_ready_completion();
                    }
                    let _ = cell.core_state.compare_exchange(
                        cur,
                        cur.with_state(slot::SlotState::Idle)
                            .with_generation(generation.wrapping_add(1)),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                } else if should_note_ready {
                    self.clear_ready_completion();
                }
                return;
            }
        }

        cell.completion_waker.wake();
    }

    #[inline]
    fn try_take_record(&self, token: u64) -> PollRecordResult<UP, R> {
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
                    .with_generation(generation.wrapping_add(1)),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return PollRecordResult::Pending;
        }

        self.clear_ready_completion();
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

            if cell_gen > generation {
                return;
            }

            // Register waker. AtomicWaker handles races with concurrent wake().
            cell.completion_waker.register(waker);

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
                        // Check for fast completion.
                        let current_after = cell.load_core_state(Ordering::Acquire);
                        if current_after.state() == slot::SlotState::InFlightReady
                            && current_after.generation() == generation
                        {
                            waker.wake_by_ref();
                        }
                        return;
                    } else {
                        continue;
                    }
                } else {
                    return;
                }
            }

            // cell_gen == generation
            let current_after = cell.load_core_state(Ordering::Acquire);
            if current_after.state() == slot::SlotState::InFlightReady
                && current_after.generation() == generation
            {
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
                slot::SlotState::InFlightReady if cell_gen == generation => {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::Idle)
                                .with_generation(generation.wrapping_add(1)),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.clear_ready_completion();
                        cell.completion_with_data(|payload_cell, detail_cell| {
                            let _ = payload_cell.take();
                            let _ = detail_cell.take();
                        });
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
