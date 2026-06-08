use crate::slot;
use crate::{DriverCoreError, DriverError, DriverResult};
use crossbeam_queue::SegQueue;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

use diagweave::prelude::*;

pub trait CompletionValue: Send {
    fn from_event_res<E>(res: i32) -> DriverResult<Self, E>
    where
        Self: Sized,
        E: DriverError;
}

impl CompletionValue for usize {
    #[inline]
    fn from_event_res<E>(res: i32) -> DriverResult<Self, E>
    where
        E: DriverError,
    {
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(E::from_core_report(
                DriverCoreError::System
                    .to_report()
                    .push_ctx("scope", "driver-core/completion")
                    .set_error_code(-res)
                    .attach_note("completion reported OS error"),
            ))
        }
    }
}

const CONTROL_TOKEN_INDEX: u32 = u32::MAX;
const CONTROL_TOKEN_KIND_SHIFT: u32 = 48;
const CONTROL_TOKEN_ID_SHIFT: u32 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CompletionControlKind {
    Waker = 1,
    Cancel = 2,
}

impl CompletionControlKind {
    #[inline]
    fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(Self::Waker),
            2 => Some(Self::Cancel),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTokenClass {
    User {
        index: usize,
        generation: u32,
    },
    Control {
        kind: CompletionControlKind,
        id: u16,
    },
    UnknownControl {
        kind: u16,
        id: u16,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionToken(u64);

impl CompletionToken {
    #[inline]
    pub fn user(index: usize, generation: u32) -> Self {
        assert!(
            index < CONTROL_TOKEN_INDEX as usize,
            "completion slot index exceeds encodable user token range"
        );
        Self(((generation as u64) << 32) | (index as u32 as u64))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn internal(kind: CompletionControlKind, id: u16) -> Self {
        Self(
            ((kind as u64) << CONTROL_TOKEN_KIND_SHIFT)
                | ((id as u64) << CONTROL_TOKEN_ID_SHIFT)
                | CONTROL_TOKEN_INDEX as u64,
        )
    }

    #[inline]
    pub const fn waker(id: u16) -> Self {
        Self::internal(CompletionControlKind::Waker, id)
    }

    #[inline]
    pub const fn cancel(id: u16) -> Self {
        Self::internal(CompletionControlKind::Cancel, id)
    }

    #[inline]
    pub fn classify(self) -> CompletionTokenClass {
        let index = (self.0 & 0xffff_ffff) as u32;
        if index != CONTROL_TOKEN_INDEX {
            return CompletionTokenClass::User {
                index: index as usize,
                generation: (self.0 >> 32) as u32,
            };
        }

        let kind = (self.0 >> CONTROL_TOKEN_KIND_SHIFT) as u16;
        let id = ((self.0 >> CONTROL_TOKEN_ID_SHIFT) & 0xffff) as u16;
        match CompletionControlKind::from_raw(kind) {
            Some(kind) => CompletionTokenClass::Control { kind, id },
            None => CompletionTokenClass::UnknownControl { kind, id },
        }
    }

    #[inline]
    pub fn user_parts(self) -> Option<(usize, u32)> {
        match self.classify() {
            CompletionTokenClass::User { index, generation } => Some((index, generation)),
            CompletionTokenClass::Control { .. } | CompletionTokenClass::UnknownControl { .. } => {
                None
            }
        }
    }
}

impl From<u64> for CompletionToken {
    #[inline]
    fn from(value: u64) -> Self {
        Self::from_raw(value)
    }
}

impl From<CompletionToken> for u64 {
    #[inline]
    fn from(value: CompletionToken) -> Self {
        value.raw()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelMode {
    UserVisible,
    Abandon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelRequest {
    pub token: CompletionToken,
    pub mode: CancelMode,
}

impl CancelRequest {
    #[inline]
    pub const fn new(token: CompletionToken, mode: CancelMode) -> Self {
        Self { token, mode }
    }

    #[inline]
    pub const fn user_visible(token: CompletionToken) -> Self {
        Self::new(token, CancelMode::UserVisible)
    }

    #[inline]
    pub const fn abandon(token: CompletionToken) -> Self {
        Self::new(token, CancelMode::Abandon)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverCompletionDiagnostics {
    pub user_completed: u64,
    pub user_orphan_completed: u64,
    pub unknown_completion: u64,
    pub stale_completion: u64,
    pub slot_corruption: u64,
    pub cancel_submitted: u64,
    pub cancel_cqe_ok: u64,
    pub cancel_cqe_enoent: u64,
    pub cancel_cqe_error: u64,
    pub waker_ok: u64,
    pub waker_error: u64,
    pub waker_rebuild: u64,
}

impl DriverCompletionDiagnostics {
    #[inline]
    pub fn inc_user_completed(&mut self) {
        self.user_completed = self.user_completed.saturating_add(1);
    }

    #[inline]
    pub fn inc_user_orphan_completed(&mut self) {
        self.user_orphan_completed = self.user_orphan_completed.saturating_add(1);
    }

    #[inline]
    pub fn inc_unknown_completion(&mut self) {
        self.unknown_completion = self.unknown_completion.saturating_add(1);
    }

    #[inline]
    pub fn inc_stale_completion(&mut self) {
        self.stale_completion = self.stale_completion.saturating_add(1);
    }

    #[inline]
    pub fn inc_slot_corruption(&mut self) {
        self.slot_corruption = self.slot_corruption.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_submitted(&mut self) {
        self.cancel_submitted = self.cancel_submitted.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_cqe_ok(&mut self) {
        self.cancel_cqe_ok = self.cancel_cqe_ok.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_cqe_enoent(&mut self) {
        self.cancel_cqe_enoent = self.cancel_cqe_enoent.saturating_add(1);
    }

    #[inline]
    pub fn inc_cancel_cqe_error(&mut self) {
        self.cancel_cqe_error = self.cancel_cqe_error.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_ok(&mut self) {
        self.waker_ok = self.waker_ok.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_error(&mut self) {
        self.waker_error = self.waker_error.saturating_add(1);
    }

    #[inline]
    pub fn inc_waker_rebuild(&mut self) {
        self.waker_rebuild = self.waker_rebuild.saturating_add(1);
    }
}

pub struct CompletionSidecar<UP, E, R = usize> {
    pub user_data: usize,
    pub generation: u32,
    pub res: i32,
    pub flags: u32,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
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

impl CompletionEvent {
    #[inline]
    pub const fn token(self) -> CompletionToken {
        CompletionToken::from_raw(self.user_data)
    }
}

pub type SharedCompletionQueue = Arc<SegQueue<CompletionEvent>>;
pub type SharedCompletionTable<UP, E, R = usize> = Arc<dyn CompletionAccess<UP, E, R>>;

pub struct CompletionRecord<UP, E, R = usize> {
    pub event: CompletionEvent,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyReason {
    UnknownSlot,
    UnknownControlToken,
    StaleGeneration,
    NonActiveSlot,
    SlotCorruption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionAnomaly {
    pub token: CompletionToken,
    pub index: Option<usize>,
    pub expected_generation: Option<u32>,
    pub actual_generation: Option<u32>,
    pub state: Option<slot::SlotState>,
    pub reason: CompletionAnomalyReason,
}

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<UP, E, R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<UP, E, R>),
    /// Operation is still in flight.
    Pending,
    /// Operation lost because the slot has been recycled for a newer generation.
    Stale,
    /// Operation lost or could not be associated with a valid active slot.
    Lost(CompletionAnomaly),
}

pub trait CompletionAccess<UP, E, R = usize>: Send + Sync {
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        payload: Option<UP>,
        detail: Option<DriverResult<R, E>>,
    );

    fn try_take_record(&self, token: u64) -> PollRecordResult<UP, E, R>;

    #[inline]
    fn try_take(&self, token: u64) -> PollRecordResult<UP, E, R> {
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
    CompletionToken::user(index, generation).raw()
}

#[inline]
pub fn decode_completion_token(token: u64) -> (usize, u32) {
    CompletionToken::from_raw(token)
        .user_parts()
        .unwrap_or((CONTROL_TOKEN_INDEX as usize, 0))
}

#[inline]
pub fn event_res_to_result<R, E>(res: i32) -> DriverResult<R, E>
where
    R: CompletionValue,
    E: DriverError,
{
    R::from_event_res(res)
}

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

impl<Spec, UP: Send, E: Send, R: Send> CompletionAccess<UP, E, R> for slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec<UserPayload = UP, Error = E, Completion = R>,
{
    #[inline]
    fn record_completion_with_data(
        &self,
        event: CompletionEvent,
        mut payload: Option<UP>,
        mut detail: Option<DriverResult<R, E>>,
    ) {
        let Some((idx, generation)) = event.token().user_parts() else {
            return;
        };
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
    fn try_take_record(&self, token: u64) -> PollRecordResult<UP, E, R> {
        let token = CompletionToken::from_raw(token);
        let Some((idx, generation)) = token.user_parts() else {
            return PollRecordResult::Lost(CompletionAnomaly {
                token,
                index: None,
                expected_generation: None,
                actual_generation: None,
                state: None,
                reason: CompletionAnomalyReason::UnknownControlToken,
            });
        };
        if idx >= self.slots.len() {
            return PollRecordResult::Lost(CompletionAnomaly {
                token,
                index: Some(idx),
                expected_generation: Some(generation),
                actual_generation: None,
                state: None,
                reason: CompletionAnomalyReason::UnknownSlot,
            });
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
                user_data: token.raw(),
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
