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
    RioWake = 3,
}

impl CompletionControlKind {
    #[inline]
    fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(Self::Waker),
            2 => Some(Self::Cancel),
            3 => Some(Self::RioWake),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTokenClass {
    User(OpToken),
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
pub struct OpToken {
    index: usize,
    generation: u32,
}

impl OpToken {
    #[inline]
    pub fn new(index: usize, generation: u32) -> Self {
        assert!(
            index < CONTROL_TOKEN_INDEX as usize,
            "completion slot index exceeds encodable user token range"
        );
        Self { index, generation }
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.index
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub const fn parts(self) -> (usize, u32) {
        (self.index, self.generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionToken(u64);

impl CompletionToken {
    #[inline]
    pub fn user(token: OpToken) -> Self {
        let (index, generation) = token.parts();
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
    pub const fn rio_wake(id: u16) -> Self {
        Self::internal(CompletionControlKind::RioWake, id)
    }

    #[inline]
    pub fn classify(self) -> CompletionTokenClass {
        let index = (self.0 & 0xffff_ffff) as u32;
        if index != CONTROL_TOKEN_INDEX {
            return CompletionTokenClass::User(OpToken::new(index as usize, (self.0 >> 32) as u32));
        }

        let kind = (self.0 >> CONTROL_TOKEN_KIND_SHIFT) as u16;
        let id = ((self.0 >> CONTROL_TOKEN_ID_SHIFT) & 0xffff) as u16;
        match CompletionControlKind::from_raw(kind) {
            Some(kind) => CompletionTokenClass::Control { kind, id },
            None => CompletionTokenClass::UnknownControl { kind, id },
        }
    }

    #[inline]
    pub fn op_token(self) -> Option<OpToken> {
        match self.classify() {
            CompletionTokenClass::User(token) => Some(token),
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
    pub target: OpToken,
    pub mode: CancelMode,
}

impl CancelRequest {
    #[inline]
    pub const fn new(target: OpToken, mode: CancelMode) -> Self {
        Self { target, mode }
    }

    #[inline]
    pub const fn user_visible(target: OpToken) -> Self {
        Self::new(target, CancelMode::UserVisible)
    }

    #[inline]
    pub const fn abandon(target: OpToken) -> Self {
        Self::new(target, CancelMode::Abandon)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelSubmitOutcome {
    Submitted,
    Queued,
    NotFound,
    NoHandle,
    AlreadyComplete,
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
    pub completion_rejected: u64,
    pub internal_unknown: u64,
    pub orphan_cleanup_error: u64,
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

    #[inline]
    pub fn inc_completion_rejected(&mut self) {
        self.completion_rejected = self.completion_rejected.saturating_add(1);
    }

    #[inline]
    pub fn inc_internal_unknown(&mut self) {
        self.internal_unknown = self.internal_unknown.saturating_add(1);
    }

    #[inline]
    pub fn inc_orphan_cleanup_error(&mut self) {
        self.orphan_cleanup_error = self.orphan_cleanup_error.saturating_add(1);
    }
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

impl CompletionAnomaly {
    #[inline]
    pub fn unknown_control(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            reason: CompletionAnomalyReason::UnknownControlToken,
        }
    }

    #[inline]
    pub fn unknown_slot(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: None,
            state: None,
            reason: CompletionAnomalyReason::UnknownSlot,
        }
    }

    #[inline]
    pub fn stale(
        token: CompletionToken,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(expected_generation),
            actual_generation: Some(actual_generation),
            state: Some(state),
            reason: CompletionAnomalyReason::StaleGeneration,
        }
    }

    #[inline]
    pub fn non_active(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: Some(state),
            reason: CompletionAnomalyReason::NonActiveSlot,
        }
    }

    #[inline]
    pub fn corrupt(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: Some(state),
            reason: CompletionAnomalyReason::SlotCorruption,
        }
    }
}

pub struct CompletionCleanup {
    action: Box<dyn FnOnce() + Send + 'static>,
}

impl CompletionCleanup {
    #[inline]
    pub fn new(action: impl FnOnce() + Send + 'static) -> Self {
        Self {
            action: Box::new(action),
        }
    }

    #[inline]
    fn run(self) {
        (self.action)();
    }
}

impl std::fmt::Debug for CompletionCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionCleanup").finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct CompletionCleanupGuard {
    cleanup: Option<CompletionCleanup>,
}

impl CompletionCleanupGuard {
    #[inline]
    pub fn new(cleanup: CompletionCleanup) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    #[inline]
    pub fn none() -> Self {
        Self::default()
    }

    #[inline]
    pub fn is_armed(&self) -> bool {
        self.cleanup.is_some()
    }

    #[inline]
    pub fn disarm(&mut self) -> bool {
        self.cleanup.take().is_some()
    }
}

impl Drop for CompletionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.run();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionRecordKind {
    User,
    Lost(CompletionAnomaly),
}

impl Default for CompletionRecordKind {
    #[inline]
    fn default() -> Self {
        Self::User
    }
}

pub struct CompletionSidecar<UP, E, R = usize> {
    pub token: OpToken,
    pub res: i32,
    pub flags: u32,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
    pub cleanup: CompletionCleanupGuard,
}

pub struct CompletionPacket<UP, E, R = usize> {
    pub event: CompletionEvent,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
    pub cleanup: CompletionCleanupGuard,
    pub record_kind: CompletionRecordKind,
}

impl<UP, E, R> CompletionPacket<UP, E, R> {
    #[inline]
    pub fn new(
        event: CompletionEvent,
        payload: Option<UP>,
        detail: Option<DriverResult<R, E>>,
    ) -> Self {
        Self::with_cleanup(
            event,
            payload,
            detail,
            CompletionCleanupGuard::default(),
            CompletionRecordKind::User,
        )
    }

    #[inline]
    pub fn with_cleanup(
        event: CompletionEvent,
        payload: Option<UP>,
        detail: Option<DriverResult<R, E>>,
        cleanup: CompletionCleanupGuard,
        record_kind: CompletionRecordKind,
    ) -> Self {
        Self {
            event,
            payload,
            detail,
            cleanup,
            record_kind,
        }
    }

    #[inline]
    pub fn user(
        token: OpToken,
        res: i32,
        flags: u32,
        payload: Option<UP>,
        detail: Option<DriverResult<R, E>>,
    ) -> Self {
        Self::user_with_cleanup(
            token,
            res,
            flags,
            payload,
            detail,
            CompletionCleanupGuard::default(),
        )
    }

    #[inline]
    pub fn user_with_cleanup(
        token: OpToken,
        res: i32,
        flags: u32,
        payload: Option<UP>,
        detail: Option<DriverResult<R, E>>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self::with_cleanup(
            CompletionEvent {
                token: CompletionToken::user(token),
                res,
                flags,
            },
            payload,
            detail,
            cleanup,
            CompletionRecordKind::User,
        )
    }

    #[inline]
    pub fn lost(
        event: CompletionEvent,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self::with_cleanup(
            event,
            None,
            None,
            cleanup,
            CompletionRecordKind::Lost(anomaly),
        )
    }
}

impl<UP, E, R> From<CompletionSidecar<UP, E, R>> for CompletionPacket<UP, E, R> {
    #[inline]
    fn from(sidecar: CompletionSidecar<UP, E, R>) -> Self {
        Self::user_with_cleanup(
            sidecar.token,
            sidecar.res,
            sidecar.flags,
            sidecar.payload,
            sidecar.detail,
            sidecar.cleanup,
        )
    }
}

/// Unified completion event produced by platform drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEvent {
    /// Completion token (generation + slot index, or backend control token).
    pub token: CompletionToken,
    /// Completion result code. Non-negative for success, negative for error.
    pub res: i32,
    /// Platform-specific completion flags.
    pub flags: u32,
}

impl CompletionEvent {
    #[inline]
    pub const fn raw_token(self) -> u64 {
        self.token.raw()
    }
}

pub type SharedCompletionQueue = Arc<SegQueue<CompletionEvent>>;
pub type SharedCompletionTable<UP, E, R = usize> = Arc<dyn CompletionAccess<UP, E, R>>;

pub struct CompletionRecord<UP, E, R = usize> {
    pub event: CompletionEvent,
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
    pub cleanup: CompletionCleanupGuard,
    pub record_kind: CompletionRecordKind,
}

impl<UP, E, R> CompletionRecord<UP, E, R> {
    #[inline]
    pub fn disarm_cleanup(&mut self) -> bool {
        self.cleanup.disarm()
    }
}

impl<UP, E, R> From<CompletionPacket<UP, E, R>> for CompletionRecord<UP, E, R> {
    #[inline]
    fn from(packet: CompletionPacket<UP, E, R>) -> Self {
        Self {
            event: packet.event,
            payload: packet.payload,
            detail: packet.detail,
            cleanup: packet.cleanup,
            record_kind: packet.record_kind,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCompletionOutcome {
    Recorded,
    OrphanedDropped,
    Missing(CompletionAnomaly),
    Stale(CompletionAnomaly),
    NonActive(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

pub enum RecordCompletionResult<UP, E, R = usize> {
    Recorded,
    Rejected {
        outcome: RecordCompletionOutcome,
        packet: CompletionPacket<UP, E, R>,
    },
}

impl<UP, E, R> RecordCompletionResult<UP, E, R> {
    #[inline]
    pub fn outcome(&self) -> &RecordCompletionOutcome {
        match self {
            Self::Recorded => &RecordCompletionOutcome::Recorded,
            Self::Rejected { outcome, .. } => outcome,
        }
    }

    #[inline]
    pub fn into_outcome(self) -> RecordCompletionOutcome {
        match self {
            Self::Recorded => RecordCompletionOutcome::Recorded,
            Self::Rejected { outcome, .. } => outcome,
        }
    }
}

impl DriverCompletionDiagnostics {
    #[inline]
    pub fn record_completion_outcome(&mut self, outcome: &RecordCompletionOutcome) {
        if !matches!(outcome, RecordCompletionOutcome::Recorded) {
            self.inc_completion_rejected();
        }
        match outcome {
            RecordCompletionOutcome::Recorded => self.inc_user_completed(),
            RecordCompletionOutcome::OrphanedDropped => self.inc_user_orphan_completed(),
            RecordCompletionOutcome::Missing(_) | RecordCompletionOutcome::NonActive(_) => {
                self.inc_unknown_completion();
            }
            RecordCompletionOutcome::Stale(_) => self.inc_stale_completion(),
            RecordCompletionOutcome::Corrupt(_) => self.inc_slot_corruption(),
        }
    }

    #[inline]
    pub fn record_completion_result<UP, E, R>(
        &mut self,
        result: &RecordCompletionResult<UP, E, R>,
    ) {
        self.record_completion_outcome(result.outcome());
    }
}

#[inline]
fn rejected_completion<UP, E, R>(
    outcome: RecordCompletionOutcome,
    packet: CompletionPacket<UP, E, R>,
) -> RecordCompletionResult<UP, E, R> {
    RecordCompletionResult::Rejected { outcome, packet }
}

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<UP, E, R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<UP, E, R>),
    /// Operation was explicitly marked lost and the waiter was woken.
    ReadyLost(CompletionAnomaly),
    /// Operation is still in flight.
    Pending,
    /// Operation lost because the slot has been recycled for a newer generation.
    Stale(CompletionAnomaly),
    /// Operation lost or could not be associated with a valid active slot.
    Lost(CompletionAnomaly),
}

pub trait CompletionAccess<UP, E, R = usize>: Send + Sync {
    fn record_completion(
        &self,
        packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R>;

    fn record_lost_completion(
        &self,
        event: CompletionEvent,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> RecordCompletionResult<UP, E, R> {
        self.record_completion(CompletionPacket::lost(event, anomaly, cleanup))
    }

    fn try_take_record(&self, token: CompletionToken) -> PollRecordResult<UP, E, R>;

    #[inline]
    fn try_take(&self, token: CompletionToken) -> PollRecordResult<UP, E, R> {
        self.try_take_record(token)
    }

    fn register_waker(&self, token: CompletionToken, waker: &Waker);

    fn mark_waiting(&self, token: CompletionToken);

    fn mark_orphaned(&self, token: CompletionToken);

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;
}

#[inline]
pub(crate) fn decode_completion_token(token: CompletionToken) -> Option<(usize, u32)> {
    token.op_token().map(OpToken::parts)
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
    fn record_completion(
        &self,
        mut packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R> {
        let token = packet.event.token;
        let Some(op_token) = token.op_token() else {
            return rejected_completion(
                RecordCompletionOutcome::Missing(CompletionAnomaly::unknown_control(token)),
                packet,
            );
        };
        let (idx, generation) = op_token.parts();
        if idx >= self.slots.len() {
            return rejected_completion(
                RecordCompletionOutcome::Missing(CompletionAnomaly::unknown_slot(
                    token, idx, generation,
                )),
                packet,
            );
        }
        let cell = &self.slots[idx];
        let should_note_ready;

        let ready_from = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            if generation < cell_gen {
                return rejected_completion(
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    )),
                    packet,
                );
            }
            if generation > cell_gen && state != slot::SlotState::Idle {
                return rejected_completion(
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    )),
                    packet,
                );
            }

            match state {
                slot::SlotState::Idle if generation > cell_gen => {
                    should_note_ready = true;
                    break current;
                }
                slot::SlotState::Reserved
                    if matches!(packet.record_kind, CompletionRecordKind::Lost(_)) =>
                {
                    should_note_ready = true;
                    break current;
                }
                slot::SlotState::InFlightWaiting => {
                    should_note_ready = true;
                    break current;
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightReady
                | slot::SlotState::ReservedValue => {
                    return rejected_completion(
                        RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                            token, idx, generation, state,
                        )),
                        packet,
                    );
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
                            return rejected_completion(
                                RecordCompletionOutcome::OrphanedDropped,
                                packet,
                            );
                        }
                    } else {
                        return rejected_completion(
                            RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                                token, idx, generation, cell_gen, state,
                            )),
                            packet,
                        );
                    }
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        if should_note_ready {
            self.note_ready_completion();
        }
        cell.completion_with_record_data(|payload_cell, detail_cell, cleanup_cell, kind_cell| {
            *payload_cell = packet.payload.take();
            *detail_cell = packet.detail.take();
            *cleanup_cell = std::mem::take(&mut packet.cleanup);
            *kind_cell = packet.record_kind;
        });
        cell.completion_res
            .store(packet.event.res, Ordering::Release);
        cell.completion_flags
            .store(packet.event.flags, Ordering::Release);

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
                    return RecordCompletionResult::Recorded;
                }

                // If we reached here, someone else either:
                // 1. already set it to InFlightReady (which is fine, we just discard our duplicate data)
                // 2. recycled the slot (generation mismatch)
                let (stored_payload, stored_detail, stored_cleanup, stored_kind) = cell
                    .completion_with_record_data(
                        |payload_cell, detail_cell, cleanup_cell, kind_cell| {
                            (
                                payload_cell.take(),
                                detail_cell.take(),
                                std::mem::take(cleanup_cell),
                                *kind_cell,
                            )
                        },
                    );
                packet.payload = stored_payload;
                packet.detail = stored_detail;
                packet.cleanup = stored_cleanup;
                packet.record_kind = stored_kind;

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
                    return rejected_completion(RecordCompletionOutcome::OrphanedDropped, packet);
                } else if should_note_ready {
                    self.clear_ready_completion();
                }
                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() != generation {
                    return rejected_completion(
                        RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                            token,
                            idx,
                            generation,
                            cur.generation(),
                            cur.state(),
                        )),
                        packet,
                    );
                }
                return rejected_completion(
                    RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                        token,
                        idx,
                        generation,
                        cur.state(),
                    )),
                    packet,
                );
            }
        }

        cell.completion_waker.wake();
        RecordCompletionResult::Recorded
    }

    #[inline]
    fn try_take_record(&self, token: CompletionToken) -> PollRecordResult<UP, E, R> {
        let Some(op_token) = token.op_token() else {
            return PollRecordResult::Lost(CompletionAnomaly::unknown_control(token));
        };
        let (idx, generation) = op_token.parts();
        if idx >= self.slots.len() {
            return PollRecordResult::Lost(CompletionAnomaly::unknown_slot(token, idx, generation));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        // If the cell's generation is strictly greater than ours, we are stale.
        if cell_gen > generation {
            return PollRecordResult::Stale(CompletionAnomaly::stale(
                token, idx, generation, cell_gen, state,
            ));
        }

        if cell_gen != generation {
            return PollRecordResult::Pending;
        }

        if state != slot::SlotState::InFlightReady {
            return match state {
                slot::SlotState::InFlightWaiting => PollRecordResult::Pending,
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::Finalizing
                | slot::SlotState::ReservedValue => PollRecordResult::Lost(
                    CompletionAnomaly::non_active(token, idx, generation, state),
                ),
                slot::SlotState::InFlightReady => unreachable!(),
            };
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
        let (payload, detail, cleanup, record_kind) = cell.completion_with_record_data(
            |payload_cell, detail_cell, cleanup_cell, kind_cell| {
                let kind = *kind_cell;
                *kind_cell = CompletionRecordKind::User;
                (
                    payload_cell.take(),
                    detail_cell.take(),
                    std::mem::take(cleanup_cell),
                    kind,
                )
            },
        );
        if let CompletionRecordKind::Lost(anomaly) = record_kind {
            drop(payload);
            drop(detail);
            drop(cleanup);
            return PollRecordResult::ReadyLost(anomaly);
        }
        PollRecordResult::Ready(CompletionRecord {
            event: CompletionEvent {
                token,
                res: cell.completion_res.load(Ordering::Acquire),
                flags: cell.completion_flags.load(Ordering::Acquire),
            },
            payload,
            detail,
            cleanup,
            record_kind,
        })
    }

    #[inline]
    fn register_waker(&self, token: CompletionToken, waker: &Waker) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
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
    fn mark_waiting(&self, token: CompletionToken) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
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
    fn mark_orphaned(&self, token: CompletionToken) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
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
                        cell.clear_completion_record_data();
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
