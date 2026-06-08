use crate::{DriverCoreError, DriverError, DriverResult};

use diagweave::prelude::*;

mod table;
mod types;

use crate::slot::{self, CheckedSlotView, SlotView};
pub use table::{
    CELL_STATE_BUSY, CELL_STATE_IDLE, CELL_STATE_ORPHANED, CELL_STATE_READY, CELL_STATE_WAITING,
    CompletionAccess, PollRecordResult, SharedCompletionTable,
};
pub use types::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionCleanup,
    CompletionCleanupGuard, DriverCompletionDiagnostics, DriverCompletionDiagnosticsSnapshot,
    RecordCompletionOutcome, RecordCompletionResult,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCompletion {
    pub backend: CompletionBackend,
    pub token: CompletionToken,
    pub res: i32,
    pub flags: u32,
}

impl RawCompletion {
    #[inline]
    pub const fn new(
        backend: CompletionBackend,
        token: CompletionToken,
        res: i32,
        flags: u32,
    ) -> Self {
        Self {
            backend,
            token,
            res,
            flags,
        }
    }

    #[inline]
    pub const fn event(self) -> CompletionEvent {
        CompletionEvent {
            token: self.token,
            res: self.res,
            flags: self.flags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDispatch {
    User { token: OpToken, raw: RawCompletion },
    Waker { id: u16, raw: RawCompletion },
    Cancel { id: u16, raw: RawCompletion },
    RioWake { id: u16, raw: RawCompletion },
    Unknown { raw: RawCompletion },
}

pub enum RoutedSlotCompletion<'a, Spec: slot::SlotSpec> {
    Waiting(slot::Slot<'a, slot::InFlightWaiting, Spec>),
    Orphaned(slot::Slot<'a, slot::InFlightOrphaned, Spec>),
    Missing(CompletionAnomaly),
    Empty(CompletionAnomaly),
    Stale(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

impl<'a, Spec: slot::SlotSpec> RoutedSlotCompletion<'a, Spec> {
    #[inline]
    pub fn anomaly(&self) -> Option<&CompletionAnomaly> {
        match self {
            Self::Waiting(_) | Self::Orphaned(_) => None,
            Self::Missing(anomaly)
            | Self::Empty(anomaly)
            | Self::Stale(anomaly)
            | Self::Corrupt(anomaly) => Some(anomaly),
        }
    }
}

#[inline]
pub fn dispatch_raw_completion(
    backend: CompletionBackend,
    raw_token: u64,
    res: i32,
    flags: u32,
) -> CompletionDispatch {
    let token = CompletionToken::from_raw(raw_token);
    let raw = RawCompletion::new(backend, token, res, flags);
    match token.classify() {
        CompletionTokenClass::User(token) => CompletionDispatch::User { token, raw },
        CompletionTokenClass::Control {
            kind: CompletionControlKind::Waker,
            id,
        } => CompletionDispatch::Waker { id, raw },
        CompletionTokenClass::Control {
            kind: CompletionControlKind::Cancel,
            id,
        } => CompletionDispatch::Cancel { id, raw },
        CompletionTokenClass::Control {
            kind: CompletionControlKind::RioWake,
            id,
        } => CompletionDispatch::RioWake { id, raw },
        CompletionTokenClass::UnknownControl { .. } => CompletionDispatch::Unknown { raw },
    }
}

impl CompletionAnomaly {
    #[inline]
    pub fn with_raw_completion(self, raw: RawCompletion) -> Self {
        self.with_backend(raw.backend).with_event(raw.event())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitTokenContext {
    pub op_token: OpToken,
    pub completion_token: CompletionToken,
}

impl SubmitTokenContext {
    #[inline]
    pub fn user(op_token: OpToken) -> Self {
        Self {
            op_token,
            completion_token: CompletionToken::user(op_token),
        }
    }

    #[inline]
    pub const fn new(op_token: OpToken, completion_token: CompletionToken) -> Self {
        Self {
            op_token,
            completion_token,
        }
    }
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

#[inline]
pub fn route_checked_slot_completion<'a, Spec: slot::SlotSpec>(
    raw: RawCompletion,
    view: CheckedSlotView<'a, Spec>,
) -> RoutedSlotCompletion<'a, Spec> {
    let token = raw
        .token
        .op_token()
        .expect("route_checked_slot_completion requires a user completion token");
    let (index, expected_generation) = token.parts();
    match view {
        CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
            RoutedSlotCompletion::Waiting(slot)
        }
        CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
            RoutedSlotCompletion::Orphaned(slot)
        }
        CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
            let snapshot = slot.snapshot();
            RoutedSlotCompletion::Corrupt(
                CompletionAnomaly::corrupt(
                    raw.token,
                    snapshot.index,
                    snapshot.generation,
                    snapshot.state,
                )
                .with_slot_snapshot(snapshot)
                .with_raw_completion(raw),
            )
        }
        CheckedSlotView::Missing { .. } => RoutedSlotCompletion::Missing(
            CompletionAnomaly::unknown_slot(raw.token, index, expected_generation)
                .with_raw_completion(raw),
        ),
        CheckedSlotView::Empty(snapshot) => RoutedSlotCompletion::Empty(
            CompletionAnomaly::non_active(
                raw.token,
                snapshot.index,
                expected_generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw),
        ),
        CheckedSlotView::Stale(snapshot) => RoutedSlotCompletion::Stale(
            CompletionAnomaly::stale(
                raw.token,
                snapshot.index,
                expected_generation,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw),
        ),
        CheckedSlotView::Corrupt(snapshot) => RoutedSlotCompletion::Corrupt(
            CompletionAnomaly::corrupt(
                raw.token,
                snapshot.index,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw),
        ),
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

#[inline]
fn run_rejected_cleanup<UP, E, R>(
    diagnostics: &mut DriverCompletionDiagnostics,
    mut packet: CompletionPacket<UP, E, R>,
) {
    run_completion_cleanup(diagnostics, &mut packet.cleanup);
    drop(packet);
}

#[inline]
pub fn run_completion_cleanup(
    diagnostics: &mut DriverCompletionDiagnostics,
    cleanup: &mut CompletionCleanupGuard,
) -> bool {
    match cleanup.run() {
        Ok(ran) => ran,
        Err(_) => {
            diagnostics.inc_orphan_cleanup_error();
            false
        }
    }
}

#[inline]
pub fn record_completion_anomaly(
    diagnostics: &mut DriverCompletionDiagnostics,
    anomaly: &CompletionAnomaly,
) {
    match anomaly.reason {
        CompletionAnomalyReason::UnknownSlot
        | CompletionAnomalyReason::UnknownControlToken
        | CompletionAnomalyReason::NonActiveSlot => diagnostics.inc_unknown_completion(),
        CompletionAnomalyReason::StaleGeneration => diagnostics.inc_stale_completion(),
        CompletionAnomalyReason::SlotCorruption => diagnostics.inc_slot_corruption(),
    }
}

#[inline]
pub fn record_user_completion<UP, E, R>(
    table: &SharedCompletionTable<UP, E, R>,
    diagnostics: &mut DriverCompletionDiagnostics,
    packet: CompletionPacket<UP, E, R>,
) -> RecordCompletionOutcome
where
    UP: Send,
    E: Send,
    R: Send,
{
    match table.record_completion(packet) {
        RecordCompletionResult::Recorded => {
            let outcome = RecordCompletionOutcome::Recorded;
            diagnostics.record_completion_outcome(&outcome);
            outcome
        }
        RecordCompletionResult::Rejected { outcome, packet } => {
            diagnostics.record_completion_outcome(&outcome);
            run_rejected_cleanup(diagnostics, packet);
            outcome
        }
    }
}

#[inline]
pub fn record_lost_completion<UP, E, R>(
    table: &SharedCompletionTable<UP, E, R>,
    diagnostics: &mut DriverCompletionDiagnostics,
    event: CompletionEvent,
    anomaly: CompletionAnomaly,
    cleanup: CompletionCleanupGuard,
) -> RecordCompletionOutcome
where
    UP: Send,
    E: Send,
    R: Send,
{
    record_user_completion(
        table,
        diagnostics,
        CompletionPacket::lost(event, anomaly, cleanup),
    )
}

#[inline]
pub fn discard_internal_completion(diagnostics: &mut DriverCompletionDiagnostics) {
    diagnostics.inc_internal_unknown();
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
