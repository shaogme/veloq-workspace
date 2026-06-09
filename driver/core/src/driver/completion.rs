use crate::{DriverCoreError, DriverError, DriverResult};

use diagweave::prelude::*;

mod flow;
mod table;
mod types;

use crate::driver::registry::OpRegistry;
use crate::slot::{self, CheckedSlotView, SlotRegistryExt, SlotView};
pub use flow::{
    CompletionBackendHooks, CompletionBackendIngressAction, CompletionControl, CompletionFlowExt,
    CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress, CompletionSource,
    CompletionWritePermit, SyntheticCompletionSource,
};
pub use table::{
    CELL_STATE_BUSY, CELL_STATE_IDLE, CELL_STATE_ORPHANED, CELL_STATE_READY, CELL_STATE_WAITING,
    CompletionAccess, PollRecordResult, SharedCompletionTable,
};
pub use types::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionCleanup,
    CompletionCleanupGuard, CompletionMutationOutcome, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsBackend, DriverCompletionDiagnosticsSnapshot,
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
pub struct UserCompletionEvent {
    token: OpToken,
    raw: RawCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserCompletionEventMismatch {
    pub token: OpToken,
    pub expected: CompletionToken,
    pub actual: CompletionToken,
}

impl UserCompletionEvent {
    #[inline]
    pub fn try_new(
        token: OpToken,
        raw: RawCompletion,
    ) -> Result<Self, UserCompletionEventMismatch> {
        let expected = CompletionToken::user(token);
        if raw.token != expected {
            return Err(UserCompletionEventMismatch {
                token,
                expected,
                actual: raw.token,
            });
        }
        Ok(Self { token, raw })
    }

    #[inline]
    pub fn from_parts(backend: CompletionBackend, token: OpToken, res: i32, flags: u32) -> Self {
        Self {
            token,
            raw: RawCompletion::new(backend, CompletionToken::user(token), res, flags),
        }
    }

    #[inline]
    fn from_classified(token: OpToken, raw: RawCompletion) -> Self {
        debug_assert_eq!(raw.token, CompletionToken::user(token));
        Self { token, raw }
    }

    #[inline]
    pub const fn token(self) -> OpToken {
        self.token
    }

    #[inline]
    pub const fn raw(self) -> RawCompletion {
        self.raw
    }

    #[inline]
    pub const fn completion_token(self) -> CompletionToken {
        self.raw.token
    }

    #[inline]
    pub const fn res(self) -> i32 {
        self.raw.res
    }

    #[inline]
    pub const fn flags(self) -> u32 {
        self.raw.flags
    }

    #[inline]
    pub const fn event(self) -> CompletionEvent {
        self.raw.event()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDispatch {
    User {
        event: UserCompletionEvent,
    },
    Waker {
        id: u16,
        raw: RawCompletion,
    },
    Cancel {
        id: CancelCompletionId,
        raw: RawCompletion,
    },
    RioWake {
        id: u16,
        raw: RawCompletion,
    },
    Unknown {
        envelope: CompletionEnvelope,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentity {
    User(OpToken),
    Waker(u16),
    Cancel(CancelCompletionId),
    RioWake(u16),
    UnknownControl {
        kind: u16,
        id: u16,
    },
    BackendContext {
        backend: CompletionBackend,
        raw_context: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentitySource {
    UserToken,
    ControlToken,
    BackendContext,
    SidecarTokenWithQueueKey { queue_key: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEnvelope {
    pub raw: RawCompletion,
    pub identity: CompletionIdentity,
    pub source: CompletionIdentitySource,
}

impl CompletionEnvelope {
    #[inline]
    pub fn from_raw_parts(
        backend: CompletionBackend,
        raw_token: u64,
        res: i32,
        flags: u32,
    ) -> Self {
        Self::from_raw(RawCompletion::new(
            backend,
            CompletionToken::from_raw(raw_token),
            res,
            flags,
        ))
    }

    #[inline]
    pub fn from_raw(raw: RawCompletion) -> Self {
        let (identity, source) = match raw.token.classify() {
            CompletionTokenClass::User(token) => (
                CompletionIdentity::User(token),
                CompletionIdentitySource::UserToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Waker,
                id,
            } => (
                CompletionIdentity::Waker(id),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Cancel,
                id,
            } => (
                CompletionIdentity::Cancel(CancelCompletionId::new(id)),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::RioWake,
                id,
            } => (
                CompletionIdentity::RioWake(id),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::UnknownControl { kind, id } => (
                CompletionIdentity::UnknownControl { kind, id },
                CompletionIdentitySource::ControlToken,
            ),
        };
        Self {
            raw,
            identity,
            source,
        }
    }

    #[inline]
    pub fn from_sidecar_user_token(
        backend: CompletionBackend,
        token: OpToken,
        queue_key: u64,
        res: i32,
        flags: u32,
    ) -> Self {
        Self {
            raw: RawCompletion::new(backend, CompletionToken::user(token), res, flags),
            identity: CompletionIdentity::User(token),
            source: CompletionIdentitySource::SidecarTokenWithQueueKey { queue_key },
        }
    }

    #[inline]
    pub const fn backend_context(
        backend: CompletionBackend,
        raw_context: u64,
        res: i32,
        flags: u32,
    ) -> Self {
        Self {
            raw: RawCompletion::new(backend, CompletionToken::rio_wake(0), res, flags),
            identity: CompletionIdentity::BackendContext {
                backend,
                raw_context,
            },
            source: CompletionIdentitySource::BackendContext,
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Finalized,
    Missing(CompletionAnomaly),
}

#[inline]
fn finalize_waiting_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_waiting_completion(token).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
fn finalize_orphaned_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_orphaned_completion(token).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
fn finalize_corrupt_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    snapshot: slot::SlotSnapshot,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    let Ok(token) = snapshot.try_token() else {
        let raw = RawCompletion::new(
            backend,
            CompletionToken::from_raw(snapshot.index as u64),
            raw_res,
            flags,
        );
        let anomaly = CompletionAnomaly::finalize_failed(
            raw.token,
            snapshot.index,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw);
        record_completion_anomaly(diagnostics, &anomaly);
        return FinalizeOutcome::Missing(anomaly);
    };

    if registry.finalize_corrupt_slot(snapshot).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
fn record_finalize_failure<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    let raw = RawCompletion::new(backend, CompletionToken::user(token), raw_res, flags);
    let anomaly = match slot_view_anomaly(backend, token, raw, registry.checked_slot_view(token)) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomaly::finalize_failed(
                raw.token,
                snapshot.index,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw)
        }
        Err(anomaly) => anomaly,
    };
    record_completion_anomaly(diagnostics, &anomaly);
    FinalizeOutcome::Missing(anomaly)
}

#[inline]
fn dispatch_envelope(envelope: CompletionEnvelope) -> CompletionDispatch {
    let raw = envelope.raw;
    match envelope.identity {
        CompletionIdentity::User(token) => CompletionDispatch::User {
            event: UserCompletionEvent::from_classified(token, raw),
        },
        CompletionIdentity::Waker(id) => CompletionDispatch::Waker { id, raw },
        CompletionIdentity::Cancel(id) => CompletionDispatch::Cancel { id, raw },
        CompletionIdentity::RioWake(id) => CompletionDispatch::RioWake { id, raw },
        CompletionIdentity::UnknownControl { .. } | CompletionIdentity::BackendContext { .. } => {
            CompletionDispatch::Unknown { envelope }
        }
    }
}

impl CompletionAnomaly {
    #[inline]
    pub fn with_raw_completion(self, raw: RawCompletion) -> Self {
        self.with_backend(raw.backend).with_event(raw.event())
    }
}

#[inline]
fn unknown_completion_anomaly(envelope: CompletionEnvelope) -> CompletionAnomaly {
    match envelope.identity {
        CompletionIdentity::BackendContext {
            backend,
            raw_context,
        } => CompletionAnomaly::backend_context_unknown(envelope.raw.token)
            .with_raw_completion(envelope.raw)
            .with_backend(backend)
            .with_backend_context(raw_context),
        CompletionIdentity::UnknownControl { .. }
        | CompletionIdentity::User(_)
        | CompletionIdentity::Waker(_)
        | CompletionIdentity::Cancel(_)
        | CompletionIdentity::RioWake(_) => {
            CompletionAnomaly::unknown_control(envelope.raw.token).with_raw_completion(envelope.raw)
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpTokenError {
    ReservedControlIndex { index: usize },
}

impl OpToken {
    #[inline]
    pub const fn try_new(index: usize, generation: u32) -> Result<Self, OpTokenError> {
        if index >= CONTROL_TOKEN_INDEX as usize {
            return Err(OpTokenError::ReservedControlIndex { index });
        }
        Ok(Self { index, generation })
    }

    #[inline]
    pub const fn from_registry_parts(index: usize, generation: u32) -> Result<Self, OpTokenError> {
        Self::try_new(index, generation)
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
pub struct CancelCompletionId(u16);

impl CancelCompletionId {
    #[inline]
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u16 {
        self.0
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
    const fn from_raw(raw: u64) -> Self {
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
    pub const fn cancel(id: CancelCompletionId) -> Self {
        Self::internal(CompletionControlKind::Cancel, id.raw())
    }

    #[inline]
    pub const fn rio_wake(id: u16) -> Self {
        Self::internal(CompletionControlKind::RioWake, id)
    }

    #[inline]
    pub fn classify(self) -> CompletionTokenClass {
        let index = (self.0 & 0xffff_ffff) as u32;
        if index != CONTROL_TOKEN_INDEX
            && let Ok(token) = OpToken::try_new(index as usize, (self.0 >> 32) as u32)
        {
            return CompletionTokenClass::User(token);
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
fn route_user_completion<'a, Spec: slot::SlotSpec>(
    event: UserCompletionEvent,
    view: CheckedSlotView<'a, Spec>,
) -> RoutedSlotCompletion<'a, Spec> {
    let token = event.token();
    let raw = event.raw();
    match slot_view_anomaly(raw.backend, token, raw, view) {
        Ok(SlotView::InFlightWaiting(slot)) => RoutedSlotCompletion::Waiting(slot),
        Ok(SlotView::InFlightOrphaned(slot)) => RoutedSlotCompletion::Orphaned(slot),
        Ok(SlotView::Reserved(slot)) => {
            let snapshot = slot.snapshot();
            RoutedSlotCompletion::Corrupt(
                CompletionAnomaly::backend_invariant_broken(
                    raw.token,
                    snapshot.index,
                    snapshot.generation,
                    snapshot.state,
                )
                .with_slot_snapshot(snapshot)
                .with_raw_completion(raw),
            )
        }
        Err(anomaly) => match anomaly.reason {
            CompletionAnomalyReason::UnknownSlot => RoutedSlotCompletion::Missing(anomaly),
            CompletionAnomalyReason::NonActiveSlot => RoutedSlotCompletion::Empty(anomaly),
            CompletionAnomalyReason::StaleGeneration => RoutedSlotCompletion::Stale(anomaly),
            _ => RoutedSlotCompletion::Corrupt(anomaly),
        },
    }
}

#[inline]
fn slot_view_anomaly<'a, Spec: slot::SlotSpec>(
    backend: CompletionBackend,
    token: OpToken,
    raw: RawCompletion,
    view: CheckedSlotView<'a, Spec>,
) -> Result<SlotView<'a, Spec>, CompletionAnomaly> {
    let raw = RawCompletion::new(backend, raw.token, raw.res, raw.flags);
    let (index, expected_generation) = token.parts();
    match view {
        CheckedSlotView::Valid(slot) => Ok(slot),
        CheckedSlotView::Missing { .. } => {
            Err(
                CompletionAnomaly::unknown_slot(raw.token, index, expected_generation)
                    .with_raw_completion(raw),
            )
        }
        CheckedSlotView::Empty(snapshot) => Err(CompletionAnomaly::non_active(
            raw.token,
            snapshot.index,
            expected_generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw)),
        CheckedSlotView::Stale(snapshot) => Err(CompletionAnomaly::stale(
            raw.token,
            snapshot.index,
            expected_generation,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw)),
        CheckedSlotView::Corrupt(snapshot) => Err(corrupt_slot_anomaly(raw.token, snapshot)
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw)),
    }
}

#[inline]
fn corrupt_slot_anomaly(token: CompletionToken, snapshot: slot::SlotSnapshot) -> CompletionAnomaly {
    if !snapshot.has_op {
        CompletionAnomaly::op_missing(token, snapshot.index, snapshot.generation)
    } else if !snapshot.has_payload {
        CompletionAnomaly::payload_missing(token, snapshot.index, snapshot.generation)
    } else {
        CompletionAnomaly::corrupt(token, snapshot.index, snapshot.generation, snapshot.state)
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
    CompletedLocally,
    TargetGone { reason: CancelTargetGoneReason },
    DiagnosticOnly { anomaly: CompletionAnomaly },
    NoBackendHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTargetGoneReason {
    Missing,
    Stale,
    Corrupt,
}

impl CancelSubmitOutcome {
    #[inline]
    pub const fn target_missing() -> Self {
        Self::TargetGone {
            reason: CancelTargetGoneReason::Missing,
        }
    }

    #[inline]
    pub const fn target_stale() -> Self {
        Self::TargetGone {
            reason: CancelTargetGoneReason::Stale,
        }
    }

    #[inline]
    pub const fn target_corrupt() -> Self {
        Self::TargetGone {
            reason: CancelTargetGoneReason::Corrupt,
        }
    }
}

#[inline]
pub fn cancel_target_anomaly<'a, Spec: slot::SlotSpec>(
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
    view: CheckedSlotView<'a, Spec>,
) -> (CancelTargetGoneReason, CompletionAnomaly) {
    let raw = RawCompletion::new(backend, CompletionToken::user(token), raw_res, flags);
    let anomaly = match slot_view_anomaly(backend, token, raw, view) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomaly::backend_invariant_broken(
                raw.token,
                snapshot.index,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw)
        }
        Err(anomaly) => anomaly,
    };
    let reason = match anomaly.reason {
        CompletionAnomalyReason::StaleGeneration => CancelTargetGoneReason::Stale,
        CompletionAnomalyReason::OpMissing
        | CompletionAnomalyReason::PayloadMissing
        | CompletionAnomalyReason::SlotCorruption
        | CompletionAnomalyReason::BackendInvariantBroken => CancelTargetGoneReason::Corrupt,
        CompletionAnomalyReason::UnknownSlot
        | CompletionAnomalyReason::NonActiveSlot
        | CompletionAnomalyReason::UnknownControlToken
        | CompletionAnomalyReason::ControlCompletionUntracked
        | CompletionAnomalyReason::RioMalformedContext
        | CompletionAnomalyReason::RioMissingContext
        | CompletionAnomalyReason::RioStaleContext
        | CompletionAnomalyReason::CompletionKeyMismatch
        | CompletionAnomalyReason::FinalizeFailed
        | CompletionAnomalyReason::CancelAckTargetStillActive
        | CompletionAnomalyReason::BackendContextUnknown => CancelTargetGoneReason::Missing,
    };
    (reason, anomaly)
}

pub struct CompletionPacket<Spec: slot::SlotSpec> {
    pub event: UserCompletionEvent,
    pub input: CompletionInput<Spec>,
}

pub struct UserCompletion<Spec: slot::SlotSpec> {
    pub payload: Spec::UserPayload,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
}

pub struct CompletionLoss {
    pub anomaly: CompletionAnomaly,
    pub cleanup: CompletionCleanupGuard,
}

pub enum CompletionInput<Spec: slot::SlotSpec> {
    User(UserCompletion<Spec>),
    Lost(CompletionLoss),
}

impl<Spec: slot::SlotSpec> CompletionInput<Spec> {
    #[inline]
    pub fn cleanup_mut(&mut self) -> &mut CompletionCleanupGuard {
        match self {
            Self::User(completion) => &mut completion.cleanup,
            Self::Lost(loss) => &mut loss.cleanup,
        }
    }

    #[inline]
    pub fn anomaly(&self) -> Option<&CompletionAnomaly> {
        match self {
            Self::User(_) => None,
            Self::Lost(loss) => Some(&loss.anomaly),
        }
    }
}

impl<Spec: slot::SlotSpec> CompletionPacket<Spec> {
    #[inline]
    pub fn user_event(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self {
            event,
            input: CompletionInput::User(UserCompletion {
                payload,
                detail,
                cleanup,
            }),
        }
    }

    #[inline]
    pub fn user(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    ) -> Self {
        Self::user_event(event, payload, detail, CompletionCleanupGuard::default())
    }

    #[inline]
    pub fn user_with_cleanup(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self::user_event(event, payload, detail, cleanup)
    }

    #[inline]
    pub fn lost(
        event: UserCompletionEvent,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self {
            event,
            input: CompletionInput::Lost(CompletionLoss { anomaly, cleanup }),
        }
    }

    #[inline]
    pub const fn token(&self) -> OpToken {
        self.event.token()
    }

    #[inline]
    pub const fn completion_event(&self) -> CompletionEvent {
        self.event.event()
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

pub struct CompletionRecord<Spec: slot::SlotSpec> {
    pub event: UserCompletionEvent,
    pub payload: Spec::UserPayload,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
}

impl<Spec: slot::SlotSpec> CompletionRecord<Spec> {
    #[inline]
    pub fn disarm_cleanup(&mut self) -> bool {
        self.cleanup.disarm()
    }
}

#[inline]
fn run_rejected_cleanup<Spec: slot::SlotSpec>(
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    mut packet: CompletionPacket<Spec>,
) {
    run_completion_cleanup(diagnostics, packet.input.cleanup_mut());
    drop(packet);
}

#[inline]
fn run_completion_cleanup<B>(
    diagnostics: &DriverCompletionDiagnostics<B>,
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
fn record_completion_anomaly<B>(
    diagnostics: &DriverCompletionDiagnostics<B>,
    anomaly: &CompletionAnomaly,
) where
    B: DriverCompletionDiagnosticsBackend,
{
    diagnostics.record_anomaly(anomaly);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::PlatformOp;
    use crate::driver::registry::OpRegistry;
    use crate::slot::SlotRegistryExt;

    #[test]
    fn op_token_try_new_rejects_control_index() {
        let err = OpToken::try_new(CONTROL_TOKEN_INDEX as usize, 1)
            .expect_err("control index must not be a user token");
        assert_eq!(
            err,
            OpTokenError::ReservedControlIndex {
                index: CONTROL_TOKEN_INDEX as usize
            }
        );
    }

    #[test]
    fn classify_user_token_does_not_panic() {
        let token = OpToken::from_registry_parts((CONTROL_TOKEN_INDEX - 1) as usize, 7)
            .expect("max user index should be encodable");
        assert_eq!(
            CompletionToken::user(token).classify(),
            CompletionTokenClass::User(token)
        );
    }

    #[test]
    fn rio_context_anomaly_keeps_backend_context_separate() {
        let raw_context = 0xa700_0001_0000_002a;
        let anomaly = CompletionAnomaly::rio_malformed_context_raw(raw_context);

        assert_eq!(anomaly.token, CompletionToken::rio_wake(0));
        assert_eq!(anomaly.backend_context, Some(raw_context));
    }

    #[test]
    fn user_completion_event_rejects_mismatched_raw_token() {
        let expected = OpToken::from_registry_parts(1, 7).expect("test token");
        let actual = OpToken::from_registry_parts(2, 7).expect("test token");
        let raw = RawCompletion::new(CompletionBackend::Core, CompletionToken::user(actual), 3, 0);

        let err = UserCompletionEvent::try_new(expected, raw)
            .expect_err("mismatched user completion must not be constructible");

        assert_eq!(err.token, expected);
        assert_eq!(err.expected, CompletionToken::user(expected));
        assert_eq!(err.actual, CompletionToken::user(actual));
    }

    #[test]
    fn completion_packet_uses_user_completion_event_token() {
        let token = OpToken::from_registry_parts(3, 9).expect("test token");
        let event = UserCompletionEvent::from_parts(CompletionBackend::Core, token, 11, 5);
        let packet = CompletionPacket::<DummySlotSpec>::user(event, (), None);

        assert_eq!(packet.token(), token);
        assert_eq!(
            packet.completion_event().token,
            CompletionToken::user(token)
        );
        assert_eq!(packet.completion_event().res, 11);
        assert_eq!(packet.completion_event().flags, 5);
    }

    #[test]
    fn sidecar_completion_envelope_keeps_queue_key_as_source_only() {
        let token = OpToken::from_registry_parts(3, 9).expect("test token");
        let envelope =
            CompletionEnvelope::from_sidecar_user_token(CompletionBackend::Iocp, token, 77, 11, 5);

        assert_eq!(envelope.identity, CompletionIdentity::User(token));
        assert_eq!(
            envelope.source,
            CompletionIdentitySource::SidecarTokenWithQueueKey { queue_key: 77 }
        );
        assert_eq!(envelope.raw.token, CompletionToken::user(token));
        assert_eq!(envelope.raw.res, 11);
        assert_eq!(envelope.raw.flags, 5);
    }

    #[test]
    fn unknown_completion_anomaly_preserves_backend_context() {
        let envelope = CompletionEnvelope::backend_context(CompletionBackend::Iocp, 0, -5, 0);

        let anomaly = unknown_completion_anomaly(envelope);

        assert_eq!(
            anomaly.reason,
            CompletionAnomalyReason::BackendContextUnknown
        );
        assert_eq!(anomaly.backend, Some(CompletionBackend::Iocp));
        assert_eq!(anomaly.backend_context, Some(0));
        assert_eq!(anomaly.raw_result, Some(-5));
    }

    #[test]
    fn cancel_target_anomaly_classifies_missing_target_without_user_completion() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let token = OpToken::from_registry_parts(4, 1).expect("test token");

        let (reason, anomaly) = cancel_target_anomaly(
            CompletionBackend::Core,
            token,
            -1,
            0,
            registry.checked_slot_view(token),
        );

        assert_eq!(reason, CancelTargetGoneReason::Missing);
        assert_eq!(anomaly.reason, CompletionAnomalyReason::UnknownSlot);
        assert_eq!(anomaly.token, CompletionToken::user(token));
    }

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {
        type CleanupContext<'a> = ();
    }

    struct DummySlotSpec;

    impl slot::SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = ();
        type Completion = usize;
        type CompletionDiagnostics = ();
    }

    #[test]
    fn finalize_waiting_checked_records_missing_slot() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let diagnostics = registry.shared.completion_diagnostics();
        let token = OpToken::from_registry_parts(4, 1).expect("test token");

        let outcome = finalize_waiting_checked(
            &mut registry,
            &diagnostics,
            CompletionBackend::Core,
            token,
            -5,
            0,
        );

        assert!(matches!(
            outcome,
            FinalizeOutcome::Missing(anomaly)
                if anomaly.reason == CompletionAnomalyReason::UnknownSlot
        ));
        assert_eq!(diagnostics.snapshot().unknown_completion, 1);
    }

    #[test]
    fn finalize_corrupt_checked_records_unencodable_snapshot() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let diagnostics = registry.shared.completion_diagnostics();
        let snapshot = slot::SlotSnapshot {
            index: u32::MAX as usize,
            generation: 1,
            state: slot::SlotState::InFlightWaiting,
            has_op: false,
            has_payload: false,
        };

        let outcome = finalize_corrupt_checked(
            &mut registry,
            &diagnostics,
            CompletionBackend::Core,
            snapshot,
            -5,
            0,
        );

        assert!(matches!(
            outcome,
            FinalizeOutcome::Missing(anomaly)
                if anomaly.reason == CompletionAnomalyReason::FinalizeFailed
        ));
        assert_eq!(diagnostics.snapshot().internal_unknown, 1);
    }
}
