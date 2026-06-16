use crate::slot;

use super::super::{CompletionEvent, CompletionToken};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyReason {
    UnknownSlot,
    UnknownControlToken,
    ControlCompletionUntracked,
    StaleGeneration,
    NonActiveSlot,
    SlotCorruption,
    OpMissing,
    PayloadMissing,
    BackendInvariantBroken,
    CompletionKeyMismatch,
    FinalizeFailed,
    CancelAckTargetStillActive,
    BackendContextUnknown,
    BackendSpecific(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlAnomalyReason {
    UnknownControlToken,
    ControlCompletionUntracked,
    BackendContextUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotIssueReason {
    BackendInvariantBroken,
    CompletionKeyMismatch,
    FinalizeFailed,
    CancelAckTargetStillActive,
    SlotCorruption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSlotRef {
    pub index: usize,
    pub expected_generation: u32,
    pub actual_generation: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionBackend {
    Core,
    Backend(std::num::NonZeroU8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionRaw {
    pub backend: CompletionBackend,
    pub res: i32,
    pub flags: u32,
}

/// Lightweight anomaly classification for hot propagation paths (~24–32 B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyKind {
    UnknownSlot {
        index: usize,
        generation: u32,
    },
    Stale {
        index: usize,
        expected: u32,
        actual: u32,
        state: slot::SlotState,
    },
    NonActive {
        index: usize,
        generation: u32,
        state: slot::SlotState,
    },
    Corrupt {
        snapshot: slot::SlotSnapshot,
    },
    SlotIssue {
        reason: SlotIssueReason,
        index: usize,
        generation: u32,
        state: slot::SlotState,
        snapshot: Option<slot::SlotSnapshot>,
    },
    Control {
        reason: ControlAnomalyReason,
    },
    BackendContext {
        backend: CompletionBackend,
        backend_context: u64,
    },
    BackendSpecific {
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
        slot: Option<BackendSlotRef>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnomalyAttach {
    pub token: CompletionToken,
    pub raw: Option<CompletionRaw>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyOutcome {
    Missing(CompletionAnomalyKind),
    Stale(CompletionAnomalyKind),
    NonActive(CompletionAnomalyKind),
    Corrupt(CompletionAnomalyKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionMutationOutcome {
    Applied,
    Rejected(AnomalyOutcome),
}

impl CompletionMutationOutcome {
    #[inline]
    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    #[inline]
    pub fn anomaly_outcome(&self) -> Option<AnomalyOutcome> {
        match self {
            Self::Applied => None,
            Self::Rejected(outcome) => Some(*outcome),
        }
    }

    #[inline]
    pub fn kind(&self) -> Option<CompletionAnomalyKind> {
        self.anomaly_outcome().map(AnomalyOutcome::kind)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomaly {
    TokenOnly {
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        raw: Option<CompletionRaw>,
    },
    UnknownSlot {
        token: CompletionToken,
        index: usize,
        expected_generation: u32,
        raw: Option<CompletionRaw>,
    },
    StaleGeneration {
        token: CompletionToken,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
        state: slot::SlotState,
        raw: Option<CompletionRaw>,
    },
    SlotState {
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
        snapshot: Option<slot::SlotSnapshot>,
        raw: Option<CompletionRaw>,
    },
    SlotCorruption {
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        snapshot: slot::SlotSnapshot,
        raw: Option<CompletionRaw>,
    },
    BackendContext {
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
        raw: CompletionRaw,
    },
    BackendSpecific {
        code: u16,
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
        index: Option<usize>,
        expected_generation: Option<u32>,
        actual_generation: Option<u32>,
        raw: Option<CompletionRaw>,
    },
}

impl AnomalyOutcome {
    #[inline]
    pub fn kind(self) -> CompletionAnomalyKind {
        match self {
            Self::Missing(kind)
            | Self::Stale(kind)
            | Self::NonActive(kind)
            | Self::Corrupt(kind) => kind,
        }
    }

    #[inline]
    pub fn materialize(self, attach: AnomalyAttach) -> CompletionAnomaly {
        self.kind().materialize(attach)
    }
}

impl AnomalyAttach {
    #[inline]
    pub const fn token_only(token: CompletionToken) -> Self {
        Self { token, raw: None }
    }

    #[inline]
    pub fn from_op_token(token: crate::driver::OpToken) -> Self {
        Self {
            token: CompletionToken::user(token),
            raw: None,
        }
    }

    #[inline]
    pub fn from_raw_completion(raw: super::super::RawCompletion) -> Self {
        Self {
            token: raw.token,
            raw: Some(CompletionRaw {
                backend: raw.backend,
                res: raw.res,
                flags: raw.flags,
            }),
        }
    }
}

#[inline]
fn corrupt_reason_from_snapshot(snapshot: slot::SlotSnapshot) -> CompletionAnomalyReason {
    if !snapshot.has_op {
        CompletionAnomalyReason::OpMissing
    } else if !snapshot.has_payload {
        CompletionAnomalyReason::PayloadMissing
    } else {
        CompletionAnomalyReason::SlotCorruption
    }
}

impl CompletionAnomalyKind {
    #[inline]
    pub const fn unknown_slot(index: usize, generation: u32) -> Self {
        Self::UnknownSlot { index, generation }
    }

    #[inline]
    pub const fn stale(index: usize, expected: u32, actual: u32, state: slot::SlotState) -> Self {
        Self::Stale {
            index,
            expected,
            actual,
            state,
        }
    }

    #[inline]
    pub const fn non_active(index: usize, generation: u32, state: slot::SlotState) -> Self {
        Self::NonActive {
            index,
            generation,
            state,
        }
    }

    #[inline]
    pub const fn corrupt_snapshot(snapshot: slot::SlotSnapshot) -> Self {
        Self::Corrupt { snapshot }
    }

    #[inline]
    pub fn corrupt_from_view(snapshot: slot::SlotSnapshot) -> Self {
        Self::Corrupt { snapshot }
    }

    #[inline]
    pub const fn slot_issue(
        reason: SlotIssueReason,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::SlotIssue {
            reason,
            index,
            generation,
            state,
            snapshot: None,
        }
    }

    #[inline]
    pub const fn slot_issue_with_snapshot(
        reason: SlotIssueReason,
        snapshot: slot::SlotSnapshot,
    ) -> Self {
        Self::SlotIssue {
            reason,
            index: snapshot.index,
            generation: snapshot.generation,
            state: snapshot.state,
            snapshot: Some(snapshot),
        }
    }

    #[inline]
    pub const fn backend_invariant_broken(
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_issue(
            SlotIssueReason::BackendInvariantBroken,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub const fn backend_invariant_broken_snapshot(snapshot: slot::SlotSnapshot) -> Self {
        Self::slot_issue_with_snapshot(SlotIssueReason::BackendInvariantBroken, snapshot)
    }

    #[inline]
    pub const fn completion_key_mismatch(
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_issue(
            SlotIssueReason::CompletionKeyMismatch,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub const fn finalize_failed(index: usize, generation: u32, state: slot::SlotState) -> Self {
        Self::slot_issue(SlotIssueReason::FinalizeFailed, index, generation, state)
    }

    #[inline]
    pub const fn finalize_failed_snapshot(snapshot: slot::SlotSnapshot) -> Self {
        Self::slot_issue_with_snapshot(SlotIssueReason::FinalizeFailed, snapshot)
    }

    #[inline]
    pub const fn cancel_ack_target_still_active(
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_issue(
            SlotIssueReason::CancelAckTargetStillActive,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub const fn unknown_control() -> Self {
        Self::Control {
            reason: ControlAnomalyReason::UnknownControlToken,
        }
    }

    #[inline]
    pub const fn control_completion_untracked() -> Self {
        Self::Control {
            reason: ControlAnomalyReason::ControlCompletionUntracked,
        }
    }

    #[inline]
    pub const fn backend_context_unknown() -> Self {
        Self::Control {
            reason: ControlAnomalyReason::BackendContextUnknown,
        }
    }

    #[inline]
    pub const fn backend_context(backend: CompletionBackend, backend_context: u64) -> Self {
        Self::BackendContext {
            backend,
            backend_context,
        }
    }

    #[inline]
    pub const fn backend_specific(
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
    ) -> Self {
        Self::BackendSpecific {
            code,
            backend,
            backend_context,
            slot: None,
        }
    }

    #[inline]
    pub const fn backend_specific_missing(
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
        index: usize,
        expected_generation: u32,
    ) -> Self {
        Self::BackendSpecific {
            code,
            backend,
            backend_context,
            slot: Some(BackendSlotRef {
                index,
                expected_generation,
                actual_generation: None,
            }),
        }
    }

    #[inline]
    pub const fn backend_specific_stale(
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
    ) -> Self {
        Self::BackendSpecific {
            code,
            backend,
            backend_context,
            slot: Some(BackendSlotRef {
                index,
                expected_generation,
                actual_generation: Some(actual_generation),
            }),
        }
    }

    #[inline]
    pub fn reason(self) -> CompletionAnomalyReason {
        match self {
            Self::UnknownSlot { .. } => CompletionAnomalyReason::UnknownSlot,
            Self::Stale { .. } => CompletionAnomalyReason::StaleGeneration,
            Self::NonActive { .. } => CompletionAnomalyReason::NonActiveSlot,
            Self::Corrupt { snapshot } => corrupt_reason_from_snapshot(snapshot),
            Self::SlotIssue { reason, .. } => match reason {
                SlotIssueReason::BackendInvariantBroken => {
                    CompletionAnomalyReason::BackendInvariantBroken
                }
                SlotIssueReason::CompletionKeyMismatch => {
                    CompletionAnomalyReason::CompletionKeyMismatch
                }
                SlotIssueReason::FinalizeFailed => CompletionAnomalyReason::FinalizeFailed,
                SlotIssueReason::CancelAckTargetStillActive => {
                    CompletionAnomalyReason::CancelAckTargetStillActive
                }
                SlotIssueReason::SlotCorruption => CompletionAnomalyReason::SlotCorruption,
            },
            Self::Control { reason } => match reason {
                ControlAnomalyReason::UnknownControlToken => {
                    CompletionAnomalyReason::UnknownControlToken
                }
                ControlAnomalyReason::ControlCompletionUntracked => {
                    CompletionAnomalyReason::ControlCompletionUntracked
                }
                ControlAnomalyReason::BackendContextUnknown => {
                    CompletionAnomalyReason::BackendContextUnknown
                }
            },
            Self::BackendContext { .. } => CompletionAnomalyReason::BackendContextUnknown,
            Self::BackendSpecific { code, .. } => CompletionAnomalyReason::BackendSpecific(code),
        }
    }

    #[inline]
    pub fn index(self) -> Option<usize> {
        match self {
            Self::UnknownSlot { index, .. }
            | Self::Stale { index, .. }
            | Self::NonActive { index, .. }
            | Self::SlotIssue { index, .. } => Some(index),
            Self::Corrupt { snapshot } => Some(snapshot.index),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.index),
            Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    #[inline]
    pub fn slot_snapshot(self) -> Option<slot::SlotSnapshot> {
        match self {
            Self::Corrupt { snapshot } => Some(snapshot),
            Self::SlotIssue {
                snapshot: Some(snapshot),
                ..
            } => Some(snapshot),
            Self::SlotIssue { .. } => None,
            _ => None,
        }
    }

    #[inline]
    pub fn expected_generation(self) -> Option<u32> {
        match self {
            Self::UnknownSlot { generation, .. } => Some(generation),
            Self::Stale { expected, .. } => Some(expected),
            Self::NonActive { generation, .. } => Some(generation),
            Self::Corrupt { snapshot } => Some(snapshot.generation),
            Self::SlotIssue { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.expected_generation),
            Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    #[inline]
    pub fn actual_generation(self) -> Option<u32> {
        match self {
            Self::Stale { actual, .. } => Some(actual),
            Self::NonActive { generation, .. } => Some(generation),
            Self::Corrupt { snapshot } => Some(snapshot.generation),
            Self::SlotIssue { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => slot.actual_generation,
            Self::UnknownSlot { .. } | Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    #[inline]
    pub fn state(self) -> Option<slot::SlotState> {
        match self {
            Self::Stale { state, .. }
            | Self::NonActive { state, .. }
            | Self::SlotIssue { state, .. } => Some(state),
            Self::Corrupt { snapshot } => Some(snapshot.state),
            Self::UnknownSlot { .. }
            | Self::Control { .. }
            | Self::BackendContext { .. }
            | Self::BackendSpecific { .. } => None,
        }
    }

    #[inline]
    pub fn backend(self) -> Option<CompletionBackend> {
        match self {
            Self::BackendContext { backend, .. } | Self::BackendSpecific { backend, .. } => {
                Some(backend)
            }
            Self::UnknownSlot { .. }
            | Self::Stale { .. }
            | Self::NonActive { .. }
            | Self::Corrupt { .. }
            | Self::SlotIssue { .. }
            | Self::Control { .. } => None,
        }
    }

    #[inline]
    pub fn backend_context_value(self) -> Option<u64> {
        match self {
            Self::BackendContext {
                backend_context, ..
            }
            | Self::BackendSpecific {
                backend_context, ..
            } => Some(backend_context),
            _ => None,
        }
    }

    #[inline]
    pub fn materialize(self, attach: AnomalyAttach) -> CompletionAnomaly {
        let token = attach.token;
        let raw = attach.raw;
        let anomaly = match self {
            Self::UnknownSlot { index, generation } => {
                CompletionAnomaly::unknown_slot(token, index, generation)
            }
            Self::Stale {
                index,
                expected,
                actual,
                state,
            } => CompletionAnomaly::stale(token, index, expected, actual, state),
            Self::NonActive {
                index,
                generation,
                state,
            } => CompletionAnomaly::non_active(token, index, generation, state),
            Self::Corrupt { snapshot } => CompletionAnomaly::corrupt_slot_snapshot(token, snapshot),
            Self::SlotIssue {
                reason,
                index,
                generation,
                state,
                snapshot,
            } => {
                let mut anomaly = match reason {
                    SlotIssueReason::BackendInvariantBroken => {
                        CompletionAnomaly::backend_invariant_broken(token, index, generation, state)
                    }
                    SlotIssueReason::CompletionKeyMismatch => {
                        CompletionAnomaly::completion_key_mismatch(token, index, generation, state)
                    }
                    SlotIssueReason::FinalizeFailed => {
                        CompletionAnomaly::finalize_failed(token, index, generation, state)
                    }
                    SlotIssueReason::CancelAckTargetStillActive => {
                        CompletionAnomaly::cancel_ack_target_still_active(
                            token, index, generation, state,
                        )
                    }
                    SlotIssueReason::SlotCorruption => {
                        CompletionAnomaly::corrupt(token, index, generation, state)
                    }
                };
                if let Some(snapshot) = snapshot {
                    anomaly = anomaly.with_slot_snapshot(snapshot);
                }
                anomaly
            }
            Self::Control { reason } => match reason {
                ControlAnomalyReason::UnknownControlToken => {
                    CompletionAnomaly::unknown_control(token)
                }
                ControlAnomalyReason::ControlCompletionUntracked => {
                    CompletionAnomaly::control_completion_untracked(token)
                }
                ControlAnomalyReason::BackendContextUnknown => {
                    CompletionAnomaly::backend_context_unknown(token)
                }
            },
            Self::BackendContext {
                backend,
                backend_context,
            } => {
                let raw = raw.unwrap_or(CompletionRaw {
                    backend,
                    res: 0,
                    flags: 0,
                });
                CompletionAnomaly::from_backend_context(token, backend, backend_context, raw)
            }
            Self::BackendSpecific {
                code,
                backend,
                backend_context,
                slot,
            } => match slot {
                None => CompletionAnomaly::backend_specific(code, token, backend, backend_context),
                Some(BackendSlotRef {
                    index,
                    expected_generation,
                    actual_generation: None,
                }) => CompletionAnomaly::backend_specific_missing(
                    code,
                    token,
                    backend,
                    backend_context,
                    index,
                    expected_generation,
                ),
                Some(BackendSlotRef {
                    index,
                    expected_generation,
                    actual_generation: Some(actual_generation),
                }) => CompletionAnomaly::backend_specific_stale(
                    code,
                    token,
                    backend,
                    backend_context,
                    index,
                    expected_generation,
                    actual_generation,
                ),
            },
        };
        match raw {
            Some(raw) if !matches!(anomaly, CompletionAnomaly::BackendContext { .. }) => {
                anomaly.with_raw(raw)
            }
            _ => anomaly,
        }
    }
}

impl CompletionAnomaly {
    #[inline]
    pub fn reason(self) -> CompletionAnomalyReason {
        match self {
            Self::TokenOnly { reason, .. }
            | Self::SlotState { reason, .. }
            | Self::SlotCorruption { reason, .. } => reason,
            Self::UnknownSlot { .. } => CompletionAnomalyReason::UnknownSlot,
            Self::StaleGeneration { .. } => CompletionAnomalyReason::StaleGeneration,
            Self::BackendContext { .. } => CompletionAnomalyReason::BackendContextUnknown,
            Self::BackendSpecific { code, .. } => CompletionAnomalyReason::BackendSpecific(code),
        }
    }

    #[inline]
    pub fn token(self) -> CompletionToken {
        match self {
            Self::TokenOnly { token, .. }
            | Self::UnknownSlot { token, .. }
            | Self::StaleGeneration { token, .. }
            | Self::SlotState { token, .. }
            | Self::SlotCorruption { token, .. }
            | Self::BackendContext { token, .. }
            | Self::BackendSpecific { token, .. } => token,
        }
    }

    #[inline]
    pub fn index(self) -> Option<usize> {
        match self {
            Self::UnknownSlot { index, .. }
            | Self::StaleGeneration { index, .. }
            | Self::SlotState { index, .. } => Some(index),
            Self::SlotCorruption { snapshot, .. } => Some(snapshot.index),
            Self::BackendSpecific { index, .. } => index,
            Self::TokenOnly { .. } | Self::BackendContext { .. } => None,
        }
    }

    #[inline]
    pub fn expected_generation(self) -> Option<u32> {
        match self {
            Self::UnknownSlot {
                expected_generation,
                ..
            } => Some(expected_generation),
            Self::StaleGeneration {
                expected_generation,
                ..
            } => Some(expected_generation),
            Self::SlotState { generation, .. } => Some(generation),
            Self::SlotCorruption { snapshot, .. } => Some(snapshot.generation),
            Self::BackendSpecific {
                expected_generation,
                ..
            } => expected_generation,
            Self::TokenOnly { .. } | Self::BackendContext { .. } => None,
        }
    }

    #[inline]
    pub fn actual_generation(self) -> Option<u32> {
        match self {
            Self::StaleGeneration {
                actual_generation, ..
            } => Some(actual_generation),
            Self::SlotState { generation, .. } => Some(generation),
            Self::SlotCorruption { snapshot, .. } => Some(snapshot.generation),
            Self::BackendSpecific {
                actual_generation, ..
            } => actual_generation,
            Self::UnknownSlot { .. } | Self::TokenOnly { .. } | Self::BackendContext { .. } => None,
        }
    }

    #[inline]
    pub fn state(self) -> Option<slot::SlotState> {
        match self {
            Self::StaleGeneration { state, .. } | Self::SlotState { state, .. } => Some(state),
            Self::SlotCorruption { snapshot, .. } => Some(snapshot.state),
            Self::UnknownSlot { .. }
            | Self::TokenOnly { .. }
            | Self::BackendContext { .. }
            | Self::BackendSpecific { .. } => None,
        }
    }

    #[inline]
    pub fn slot_snapshot(self) -> Option<slot::SlotSnapshot> {
        match self {
            Self::SlotCorruption { snapshot, .. } => Some(snapshot),
            Self::SlotState {
                snapshot: Some(snapshot),
                ..
            } => Some(snapshot),
            Self::SlotState { .. } => None,
            _ => None,
        }
    }

    #[inline]
    pub fn backend(self) -> Option<CompletionBackend> {
        match self {
            Self::BackendContext { backend, .. } | Self::BackendSpecific { backend, .. } => {
                Some(backend)
            }
            Self::TokenOnly { raw: Some(raw), .. }
            | Self::UnknownSlot { raw: Some(raw), .. }
            | Self::StaleGeneration { raw: Some(raw), .. }
            | Self::SlotState { raw: Some(raw), .. }
            | Self::SlotCorruption { raw: Some(raw), .. } => Some(raw.backend),
            Self::TokenOnly { raw: None, .. }
            | Self::UnknownSlot { raw: None, .. }
            | Self::StaleGeneration { raw: None, .. }
            | Self::SlotState { raw: None, .. }
            | Self::SlotCorruption { raw: None, .. } => None,
        }
    }

    #[inline]
    pub fn backend_context(self) -> Option<u64> {
        match self {
            Self::BackendContext {
                backend_context, ..
            }
            | Self::BackendSpecific {
                backend_context, ..
            } => Some(backend_context),
            _ => None,
        }
    }

    #[inline]
    pub fn raw_result(self) -> Option<i32> {
        self.raw_attachment().map(|raw| raw.res)
    }

    #[inline]
    pub fn flags(self) -> Option<u32> {
        self.raw_attachment().map(|raw| raw.flags)
    }

    #[inline]
    fn raw_attachment(self) -> Option<CompletionRaw> {
        match self {
            Self::TokenOnly { raw, .. }
            | Self::UnknownSlot { raw, .. }
            | Self::StaleGeneration { raw, .. }
            | Self::SlotState { raw, .. }
            | Self::SlotCorruption { raw, .. }
            | Self::BackendSpecific { raw, .. } => raw,
            Self::BackendContext { raw, .. } => Some(raw),
        }
    }

    #[inline]
    fn slot_state(
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::SlotState {
            reason,
            token,
            index,
            generation,
            state,
            snapshot: None,
            raw: None,
        }
    }

    #[inline]
    pub fn unknown_control(token: CompletionToken) -> Self {
        Self::TokenOnly {
            reason: CompletionAnomalyReason::UnknownControlToken,
            token,
            raw: None,
        }
    }

    #[inline]
    pub fn control_completion_untracked(token: CompletionToken) -> Self {
        Self::TokenOnly {
            reason: CompletionAnomalyReason::ControlCompletionUntracked,
            token,
            raw: None,
        }
    }

    #[inline]
    pub fn unknown_slot(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self::UnknownSlot {
            token,
            index,
            expected_generation: generation,
            raw: None,
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
        Self::StaleGeneration {
            token,
            index,
            expected_generation,
            actual_generation,
            state,
            raw: None,
        }
    }

    #[inline]
    pub fn non_active(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::NonActiveSlot,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn corrupt(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::SlotCorruption,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn op_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self::slot_corruption_from_parts(
            CompletionAnomalyReason::OpMissing,
            token,
            index,
            generation,
            slot::SlotState::Idle,
            false,
            false,
        )
    }

    #[inline]
    pub fn payload_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self::slot_corruption_from_parts(
            CompletionAnomalyReason::PayloadMissing,
            token,
            index,
            generation,
            slot::SlotState::Idle,
            true,
            false,
        )
    }

    #[inline]
    fn slot_corruption_from_parts(
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
        has_op: bool,
        has_payload: bool,
    ) -> Self {
        Self::SlotCorruption {
            reason,
            token,
            snapshot: slot::SlotSnapshot {
                index,
                generation,
                state,
                has_op,
                has_payload,
            },
            raw: None,
        }
    }

    #[inline]
    pub fn corrupt_slot_snapshot(token: CompletionToken, snapshot: slot::SlotSnapshot) -> Self {
        let reason = if !snapshot.has_op {
            CompletionAnomalyReason::OpMissing
        } else if !snapshot.has_payload {
            CompletionAnomalyReason::PayloadMissing
        } else {
            CompletionAnomalyReason::SlotCorruption
        };
        Self::SlotCorruption {
            reason,
            token,
            snapshot,
            raw: None,
        }
    }

    #[inline]
    pub fn backend_invariant_broken(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::BackendInvariantBroken,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn completion_key_mismatch(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::CompletionKeyMismatch,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn finalize_failed(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::FinalizeFailed,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn cancel_ack_target_still_active(
        token: CompletionToken,
        index: usize,
        generation: u32,
        state: slot::SlotState,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::CancelAckTargetStillActive,
            token,
            index,
            generation,
            state,
        )
    }

    #[inline]
    pub fn backend_context_unknown(token: CompletionToken) -> Self {
        Self::TokenOnly {
            reason: CompletionAnomalyReason::BackendContextUnknown,
            token,
            raw: None,
        }
    }

    #[inline]
    pub fn from_backend_context(
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
        raw: CompletionRaw,
    ) -> Self {
        Self::BackendContext {
            token,
            backend,
            backend_context,
            raw,
        }
    }

    #[inline]
    pub fn backend_specific(
        code: u16,
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
    ) -> Self {
        Self::BackendSpecific {
            code,
            token,
            backend,
            backend_context,
            index: None,
            expected_generation: None,
            actual_generation: None,
            raw: None,
        }
    }

    #[inline]
    pub fn backend_specific_missing(
        code: u16,
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
        index: usize,
        expected_generation: u32,
    ) -> Self {
        Self::BackendSpecific {
            code,
            token,
            backend,
            backend_context,
            index: Some(index),
            expected_generation: Some(expected_generation),
            actual_generation: None,
            raw: None,
        }
    }

    #[inline]
    pub fn backend_specific_stale(
        code: u16,
        token: CompletionToken,
        backend: CompletionBackend,
        backend_context: u64,
        index: usize,
        expected_generation: u32,
        actual_generation: u32,
    ) -> Self {
        Self::BackendSpecific {
            code,
            token,
            backend,
            backend_context,
            index: Some(index),
            expected_generation: Some(expected_generation),
            actual_generation: Some(actual_generation),
            raw: None,
        }
    }

    #[inline]
    pub fn with_raw(mut self, raw: CompletionRaw) -> Self {
        match &mut self {
            Self::TokenOnly { raw: slot, .. }
            | Self::UnknownSlot { raw: slot, .. }
            | Self::StaleGeneration { raw: slot, .. }
            | Self::SlotState { raw: slot, .. }
            | Self::SlotCorruption { raw: slot, .. }
            | Self::BackendSpecific { raw: slot, .. } => *slot = Some(raw),
            Self::BackendContext { raw: slot, .. } => *slot = raw,
        }
        self
    }

    #[inline]
    pub fn with_backend(self, backend: CompletionBackend) -> Self {
        match self {
            Self::TokenOnly {
                reason: CompletionAnomalyReason::BackendContextUnknown,
                token,
                raw,
            } => Self::BackendContext {
                token,
                backend,
                backend_context: 0,
                raw: raw.unwrap_or(CompletionRaw {
                    backend,
                    res: 0,
                    flags: 0,
                }),
            },
            Self::BackendSpecific {
                code,
                token,
                backend: _,
                backend_context,
                index,
                expected_generation,
                actual_generation,
                raw,
            } => Self::BackendSpecific {
                code,
                token,
                backend,
                backend_context,
                index,
                expected_generation,
                actual_generation,
                raw,
            },
            other => {
                if let Some(raw) = other.raw_attachment() {
                    other.with_raw(CompletionRaw {
                        backend,
                        res: raw.res,
                        flags: raw.flags,
                    })
                } else {
                    other.with_raw(CompletionRaw {
                        backend,
                        res: 0,
                        flags: 0,
                    })
                }
            }
        }
    }

    #[inline]
    pub fn with_backend_context(self, context: u64) -> Self {
        match self {
            Self::BackendContext {
                token,
                backend,
                raw,
                ..
            } => Self::BackendContext {
                token,
                backend,
                backend_context: context,
                raw,
            },
            Self::BackendSpecific {
                code,
                token,
                backend,
                index,
                expected_generation,
                actual_generation,
                raw,
                ..
            } => Self::BackendSpecific {
                code,
                token,
                backend,
                backend_context: context,
                index,
                expected_generation,
                actual_generation,
                raw,
            },
            Self::TokenOnly {
                reason: CompletionAnomalyReason::BackendContextUnknown,
                token,
                raw,
            } => {
                let backend = raw
                    .map(|raw| raw.backend)
                    .unwrap_or(CompletionBackend::Core);
                Self::BackendContext {
                    token,
                    backend,
                    backend_context: context,
                    raw: raw.unwrap_or(CompletionRaw {
                        backend,
                        res: 0,
                        flags: 0,
                    }),
                }
            }
            other => other,
        }
    }

    #[inline]
    pub fn with_event(mut self, event: CompletionEvent) -> Self {
        let raw = CompletionRaw {
            backend: self.backend().unwrap_or(CompletionBackend::Core),
            res: event.res,
            flags: event.flags,
        };
        self = self.with_token(event.token);
        self.with_raw(raw)
    }

    #[inline]
    pub fn with_token(mut self, token: CompletionToken) -> Self {
        match &mut self {
            Self::TokenOnly { token: slot, .. }
            | Self::UnknownSlot { token: slot, .. }
            | Self::StaleGeneration { token: slot, .. }
            | Self::SlotState { token: slot, .. }
            | Self::SlotCorruption { token: slot, .. }
            | Self::BackendContext { token: slot, .. }
            | Self::BackendSpecific { token: slot, .. } => *slot = token,
        }
        self
    }

    #[inline]
    pub fn with_raw_result(self, raw_result: i32) -> Self {
        let backend = self.backend().unwrap_or(CompletionBackend::Core);
        self.with_raw(CompletionRaw {
            backend,
            res: raw_result,
            flags: self.flags().unwrap_or(0),
        })
    }

    #[inline]
    pub fn with_flags(self, flags: u32) -> Self {
        let backend = self.backend().unwrap_or(CompletionBackend::Core);
        self.with_raw(CompletionRaw {
            backend,
            res: self.raw_result().unwrap_or(0),
            flags,
        })
    }

    #[inline]
    pub fn with_slot_snapshot(self, snapshot: slot::SlotSnapshot) -> Self {
        match self {
            Self::SlotState {
                reason, token, raw, ..
            } => Self::SlotState {
                reason,
                token,
                index: snapshot.index,
                generation: snapshot.generation,
                state: snapshot.state,
                snapshot: Some(snapshot),
                raw,
            },
            Self::SlotCorruption {
                reason, token, raw, ..
            } => Self::SlotCorruption {
                reason,
                token,
                snapshot,
                raw,
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn completion_anomaly_is_compact_enough_to_copy() {
        assert!(size_of::<CompletionAnomaly>() <= 72);
    }

    #[test]
    fn completion_anomaly_kind_is_lightweight() {
        assert!(size_of::<CompletionAnomalyKind>() <= 40);
    }

    #[test]
    fn anomaly_outcome_is_compact() {
        assert!(size_of::<AnomalyOutcome>() <= 48);
        assert!(size_of::<CompletionMutationOutcome>() <= 48);
    }

    #[test]
    fn unavailable_completion_attach_is_compact() {
        assert!(size_of::<(CompletionAnomalyKind, AnomalyAttach)>() <= 56);
    }
}
