use crate::slot;

use super::super::CompletionToken;

mod entity;
mod kind;

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests;

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
pub(super) fn corrupt_reason_from_snapshot(
    snapshot: slot::SlotSnapshot,
) -> CompletionAnomalyReason {
    if !snapshot.has_op {
        CompletionAnomalyReason::OpMissing
    } else if !snapshot.has_payload {
        CompletionAnomalyReason::PayloadMissing
    } else {
        CompletionAnomalyReason::SlotCorruption
    }
}
