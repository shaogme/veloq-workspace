use crate::slot;

use super::super::CompletionToken;

mod entity;
mod kind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionAnomalyReason {
    UnknownSlot,
    StaleGeneration,
    NonActiveSlot,
    BackendContextUnknown,
    BackendSpecific(u16),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionMutationOutcome {
    Applied,
    Rejected(AnomalyOutcome),
}

impl CompletionMutationOutcome {
    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub fn anomaly_outcome(&self) -> Option<AnomalyOutcome> {
        match self {
            Self::Applied => None,
            Self::Rejected(outcome) => Some(*outcome),
        }
    }

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
    pub fn kind(self) -> CompletionAnomalyKind {
        match self {
            Self::Missing(kind) | Self::Stale(kind) | Self::NonActive(kind) => kind,
        }
    }

    pub fn materialize(self, attach: AnomalyAttach) -> CompletionAnomaly {
        self.kind().materialize(attach)
    }
}

impl AnomalyAttach {
    pub const fn token_only(token: CompletionToken) -> Self {
        Self { token, raw: None }
    }

    pub fn from_op_token(token: crate::driver::OpToken) -> Self {
        Self {
            token: CompletionToken::user(token),
            raw: None,
        }
    }

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
