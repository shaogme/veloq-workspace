use crate::{
    driver::OpToken,
    slot::{self, Generation},
};
use veloq_std::num::NonZeroU8;

use super::super::{CompletionEvent, CompletionToken, RawCompletion};

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
    pub expected_generation: Generation,
    pub actual_generation: Option<Generation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionBackend {
    Core,
    Backend(NonZeroU8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSpecificDetails {
    pub code: u16,
    pub token: CompletionToken,
    pub backend: CompletionBackend,
    pub backend_context: u64,
    pub slot: BackendSlotRef,
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
        generation: Generation,
    },
    Stale {
        index: usize,
        expected: Generation,
        actual: Generation,
        status: slot::SlotStatus,
    },
    NonActive {
        index: usize,
        generation: Generation,
        status: slot::SlotStatus,
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
    token: CompletionToken,
    raw: Option<CompletionRaw>,
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
    UnknownSlot {
        token: CompletionToken,
        index: usize,
        expected_generation: Generation,
        raw: Option<CompletionRaw>,
    },
    StaleGeneration {
        token: CompletionToken,
        index: usize,
        expected_generation: Generation,
        actual_generation: Generation,
        status: slot::SlotStatus,
        raw: Option<CompletionRaw>,
    },
    SlotState {
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        index: usize,
        generation: Generation,
        status: slot::SlotStatus,
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
        expected_generation: Option<Generation>,
        actual_generation: Option<Generation>,
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

    pub fn from_op_token(token: OpToken) -> Self {
        Self {
            token: CompletionToken::user(token),
            raw: None,
        }
    }

    pub fn from_raw_completion(raw: RawCompletion) -> Self {
        Self {
            token: raw.token(),
            raw: Some(CompletionRaw {
                backend: raw.backend(),
                res: raw.res(),
                flags: raw.flags(),
            }),
        }
    }

    pub const fn token(self) -> CompletionToken {
        self.token
    }

    pub const fn raw(self) -> Option<CompletionRaw> {
        self.raw
    }
}

impl CompletionAnomaly {
    pub fn reason(self) -> CompletionAnomalyReason {
        match self {
            Self::SlotState { reason, .. } => reason,
            Self::UnknownSlot { .. } => CompletionAnomalyReason::UnknownSlot,
            Self::StaleGeneration { .. } => CompletionAnomalyReason::StaleGeneration,
            Self::BackendContext { .. } => CompletionAnomalyReason::BackendContextUnknown,
            Self::BackendSpecific { code, .. } => CompletionAnomalyReason::BackendSpecific(code),
        }
    }

    pub fn token(self) -> CompletionToken {
        match self {
            Self::UnknownSlot { token, .. }
            | Self::StaleGeneration { token, .. }
            | Self::SlotState { token, .. }
            | Self::BackendContext { token, .. }
            | Self::BackendSpecific { token, .. } => token,
        }
    }

    pub fn index(self) -> Option<usize> {
        match self {
            Self::UnknownSlot { index, .. }
            | Self::StaleGeneration { index, .. }
            | Self::SlotState { index, .. } => Some(index),
            Self::BackendSpecific { index, .. } => index,
            Self::BackendContext { .. } => None,
        }
    }

    pub fn expected_generation(self) -> Option<Generation> {
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
            Self::BackendSpecific {
                expected_generation,
                ..
            } => expected_generation,
            Self::BackendContext { .. } => None,
        }
    }

    pub fn actual_generation(self) -> Option<Generation> {
        match self {
            Self::StaleGeneration {
                actual_generation, ..
            } => Some(actual_generation),
            Self::SlotState { generation, .. } => Some(generation),
            Self::BackendSpecific {
                actual_generation, ..
            } => actual_generation,
            Self::UnknownSlot { .. } | Self::BackendContext { .. } => None,
        }
    }

    pub fn status(self) -> Option<slot::SlotStatus> {
        match self {
            Self::StaleGeneration { status, .. } | Self::SlotState { status, .. } => Some(status),
            Self::UnknownSlot { .. }
            | Self::BackendContext { .. }
            | Self::BackendSpecific { .. } => None,
        }
    }

    pub fn slot_snapshot(self) -> Option<slot::SlotSnapshot> {
        match self {
            Self::SlotState {
                snapshot: Some(snapshot),
                ..
            } => Some(snapshot),
            _ => None,
        }
    }

    pub fn backend(self) -> Option<CompletionBackend> {
        match self {
            Self::BackendContext { backend, .. } | Self::BackendSpecific { backend, .. } => {
                Some(backend)
            }
            Self::UnknownSlot { raw: Some(raw), .. }
            | Self::StaleGeneration { raw: Some(raw), .. }
            | Self::SlotState { raw: Some(raw), .. } => Some(raw.backend),
            Self::UnknownSlot { raw: None, .. }
            | Self::StaleGeneration { raw: None, .. }
            | Self::SlotState { raw: None, .. } => None,
        }
    }

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

    pub fn raw_result(self) -> Option<i32> {
        self.raw_attachment().map(|raw| raw.res)
    }

    pub fn flags(self) -> Option<u32> {
        self.raw_attachment().map(|raw| raw.flags)
    }

    fn raw_attachment(self) -> Option<CompletionRaw> {
        match self {
            Self::UnknownSlot { raw, .. }
            | Self::StaleGeneration { raw, .. }
            | Self::SlotState { raw, .. }
            | Self::BackendSpecific { raw, .. } => raw,
            Self::BackendContext { raw, .. } => Some(raw),
        }
    }

    fn slot_state(
        reason: CompletionAnomalyReason,
        token: CompletionToken,
        index: usize,
        generation: Generation,
        status: slot::SlotStatus,
    ) -> Self {
        Self::SlotState {
            reason,
            token,
            index,
            generation,
            status,
            snapshot: None,
            raw: None,
        }
    }

    pub fn unknown_slot(token: CompletionToken, index: usize, generation: Generation) -> Self {
        Self::UnknownSlot {
            token,
            index,
            expected_generation: generation,
            raw: None,
        }
    }

    pub fn stale(
        token: CompletionToken,
        index: usize,
        expected_generation: Generation,
        actual_generation: Generation,
        status: slot::SlotStatus,
    ) -> Self {
        Self::StaleGeneration {
            token,
            index,
            expected_generation,
            actual_generation,
            status,
            raw: None,
        }
    }

    pub fn non_active(
        token: CompletionToken,
        index: usize,
        generation: Generation,
        status: slot::SlotStatus,
    ) -> Self {
        Self::slot_state(
            CompletionAnomalyReason::NonActiveSlot,
            token,
            index,
            generation,
            status,
        )
    }

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

    pub fn backend_specific_slot(details: BackendSpecificDetails) -> Self {
        Self::BackendSpecific {
            code: details.code,
            token: details.token,
            backend: details.backend,
            backend_context: details.backend_context,
            index: Some(details.slot.index),
            expected_generation: Some(details.slot.expected_generation),
            actual_generation: details.slot.actual_generation,
            raw: None,
        }
    }

    pub fn with_raw(mut self, raw: CompletionRaw) -> Self {
        match &mut self {
            Self::UnknownSlot { raw: slot, .. }
            | Self::StaleGeneration { raw: slot, .. }
            | Self::SlotState { raw: slot, .. }
            | Self::BackendSpecific { raw: slot, .. } => *slot = Some(raw),
            Self::BackendContext { raw: slot, .. } => *slot = raw,
        }
        self
    }

    pub fn with_backend(self, backend: CompletionBackend) -> Self {
        match self {
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
            other => other,
        }
    }

    pub fn with_event(mut self, event: CompletionEvent) -> Self {
        let raw = CompletionRaw {
            backend: self.backend().unwrap_or(CompletionBackend::Core),
            res: event.res(),
            flags: event.flags(),
        };
        self = self.with_token(event.token());
        self.with_raw(raw)
    }

    pub fn with_token(mut self, token: CompletionToken) -> Self {
        match &mut self {
            Self::UnknownSlot { token: slot, .. }
            | Self::StaleGeneration { token: slot, .. }
            | Self::SlotState { token: slot, .. }
            | Self::BackendContext { token: slot, .. }
            | Self::BackendSpecific { token: slot, .. } => *slot = token,
        }
        self
    }

    pub fn with_raw_result(self, raw_result: i32) -> Self {
        let backend = self.backend().unwrap_or(CompletionBackend::Core);
        self.with_raw(CompletionRaw {
            backend,
            res: raw_result,
            flags: self.flags().unwrap_or(0),
        })
    }

    pub fn with_flags(self, flags: u32) -> Self {
        let backend = self.backend().unwrap_or(CompletionBackend::Core);
        self.with_raw(CompletionRaw {
            backend,
            res: self.raw_result().unwrap_or(0),
            flags,
        })
    }

    pub fn with_slot_snapshot(self, snapshot: slot::SlotSnapshot) -> Self {
        match self {
            Self::SlotState {
                reason, token, raw, ..
            } => Self::SlotState {
                reason,
                token,
                index: snapshot.index(),
                generation: snapshot.generation(),
                status: snapshot.status(),
                snapshot: Some(snapshot),
                raw,
            },
            other => other,
        }
    }
}

impl CompletionAnomalyKind {
    pub const fn unknown_slot(index: usize, generation: Generation) -> Self {
        Self::UnknownSlot { index, generation }
    }

    pub const fn stale(
        index: usize,
        expected: Generation,
        actual: Generation,
        status: slot::SlotStatus,
    ) -> Self {
        Self::Stale {
            index,
            expected,
            actual,
            status,
        }
    }

    pub const fn non_active(
        index: usize,
        generation: Generation,
        status: slot::SlotStatus,
    ) -> Self {
        Self::NonActive {
            index,
            generation,
            status,
        }
    }

    pub const fn backend_context(backend: CompletionBackend, backend_context: u64) -> Self {
        Self::BackendContext {
            backend,
            backend_context,
        }
    }

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

    pub const fn backend_specific_missing(
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
        index: usize,
        expected_generation: Generation,
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

    pub const fn backend_specific_stale(
        code: u16,
        backend: CompletionBackend,
        backend_context: u64,
        slot: BackendSlotRef,
    ) -> Self {
        Self::BackendSpecific {
            code,
            backend,
            backend_context,
            slot: Some(slot),
        }
    }

    pub fn reason(self) -> CompletionAnomalyReason {
        match self {
            Self::UnknownSlot { .. } => CompletionAnomalyReason::UnknownSlot,
            Self::Stale { .. } => CompletionAnomalyReason::StaleGeneration,
            Self::NonActive { .. } => CompletionAnomalyReason::NonActiveSlot,
            Self::BackendContext { .. } => CompletionAnomalyReason::BackendContextUnknown,
            Self::BackendSpecific { code, .. } => CompletionAnomalyReason::BackendSpecific(code),
        }
    }

    pub fn index(self) -> Option<usize> {
        match self {
            Self::UnknownSlot { index, .. }
            | Self::Stale { index, .. }
            | Self::NonActive { index, .. } => Some(index),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.index),
            Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn expected_generation(self) -> Option<Generation> {
        match self {
            Self::UnknownSlot { generation, .. } => Some(generation),
            Self::Stale { expected, .. } => Some(expected),
            Self::NonActive { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.expected_generation),
            Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn actual_generation(self) -> Option<Generation> {
        match self {
            Self::Stale { actual, .. } => Some(actual),
            Self::NonActive { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => slot.actual_generation,
            Self::UnknownSlot { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn status(self) -> Option<slot::SlotStatus> {
        match self {
            Self::Stale { status, .. } | Self::NonActive { status, .. } => Some(status),
            Self::UnknownSlot { .. }
            | Self::BackendContext { .. }
            | Self::BackendSpecific { .. } => None,
        }
    }

    pub fn backend(self) -> Option<CompletionBackend> {
        match self {
            Self::BackendContext { backend, .. } | Self::BackendSpecific { backend, .. } => {
                Some(backend)
            }
            Self::UnknownSlot { .. } | Self::Stale { .. } | Self::NonActive { .. } => None,
        }
    }

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

    pub fn materialize(self, attach: AnomalyAttach) -> CompletionAnomaly {
        let token = attach.token();
        let raw = attach.raw();
        let anomaly = match self {
            Self::UnknownSlot { index, generation } => {
                CompletionAnomaly::unknown_slot(token, index, generation)
            }
            Self::Stale {
                index,
                expected,
                actual,
                status,
            } => CompletionAnomaly::stale(token, index, expected, actual, status),
            Self::NonActive {
                index,
                generation,
                status,
            } => CompletionAnomaly::non_active(token, index, generation, status),

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
                Some(slot) => CompletionAnomaly::backend_specific_slot(BackendSpecificDetails {
                    code,
                    token,
                    backend,
                    backend_context,
                    slot,
                }),
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
