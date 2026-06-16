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
pub enum CompletionMutationOutcome {
    Applied,
    Missing(CompletionAnomaly),
    Stale(CompletionAnomaly),
    NonActive(CompletionAnomaly),
    UnknownControl(CompletionAnomaly),
}

impl CompletionMutationOutcome {
    #[inline]
    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    #[inline]
    pub const fn anomaly(&self) -> Option<&CompletionAnomaly> {
        match self {
            Self::Applied => None,
            Self::Missing(anomaly)
            | Self::Stale(anomaly)
            | Self::NonActive(anomaly)
            | Self::UnknownControl(anomaly) => Some(anomaly),
        }
    }
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
}
