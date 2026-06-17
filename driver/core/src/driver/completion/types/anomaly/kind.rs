use crate::slot;

use super::{
    AnomalyAttach, BackendSlotRef, CompletionAnomaly, CompletionAnomalyKind,
    CompletionAnomalyReason, CompletionBackend, CompletionRaw, ControlAnomalyReason,
    SlotIssueReason,
};

impl CompletionAnomalyKind {
    pub const fn unknown_slot(index: usize, generation: u32) -> Self {
        Self::UnknownSlot { index, generation }
    }

    pub const fn stale(index: usize, expected: u32, actual: u32, state: slot::SlotState) -> Self {
        Self::Stale {
            index,
            expected,
            actual,
            state,
        }
    }

    pub const fn non_active(index: usize, generation: u32, state: slot::SlotState) -> Self {
        Self::NonActive {
            index,
            generation,
            state,
        }
    }

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

    pub const fn finalize_failed(index: usize, generation: u32, state: slot::SlotState) -> Self {
        Self::slot_issue(SlotIssueReason::FinalizeFailed, index, generation, state)
    }

    pub const fn finalize_failed_snapshot(snapshot: slot::SlotSnapshot) -> Self {
        Self::slot_issue_with_snapshot(SlotIssueReason::FinalizeFailed, snapshot)
    }

    pub const fn backend_context_unknown() -> Self {
        Self::Control {
            reason: ControlAnomalyReason::BackendContextUnknown,
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

    pub fn reason(self) -> CompletionAnomalyReason {
        match self {
            Self::UnknownSlot { .. } => CompletionAnomalyReason::UnknownSlot,
            Self::Stale { .. } => CompletionAnomalyReason::StaleGeneration,
            Self::NonActive { .. } => CompletionAnomalyReason::NonActiveSlot,
            Self::SlotIssue { reason, .. } => match reason {
                SlotIssueReason::FinalizeFailed => CompletionAnomalyReason::FinalizeFailed,
            },
            Self::Control { reason } => match reason {
                ControlAnomalyReason::BackendContextUnknown => {
                    CompletionAnomalyReason::BackendContextUnknown
                }
            },
            Self::BackendContext { .. } => CompletionAnomalyReason::BackendContextUnknown,
            Self::BackendSpecific { code, .. } => CompletionAnomalyReason::BackendSpecific(code),
        }
    }

    pub fn index(self) -> Option<usize> {
        match self {
            Self::UnknownSlot { index, .. }
            | Self::Stale { index, .. }
            | Self::NonActive { index, .. }
            | Self::SlotIssue { index, .. } => Some(index),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.index),
            Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn slot_snapshot(self) -> Option<slot::SlotSnapshot> {
        match self {
            Self::SlotIssue {
                snapshot: Some(snapshot),
                ..
            } => Some(snapshot),
            _ => None,
        }
    }

    pub fn expected_generation(self) -> Option<u32> {
        match self {
            Self::UnknownSlot { generation, .. } => Some(generation),
            Self::Stale { expected, .. } => Some(expected),
            Self::NonActive { generation, .. } => Some(generation),
            Self::SlotIssue { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => Some(slot.expected_generation),
            Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn actual_generation(self) -> Option<u32> {
        match self {
            Self::Stale { actual, .. } => Some(actual),
            Self::NonActive { generation, .. } => Some(generation),
            Self::SlotIssue { generation, .. } => Some(generation),
            Self::BackendSpecific {
                slot: Some(slot), ..
            } => slot.actual_generation,
            Self::UnknownSlot { .. } | Self::Control { .. } | Self::BackendContext { .. } => None,
            Self::BackendSpecific { slot: None, .. } => None,
        }
    }

    pub fn state(self) -> Option<slot::SlotState> {
        match self {
            Self::Stale { state, .. }
            | Self::NonActive { state, .. }
            | Self::SlotIssue { state, .. } => Some(state),
            Self::UnknownSlot { .. }
            | Self::Control { .. }
            | Self::BackendContext { .. }
            | Self::BackendSpecific { .. } => None,
        }
    }

    pub fn backend(self) -> Option<CompletionBackend> {
        match self {
            Self::BackendContext { backend, .. } | Self::BackendSpecific { backend, .. } => {
                Some(backend)
            }
            Self::UnknownSlot { .. }
            | Self::Stale { .. }
            | Self::NonActive { .. }
            | Self::SlotIssue { .. }
            | Self::Control { .. } => None,
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
            Self::SlotIssue {
                reason,
                index,
                generation,
                state,
                snapshot,
            } => {
                let mut anomaly = match reason {
                    SlotIssueReason::FinalizeFailed => {
                        CompletionAnomaly::finalize_failed(token, index, generation, state)
                    }
                };
                if let Some(snapshot) = snapshot {
                    anomaly = anomaly.with_slot_snapshot(snapshot);
                }
                anomaly
            }
            Self::Control { reason } => match reason {
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
