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
pub struct CompletionAnomaly {
    pub token: CompletionToken,
    pub index: Option<usize>,
    pub expected_generation: Option<u32>,
    pub actual_generation: Option<u32>,
    pub state: Option<slot::SlotState>,
    pub backend: Option<CompletionBackend>,
    pub backend_context: Option<u64>,
    pub raw_result: Option<i32>,
    pub flags: Option<u32>,
    pub slot_snapshot: Option<slot::SlotSnapshot>,
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::UnknownControlToken,
        }
    }

    #[inline]
    pub fn control_completion_untracked(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::ControlCompletionUntracked,
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::SlotCorruption,
        }
    }

    #[inline]
    pub fn op_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::OpMissing,
        }
    }

    #[inline]
    pub fn payload_missing(token: CompletionToken, index: usize, generation: u32) -> Self {
        Self {
            token,
            index: Some(index),
            expected_generation: Some(generation),
            actual_generation: Some(generation),
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::PayloadMissing,
        }
    }

    #[inline]
    pub fn corrupt_slot_snapshot(token: CompletionToken, snapshot: slot::SlotSnapshot) -> Self {
        let anomaly = if !snapshot.has_op {
            Self::op_missing(token, snapshot.index, snapshot.generation)
        } else if !snapshot.has_payload {
            Self::payload_missing(token, snapshot.index, snapshot.generation)
        } else {
            Self::corrupt(token, snapshot.index, snapshot.generation, snapshot.state)
        };
        anomaly.with_slot_snapshot(snapshot)
    }

    #[inline]
    pub fn backend_invariant_broken(
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::BackendInvariantBroken,
        }
    }

    #[inline]
    pub fn completion_key_mismatch(
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::CompletionKeyMismatch,
        }
    }

    #[inline]
    pub fn finalize_failed(
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::FinalizeFailed,
        }
    }

    #[inline]
    pub fn cancel_ack_target_still_active(
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
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::CancelAckTargetStillActive,
        }
    }

    #[inline]
    pub fn backend_context_unknown(token: CompletionToken) -> Self {
        Self {
            token,
            index: None,
            expected_generation: None,
            actual_generation: None,
            state: None,
            backend: None,
            backend_context: None,
            raw_result: None,
            flags: None,
            slot_snapshot: None,
            reason: CompletionAnomalyReason::BackendContextUnknown,
        }
    }

    #[inline]
    pub fn with_backend(mut self, backend: CompletionBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    #[inline]
    pub fn with_backend_context(mut self, context: u64) -> Self {
        self.backend_context = Some(context);
        self
    }

    #[inline]
    pub fn with_event(mut self, event: CompletionEvent) -> Self {
        self.token = event.token;
        self.raw_result = Some(event.res);
        self.flags = Some(event.flags);
        self
    }

    #[inline]
    pub fn with_raw_result(mut self, raw_result: i32) -> Self {
        self.raw_result = Some(raw_result);
        self
    }

    #[inline]
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = Some(flags);
        self
    }

    #[inline]
    pub fn with_slot_snapshot(mut self, snapshot: slot::SlotSnapshot) -> Self {
        self.index = Some(snapshot.index);
        self.actual_generation = Some(snapshot.generation);
        self.state = Some(snapshot.state);
        self.slot_snapshot = Some(snapshot);
        self
    }
}
