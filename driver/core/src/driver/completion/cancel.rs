use crate::slot::{self, CheckedSlotView, SlotView};

use super::routing::slot_view_kind;
use super::{CompletionAnomalyKind, CompletionAnomalyReason, OpToken};

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
    DiagnosticOnly { kind: CompletionAnomalyKind },
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
pub fn cancel_target_kind<'a, Spec: slot::SlotSpec>(
    token: OpToken,
    view: CheckedSlotView<'a, Spec>,
) -> (CancelTargetGoneReason, CompletionAnomalyKind) {
    let kind = match slot_view_kind(token, view) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomalyKind::backend_invariant_broken_snapshot(snapshot)
        }
        Err(kind) => kind,
    };
    let reason = match kind.reason() {
        CompletionAnomalyReason::StaleGeneration => CancelTargetGoneReason::Stale,
        CompletionAnomalyReason::OpMissing
        | CompletionAnomalyReason::PayloadMissing
        | CompletionAnomalyReason::SlotCorruption
        | CompletionAnomalyReason::BackendInvariantBroken => CancelTargetGoneReason::Corrupt,
        CompletionAnomalyReason::UnknownSlot
        | CompletionAnomalyReason::NonActiveSlot
        | CompletionAnomalyReason::UnknownControlToken
        | CompletionAnomalyReason::ControlCompletionUntracked
        | CompletionAnomalyReason::CompletionKeyMismatch
        | CompletionAnomalyReason::FinalizeFailed
        | CompletionAnomalyReason::CancelAckTargetStillActive
        | CompletionAnomalyReason::BackendContextUnknown
        | CompletionAnomalyReason::BackendSpecific(_) => CancelTargetGoneReason::Missing,
    };
    (reason, kind)
}
