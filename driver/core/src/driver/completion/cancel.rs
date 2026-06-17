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
    pub const fn new(target: OpToken, mode: CancelMode) -> Self {
        Self { target, mode }
    }

    pub const fn user_visible(target: OpToken) -> Self {
        Self::new(target, CancelMode::UserVisible)
    }

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
    NoBackendHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTargetGoneReason {
    Missing,
    Stale,
    Corrupt,
}

impl CancelSubmitOutcome {
    pub const fn target_missing() -> Self {
        Self::TargetGone {
            reason: CancelTargetGoneReason::Missing,
        }
    }

    pub const fn target_stale() -> Self {
        Self::TargetGone {
            reason: CancelTargetGoneReason::Stale,
        }
    }

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
            CompletionAnomalyKind::non_active(snapshot.index, token.generation(), snapshot.state)
        }
        Err(kind) => kind,
    };
    let reason = match kind.reason() {
        CompletionAnomalyReason::StaleGeneration => CancelTargetGoneReason::Stale,
        CompletionAnomalyReason::UnknownSlot
        | CompletionAnomalyReason::NonActiveSlot
        | CompletionAnomalyReason::BackendContextUnknown
        | CompletionAnomalyReason::BackendSpecific(_) => CancelTargetGoneReason::Missing,
    };
    (reason, kind)
}
