use crate::slot::{self, CheckedSlotView, SlotView};

use super::routing::{SlotLookupFailure, slot_view_kind};
use super::{CancelTicket, CompletionAnomalyKind, OpToken};

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
    /// A cancel intent for this target already exists and has the same visibility mode.
    AlreadyPending {
        ticket: CancelTicket,
    },
    /// A new cancel request was merged into the existing intent. `Abandon` takes precedence.
    Merged {
        ticket: CancelTicket,
    },
    CompletedLocally,
    TargetGone {
        reason: CancelTargetGoneReason,
    },
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
    match slot_view_kind(token, view) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            (
                CancelTargetGoneReason::Missing,
                CompletionAnomalyKind::non_active(
                    snapshot.index,
                    token.generation(),
                    snapshot.status,
                ),
            )
        }
        Err(failure @ (SlotLookupFailure::Missing(_) | SlotLookupFailure::NonActive(_))) => {
            (CancelTargetGoneReason::Missing, failure.kind())
        }
        Err(failure @ SlotLookupFailure::Stale(_)) => {
            (CancelTargetGoneReason::Stale, failure.kind())
        }
    }
}
