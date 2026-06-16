use crate::slot::{self, CheckedSlotView, SlotView};

use super::routing::slot_view_anomaly;
use super::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionToken, OpToken,
    RawCompletion,
};

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
    DiagnosticOnly { anomaly: CompletionAnomaly },
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
pub fn cancel_target_anomaly<'a, Spec: slot::SlotSpec>(
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
    view: CheckedSlotView<'a, Spec>,
) -> (CancelTargetGoneReason, CompletionAnomaly) {
    let raw = RawCompletion::new(backend, CompletionToken::user(token), raw_res, flags);
    let anomaly = match slot_view_anomaly(backend, token, raw, view) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomaly::backend_invariant_broken(
                raw.token,
                snapshot.index,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw)
        }
        Err(anomaly) => anomaly,
    };
    let reason = match anomaly.reason() {
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
    (reason, anomaly)
}
