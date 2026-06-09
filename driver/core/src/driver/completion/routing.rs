use crate::driver::registry::OpRegistry;
use crate::slot::{self, CheckedSlotView, SlotRegistryExt, SlotView};

use super::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionToken,
    DriverCompletionDiagnostics, OpToken, RawCompletion, UserCompletionEvent,
    record_completion_anomaly,
};

pub enum RoutedSlotCompletion<'a, Spec: slot::SlotSpec> {
    Waiting(slot::Slot<'a, slot::InFlightWaiting, Spec>),
    Orphaned(slot::Slot<'a, slot::InFlightOrphaned, Spec>),
    Missing(CompletionAnomaly),
    Empty(CompletionAnomaly),
    Stale(CompletionAnomaly),
    Corrupt(CompletionAnomaly),
}

impl<'a, Spec: slot::SlotSpec> RoutedSlotCompletion<'a, Spec> {
    #[inline]
    pub fn anomaly(&self) -> Option<&CompletionAnomaly> {
        match self {
            Self::Waiting(_) | Self::Orphaned(_) => None,
            Self::Missing(anomaly)
            | Self::Empty(anomaly)
            | Self::Stale(anomaly)
            | Self::Corrupt(anomaly) => Some(anomaly),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Finalized,
    Missing(CompletionAnomaly),
}

#[inline]
pub(super) fn finalize_waiting_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_waiting_completion(token).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
pub(super) fn finalize_orphaned_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_orphaned_completion(token).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
pub(super) fn finalize_corrupt_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    snapshot: slot::SlotSnapshot,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    let Ok(token) = snapshot.try_token() else {
        let raw = RawCompletion::new(
            backend,
            CompletionToken::from_raw(snapshot.index as u64),
            raw_res,
            flags,
        );
        let anomaly = CompletionAnomaly::finalize_failed(
            raw.token,
            snapshot.index,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw);
        record_completion_anomaly(diagnostics, &anomaly);
        return FinalizeOutcome::Missing(anomaly);
    };

    if registry.finalize_corrupt_slot(snapshot).is_some() {
        FinalizeOutcome::Finalized
    } else {
        record_finalize_failure(registry, diagnostics, backend, token, raw_res, flags)
    }
}

#[inline]
fn record_finalize_failure<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    backend: CompletionBackend,
    token: OpToken,
    raw_res: i32,
    flags: u32,
) -> FinalizeOutcome
where
    Spec: slot::SlotSpec,
{
    let raw = RawCompletion::new(backend, CompletionToken::user(token), raw_res, flags);
    let anomaly = match slot_view_anomaly(backend, token, raw, registry.checked_slot_view(token)) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomaly::finalize_failed(
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
    record_completion_anomaly(diagnostics, &anomaly);
    FinalizeOutcome::Missing(anomaly)
}

#[inline]
pub(super) fn route_user_completion<'a, Spec: slot::SlotSpec>(
    event: UserCompletionEvent,
    view: CheckedSlotView<'a, Spec>,
) -> RoutedSlotCompletion<'a, Spec> {
    let token = event.token();
    let raw = event.raw();
    match slot_view_anomaly(raw.backend, token, raw, view) {
        Ok(SlotView::InFlightWaiting(slot)) => RoutedSlotCompletion::Waiting(slot),
        Ok(SlotView::InFlightOrphaned(slot)) => RoutedSlotCompletion::Orphaned(slot),
        Ok(SlotView::Reserved(slot)) => {
            let snapshot = slot.snapshot();
            RoutedSlotCompletion::Corrupt(
                CompletionAnomaly::backend_invariant_broken(
                    raw.token,
                    snapshot.index,
                    snapshot.generation,
                    snapshot.state,
                )
                .with_slot_snapshot(snapshot)
                .with_raw_completion(raw),
            )
        }
        Err(anomaly) => match anomaly.reason {
            CompletionAnomalyReason::UnknownSlot => RoutedSlotCompletion::Missing(anomaly),
            CompletionAnomalyReason::NonActiveSlot => RoutedSlotCompletion::Empty(anomaly),
            CompletionAnomalyReason::StaleGeneration => RoutedSlotCompletion::Stale(anomaly),
            _ => RoutedSlotCompletion::Corrupt(anomaly),
        },
    }
}

#[inline]
pub(super) fn slot_view_anomaly<'a, Spec: slot::SlotSpec>(
    backend: CompletionBackend,
    token: OpToken,
    raw: RawCompletion,
    view: CheckedSlotView<'a, Spec>,
) -> Result<SlotView<'a, Spec>, CompletionAnomaly> {
    let raw = RawCompletion::new(backend, raw.token, raw.res, raw.flags);
    let (index, expected_generation) = token.parts();
    match view {
        CheckedSlotView::Valid(slot) => Ok(slot),
        CheckedSlotView::Missing { .. } => {
            Err(
                CompletionAnomaly::unknown_slot(raw.token, index, expected_generation)
                    .with_raw_completion(raw),
            )
        }
        CheckedSlotView::Empty(snapshot) => Err(CompletionAnomaly::non_active(
            raw.token,
            snapshot.index,
            expected_generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw)),
        CheckedSlotView::Stale(snapshot) => Err(CompletionAnomaly::stale(
            raw.token,
            snapshot.index,
            expected_generation,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(raw)),
        CheckedSlotView::Corrupt(snapshot) => Err(corrupt_slot_anomaly(raw.token, snapshot)
            .with_slot_snapshot(snapshot)
            .with_raw_completion(raw)),
    }
}

#[inline]
fn corrupt_slot_anomaly(token: CompletionToken, snapshot: slot::SlotSnapshot) -> CompletionAnomaly {
    if !snapshot.has_op {
        CompletionAnomaly::op_missing(token, snapshot.index, snapshot.generation)
    } else if !snapshot.has_payload {
        CompletionAnomaly::payload_missing(token, snapshot.index, snapshot.generation)
    } else {
        CompletionAnomaly::corrupt(token, snapshot.index, snapshot.generation, snapshot.state)
    }
}
