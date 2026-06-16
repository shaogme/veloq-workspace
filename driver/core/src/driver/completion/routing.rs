use crate::driver::registry::OpRegistry;
use crate::slot::{self, CheckedSlotView, SlotRegistryExt, SlotView};

use super::{
    AnomalyAttach, CompletionAnomalyKind, CompletionAnomalyReason, CompletionBackend,
    CompletionToken, DriverCompletionDiagnostics, OpToken, RawCompletion, UserCompletionEvent,
};

pub enum RoutedSlotCompletion<'a, Spec: slot::SlotSpec> {
    Waiting(slot::Slot<'a, slot::InFlightWaiting, Spec>),
    Orphaned(slot::Slot<'a, slot::InFlightOrphaned, Spec>),
    Missing(CompletionAnomalyKind),
    Empty(CompletionAnomalyKind),
    Stale(CompletionAnomalyKind),
    Corrupt(CompletionAnomalyKind),
}

impl<'a, Spec: slot::SlotSpec> RoutedSlotCompletion<'a, Spec> {
    #[inline]
    pub fn kind(&self) -> Option<CompletionAnomalyKind> {
        match self {
            Self::Waiting(_) | Self::Orphaned(_) => None,
            Self::Missing(kind) | Self::Empty(kind) | Self::Stale(kind) | Self::Corrupt(kind) => {
                Some(*kind)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Finalized,
    Missing(CompletionAnomalyKind),
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
        let kind = CompletionAnomalyKind::finalize_failed_snapshot(snapshot);
        let attach = AnomalyAttach::from_raw_completion(raw);
        diagnostics.record_anomaly_kind(kind, attach);
        return FinalizeOutcome::Missing(kind);
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
    let attach = AnomalyAttach::from_raw_completion(raw);
    let kind = match slot_view_kind(token, registry.checked_slot_view(token)) {
        Ok(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomalyKind::finalize_failed_snapshot(snapshot)
        }
        Err(kind) => kind,
    };
    diagnostics.record_anomaly_kind(kind, attach);
    FinalizeOutcome::Missing(kind)
}

#[inline]
pub(super) fn route_user_completion<'a, Spec: slot::SlotSpec>(
    event: UserCompletionEvent,
    view: CheckedSlotView<'a, Spec>,
) -> RoutedSlotCompletion<'a, Spec> {
    let token = event.token();
    match slot_view_kind(token, view) {
        Ok(SlotView::InFlightWaiting(slot)) => RoutedSlotCompletion::Waiting(slot),
        Ok(SlotView::InFlightOrphaned(slot)) => RoutedSlotCompletion::Orphaned(slot),
        Ok(SlotView::Reserved(slot)) => {
            let snapshot = slot.snapshot();
            RoutedSlotCompletion::Corrupt(CompletionAnomalyKind::backend_invariant_broken_snapshot(
                snapshot,
            ))
        }
        Err(kind) => match kind.reason() {
            CompletionAnomalyReason::UnknownSlot => RoutedSlotCompletion::Missing(kind),
            CompletionAnomalyReason::NonActiveSlot => RoutedSlotCompletion::Empty(kind),
            CompletionAnomalyReason::StaleGeneration => RoutedSlotCompletion::Stale(kind),
            _ => RoutedSlotCompletion::Corrupt(kind),
        },
    }
}

#[inline]
pub(super) fn slot_view_kind<'a, Spec: slot::SlotSpec>(
    token: OpToken,
    view: CheckedSlotView<'a, Spec>,
) -> Result<SlotView<'a, Spec>, CompletionAnomalyKind> {
    let (index, expected_generation) = token.parts();
    match view {
        CheckedSlotView::Valid(slot) => Ok(slot),
        CheckedSlotView::Missing { .. } => Err(CompletionAnomalyKind::unknown_slot(
            index,
            expected_generation,
        )),
        CheckedSlotView::Empty(snapshot) => Err(CompletionAnomalyKind::non_active(
            snapshot.index,
            expected_generation,
            snapshot.state,
        )),
        CheckedSlotView::Stale(snapshot) => Err(CompletionAnomalyKind::stale(
            snapshot.index,
            expected_generation,
            snapshot.generation,
            snapshot.state,
        )),
        CheckedSlotView::Corrupt(snapshot) => Err(corrupt_slot_kind(snapshot)),
    }
}

#[inline]
fn corrupt_slot_kind(snapshot: slot::SlotSnapshot) -> CompletionAnomalyKind {
    CompletionAnomalyKind::corrupt_snapshot(snapshot)
}
