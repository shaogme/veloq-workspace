use crate::slot::{self, CheckedSlotView, SlotRegistryExt, SlotView};
use crate::{DriverCoreError, DriverError, driver::registry::OpRegistry};
use diagweave::prelude::*;

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
}

impl<'a, Spec: slot::SlotSpec> RoutedSlotCompletion<'a, Spec> {
    pub fn kind(&self) -> Option<CompletionAnomalyKind> {
        match self {
            Self::Waiting(_) | Self::Orphaned(_) => None,
            Self::Missing(kind) | Self::Empty(kind) | Self::Stale(kind) => Some(*kind),
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
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_waiting_completion(token).is_some() {
        Ok(FinalizeOutcome::Finalized)
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
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_orphaned_completion(token).is_some() {
        Ok(FinalizeOutcome::Finalized)
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
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    let raw = RawCompletion::new(backend, CompletionToken::user(token), raw_res, flags);
    let attach = AnomalyAttach::from_raw_completion(raw);
    let kind = match slot_view_kind(token, registry.checked_slot_view(token)?) {
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
    Ok(FinalizeOutcome::Missing(kind))
}

#[inline]
pub(super) fn route_user_completion<'a, Spec: slot::SlotSpec>(
    event: UserCompletionEvent,
    view: CheckedSlotView<'a, Spec>,
) -> Result<RoutedSlotCompletion<'a, Spec>, Report<Spec::Error>> {
    let token = event.token();
    match slot_view_kind(token, view) {
        Ok(SlotView::InFlightWaiting(slot)) => Ok(RoutedSlotCompletion::Waiting(slot)),
        Ok(SlotView::InFlightOrphaned(slot)) => Ok(RoutedSlotCompletion::Orphaned(slot)),
        Ok(SlotView::Reserved(slot)) => {
            let report = DriverCoreError::Internal
                .to_report()
                .push_ctx("scope", "route_user_completion")
                .attach_note(format!(
                    "corrupt slot state: Reserved slot completed: {:?}",
                    slot.snapshot()
                ));
            Err(Spec::Error::from_core_report(report))
        }
        Err(kind) => match kind.reason() {
            CompletionAnomalyReason::UnknownSlot => Ok(RoutedSlotCompletion::Missing(kind)),
            CompletionAnomalyReason::NonActiveSlot => Ok(RoutedSlotCompletion::Empty(kind)),
            CompletionAnomalyReason::StaleGeneration => Ok(RoutedSlotCompletion::Stale(kind)),
            _ => unreachable!(),
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
    }
}
