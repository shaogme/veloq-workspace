use crate::slot::{self, CheckedSlotView, SlotRegistryExt, SlotView};
use crate::{DriverCoreError, DriverError, driver::registry::OpRegistry};
use diagweave::prelude::*;
use veloq_std::format;

use super::{CompletionAnomalyKind, OpToken, UserCompletionEvent};

/// `checked_slot_view` 拿不到 slot 时的三种原因。
///
/// 它是 [`CompletionAnomalyKind`] 的真子集：后者还包含后端相关的变体，用它做路由就
/// 必须写 `_ => unreachable!()` 兜底。用独立类型表达之后，下游的 match 天然穷尽。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotLookupFailure {
    /// 索引越界——不存在这个 slot。
    Missing(CompletionAnomalyKind),
    /// slot 存在但当前不承载任何在途操作（`Idle`，可能信箱里还压着完成）。
    NonActive(CompletionAnomalyKind),
    /// slot 已被复用给新一代操作。
    Stale(CompletionAnomalyKind),
}

impl SlotLookupFailure {
    pub const fn kind(self) -> CompletionAnomalyKind {
        match self {
            Self::Missing(kind) | Self::NonActive(kind) | Self::Stale(kind) => kind,
        }
    }
}

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
    token: OpToken,
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_waiting_completion(token).is_some() {
        Ok(FinalizeOutcome::Finalized)
    } else {
        record_finalize_failure(registry, token)
    }
}

#[inline]
pub(super) fn finalize_orphaned_checked<Spec>(
    registry: &mut OpRegistry<Spec>,
    token: OpToken,
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    if registry.finalize_orphaned_completion(token).is_some() {
        Ok(FinalizeOutcome::Finalized)
    } else {
        record_finalize_failure(registry, token)
    }
}

#[inline]
fn record_finalize_failure<Spec>(
    registry: &mut OpRegistry<Spec>,
    token: OpToken,
) -> Result<FinalizeOutcome, Report<Spec::Error>>
where
    Spec: slot::SlotSpec,
{
    let snapshot = match registry.checked_slot_view(token)? {
        CheckedSlotView::Valid(slot) => match slot {
            SlotView::Reserved(s) => s.snapshot(),
            SlotView::InFlightWaiting(s) => s.snapshot(),
            SlotView::InFlightOrphaned(s) => s.snapshot(),
        },
        CheckedSlotView::Missing {
            index,
            expected_generation,
        } => slot::SlotSnapshot {
            index,
            generation: expected_generation,
            status: slot::SlotStatus::of(slot::SlotState::Idle),
            has_op: false,
            has_payload: false,
        },
        CheckedSlotView::Empty(s) | CheckedSlotView::Stale(s) => s,
    };
    let report = DriverCoreError::Internal
        .to_report()
        .push_ctx("scope", "record_finalize_failure")
        .attach_note(format!(
            "corrupt slot state: unable to finalize slot: {:?}",
            snapshot
        ));
    Err(Spec::Error::from_core_report(report))
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
        Err(SlotLookupFailure::Missing(kind)) => Ok(RoutedSlotCompletion::Missing(kind)),
        Err(SlotLookupFailure::NonActive(kind)) => Ok(RoutedSlotCompletion::Empty(kind)),
        Err(SlotLookupFailure::Stale(kind)) => Ok(RoutedSlotCompletion::Stale(kind)),
    }
}

#[inline]
pub(super) fn slot_view_kind<'a, Spec: slot::SlotSpec>(
    token: OpToken,
    view: CheckedSlotView<'a, Spec>,
) -> Result<SlotView<'a, Spec>, SlotLookupFailure> {
    let (index, expected_generation) = token.parts();
    match view {
        CheckedSlotView::Valid(slot) => Ok(slot),
        CheckedSlotView::Missing { .. } => Err(SlotLookupFailure::Missing(
            CompletionAnomalyKind::unknown_slot(index, expected_generation),
        )),
        CheckedSlotView::Empty(snapshot) => Err(SlotLookupFailure::NonActive(
            CompletionAnomalyKind::non_active(snapshot.index, expected_generation, snapshot.status),
        )),
        CheckedSlotView::Stale(snapshot) => {
            Err(SlotLookupFailure::Stale(CompletionAnomalyKind::stale(
                snapshot.index,
                expected_generation,
                snapshot.generation,
                snapshot.status,
            )))
        }
    }
}
