use crate::{
    DriverCoreError, DriverError, DriverResult,
    driver::OpRegistry,
    slot::{SlotCompletionDiagnostics, SlotError, SlotSpec},
};
use diagweave::{DiagnosticError, Report};

use super::super::{
    CompletionCleanupGuard, CompletionContinuation, CompletionSlotDisposition,
    DriverCompletionDiagnostics, DriverCompletionDiagnosticsBackend, SharedCompletionTable,
    UserCompletionEvent, routing::CompletionRouter,
};

#[derive(Clone, Copy)]
pub(super) enum FinalizeAction {
    Waiting(UserCompletionEvent),
    Orphaned(UserCompletionEvent),
}

pub(super) struct Settlement;

impl Settlement {
    pub(super) fn merge_error<Spec: SlotSpec>(
        primary: Option<Report<SlotError<Spec>>>,
        secondary: Option<Report<SlotError<Spec>>>,
    ) -> Option<Report<SlotError<Spec>>> {
        match (primary, secondary) {
            (Some(primary), Some(secondary)) => Some(primary.with_diag_src_err(secondary)),
            (Some(error), None) | (None, Some(error)) => Some(error),
            (None, None) => None,
        }
    }

    pub(super) fn run_cleanup<Spec>(
        diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
        cleanup: &mut CompletionCleanupGuard,
    ) -> Option<Report<SlotError<Spec>>>
    where
        Spec: SlotSpec,
        SlotError<Spec>: DriverError,
        SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    {
        match cleanup.run() {
            Ok(_) => None,
            Err(error) => {
                diagnostics.inc_orphan_cleanup_error();
                Some(SlotError::<Spec>::from_core_report(error))
            }
        }
    }

    pub(super) fn settle_slot_after_failure<Spec>(
        registry: &mut OpRegistry<Spec>,
        table: &SharedCompletionTable<Spec>,
        finalize: Option<FinalizeAction>,
        disposition: CompletionSlotDisposition,
        continuation: CompletionContinuation,
    ) -> Option<Report<SlotError<Spec>>>
    where
        Spec: SlotSpec,
        SlotError<Spec>: DriverError,
        SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    {
        let disposition_is_valid = match continuation {
            CompletionContinuation::Final => matches!(
                disposition,
                CompletionSlotDisposition::Finalized | CompletionSlotDisposition::NotApplicable
            ),
            CompletionContinuation::More => matches!(
                disposition,
                CompletionSlotDisposition::Quarantined | CompletionSlotDisposition::NotApplicable
            ),
        };
        let mut error = if disposition_is_valid {
            None
        } else {
            Some(Self::invalid_failure_disposition::<Spec>(
                "completion failure disposition does not match continuation",
            ))
        };

        let lifecycle_error = match continuation {
            CompletionContinuation::Final => match finalize {
                Some(FinalizeAction::Waiting(event)) => Self::finish_waiting_if_needed(
                    registry,
                    Some(FinalizeAction::Waiting(event)),
                    event,
                )
                .err(),
                Some(FinalizeAction::Orphaned(event)) => {
                    Self::finish_orphaned(registry, event).err()
                }
                None => None,
            },
            CompletionContinuation::More => match finalize {
                Some(FinalizeAction::Waiting(event)) => {
                    let outcome = table.mark_orphaned(event.token());
                    if outcome.is_applied() {
                        None
                    } else {
                        Some(Self::invalid_failure_disposition::<Spec>(
                            "unable to quarantine waiting completion slot",
                        ))
                    }
                }
                Some(FinalizeAction::Orphaned(_)) | None => None,
            },
        };
        error = Self::merge_error::<Spec>(error, lifecycle_error);
        error
    }

    pub(super) fn invalid_failure_disposition<Spec: SlotSpec>(
        note: &'static str,
    ) -> Report<SlotError<Spec>> {
        SlotError::<Spec>::from_core_report(
            DriverCoreError::Internal
                .to_report()
                .push_ctx("scope", "driver-core/completion.settlement")
                .attach_note(note),
        )
    }

    pub(super) fn finish_waiting_if_needed<Spec>(
        registry: &mut OpRegistry<Spec>,
        finalize: Option<FinalizeAction>,
        fallback_event: UserCompletionEvent,
    ) -> DriverResult<(), SlotError<Spec>>
    where
        Spec: SlotSpec,
        SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
        SlotError<Spec>: DriverError,
    {
        match finalize {
            Some(FinalizeAction::Waiting(event)) => {
                let _ = CompletionRouter::finalize_waiting_checked(registry, event.token())?;
            }
            Some(FinalizeAction::Orphaned(event)) => {
                let _ = CompletionRouter::finalize_orphaned_checked(registry, event.token())?;
            }
            None => {
                let _ =
                    CompletionRouter::finalize_waiting_checked(registry, fallback_event.token())?;
            }
        }
        Ok(())
    }

    pub(super) fn finish_orphaned<Spec>(
        registry: &mut OpRegistry<Spec>,
        event: UserCompletionEvent,
    ) -> DriverResult<(), SlotError<Spec>>
    where
        Spec: SlotSpec,
        SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
        SlotError<Spec>: DriverError,
    {
        let _ = CompletionRouter::finalize_orphaned_checked(registry, event.token())?;
        Ok(())
    }
}
