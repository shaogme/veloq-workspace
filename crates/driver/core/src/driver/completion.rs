use crate::{DriverCoreError, DriverError, DriverResult};

use diagweave::prelude::*;

mod cancel;
mod event;
mod flow;
mod packet;
mod routing;
mod table;
mod token;
mod types;

#[cfg(test)]
mod settlement_model;

pub use cancel::{
    CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason, cancel_target_kind,
};
pub use event::{
    CompletionDispatch, CompletionEnvelope, CompletionEvent, CompletionIdentity,
    CompletionIdentitySource, RawCompletion, UserCompletionEvent, UserCompletionEventMismatch,
};
pub use flow::{
    CompletionBackendHooks, CompletionBackendIngressAction, CompletionControl, CompletionFailure,
    CompletionFlowExt, CompletionFlowOutcome, CompletionIngress, CompletionSettlement,
    CompletionSlotDisposition, CompletionSource, CompletionWritePermit, HookResult,
    SyntheticCompletionSource,
};
pub use packet::{
    CompletionContinuation, CompletionInput, CompletionPacket, CompletionRecord, UserCompletion,
    run_completion_cleanup,
};
pub use routing::{FinalizeOutcome, RoutedSlotCompletion, SlotLookupFailure};
pub use table::{
    CELL_STATE_BUSY, CELL_STATE_IDLE, CELL_STATE_ORPHANED, CELL_STATE_READY, CELL_STATE_WAITING,
    CompletionAccess, PollRecordResult, SharedCompletionTable,
};
pub use token::{
    CancelTicket, CancelTicketError, CompletionControlKind, CompletionToken, CompletionTokenClass,
    CompletionTokenError, OpToken, OpTokenError, SubmitTokenContext,
};
pub use types::{
    AnomalyAttach, AnomalyOutcome, BackendSlotRef, CompletionAnomaly, CompletionAnomalyKind,
    CompletionAnomalyReason, CompletionBackend, CompletionCleanup, CompletionCleanupGuard,
    CompletionMutationOutcome, CompletionRaw, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsBackend, DriverCompletionDiagnosticsSnapshot,
    RecordCompletionOutcome, RecordCompletionResult,
};

pub trait CompletionValue: Send {
    fn from_event_res<E>(res: i32) -> DriverResult<Self, E>
    where
        Self: Sized,
        E: DriverError;
}

impl CompletionValue for usize {
    fn from_event_res<E>(res: i32) -> DriverResult<Self, E>
    where
        E: DriverError,
    {
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(E::from_core_report(
                DriverCoreError::System
                    .to_report()
                    .push_ctx("scope", "driver-core/completion")
                    .set_error_code(-res)
                    .attach_note("completion reported OS error"),
            ))
        }
    }
}

use event::dispatch_envelope;
use packet::run_rejected_cleanup;
use routing::{finalize_orphaned_checked, finalize_waiting_checked, route_user_completion};
