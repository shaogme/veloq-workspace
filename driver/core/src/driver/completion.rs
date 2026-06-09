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

pub use cancel::{
    CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason, cancel_target_anomaly,
};
pub use event::{
    CompletionDispatch, CompletionEnvelope, CompletionEvent, CompletionIdentity,
    CompletionIdentitySource, RawCompletion, UserCompletionEvent, UserCompletionEventMismatch,
};
pub use flow::{
    CompletionBackendHooks, CompletionBackendIngressAction, CompletionControl, CompletionFlowExt,
    CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress, CompletionSource,
    CompletionWritePermit, SyntheticCompletionSource,
};
pub use packet::{
    CompletionInput, CompletionLoss, CompletionPacket, CompletionRecord, UserCompletion,
};
pub use routing::{FinalizeOutcome, RoutedSlotCompletion};
pub use table::{
    CELL_STATE_BUSY, CELL_STATE_IDLE, CELL_STATE_ORPHANED, CELL_STATE_READY, CELL_STATE_WAITING,
    CompletionAccess, PollRecordResult, SharedCompletionTable,
};
pub use token::{
    CancelCompletionId, CompletionControlKind, CompletionToken, CompletionTokenClass, OpToken,
    OpTokenError, SubmitTokenContext,
};
pub use types::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionCleanup,
    CompletionCleanupGuard, CompletionMutationOutcome, DriverCompletionDiagnostics,
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
    #[inline]
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

use event::{dispatch_envelope, unknown_completion_anomaly};
use packet::{record_completion_anomaly, run_completion_cleanup, run_rejected_cleanup};
use routing::{
    finalize_corrupt_checked, finalize_orphaned_checked, finalize_waiting_checked,
    route_user_completion,
};
