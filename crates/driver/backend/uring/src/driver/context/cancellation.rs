use diagweave::prelude::*;
use veloq_std::vec::Vec;

use crate::{
    driver::{
        completion::{CompletionEngine, UringSyntheticCompletion},
        context::{DrivePortAccess, DrivePorts},
        lifecycle::{LifecycleCompletionAction, LifecycleEngine},
        submission::SubmissionEngine,
    },
    error::{UringError, UringResult},
};
use veloq_driver_core::driver::{
    CancelRequest, CancelSubmitOutcome, CompletionFlowOutcome, SyntheticCompletionSource,
};

pub(crate) struct CancellationCoordinator;

impl CancellationCoordinator {
    pub(crate) fn apply_lifecycle_actions(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
    ) -> UringResult<()> {
        let mut actions = Vec::new();
        lifecycle.drain_completion_actions_into(&mut actions);
        let mut first_error: Option<Report<UringError>> = None;
        for action in actions.drain(..) {
            let result = match action {
                LifecycleCompletionAction::SyntheticCancel { event, mode } => {
                    DrivePortAccess::accept_completion(
                        ports,
                        lifecycle,
                        completion,
                        |completion, context| {
                            completion.accept_synthetic_completion(
                                context,
                                event,
                                SyntheticCompletionSource::Cancel,
                                UringSyntheticCompletion::Cancel { mode },
                            )
                        },
                    )
                    .map(|_| ())
                }
                LifecycleCompletionAction::SyntheticSubmissionFailure { event, report } => {
                    DrivePortAccess::accept_completion(
                        ports,
                        lifecycle,
                        completion,
                        |completion, context| {
                            completion.accept_synthetic_completion(
                                context,
                                event,
                                SyntheticCompletionSource::SubmissionFailure,
                                UringSyntheticCompletion::SubmissionFailure {
                                    report: Some(report),
                                },
                            )
                        },
                    )
                    .map(|_| ())
                }
                LifecycleCompletionAction::Anomaly { kind, attach } => {
                    DrivePortAccess::accept_completion(
                        ports,
                        lifecycle,
                        completion,
                        |completion, context| {
                            completion.accept_completion_anomaly_kind(context, kind, attach)
                        },
                    )
                    .map(|_: CompletionFlowOutcome| ())
                }
            };
            if let Err(error) = result {
                first_error = Some(match first_error {
                    Some(previous) => previous.with_diag_src_err(error),
                    None => error,
                });
            }
        }
        lifecycle.restore_completion_actions(&mut actions);
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn cancel_operation(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        submission: &mut SubmissionEngine,
        request: CancelRequest,
    ) -> UringResult<CancelSubmitOutcome> {
        let result = DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
            lifecycle.cancel_op_internal(context, request)
        });
        let actions = Self::apply_lifecycle_actions(ports, lifecycle, completion);
        let combined = match (result, actions) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        };
        if combined.is_err() {
            submission.enter_fail_stop();
        }
        combined
    }

    pub(crate) fn drain_cancel_requests_bounded(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let result = DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
            lifecycle.drain_cancel_requests_bounded(context, limit)
        });
        let actions = Self::apply_lifecycle_actions(ports, lifecycle, completion);
        match (result, actions) {
            (Ok(count), Ok(())) => Ok(count),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        }
    }

    pub(crate) fn stage_pending_cancellations(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<usize> {
        let result = DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
            lifecycle.stage_pending_cancellations(context, limit)
        });
        let actions = Self::apply_lifecycle_actions(ports, lifecycle, completion);
        match (result, actions) {
            (Ok(count), Ok(())) => Ok(count),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(action_error)) => Err(action_error.with_diag_src_err(error)),
        }
    }
}
