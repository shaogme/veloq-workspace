use veloq_driver_core::driver::{CancelRequest, CancelSubmitOutcome, OpToken};
use veloq_std::{format, vec::Vec};

use crate::{
    driver::{
        completion::CompletionEngine,
        context::{
            CancellationCoordinator, DrivePortAccess, DrivePorts, SubmitPort, SubmitPortParts,
        },
        control::UringControlPlane,
        lifecycle::{BacklogAction, BacklogProgress, LifecycleEngine},
        submission::SubmissionEngine,
    },
    error::{UringError, UringResult},
};

pub(crate) struct BacklogExecutor;

impl BacklogExecutor {
    pub(crate) fn stage_entries(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        submission: &mut SubmissionEngine,
        completion: &mut CompletionEngine,
        limit: usize,
    ) -> UringResult<BacklogProgress> {
        let progress = DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
            lifecycle.plan_backlog_entries(context, limit)
        })?;
        let mut actions = Vec::new();
        lifecycle.drain_backlog_actions_into(&mut actions);
        let mut progress = progress;
        let mut stop = false;
        for action in actions.drain(..) {
            if stop {
                Self::requeue_backlog_action(ports.control, action)?;
                continue;
            }
            match action {
                BacklogAction::Drop => {}
                BacklogAction::CancelQueued(token) => {
                    DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
                        lifecycle.complete_backlog_cancel(context, token)
                    })?;
                    progress.synthetic();
                }
                BacklogAction::CancelKernel(token) => {
                    match DrivePortAccess::with_lifecycle(
                        ports,
                        lifecycle,
                        |lifecycle, context| {
                            lifecycle.cancel_op_internal(context, CancelRequest::abandon(token))
                        },
                    )? {
                        CancelSubmitOutcome::Submitted => progress.submitted(),
                        CancelSubmitOutcome::CompletedLocally => progress.synthetic(),
                        CancelSubmitOutcome::CompletionPending => progress.mark_still_full(),
                        CancelSubmitOutcome::Queued
                        | CancelSubmitOutcome::AlreadyPending { .. }
                        | CancelSubmitOutcome::Merged { .. } => progress.mark_still_full(),
                        CancelSubmitOutcome::TargetGone { .. }
                        | CancelSubmitOutcome::NoBackendHandle => {}
                    }
                }
                BacklogAction::SubmitReserved(token) => {
                    match Self::submit_reserved(ports, submission, token) {
                        Ok(true) => progress.submitted(),
                        Ok(false) => {
                            progress.mark_still_full();
                            stop = true;
                            Self::requeue_backlog(ports.control, token)?;
                        }
                        Err(report) => {
                            DrivePortAccess::with_lifecycle(
                                ports,
                                lifecycle,
                                |lifecycle, context| {
                                    lifecycle.complete_backlog_submission_error(
                                        context, token, true, report,
                                    )
                                },
                            )?;
                            progress.synthetic();
                        }
                    }
                }
                BacklogAction::SubmitQueued(token) => {
                    match Self::submit_queued(ports, submission, token) {
                        Ok(true) => progress.submitted(),
                        Ok(false) => {
                            progress.mark_still_full();
                            stop = true;
                            Self::requeue_backlog(ports.control, token)?;
                        }
                        Err(report) => {
                            DrivePortAccess::with_lifecycle(
                                ports,
                                lifecycle,
                                |lifecycle, context| {
                                    lifecycle.complete_backlog_submission_error(
                                        context, token, false, report,
                                    )
                                },
                            )?;
                            progress.synthetic();
                        }
                    }
                }
            }
        }
        lifecycle.restore_backlog_actions(&mut actions);
        CancellationCoordinator::apply_lifecycle_actions(ports, lifecycle, completion)?;
        Ok(progress)
    }

    fn submit_reserved(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        submission: &mut SubmissionEngine,
        token: OpToken,
    ) -> UringResult<bool> {
        let DrivePorts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = ports;
        let parts = SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        };
        let mut context = SubmitPort::from_parts(parts);
        submission.submit_from_slot_token(&mut context, token)
    }

    fn submit_queued(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        submission: &mut SubmissionEngine,
        token: OpToken,
    ) -> UringResult<bool> {
        let DrivePorts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
            ..
        } = ports;
        let parts = SubmitPortParts {
            ops,
            control,
            registration,
            diagnostics,
            ring,
            kernel_capabilities,
        };
        let mut context = SubmitPort::from_parts(parts);
        submission.submit_queued_from_slot_token(&mut context, token)
    }

    fn requeue_backlog(control: &mut UringControlPlane, token: OpToken) -> UringResult<()> {
        control.push_backlog(token).map_err(|error| {
            UringError::InvalidState
                .report(
                    "uring.backlog.requeue",
                    format!("backlog rejected token: {error:?}"),
                )
                .with_ctx("token", token.index())
        })
    }

    fn requeue_backlog_action(
        control: &mut UringControlPlane,
        action: BacklogAction,
    ) -> UringResult<()> {
        let token = match action {
            BacklogAction::SubmitReserved(token)
            | BacklogAction::SubmitQueued(token)
            | BacklogAction::CancelQueued(token)
            | BacklogAction::CancelKernel(token) => token,
            BacklogAction::Drop => return Ok(()),
        };
        Self::requeue_backlog(control, token)
    }
}
