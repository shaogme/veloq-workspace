use crate::{
    driver::{
        PendingCancel, UringDriver,
        completion::{COMP_BACKEND_URING, UringSyntheticCompletion},
        control::{BacklogStageKind, ControlPlaneEvent},
        submission::{submit_queued_from_slot, txn::slot_access_report},
    },
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{CheckedSlotView, Slot, SlotState, SlotView, UringOpRegistryExt},
};
use diagweave::prelude::*;
use io_uring::opcode;
use tracing::{debug, trace};
use veloq_driver_core::driver::{
    AnomalyAttach, CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason,
    CancelTicket, CompletionToken, OpToken, SyntheticCompletionSource, UserCompletionEvent,
    cancel_target_kind,
};
use veloq_std::vec::Vec;
use veloq_wheel::TaskId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SubmissionPhase {
    #[default]
    Reserved,
    SqeStaged,
    KernelOutstanding,
    TimerArmed,
    Terminal,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CancellationPhase {
    #[default]
    None,
    Requested,
    CancelStaged,
    CancelOutstanding,
    Acked,
    NotFound,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UringOpControl {
    pub(crate) submission: SubmissionPhase,
    pub(crate) cancellation: CancellationPhase,
}

#[derive(Clone, Default)]
pub struct UringOpState {
    pub(crate) timer_id: Option<TaskId>,
    pub(crate) control: UringOpControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BacklogActionResult {
    Submitted,
    StillFull,
    VoidRecovery,
    SyntheticCompletion,
    FatalControlError,
}

#[derive(Debug, Default)]
pub(crate) struct BacklogProgress {
    pub(crate) actions: Vec<BacklogActionResult>,
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<'a> UringDriver<'a> {
    fn try_submit_cancel_request(
        &mut self,
        request: PendingCancel,
    ) -> UringResult<Option<CancelTicket>> {
        let (user_data, generation) = request.user_parts();

        let cancel_ticket = self
            .control
            .cancellations
            .allocate_cancel_ticket()
            .map_err(|_| {
                self.completion_diagnostics
                    .backend()
                    .inc_cancel_ticket_exhausted();
                UringError::CancelTicketExhausted
                    .report(
                        "uring.cancel.allocate_ticket",
                        "cancel ticket space exhausted",
                    )
                    .attach_note("target remains active and no cancel map entry was overwritten")
            })?;
        let cancel_sqe = opcode::AsyncCancel::new(CompletionToken::user(request.target).raw())
            .build()
            .user_data(CompletionToken::cancel(cancel_ticket).raw());

        if self.push_entry(cancel_sqe) == crate::driver::env::StageResult::Staged {
            if self.control.stage_cancel(cancel_ticket, request).is_err() {
                self.completion_diagnostics
                    .backend()
                    .inc_cancel_duplicate_ticket();
                return Err(UringError::InvalidState
                    .report(
                        "uring.cancel.stage_ticket",
                        "cancel ticket was already in use",
                    )
                    .attach_note("staged cancel bookkeeping could not be made authoritative"));
            }
            self.set_cancel_phase(request.target, CancellationPhase::CancelStaged);
            self.completion_diagnostics.backend().inc_cancel_submitted();
            trace!(
                user_data,
                generation = generation.get(),
                cancel_ticket = cancel_ticket.raw(),
                mode = ?request.mode,
                "submitted async cancel"
            );
            Ok(Some(cancel_ticket))
        } else {
            Ok(None)
        }
    }

    fn submit_cancel_request(
        &mut self,
        request: PendingCancel,
    ) -> UringResult<CancelSubmitOutcome> {
        if self.try_submit_cancel_request(request)?.is_some() {
            Ok(CancelSubmitOutcome::Submitted)
        } else {
            self.control.cancellations.push_pending(request);
            self.control
                .record(ControlPlaneEvent::CancelPendingPush(request.target));
            self.completion_diagnostics.backend().inc_cancel_queued();
            Ok(CancelSubmitOutcome::Queued)
        }
    }

    fn complete_local_cancel(&mut self, token: OpToken, mode: CancelMode) -> UringResult<()> {
        self.completion_diagnostics
            .backend()
            .inc_cancel_local_completed();
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, -libc::ECANCELED, 0);
        self.accept_synthetic_completion(
            event,
            SyntheticCompletionSource::Cancel,
            UringSyntheticCompletion::Cancel { mode },
        )?;
        Ok(())
    }

    pub(crate) fn set_cancel_phase(&mut self, token: OpToken, phase: CancellationPhase) {
        let Ok(view) = self.ops.checked_slot_view(token) else {
            return;
        };
        match view {
            CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                slot.platform_mut().control.cancellation = phase;
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                slot.platform_mut().control.cancellation = phase;
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                slot.platform_mut().control.cancellation = phase;
            }
            CheckedSlotView::Empty(_)
            | CheckedSlotView::Missing { .. }
            | CheckedSlotView::Stale(_) => {}
        }
    }

    pub(crate) fn cancel_op_internal(
        &mut self,
        request: CancelRequest,
    ) -> UringResult<CancelSubmitOutcome> {
        let request = PendingCancel::new(request);
        let (user_data, generation) = request.user_parts();
        let token = request.target;

        match self.ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                let prepared = if slot.has_op() {
                    match slot.start_submission_with(None) {
                        Ok(guard) => {
                            let _ = guard.persist();
                            true
                        }
                        Err(err) => {
                            debug!(
                                user_data,
                                generation = generation.get(),
                                snapshot = ?err.snapshot,
                                "reserved uring cancel could not prepare synthetic completion"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
                if prepared {
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, request.mode)?;
                } else {
                    let _ = self.ops.remove(token);
                }
                Ok(CancelSubmitOutcome::CompletedLocally)
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                if slot.platform().control.submission == SubmissionPhase::Reserved
                    && self.control.backlog.contains(token)
                {
                    self.remove_backlog_token(token);
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, request.mode)?;
                    return Ok(CancelSubmitOutcome::CompletedLocally);
                }

                if slot.platform().control.submission == SubmissionPhase::TimerArmed
                    && let Some(tid) = slot.platform_mut().timer_id.take()
                {
                    self.control.timers.cancel(tid);
                    self.control.record(ControlPlaneEvent::TimerCancel {
                        task_id: tid,
                        token,
                    });
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, request.mode)?;
                    return Ok(CancelSubmitOutcome::CompletedLocally);
                }

                if request.mode == CancelMode::Abandon {
                    let _ = slot.cancel();
                }
                self.set_cancel_phase(token, CancellationPhase::Requested);
                self.submit_cancel_request(request)
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                if slot.platform().control.submission == SubmissionPhase::Reserved
                    && self.control.backlog.contains(token)
                {
                    self.remove_backlog_token(token);
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, CancelMode::Abandon)?;
                    return Ok(CancelSubmitOutcome::CompletedLocally);
                }

                if slot.platform().control.submission == SubmissionPhase::TimerArmed
                    && let Some(tid) = slot.platform_mut().timer_id.take()
                {
                    self.control.timers.cancel(tid);
                    self.control.record(ControlPlaneEvent::TimerCancel {
                        task_id: tid,
                        token,
                    });
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, CancelMode::Abandon)?;
                    return Ok(CancelSubmitOutcome::CompletedLocally);
                }

                self.set_cancel_phase(token, CancellationPhase::Requested);
                self.submit_cancel_request(request)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)) => {
                let (reason, kind) = cancel_target_kind(token, view);
                self.record_cancel_target_gone(reason);
                let attach = AnomalyAttach::from_op_token(token);
                let _ = self.accept_completion_anomaly_kind(kind, attach);
                debug!(
                    user_data,
                    generation = generation.get(),
                    token = CompletionToken::user(request.target).raw(),
                    reason = ?reason,
                    "cancel request did not match an active uring slot"
                );
                Ok(CancelSubmitOutcome::TargetGone { reason })
            }
        }
    }

    pub(crate) fn flush_cancellations(&mut self) -> UringResult<()> {
        let mut submitted_count = 0;
        let limit = self.control.cancellations.pending_len();

        while submitted_count < limit {
            if let Some(request) = self.control.cancellations.front_pending().copied() {
                let view = self.ops.checked_slot_view(request.target)?;
                match view {
                    CheckedSlotView::Valid(_) => {}
                    CheckedSlotView::Missing { .. }
                    | CheckedSlotView::Empty(_)
                    | CheckedSlotView::Stale(_) => {
                        if let Some(request) = self.control.cancellations.pop_pending() {
                            self.control
                                .record(ControlPlaneEvent::CancelPendingPop(request.target));
                        }
                        let (reason, kind) = cancel_target_kind(request.target, view);
                        self.record_cancel_target_gone(reason);
                        let attach = AnomalyAttach::from_op_token(request.target);
                        let _ = self.accept_completion_anomaly_kind(kind, attach)?;
                        continue;
                    }
                }

                if self.try_submit_cancel_request(request)?.is_some() {
                    if let Some(request) = self.control.cancellations.pop_pending() {
                        self.control
                            .record(ControlPlaneEvent::CancelPendingPop(request.target));
                    }
                    submitted_count += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn flush_backlog(&mut self) -> UringResult<BacklogProgress> {
        enum BacklogAction {
            SubmitReserved,
            SubmitQueued,
            CancelQueued,
            CancelKernel,
            Drop,
        }

        let mut progress = BacklogProgress::default();
        while let Some(entry) = self.control.backlog.front() {
            let token = entry.token;
            let action = match self.ops.checked_slot_view(token)? {
                CheckedSlotView::Valid(slot) => match slot {
                    SlotView::InFlightOrphaned(slot) => {
                        if slot.platform().control.submission == SubmissionPhase::Reserved {
                            BacklogAction::CancelQueued
                        } else {
                            BacklogAction::CancelKernel
                        }
                    }
                    SlotView::Reserved(slot) => {
                        if slot_has_op(slot) {
                            BacklogAction::SubmitReserved
                        } else {
                            BacklogAction::Drop
                        }
                    }
                    SlotView::InFlightWaiting(slot) => {
                        if slot.platform().control.submission == SubmissionPhase::Reserved {
                            BacklogAction::SubmitQueued
                        } else {
                            BacklogAction::Drop
                        }
                    }
                },
                _ => BacklogAction::Drop,
            };

            match action {
                BacklogAction::CancelQueued => {
                    self.pop_backlog();
                    self.set_cancel_phase(token, CancellationPhase::Acked);
                    self.complete_local_cancel(token, CancelMode::Abandon)?;
                    progress
                        .actions
                        .push(BacklogActionResult::SyntheticCompletion);
                }
                BacklogAction::CancelKernel => {
                    self.pop_backlog();
                    let outcome = self.cancel_op_internal(CancelRequest::abandon(token))?;
                    progress.actions.push(match outcome {
                        CancelSubmitOutcome::Submitted => BacklogActionResult::Submitted,
                        CancelSubmitOutcome::Queued => BacklogActionResult::StillFull,
                        CancelSubmitOutcome::CompletedLocally => {
                            BacklogActionResult::SyntheticCompletion
                        }
                        CancelSubmitOutcome::TargetGone { .. }
                        | CancelSubmitOutcome::NoBackendHandle => {
                            BacklogActionResult::FatalControlError
                        }
                    });
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                    progress.actions.push(BacklogActionResult::VoidRecovery);
                }
                BacklogAction::SubmitReserved => match self.submit_from_slot_token(token) {
                    Ok(true) => {
                        self.pop_backlog();
                        progress.actions.push(BacklogActionResult::Submitted);
                    }
                    Ok(false) => {
                        progress.actions.push(BacklogActionResult::StillFull);
                        break;
                    }
                    Err(report) => {
                        self.pop_backlog();
                        self.complete_reserved_submission_error(token, report)?;
                        progress
                            .actions
                            .push(BacklogActionResult::SyntheticCompletion);
                    }
                },
                BacklogAction::SubmitQueued => {
                    let result = {
                        let (ops, mut env) = self.split_for_submit();
                        match ops.checked_slot_view(token)? {
                            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                                submit_queued_from_slot(&mut env, token, slot)
                            }
                            _ => Ok(true),
                        }
                    };
                    match result {
                        Ok(true) => {
                            self.pop_backlog();
                            progress.actions.push(BacklogActionResult::Submitted);
                        }
                        Ok(false) => {
                            progress.actions.push(BacklogActionResult::StillFull);
                            break;
                        }
                        Err(report) => {
                            self.pop_backlog();
                            self.complete_queued_submission_error(token, report)?;
                            progress
                                .actions
                                .push(BacklogActionResult::SyntheticCompletion);
                        }
                    }
                }
            }
        }
        Ok(progress)
    }

    pub(crate) fn push_backlog(
        &mut self,
        token: OpToken,
        kind: BacklogStageKind,
    ) -> UringResult<()> {
        if self.control.backlog.push(token, kind).is_err() {
            return Err(UringError::InvalidState
                .report(
                    "uring.backlog.push",
                    "backlog already contains the operation token",
                )
                .with_ctx("token", token.index()));
        }
        self.control.record(ControlPlaneEvent::BacklogPush(token));
        Ok(())
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<OpToken> {
        let token = self.control.backlog.pop_front().map(|entry| entry.token);
        if let Some(token) = token {
            self.control.record(ControlPlaneEvent::BacklogPop(token));
        }
        token
    }

    pub(crate) fn remove_backlog_token(&mut self, token: OpToken) -> bool {
        if !self.control.backlog.remove(token) {
            return false;
        }
        self.control.record(ControlPlaneEvent::BacklogRemove(token));
        true
    }

    fn complete_reserved_submission_error(
        &mut self,
        token: OpToken,
        report: Report<UringError>,
    ) -> UringResult<()> {
        let prepared = match self.ops.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) if slot.has_op() => {
                let guard = slot.start_submission_with(None).map_err(|err| {
                    slot_access_report("uring.complete_reserved_submission_error", err)
                })?;
                let _ = guard.persist();
                true
            }
            _ => false,
        };
        if prepared {
            self.complete_queued_submission_error(token, report)
        } else {
            Err(report)
        }
    }

    fn complete_queued_submission_error(
        &mut self,
        token: OpToken,
        report: Report<UringError>,
    ) -> UringResult<()> {
        let event_res = uring_report_to_event_res(&report);
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, event_res, 0);
        self.accept_synthetic_completion(
            event,
            SyntheticCompletionSource::SubmissionFailure,
            UringSyntheticCompletion::SubmissionFailure {
                report: Some(report),
            },
        )?;
        Ok(())
    }

    fn record_cancel_target_gone(&self, reason: CancelTargetGoneReason) {
        match reason {
            CancelTargetGoneReason::Missing => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_missing(),
            CancelTargetGoneReason::Stale => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_stale(),
            CancelTargetGoneReason::Corrupt => self
                .completion_diagnostics
                .backend()
                .inc_cancel_target_corrupt(),
        }
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.snapshot().has_op
}
