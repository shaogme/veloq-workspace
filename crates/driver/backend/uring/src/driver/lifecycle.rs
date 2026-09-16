use crate::{
    driver::{
        PendingCancel, UringDriver,
        completion::{COMP_BACKEND_URING, UringSyntheticCompletion},
        control::{CancelIntentError, CancelRequestDisposition, ControlPlaneEvent},
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
use veloq_std::format;
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogProgress {
    pub(crate) actions: usize,
    pub(crate) submitted: usize,
    pub(crate) synthetic: usize,
    pub(crate) still_full: bool,
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
        cancel_ticket: CancelTicket,
    ) -> UringResult<Option<CancelTicket>> {
        let (user_data, generation) = request.user_parts();
        let cancel_sqe = opcode::AsyncCancel::new(CompletionToken::user(request.target).raw())
            .build()
            .user_data(CompletionToken::cancel(cancel_ticket).raw());

        if self
            .submit_env()
            .stage_cancel_entry(cancel_ticket, request, cancel_sqe)?
            == crate::driver::env::StageResult::Staged
        {
            self.control
                .cancellations
                .mark_staged(cancel_ticket, request.target)
                .map_err(|error| {
                    UringError::InvalidState
                        .report("uring.cancel.stage_intent", format!("{error:?}"))
                        .attach_note("cancel SQE was staged without a matching intent")
                })?;
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
        let disposition =
            self.control
                .cancellations
                .request(request)
                .map_err(|error| match error {
                    CancelIntentError::TicketExhausted => {
                        self.completion_diagnostics
                            .backend()
                            .inc_cancel_ticket_exhausted();
                        UringError::CancelTicketExhausted
                            .report(
                                "uring.cancel.allocate_ticket",
                                "cancel ticket space exhausted",
                            )
                            .attach_note(
                                "target remains active and no cancel map entry was overwritten",
                            )
                    }
                    CancelIntentError::Capacity => UringError::InvalidState
                        .report("uring.cancel.intent", "cancel intent ledger is full")
                        .attach_note("operation capacity and cancel ledger capacity diverged"),
                    CancelIntentError::GenerationMismatch => UringError::InvalidState
                        .report("uring.cancel.intent", "cancel intent generation mismatch")
                        .attach_note("a stale cancel request targeted a reused slot"),
                    CancelIntentError::InvalidPhase => UringError::InvalidState
                        .report(
                            "uring.cancel.intent",
                            "cancel intent phase transition is invalid",
                        )
                        .attach_note("cancel intent bookkeeping observed an impossible phase"),
                })?;

        match disposition {
            CancelRequestDisposition::AlreadyPending { ticket } => {
                Ok(CancelSubmitOutcome::AlreadyPending { ticket })
            }
            CancelRequestDisposition::Merged { ticket } => {
                Ok(CancelSubmitOutcome::Merged { ticket })
            }
            CancelRequestDisposition::New { ticket } => {
                match self.try_submit_cancel_request(request, ticket) {
                    Ok(Some(_)) => Ok(CancelSubmitOutcome::Submitted),
                    Ok(None) => {
                        self.completion_diagnostics.backend().inc_cancel_queued();
                        Ok(CancelSubmitOutcome::Queued)
                    }
                    Err(report) => {
                        self.control
                            .cancellations
                            .cancel_request_failed(ticket, request.target);
                        Err(report)
                    }
                }
            }
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

    pub(crate) fn drain_cancel_requests_bounded(&mut self, limit: usize) -> UringResult<usize> {
        let mut drained = 0;
        while drained < limit {
            let Some(request) = self.control.cancellations.try_recv_remote() else {
                break;
            };
            self.cancel_op_internal(request)?;
            drained += 1;
        }
        Ok(drained)
    }

    pub(crate) fn stage_pending_cancellations(&mut self, limit: usize) -> UringResult<usize> {
        let mut submitted_count = 0;

        while submitted_count < limit {
            if let Some(request) = self.control.cancellations.front_pending().copied() {
                let view = self.ops.checked_slot_view(request.target)?;
                match view {
                    CheckedSlotView::Valid(_) => {}
                    CheckedSlotView::Missing { .. }
                    | CheckedSlotView::Empty(_)
                    | CheckedSlotView::Stale(_) => {
                        let _ = self
                            .control
                            .cancellations
                            .remove_pending_target(request.target);
                        let (reason, kind) = cancel_target_kind(request.target, view);
                        self.record_cancel_target_gone(reason);
                        let attach = AnomalyAttach::from_op_token(request.target);
                        let _ = self.accept_completion_anomaly_kind(kind, attach)?;
                        continue;
                    }
                }

                let Some(ticket) = self.control.cancellations.ticket_for(request.target) else {
                    return Err(UringError::InvalidState
                        .report("uring.cancel.stage", "pending cancel has no ticket")
                        .with_ctx("target", request.target.index()));
                };
                if self.try_submit_cancel_request(request, ticket)?.is_some() {
                    submitted_count += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(submitted_count)
    }

    pub(crate) fn stage_backlog_entries(&mut self, limit: usize) -> UringResult<BacklogProgress> {
        enum BacklogAction {
            SubmitReserved,
            SubmitQueued,
            CancelQueued,
            CancelKernel,
            Drop,
        }

        let mut progress = BacklogProgress::default();
        while progress.actions < limit {
            let Some(entry) = self.control.backlog.front() else {
                break;
            };
            progress.actions += 1;
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
                    progress.synthetic += 1;
                }
                BacklogAction::CancelKernel => {
                    self.pop_backlog();
                    let outcome = self.cancel_op_internal(CancelRequest::abandon(token))?;
                    match outcome {
                        CancelSubmitOutcome::Submitted => progress.submitted += 1,
                        CancelSubmitOutcome::Queued => {
                            progress.still_full = true;
                        }
                        CancelSubmitOutcome::AlreadyPending { .. }
                        | CancelSubmitOutcome::Merged { .. } => progress.still_full = true,
                        CancelSubmitOutcome::CompletedLocally => {
                            progress.synthetic += 1;
                        }
                        CancelSubmitOutcome::TargetGone { .. }
                        | CancelSubmitOutcome::NoBackendHandle => {}
                    }
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                }
                BacklogAction::SubmitReserved => match self.submit_from_slot_token(token) {
                    Ok(true) => {
                        self.pop_backlog();
                        progress.submitted += 1;
                    }
                    Ok(false) => {
                        progress.still_full = true;
                        break;
                    }
                    Err(report) => {
                        self.pop_backlog();
                        self.complete_reserved_submission_error(token, report)?;
                        progress.synthetic += 1;
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
                            progress.submitted += 1;
                        }
                        Ok(false) => {
                            progress.still_full = true;
                            break;
                        }
                        Err(report) => {
                            self.pop_backlog();
                            self.complete_queued_submission_error(token, report)?;
                            progress.synthetic += 1;
                        }
                    }
                }
            }
        }
        Ok(progress)
    }

    pub(crate) fn push_backlog(&mut self, token: OpToken) -> UringResult<()> {
        if let Err(error) = self.control.backlog.push(token) {
            return Err(UringError::InvalidState
                .report(
                    "uring.backlog.push",
                    format!("backlog rejected token: {error:?}"),
                )
                .with_ctx("token", token.index())
                .with_ctx("generation", token.generation().get()));
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
        if self.control.backlog.remove(token).is_err() {
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
