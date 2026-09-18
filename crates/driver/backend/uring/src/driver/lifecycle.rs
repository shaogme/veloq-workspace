use crate::{
    driver::{
        context::LifecycleContext,
        control::{CancelIntentError, CancelRequestDisposition, PendingCancel},
        control::{ControlPlaneEvent, ControlPlaneObserver, ControlTransition},
        env::StageResult,
        submission::txn::slot_access_report,
    },
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{CheckedSlotView, Slot, SlotState, SlotView, UringOpRegistryExt},
};
use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_driver_core::driver::{
    AnomalyAttach, CancelMode, CancelRequest, CancelSubmitOutcome, CancelTargetGoneReason,
    CancelTicket, CompletionAnomalyKind, CompletionBackend, CompletionToken, OpToken,
    UserCompletionEvent, cancel_target_kind,
};
use veloq_io_uring::opcode;
use veloq_std::{format, mem, vec::Vec};
use veloq_wheel::TimerId;

pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match veloq_std::num::NonZeroU8::new(2) {
        Some(value) => value,
        None => unreachable!(),
    });

pub(crate) enum LifecycleCompletionAction {
    SyntheticCancel {
        event: UserCompletionEvent,
        mode: CancelMode,
    },
    SyntheticSubmissionFailure {
        event: UserCompletionEvent,
        report: Report<UringError>,
    },
    Anomaly {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    },
}

pub(crate) struct LifecycleEngine {
    completion_actions: Vec<LifecycleCompletionAction>,
    backlog_actions: Vec<BacklogAction>,
}

impl LifecycleEngine {
    pub(crate) const fn new() -> Self {
        Self {
            completion_actions: Vec::new(),
            backlog_actions: Vec::new(),
        }
    }

    pub(crate) fn drain_completion_actions_into(
        &mut self,
        destination: &mut Vec<LifecycleCompletionAction>,
    ) {
        destination.clear();
        destination.append(&mut self.completion_actions);
    }

    pub(crate) fn restore_completion_actions(
        &mut self,
        source: &mut Vec<LifecycleCompletionAction>,
    ) {
        if self.completion_actions.is_empty() {
            mem::swap(&mut self.completion_actions, source);
        } else {
            self.completion_actions.append(source);
        }
    }

    pub(crate) fn pending_action_count(&self) -> usize {
        self.completion_actions.len() + self.backlog_actions.len()
    }
}

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
    submission: SubmissionPhase,
    cancellation: CancellationPhase,
}

/// An operation's backend lifecycle state.
///
/// The phase and timer data intentionally remain private. They are protocol state owned by the
/// operation ledger, not a construction or mutation API for callers of the driver.
#[derive(Clone, Default)]
pub struct UringOpState {
    timer_id: Option<TimerId>,
    control: UringOpControl,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogProgress {
    actions: usize,
    submitted: usize,
    synthetic: usize,
    still_full: bool,
}

pub(crate) enum BacklogAction {
    SubmitReserved(OpToken),
    SubmitQueued(OpToken),
    CancelQueued(OpToken),
    CancelKernel(OpToken),
    Drop,
}

impl BacklogProgress {
    pub(crate) const fn actions(self) -> usize {
        self.actions
    }

    pub(crate) fn submitted(&mut self) {
        self.submitted += 1;
    }

    pub(crate) fn synthetic(&mut self) {
        self.synthetic += 1;
    }

    pub(crate) fn mark_still_full(&mut self) {
        self.still_full = true;
    }
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) const fn submission_phase(&self) -> SubmissionPhase {
        self.control.submission
    }

    pub(crate) fn transition_submission_phase(
        &mut self,
        token: OpToken,
        next: SubmissionPhase,
        reason: &'static str,
        observer: &mut ControlPlaneObserver,
    ) {
        let from = self.control.submission;
        self.control.submission = next;
        observer.record(ControlPlaneEvent::SubmissionTransition(
            ControlTransition::new(token, from, next, reason),
        ));
    }

    pub(crate) fn set_submission_phase(&mut self, phase: SubmissionPhase) {
        self.control.submission = phase;
    }

    pub(crate) fn set_cancellation_phase(&mut self, phase: CancellationPhase) {
        self.control.cancellation = phase;
    }

    pub(crate) const fn timer_id(&self) -> Option<TimerId> {
        self.timer_id
    }

    pub(crate) fn take_timer(&mut self) -> Option<TimerId> {
        self.timer_id.take()
    }

    pub(crate) fn arm_timer(&mut self, task_id: TimerId) {
        self.timer_id = Some(task_id);
    }

    pub(crate) fn clear_timer(&mut self) {
        self.timer_id = None;
    }
}

impl LifecycleEngine {
    fn try_submit_cancel_request(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        request: PendingCancel,
        cancel_ticket: CancelTicket,
    ) -> UringResult<Option<CancelTicket>> {
        let (user_data, generation) = request.user_parts();
        let cancel_sqe = opcode::AsyncCancel::new(CompletionToken::user(request.target()).raw())
            .build()
            .map_err(|error| {
                UringError::InvalidInput.io_report("uring.cancel.build_opcode", error)
            })?
            .user_data(CompletionToken::cancel(cancel_ticket).raw());

        let staged = context.with_submit_port(|submit| {
            let (_, mut env) = submit.split_for_submit();
            env.stage_cancel_entry(cancel_ticket, request, cancel_sqe)
        })?;
        if staged == StageResult::Staged {
            context
                .control()
                .mark_cancel_staged(cancel_ticket, request.target())
                .map_err(|error| {
                    UringError::InvalidState
                        .report("uring.cancel.stage_intent", format!("{error:?}"))
                        .attach_note("cancel SQE was staged without a matching intent")
                })?;
            self.set_cancel_phase(context, request.target(), CancellationPhase::CancelStaged);
            context.diagnostics().backend().inc_cancel_submitted();
            trace!(
                user_data,
                generation = generation.get(),
                cancel_ticket = cancel_ticket.raw(),
                mode = ?request.mode(),
                "submitted async cancel"
            );
            Ok(Some(cancel_ticket))
        } else {
            Ok(None)
        }
    }

    fn submit_cancel_request(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        request: PendingCancel,
    ) -> UringResult<CancelSubmitOutcome> {
        let disposition =
            context
                .control()
                .request_cancel(request)
                .map_err(|error| match error {
                    CancelIntentError::TicketExhausted => {
                        context
                            .diagnostics()
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
                match self.try_submit_cancel_request(context, request, ticket) {
                    Ok(Some(_)) => Ok(CancelSubmitOutcome::Submitted),
                    Ok(None) => {
                        context.diagnostics().backend().inc_cancel_queued();
                        Ok(CancelSubmitOutcome::Queued)
                    }
                    Err(report) => {
                        context
                            .control()
                            .fail_cancel_request(ticket, request.target());
                        Err(report)
                    }
                }
            }
        }
    }

    fn complete_local_cancel(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
        mode: CancelMode,
    ) -> UringResult<()> {
        context.diagnostics().backend().inc_cancel_local_completed();
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, -libc::ECANCELED, 0);
        self.completion_actions
            .push(LifecycleCompletionAction::SyntheticCancel { event, mode });
        Ok(())
    }

    pub(crate) fn set_cancel_phase(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
        phase: CancellationPhase,
    ) {
        let _ = context.ops().set_cancellation(token, phase);
    }

    pub(crate) fn cancel_op_internal(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        request: CancelRequest,
    ) -> UringResult<CancelSubmitOutcome> {
        let request = PendingCancel::new(request);
        let (user_data, generation) = request.user_parts();
        let token = request.target();

        let in_backlog = context.control().backlog_contains(token);
        enum CancelPath {
            Backlog,
            Timer(Option<TimerId>),
            Kernel,
        }

        match context.ops().checked_slot_view(token)? {
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
                    self.set_cancel_phase(context, token, CancellationPhase::Acked);
                    self.complete_local_cancel(context, token, request.mode())?;
                } else {
                    let _ = context.ops().remove(token);
                }
                Ok(CancelSubmitOutcome::CompletedLocally)
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let phase = slot.platform().submission_phase();
                let path = if phase == SubmissionPhase::Reserved && in_backlog {
                    CancelPath::Backlog
                } else if phase == SubmissionPhase::TimerArmed {
                    CancelPath::Timer(context.ops().take_timer(token)?)
                } else {
                    if request.mode() == CancelMode::Abandon {
                        let _ = slot.cancel();
                    }
                    CancelPath::Kernel
                };
                match path {
                    CancelPath::Backlog => {
                        Self::remove_backlog_token(context, token);
                        self.set_cancel_phase(context, token, CancellationPhase::Acked);
                        self.complete_local_cancel(context, token, request.mode())
                            .map(|()| CancelSubmitOutcome::CompletedLocally)
                    }
                    CancelPath::Timer(Some(tid)) => {
                        context.control().cancel_timer(tid, token);
                        self.set_cancel_phase(context, token, CancellationPhase::Acked);
                        self.complete_local_cancel(context, token, request.mode())
                            .map(|()| CancelSubmitOutcome::CompletedLocally)
                    }
                    CancelPath::Timer(None) => Err(UringError::InvalidState.report(
                        "uring.cancel.timer",
                        "timer-armed operation did not contain a timer id",
                    )),
                    CancelPath::Kernel => {
                        self.set_cancel_phase(context, token, CancellationPhase::Requested);
                        self.submit_cancel_request(context, request)
                    }
                }
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => {
                let phase = slot.platform().submission_phase();
                let path = if phase == SubmissionPhase::Reserved && in_backlog {
                    CancelPath::Backlog
                } else if phase == SubmissionPhase::TimerArmed {
                    CancelPath::Timer(context.ops().take_timer(token)?)
                } else {
                    CancelPath::Kernel
                };
                match path {
                    CancelPath::Backlog => {
                        Self::remove_backlog_token(context, token);
                        self.set_cancel_phase(context, token, CancellationPhase::Acked);
                        self.complete_local_cancel(context, token, CancelMode::Abandon)
                            .map(|()| CancelSubmitOutcome::CompletedLocally)
                    }
                    CancelPath::Timer(Some(tid)) => {
                        context.control().cancel_timer(tid, token);
                        self.set_cancel_phase(context, token, CancellationPhase::Acked);
                        self.complete_local_cancel(context, token, CancelMode::Abandon)
                            .map(|()| CancelSubmitOutcome::CompletedLocally)
                    }
                    CancelPath::Timer(None) => Err(UringError::InvalidState.report(
                        "uring.cancel.timer",
                        "timer-armed orphan did not contain a timer id",
                    )),
                    CancelPath::Kernel => {
                        self.set_cancel_phase(context, token, CancellationPhase::Requested);
                        self.submit_cancel_request(context, request)
                    }
                }
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)) => {
                let (reason, kind) = cancel_target_kind(token, view);
                self.record_cancel_target_gone(context, reason);
                let attach = AnomalyAttach::from_op_token(token);
                self.completion_actions
                    .push(LifecycleCompletionAction::Anomaly { kind, attach });
                debug!(
                    user_data,
                    generation = generation.get(),
                    token = CompletionToken::user(request.target()).raw(),
                    reason = ?reason,
                    "cancel request did not match an active uring slot"
                );
                Ok(CancelSubmitOutcome::TargetGone { reason })
            }
        }
    }

    pub(crate) fn drain_cancel_requests_bounded(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        limit: usize,
    ) -> UringResult<usize> {
        let mut drained = 0;
        while drained < limit {
            let Some(request) = context.control().try_recv_cancel() else {
                break;
            };
            self.cancel_op_internal(context, request)?;
            drained += 1;
        }
        Ok(drained)
    }

    pub(crate) fn stage_pending_cancellations(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        limit: usize,
    ) -> UringResult<usize> {
        let mut submitted_count = 0;

        while submitted_count < limit {
            if let Some(request) = context.control().front_pending_cancel() {
                let target_gone = {
                    let view = context.ops().checked_slot_view(request.target())?;
                    match view {
                        CheckedSlotView::Valid(_) => None,
                        CheckedSlotView::Missing { .. }
                        | CheckedSlotView::Empty(_)
                        | CheckedSlotView::Stale(_) => {
                            Some(cancel_target_kind(request.target(), view))
                        }
                    }
                };
                if let Some((reason, kind)) = target_gone {
                    let _ = context.control().remove_pending_cancel(request.target());
                    self.record_cancel_target_gone(context, reason);
                    let attach = AnomalyAttach::from_op_token(request.target());
                    self.completion_actions
                        .push(LifecycleCompletionAction::Anomaly { kind, attach });
                    continue;
                }

                let Some(ticket) = context.control().cancel_ticket_for(request.target()) else {
                    return Err(UringError::InvalidState
                        .report("uring.cancel.stage", "pending cancel has no ticket")
                        .with_ctx("target", request.target().index()));
                };
                if self
                    .try_submit_cancel_request(context, request, ticket)?
                    .is_some()
                {
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

    pub(crate) fn plan_backlog_entries(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        limit: usize,
    ) -> UringResult<BacklogProgress> {
        let mut progress = BacklogProgress::default();
        while progress.actions < limit {
            let Some(entry) = context.control().pop_backlog() else {
                break;
            };
            progress.actions += 1;
            let token = entry.token();
            let action = match context.ops().checked_slot_view(token)? {
                CheckedSlotView::Valid(slot) => match slot {
                    SlotView::InFlightOrphaned(slot) => {
                        if slot.platform().submission_phase() == SubmissionPhase::Reserved {
                            BacklogAction::CancelQueued(token)
                        } else {
                            BacklogAction::CancelKernel(token)
                        }
                    }
                    SlotView::Reserved(slot) => {
                        if slot_has_op(slot) {
                            BacklogAction::SubmitReserved(token)
                        } else {
                            BacklogAction::Drop
                        }
                    }
                    SlotView::InFlightWaiting(slot) => {
                        if slot.platform().submission_phase() == SubmissionPhase::Reserved {
                            BacklogAction::SubmitQueued(token)
                        } else {
                            BacklogAction::Drop
                        }
                    }
                },
                _ => BacklogAction::Drop,
            };
            self.backlog_actions.push(action);
        }
        Ok(progress)
    }

    pub(crate) fn drain_backlog_actions_into(&mut self, destination: &mut Vec<BacklogAction>) {
        destination.clear();
        destination.append(&mut self.backlog_actions);
    }

    pub(crate) fn restore_backlog_actions(&mut self, source: &mut Vec<BacklogAction>) {
        if self.backlog_actions.is_empty() {
            mem::swap(&mut self.backlog_actions, source);
        } else {
            self.backlog_actions.append(source);
        }
    }

    pub(crate) fn complete_backlog_cancel(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
    ) -> UringResult<()> {
        self.set_cancel_phase(context, token, CancellationPhase::Acked);
        self.complete_local_cancel(context, token, CancelMode::Abandon)
    }

    pub(crate) fn complete_backlog_submission_error(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
        reserved: bool,
        report: Report<UringError>,
    ) -> UringResult<()> {
        if reserved {
            self.complete_reserved_submission_error(context, token, report)
        } else {
            self.complete_queued_submission_error(context, token, report)
        }
    }

    fn remove_backlog_token(
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
    ) -> bool {
        context.control().remove_backlog(token).is_ok()
    }

    fn complete_reserved_submission_error(
        &mut self,
        context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
        report: Report<UringError>,
    ) -> UringResult<()> {
        let prepared = match context.ops().checked_slot_view(token)? {
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
            self.complete_queued_submission_error(context, token, report)
        } else {
            Err(report)
        }
    }

    fn complete_queued_submission_error(
        &mut self,
        _context: &mut LifecycleContext<'_, '_, '_, '_>,
        token: OpToken,
        report: Report<UringError>,
    ) -> UringResult<()> {
        let event_res = uring_report_to_event_res(&report);
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, event_res, 0);
        self.completion_actions
            .push(LifecycleCompletionAction::SyntheticSubmissionFailure { event, report });
        Ok(())
    }

    fn record_cancel_target_gone(
        &self,
        context: &LifecycleContext<'_, '_, '_, '_>,
        reason: CancelTargetGoneReason,
    ) {
        match reason {
            CancelTargetGoneReason::Missing => {
                context.diagnostics().backend().inc_cancel_target_missing()
            }
            CancelTargetGoneReason::Stale => {
                context.diagnostics().backend().inc_cancel_target_stale()
            }
            CancelTargetGoneReason::Corrupt => {
                context.diagnostics().backend().inc_cancel_target_corrupt()
            }
        }
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.snapshot().has_op
}
