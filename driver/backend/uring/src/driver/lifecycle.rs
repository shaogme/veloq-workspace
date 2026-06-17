use crate::{
    driver::{
        PendingCancel, UringDriver,
        completion::{COMP_BACKEND_URING, UringSyntheticCompletion},
    },
    error::{UringError, uring_report_to_event_res},
    op::{CheckedSlotView, Slot, SlotState, SlotView, UringOpRegistryExt},
};
use diagweave::prelude::*;
use io_uring::opcode;
use tracing::{debug, trace};
use veloq_driver_core::driver::{
    AnomalyAttach, CancelCompletionId, CancelMode, CancelRequest, CancelSubmitOutcome,
    CancelTargetGoneReason, CompletionToken, OpToken, SyntheticCompletionSource,
    UserCompletionEvent, cancel_target_kind,
};
use veloq_wheel::TaskId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum UringSubmissionState {
    #[default]
    Idle,
    Queued,
    KernelSubmitted,
    Timer,
}

#[derive(Clone, Default)]
pub struct UringOpState {
    pub(crate) timer_id: Option<TaskId>,
    pub(crate) submission_state: UringSubmissionState,
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<'a> UringDriver<'a> {
    fn next_cancel_completion_id(&mut self) -> Option<CancelCompletionId> {
        for _ in 0..u16::MAX {
            let id = self.next_cancel_id;
            self.next_cancel_id = self.next_cancel_id.wrapping_add(1);
            if self.next_cancel_id == 0 {
                self.next_cancel_id = 1;
            }
            let id = CancelCompletionId::new(id);
            if !self.pending_cancel_cqes.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    fn try_submit_cancel_request(&mut self, request: PendingCancel) -> Option<CancelCompletionId> {
        let (user_data, generation) = request.user_parts();

        let cancel_id = self.next_cancel_completion_id()?;
        let cancel_sqe = opcode::AsyncCancel::new(CompletionToken::user(request.target).raw())
            .build()
            .user_data(CompletionToken::cancel(cancel_id).raw());

        if self.push_entry(cancel_sqe) {
            self.pending_cancel_cqes.insert(cancel_id, request);
            self.completion_diagnostics.backend().inc_cancel_submitted();
            trace!(
                user_data,
                generation,
                cancel_id = cancel_id.raw(),
                mode = ?request.mode,
                "submitted async cancel"
            );
            Some(cancel_id)
        } else {
            None
        }
    }

    fn submit_cancel_request(&mut self, request: PendingCancel) -> CancelSubmitOutcome {
        if self.try_submit_cancel_request(request).is_some() {
            CancelSubmitOutcome::Submitted
        } else {
            self.pending_cancellations.push_back(request);
            self.completion_diagnostics.backend().inc_cancel_queued();
            CancelSubmitOutcome::Queued
        }
    }

    fn complete_local_cancel(&mut self, token: OpToken, mode: CancelMode) {
        self.completion_diagnostics
            .backend()
            .inc_cancel_local_completed();
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, -libc::ECANCELED, 0);
        let _ = self.accept_synthetic_completion(
            event,
            SyntheticCompletionSource::Cancel,
            UringSyntheticCompletion::Cancel { mode },
        );
    }

    pub(crate) fn cancel_op_internal(&mut self, request: CancelRequest) -> CancelSubmitOutcome {
        let request = PendingCancel::new(request);
        let (user_data, generation) = request.user_parts();
        let token = request.target;

        match self.ops.checked_slot_view(token) {
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
                                generation,
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
                    self.complete_local_cancel(token, request.mode);
                } else {
                    let _ = self.ops.remove(token);
                }
                CancelSubmitOutcome::CompletedLocally
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                if slot.platform().submission_state == UringSubmissionState::Queued {
                    self.remove_backlog_token(token);
                    self.complete_local_cancel(token, request.mode);
                    return CancelSubmitOutcome::CompletedLocally;
                }

                if let Some(tid) = slot.platform_mut().timer_id.take() {
                    self.wheel.cancel(tid);
                    self.complete_local_cancel(token, request.mode);
                    return CancelSubmitOutcome::CompletedLocally;
                }

                if request.mode == CancelMode::Abandon {
                    let _ = slot.cancel();
                }
                self.submit_cancel_request(request)
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                if slot.platform().submission_state == UringSubmissionState::Queued {
                    self.remove_backlog_token(token);
                    self.complete_local_cancel(token, CancelMode::Abandon);
                    return CancelSubmitOutcome::CompletedLocally;
                }

                if let Some(tid) = slot.platform_mut().timer_id.take() {
                    self.wheel.cancel(tid);
                    self.complete_local_cancel(token, CancelMode::Abandon);
                    return CancelSubmitOutcome::CompletedLocally;
                }

                self.submit_cancel_request(request)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)
            | CheckedSlotView::Corrupt(_)) => {
                let (reason, kind) = cancel_target_kind(token, view);
                self.record_cancel_target_gone(reason);
                let attach = AnomalyAttach::from_op_token(token);
                let _ = self.accept_completion_anomaly_kind(kind, attach);
                debug!(
                    user_data,
                    generation,
                    token = CompletionToken::user(request.target).raw(),
                    reason = ?reason,
                    "cancel request did not match an active uring slot"
                );
                CancelSubmitOutcome::TargetGone { reason }
            }
        }
    }

    pub(crate) fn flush_cancellations(&mut self) {
        let mut submitted_count = 0;
        let limit = self.pending_cancellations.len();

        while submitted_count < limit {
            if let Some(request) = self.pending_cancellations.front().copied() {
                match self.ops.checked_slot_view(request.target) {
                    CheckedSlotView::Valid(_) => {}
                    CheckedSlotView::Missing { .. }
                    | CheckedSlotView::Empty(_)
                    | CheckedSlotView::Stale(_)
                    | CheckedSlotView::Corrupt(_) => {
                        self.pending_cancellations.pop_front();
                        let (reason, kind) = cancel_target_kind(
                            request.target,
                            self.ops.checked_slot_view(request.target),
                        );
                        self.record_cancel_target_gone(reason);
                        let attach = AnomalyAttach::from_op_token(request.target);
                        let _ = self.accept_completion_anomaly_kind(kind, attach);
                        continue;
                    }
                }

                if self.try_submit_cancel_request(request).is_some() {
                    self.pending_cancellations.pop_front();
                    submitted_count += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn flush_backlog(&mut self) {
        enum BacklogAction {
            SubmitReserved,
            SubmitQueued,
            CancelQueued,
            CancelKernel,
            Drop,
        }

        while let Some(&token) = self.backlog.front() {
            let action = match self.ops.checked_slot_view(token) {
                CheckedSlotView::Valid(slot) => match slot {
                    SlotView::InFlightOrphaned(slot) => {
                        if slot.platform().submission_state == UringSubmissionState::Queued {
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
                        if slot.platform().submission_state == UringSubmissionState::Queued {
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
                    self.complete_local_cancel(token, CancelMode::Abandon);
                }
                BacklogAction::CancelKernel => {
                    self.pop_backlog();
                    let _ = self.cancel_op_internal(CancelRequest::abandon(token));
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                }
                BacklogAction::SubmitReserved => match self.submit_from_slot_token(token) {
                    Ok(true) => {
                        self.pop_backlog();
                    }
                    Ok(false) => break,
                    Err(_) => {
                        self.pop_backlog();
                    }
                },
                BacklogAction::SubmitQueued => {
                    let driver_ptr = self as *mut UringDriver;
                    let result = match self.ops.checked_slot_view(token) {
                        CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => unsafe {
                            Self::submit_queued_from_slot_raw(driver_ptr, token, slot)
                        },
                        _ => Ok(true),
                    };
                    match result {
                        Ok(true) => {
                            self.pop_backlog();
                        }
                        Ok(false) => break,
                        Err(report) => {
                            self.pop_backlog();
                            self.complete_queued_submission_error(token, report);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn push_backlog(&mut self, token: OpToken) {
        self.backlog.push_back(token);
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<OpToken> {
        self.backlog.pop_front()
    }

    pub(crate) fn remove_backlog_token(&mut self, token: OpToken) -> bool {
        let Some(pos) = self.backlog.iter().position(|queued| *queued == token) else {
            return false;
        };
        self.backlog.remove(pos);
        true
    }

    fn complete_queued_submission_error(&mut self, token: OpToken, report: Report<UringError>) {
        let event_res = uring_report_to_event_res(&report);
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, event_res, 0);
        let _ = self.accept_synthetic_completion(
            event,
            SyntheticCompletionSource::SubmissionFailure,
            UringSyntheticCompletion::SubmissionFailure {
                report: Some(report),
            },
        );
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
