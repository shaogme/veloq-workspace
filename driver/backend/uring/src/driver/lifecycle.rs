use crate::driver::{PendingCancel, UringDriver};
use crate::error::{UringDriverResult, UringError, uring_report_to_event_res};
use diagweave::prelude::*;
use io_uring::opcode;
use tracing::{debug, error, trace};
use veloq_driver_core::driver::{
    CancelMode, CancelRequest, CancelSubmitOutcome, CompletionAnomaly, CompletionCleanupGuard,
    CompletionSidecar, CompletionToken, OpToken, record_completion_anomaly, run_completion_cleanup,
};

use crate::op::{CheckedSlotView, Slot, SlotState, SlotView, UringOpRegistryExt, UringUserPayload};

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
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
    pub(crate) submission_state: UringSubmissionState,
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<'a> UringDriver<'a> {
    fn next_cancel_completion_id(&mut self) -> u16 {
        loop {
            let id = self.next_cancel_id;
            self.next_cancel_id = self.next_cancel_id.wrapping_add(1);
            if self.next_cancel_id == 0 {
                self.next_cancel_id = 1;
            }
            if !self.pending_cancel_cqes.contains_key(&id) {
                return id;
            }
        }
    }

    fn try_submit_cancel_request(&mut self, request: PendingCancel) -> Option<u16> {
        let (user_data, generation) = request.user_parts();

        let cancel_id = self.next_cancel_completion_id();
        let cancel_sqe = opcode::AsyncCancel::new(CompletionToken::user(request.target).raw())
            .build()
            .user_data(CompletionToken::cancel(cancel_id).raw());

        if self.push_entry(cancel_sqe) {
            self.pending_cancel_cqes.insert(cancel_id, request);
            self.completion_diagnostics.inc_cancel_submitted();
            trace!(
                user_data,
                generation,
                cancel_id,
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
            CancelSubmitOutcome::Queued
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, request: CancelRequest) -> CancelSubmitOutcome {
        let request = PendingCancel::new(request);
        let (user_data, generation) = request.user_parts();
        let token = request.target;

        match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                let sidecar = cancel_slot_immediate(slot, token);
                if request.mode == CancelMode::UserVisible {
                    self.push_completion_event(sidecar);
                } else {
                    let mut cleanup = sidecar.cleanup;
                    let _ = run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                }
                self.finalize_orphaned_completion_checked(
                    token,
                    "uring.cancel_op_internal.reserved",
                );
                CancelSubmitOutcome::AlreadyComplete
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                if slot.platform().submission_state == UringSubmissionState::Queued {
                    let sidecar = cancel_slot_immediate(slot, token);
                    self.remove_backlog_token(token);
                    if request.mode == CancelMode::UserVisible {
                        self.push_completion_event(sidecar);
                    } else {
                        let mut cleanup = sidecar.cleanup;
                        let _ =
                            run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                    }
                    self.finalize_waiting_completion_checked(
                        token,
                        "uring.cancel_op_internal.waiting_queued",
                    );
                    return CancelSubmitOutcome::AlreadyComplete;
                }

                if let Some(tid) = slot.platform_mut().timer_id {
                    let mut completed = if request.mode == CancelMode::Abandon {
                        slot.cancel().complete()
                    } else {
                        slot.complete()
                    };
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();
                    let sidecar = CompletionSidecar::<UringUserPayload, UringError> {
                        token,
                        res: -libc::ECANCELED,
                        flags: 0,
                        payload,
                        detail,
                        cleanup: CompletionCleanupGuard::default(),
                    };
                    self.wheel.cancel(tid);
                    if request.mode == CancelMode::UserVisible {
                        self.push_completion_event(sidecar);
                    } else {
                        let mut cleanup = sidecar.cleanup;
                        let _ =
                            run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                    }
                    self.finalize_orphaned_completion_checked(
                        token,
                        "uring.cancel_op_internal.waiting_timer",
                    );
                    return CancelSubmitOutcome::AlreadyComplete;
                }

                if request.mode == CancelMode::Abandon {
                    let _ = slot.cancel();
                }
                self.submit_cancel_request(request)

                // Cancellation is async, we wait for CQE to clean up.
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                if slot.platform().submission_state == UringSubmissionState::Queued {
                    let sidecar = cancel_slot_immediate(slot, token);
                    self.remove_backlog_token(token);
                    let mut cleanup = sidecar.cleanup;
                    let _ = run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                    self.finalize_orphaned_completion_checked(
                        token,
                        "uring.cancel_op_internal.orphaned_queued",
                    );
                    return CancelSubmitOutcome::AlreadyComplete;
                }

                if let Some(tid) = slot.platform_mut().timer_id {
                    let sidecar = cancel_slot_immediate(slot, token);
                    self.wheel.cancel(tid);
                    let mut cleanup = sidecar.cleanup;
                    let _ = run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                    self.finalize_orphaned_completion_checked(
                        token,
                        "uring.cancel_op_internal.orphaned_timer",
                    );
                    return CancelSubmitOutcome::AlreadyComplete;
                }

                self.submit_cancel_request(request)
            }
            CheckedSlotView::Missing { .. } | CheckedSlotView::Empty(_) => {
                self.completion_diagnostics.inc_unknown_completion();
                debug!(
                    user_data,
                    generation,
                    token = CompletionToken::user(request.target).raw(),
                    "cancel request did not match an active slot"
                );
                CancelSubmitOutcome::NotFound
            }
            CheckedSlotView::Stale(snapshot) => {
                self.completion_diagnostics.inc_stale_completion();
                debug!(
                    user_data,
                    generation,
                    actual_generation = snapshot.generation,
                    state = ?snapshot.state,
                    "cancel request is stale"
                );
                CancelSubmitOutcome::NotFound
            }
            CheckedSlotView::Corrupt(snapshot) => {
                self.completion_diagnostics.inc_slot_corruption();
                error!(
                    user_data,
                    generation,
                    actual_generation = snapshot.generation,
                    state = ?snapshot.state,
                    has_op = snapshot.has_op,
                    has_payload = snapshot.has_payload,
                    "cancel request found corrupt slot; recycling"
                );
                self.finalize_corrupt_slot_checked(snapshot, "uring.cancel_op_internal.corrupt");
                CancelSubmitOutcome::NotFound
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
                    CheckedSlotView::Missing {
                        index,
                        expected_generation,
                    } => {
                        self.pending_cancellations.pop_front();
                        let anomaly = CompletionAnomaly::unknown_slot(
                            CompletionToken::user(request.target),
                            index,
                            expected_generation,
                        );
                        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                        continue;
                    }
                    CheckedSlotView::Empty(snapshot) => {
                        self.pending_cancellations.pop_front();
                        let anomaly = CompletionAnomaly::non_active(
                            CompletionToken::user(request.target),
                            snapshot.index,
                            request.target.generation(),
                            snapshot.state,
                        )
                        .with_slot_snapshot(snapshot);
                        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                        continue;
                    }
                    CheckedSlotView::Stale(snapshot) => {
                        self.pending_cancellations.pop_front();
                        let anomaly = CompletionAnomaly::stale(
                            CompletionToken::user(request.target),
                            snapshot.index,
                            request.target.generation(),
                            snapshot.generation,
                            snapshot.state,
                        )
                        .with_slot_snapshot(snapshot);
                        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                        continue;
                    }
                    CheckedSlotView::Corrupt(snapshot) => {
                        self.pending_cancellations.pop_front();
                        let anomaly = CompletionAnomaly::corrupt(
                            CompletionToken::user(request.target),
                            snapshot.index,
                            snapshot.generation,
                            snapshot.state,
                        )
                        .with_slot_snapshot(snapshot);
                        record_completion_anomaly(&mut self.completion_diagnostics, &anomaly);
                        self.finalize_corrupt_slot_checked(
                            snapshot,
                            "uring.flush_cancellations.corrupt",
                        );
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
                    if let CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) =
                        self.ops.checked_slot_view(token)
                    {
                        let sidecar = cancel_slot_immediate(slot, token);
                        let mut cleanup = sidecar.cleanup;
                        let _ =
                            run_completion_cleanup(&mut self.completion_diagnostics, &mut cleanup);
                        self.finalize_orphaned_completion_checked(
                            token,
                            "uring.flush_backlog.cancel_queued",
                        );
                    }
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
                    Ok(false) => {
                        // SQ Full, stop processing backlog
                        break;
                    }
                    Err(_) => {
                        // Error during submission
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
        if let CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) =
            self.ops.checked_slot_view(token)
        {
            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            let sidecar = CompletionSidecar::<UringUserPayload, UringError> {
                token,
                res: event_res,
                flags: 0,
                payload,
                detail: detail.or(Some(Err(report))),
                cleanup: CompletionCleanupGuard::default(),
            };
            self.push_completion_event(sidecar);
            self.finalize_waiting_completion_checked(
                token,
                "uring.complete_queued_submission_error",
            );
        }
    }
}

fn cancel_slot_immediate<'a, S: SlotState>(
    slot: Slot<'a, S>,
    token: OpToken,
) -> CompletionSidecar<UringUserPayload, UringError> {
    let (payload, detail) = slot.storage.with_mut(
        |result: &mut Option<UringDriverResult<usize>>,
         payload: &mut Option<UringUserPayload>,
         _sidecar| (payload.take(), result.take()),
    );
    let _ = slot.op.take();

    CompletionSidecar::<UringUserPayload, UringError> {
        token,
        res: -libc::ECANCELED,
        flags: 0,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.op.is_some()
}
