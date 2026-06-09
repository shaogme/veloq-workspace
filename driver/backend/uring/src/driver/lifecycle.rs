use crate::driver::{PendingCancel, UringDriver};
use crate::error::{UringDriverResult, UringError, uring_report_to_event_res};
use diagweave::prelude::*;
use io_uring::opcode;
use tracing::{debug, error, trace};
use veloq_driver_core::driver::{
    CancelCompletionId, CancelMode, CancelRequest, CancelSubmitOutcome, CompletionAnomalyReason,
    CompletionBackend, CompletionCleanupGuard, CompletionSidecar, CompletionToken, OpToken,
    RawCompletion, UserCompletionEvent, record_completion_anomaly, record_lost_completion,
    run_completion_cleanup, slot_view_anomaly,
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

enum ImmediateCancelOutcome {
    User(CompletionSidecar<UringUserPayload, UringError>),
    Lost {
        anomaly: veloq_driver_core::driver::CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
        snapshot: veloq_driver_core::slot::SlotSnapshot,
    },
}

impl<'a> UringDriver<'a> {
    fn next_cancel_completion_id(&mut self) -> CancelCompletionId {
        loop {
            let id = self.next_cancel_id;
            self.next_cancel_id = self.next_cancel_id.wrapping_add(1);
            if self.next_cancel_id == 0 {
                self.next_cancel_id = 1;
            }
            let id = CancelCompletionId::new(id);
            if !self.pending_cancel_cqes.contains_key(&id) {
                return id;
            }
        }
    }

    fn try_submit_cancel_request(&mut self, request: PendingCancel) -> Option<CancelCompletionId> {
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
            CancelSubmitOutcome::Queued
        }
    }

    fn finish_immediate_cancel(
        &mut self,
        token: OpToken,
        outcome: ImmediateCancelOutcome,
        emit_completion: bool,
        finalize_waiting: bool,
        context: &'static str,
    ) {
        match outcome {
            ImmediateCancelOutcome::User(sidecar) => {
                if emit_completion {
                    self.push_completion_event(sidecar);
                } else {
                    let mut cleanup = sidecar.cleanup;
                    let _ = run_completion_cleanup(&self.completion_diagnostics, &mut cleanup);
                }
                if finalize_waiting {
                    self.finalize_waiting_completion_checked(token, context);
                } else {
                    self.finalize_orphaned_completion_checked(token, context);
                }
            }
            ImmediateCancelOutcome::Lost {
                anomaly,
                mut cleanup,
                snapshot,
            } => {
                if emit_completion {
                    let event = UserCompletionEvent::from_parts(
                        CompletionBackend::Uring,
                        token,
                        -libc::ECANCELED,
                        0,
                    );
                    let _ = record_lost_completion(
                        &self.completion_table,
                        &self.completion_diagnostics,
                        event,
                        anomaly,
                        cleanup,
                    );
                } else {
                    record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                    let _ = run_completion_cleanup(&self.completion_diagnostics, &mut cleanup);
                }
                self.finalize_corrupt_slot_checked(snapshot, context);
            }
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, request: CancelRequest) -> CancelSubmitOutcome {
        let request = PendingCancel::new(request);
        let (user_data, generation) = request.user_parts();
        let token = request.target;

        match self.ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                let sidecar = cancel_slot_immediate(slot, token);
                self.finish_immediate_cancel(
                    token,
                    sidecar,
                    request.mode == CancelMode::UserVisible,
                    false,
                    "uring.cancel_op_internal.reserved",
                );
                CancelSubmitOutcome::CompletedLocally
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                if slot.platform().submission_state == UringSubmissionState::Queued {
                    let sidecar = cancel_slot_immediate(slot, token);
                    self.remove_backlog_token(token);
                    self.finish_immediate_cancel(
                        token,
                        sidecar,
                        request.mode == CancelMode::UserVisible,
                        true,
                        "uring.cancel_op_internal.waiting_queued",
                    );
                    return CancelSubmitOutcome::CompletedLocally;
                }

                if let Some(tid) = slot.platform_mut().timer_id {
                    let mut completed = if request.mode == CancelMode::Abandon {
                        slot.cancel().complete()
                    } else {
                        slot.complete()
                    };
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();
                    self.wheel.cancel(tid);
                    if let Some(payload) = payload {
                        let sidecar = CompletionSidecar::<UringUserPayload, UringError>::new(
                            UserCompletionEvent::from_parts(
                                CompletionBackend::Uring,
                                token,
                                -libc::ECANCELED,
                                0,
                            ),
                            payload,
                            detail,
                            CompletionCleanupGuard::default(),
                        );
                        if request.mode == CancelMode::UserVisible {
                            self.push_completion_event(sidecar);
                        } else {
                            let mut cleanup = sidecar.cleanup;
                            let _ =
                                run_completion_cleanup(&self.completion_diagnostics, &mut cleanup);
                        }
                    } else {
                        drop(detail);
                        let raw = RawCompletion::new(
                            CompletionBackend::Uring,
                            CompletionToken::user(token),
                            -libc::ECANCELED,
                            0,
                        );
                        let snapshot = completed.snapshot();
                        let anomaly =
                            veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                                .with_slot_snapshot(snapshot)
                                .with_raw_completion(raw);
                        record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                        self.finalize_corrupt_slot_checked(
                            snapshot,
                            "uring.cancel_op_internal.waiting_timer_missing_payload",
                        );
                        return CancelSubmitOutcome::CompletedLocally;
                    }
                    self.finalize_orphaned_completion_checked(
                        token,
                        "uring.cancel_op_internal.waiting_timer",
                    );
                    return CancelSubmitOutcome::CompletedLocally;
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
                    self.finish_immediate_cancel(
                        token,
                        sidecar,
                        false,
                        false,
                        "uring.cancel_op_internal.orphaned_queued",
                    );
                    return CancelSubmitOutcome::CompletedLocally;
                }

                if let Some(tid) = slot.platform_mut().timer_id {
                    let sidecar = cancel_slot_immediate(slot, token);
                    self.wheel.cancel(tid);
                    self.finish_immediate_cancel(
                        token,
                        sidecar,
                        false,
                        false,
                        "uring.cancel_op_internal.orphaned_timer",
                    );
                    return CancelSubmitOutcome::CompletedLocally;
                }

                self.submit_cancel_request(request)
            }
            view @ (CheckedSlotView::Missing { .. }
            | CheckedSlotView::Empty(_)
            | CheckedSlotView::Stale(_)
            | CheckedSlotView::Corrupt(_)) => {
                let raw = RawCompletion::new(
                    CompletionBackend::Uring,
                    CompletionToken::user(request.target),
                    -libc::ECANCELED,
                    0,
                );
                let anomaly = match slot_view_anomaly(CompletionBackend::Uring, token, raw, view) {
                    Ok(_) => {
                        return CancelSubmitOutcome::TargetMissing;
                    }
                    Err(anomaly) => anomaly,
                };
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                match anomaly.reason {
                    CompletionAnomalyReason::StaleGeneration => {
                        debug!(
                            user_data,
                            generation,
                            actual_generation = anomaly.actual_generation,
                            state = ?anomaly.state,
                            "cancel request is stale"
                        );
                        CancelSubmitOutcome::TargetStale
                    }
                    CompletionAnomalyReason::OpMissing
                    | CompletionAnomalyReason::PayloadMissing
                    | CompletionAnomalyReason::SlotCorruption => {
                        error!(
                            user_data,
                            generation,
                            snapshot = ?anomaly.slot_snapshot,
                            "cancel request found corrupt slot; recycling"
                        );
                        if let Some(snapshot) = anomaly.slot_snapshot {
                            self.finalize_corrupt_slot_checked(
                                snapshot,
                                "uring.cancel_op_internal.corrupt",
                            );
                        }
                        CancelSubmitOutcome::TargetMissing
                    }
                    _ => {
                        debug!(
                            user_data,
                            generation,
                            token = CompletionToken::user(request.target).raw(),
                            reason = ?anomaly.reason,
                            "cancel request did not match an active slot"
                        );
                        CancelSubmitOutcome::TargetMissing
                    }
                }
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
                    view @ (CheckedSlotView::Missing { .. }
                    | CheckedSlotView::Empty(_)
                    | CheckedSlotView::Stale(_)
                    | CheckedSlotView::Corrupt(_)) => {
                        self.pending_cancellations.pop_front();
                        let raw = RawCompletion::new(
                            CompletionBackend::Uring,
                            CompletionToken::user(request.target),
                            -libc::ECANCELED,
                            0,
                        );
                        let anomaly = match slot_view_anomaly(
                            CompletionBackend::Uring,
                            request.target,
                            raw,
                            view,
                        ) {
                            Ok(_) => continue,
                            Err(anomaly) => anomaly,
                        };
                        record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                        if matches!(
                            anomaly.reason,
                            CompletionAnomalyReason::OpMissing
                                | CompletionAnomalyReason::PayloadMissing
                                | CompletionAnomalyReason::SlotCorruption
                        ) && let Some(snapshot) = anomaly.slot_snapshot
                        {
                            self.finalize_corrupt_slot_checked(
                                snapshot,
                                "uring.flush_cancellations.corrupt",
                            );
                        }
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
                        self.finish_immediate_cancel(
                            token,
                            sidecar,
                            false,
                            false,
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
            if let Some(payload) = payload {
                let sidecar = CompletionSidecar::<UringUserPayload, UringError>::new(
                    UserCompletionEvent::from_parts(CompletionBackend::Uring, token, event_res, 0),
                    payload,
                    detail.or(Some(Err(report))),
                    CompletionCleanupGuard::default(),
                );
                self.push_completion_event(sidecar);
            } else {
                drop(detail);
                let raw = RawCompletion::new(
                    CompletionBackend::Uring,
                    CompletionToken::user(token),
                    event_res,
                    0,
                );
                let snapshot = completed.snapshot();
                let anomaly = veloq_driver_core::driver::corrupt_slot_anomaly(raw.token, snapshot)
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                self.finalize_corrupt_slot_checked(
                    snapshot,
                    "uring.complete_queued_submission_error.missing_payload",
                );
                return;
            }
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
) -> ImmediateCancelOutcome {
    let snapshot = slot.snapshot();
    let cleanup = slot
        .op
        .as_mut()
        .map(|op| unsafe { (op.vtable.completion_cleanup)(op, -libc::ECANCELED) })
        .unwrap_or_default();
    let (payload, detail) = slot.storage.with_mut(
        |result: &mut Option<UringDriverResult<usize>>,
         payload: &mut Option<UringUserPayload>,
         _sidecar| (payload.take(), result.take()),
    );
    let _ = slot.op.take();

    let event =
        UserCompletionEvent::from_parts(CompletionBackend::Uring, token, -libc::ECANCELED, 0);
    match (snapshot.has_op, payload) {
        (true, Some(payload)) => {
            ImmediateCancelOutcome::User(CompletionSidecar::<UringUserPayload, UringError>::new(
                event, payload, detail, cleanup,
            ))
        }
        (_, payload) => {
            drop(payload);
            drop(detail);
            let anomaly =
                veloq_driver_core::driver::corrupt_slot_anomaly(event.completion_token(), snapshot)
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(event.raw());
            ImmediateCancelOutcome::Lost {
                anomaly,
                cleanup,
                snapshot,
            }
        }
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.op.is_some()
}
