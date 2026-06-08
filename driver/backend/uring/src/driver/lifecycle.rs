use crate::driver::{PendingCancel, UringDriver};
use crate::error::{UringDriverResult, UringError};
use io_uring::opcode;
use std::sync::atomic::Ordering;
use tracing::{debug, error, trace};
use veloq_driver_core::driver::{CancelMode, CancelRequest, CompletionSidecar, CompletionToken};

use crate::op::{
    UringUserPayload,
    slot::{CheckedSlotView, Slot, SlotState, SlotView, UringOpRegistryExt},
};

#[derive(Clone, Default)]
pub struct UringOpState {
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
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

    fn submit_cancel_request(&mut self, request: PendingCancel) {
        let Some((user_data, generation)) = request.user_parts() else {
            self.completion_diagnostics.inc_unknown_completion();
            debug!(
                token = request.token.raw(),
                mode = ?request.mode,
                "skipping cancel request for non-user token"
            );
            return;
        };

        let cancel_id = self.next_cancel_completion_id();
        let cancel_sqe = opcode::AsyncCancel::new(request.token.raw())
            .build()
            .user_data(CompletionToken::cancel(cancel_id).raw());

        if !self.push_entry(cancel_sqe) {
            self.pending_cancellations.push_back(request);
        } else {
            self.pending_cancel_cqes.insert(cancel_id, request);
            self.completion_diagnostics.inc_cancel_submitted();
            trace!(
                user_data,
                generation,
                cancel_id,
                mode = ?request.mode,
                "submitted async cancel"
            );
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, request: CancelRequest) {
        let request = PendingCancel::new(request);
        let Some((user_data, generation)) = request.user_parts() else {
            self.completion_diagnostics.inc_unknown_completion();
            debug!(
                token = request.token.raw(),
                "cancel request token is not a user op"
            );
            return;
        };

        match self.ops.checked_slot_view(user_data, generation) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => {
                let sidecar = cancel_slot_immediate(slot, user_data);
                if request.mode == CancelMode::UserVisible {
                    self.push_completion_event(sidecar);
                }
                self.ops.remove(user_data);
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                if let Some(tid) = slot.platform_mut().timer_id {
                    let mut completed = if request.mode == CancelMode::Abandon {
                        slot.cancel().complete()
                    } else {
                        slot.complete()
                    };
                    let generation = completed.entry.generation(Ordering::Acquire);
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();
                    let sidecar = CompletionSidecar::<UringUserPayload, UringError> {
                        user_data,
                        generation,
                        res: -libc::ECANCELED,
                        flags: 0,
                        payload,
                        detail,
                    };
                    self.wheel.cancel(tid);
                    self.push_completion_event(sidecar);
                    self.ops.remove(user_data);
                    return;
                }

                if request.mode == CancelMode::Abandon {
                    let _ = slot.cancel();
                }
                self.submit_cancel_request(request);

                // Cancellation is async, we wait for CQE to clean up.
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                if let Some(tid) = slot.platform_mut().timer_id {
                    let sidecar = cancel_slot_immediate(slot, user_data);
                    self.wheel.cancel(tid);
                    self.push_completion_event(sidecar);
                    self.ops.remove(user_data);
                    return;
                }

                self.submit_cancel_request(request);
            }
            CheckedSlotView::Missing { .. } | CheckedSlotView::Empty(_) => {
                self.completion_diagnostics.inc_unknown_completion();
                debug!(
                    user_data,
                    generation,
                    token = request.token.raw(),
                    "cancel request did not match an active slot"
                );
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
                let _ = self
                    .ops
                    .recycle_if_active(user_data, snapshot.generation.wrapping_add(1));
            }
        }
    }

    pub(crate) fn flush_cancellations(&mut self) {
        let mut submitted_count = 0;
        let limit = self.pending_cancellations.len();

        while submitted_count < limit {
            if let Some(request) = self.pending_cancellations.front().copied() {
                let Some((user_data, generation)) = request.user_parts() else {
                    self.pending_cancellations.pop_front();
                    self.completion_diagnostics.inc_unknown_completion();
                    continue;
                };

                let stale_or_missing = !matches!(
                    self.ops.checked_slot_view(user_data, generation),
                    CheckedSlotView::Valid(_)
                );
                if stale_or_missing {
                    self.pending_cancellations.pop_front();
                    self.completion_diagnostics.inc_stale_completion();
                    continue;
                }

                let cancel_id = self.next_cancel_completion_id();
                let cancel_sqe = opcode::AsyncCancel::new(request.token.raw())
                    .build()
                    .user_data(CompletionToken::cancel(cancel_id).raw());

                if self.push_entry(cancel_sqe) {
                    self.pending_cancel_cqes.insert(cancel_id, request);
                    self.completion_diagnostics.inc_cancel_submitted();
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
            Submit,
            Cancel,
            Drop,
        }

        while let Some(&user_data) = self.backlog.front() {
            let action = match self.ops.slot_view(user_data) {
                Some(SlotView::InFlightOrphaned(_)) => BacklogAction::Cancel,
                Some(SlotView::Reserved(slot)) => {
                    if slot_has_op(slot) {
                        BacklogAction::Submit
                    } else {
                        BacklogAction::Drop
                    }
                }
                _ => BacklogAction::Drop,
            };

            match action {
                BacklogAction::Cancel => {
                    self.pop_backlog();
                    let generation = self.ops.shared.slots[user_data].generation(Ordering::Acquire);
                    self.cancel_op_internal(CancelRequest::abandon(CompletionToken::user(
                        user_data, generation,
                    )));
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                }
                BacklogAction::Submit => match self.submit_from_slot_index(user_data) {
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
            }
        }
    }

    pub(crate) fn push_backlog(&mut self, user_data: usize) {
        self.backlog.push_back(user_data);
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<usize> {
        self.backlog.pop_front()
    }
}

fn cancel_slot_immediate<'a, S: SlotState>(
    slot: Slot<'a, S>,
    user_data: usize,
) -> CompletionSidecar<UringUserPayload, UringError> {
    let generation = slot.entry.generation(Ordering::Acquire);
    let (payload, detail) = slot.storage.with_mut(
        |result: &mut Option<UringDriverResult<usize>>,
         payload: &mut Option<UringUserPayload>,
         _sidecar| (payload.take(), result.take()),
    );
    let _ = slot.op.take();

    CompletionSidecar::<UringUserPayload, UringError> {
        user_data,
        generation,
        res: -libc::ECANCELED,
        flags: 0,
        payload,
        detail,
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.op.is_some()
}
