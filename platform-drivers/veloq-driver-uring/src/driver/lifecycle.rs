use crate::driver::{CANCEL_USER_DATA, UringDriver};
use io_uring::opcode;
use std::io;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::CompletionSidecar;

use crate::op::slot::{Slot, SlotState, SlotView, UringOpRegistryExt};
use veloq_driver_core::slot::ErasedPayload;

#[derive(Clone, Default)]
pub struct UringOpState {
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl UringDriver {
    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        let Some(slot) = self.ops.slot_view(user_data) else {
            return;
        };

        match slot {
            SlotView::Pending(slot) => {
                let sidecar = cancel_slot_immediate(slot, user_data);
                self.push_completion_event(sidecar);
                self.ops.remove(user_data);
            }
            SlotView::Initialized(slot) => {
                let sidecar = cancel_slot_immediate(slot, user_data);
                self.push_completion_event(sidecar);
                self.ops.remove(user_data);
            }
            SlotView::InFlight(slot) => {
                let timer_data = {
                    let timer_id = slot.platform.timer_id;
                    let cancelled = slot.cancel();

                    timer_id.map(|tid| {
                        let mut completed = cancelled.complete();
                        let generation = completed.entry.generation.load(Ordering::Acquire);
                        let _ = completed.take_op();
                        let (payload, detail) = completed.take_completion_data();

                        (
                            tid,
                            CompletionSidecar {
                                user_data,
                                generation,
                                res: -libc::ECANCELED,
                                flags: 0,
                                payload,
                                detail,
                            },
                        )
                    })
                };

                if let Some((tid, sidecar)) = timer_data {
                    self.wheel.cancel(tid);
                    self.push_completion_event(sidecar);
                    self.ops.remove(user_data);
                    return;
                }

                let cancel_sqe = opcode::AsyncCancel::new(user_data as u64)
                    .build()
                    .user_data(CANCEL_USER_DATA);

                if !self.push_entry(cancel_sqe) {
                    self.pending_cancellations.push_back(user_data);
                }

                // Cancellation is async, we wait for CQE to clean up.
            }
            SlotView::Cancelled(_) => {}
        }
    }

    pub(crate) fn flush_cancellations(&mut self) {
        let mut submitted_count = 0;
        let limit = self.pending_cancellations.len();

        while submitted_count < limit {
            if let Some(user_data) = self.pending_cancellations.front().cloned() {
                if !self.ops.contains(user_data) {
                    self.pending_cancellations.pop_front();
                    continue;
                }

                let cancel_sqe = opcode::AsyncCancel::new(user_data as u64)
                    .build()
                    .user_data(CANCEL_USER_DATA);

                if self.push_entry(cancel_sqe) {
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
                Some(SlotView::Cancelled(_)) => BacklogAction::Cancel,
                Some(SlotView::Pending(slot)) => {
                    if slot_has_op(slot) {
                        BacklogAction::Submit
                    } else {
                        BacklogAction::Drop
                    }
                }
                Some(SlotView::Initialized(slot)) => {
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
                    self.cancel_op_internal(user_data);
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
) -> CompletionSidecar {
    let generation = slot.entry.generation.load(Ordering::Acquire);
    let (payload, detail) = slot.storage.with_mut(
        |_op: &mut Option<crate::op::UringOp>,
         result: &mut Option<io::Result<usize>>,
         payload: &mut Option<ErasedPayload>,
         _sidecar| (payload.take(), result.take()),
    );
    let _ = slot
        .storage
        .with_mut(|op: &mut Option<crate::op::UringOp>, _, _, _| op.take());

    CompletionSidecar {
        user_data,
        generation,
        res: -libc::ECANCELED,
        flags: 0,
        payload,
        detail,
    }
}

fn slot_has_op<'a, S: SlotState>(slot: Slot<'a, S>) -> bool {
    slot.storage
        .with_mut(|slot_op, _result, _payload, _sidecar| slot_op.is_some())
}
