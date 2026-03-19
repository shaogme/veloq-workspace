use crate::driver::{CANCEL_USER_DATA, UringDriver};
use io_uring::opcode;
use std::io;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::CompletionSidecar;

use crate::op::slot::UringOpRegistryExt;
use veloq_driver_core::slot::{ErasedPayload, SlotState as CoreState};

#[derive(Clone, Default)]
pub struct UringOpState {
    pub(crate) next: Option<usize>,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl UringDriver {
    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        let state = match self.ops.get_slot_and_entry_mut(user_data) {
            Some((slot_entry, _op_entry)) => slot_entry.state.load(Ordering::Acquire),
            None => return,
        };

        if state == CoreState::Cancelled as u8 || state == CoreState::Completed as u8 {
            return;
        }

        match state {
            s if s == CoreState::Pending as u8 || s == CoreState::Initialized as u8 => {
                let sidecar = self
                    .ops
                    .slot_initialized(user_data)
                    .map(|slot| {
                        let generation = slot.entry.generation.load(Ordering::Acquire);
                        let (payload, detail) = slot.storage.with_mut(
                            |_op: &mut Option<crate::op::UringOp>,
                             result: &mut Option<io::Result<usize>>,
                             payload: &mut Option<ErasedPayload>,
                             _sidecar| {
                                (payload.take(), result.take())
                            },
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
                    })
                    .or_else(|| {
                        self.ops.slot_pending(user_data).map(|slot| {
                            let generation = slot.entry.generation.load(Ordering::Acquire);
                            let (payload, detail) = slot.storage.with_mut(
                                |_op: &mut Option<crate::op::UringOp>,
                                 result: &mut Option<io::Result<usize>>,
                                 payload: &mut Option<ErasedPayload>,
                                 _sidecar| {
                                    (payload.take(), result.take())
                                },
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
                        })
                    });

                if let Some(s) = sidecar {
                    self.push_completion_event(s);
                    self.ops.remove(user_data);
                }
            }
            s if s == CoreState::InFlight as u8 => {
                let timer_data = self.ops.slot_in_flight(user_data).map(|slot| {
                    let timer_id = slot.platform.timer_id;
                    let cancelled = slot.cancel();

                    if let Some(tid) = timer_id {
                        let mut completed = cancelled.complete();
                        let generation = completed.entry.generation.load(Ordering::Acquire);
                        let _ = completed.take_op();
                        let (payload, detail) = completed.take_completion_data();

                        Some((
                            tid,
                            CompletionSidecar {
                                user_data,
                                generation,
                                res: -libc::ECANCELED,
                                flags: 0,
                                payload,
                                detail,
                            },
                        ))
                    } else {
                        None
                    }
                });

                if let Some(Some((tid, sidecar))) = timer_data {
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
            _ => {}
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

        while let Some(user_data) = self.backlog_head {
            // Inspect state to decide action before taking mutable borrow for processing.
            let mut action = BacklogAction::Drop;

            if let Some((slot_entry, _op_entry)) = self.ops.get_slot_and_entry_mut(user_data) {
                let state = slot_entry.state.load(Ordering::Acquire);
                action = if state == CoreState::Cancelled as u8 {
                    BacklogAction::Cancel
                } else if state == CoreState::Pending as u8 || state == CoreState::Initialized as u8
                {
                    // Check if op exists in slot
                    if self
                        .ops
                        .with_slot_storage_mut(user_data, |slot_op, _result, _payload, _sidecar| {
                            slot_op.is_some()
                        })
                        .unwrap_or(false)
                    {
                        BacklogAction::Submit
                    } else {
                        BacklogAction::Drop
                    }
                } else {
                    BacklogAction::Drop
                };
            }

            match action {
                BacklogAction::Cancel => {
                    self.pop_backlog();
                    self.cancel_op_internal(user_data);
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                }
                BacklogAction::Submit => {
                    match self.submit_from_slot(user_data) {
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
                    }
                }
            }
        }
    }

    pub(crate) fn push_backlog(&mut self, user_data: usize) {
        if let Some(tail) = self.backlog_tail {
            if let Some(entry) = self.ops.get_mut(tail) {
                entry.platform_data.next = Some(user_data);
            }
            self.backlog_tail = Some(user_data);
        } else {
            self.backlog_head = Some(user_data);
            self.backlog_tail = Some(user_data);
        }
        if let Some(entry) = self.ops.get_mut(user_data) {
            entry.platform_data.next = None;
        }
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<usize> {
        let head = self.backlog_head?;
        let next = if let Some(entry) = self.ops.get_mut(head) {
            entry.platform_data.next
        } else {
            None
        };

        self.backlog_head = next;
        if next.is_none() {
            self.backlog_tail = None;
        }

        if let Some(entry) = self.ops.get_mut(head) {
            entry.platform_data.next = None;
        }

        Some(head)
    }
}
