use crate::driver::{CANCEL_USER_DATA, UringDriver};
use io_uring::opcode;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::CompletionSidecar;

use crate::op::slot::{InFlight, Pending, Slot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OpLifecycle {
    /// Created, waiting to be submitted
    Pending,
    /// Submitted to ring or timer wheel
    InFlight,
    /// Completion arrived (result is in Slot)
    #[default]
    Completed,
    /// Aborted by user
    Cancelled,
}

#[derive(Clone)]
pub struct UringOpState {
    pub(crate) lifecycle: OpLifecycle,
    pub(crate) next: Option<usize>,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
}

impl Default for UringOpState {
    fn default() -> Self {
        Self {
            lifecycle: OpLifecycle::Completed,
            next: None,
            timer_id: None,
        }
    }
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl UringDriver {
    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        let (slot_entry, op_entry, storage) = match self.ops.get_slot_entry_storage_and_entry_mut(user_data) {
            Some(v) => v,
            None => return,
        };

        match op_entry.platform_data.lifecycle {
            OpLifecycle::Completed | OpLifecycle::Cancelled => {} // already done
            OpLifecycle::Pending => {
                let slot = Slot::<Pending>::new(slot_entry, storage, &mut op_entry.platform_data, user_data);
                
                let generation = slot.entry.generation.load(Ordering::Acquire);
                let _ = slot.storage.with_mut(|op: &mut Option<crate::op::UringOp>, _, _, _| op.take());
                let (payload, detail) = slot.storage.with_mut(|_op: &mut Option<crate::op::UringOp>, result, payload, _sidecar| (payload.take(), result.take()));
                
                self.push_completion_event(CompletionSidecar {
                    user_data,
                    generation,
                    res: -(libc::ECANCELED as i32),
                    flags: 0,
                    payload,
                    detail,
                });
                self.ops.remove(user_data);
            }
            OpLifecycle::InFlight => {
                let timer_id = op_entry.platform_data.timer_id;
                let slot = Slot::<InFlight>::as_in_flight(slot_entry, storage, &mut op_entry.platform_data, user_data);
                
                let cancelled = slot.cancel();

                if let Some(tid) = timer_id {
                    self.wheel.cancel(tid);
                    
                    let mut completed = cancelled.complete();
                    let generation = completed.entry.generation.load(Ordering::Acquire);
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();
                    
                    self.push_completion_event(CompletionSidecar {
                        user_data,
                        generation,
                        res: -(libc::ECANCELED as i32),
                        flags: 0,
                        payload,
                        detail,
                    });
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

            if let Some(entry) = self.ops.get(user_data) {
                action = match entry.platform_data.lifecycle {
                    OpLifecycle::Cancelled => BacklogAction::Cancel,
                    OpLifecycle::Pending => {
                        // Check if op exists in slot
                        if self
                            .ops
                            .with_slot_storage_mut(
                                user_data,
                                |slot_op, _result, _payload, _sidecar| slot_op.is_some(),
                            )
                            .unwrap_or(false)
                        {
                            BacklogAction::Submit
                        } else {
                            BacklogAction::Drop
                        }
                    }
                    _ => BacklogAction::Drop,
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
