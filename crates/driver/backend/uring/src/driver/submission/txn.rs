use crate::{
    driver::{env::SubmitEnv, lifecycle::UringSubmissionState},
    error::{UringError, UringResult},
    op::{Reserved, Slot, SubmissionStrategy, UringSlotSpec},
};
use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionToken, OpToken, SubmitTokenContext},
    slot::{SlotAccessError, SubmissionGuard},
};
use veloq_std::format;
use veloq_wheel::TaskId;

pub(crate) fn slot_access_report(scope: &'static str, err: SlotAccessError) -> Report<UringError> {
    UringError::InvalidState
        .to_report()
        .push_ctx("scope", scope)
        .with_ctx("slot_index", err.snapshot.index)
        .with_ctx("slot_generation", err.snapshot.generation)
        .with_ctx("slot_status", format!("{:?}", err.snapshot.status))
        .with_ctx("slot_has_op", err.snapshot.has_op)
        .with_ctx("slot_has_payload", err.snapshot.has_payload)
        .with_ctx("slot_access_action", format!("{:?}", err.action))
        .with_ctx("slot_access_reason", format!("{:?}", err.reason))
        .attach_note("slot access failed during uring submission transaction")
}

pub(crate) struct UringSubmitTxn<'a, 'b, 'e, 's> {
    env: &'e mut SubmitEnv<'a, 'b>,
    token: OpToken,
    slot_guard: Option<SubmissionGuard<'s, UringSlotSpec>>,
    newly_registered_chunks: [ChunkId; 4],
    newly_registered_count: usize,
    timer_inserted: Option<TaskId>,
    submitted: bool,
}

impl<'a, 'b, 'e, 's> UringSubmitTxn<'a, 'b, 'e, 's> {
    pub(crate) fn new(
        env: &'e mut SubmitEnv<'a, 'b>,
        token: OpToken,
        slot: Slot<'s, Reserved>,
    ) -> UringResult<Self> {
        let slot_guard = slot
            .start_submission_with(None)
            .map_err(|err| slot_access_report("driver.submit_txn.start", err))?;

        Ok(Self {
            env,
            token,
            slot_guard: Some(slot_guard),
            newly_registered_chunks: [ChunkId::ZERO; 4],
            newly_registered_count: 0,
            timer_inserted: None,
            submitted: false,
        })
    }

    pub(crate) fn submit(mut self) -> UringResult<bool> {
        let user_data = self.token.index();
        let slot = self
            .slot_guard
            .as_mut()
            .and_then(|g| g.slot.as_mut())
            .ok_or_else(|| {
                UringError::InvalidState
                    .report("driver.submit_txn.submit", "submission guard slot missing")
            })?;

        let strategy = slot
            .op_mut()
            .map_err(|err| slot_access_report("driver.submit_txn.strategy", err))?
            .vtable
            .strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [ChunkId::ZERO; 4];
                let (count, sqe) = {
                    let sqe_env = self.env.sqe_env();
                    let token = self.token;
                    slot.with_op_and_payload_mut(|op, payload| {
                        let vtable = op.vtable;
                        let count = unsafe { (vtable.resolve_chunks)(op, payload, &mut chunks) };
                        let completion_token = CompletionToken::user(token);
                        let sqe = unsafe {
                            (vtable.make_sqe)(
                                op,
                                payload,
                                &sqe_env,
                                SubmitTokenContext::new(token, completion_token),
                            )
                            .attach_note("driver.submit_txn.make_sqe")?
                            .user_data(completion_token.raw())
                        };
                        Ok::<_, Report<UringError>>((count, sqe))
                    })
                    .map_err(|err| slot_access_report("driver.submit_txn.op_payload", err))??
                };

                for &chunk_id in chunks.iter().take(count) {
                    let is_registered = self.env.is_chunk_registered(chunk_id);
                    self.env.ensure_chunk_registered(
                        chunk_id,
                        user_data,
                        "driver.submit_txn.ensure_chunk_registered",
                    )?;
                    if !is_registered && self.newly_registered_count < 4 {
                        self.newly_registered_chunks[self.newly_registered_count] = chunk_id;
                        self.newly_registered_count += 1;
                    }
                }

                let pushed = self.env.push_entry(sqe);
                slot.platform_mut().submission_state = if pushed {
                    UringSubmissionState::KernelSubmitted
                } else {
                    UringSubmissionState::Queued
                };

                self.commit();

                if pushed {
                    trace!(user_data, "Submitted to SQ");
                } else {
                    debug!(user_data, "SQ full");
                }
                Ok(pushed)
            }
            SubmissionStrategy::SoftwareTimer => {
                let duration_opt = slot
                    .with_op_and_payload_mut(|op, payload| {
                        let vtable = op.vtable;
                        unsafe { (vtable.get_timeout)(op, payload) }
                    })
                    .map_err(|err| slot_access_report("driver.submit_txn.timer.op_payload", err))?;
                let Some(duration) = duration_opt else {
                    return Err(UringError::InvalidInput
                        .report("driver.submit_txn.timer_duration", "Timer duration missing"));
                };

                let task_id = self.env.wheel.insert(self.token, duration);
                self.timer_inserted = Some(task_id);

                let platform = slot.platform_mut();
                platform.timer_id = Some(task_id);
                platform.submission_state = UringSubmissionState::Timer;

                self.commit();

                trace!(user_data, ?duration, "Registered software timer");
                Ok(true)
            }
        }
    }

    fn commit(&mut self) {
        self.submitted = true;
        if let Some(guard) = self.slot_guard.take() {
            let _ = guard.persist();
        }
    }
}

impl Drop for UringSubmitTxn<'_, '_, '_, '_> {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }

        if let Some(task_id) = self.timer_inserted.take() {
            self.env.wheel.cancel(task_id);
        }

        for i in 0..self.newly_registered_count {
            let chunk_id = self.newly_registered_chunks[i];
            self.env.unmark_registered_chunk(chunk_id);
        }
    }
}
