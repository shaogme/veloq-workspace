use crate::{
    driver::{
        env::{StageResult, SubmitEnv},
        lifecycle::SubmissionPhase,
    },
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
use veloq_wheel::TimerId;

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
    timer_inserted: Option<TimerId>,
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
            timer_inserted: None,
            submitted: false,
        })
    }

    pub(crate) fn submit(mut self) -> UringResult<bool> {
        let user_data = self.token.index();
        let strategy = self
            .slot_guard
            .as_mut()
            .ok_or_else(|| {
                UringError::InvalidState
                    .report("driver.submit_txn.submit", "submission guard missing")
            })?
            .with_access_mut(|access| access.operation().get_ref().descriptor().strategy)
            .map_err(|err| slot_access_report("driver.submit_txn.strategy", err))?;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [ChunkId::ZERO; 4];
                let (count, sqe, cleanup_hint) = {
                    let sqe_env = self.env.sqe_env();
                    let token = self.token;
                    let guard = self.slot_guard.as_mut().ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_txn.submit_sqe",
                            "submission guard missing before SQE build",
                        )
                    })?;
                    guard
                        .with_access_mut(|access| {
                            let descriptor = access.operation().get_ref().descriptor();
                            let count =
                                unsafe { (descriptor.resolve_chunks)(access, token, &mut chunks) }?;
                            super::validate_resolved_chunk_count(
                                count,
                                chunks.len(),
                                "driver.submit_txn.resolve_chunks",
                            )?;
                            let completion_token = CompletionToken::user(token);
                            let sqe = unsafe {
                                (descriptor.make_sqe)(
                                    access,
                                    &sqe_env,
                                    SubmitTokenContext::new(token, completion_token),
                                )
                                .attach_note("driver.submit_txn.make_sqe")?
                                .user_data(completion_token.raw())
                            };
                            Ok::<_, Report<UringError>>((
                                count,
                                sqe,
                                descriptor.completion_cleanup_hint,
                            ))
                        })
                        .map_err(|err| slot_access_report("driver.submit_txn.op_payload", err))??
                };

                for &chunk_id in chunks.iter().take(count) {
                    let _decision = self.env.ensure_chunk_registered(
                        chunk_id,
                        user_data,
                        "driver.submit_txn.ensure_chunk_registered",
                    )?;
                }

                let staged = self.env.stage_user_entry(self.token, sqe, cleanup_hint)?;
                let pushed = staged == StageResult::Staged;
                let next_phase = if pushed {
                    SubmissionPhase::SqeStaged
                } else {
                    SubmissionPhase::Reserved
                };
                let slot = self
                    .slot_guard
                    .as_mut()
                    .and_then(|guard| guard.slot.as_mut())
                    .ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_txn.submit_sqe",
                            "submission guard missing after SQE build",
                        )
                    })?;
                self.env.transition_submission_state(
                    self.token,
                    &mut slot.platform_mut().control.submission,
                    next_phase,
                    if pushed {
                        "submit transaction staged SQE"
                    } else {
                        "submit transaction queued after SQ full"
                    },
                );
                self.commit();

                if pushed {
                    trace!(user_data, "Submitted to SQ");
                } else {
                    debug!(user_data, "SQ full");
                }
                Ok(pushed)
            }
            SubmissionStrategy::SoftwareTimer => {
                let duration_opt = self
                    .slot_guard
                    .as_mut()
                    .ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_txn.timer",
                            "submission guard missing before timer dispatch",
                        )
                    })?
                    .with_access_mut(|access| unsafe {
                        let descriptor = access.operation().get_ref().descriptor();
                        (descriptor.get_timeout)(access, self.token)
                    })
                    .map_err(|err| {
                        slot_access_report("driver.submit_txn.timer.op_payload", err)
                    })??;
                let Some(duration) = duration_opt else {
                    return Err(UringError::InvalidInput
                        .report("driver.submit_txn.timer_duration", "Timer duration missing"));
                };

                let task_id = self.env.insert_timer(self.token, duration)?;
                self.timer_inserted = Some(task_id);
                self.env.record_timer_insert(self.token, task_id);

                let slot = self
                    .slot_guard
                    .as_mut()
                    .and_then(|guard| guard.slot.as_mut())
                    .ok_or_else(|| {
                        UringError::InvalidState.report(
                            "driver.submit_txn.timer",
                            "submission guard missing after timer dispatch",
                        )
                    })?;
                let platform = slot.platform_mut();
                platform.timer_id = Some(task_id);
                self.env.transition_submission_state(
                    self.token,
                    &mut platform.control.submission,
                    SubmissionPhase::TimerArmed,
                    "submit transaction armed software timer",
                );

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
            self.env.cancel_timer(self.token, task_id);
        }

        // Fixed-buffer registration is a persistent registry resource. It is deliberately not
        // rolled back here: Drop cannot report a cleanup syscall failure, and clearing only the
        // user-space bitset would diverge from the kernel table.
    }
}
