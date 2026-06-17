use crate::DriverError;
use crate::slot;
use diagweave::prelude::*;
use std::{sync::Arc, task::Waker};
use veloq_shim::atomic::Ordering;

use super::{
    AnomalyAttach, AnomalyOutcome, CompletionAnomalyKind, CompletionInput, CompletionPacket,
    CompletionRecord, CompletionWritePermit, DriverCompletionDiagnosticsBackend, OpToken,
    RecordCompletionOutcome, RecordCompletionResult, UserCompletionEvent, run_completion_cleanup,
    types::CompletionMutationOutcome,
};

pub type SharedCompletionTable<Spec> = Arc<dyn CompletionAccess<Spec>>;

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<Spec: slot::SlotSpec> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<Spec>),
    /// Operation completion became unavailable; materialize at the poll boundary.
    Unavailable {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    },
    /// Operation is still in flight.
    Pending,
}

pub trait CompletionAccess<Spec: slot::SlotSpec>: Send + Sync {
    fn record_completion(
        &self,
        permit: CompletionWritePermit,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec>;

    fn try_take_record(
        &self,
        token: OpToken,
    ) -> Result<PollRecordResult<Spec>, Report<Spec::Error>>;

    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome;

    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome;

    fn discard_ready_record(&self, token: OpToken) -> CompletionMutationOutcome;

    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome;

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;
}

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

#[inline(always)]
fn spin_yield() {
    #[cfg(feature = "loom")]
    veloq_shim::thread::yield_now();
    #[cfg(not(feature = "loom"))]
    std::hint::spin_loop();
}

#[inline]
fn recorded_outcome<Spec: slot::SlotSpec>(
    input: &CompletionInput<Spec>,
) -> RecordCompletionOutcome {
    match input {
        CompletionInput::User(_) => RecordCompletionOutcome::RecordedUser,
    }
}

#[inline]
fn mutation_missing(token: OpToken) -> CompletionMutationOutcome {
    let (idx, generation) = token.parts();
    CompletionMutationOutcome::Rejected(AnomalyOutcome::Missing(
        CompletionAnomalyKind::unknown_slot(idx, generation),
    ))
}

#[inline]
fn mutation_generation_mismatch(
    idx: usize,
    expected_generation: u32,
    actual_generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    if actual_generation > expected_generation {
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Stale(CompletionAnomalyKind::stale(
            idx,
            expected_generation,
            actual_generation,
            state,
        )))
    } else {
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(
            CompletionAnomalyKind::non_active(idx, expected_generation, state),
        ))
    }
}

#[inline]
fn mutation_non_active(
    idx: usize,
    generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(
        CompletionAnomalyKind::non_active(idx, generation, state),
    ))
}

impl<Spec> slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    Spec::CompletionDiagnostics: DriverCompletionDiagnosticsBackend,
{
    #[inline]
    fn recorded_completion(
        &self,
        outcome: RecordCompletionOutcome,
    ) -> RecordCompletionResult<Spec> {
        self.diagnostics.record_completion_outcome(&outcome);
        RecordCompletionResult::Recorded(outcome)
    }

    #[inline]
    fn rejected_completion(
        &self,
        outcome: RecordCompletionOutcome,
        event: UserCompletionEvent,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec> {
        if let RecordCompletionOutcome::Rejected(anomaly_outcome) = outcome {
            self.diagnostics.record_anomaly_outcome(
                anomaly_outcome,
                AnomalyAttach::from_raw_completion(event.raw()),
            );
        }
        self.diagnostics.record_completion_outcome(&outcome);
        RecordCompletionResult::Rejected {
            outcome,
            packet: Box::new(packet),
        }
    }

    #[inline]
    fn recorded_mutation(
        &self,
        token: OpToken,
        outcome: CompletionMutationOutcome,
    ) -> CompletionMutationOutcome {
        if let Some(anomaly_outcome) = outcome.anomaly_outcome() {
            self.diagnostics
                .record_anomaly_outcome(anomaly_outcome, AnomalyAttach::from_op_token(token));
        }
        outcome
    }

    #[inline]
    fn run_discarded_record_cleanup(&self, record_data: slot::CompletionData<Spec>) {
        match record_data {
            slot::CompletionData::User {
                event: _,
                payload,
                detail,
                mut cleanup,
            } => {
                drop(payload);
                drop(detail);
                let _ = run_completion_cleanup(&self.diagnostics, &mut cleanup);
            }
            slot::CompletionData::Empty => {}
        }
    }
}

impl<Spec> CompletionAccess<Spec> for slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
{
    fn record_completion(
        &self,
        _permit: CompletionWritePermit,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec> {
        let op_token = packet.token();
        let event = packet.event;
        let (idx, generation) = op_token.parts();
        let success_outcome = recorded_outcome(&packet.input);
        if idx >= self.slots.len() {
            return self.rejected_completion(
                RecordCompletionOutcome::Rejected(AnomalyOutcome::Missing(
                    CompletionAnomalyKind::unknown_slot(idx, generation),
                )),
                event,
                packet,
            );
        }
        let cell = &self.slots[idx];

        let finalizing = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            if generation < cell_gen {
                return self.rejected_completion(
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::Stale(
                        CompletionAnomalyKind::stale(idx, generation, cell_gen, state),
                    )),
                    event,
                    packet,
                );
            }
            if generation > cell_gen {
                let outcome = if state == slot::SlotState::Idle {
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                        CompletionAnomalyKind::non_active(idx, generation, state),
                    ))
                } else {
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::Stale(
                        CompletionAnomalyKind::stale(idx, generation, cell_gen, state),
                    ))
                };
                return self.rejected_completion(outcome, event, packet);
            }

            match state {
                slot::SlotState::InFlightWaiting => match cell.core_state.compare_exchange(
                    current,
                    current
                        .with_state(slot::SlotState::Finalizing)
                        .with_generation(generation),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break current.with_state(slot::SlotState::Finalizing),
                    Err(_) => continue,
                },
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightReady
                | slot::SlotState::ReservedValue => {
                    return self.rejected_completion(
                        RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                            CompletionAnomalyKind::non_active(idx, generation, state),
                        )),
                        event,
                        packet,
                    );
                }
                slot::SlotState::InFlightOrphaned => {
                    return self.rejected_completion(
                        RecordCompletionOutcome::OrphanedDropped,
                        event,
                        packet,
                    );
                }
                slot::SlotState::Finalizing => {
                    spin_yield();
                    continue;
                }
            }
        };

        let input = packet.input;
        cell.completion_with_record_data(|record| {
            *record = match input {
                CompletionInput::User(completion) => slot::CompletionData::User {
                    event,
                    payload: completion.payload,
                    detail: completion.detail,
                    cleanup: completion.cleanup,
                },
            };
        });
        cell.completion_res.store(event.res(), Ordering::Release);
        cell.completion_flags
            .store(event.flags(), Ordering::Release);
        self.note_ready_completion();
        cell.core_state.store(
            finalizing
                .with_state(slot::SlotState::InFlightReady)
                .with_generation(generation),
            Ordering::Release,
        );

        cell.completion_waker.wake();
        self.recorded_completion(success_outcome)
    }

    fn try_take_record(
        &self,
        token: OpToken,
    ) -> Result<PollRecordResult<Spec>, Report<Spec::Error>> {
        let attach = AnomalyAttach::from_op_token(token);
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            let kind = CompletionAnomalyKind::unknown_slot(idx, generation);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return Ok(PollRecordResult::Unavailable { kind, attach });
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        if cell_gen > generation {
            let kind = CompletionAnomalyKind::stale(idx, generation, cell_gen, state);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return Ok(PollRecordResult::Unavailable { kind, attach });
        }

        if cell_gen < generation {
            let kind = CompletionAnomalyKind::non_active(idx, generation, state);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return Ok(PollRecordResult::Unavailable { kind, attach });
        }

        if state != slot::SlotState::InFlightReady {
            return match state {
                slot::SlotState::InFlightWaiting | slot::SlotState::Finalizing => {
                    Ok(PollRecordResult::Pending)
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::ReservedValue => {
                    let kind = CompletionAnomalyKind::non_active(idx, generation, state);
                    self.diagnostics.record_anomaly_kind(kind, attach);
                    Ok(PollRecordResult::Unavailable { kind, attach })
                }
                slot::SlotState::InFlightReady => unreachable!(),
            };
        }

        if cell
            .core_state
            .compare_exchange(
                current,
                current
                    .with_state(slot::SlotState::Idle)
                    .with_generation(generation.wrapping_add(1)),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(PollRecordResult::Pending);
        }

        self.clear_ready_completion();
        let record_data = cell.completion_with_record_data(std::mem::take);

        match record_data {
            slot::CompletionData::User {
                event,
                payload,
                detail,
                cleanup,
            } => Ok(PollRecordResult::Ready(CompletionRecord {
                event,
                payload,
                detail,
                cleanup,
            })),
            slot::CompletionData::Empty => {
                let report = crate::DriverCoreError::Internal
                    .to_report()
                    .push_ctx("scope", "try_take_record")
                    .attach_note(format!(
                        "corrupt slot state: InFlightReady slot completion data is empty. index: {}, generation: {}",
                        idx, generation
                    ));
                Err(Spec::Error::from_core_report(report))
            }
        }
    }

    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        if cell_gen != generation {
            return self.recorded_mutation(
                token,
                mutation_generation_mismatch(idx, generation, cell_gen, state),
            );
        }

        cell.completion_waker.register(waker);

        let current_after = cell.load_core_state(Ordering::Acquire);
        let state_after = current_after.state();
        let generation_after = current_after.generation();
        if generation_after != generation {
            return self.recorded_mutation(
                token,
                mutation_generation_mismatch(idx, generation, generation_after, state_after),
            );
        }
        if current_after.state() == slot::SlotState::InFlightReady && generation_after == generation
        {
            waker.wake_by_ref();
            return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
        }

        let outcome = match state_after {
            slot::SlotState::InFlightWaiting
            | slot::SlotState::InFlightReady
            | slot::SlotState::Finalizing => CompletionMutationOutcome::Applied,
            state => mutation_non_active(idx, generation, state),
        };
        self.recorded_mutation(token, outcome)
    }

    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_generation = current.generation();

            if cell_generation != generation {
                return self.recorded_mutation(
                    token,
                    mutation_generation_mismatch(idx, generation, cell_generation, state),
                );
            }

            if state == slot::SlotState::InFlightReady {
                return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
            }

            match state {
                slot::SlotState::InFlightWaiting => {
                    return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
                }
                slot::SlotState::Finalizing => {
                    spin_yield();
                    continue;
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::InFlightReady
                | slot::SlotState::ReservedValue => {
                    return self
                        .recorded_mutation(token, mutation_non_active(idx, generation, state));
                }
            }
        }
    }

    fn discard_ready_record(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            match state {
                slot::SlotState::InFlightReady if cell_gen == generation => {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::Idle)
                                .with_generation(generation.wrapping_add(1)),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.clear_ready_completion();
                        let record_data = cell.completion_with_record_data(std::mem::take);
                        self.run_discarded_record_cleanup(record_data);
                        return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
                    }
                }
                slot::SlotState::Finalizing => {
                    spin_yield();
                    continue;
                }
                _ if cell_gen != generation => {
                    return self.recorded_mutation(
                        token,
                        mutation_generation_mismatch(idx, generation, cell_gen, state),
                    );
                }
                _ => {
                    return self
                        .recorded_mutation(token, mutation_non_active(idx, generation, state));
                }
            }
        }
    }

    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            match state {
                slot::SlotState::InFlightWaiting => {
                    if cell_gen != generation {
                        return self.recorded_mutation(
                            token,
                            mutation_generation_mismatch(idx, generation, cell_gen, state),
                        );
                    }
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightOrphaned)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
                    }
                }
                slot::SlotState::InFlightReady if cell_gen == generation => {
                    return self.discard_ready_record(token);
                }
                slot::SlotState::Finalizing => {
                    spin_yield();
                    continue;
                }
                _ if cell_gen != generation => {
                    return self.recorded_mutation(
                        token,
                        mutation_generation_mismatch(idx, generation, cell_gen, state),
                    );
                }
                _ => {
                    return self
                        .recorded_mutation(token, mutation_non_active(idx, generation, state));
                }
            }
        }
    }

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8 {
        let current = self.slots[idx].load_core_state(Ordering::Acquire);
        match current.state() {
            slot::SlotState::Idle => CELL_STATE_IDLE,
            slot::SlotState::InFlightWaiting => CELL_STATE_WAITING,
            slot::SlotState::InFlightReady => CELL_STATE_READY,
            slot::SlotState::InFlightOrphaned => CELL_STATE_ORPHANED,
            slot::SlotState::Finalizing => CELL_STATE_BUSY,
            slot::SlotState::Reserved => CELL_STATE_IDLE,
            slot::SlotState::ReservedValue => CELL_STATE_IDLE,
        }
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests;
