use crate::slot;
use std::{sync::Arc, task::Waker};
use veloq_shim::atomic::Ordering;

use super::{
    AnomalyAttach, AnomalyOutcome, CompletionAnomalyKind, CompletionBackend,
    CompletionCleanupGuard, CompletionInput, CompletionPacket, CompletionRaw, CompletionRecord,
    CompletionWritePermit, DriverCompletionDiagnostics, DriverCompletionDiagnosticsBackend,
    OpToken, RecordCompletionOutcome, RecordCompletionResult, UserCompletionEvent,
    run_completion_cleanup, types::CompletionMutationOutcome,
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

    fn record_lost_completion(
        &self,
        permit: CompletionWritePermit,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        cleanup: CompletionCleanupGuard,
    ) -> RecordCompletionResult<Spec> {
        self.record_completion(permit, CompletionPacket::lost(event, kind, cleanup))
    }

    fn try_take_record(&self, token: OpToken) -> PollRecordResult<Spec>;

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

#[inline]
fn recorded_completion<Spec: slot::SlotSpec>(
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    outcome: RecordCompletionOutcome,
) -> RecordCompletionResult<Spec>
where
    Spec::CompletionDiagnostics: DriverCompletionDiagnosticsBackend,
{
    diagnostics.record_completion_outcome(&outcome);
    RecordCompletionResult::Recorded(outcome)
}

#[inline]
fn rejected_completion<Spec: slot::SlotSpec>(
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    outcome: RecordCompletionOutcome,
    event: UserCompletionEvent,
    packet: CompletionPacket<Spec>,
) -> RecordCompletionResult<Spec>
where
    Spec::CompletionDiagnostics: DriverCompletionDiagnosticsBackend,
{
    if let RecordCompletionOutcome::Rejected(anomaly_outcome) = outcome {
        diagnostics.record_anomaly_outcome(
            anomaly_outcome,
            AnomalyAttach::from_raw_completion(event.raw()),
        );
    }
    diagnostics.record_completion_outcome(&outcome);
    RecordCompletionResult::Rejected {
        outcome,
        packet: Box::new(packet),
    }
}

#[inline]
fn recorded_outcome<Spec: slot::SlotSpec>(
    input: &CompletionInput<Spec>,
) -> RecordCompletionOutcome {
    match input {
        CompletionInput::User(_) => RecordCompletionOutcome::RecordedUser,
        CompletionInput::Lost(_) => RecordCompletionOutcome::RecordedLost,
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

#[inline]
fn recorded_mutation<B>(
    diagnostics: &DriverCompletionDiagnostics<B>,
    token: OpToken,
    outcome: CompletionMutationOutcome,
) -> CompletionMutationOutcome
where
    B: DriverCompletionDiagnosticsBackend,
{
    if let Some(anomaly_outcome) = outcome.anomaly_outcome() {
        diagnostics.record_anomaly_outcome(anomaly_outcome, AnomalyAttach::from_op_token(token));
    }
    outcome
}

#[inline]
fn run_discarded_record_cleanup<Spec: slot::SlotSpec>(
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    record_data: slot::CompletionData<Spec>,
) where
    Spec::CompletionDiagnostics: DriverCompletionDiagnosticsBackend,
{
    match record_data {
        slot::CompletionData::User {
            event: _,
            payload,
            detail,
            mut cleanup,
        } => {
            drop(payload);
            drop(detail);
            let _ = run_completion_cleanup(diagnostics, &mut cleanup);
        }
        slot::CompletionData::Lost {
            kind: _,
            attach: _,
            mut cleanup,
        } => {
            let _ = run_completion_cleanup(diagnostics, &mut cleanup);
        }
        slot::CompletionData::Empty => {}
    }
}

impl<Spec> CompletionAccess<Spec> for slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
{
    #[inline]
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
            return rejected_completion(
                &self.diagnostics,
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
                return rejected_completion(
                    &self.diagnostics,
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
                return rejected_completion(&self.diagnostics, outcome, event, packet);
            }

            match state {
                slot::SlotState::Reserved if matches!(packet.input, CompletionInput::Lost(_)) => {
                    match cell.core_state.compare_exchange(
                        current,
                        current
                            .with_state(slot::SlotState::Finalizing)
                            .with_generation(generation),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break current.with_state(slot::SlotState::Finalizing),
                        Err(_) => continue,
                    }
                }
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
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                            CompletionAnomalyKind::non_active(idx, generation, state),
                        )),
                        event,
                        packet,
                    );
                }
                slot::SlotState::InFlightOrphaned => {
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::OrphanedDropped,
                        event,
                        packet,
                    );
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        if let Some((kind, attach)) = packet.input.lost_kind() {
            self.diagnostics.record_anomaly_kind(kind, attach);
        }
        let input = packet.input;
        cell.completion_with_record_data(|record| {
            *record = match input {
                CompletionInput::User(completion) => slot::CompletionData::User {
                    event,
                    payload: completion.payload,
                    detail: completion.detail,
                    cleanup: completion.cleanup,
                },
                CompletionInput::Lost(loss) => slot::CompletionData::Lost {
                    kind: loss.kind,
                    attach: loss.attach,
                    cleanup: loss.cleanup,
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
        recorded_completion(&self.diagnostics, success_outcome)
    }

    #[inline]
    fn try_take_record(&self, token: OpToken) -> PollRecordResult<Spec> {
        let attach = AnomalyAttach::from_op_token(token);
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            let kind = CompletionAnomalyKind::unknown_slot(idx, generation);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return PollRecordResult::Unavailable { kind, attach };
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        if cell_gen > generation {
            let kind = CompletionAnomalyKind::stale(idx, generation, cell_gen, state);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return PollRecordResult::Unavailable { kind, attach };
        }

        if cell_gen < generation {
            let kind = CompletionAnomalyKind::non_active(idx, generation, state);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return PollRecordResult::Unavailable { kind, attach };
        }

        if state != slot::SlotState::InFlightReady {
            return match state {
                slot::SlotState::InFlightWaiting | slot::SlotState::Finalizing => {
                    PollRecordResult::Pending
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::ReservedValue => {
                    let kind = CompletionAnomalyKind::non_active(idx, generation, state);
                    self.diagnostics.record_anomaly_kind(kind, attach);
                    PollRecordResult::Unavailable { kind, attach }
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
            return PollRecordResult::Pending;
        }

        self.clear_ready_completion();
        let record_data = cell.completion_with_record_data(std::mem::take);

        match record_data {
            slot::CompletionData::User {
                event,
                payload,
                detail,
                cleanup,
            } => PollRecordResult::Ready(CompletionRecord {
                event,
                payload,
                detail,
                cleanup,
            }),
            slot::CompletionData::Lost {
                kind,
                attach,
                mut cleanup,
            } => {
                let _ = run_completion_cleanup(&self.diagnostics, &mut cleanup);
                PollRecordResult::Unavailable { kind, attach }
            }
            slot::CompletionData::Empty => {
                let kind = CompletionAnomalyKind::corrupt_snapshot(slot::SlotSnapshot {
                    index: idx,
                    generation,
                    state: slot::SlotState::Idle,
                    has_op: true,
                    has_payload: false,
                });
                let attach = AnomalyAttach {
                    token: attach.token,
                    raw: Some(CompletionRaw {
                        backend: CompletionBackend::Core,
                        res: cell.completion_res.load(Ordering::Acquire),
                        flags: cell.completion_flags.load(Ordering::Acquire),
                    }),
                };
                self.diagnostics.record_anomaly_kind(kind, attach);
                self.diagnostics.inc_user_lost();
                PollRecordResult::Unavailable { kind, attach }
            }
        }
    }

    #[inline]
    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        if cell_gen != generation {
            return recorded_mutation(
                &self.diagnostics,
                token,
                mutation_generation_mismatch(idx, generation, cell_gen, state),
            );
        }

        cell.completion_waker.register(waker);

        let current_after = cell.load_core_state(Ordering::Acquire);
        let state_after = current_after.state();
        let generation_after = current_after.generation();
        if generation_after != generation {
            return recorded_mutation(
                &self.diagnostics,
                token,
                mutation_generation_mismatch(idx, generation, generation_after, state_after),
            );
        }
        if current_after.state() == slot::SlotState::InFlightReady && generation_after == generation
        {
            waker.wake_by_ref();
            return recorded_mutation(&self.diagnostics, token, CompletionMutationOutcome::Applied);
        }

        let outcome = match state_after {
            slot::SlotState::InFlightWaiting
            | slot::SlotState::InFlightReady
            | slot::SlotState::Finalizing => CompletionMutationOutcome::Applied,
            state => mutation_non_active(idx, generation, state),
        };
        recorded_mutation(&self.diagnostics, token, outcome)
    }

    #[inline]
    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_generation = current.generation();

            if cell_generation != generation {
                return recorded_mutation(
                    &self.diagnostics,
                    token,
                    mutation_generation_mismatch(idx, generation, cell_generation, state),
                );
            }

            if state == slot::SlotState::InFlightReady {
                return recorded_mutation(
                    &self.diagnostics,
                    token,
                    CompletionMutationOutcome::Applied,
                );
            }

            match state {
                slot::SlotState::InFlightWaiting => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        CompletionMutationOutcome::Applied,
                    );
                }
                slot::SlotState::Finalizing => continue,
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::InFlightReady
                | slot::SlotState::ReservedValue => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        mutation_non_active(idx, generation, state),
                    );
                }
            }
        }
    }

    #[inline]
    fn discard_ready_record(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, token, mutation_missing(token));
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
                        run_discarded_record_cleanup::<Spec>(&self.diagnostics, record_data);
                        return recorded_mutation(
                            &self.diagnostics,
                            token,
                            CompletionMutationOutcome::Applied,
                        );
                    }
                }
                slot::SlotState::Finalizing => continue,
                _ if cell_gen != generation => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        mutation_generation_mismatch(idx, generation, cell_gen, state),
                    );
                }
                _ => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        mutation_non_active(idx, generation, state),
                    );
                }
            }
        }
    }

    #[inline]
    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            match state {
                slot::SlotState::InFlightWaiting => {
                    if cell_gen != generation {
                        return recorded_mutation(
                            &self.diagnostics,
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
                        return recorded_mutation(
                            &self.diagnostics,
                            token,
                            CompletionMutationOutcome::Applied,
                        );
                    }
                }
                slot::SlotState::InFlightReady if cell_gen == generation => {
                    return self.discard_ready_record(token);
                }
                slot::SlotState::Finalizing => continue,
                _ if cell_gen != generation => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        mutation_generation_mismatch(idx, generation, cell_gen, state),
                    );
                }
                _ => {
                    return recorded_mutation(
                        &self.diagnostics,
                        token,
                        mutation_non_active(idx, generation, state),
                    );
                }
            }
        }
    }

    #[inline]
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
