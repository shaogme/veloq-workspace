use crate::slot;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

use super::types::CompletionMutationOutcome;
use super::{
    CompletionAnomaly, CompletionCleanupGuard, CompletionEvent, CompletionInput, CompletionPacket,
    CompletionRecord, CompletionToken, OpToken, RecordCompletionOutcome, RecordCompletionResult,
};

pub type SharedCompletionTable<UP, E, R = usize> = Arc<dyn CompletionAccess<UP, E, R>>;

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<UP, E, R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<UP, E, R>),
    /// Operation completion became unavailable and the waiter can finish as resource-lost.
    Unavailable(CompletionAnomaly),
    /// Operation is still in flight.
    Pending,
}

pub trait CompletionAccess<UP, E, R = usize>: Send + Sync {
    fn record_completion(
        &self,
        packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R>;

    fn record_lost_completion(
        &self,
        token: OpToken,
        event: CompletionEvent,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> RecordCompletionResult<UP, E, R> {
        self.record_completion(CompletionPacket::lost(token, event, anomaly, cleanup))
    }

    fn try_take_record(&self, token: OpToken) -> PollRecordResult<UP, E, R>;

    #[inline]
    fn try_take(&self, token: OpToken) -> PollRecordResult<UP, E, R> {
        self.try_take_record(token)
    }

    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome;

    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome;

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
fn recorded_completion<UP, E, R>(
    diagnostics: &super::DriverCompletionDiagnostics,
    outcome: RecordCompletionOutcome,
) -> RecordCompletionResult<UP, E, R> {
    diagnostics.record_completion_outcome(&outcome);
    RecordCompletionResult::Recorded(outcome)
}

#[inline]
fn rejected_completion<UP, E, R>(
    diagnostics: &super::DriverCompletionDiagnostics,
    outcome: RecordCompletionOutcome,
    packet: CompletionPacket<UP, E, R>,
) -> RecordCompletionResult<UP, E, R> {
    diagnostics.record_completion_outcome(&outcome);
    RecordCompletionResult::Rejected {
        outcome,
        packet: Box::new(packet),
    }
}

#[inline]
fn recorded_outcome<UP, E, R>(input: &CompletionInput<UP, E, R>) -> RecordCompletionOutcome {
    match input {
        CompletionInput::User(_) => RecordCompletionOutcome::RecordedUser,
        CompletionInput::Lost(_) => RecordCompletionOutcome::RecordedLost,
    }
}

#[inline]
fn mutation_missing(token: OpToken) -> CompletionMutationOutcome {
    let (idx, generation) = token.parts();
    CompletionMutationOutcome::Missing(CompletionAnomaly::unknown_slot(
        CompletionToken::user(token),
        idx,
        generation,
    ))
}

#[inline]
fn mutation_generation_mismatch(
    token: OpToken,
    idx: usize,
    expected_generation: u32,
    actual_generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    let completion_token = CompletionToken::user(token);
    if actual_generation > expected_generation {
        CompletionMutationOutcome::Stale(CompletionAnomaly::stale(
            completion_token,
            idx,
            expected_generation,
            actual_generation,
            state,
        ))
    } else {
        CompletionMutationOutcome::NonActive(CompletionAnomaly::non_active(
            completion_token,
            idx,
            expected_generation,
            state,
        ))
    }
}

#[inline]
fn mutation_non_active(
    token: OpToken,
    idx: usize,
    generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    CompletionMutationOutcome::NonActive(CompletionAnomaly::non_active(
        CompletionToken::user(token),
        idx,
        generation,
        state,
    ))
}

#[inline]
fn recorded_mutation(
    diagnostics: &super::DriverCompletionDiagnostics,
    outcome: CompletionMutationOutcome,
) -> CompletionMutationOutcome {
    if let Some(anomaly) = outcome.anomaly() {
        diagnostics.record_anomaly(anomaly);
    }
    outcome
}

impl<Spec, UP: Send, E: Send, R: Send> CompletionAccess<UP, E, R> for slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec<UserPayload = UP, Error = E, Completion = R>,
{
    #[inline]
    fn record_completion(
        &self,
        packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R> {
        let op_token = packet.token;
        let token = packet.event.token;
        let (idx, generation) = op_token.parts();
        let success_outcome = recorded_outcome(&packet.input);
        if idx >= self.slots.len() {
            return rejected_completion(
                &self.diagnostics,
                RecordCompletionOutcome::Missing(CompletionAnomaly::unknown_slot(
                    token, idx, generation,
                )),
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
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    )),
                    packet,
                );
            }
            if generation > cell_gen {
                let outcome = if state == slot::SlotState::Idle {
                    RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                        token, idx, generation, state,
                    ))
                } else {
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    ))
                };
                return rejected_completion(&self.diagnostics, outcome, packet);
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
                slot::SlotState::InFlightWaiting => {
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
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightReady
                | slot::SlotState::ReservedValue => {
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                            token, idx, generation, state,
                        )),
                        packet,
                    );
                }
                slot::SlotState::InFlightOrphaned => {
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::OrphanedDropped,
                        packet,
                    );
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        if let Some(anomaly) = packet.input.anomaly() {
            self.diagnostics.record_anomaly(anomaly);
        }
        let event = packet.event;
        let input = packet.input;
        cell.completion_with_record_data(|record| {
            *record = match input {
                CompletionInput::User(completion) => slot::CompletionData::User {
                    payload: completion.payload,
                    detail: completion.detail,
                    cleanup: completion.cleanup,
                },
                CompletionInput::Lost(loss) => slot::CompletionData::Lost {
                    anomaly: loss.anomaly,
                    cleanup: loss.cleanup,
                },
            };
        });
        cell.completion_res.store(event.res, Ordering::Release);
        cell.completion_flags.store(event.flags, Ordering::Release);
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
    fn try_take_record(&self, token: OpToken) -> PollRecordResult<UP, E, R> {
        let completion_token = CompletionToken::user(token);
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            let anomaly = CompletionAnomaly::unknown_slot(completion_token, idx, generation);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Unavailable(anomaly);
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        // If the cell's generation is strictly greater than ours, we are stale.
        if cell_gen > generation {
            let anomaly =
                CompletionAnomaly::stale(completion_token, idx, generation, cell_gen, state);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Unavailable(anomaly);
        }

        if cell_gen != generation {
            return PollRecordResult::Pending;
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
                    let anomaly =
                        CompletionAnomaly::non_active(completion_token, idx, generation, state);
                    self.diagnostics.record_anomaly(&anomaly);
                    PollRecordResult::Unavailable(anomaly)
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
        let event = CompletionEvent {
            token: completion_token,
            res: cell.completion_res.load(Ordering::Acquire),
            flags: cell.completion_flags.load(Ordering::Acquire),
        };

        match record_data {
            slot::CompletionData::User {
                payload,
                detail,
                cleanup,
            } => PollRecordResult::Ready(CompletionRecord {
                event,
                payload,
                detail,
                cleanup,
            }),
            slot::CompletionData::Lost { anomaly, cleanup } => {
                drop(cleanup);
                PollRecordResult::Unavailable(anomaly)
            }
            slot::CompletionData::Empty => {
                let anomaly = CompletionAnomaly::payload_missing(completion_token, idx, generation)
                    .with_event(event);
                self.diagnostics.record_anomaly(&anomaly);
                self.diagnostics.inc_user_lost();
                PollRecordResult::Unavailable(anomaly)
            }
        }
    }

    #[inline]
    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        if cell_gen != generation {
            return recorded_mutation(
                &self.diagnostics,
                mutation_generation_mismatch(token, idx, generation, cell_gen, state),
            );
        }

        // Register waker. AtomicWaker handles races with concurrent wake().
        cell.completion_waker.register(waker);

        // cell_gen == generation
        let current_after = cell.load_core_state(Ordering::Acquire);
        let state_after = current_after.state();
        let generation_after = current_after.generation();
        if generation_after != generation {
            return recorded_mutation(
                &self.diagnostics,
                mutation_generation_mismatch(token, idx, generation, generation_after, state_after),
            );
        }
        if current_after.state() == slot::SlotState::InFlightReady && generation_after == generation
        {
            waker.wake_by_ref();
            return recorded_mutation(&self.diagnostics, CompletionMutationOutcome::Applied);
        }

        let outcome = match state_after {
            slot::SlotState::InFlightWaiting
            | slot::SlotState::InFlightReady
            | slot::SlotState::Finalizing => CompletionMutationOutcome::Applied,
            state => mutation_non_active(token, idx, generation, state),
        };
        recorded_mutation(&self.diagnostics, outcome)
    }

    #[inline]
    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_generation = current.generation();

            if cell_generation != generation {
                return recorded_mutation(
                    &self.diagnostics,
                    mutation_generation_mismatch(token, idx, generation, cell_generation, state),
                );
            }

            // cell_generation == generation
            if state == slot::SlotState::InFlightReady {
                // Fast completion happened, leave as READY.
                return recorded_mutation(&self.diagnostics, CompletionMutationOutcome::Applied);
            }

            match state {
                slot::SlotState::InFlightWaiting => {
                    return recorded_mutation(
                        &self.diagnostics,
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
                        mutation_non_active(token, idx, generation, state),
                    );
                }
            }
        }
    }

    #[inline]
    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return recorded_mutation(&self.diagnostics, mutation_missing(token));
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
                            mutation_generation_mismatch(token, idx, generation, cell_gen, state),
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
                            CompletionMutationOutcome::Applied,
                        );
                    }
                }
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
                        cell.clear_completion_record_data();
                        return recorded_mutation(
                            &self.diagnostics,
                            CompletionMutationOutcome::Applied,
                        );
                    }
                }
                slot::SlotState::Finalizing => continue,
                _ if cell_gen != generation => {
                    return recorded_mutation(
                        &self.diagnostics,
                        mutation_generation_mismatch(token, idx, generation, cell_gen, state),
                    );
                }
                _ => {
                    return recorded_mutation(
                        &self.diagnostics,
                        mutation_non_active(token, idx, generation, state),
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
mod tests {
    use super::*;
    use crate::driver::CompletionAnomalyReason;
    use crate::driver::OpToken;
    use crate::driver::PlatformOp;

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {}

    struct DummySlotSpec;

    impl slot::SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = ();
        type Completion = usize;
    }

    fn test_token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, generation).expect("test token should be encodable")
    }

    fn test_event(token: OpToken, res: i32) -> CompletionEvent {
        CompletionEvent {
            token: CompletionToken::user(token),
            res,
            flags: 0,
        }
    }

    #[test]
    fn record_completion_rejects_idle_future_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = test_token(0, 1);

        let outcome = table
            .record_completion(CompletionPacket::user(token, 0, 0, (), None))
            .into_outcome();

        assert!(matches!(outcome, RecordCompletionOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    }

    #[test]
    fn mark_waiting_does_not_activate_idle_future_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = test_token(0, 1);

        let outcome = table.mark_waiting(token);

        assert!(matches!(outcome, CompletionMutationOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    }

    #[test]
    fn mark_waiting_does_not_revive_orphaned_slot() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::InFlightOrphaned, Ordering::Release);
        let token = test_token(0, 1);

        let outcome = table.mark_waiting(token);

        assert!(matches!(outcome, CompletionMutationOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_ORPHANED);
    }

    #[test]
    fn mark_orphaned_reports_stale_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(2);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = test_token(0, 1);

        let outcome = table.mark_orphaned(token);

        assert!(matches!(outcome, CompletionMutationOutcome::Stale(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_WAITING);
        assert_eq!(
            table.completion_diagnostics().snapshot().stale_completion,
            1
        );
    }

    #[test]
    fn register_waker_reports_missing_slot() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let waker = std::task::Waker::noop();
        let token = test_token(3, 1);

        let outcome = table.register_waker(token, waker);

        assert!(matches!(outcome, CompletionMutationOutcome::Missing(_)));
    }

    #[test]
    fn lost_completion_is_observable_as_unavailable() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::Reserved, Ordering::Release);
        let token = test_token(0, 1);
        let completion_token = CompletionToken::user(token);
        let anomaly = CompletionAnomaly::corrupt(completion_token, 0, 1, slot::SlotState::Reserved);

        let outcome = table
            .record_lost_completion(
                token,
                test_event(token, -5),
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::Unavailable(observed) if observed.reason == anomaly.reason
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.user_lost, 1);
        assert_eq!(snapshot.user_completed, 0);
    }

    #[test]
    fn lost_completion_reports_stale_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(2);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = test_token(0, 1);
        let anomaly = CompletionAnomaly::stale(
            CompletionToken::user(token),
            0,
            1,
            2,
            slot::SlotState::InFlightWaiting,
        );

        let outcome = table
            .record_lost_completion(
                token,
                test_event(token, -1),
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert!(matches!(outcome, RecordCompletionOutcome::Stale(_)));
    }

    #[test]
    fn lost_completion_reports_empty_slot() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = test_token(0, 0);
        let anomaly = CompletionAnomaly::non_active(
            CompletionToken::user(token),
            0,
            0,
            slot::SlotState::Idle,
        );

        let outcome = table
            .record_lost_completion(
                token,
                test_event(token, -1),
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert!(matches!(outcome, RecordCompletionOutcome::NonActive(_)));
    }

    #[test]
    fn lost_completion_preserves_payload_missing_reason() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::Reserved, Ordering::Release);
        let token = test_token(0, 1);
        let anomaly = CompletionAnomaly::payload_missing(CompletionToken::user(token), 0, 1);

        let outcome = table
            .record_lost_completion(
                token,
                test_event(token, -1),
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::Unavailable(observed) if observed.reason == anomaly.reason
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.user_lost, 1);
        assert_eq!(snapshot.payload_missing, 1);
    }

    #[test]
    fn lost_completion_preserves_op_missing_reason() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::Reserved, Ordering::Release);
        let token = test_token(0, 1);
        let anomaly = CompletionAnomaly::op_missing(CompletionToken::user(token), 0, 1);

        let outcome = table
            .record_lost_completion(
                token,
                test_event(token, -1),
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::Unavailable(observed) if observed.reason == anomaly.reason
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.user_lost, 1);
        assert_eq!(snapshot.slot_corruption, 1);
    }

    #[test]
    fn duplicate_completion_does_not_clear_ready_data() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = test_token(0, 1);

        let first = table
            .record_completion(CompletionPacket::user(token, 11, 0, (), None))
            .into_outcome();
        let duplicate = table
            .record_completion(CompletionPacket::user(token, 22, 0, (), None))
            .into_outcome();

        assert_eq!(first, RecordCompletionOutcome::RecordedUser);
        assert!(matches!(duplicate, RecordCompletionOutcome::NonActive(_)));
        let record = match table.try_take_record(token) {
            PollRecordResult::Ready(record) => record,
            PollRecordResult::Pending => panic!("first completion should be ready"),
            PollRecordResult::Unavailable(anomaly) => {
                panic!("first completion should remain available: {anomaly:?}")
            }
        };
        assert_eq!(record.event.res, 11);
    }

    #[test]
    fn ready_mark_orphaned_cleanup_leaves_diagnostic_stale_result() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = test_token(0, 1);

        let outcome = table
            .record_completion(CompletionPacket::user(token, 0, 0, (), None))
            .into_outcome();
        assert_eq!(outcome, RecordCompletionOutcome::RecordedUser);
        assert_eq!(
            table.mark_orphaned(token),
            CompletionMutationOutcome::Applied
        );

        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::Unavailable(anomaly)
                if anomaly.reason == CompletionAnomalyReason::StaleGeneration
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.stale_completion, 1);
    }
}
