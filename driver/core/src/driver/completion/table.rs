use crate::slot;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

use super::types::CompletionMutationOutcome;
use super::{
    CompletionAnomaly, CompletionCleanupGuard, CompletionEvent, CompletionPacket, CompletionRecord,
    CompletionRecordKind, CompletionToken, RecordCompletionOutcome, RecordCompletionResult,
    decode_completion_token,
};

pub type SharedCompletionTable<UP, E, R = usize> = Arc<dyn CompletionAccess<UP, E, R>>;

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<UP, E, R = usize> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<UP, E, R>),
    /// Operation was explicitly marked lost and the waiter was woken.
    ReadyLost(CompletionAnomaly),
    /// Operation is still in flight.
    Pending,
    /// Operation lost because the slot has been recycled for a newer generation.
    Stale(CompletionAnomaly),
    /// Operation lost or could not be associated with a valid active slot.
    Lost(CompletionAnomaly),
}

pub trait CompletionAccess<UP, E, R = usize>: Send + Sync {
    fn record_completion(
        &self,
        packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R>;

    fn record_lost_completion(
        &self,
        event: CompletionEvent,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> RecordCompletionResult<UP, E, R> {
        self.record_completion(CompletionPacket::lost(event, anomaly, cleanup))
    }

    fn try_take_record(&self, token: CompletionToken) -> PollRecordResult<UP, E, R>;

    #[inline]
    fn try_take(&self, token: CompletionToken) -> PollRecordResult<UP, E, R> {
        self.try_take_record(token)
    }

    fn register_waker(&self, token: CompletionToken, waker: &Waker) -> CompletionMutationOutcome;

    fn mark_waiting(&self, token: CompletionToken) -> CompletionMutationOutcome;

    fn mark_orphaned(&self, token: CompletionToken) -> CompletionMutationOutcome;

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
fn recorded_outcome(record_kind: CompletionRecordKind) -> RecordCompletionOutcome {
    match record_kind {
        CompletionRecordKind::User => RecordCompletionOutcome::RecordedUser,
        CompletionRecordKind::Lost(_) => RecordCompletionOutcome::RecordedLost,
    }
}

#[inline]
fn mutation_missing(token: CompletionToken) -> CompletionMutationOutcome {
    match token.op_token() {
        Some(op_token) => {
            let (idx, generation) = op_token.parts();
            CompletionMutationOutcome::Missing(CompletionAnomaly::unknown_slot(
                token, idx, generation,
            ))
        }
        None => {
            CompletionMutationOutcome::UnknownControl(CompletionAnomaly::unknown_control(token))
        }
    }
}

#[inline]
fn mutation_generation_mismatch(
    token: CompletionToken,
    idx: usize,
    expected_generation: u32,
    actual_generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    if actual_generation > expected_generation {
        CompletionMutationOutcome::Stale(CompletionAnomaly::stale(
            token,
            idx,
            expected_generation,
            actual_generation,
            state,
        ))
    } else {
        CompletionMutationOutcome::NonActive(CompletionAnomaly::non_active(
            token,
            idx,
            expected_generation,
            state,
        ))
    }
}

#[inline]
fn mutation_non_active(
    token: CompletionToken,
    idx: usize,
    generation: u32,
    state: slot::SlotState,
) -> CompletionMutationOutcome {
    CompletionMutationOutcome::NonActive(CompletionAnomaly::non_active(
        token, idx, generation, state,
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
        mut packet: CompletionPacket<UP, E, R>,
    ) -> RecordCompletionResult<UP, E, R> {
        let token = packet.event.token;
        let Some(op_token) = token.op_token() else {
            return rejected_completion(
                &self.diagnostics,
                RecordCompletionOutcome::Missing(CompletionAnomaly::unknown_control(token)),
                packet,
            );
        };
        let (idx, generation) = op_token.parts();
        let lost_anomaly = if packet.payload.is_none()
            && matches!(packet.record_kind, CompletionRecordKind::User)
        {
            let anomaly =
                CompletionAnomaly::payload_missing(token, idx, generation).with_event(packet.event);
            packet.record_kind = CompletionRecordKind::Lost(anomaly);
            Some(anomaly)
        } else if let CompletionRecordKind::Lost(anomaly) = packet.record_kind {
            Some(anomaly)
        } else {
            None
        };
        if let Some(anomaly) = lost_anomaly {
            self.diagnostics.record_anomaly(&anomaly);
        }
        let success_outcome = recorded_outcome(packet.record_kind);
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
        let should_note_ready;

        let ready_from = loop {
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
                slot::SlotState::Reserved
                    if matches!(packet.record_kind, CompletionRecordKind::Lost(_)) =>
                {
                    should_note_ready = true;
                    break current;
                }
                slot::SlotState::InFlightWaiting => {
                    should_note_ready = true;
                    break current;
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
                    if cell_gen == generation {
                        return rejected_completion(
                            &self.diagnostics,
                            RecordCompletionOutcome::OrphanedDropped,
                            packet,
                        );
                    } else {
                        return rejected_completion(
                            &self.diagnostics,
                            RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                                token, idx, generation, cell_gen, state,
                            )),
                            packet,
                        );
                    }
                }
                slot::SlotState::Finalizing => continue,
            }
        };

        if should_note_ready {
            self.note_ready_completion();
        }
        cell.completion_with_record_data(|payload_cell, detail_cell, cleanup_cell, kind_cell| {
            *payload_cell = packet.payload.take();
            *detail_cell = packet.detail.take();
            *cleanup_cell = std::mem::take(&mut packet.cleanup);
            *kind_cell = packet.record_kind;
        });
        cell.completion_res
            .store(packet.event.res, Ordering::Release);
        cell.completion_flags
            .store(packet.event.flags, Ordering::Release);

        match cell.core_state.compare_exchange(
            ready_from,
            ready_from
                .with_state(slot::SlotState::InFlightReady)
                .with_generation(generation),
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(next) => {
                let next_gen = next.generation();
                let next_state = next.state();
                if next_gen == generation
                    && next_state == slot::SlotState::InFlightWaiting
                    && cell
                        .core_state
                        .compare_exchange(
                            next,
                            next.with_state(slot::SlotState::InFlightReady)
                                .with_generation(generation),
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    cell.completion_waker.wake();
                    return recorded_completion(&self.diagnostics, success_outcome);
                }

                // If we reached here, someone else either:
                // 1. already set it to InFlightReady (which is fine, we just discard our duplicate data)
                // 2. recycled the slot (generation mismatch)
                let (stored_payload, stored_detail, stored_cleanup, stored_kind) = cell
                    .completion_with_record_data(
                        |payload_cell, detail_cell, cleanup_cell, kind_cell| {
                            (
                                payload_cell.take(),
                                detail_cell.take(),
                                std::mem::take(cleanup_cell),
                                *kind_cell,
                            )
                        },
                    );
                packet.payload = stored_payload;
                packet.detail = stored_detail;
                packet.cleanup = stored_cleanup;
                packet.record_kind = stored_kind;

                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() == generation
                    && cur.state() == slot::SlotState::InFlightOrphaned
                {
                    if should_note_ready {
                        self.clear_ready_completion();
                    }
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::OrphanedDropped,
                        packet,
                    );
                } else if should_note_ready {
                    self.clear_ready_completion();
                }
                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() != generation {
                    return rejected_completion(
                        &self.diagnostics,
                        RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                            token,
                            idx,
                            generation,
                            cur.generation(),
                            cur.state(),
                        )),
                        packet,
                    );
                }
                return rejected_completion(
                    &self.diagnostics,
                    RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                        token,
                        idx,
                        generation,
                        cur.state(),
                    )),
                    packet,
                );
            }
        }

        cell.completion_waker.wake();
        recorded_completion(&self.diagnostics, success_outcome)
    }

    #[inline]
    fn try_take_record(&self, token: CompletionToken) -> PollRecordResult<UP, E, R> {
        let Some(op_token) = token.op_token() else {
            let anomaly = CompletionAnomaly::unknown_control(token);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Lost(anomaly);
        };
        let (idx, generation) = op_token.parts();
        if idx >= self.slots.len() {
            let anomaly = CompletionAnomaly::unknown_slot(token, idx, generation);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Lost(anomaly);
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        // If the cell's generation is strictly greater than ours, we are stale.
        if cell_gen > generation {
            let anomaly = CompletionAnomaly::stale(token, idx, generation, cell_gen, state);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Stale(anomaly);
        }

        if cell_gen != generation {
            return PollRecordResult::Pending;
        }

        if state != slot::SlotState::InFlightReady {
            return match state {
                slot::SlotState::InFlightWaiting => PollRecordResult::Pending,
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned
                | slot::SlotState::Finalizing
                | slot::SlotState::ReservedValue => {
                    let anomaly = CompletionAnomaly::non_active(token, idx, generation, state);
                    self.diagnostics.record_anomaly(&anomaly);
                    PollRecordResult::Lost(anomaly)
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
        let (payload, detail, cleanup, record_kind) = cell.completion_with_record_data(
            |payload_cell, detail_cell, cleanup_cell, kind_cell| {
                let kind = *kind_cell;
                *kind_cell = CompletionRecordKind::User;
                (
                    payload_cell.take(),
                    detail_cell.take(),
                    std::mem::take(cleanup_cell),
                    kind,
                )
            },
        );
        if let CompletionRecordKind::Lost(anomaly) = record_kind {
            drop(payload);
            drop(detail);
            drop(cleanup);
            return PollRecordResult::ReadyLost(anomaly);
        }
        let Some(payload) = payload else {
            drop(detail);
            drop(cleanup);
            let anomaly = CompletionAnomaly::payload_missing(token, idx, generation).with_event(
                CompletionEvent {
                    token,
                    res: cell.completion_res.load(Ordering::Acquire),
                    flags: cell.completion_flags.load(Ordering::Acquire),
                },
            );
            self.diagnostics.record_anomaly(&anomaly);
            self.diagnostics.inc_user_lost();
            return PollRecordResult::ReadyLost(anomaly);
        };
        PollRecordResult::Ready(CompletionRecord {
            event: CompletionEvent {
                token,
                res: cell.completion_res.load(Ordering::Acquire),
                flags: cell.completion_flags.load(Ordering::Acquire),
            },
            payload,
            detail,
            cleanup,
            record_kind,
        })
    }

    #[inline]
    fn register_waker(&self, token: CompletionToken, waker: &Waker) -> CompletionMutationOutcome {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return recorded_mutation(
                &self.diagnostics,
                CompletionMutationOutcome::UnknownControl(CompletionAnomaly::unknown_control(
                    token,
                )),
            );
        };
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
        if current_after.state() == slot::SlotState::InFlightReady
            && current_after.generation() == generation
        {
            waker.wake_by_ref();
            return recorded_mutation(&self.diagnostics, CompletionMutationOutcome::Applied);
        }

        let outcome = match current_after.state() {
            slot::SlotState::InFlightWaiting | slot::SlotState::InFlightReady => {
                CompletionMutationOutcome::Applied
            }
            state => mutation_non_active(token, idx, generation, state),
        };
        recorded_mutation(&self.diagnostics, outcome)
    }

    #[inline]
    fn mark_waiting(&self, token: CompletionToken) -> CompletionMutationOutcome {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return recorded_mutation(
                &self.diagnostics,
                CompletionMutationOutcome::UnknownControl(CompletionAnomaly::unknown_control(
                    token,
                )),
            );
        };
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
    fn mark_orphaned(&self, token: CompletionToken) -> CompletionMutationOutcome {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return recorded_mutation(
                &self.diagnostics,
                CompletionMutationOutcome::UnknownControl(CompletionAnomaly::unknown_control(
                    token,
                )),
            );
        };
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

    #[test]
    fn record_completion_rejects_idle_future_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );

        let outcome = table
            .record_completion(CompletionPacket::new(
                CompletionEvent {
                    token,
                    res: 0,
                    flags: 0,
                },
                None,
                None,
            ))
            .into_outcome();

        assert!(matches!(outcome, RecordCompletionOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    }

    #[test]
    fn mark_waiting_does_not_activate_idle_future_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );

        let outcome = table.mark_waiting(token);

        assert!(matches!(outcome, CompletionMutationOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    }

    #[test]
    fn mark_waiting_does_not_revive_orphaned_slot() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::InFlightOrphaned, Ordering::Release);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );

        let outcome = table.mark_waiting(token);

        assert!(matches!(outcome, CompletionMutationOutcome::NonActive(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_ORPHANED);
    }

    #[test]
    fn mark_orphaned_reports_stale_generation() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(2);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );

        let outcome = table.mark_orphaned(token);

        assert!(matches!(outcome, CompletionMutationOutcome::Stale(_)));
        assert_eq!(table.debug_get_state(0), CELL_STATE_WAITING);
        assert_eq!(
            table.completion_diagnostics().snapshot().stale_completion,
            1
        );
    }

    #[test]
    fn register_waker_reports_unknown_control_token() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let waker = std::task::Waker::noop();

        let outcome = table.register_waker(CompletionToken::waker(0), waker);

        assert!(matches!(
            outcome,
            CompletionMutationOutcome::UnknownControl(_)
        ));
    }

    #[test]
    fn lost_completion_is_observable_as_ready_lost() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::Reserved, Ordering::Release);
        let op_token = OpToken::from_registry_parts(0, 1).expect("test token should be encodable");
        let token = CompletionToken::user(op_token);
        let anomaly = CompletionAnomaly::corrupt(token, 0, 1, slot::SlotState::Reserved);

        let outcome = table
            .record_lost_completion(
                CompletionEvent {
                    token,
                    res: -5,
                    flags: 0,
                },
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::ReadyLost(observed) if observed.reason == anomaly.reason
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
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );
        let anomaly = CompletionAnomaly::stale(token, 0, 1, 2, slot::SlotState::InFlightWaiting);

        let outcome = table
            .record_lost_completion(
                CompletionEvent {
                    token,
                    res: -1,
                    flags: 0,
                },
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert!(matches!(outcome, RecordCompletionOutcome::Stale(_)));
    }

    #[test]
    fn lost_completion_reports_empty_slot() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 0).expect("test token should be encodable"),
        );
        let anomaly = CompletionAnomaly::non_active(token, 0, 0, slot::SlotState::Idle);

        let outcome = table
            .record_lost_completion(
                CompletionEvent {
                    token,
                    res: -1,
                    flags: 0,
                },
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
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );
        let anomaly = CompletionAnomaly::payload_missing(token, 0, 1);

        let outcome = table
            .record_lost_completion(
                CompletionEvent {
                    token,
                    res: -1,
                    flags: 0,
                },
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::ReadyLost(observed) if observed.reason == anomaly.reason
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
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );
        let anomaly = CompletionAnomaly::op_missing(token, 0, 1);

        let outcome = table
            .record_lost_completion(
                CompletionEvent {
                    token,
                    res: -1,
                    flags: 0,
                },
                anomaly,
                CompletionCleanupGuard::default(),
            )
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::ReadyLost(observed) if observed.reason == anomaly.reason
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.user_lost, 1);
        assert_eq!(snapshot.slot_corruption, 1);
    }

    #[test]
    fn user_completion_without_payload_is_recorded_lost_and_counted() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        table.slots[0].reset(1);
        table.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let token = CompletionToken::user(
            OpToken::from_registry_parts(0, 1).expect("test token should be encodable"),
        );

        let outcome = table
            .record_completion(CompletionPacket::new(
                CompletionEvent {
                    token,
                    res: 0,
                    flags: 0,
                },
                None,
                None,
            ))
            .into_outcome();

        assert_eq!(outcome, RecordCompletionOutcome::RecordedLost);
        assert!(matches!(
            table.try_take_record(token),
            PollRecordResult::ReadyLost(anomaly)
                if anomaly.reason == CompletionAnomalyReason::PayloadMissing
        ));
        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.user_lost, 1);
        assert_eq!(snapshot.user_completed, 0);
        assert_eq!(snapshot.payload_missing, 1);
    }
}
