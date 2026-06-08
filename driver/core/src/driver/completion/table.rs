use crate::slot;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

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

    fn register_waker(&self, token: CompletionToken, waker: &Waker);

    fn mark_waiting(&self, token: CompletionToken);

    fn mark_orphaned(&self, token: CompletionToken);

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;
}

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

#[inline]
fn rejected_completion<UP, E, R>(
    outcome: RecordCompletionOutcome,
    packet: CompletionPacket<UP, E, R>,
) -> RecordCompletionResult<UP, E, R> {
    RecordCompletionResult::Rejected { outcome, packet }
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
                RecordCompletionOutcome::Missing(CompletionAnomaly::unknown_control(token)),
                packet,
            );
        };
        let (idx, generation) = op_token.parts();
        if idx >= self.slots.len() {
            return rejected_completion(
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
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    )),
                    packet,
                );
            }
            if generation > cell_gen && state != slot::SlotState::Idle {
                return rejected_completion(
                    RecordCompletionOutcome::Stale(CompletionAnomaly::stale(
                        token, idx, generation, cell_gen, state,
                    )),
                    packet,
                );
            }

            match state {
                slot::SlotState::Idle if generation > cell_gen => {
                    should_note_ready = true;
                    break current;
                }
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
                        RecordCompletionOutcome::NonActive(CompletionAnomaly::non_active(
                            token, idx, generation, state,
                        )),
                        packet,
                    );
                }
                slot::SlotState::InFlightOrphaned => {
                    if cell_gen == generation {
                        return rejected_completion(
                            RecordCompletionOutcome::OrphanedDropped,
                            packet,
                        );
                    } else {
                        return rejected_completion(
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
                    return RecordCompletionResult::Recorded;
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
                    return rejected_completion(RecordCompletionOutcome::OrphanedDropped, packet);
                } else if should_note_ready {
                    self.clear_ready_completion();
                }
                let cur = cell.load_core_state(Ordering::Acquire);
                if cur.generation() != generation {
                    return rejected_completion(
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
        RecordCompletionResult::Recorded
    }

    #[inline]
    fn try_take_record(&self, token: CompletionToken) -> PollRecordResult<UP, E, R> {
        let Some(op_token) = token.op_token() else {
            return PollRecordResult::Lost(CompletionAnomaly::unknown_control(token));
        };
        let (idx, generation) = op_token.parts();
        if idx >= self.slots.len() {
            return PollRecordResult::Lost(CompletionAnomaly::unknown_slot(token, idx, generation));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let state = current.state();
        let cell_gen = current.generation();

        // If the cell's generation is strictly greater than ours, we are stale.
        if cell_gen > generation {
            return PollRecordResult::Stale(CompletionAnomaly::stale(
                token, idx, generation, cell_gen, state,
            ));
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
                | slot::SlotState::ReservedValue => PollRecordResult::Lost(
                    CompletionAnomaly::non_active(token, idx, generation, state),
                ),
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
    fn register_waker(&self, token: CompletionToken, waker: &Waker) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            if cell_gen > generation {
                return;
            }

            // Register waker. AtomicWaker handles races with concurrent wake().
            cell.completion_waker.register(waker);

            if cell_gen < generation {
                if state == slot::SlotState::Idle {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightWaiting)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        // Check for fast completion.
                        let current_after = cell.load_core_state(Ordering::Acquire);
                        if current_after.state() == slot::SlotState::InFlightReady
                            && current_after.generation() == generation
                        {
                            waker.wake_by_ref();
                        }
                        return;
                    } else {
                        continue;
                    }
                } else {
                    return;
                }
            }

            // cell_gen == generation
            let current_after = cell.load_core_state(Ordering::Acquire);
            if current_after.state() == slot::SlotState::InFlightReady
                && current_after.generation() == generation
            {
                waker.wake_by_ref();
            }
            return;
        }
    }

    #[inline]
    fn mark_waiting(&self, token: CompletionToken) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_generation = current.generation();

            if cell_generation > generation {
                // Stale request, slot already repurposed for a newer op.
                return;
            }

            if cell_generation < generation {
                if state == slot::SlotState::Idle {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightWaiting)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                } else {
                    // Cannot upgrade yet.
                    return;
                }
            } else {
                // cell_generation == generation
                if state == slot::SlotState::InFlightReady {
                    // Fast completion happened, leave as READY.
                    return;
                }

                match state {
                    slot::SlotState::Idle
                    | slot::SlotState::InFlightOrphaned
                    | slot::SlotState::InFlightWaiting
                    | slot::SlotState::ReservedValue => {
                        if cell
                            .core_state
                            .compare_exchange(
                                current,
                                current
                                    .with_state(slot::SlotState::InFlightWaiting)
                                    .with_generation(generation),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            return;
                        }
                    }
                    slot::SlotState::Finalizing => {
                        return;
                    }
                    slot::SlotState::Reserved | slot::SlotState::InFlightReady => return,
                }
            }
        }
    }

    #[inline]
    fn mark_orphaned(&self, token: CompletionToken) {
        let Some((idx, generation)) = decode_completion_token(token) else {
            return;
        };
        if idx >= self.slots.len() {
            return;
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let state = current.state();
            let cell_gen = current.generation();

            match state {
                slot::SlotState::InFlightWaiting => {
                    if cell_gen != generation {
                        return;
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
                        return;
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
                        return;
                    }
                }
                slot::SlotState::Finalizing => continue,
                _ => return,
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
