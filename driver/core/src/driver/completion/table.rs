use crate::slot;
use std::sync::Arc;
use std::task::Waker;
use veloq_shim::atomic::Ordering;

use super::types::CompletionMutationOutcome;
use super::{
    CompletionAnomaly, CompletionCleanupGuard, CompletionInput, CompletionPacket, CompletionRecord,
    CompletionToken, CompletionWritePermit, OpToken, RecordCompletionOutcome,
    RecordCompletionResult, UserCompletionEvent, run_completion_cleanup,
};

pub type SharedCompletionTable<Spec> = Arc<dyn CompletionAccess<Spec>>;

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<Spec: slot::SlotSpec> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<Spec>),
    /// Operation completion became unavailable and the waiter can finish as resource-lost.
    Unavailable(CompletionAnomaly),
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
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> RecordCompletionResult<Spec> {
        self.record_completion(permit, CompletionPacket::lost(event, anomaly, cleanup))
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
    diagnostics: &super::DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    outcome: RecordCompletionOutcome,
) -> RecordCompletionResult<Spec>
where
    Spec::CompletionDiagnostics: super::DriverCompletionDiagnosticsBackend,
{
    diagnostics.record_completion_outcome(&outcome);
    RecordCompletionResult::Recorded(outcome)
}

#[inline]
fn rejected_completion<Spec: slot::SlotSpec>(
    diagnostics: &super::DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    outcome: RecordCompletionOutcome,
    packet: CompletionPacket<Spec>,
) -> RecordCompletionResult<Spec>
where
    Spec::CompletionDiagnostics: super::DriverCompletionDiagnosticsBackend,
{
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
fn recorded_mutation<B>(
    diagnostics: &super::DriverCompletionDiagnostics<B>,
    outcome: CompletionMutationOutcome,
) -> CompletionMutationOutcome
where
    B: super::DriverCompletionDiagnosticsBackend,
{
    if let Some(anomaly) = outcome.anomaly() {
        diagnostics.record_anomaly(anomaly);
    }
    outcome
}

#[inline]
fn run_discarded_record_cleanup<Spec: slot::SlotSpec>(
    diagnostics: &super::DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    record_data: slot::CompletionData<Spec>,
) where
    Spec::CompletionDiagnostics: super::DriverCompletionDiagnosticsBackend,
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
            anomaly: _,
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
        let token = event.completion_token();
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
                    anomaly: loss.anomaly,
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

        if cell_gen < generation {
            let anomaly = CompletionAnomaly::non_active(completion_token, idx, generation, state);
            self.diagnostics.record_anomaly(&anomaly);
            return PollRecordResult::Unavailable(anomaly);
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
            slot::CompletionData::Lost { anomaly, cleanup } => {
                let mut cleanup = cleanup;
                let _ = run_completion_cleanup(&self.diagnostics, &mut cleanup);
                PollRecordResult::Unavailable(anomaly)
            }
            slot::CompletionData::Empty => {
                let anomaly = CompletionAnomaly::payload_missing(completion_token, idx, generation)
                    .with_raw_result(cell.completion_res.load(Ordering::Acquire))
                    .with_flags(cell.completion_flags.load(Ordering::Acquire));
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
    fn discard_ready_record(&self, token: OpToken) -> CompletionMutationOutcome {
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
                        run_discarded_record_cleanup(&self.diagnostics, record_data);
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
                    return self.discard_ready_record(token);
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
    use crate::DriverCoreError;
    use crate::driver::OpToken;
    use crate::driver::PlatformOp;
    use crate::driver::registry::OpRegistry;
    use crate::driver::{
        CompletionAnomalyReason, CompletionBackend, CompletionBackendHooks, CompletionCleanup,
        CompletionControl, CompletionEnvelope, CompletionFlowExt, CompletionFlowOutcome,
        CompletionHookOutcome, CompletionIngress, CompletionSource,
    };
    use crate::slot::{CheckedSlotView, SlotRegistryExt, SlotView};
    use diagweave::prelude::*;

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {
        type CleanupContext<'a> = ();
    }

    struct DummySlotSpec;

    impl slot::SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = ();
        type Completion = usize;
        type CompletionDiagnostics = ();
    }

    fn test_token(index: usize, generation: u32) -> OpToken {
        OpToken::from_registry_parts(index, generation).expect("test token should be encodable")
    }

    fn test_event(token: OpToken, res: i32) -> UserCompletionEvent {
        UserCompletionEvent::from_parts(CompletionBackend::Core, token, res, 0)
    }

    #[derive(Default)]
    struct TestHooks {
        loss_reason: Option<CompletionAnomaly>,
        cleanup: Option<CompletionCleanupGuard>,
    }

    impl CompletionBackendHooks<DummySlotSpec> for TestHooks {
        type BackendIngress = ();
        type BackendEffect = ();

        fn handle_control(
            &mut self,
            _control: CompletionControl,
        ) -> CompletionHookOutcome<DummySlotSpec, Self::BackendEffect> {
            CompletionHookOutcome::Ignore { effect: () }
        }

        fn complete_waiting(
            &mut self,
            event: UserCompletionEvent,
            slot: slot::Slot<'_, slot::InFlightWaiting, DummySlotSpec>,
            _source: CompletionSource<'_, Self::BackendIngress>,
        ) -> CompletionHookOutcome<DummySlotSpec, Self::BackendEffect> {
            if let Some(loss_reason) = self.loss_reason.take() {
                let snapshot = slot.snapshot();
                let mut completed = slot.complete();
                let _ = completed.take_op();
                let (payload, detail) = completed.take_completion_data();
                let _ = payload;
                drop(detail);
                return CompletionHookOutcome::Lost {
                    event,
                    loss_reason,
                    snapshot,
                    cleanup: self.cleanup.take().unwrap_or_default(),
                    effect: (),
                };
            }

            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            CompletionHookOutcome::User {
                event,
                payload: payload.expect("test slot payload should exist"),
                detail,
                cleanup: self.cleanup.take().unwrap_or_default(),
                effect: (),
            }
        }

        fn complete_orphaned(
            &mut self,
            _event: UserCompletionEvent,
            slot: slot::Slot<'_, slot::InFlightOrphaned, DummySlotSpec>,
            _source: CompletionSource<'_, Self::BackendIngress>,
        ) -> CompletionHookOutcome<DummySlotSpec, Self::BackendEffect> {
            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            let _ = payload;
            drop(detail);
            CompletionHookOutcome::Cleanup {
                cleanup: self.cleanup.take().unwrap_or_default(),
                effect: (),
            }
        }

        fn complete_corrupt(
            &mut self,
            event: UserCompletionEvent,
            anomaly: CompletionAnomaly,
            _source: CompletionSource<'_, Self::BackendIngress>,
        ) -> CompletionHookOutcome<DummySlotSpec, Self::BackendEffect> {
            let Some(snapshot) = anomaly.slot_snapshot else {
                return CompletionHookOutcome::Anomaly {
                    anomaly,
                    effect: (),
                };
            };
            CompletionHookOutcome::Lost {
                event,
                loss_reason: self.loss_reason.take().unwrap_or(anomaly),
                snapshot,
                cleanup: self.cleanup.take().unwrap_or_default(),
                effect: (),
            }
        }

        fn finish_backend_effect(&mut self, _effect: Self::BackendEffect) {}
    }

    fn active_registry() -> (OpRegistry<DummySlotSpec>, OpToken) {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = test_token(handle.index, handle.generation);
        registry
            .with_slot_storage_mut(token, |_result, payload, _sidecar| {
                *payload = Some(());
            })
            .expect("slot storage should exist");
        let slot = match registry.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot
                .init_op_with(DummyPlatformOp, |_| {})
                .expect("reserved slot should accept op"),
            _ => panic!("reserved slot should be available"),
        };
        let _in_flight = slot
            .start_submission_with(None)
            .expect("reserved slot should start submission")
            .persist();
        (registry, token)
    }

    fn reserved_registry() -> (OpRegistry<DummySlotSpec>, OpToken) {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = test_token(handle.index, handle.generation);
        (registry, token)
    }

    fn accept_with_hooks(
        registry: &mut OpRegistry<DummySlotSpec>,
        event: UserCompletionEvent,
        hooks: &mut TestHooks,
    ) -> CompletionFlowOutcome {
        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
        registry.accept_completion(&table, &diagnostics, hooks, CompletionIngress::User(event))
    }

    fn accept_user(registry: &mut OpRegistry<DummySlotSpec>, token: OpToken, res: i32) {
        let mut hooks = TestHooks::default();
        let _ = accept_with_hooks(registry, test_event(token, res), &mut hooks);
    }

    fn accept_lost(
        registry: &mut OpRegistry<DummySlotSpec>,
        token: OpToken,
        res: i32,
        anomaly: CompletionAnomaly,
        cleanup: CompletionCleanupGuard,
    ) -> CompletionFlowOutcome {
        let mut hooks = TestHooks {
            loss_reason: Some(anomaly),
            cleanup: Some(cleanup),
        };
        accept_with_hooks(registry, test_event(token, res), &mut hooks)
    }

    #[test]
    fn record_completion_rejects_idle_future_generation() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let table = registry.shared.clone();
        let token = test_token(0, 1);

        let mut hooks = TestHooks::default();
        let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);

        assert_eq!(outcome.anomaly, 1);
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    }

    #[test]
    fn try_take_record_reports_future_generation_unavailable() {
        let table = slot::SlotTable::<DummySlotSpec>::new(1);
        let token = test_token(0, 1);

        match table.try_take_record(token) {
            PollRecordResult::Unavailable(anomaly) => {
                assert_eq!(anomaly.reason, CompletionAnomalyReason::NonActiveSlot);
                assert_eq!(anomaly.index, Some(0));
                assert_eq!(anomaly.expected_generation, Some(1));
            }
            PollRecordResult::Pending => panic!("future generation token must not stay pending"),
            PollRecordResult::Ready(_) => panic!("future generation token must not become ready"),
        }
    }

    #[test]
    fn raw_unknown_control_is_recorded_as_anomaly() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
        let mut hooks = TestHooks::default();
        let raw_unknown_control = (99u64 << 48) | (7u64 << 32) | u64::from(u32::MAX);

        let outcome = registry.accept_completion(
            &table,
            &diagnostics,
            &mut hooks,
            CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                CompletionBackend::Core,
                raw_unknown_control,
                -5,
                0,
            )),
        );

        assert_eq!(outcome.anomaly, 1);
        assert_eq!(diagnostics.snapshot().unknown_completion, 1);
        assert!(!registry.shared.has_ready_completion());
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
        let (mut registry, token) = reserved_registry();
        let table = registry.shared.clone();
        let completion_token = CompletionToken::user(token);
        let anomaly = CompletionAnomaly::corrupt(completion_token, 0, 1, slot::SlotState::Reserved);

        let outcome = accept_lost(
            &mut registry,
            token,
            -5,
            anomaly,
            CompletionCleanupGuard::default(),
        );

        assert_eq!(outcome.user_lost, 1);
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
        let (mut registry, token) = active_registry();
        let _ = registry.remove(token);
        let fresh = registry.alloc(()).expect("fresh slot").handle;
        registry.shared.slots[0].set_state(slot::SlotState::InFlightWaiting, Ordering::Release);
        let anomaly = CompletionAnomaly::stale(
            CompletionToken::user(token),
            0,
            1,
            fresh.generation,
            slot::SlotState::InFlightWaiting,
        );

        let outcome = accept_lost(
            &mut registry,
            token,
            -1,
            anomaly,
            CompletionCleanupGuard::default(),
        );

        assert_eq!(outcome.anomaly, 1);
    }

    #[test]
    fn lost_completion_reports_empty_slot() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let token = test_token(0, 0);
        let anomaly = CompletionAnomaly::non_active(
            CompletionToken::user(token),
            0,
            0,
            slot::SlotState::Idle,
        );

        let outcome = accept_lost(
            &mut registry,
            token,
            -1,
            anomaly,
            CompletionCleanupGuard::default(),
        );

        assert_eq!(outcome.anomaly, 1);
    }

    #[test]
    fn lost_completion_preserves_payload_missing_reason() {
        let (mut registry, token) = reserved_registry();
        let table = registry.shared.clone();
        let anomaly = CompletionAnomaly::payload_missing(CompletionToken::user(token), 0, 1);

        let outcome = accept_lost(
            &mut registry,
            token,
            -1,
            anomaly,
            CompletionCleanupGuard::default(),
        );

        assert_eq!(outcome.user_lost, 1);
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
        let (mut registry, token) = reserved_registry();
        let table = registry.shared.clone();
        let anomaly = CompletionAnomaly::op_missing(CompletionToken::user(token), 0, 1);

        let outcome = accept_lost(
            &mut registry,
            token,
            -1,
            anomaly,
            CompletionCleanupGuard::default(),
        );

        assert_eq!(outcome.user_lost, 1);
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
        let (mut registry, token) = active_registry();
        let table = registry.shared.clone();

        let mut hooks = TestHooks::default();
        let first = accept_with_hooks(&mut registry, test_event(token, 11), &mut hooks);
        let duplicate = accept_with_hooks(&mut registry, test_event(token, 22), &mut hooks);

        assert_eq!(first.user_completed, 1);
        assert_eq!(duplicate.anomaly, 1);
        let record = match table.try_take_record(token) {
            PollRecordResult::Ready(record) => record,
            PollRecordResult::Pending => panic!("first completion should be ready"),
            PollRecordResult::Unavailable(anomaly) => {
                panic!("first completion should remain available: {anomaly:?}")
            }
        };
        assert_eq!(record.event.res(), 11);
    }

    #[test]
    fn ready_mark_orphaned_cleanup_leaves_diagnostic_stale_result() {
        let (mut registry, token) = active_registry();
        let table = registry.shared.clone();

        accept_user(&mut registry, token, 0);
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

    #[test]
    fn ready_mark_orphaned_runs_cleanup_and_records_error() {
        let (mut registry, token) = active_registry();
        let table = registry.shared.clone();
        let cleanup = CompletionCleanupGuard::new(CompletionCleanup::new(|| {
            Err(DriverCoreError::Internal
                .to_report()
                .attach_note("test cleanup failure"))
        }));

        let mut hooks = TestHooks {
            cleanup: Some(cleanup),
            ..TestHooks::default()
        };
        let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);
        assert_eq!(outcome.user_completed, 1);
        assert_eq!(
            table.mark_orphaned(token),
            CompletionMutationOutcome::Applied
        );

        let snapshot = table.completion_diagnostics().snapshot();
        assert_eq!(snapshot.orphan_cleanup_error, 1);
    }
}
