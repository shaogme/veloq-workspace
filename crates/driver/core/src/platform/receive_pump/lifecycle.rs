use veloq_std::collections::VecDeque;

use super::{
    ReceivePermitPool, ReceivePumpError, ReceivePumpEvent, ReceivePumpPhase, ReceivePumpStats,
    ReceiveSlot, ReceiveSlotState, ReceiveSubmission, types::PendingCompletion,
};

pub(super) struct FinalizationContext<'a> {
    pub(super) final_emitted: &'a mut bool,
    pub(super) terminal_error: Option<ReceivePumpError>,
    pub(super) actual_in_flight: usize,
    pub(super) pending_count: usize,
    pub(super) held_count: usize,
    pub(super) datagram_count: usize,
    pub(super) has_prepared: bool,
}

pub(super) struct DrainContext<'a> {
    pub(super) phase: &'a mut ReceivePumpPhase,
    pub(super) actual_in_flight: usize,
    pub(super) pending_count: usize,
    pub(super) held_count: usize,
    pub(super) datagram_count: usize,
    pub(super) has_prepared: bool,
    pub(super) final_emitted: bool,
    pub(super) slots: &'a mut [ReceiveSlot],
}

pub(super) struct InvariantContext<'a> {
    pub(super) multishot: bool,
    pub(super) actual_in_flight: usize,
    pub(super) target_depth: usize,
    pub(super) slots: &'a [ReceiveSlot],
}

pub(super) struct PumpLifecycle;

impl PumpLifecycle {
    pub(super) fn slot_index(slots: &[ReceiveSlot], slot_id: u32) -> Option<usize> {
        slots.iter().position(|slot| slot.slot_id() == slot_id)
    }

    pub(super) fn oldest_pending_event(
        pending: &VecDeque<PendingCompletion>,
    ) -> Option<ReceivePumpEvent> {
        pending.front().map(|item| ReceivePumpEvent::Pending {
            request: item.request,
            key: item.key,
        })
    }

    pub(super) fn validate_submission(
        slots: &[ReceiveSlot],
        submission: ReceiveSubmission,
    ) -> Result<usize, ReceivePumpError> {
        let Some(index) = Self::slot_index(slots, submission.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &slots[index];
        if slot.state() != ReceiveSlotState::SubmitPending
            || slot.generation() != submission.slot_generation
        {
            return Err(ReceivePumpError::InvalidTransition);
        }
        Ok(index)
    }

    pub(super) fn try_enter_running(
        phase: &mut ReceivePumpPhase,
        actual_in_flight: usize,
        target_depth: usize,
        slots: &[ReceiveSlot],
    ) {
        if *phase == ReceivePumpPhase::Starting
            && actual_in_flight == target_depth
            && !slots
                .iter()
                .any(|slot| slot.state() == ReceiveSlotState::SubmitPending)
        {
            *phase = ReceivePumpPhase::Running;
        }
    }

    pub(super) fn pause_for_backpressure(
        phase: &mut ReceivePumpPhase,
        stats: &mut ReceivePumpStats,
    ) {
        if *phase != ReceivePumpPhase::PausedByBackpressure {
            stats.receive_backpressure_events += 1;
        }
        stats.receive_permit_exhausted += 1;
        *phase = ReceivePumpPhase::PausedByBackpressure;
    }

    pub(super) fn resume_if_possible(phase: &mut ReceivePumpPhase, permits: &ReceivePermitPool) {
        if *phase == ReceivePumpPhase::PausedByBackpressure && permits.available() > 0 {
            *phase = ReceivePumpPhase::Running;
        }
    }

    pub(super) fn set_terminal(
        phase: &mut ReceivePumpPhase,
        terminal_error: &mut Option<ReceivePumpError>,
        error: ReceivePumpError,
    ) {
        if terminal_error.is_none() {
            *terminal_error = Some(error);
        }
        if *phase != ReceivePumpPhase::Closing {
            *phase = ReceivePumpPhase::RunningWithTerminalError;
        }
    }

    pub(super) fn retire_pending(
        slots: &mut [ReceiveSlot],
        pending: &mut VecDeque<PendingCompletion>,
        held: &mut VecDeque<PendingCompletion>,
    ) {
        for completion in pending.drain(..).chain(held.drain(..)) {
            if let Some(index) = Self::slot_index(slots, completion.request.slot_id) {
                slots[index].set_state(ReceiveSlotState::Retiring);
            }
        }
    }

    pub(super) fn emit_final_if_possible(
        context: FinalizationContext<'_>,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if *context.final_emitted {
            return Ok(ReceivePumpEvent::Ignored);
        }
        if context.actual_in_flight != 0
            || context.pending_count != 0
            || context.held_count != 0
            || context.datagram_count != 0
            || context.has_prepared
        {
            return Ok(ReceivePumpEvent::Ignored);
        }
        *context.final_emitted = true;
        let error = context
            .terminal_error
            .unwrap_or(ReceivePumpError::DriverShutdown);
        Ok(ReceivePumpEvent::Final { error })
    }

    pub(super) fn emit_final_or_drain(
        context: FinalizationContext<'_>,
        phase: ReceivePumpPhase,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        let actual_in_flight = context.actual_in_flight;
        let event = Self::emit_final_if_possible(context)?;
        if matches!(event, ReceivePumpEvent::Ignored)
            && actual_in_flight != 0
            && matches!(
                phase,
                ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
            )
        {
            Ok(ReceivePumpEvent::Drain)
        } else {
            Ok(event)
        }
    }

    pub(super) fn maybe_drained(context: DrainContext<'_>) {
        if matches!(
            *context.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) && context.actual_in_flight == 0
            && context.pending_count == 0
            && context.held_count == 0
            && context.datagram_count == 0
            && !context.has_prepared
            && context.final_emitted
        {
            *context.phase = ReceivePumpPhase::Drained;
            for slot in context.slots.iter_mut() {
                if slot.state() != ReceiveSlotState::InFlight {
                    slot.set_state(ReceiveSlotState::Retiring);
                }
            }
        }
    }

    pub(super) fn debug_assert_invariants(context: InvariantContext<'_>) {
        if !context.multishot {
            let in_flight_slots = context
                .slots
                .iter()
                .filter(|slot| slot.state() == ReceiveSlotState::InFlight)
                .count();
            debug_assert_eq!(context.actual_in_flight, in_flight_slots);
            debug_assert!(context.actual_in_flight <= context.target_depth);
        }
    }
}
