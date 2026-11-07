//! Platform-independent state and ownership rules for a persistent UDP receive pump.
//!
//! This module deliberately does not know about IOCP, RIO, io_uring, or an operating-system
//! error type.  A backend drives it with three facts only: a receive request was submitted, a
//! request completed, and a replacement request was submitted.  Keeping those transitions here
//! makes the resource invariants testable without booting a driver or an operating system.

mod config;
mod datagram;
mod lifecycle;
mod permit;
mod queues;
mod types;

pub use config::*;
pub use datagram::*;
pub use permit::*;
pub use queues::*;
pub use types::*;

use crate::{
    driver::CompletionContinuation,
    op::{UdpRecvPacket, UdpRecvPacketBuf},
};
use datagram::{DatagramQueue, DatagramQueueContext};
use lifecycle::{DrainContext, FinalizationContext, InvariantContext, PumpLifecycle};
use queues::PumpQueues;
use veloq_buf::FixedBuf;
use veloq_std::{boxed::Box, collections::VecDeque, net::SocketAddr, sync::Arc};

/// The pure state machine for one logical UDP receive operation.
pub struct ReceivePumpState {
    config: ReceivePumpConfig,
    phase: ReceivePumpPhase,
    slots: Box<[ReceiveSlot]>,
    permits: ReceivePermitPool,
    pending: VecDeque<PendingCompletion>,
    held: VecDeque<PendingCompletion>,
    prepared: Option<ReceiveRequest>,
    actual_in_flight: usize,
    ready_packets: usize,
    terminal_error: Option<ReceivePumpError>,
    eof_on_empty: bool,
    final_emitted: bool,
    stats: ReceivePumpStats,
    completion_diagnostics: ReceiveCompletionDiagnostics,
    datagrams: DatagramQueue,
}

impl ReceivePumpState {
    pub fn try_new(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_eof(config, slots, false, None)
    }

    /// Construct a fixed-slot pump with an optional permit-release notifier. / 构造带可选许可释放通知器的固定槽位接收泵。
    pub fn try_new_with_notifier(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_eof(config, slots, false, notifier)
    }

    /// Construct a pump for a stream receive operation where an empty successful completion is
    /// the terminal EOF record rather than a zero-length datagram.
    pub fn try_new_tcp(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_eof(config, slots, true, None)
    }

    /// Construct the platform-independent state for one io_uring multishot datagram operation.
    ///
    /// Unlike the RIO request model this mode has no fixed receive slots. The provided-buffer
    /// lease remains owned by the backend while this state machine owns only bounded datagram
    /// metadata and packet permits.
    pub fn try_new_multishot(
        config: ReceivePumpConfig,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_options(config, Box::new([]), false, true, notifier)
    }

    fn try_new_with_eof(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        eof_on_empty: bool,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_options(config, slots, eof_on_empty, false, notifier)
    }

    fn try_new_with_options(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        eof_on_empty: bool,
        multishot: bool,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        let queues = PumpQueues::allocate(config, slots, multishot)?;
        Ok(Self {
            config,
            phase: ReceivePumpPhase::Created,
            slots: queues.slots,
            permits: ReceivePermitPool::new_with_notifier(config.queue_capacity(), notifier),
            pending: queues.pending,
            held: queues.held,
            prepared: None,
            actual_in_flight: 0,
            ready_packets: 0,
            terminal_error: None,
            eof_on_empty,
            final_emitted: false,
            stats: ReceivePumpStats::default(),
            completion_diagnostics: ReceiveCompletionDiagnostics::default(),
            datagrams: queues.datagrams,
        })
    }

    pub fn config(&self) -> ReceivePumpConfig {
        self.config
    }

    /// Install the backend wakeup used when a packet permit becomes available. / 安装 packet permit 可用时使用的后端唤醒器。
    pub fn set_permit_notifier(
        &mut self,
        notifier: Arc<dyn ReceivePermitNotifier>,
    ) -> Result<(), ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Created {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.permits.set_notifier(notifier)
    }

    /// Update the opaque identity sent to the notifier when a permit is released. / 更新许可释放时通知器投递的不透明标识。
    pub fn bind_resume_token(&self, token: u64) {
        self.permits.bind_resume_token(token);
    }

    pub fn phase(&self) -> ReceivePumpPhase {
        self.phase
    }

    pub fn stats(&self) -> ReceivePumpStats {
        self.stats
    }

    pub fn completion_diagnostics(&self) -> ReceiveCompletionDiagnostics {
        self.completion_diagnostics
    }

    /// Record the raw status observed by a backend before logical result classification.
    pub fn record_completion_observation(&mut self, status: i32, bytes: u32, user_cancelled: bool) {
        let diagnostics = &mut self.completion_diagnostics;
        diagnostics.completion_count = diagnostics.completion_count.saturating_add(1);
        diagnostics.total_bytes = diagnostics.total_bytes.saturating_add(bytes as usize);
        diagnostics.last_status = Some(status);
        if user_cancelled {
            diagnostics.cancelled_completion_count =
                diagnostics.cancelled_completion_count.saturating_add(1);
        }
        if status != 0 {
            diagnostics.error_count = diagnostics.error_count.saturating_add(1);
            if diagnostics.first_error_status.is_none() {
                diagnostics.first_error_status = Some(status);
            }
        }
    }

    pub fn actual_in_flight(&self) -> usize {
        self.debug_assert_invariants();
        self.actual_in_flight
    }

    pub fn pending_completions(&self) -> usize {
        self.pending.len()
    }

    /// Number of completion metadata entries retained while their backend payload lease waits
    /// for room in the bounded pending queue.
    pub fn held_completions(&self) -> usize {
        self.held.len()
    }

    pub fn ready_packets(&self) -> usize {
        self.ready_packets
    }

    pub fn held_packets(&self) -> usize {
        self.permits.in_use().saturating_sub(self.ready_packets)
    }

    pub fn permits_available(&self) -> usize {
        self.permits.available()
    }

    pub fn is_multishot(&self) -> bool {
        self.datagrams.enabled()
    }

    /// Arm the logical multishot receiver after its SQE has been accepted by the backend.
    pub fn arm_multishot(&mut self) -> Result<u32, ReceivePumpError> {
        self.datagrams.arm(&mut self.phase)
    }

    /// 将一个完整 datagram 的 metadata 按完成顺序提交到 multishot FIFO。
    pub fn admit_datagram(
        &mut self,
        len: usize,
        remote_addr: SocketAddr,
    ) -> Result<DatagramAdmission, ReceivePumpError> {
        self.datagrams.admit(
            DatagramQueueContext::new(self.config, &mut self.phase, &mut self.stats, &self.permits),
            len,
            remote_addr,
        )
    }

    /// 取得最旧的 multishot metadata 和一个 packet permit。
    pub fn next_datagram_delivery(&mut self) -> Option<DatagramDelivery> {
        self.datagrams.next_delivery(DatagramQueueContext::new(
            self.config,
            &mut self.phase,
            &mut self.stats,
            &self.permits,
        ))
    }

    pub fn finish_datagram_delivery(
        &mut self,
        delivery: DatagramDelivery,
        output: FixedBuf,
    ) -> Result<UdpRecvPacket, ReceivePumpError> {
        self.datagrams.finish_delivery(
            DatagramQueueContext::new(self.config, &mut self.phase, &mut self.stats, &self.permits),
            delivery,
            output,
        )
    }

    pub fn abort_datagram_delivery(
        &mut self,
        delivery: DatagramDelivery,
    ) -> Result<(), ReceivePumpError> {
        self.datagrams.abort_delivery(delivery)
    }

    pub fn pending_datagrams(&self) -> usize {
        self.datagrams.pending_count()
    }

    pub fn rearm_pending(&self) -> bool {
        self.datagrams.rearm_pending(self.phase)
    }

    pub fn clear_multishot_pending(&mut self) -> usize {
        let count = self.datagrams.clear_pending();
        self.maybe_drained();
        count
    }

    pub fn terminal_error(&self) -> Option<ReceivePumpError> {
        self.terminal_error
    }

    pub fn logical_receiver_generation(&self) -> u32 {
        self.datagrams.generation()
    }

    /// Retain the logical receive operation after a backend arm ended without a user-visible
    /// terminal error. The backend must call [`Self::submit_rearm`] only after all eligible
    /// selected leases have settled.
    pub fn retain_for_rearm(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if !matches!(
            self.phase,
            ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
        ) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Ok(generation) = self.datagrams.advance_generation() else {
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return self.emit_final_or_drain();
        };
        self.phase = ReceivePumpPhase::RearmPending;
        Ok(ReceivePumpEvent::Retained {
            logical_receiver_generation: generation,
        })
    }

    /// Commit a successful logical rearm without creating a second receiver claim.
    pub fn submit_rearm(&mut self) -> Result<(), ReceivePumpError> {
        if self.phase != ReceivePumpPhase::RearmPending {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.phase = ReceivePumpPhase::Running;
        self.stats.receive_logical_rearms = self.stats.receive_logical_rearms.saturating_add(1);
        Ok(())
    }

    pub fn snapshot(&self) -> ReceivePumpSnapshot {
        self.debug_assert_invariants();
        ReceivePumpSnapshot {
            phase: self.phase,
            target_depth: self.config.kernel_capacity().get(),
            actual_in_flight: self.actual_in_flight,
            pending_completions: self.pending.len(),
            held_completions: self.held.len(),
            ready_packets: self.ready_packets,
            held_packets: self.held_packets(),
            permits_available: self.permits.available(),
            final_emitted: self.final_emitted,
            logical_receiver_generation: self.datagrams.generation(),
        }
    }

    pub fn slot(&self, slot_id: u32) -> Option<&ReceiveSlot> {
        self.slot_index(slot_id).map(|index| &self.slots[index])
    }

    pub fn slot_mut(&mut self, slot_id: u32) -> Option<&mut ReceiveSlot> {
        let index = self.slot_index(slot_id)?;
        self.slots.get_mut(index)
    }

    pub fn is_ready(&self) -> bool {
        matches!(
            self.phase,
            ReceivePumpPhase::Running
                | ReceivePumpPhase::PausedByBackpressure
                | ReceivePumpPhase::RearmPending
        )
    }

    pub fn start(&mut self) -> Result<(), ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Created {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.phase = ReceivePumpPhase::Starting;
        Ok(())
    }

    /// Select the next fixed slot for an initial submit.
    pub fn next_submission(&mut self) -> Option<ReceiveSubmission> {
        if self.phase != ReceivePumpPhase::Starting
            || self.actual_in_flight >= self.config.kernel_capacity().get()
        {
            return None;
        }
        let index = self
            .slots
            .iter()
            .position(|slot| slot.state() == ReceiveSlotState::Free)?;
        let slot = &mut self.slots[index];
        slot.set_state(ReceiveSlotState::SubmitPending);
        Some(ReceiveSubmission {
            slot_id: slot.slot_id(),
            slot_generation: slot.generation(),
        })
    }

    /// Commit a successful initial submit.  Readiness is reached only after the target depth is
    /// fully in flight.
    pub fn submit_initial(
        &mut self,
        submission: ReceiveSubmission,
        request_id: u64,
    ) -> Result<ReceiveRequest, ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Starting {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let index = self.validate_submission(submission)?;
        let request = ReceiveRequest {
            request_id,
            slot_id: submission.slot_id,
            slot_generation: submission.slot_generation,
        };
        let slot = &mut self.slots[index];
        slot.set_state(ReceiveSlotState::InFlight);
        slot.set_current_request(Some(request));
        self.actual_in_flight += 1;
        self.stats.receive_requests_submitted += 1;
        self.try_enter_running();
        Ok(request)
    }

    /// Abort startup after a submit failed.  Already-submitted requests remain owned and must be
    /// drained through their completions before the pump can be reclaimed.
    pub fn fail_initial_submit(
        &mut self,
        submission: ReceiveSubmission,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Starting {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let index = self.validate_submission(submission)?;
        self.slots[index].set_state(ReceiveSlotState::Retiring);
        self.terminal_error = Some(ReceivePumpError::InitialReceiveSubmitFailed);
        self.phase = ReceivePumpPhase::Closing;
        self.emit_final_if_possible()
    }

    /// Route one backend completion through the state machine.
    pub fn complete(
        &mut self,
        completion: ReceiveCompletion,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        let Some(index) = self.slot_index(completion.request.slot_id) else {
            self.stats.receive_context_anomaly += 1;
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state() != ReceiveSlotState::InFlight
            || slot.current_request() != Some(completion.request)
        {
            self.stats.receive_context_anomaly += 1;
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        if !matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) && self.pending.len() >= self.config.pending_capacity()
            && self.held.len() >= self.config.pending_capacity()
        {
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        slot.set_current_request(None);
        self.actual_in_flight -= 1;
        self.stats.receive_requests_completed += 1;

        if matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) {
            slot.set_state(ReceiveSlotState::Retiring);
            slot.set_completed_len(0);
            slot.set_completed_addr(None);
            self.promote_held();
            return match self.oldest_pending_event() {
                Some(event) => Ok(event),
                None => self.emit_final_or_drain(),
            };
        }
        match completion.result {
            Ok(len) if len <= slot.data_buf().capacity() => {
                let key = match self.datagrams.next_key() {
                    Ok(key) => key,
                    Err(error) => {
                        slot.set_state(ReceiveSlotState::Retiring);
                        self.set_terminal(error);
                        return self.emit_final_or_drain();
                    }
                };
                slot.set_completed_len(len);
                slot.set_completed_addr(Some(completion.remote_addr));
                let pending = PendingCompletion {
                    request: completion.request,
                    key,
                    len,
                    remote_addr: completion.remote_addr,
                };
                let pending_capacity = self.config.pending_capacity();
                if self.pending.len() >= pending_capacity {
                    slot.set_state(ReceiveSlotState::LeaseHeld);
                    self.held.push_back(pending);
                    self.pause_for_backpressure();
                    return Ok(ReceivePumpEvent::Held {
                        request: completion.request,
                        key,
                    });
                }
                slot.set_state(ReceiveSlotState::CompletedPending);
                self.pending.push_back(pending);
                if len == 0 && self.eof_on_empty {
                    self.set_terminal(ReceivePumpError::ReceiveEof);
                }
                if self.permits.available() == 0 {
                    self.pause_for_backpressure();
                }
                Ok(ReceivePumpEvent::Pending {
                    request: completion.request,
                    key,
                })
            }
            Ok(_) => {
                slot.set_state(ReceiveSlotState::Retiring);
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                match self.oldest_pending_event() {
                    Some(event) => Ok(event),
                    None => self.emit_final_or_drain(),
                }
            }
            Err(error) => {
                slot.set_state(ReceiveSlotState::Retiring);
                self.set_terminal(error);
                match self.oldest_pending_event() {
                    Some(event) => Ok(event),
                    None => self.emit_final_or_drain(),
                }
            }
        }
    }

    /// Move the oldest pending datagram into the copy/replacement phase if a permit is available.
    pub fn next_delivery(&mut self) -> Option<ReceiveDelivery> {
        self.promote_held();
        if self.prepared.is_some()
            || !matches!(
                self.phase,
                ReceivePumpPhase::Running
                    | ReceivePumpPhase::PausedByBackpressure
                    | ReceivePumpPhase::RunningWithTerminalError
            )
        {
            return None;
        }
        let permit = match self.permits.acquire() {
            Some(permit) => permit,
            None => {
                self.pause_for_backpressure();
                return None;
            }
        };
        let pending = self.pending.pop_front()?;
        let Some(index) = self.slot_index(pending.request.slot_id) else {
            drop(permit);
            self.stats.receive_context_anomaly += 1;
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return None;
        };
        let slot = &mut self.slots[index];
        if slot.state() != ReceiveSlotState::CompletedPending {
            drop(permit);
            self.stats.receive_context_anomaly += 1;
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return None;
        }
        slot.set_state(ReceiveSlotState::SubmitPending);
        self.prepared = Some(pending.request);
        self.resume_if_possible();
        Some(ReceiveDelivery::new(ReceiveDeliveryParts {
            request: pending.request,
            key: pending.key,
            len: pending.len,
            remote_addr: pending.remote_addr,
            replacement_required: matches!(
                self.phase,
                ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
            ),
            permit,
        }))
    }

    /// Promote retained completion metadata after a backend payload lease becomes eligible for
    /// delivery.  The operation stays owned and no empty user record is created.
    pub fn resume_pending(&mut self) -> Result<usize, ReceivePumpError> {
        if matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::Drained
        ) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let promoted = self.promote_held();
        self.resume_if_possible();
        Ok(promoted)
    }

    /// Reserve the next generation before submitting a replacement request to the backend.
    pub fn reserve_replacement(
        &mut self,
        delivery: &ReceiveDelivery,
    ) -> Result<ReceiveReplacementReservation, ReceivePumpError> {
        let request = delivery.request();
        if self.prepared != Some(request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Some(index) = self.slot_index(request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state() != ReceiveSlotState::SubmitPending
            || slot.generation() != request.slot_generation
        {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        let Some(slot_generation) = slot.generation().checked_add(1) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        slot.set_generation(slot_generation);
        Ok(ReceiveReplacementReservation::new(
            slot.slot_id(),
            slot_generation,
        ))
    }

    /// Complete the copy/replacement transaction.  A successful replacement is committed before
    /// the returned record is marked `More`; a failure publishes the current packet as `Final`.
    pub fn finish_delivery(
        &mut self,
        mut delivery: ReceiveDelivery,
        mut output: FixedBuf,
        replacement: ReplacementSubmit,
    ) -> Result<ReceiveRecord, ReceivePumpError> {
        let request = delivery.request();
        let key = delivery.key();
        let len = delivery.len();
        let remote_addr = delivery.remote_addr();
        let replacement_required = delivery.replacement_required();
        if self.prepared != Some(request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        if !replacement_required && !matches!(replacement, ReplacementSubmit::NotSubmitted) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Some(index) = self.slot_index(request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state() != ReceiveSlotState::SubmitPending {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        let reservation = match replacement {
            ReplacementSubmit::NotSubmitted => None,
            ReplacementSubmit::Submitted { reservation, .. }
            | ReplacementSubmit::Failed { reservation } => {
                if reservation.slot_id() != slot.slot_id()
                    || reservation.slot_generation() != slot.generation()
                    || reservation.slot_generation() == request.slot_generation
                {
                    return Err(ReceivePumpError::ReceiveContextCorrupt);
                }
                Some(reservation)
            }
        };
        self.prepared = None;
        if output.capacity() < len {
            if reservation.is_some() {
                slot.set_generation(request.slot_generation);
            }
            slot.set_state(ReceiveSlotState::CompletedPending);
            self.pending.push_front(PendingCompletion {
                request,
                key,
                len,
                remote_addr,
            });
            drop(delivery);
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        output.set_len(len);
        let permit = delivery
            .take_permit()
            .ok_or(ReceivePumpError::InvalidTransition)?;
        let packet = UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf_with_permit(output, permit),
            addr: remote_addr,
        };

        match replacement {
            ReplacementSubmit::Submitted { request_id, .. } => {
                let reservation = reservation.expect("validated replacement reservation");
                let request = ReceiveRequest {
                    request_id,
                    slot_id: slot.slot_id(),
                    slot_generation: reservation.slot_generation(),
                };
                slot.set_current_request(Some(request));
                slot.set_state(ReceiveSlotState::InFlight);
                slot.set_completed_addr(None);
                slot.set_completed_len(0);
                self.actual_in_flight += 1;
                self.stats.receive_requests_submitted += 1;
                self.stats.receive_requests_rearmed += 1;
                self.ready_packets += 1;
                self.resume_if_possible();
                Ok(ReceiveRecord {
                    request,
                    key,
                    packet,
                    continuation: CompletionContinuation::More,
                })
            }
            ReplacementSubmit::Failed { .. } => {
                slot.set_state(ReceiveSlotState::Retiring);
                self.set_terminal(ReceivePumpError::ReplacementSubmitFailed);
                let final_now =
                    self.actual_in_flight == 0 && self.pending.is_empty() && self.held.is_empty();
                self.final_emitted = final_now;
                self.ready_packets += 1;
                Ok(ReceiveRecord {
                    request,
                    key,
                    packet,
                    continuation: if final_now {
                        CompletionContinuation::Final
                    } else {
                        CompletionContinuation::More
                    },
                })
            }
            ReplacementSubmit::NotSubmitted => {
                slot.set_state(ReceiveSlotState::Retiring);
                let final_now =
                    self.actual_in_flight == 0 && self.pending.is_empty() && self.held.is_empty();
                self.final_emitted = final_now;
                self.ready_packets += 1;
                Ok(ReceiveRecord {
                    request,
                    key,
                    packet,
                    continuation: if final_now {
                        CompletionContinuation::Final
                    } else {
                        CompletionContinuation::More
                    },
                })
            }
        }
    }

    /// Return an output allocation to the pending queue after a copy/allocation failure.
    pub fn abort_delivery(
        &mut self,
        delivery: ReceiveDelivery,
        error: ReceivePumpError,
    ) -> Result<(), ReceivePumpError> {
        let request = delivery.request();
        if self.prepared != Some(request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.prepared = None;
        let Some(index) = self.slot_index(request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        self.slots[index].set_state(ReceiveSlotState::CompletedPending);
        self.pending.push_front(PendingCompletion {
            request,
            key: delivery.key(),
            len: delivery.len(),
            remote_addr: delivery.remote_addr(),
        });
        drop(delivery);
        if matches!(error, ReceivePumpError::ReceiveQueueStalled) {
            self.pause_for_backpressure();
        } else {
            self.set_terminal(error);
        }
        Ok(())
    }

    pub fn record_consumed(&mut self) -> Result<(), ReceivePumpError> {
        if self.ready_packets == 0 {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.ready_packets -= 1;
        self.promote_held();
        self.resume_if_possible();
        self.maybe_drained();
        Ok(())
    }

    /// Request cancellation.  The pump remains alive until every actual request has completed.
    pub fn cancel(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if self.phase == ReceivePumpPhase::Drained {
            return Ok(ReceivePumpEvent::Ignored);
        }
        self.phase = ReceivePumpPhase::Closing;
        self.set_terminal(ReceivePumpError::ReceiveCancelled);
        self.retire_pending();
        self.prepared = None;
        self.emit_final_or_drain()
    }

    pub fn acknowledge_final(&mut self) -> Result<(), ReceivePumpError> {
        if !self.final_emitted {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.maybe_drained();
        Ok(())
    }

    /// Poll the terminal transition after previously admitted packets have been delivered.
    pub fn poll_final(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        self.emit_final_or_drain()
    }

    fn slot_index(&self, slot_id: u32) -> Option<usize> {
        PumpLifecycle::slot_index(&self.slots, slot_id)
    }

    fn oldest_pending_event(&self) -> Option<ReceivePumpEvent> {
        PumpLifecycle::oldest_pending_event(&self.pending)
    }

    fn promote_held(&mut self) -> usize {
        let mut promoted = 0;
        while self.pending.len() < self.config.pending_capacity() {
            let Some(held) = self.held.pop_front() else {
                break;
            };
            let Some(index) = self.slot_index(held.request.slot_id) else {
                self.stats.receive_context_anomaly += 1;
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                break;
            };
            let slot = &mut self.slots[index];
            if slot.state() != ReceiveSlotState::LeaseHeld
                || slot.generation() != held.request.slot_generation
            {
                self.stats.receive_context_anomaly += 1;
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                break;
            }
            slot.set_state(ReceiveSlotState::CompletedPending);
            self.pending.push_back(held);
            promoted += 1;
        }
        promoted
    }

    fn validate_submission(
        &self,
        submission: ReceiveSubmission,
    ) -> Result<usize, ReceivePumpError> {
        PumpLifecycle::validate_submission(&self.slots, submission)
    }

    fn try_enter_running(&mut self) {
        PumpLifecycle::try_enter_running(
            &mut self.phase,
            self.actual_in_flight,
            self.config.kernel_capacity().get(),
            &self.slots,
        );
    }

    fn pause_for_backpressure(&mut self) {
        PumpLifecycle::pause_for_backpressure(&mut self.phase, &mut self.stats);
    }

    fn resume_if_possible(&mut self) {
        PumpLifecycle::resume_if_possible(&mut self.phase, &self.permits);
    }

    fn set_terminal(&mut self, error: ReceivePumpError) {
        PumpLifecycle::set_terminal(&mut self.phase, &mut self.terminal_error, error);
    }

    fn retire_pending(&mut self) {
        PumpLifecycle::retire_pending(&mut self.slots, &mut self.pending, &mut self.held);
    }

    fn emit_final_if_possible(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        PumpLifecycle::emit_final_if_possible(FinalizationContext {
            final_emitted: &mut self.final_emitted,
            terminal_error: self.terminal_error,
            actual_in_flight: self.actual_in_flight,
            pending_count: self.pending.len(),
            held_count: self.held.len(),
            datagram_count: self.datagrams.pending_count(),
            has_prepared: self.prepared.is_some(),
        })
    }

    fn emit_final_or_drain(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        PumpLifecycle::emit_final_or_drain(
            FinalizationContext {
                final_emitted: &mut self.final_emitted,
                terminal_error: self.terminal_error,
                actual_in_flight: self.actual_in_flight,
                pending_count: self.pending.len(),
                held_count: self.held.len(),
                datagram_count: self.datagrams.pending_count(),
                has_prepared: self.prepared.is_some(),
            },
            self.phase,
        )
    }

    fn maybe_drained(&mut self) {
        PumpLifecycle::maybe_drained(DrainContext {
            phase: &mut self.phase,
            actual_in_flight: self.actual_in_flight,
            pending_count: self.pending.len(),
            held_count: self.held.len(),
            datagram_count: self.datagrams.pending_count(),
            has_prepared: self.prepared.is_some(),
            final_emitted: self.final_emitted,
            slots: &mut self.slots,
        });
    }

    fn debug_assert_invariants(&self) {
        PumpLifecycle::debug_assert_invariants(InvariantContext {
            multishot: self.datagrams.enabled(),
            actual_in_flight: self.actual_in_flight,
            target_depth: self.config.kernel_capacity().get(),
            slots: &self.slots,
        });
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::driver::CompletionContinuation;
    use veloq_std::{
        net::SocketAddr,
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
        vec,
    };

    fn config(depth: usize, queue: usize) -> ReceivePumpConfig {
        ReceivePumpConfig::new(
            NonZeroUsize::new(depth).expect("depth must be non-zero"),
            NonZeroUsize::new(queue).expect("queue must be non-zero"),
            NonZeroUsize::new(1024).expect("capacity must be non-zero"),
            Duration::from_secs(1),
        )
    }

    fn pump(depth: usize, queue: usize) -> ReceivePumpState {
        let slots = (0..depth)
            .map(|slot_id| {
                ReceiveSlot::new(
                    slot_id as u32,
                    FixedBuf::alloc_heap(
                        NonZeroUsize::new(1024).expect("capacity must be non-zero"),
                        1024,
                    )
                    .expect("test buffer allocation should succeed"),
                )
            })
            .collect::<vec::Vec<_>>()
            .into_boxed_slice();
        let mut pump = ReceivePumpState::try_new(config(depth, queue), slots)
            .expect("test pump should be valid");
        pump.start().expect("pump should start");
        for _ in 0..depth {
            let submission = pump.next_submission().expect("slot should be selected");
            pump.submit_initial(submission, submission.slot_id as u64 + 1)
                .expect("initial submit should succeed");
        }
        assert!(pump.is_ready());
        pump
    }

    fn completion(request: ReceiveRequest, len: usize) -> ReceiveCompletion {
        ReceiveCompletion {
            request,
            result: Ok(len),
            remote_addr: "127.0.0.1:9000"
                .parse::<SocketAddr>()
                .expect("valid address"),
        }
    }

    fn failed_completion(request: ReceiveRequest, error: ReceivePumpError) -> ReceiveCompletion {
        ReceiveCompletion {
            request,
            result: Err(error),
            remote_addr: "127.0.0.1:9000"
                .parse::<SocketAddr>()
                .expect("valid address"),
        }
    }

    fn output(len: usize) -> FixedBuf {
        FixedBuf::alloc_heap(
            NonZeroUsize::new(len.max(1)).expect("capacity must be non-zero"),
            0,
        )
        .expect("test output allocation should succeed")
    }

    #[test]
    fn multishot_admission_preserves_fifo_while_permit_is_held() {
        let mut pump = ReceivePumpState::try_new_multishot(config(2, 1), None)
            .expect("multishot pump should be valid");
        pump.arm_multishot().expect("multishot should arm");
        let first_addr = "127.0.0.1:9001".parse().expect("valid address");
        let second_addr = "127.0.0.1:9002".parse().expect("valid address");
        let first = pump
            .admit_datagram(3, first_addr)
            .expect("first datagram should be admitted");
        let first_delivery = pump.next_datagram_delivery().expect("first permit");
        assert_eq!(first_delivery.key(), first.key());
        let first_packet = pump
            .finish_datagram_delivery(first_delivery, output(3))
            .expect("first packet should finish");

        let second = pump
            .admit_datagram(4, second_addr)
            .expect("second datagram should be retained");
        assert_eq!(pump.phase(), ReceivePumpPhase::PausedByBackpressure);
        assert_eq!(pump.pending_datagrams(), 1);
        assert!(pump.next_datagram_delivery().is_none());

        drop(first_packet);
        let second_delivery = pump.next_datagram_delivery().expect("released permit");
        assert_eq!(second_delivery.key(), second.key());
        assert_eq!(second_delivery.remote_addr(), second_addr);
        let second_packet = pump
            .finish_datagram_delivery(second_delivery, output(4))
            .expect("second packet should finish");
        assert_eq!(second_packet.buf.len(), 4);
        drop(second_packet);
    }

    #[test]
    fn multishot_permit_release_notifies_without_touching_pump_state() {
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notifications);
        let notifier: Arc<dyn ReceivePermitNotifier> = unsafe {
            Arc::new_unsized(
                move || {
                    observed.fetch_add(1, Ordering::Relaxed);
                },
                |p| p as *const dyn ReceivePermitNotifier,
            )
        };
        let mut pump = ReceivePumpState::try_new_multishot(config(1, 1), Some(notifier))
            .expect("multishot pump should be valid");
        pump.arm_multishot().expect("multishot should arm");
        pump.admit_datagram(2, "127.0.0.1:9001".parse().expect("valid address"))
            .expect("datagram should be admitted");
        let delivery = pump.next_datagram_delivery().expect("permit");
        let packet = pump
            .finish_datagram_delivery(delivery, output(2))
            .expect("packet should finish");
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
        drop(packet);
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn depth_one_completion_always_rearms_before_more_is_published() {
        let mut pump = pump(1, 2);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        assert!(matches!(
            pump.complete(completion(request, 4)),
            Ok(ReceivePumpEvent::Pending { .. })
        ));
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should succeed");
        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.stats().receive_requests_rearmed, 1);
    }

    #[test]
    fn initial_ready_waits_for_every_successful_submit() {
        let mut pump = pump_without_arm(3, 3);
        pump.start().expect("pump should start");
        for id in 0..2 {
            let submission = pump.next_submission().expect("slot should be selected");
            pump.submit_initial(submission, id + 1)
                .expect("initial submit should succeed");
            assert!(!pump.is_ready(), "depth is not complete after {id} submits");
        }
        let submission = pump
            .next_submission()
            .expect("last slot should be selected");
        pump.submit_initial(submission, 3)
            .expect("initial submit should succeed");
        assert!(pump.is_ready());
        assert_eq!(pump.actual_in_flight(), 3);
    }

    #[test]
    fn permit_exhaustion_keeps_completion_pending_without_publish_or_rearm() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        let first = pump
            .complete(completion(request, 3))
            .expect("completion should route");
        let delivery = pump
            .next_delivery()
            .expect("first permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(3),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should succeed");
        assert_eq!(record.continuation, CompletionContinuation::More);
        let held_record = record.packet;
        assert_eq!(pump.permits_available(), 0);

        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("rearmed");
        assert!(matches!(first, ReceivePumpEvent::Pending { .. }));
        assert!(matches!(
            pump.complete(completion(request, 5)),
            Ok(ReceivePumpEvent::Pending { .. })
        ));
        assert_eq!(pump.pending_completions(), 1);
        assert_eq!(pump.stats().receive_requests_rearmed, 1);
        assert!(pump.next_delivery().is_none());
        drop(held_record);
        let delivery = pump.next_delivery().expect("released permit should resume");
        assert_eq!(delivery.len(), 5);
    }

    #[test]
    fn replacement_failure_publishes_current_packet_then_exposes_terminal_error() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(request, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Failed { reservation },
            )
            .expect("current packet must be delivered");
        assert_eq!(record.continuation, CompletionContinuation::Final);
        assert_eq!(
            pump.terminal_error(),
            Some(ReceivePumpError::ReplacementSubmitFailed)
        );
        assert_eq!(pump.ready_packets(), 1);
        pump.record_consumed().expect("record should be consumed");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn one_request_error_keeps_the_logical_slot_until_all_requests_drain() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");

        assert_eq!(
            pump.complete(failed_completion(
                first,
                ReceivePumpError::ReceiveOsError { code: 10035 },
            ))
            .expect("request error should enter drain"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.phase(), ReceivePumpPhase::RunningWithTerminalError);

        assert_eq!(
            pump.complete(completion(second, 8))
                .expect("remaining request should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveOsError { code: 10035 }
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
    }

    #[test]
    fn terminal_error_drains_admitted_packets_without_replacement() {
        let mut pump = pump(3, 3);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");
        let third = pump
            .slot(2)
            .expect("third slot exists")
            .current_request()
            .expect("third request is in flight");

        pump.complete(completion(first, 4))
            .expect("first packet should be admitted");
        pump.complete(completion(second, 5))
            .expect("second packet should be admitted");
        assert!(matches!(
            pump.complete(failed_completion(
                third,
                ReceivePumpError::ReceiveOsError { code: 10054 },
            ))
            .expect("terminal request error should preserve pending packets"),
            ReceivePumpEvent::Pending { .. }
        ));

        let first_delivery = pump.next_delivery().expect("first packet should drain");
        assert!(!first_delivery.replacement_required());
        let first_key = first_delivery.key();
        let first_record = pump
            .finish_delivery(first_delivery, output(4), ReplacementSubmit::NotSubmitted)
            .expect("first packet should finish without replacement");
        assert_eq!(first_record.key, first_key);
        assert_eq!(first_record.continuation, CompletionContinuation::More);
        drop(first_record.packet);
        pump.record_consumed()
            .expect("first record should be consumed");

        let second_delivery = pump.next_delivery().expect("second packet should drain");
        assert!(!second_delivery.replacement_required());
        let second_record = pump
            .finish_delivery(second_delivery, output(5), ReplacementSubmit::NotSubmitted)
            .expect("second packet should finish without replacement");
        assert_eq!(second_record.key.completion_sequence(), 2);
        assert_eq!(second_record.continuation, CompletionContinuation::Final);
        drop(second_record.packet);
        pump.record_consumed()
            .expect("second record should be consumed");
        assert_eq!(
            pump.terminal_error(),
            Some(ReceivePumpError::ReceiveOsError { code: 10054 })
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn replacement_failure_drains_when_other_requests_remain() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        pump.complete(completion(first, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Failed { reservation },
            )
            .expect("completed packet should still be delivered");

        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(pump.phase(), ReceivePumpPhase::RunningWithTerminalError);
        assert_eq!(pump.actual_in_flight(), 1);
    }

    #[test]
    fn replacement_reservation_advances_generation_before_submit_commit() {
        let mut pump = pump(1, 2);
        let old = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(old, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");

        assert_eq!(reservation.slot_id(), old.slot_id);
        assert_eq!(reservation.slot_generation(), old.slot_generation + 1);
        assert_eq!(
            pump.slot(0).expect("slot exists").generation(),
            reservation.slot_generation()
        );
        assert!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .is_none()
        );

        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should commit");
        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .expect("replacement should be in flight")
                .slot_generation,
            reservation.slot_generation()
        );
    }

    #[test]
    fn pending_keys_follow_completion_order_and_logical_rearms() {
        let mut pump = pump(1, 2);
        let first_request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(first_request, 4))
            .expect("completion should route");
        let first_delivery = pump.next_delivery().expect("permit should be available");
        assert_eq!(first_delivery.key().logical_receiver_generation(), 0);
        assert_eq!(first_delivery.key().completion_sequence(), 1);
        let first_reservation = pump
            .reserve_replacement(&first_delivery)
            .expect("replacement generation should be reserved");
        let first_record = pump
            .finish_delivery(
                first_delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation: first_reservation,
                },
            )
            .expect("first delivery should finish");
        let first_key = first_record.key;
        drop(first_record.packet);
        pump.record_consumed()
            .expect("first record should be consumed");

        assert_eq!(
            pump.retain_for_rearm().expect("rearm should be retained"),
            ReceivePumpEvent::Retained {
                logical_receiver_generation: 1
            }
        );
        pump.submit_rearm().expect("logical rearm should commit");
        let second_request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("request remains owned by the logical receiver");
        pump.complete(completion(second_request, 5))
            .expect("second completion should route");
        let second_delivery = pump
            .next_delivery()
            .expect("second permit should be available");
        assert_eq!(first_key.logical_receiver_generation(), 0);
        assert_eq!(second_delivery.key().logical_receiver_generation(), 1);
        assert_eq!(second_delivery.key().completion_sequence(), 2);
    }

    #[test]
    fn stale_generation_cannot_overwrite_a_rearmed_slot() {
        let mut pump = pump(1, 2);
        let old = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(old, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        pump.finish_delivery(
            delivery,
            output(4),
            ReplacementSubmit::Submitted {
                request_id: 2,
                reservation,
            },
        )
        .expect("replacement should succeed");
        let current = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("rearmed");
        assert_eq!(current.slot_generation, old.slot_generation + 1);
        assert_eq!(
            pump.complete(completion(old, 8)),
            Err(ReceivePumpError::ReceiveContextCorrupt)
        );
        assert_eq!(
            pump.slot(0).expect("slot exists").current_request(),
            Some(current)
        );
        assert_eq!(pump.actual_in_flight(), 1);
    }

    #[test]
    fn cancel_mid_stream_emits_final_only_after_the_actual_request_completes() {
        let mut pump = pump(1, 1);
        assert_eq!(
            pump.cancel().expect("cancel should be accepted"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Closing);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        assert_eq!(
            pump.complete(completion(request, 4))
                .expect("completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveCancelled
            }
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Closing);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn cancel_discards_every_closing_completion_until_final() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");

        assert_eq!(
            pump.cancel().expect("cancel should be accepted"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.actual_in_flight(), 2);
        assert_eq!(pump.pending_completions(), 0);
        assert_eq!(pump.held_completions(), 0);

        assert_eq!(
            pump.complete(completion(first, 4))
                .expect("first completion should drain"),
            ReceivePumpEvent::Drain
        );
        let first_slot = pump.slot(0).expect("first slot exists");
        assert_eq!(first_slot.state(), ReceiveSlotState::Retiring);
        assert_eq!(first_slot.completed_len(), None);
        assert_eq!(first_slot.completed_data(), None);
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.pending_completions(), 0);

        assert_eq!(
            pump.complete(completion(second, 8))
                .expect("second completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveCancelled
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.pending_completions(), 0);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn orphaned_more_does_not_take_the_persistent_submit_payload() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.cancel().expect("cancel should be accepted");
        let event = pump
            .complete(completion(request, 4))
            .expect("completion should drain");
        assert!(matches!(event, ReceivePumpEvent::Final { .. }));
        assert!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .is_none()
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.stats().receive_requests_completed, 1);
    }

    #[test]
    fn failed_submit_and_completion_keep_request_counters_balanced() {
        let mut pump = pump_without_arm(2, 2);
        pump.start().expect("pump should start");
        let first = pump
            .next_submission()
            .expect("first slot should be selected");
        pump.submit_initial(first, 1)
            .expect("first submit should succeed");
        let second = pump
            .next_submission()
            .expect("second slot should be selected");
        assert_eq!(
            pump.fail_initial_submit(second)
                .expect("failure should enter closing"),
            ReceivePumpEvent::Ignored
        );
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("first request remains");
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.stats().receive_requests_submitted, 1);
        assert_eq!(
            pump.complete(completion(request, 4))
                .expect("completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::InitialReceiveSubmitFailed
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.stats().receive_requests_completed, 1);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    fn pump_without_arm(depth: usize, queue: usize) -> ReceivePumpState {
        let slots = (0..depth)
            .map(|slot_id| {
                ReceiveSlot::new(
                    slot_id as u32,
                    FixedBuf::alloc_heap(
                        NonZeroUsize::new(1024).expect("capacity must be non-zero"),
                        1024,
                    )
                    .expect("test buffer allocation should succeed"),
                )
            })
            .collect::<vec::Vec<_>>()
            .into_boxed_slice();
        ReceivePumpState::try_new(config(depth, queue), slots).expect("test pump should be valid")
    }
}
