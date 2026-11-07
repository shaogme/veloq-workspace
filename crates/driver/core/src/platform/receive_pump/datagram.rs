use crate::op::{UdpRecvPacket, UdpRecvPacketBuf};
use veloq_buf::FixedBuf;
use veloq_std::{collections::VecDeque, net::SocketAddr};

use super::{
    ReceivePendingKey, ReceivePermit, ReceivePermitPool, ReceivePumpError, ReceivePumpPhase,
    ReceivePumpStats, ReceiveRequest, config::ReceivePumpConfig, types::PendingCompletion,
};

/// An io_uring multishot datagram admission identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramAdmission {
    key: ReceivePendingKey,
}

impl DatagramAdmission {
    pub const fn key(self) -> ReceivePendingKey {
        self.key
    }
}

/// A datagram that owns one permit while the backend copies it into an output buffer.
pub struct DatagramDelivery {
    key: ReceivePendingKey,
    len: usize,
    remote_addr: SocketAddr,
    permit: Option<ReceivePermit>,
}

impl DatagramDelivery {
    pub const fn key(&self) -> ReceivePendingKey {
        self.key
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}

pub(super) struct DatagramQueue {
    enabled: bool,
    generation: u32,
    next_sequence: u64,
    pending: VecDeque<PendingCompletion>,
}

pub(super) struct DatagramQueueContext<'a> {
    config: ReceivePumpConfig,
    phase: &'a mut ReceivePumpPhase,
    stats: &'a mut ReceivePumpStats,
    permits: &'a ReceivePermitPool,
}

impl<'a> DatagramQueueContext<'a> {
    pub(super) fn new(
        config: ReceivePumpConfig,
        phase: &'a mut ReceivePumpPhase,
        stats: &'a mut ReceivePumpStats,
        permits: &'a ReceivePermitPool,
    ) -> Self {
        Self {
            config,
            phase,
            stats,
            permits,
        }
    }

    fn pause_for_backpressure(&mut self) {
        if *self.phase != ReceivePumpPhase::PausedByBackpressure {
            self.stats.receive_backpressure_events += 1;
        }
        self.stats.receive_permit_exhausted += 1;
        *self.phase = ReceivePumpPhase::PausedByBackpressure;
    }

    fn resume_if_possible(&mut self) {
        if *self.phase == ReceivePumpPhase::PausedByBackpressure && self.permits.available() > 0 {
            *self.phase = ReceivePumpPhase::Running;
        }
    }
}

impl DatagramQueue {
    pub(super) fn new(capacity: usize, enabled: bool) -> Result<Self, ReceivePumpError> {
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(capacity)
            .map_err(|_| ReceivePumpError::ReceiveConfigInvalid)?;
        Ok(Self {
            enabled,
            generation: 0,
            next_sequence: 0,
            pending,
        })
    }

    pub(super) const fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) const fn generation(&self) -> u32 {
        self.generation
    }

    pub(super) fn advance_generation(&mut self) -> Result<u32, ReceivePumpError> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(ReceivePumpError::ReceiveContextCorrupt)?;
        self.generation = generation;
        Ok(generation)
    }

    pub(super) fn next_key(&mut self) -> Result<ReceivePendingKey, ReceivePumpError> {
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ReceivePumpError::ReceiveContextCorrupt)?;
        Ok(ReceivePendingKey::new(self.generation, self.next_sequence))
    }

    pub(super) fn arm(&mut self, phase: &mut ReceivePumpPhase) -> Result<u32, ReceivePumpError> {
        if !self.enabled || *phase != ReceivePumpPhase::Created {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let generation = self.advance_generation()?;
        *phase = ReceivePumpPhase::Running;
        Ok(generation)
    }

    pub(super) fn admit(
        &mut self,
        mut context: DatagramQueueContext<'_>,
        len: usize,
        remote_addr: SocketAddr,
    ) -> Result<DatagramAdmission, ReceivePumpError> {
        if !self.enabled
            || !matches!(
                *context.phase,
                ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
            )
        {
            return Err(ReceivePumpError::InvalidTransition);
        }
        if len > context.config.datagram_capacity().get()
            || self.pending.len() >= context.config.pending_capacity()
        {
            context.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        let key = self.next_key()?;
        self.pending.push_back(PendingCompletion {
            request: ReceiveRequest {
                request_id: key.completion_sequence(),
                slot_id: u32::MAX,
                slot_generation: key.logical_receiver_generation(),
            },
            key,
            len,
            remote_addr,
        });
        if context.permits.available() == 0 {
            context.pause_for_backpressure();
        }
        Ok(DatagramAdmission { key })
    }

    pub(super) fn next_delivery(
        &mut self,
        mut context: DatagramQueueContext<'_>,
    ) -> Option<DatagramDelivery> {
        if !self.enabled
            || !matches!(
                *context.phase,
                ReceivePumpPhase::Running
                    | ReceivePumpPhase::PausedByBackpressure
                    | ReceivePumpPhase::RearmPending
            )
            || self.pending.is_empty()
        {
            return None;
        }
        let permit = match context.permits.acquire() {
            Some(permit) => permit,
            None => {
                context.pause_for_backpressure();
                return None;
            }
        };
        let pending = self.pending.pop_front()?;
        context.resume_if_possible();
        Some(DatagramDelivery {
            key: pending.key,
            len: pending.len,
            remote_addr: pending.remote_addr,
            permit: Some(permit),
        })
    }

    pub(super) fn finish_delivery(
        &mut self,
        mut context: DatagramQueueContext<'_>,
        mut delivery: DatagramDelivery,
        mut output: FixedBuf,
    ) -> Result<UdpRecvPacket, ReceivePumpError> {
        if output.capacity() < delivery.len {
            self.requeue(delivery.key, delivery.len, delivery.remote_addr);
            drop(delivery);
            context.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        output.set_len(delivery.len);
        let permit = delivery
            .permit
            .take()
            .ok_or(ReceivePumpError::InvalidTransition)?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf_with_permit(output, permit),
            addr: delivery.remote_addr,
        })
    }

    pub(super) fn abort_delivery(
        &mut self,
        delivery: DatagramDelivery,
    ) -> Result<(), ReceivePumpError> {
        self.requeue(delivery.key, delivery.len, delivery.remote_addr);
        drop(delivery);
        Ok(())
    }

    pub(super) fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn rearm_pending(&self, phase: ReceivePumpPhase) -> bool {
        self.enabled && phase == ReceivePumpPhase::RearmPending
    }

    pub(super) fn clear_pending(&mut self) -> usize {
        let count = self.pending.len();
        self.pending.clear();
        count
    }

    fn requeue(&mut self, key: ReceivePendingKey, len: usize, remote_addr: SocketAddr) {
        self.pending.push_front(PendingCompletion {
            request: ReceiveRequest {
                request_id: key.completion_sequence(),
                slot_id: u32::MAX,
                slot_generation: key.logical_receiver_generation(),
            },
            key,
            len,
            remote_addr,
        });
    }
}
