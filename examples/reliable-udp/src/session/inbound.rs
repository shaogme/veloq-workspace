use tracing::{debug, trace};
use veloq::{
    buf::FixedBuf,
    std::collections::{HashMap, VecDeque},
};

use crate::{
    config::Config,
    error::Result,
    packet::{AckObserve, AckWindow, ConnectionId, MessageSequence, PacketMeta, non_zero_sequence},
};

use super::Message;
use super::metrics::SessionMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AckDecision {
    None,
    Immediate,
    Delayed,
}

pub(super) struct InboundOutput {
    decision: AckDecision,
    delivered: usize,
}

impl InboundOutput {
    pub(super) fn ack(&self) -> AckDecision {
        self.decision
    }

    pub(super) fn delivered(&self) -> usize {
        self.delivered
    }
}

pub(super) struct InboundState {
    recv_ack: AckWindow,
    recv_reorder: HashMap<MessageSequence, FixedBuf>,
    recv_ready: VecDeque<Message>,
    next_deliver_seq: MessageSequence,
    pending_ack_packets: usize,
}

impl InboundState {
    pub(super) fn new() -> Self {
        Self {
            recv_ack: AckWindow::new(),
            recv_reorder: HashMap::default(),
            recv_ready: VecDeque::new(),
            next_deliver_seq: MessageSequence::new(1).expect("one is a valid message sequence"),
            pending_ack_packets: 0,
        }
    }

    pub(super) fn on_data(
        &mut self,
        datagram: FixedBuf,
        packet: PacketMeta,
        config: &Config,
        connection_id: ConnectionId,
        metrics: &mut SessionMetrics,
    ) -> Result<InboundOutput> {
        if packet.payload_range.is_empty() && packet.sequence == 0 {
            return Ok(InboundOutput {
                decision: AckDecision::None,
                delivered: 0,
            });
        }
        let sequence = non_zero_sequence(packet.sequence)?;
        if self.recv_ack.contains(sequence)
            || self.recv_reorder.contains_key(&sequence)
            || self
                .recv_ready
                .iter()
                .any(|message| message.sequence == sequence)
        {
            metrics.record_duplicate();
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = connection_id.get(),
                sequence = sequence.get(),
                "session suppressed duplicate data and emitted ACK"
            );
            return Ok(InboundOutput {
                decision: AckDecision::Immediate,
                delivered: 0,
            });
        }

        let Some(distance) = MessageSequence::forward_distance(self.next_deliver_seq, sequence)
        else {
            metrics.record_drop();
            return Ok(InboundOutput {
                decision: self.available_ack_decision(),
                delivered: 0,
            });
        };
        if distance >= config.receive_window.get() as u64 {
            metrics.record_out_of_window_drop();
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = connection_id.get(),
                sequence = sequence.get(),
                distance,
                "session suppressed data outside receive window"
            );
            return Ok(InboundOutput {
                decision: self.available_ack_decision(),
                delivered: 0,
            });
        }
        if self.buffered_messages() >= config.receive_window.get() {
            metrics.record_receive_window_drop();
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = connection_id.get(),
                sequence = sequence.get(),
                "session suppressed data because receive window is full"
            );
            return Ok(InboundOutput {
                decision: AckDecision::None,
                delivered: 0,
            });
        }

        let observation = self.recv_ack.observe(sequence);
        if matches!(observation, AckObserve::Duplicate | AckObserve::TooOld) {
            metrics.record_duplicate();
            return Ok(InboundOutput {
                decision: self.available_ack_decision(),
                delivered: 0,
            });
        }

        let payload = datagram.into_subbuf(packet.payload_range);
        self.recv_reorder.insert(sequence, payload);
        let delivered = self.deliver_ready(config);
        trace!(
            target: "veloq_reliable_udp::session",
            connection_id = connection_id.get(),
            sequence = sequence.get(),
            distance,
            observation = ?observation,
            delivered,
            "session accepted data"
        );
        self.pending_ack_packets = self.pending_ack_packets.saturating_add(1);
        let decision = if distance == 0
            && delivered > 0
            && self.pending_ack_packets < config.ack_batch_size.get()
        {
            AckDecision::Delayed
        } else {
            AckDecision::Immediate
        };
        Ok(InboundOutput {
            decision,
            delivered,
        })
    }

    pub(super) fn pop_message(&mut self) -> Option<Message> {
        self.recv_ready.pop_front()
    }

    pub(super) fn buffered_messages(&self) -> usize {
        self.recv_ready.len() + self.recv_reorder.len()
    }

    pub(super) fn available_window(&self, config: &Config) -> usize {
        config
            .receive_window
            .get()
            .saturating_sub(self.buffered_messages())
    }

    pub(super) fn ack_snapshot(&self) -> AckWindow {
        self.recv_ack
    }

    pub(super) fn ack_is_empty(&self) -> bool {
        self.recv_ack.is_empty()
    }

    pub(super) fn mark_ack_sent(&mut self) {
        self.pending_ack_packets = 0;
    }

    fn available_ack_decision(&self) -> AckDecision {
        if self.recv_ack.is_empty() {
            AckDecision::None
        } else {
            AckDecision::Immediate
        }
    }

    fn deliver_ready(&mut self, config: &Config) -> usize {
        let mut delivered = 0;
        while let Some(payload) = self.recv_reorder.remove(&self.next_deliver_seq) {
            if self.recv_ready.len() >= config.receive_window.get() {
                self.recv_reorder.insert(self.next_deliver_seq, payload);
                break;
            }
            let sequence = self.next_deliver_seq;
            self.recv_ready.push_back(Message { sequence, payload });
            self.next_deliver_seq = sequence.next();
            delivered += 1;
        }
        delivered
    }
}
