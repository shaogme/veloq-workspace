use tracing::trace;
use veloq::{
    buf::FixedBuf,
    std::{
        collections::{HashMap, VecDeque},
        num::NonZeroUsize,
        vec,
        vec::Vec,
    },
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{
        AckObserve, AckWindow, ConnectionId, FrameSequence, MessageId, PacketBufAllocator,
        PacketMeta, non_zero_sequence,
    },
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
    message_acks: Vec<MessageId>,
    arm_reassembly: Vec<MessageId>,
    cancel_reassembly: Vec<MessageId>,
}

impl InboundOutput {
    pub(super) fn ack(&self) -> AckDecision {
        self.decision
    }

    pub(super) fn delivered(&self) -> usize {
        self.delivered
    }

    pub(super) fn message_acks(&self) -> &[MessageId] {
        &self.message_acks
    }

    pub(super) fn arm_reassembly(&self) -> &[MessageId] {
        &self.arm_reassembly
    }

    pub(super) fn cancel_reassembly(&self) -> &[MessageId] {
        &self.cancel_reassembly
    }
}

struct InboundFragment {
    datagram: FixedBuf,
    meta: PacketMeta,
}

struct ReassemblyEntry {
    message_len: usize,
    fragment_count: u32,
    buffer: FixedBuf,
    received_bitmap: Vec<u64>,
    received_fragments: u32,
    received_bytes: usize,
    complete_pending_delivery: bool,
}

pub(super) struct InboundState {
    recv_ack: AckWindow,
    recv_reorder: HashMap<FrameSequence, InboundFragment>,
    reassembly: HashMap<MessageId, ReassemblyEntry>,
    recv_ready: VecDeque<Message>,
    recently_completed: VecDeque<MessageId>,
    next_deliver_frame: FrameSequence,
    next_deliver_message: MessageId,
    reserved_reassembly_bytes: usize,
    buffered_inbound_bytes: usize,
    pending_ack_packets: usize,
}

impl InboundState {
    pub(super) fn new() -> Self {
        Self {
            recv_ack: AckWindow::new(),
            recv_reorder: HashMap::default(),
            reassembly: HashMap::default(),
            recv_ready: VecDeque::new(),
            recently_completed: VecDeque::new(),
            next_deliver_frame: FrameSequence::new(1).expect("one is a valid frame sequence"),
            next_deliver_message: MessageId::new(1).expect("one is a valid message ID"),
            reserved_reassembly_bytes: 0,
            buffered_inbound_bytes: 0,
            pending_ack_packets: 0,
        }
    }

    pub(super) fn on_data<A: PacketBufAllocator + ?Sized>(
        &mut self,
        datagram: FixedBuf,
        packet: PacketMeta,
        config: &Config,
        connection_id: ConnectionId,
        allocator: &A,
        metrics: &mut SessionMetrics,
    ) -> Result<InboundOutput> {
        let sequence = non_zero_sequence(packet.frame_sequence)?;
        let message_id = MessageId::new(packet.message_id).ok_or(Error::InvalidFragment)?;
        if self.recv_ack.contains(sequence) || self.recv_reorder.contains_key(&sequence) {
            metrics.record_duplicate();
            let message_acks = self
                .recently_completed
                .contains(&message_id)
                .then_some(message_id)
                .into_iter()
                .collect();
            return Ok(InboundOutput {
                decision: AckDecision::Immediate,
                delivered: 0,
                message_acks,
                arm_reassembly: Vec::new(),
                cancel_reassembly: Vec::new(),
            });
        }
        if self.recently_completed.contains(&message_id) {
            metrics.record_duplicate_fragment();
            return Ok(InboundOutput {
                decision: AckDecision::Immediate,
                delivered: 0,
                message_acks: vec![message_id],
                arm_reassembly: Vec::new(),
                cancel_reassembly: Vec::new(),
            });
        }
        let Some(distance) = FrameSequence::forward_distance(self.next_deliver_frame, sequence)
        else {
            metrics.record_drop();
            return Ok(Self::empty_output());
        };
        if distance >= config.receive_window.get() as u64 {
            metrics.record_receive_window_drop();
            return Ok(Self::empty_output());
        }
        if self.recv_reorder.len() >= config.receive_window.get() {
            metrics.record_receive_window_drop();
            return Ok(Self::empty_output());
        }

        let message_len =
            usize::try_from(packet.message_len).map_err(|_| Error::InvalidFragment)?;
        let mut arm_reassembly = Vec::new();
        let mut created_reassembly = false;
        if let Some(entry) = self.reassembly.get(&message_id) {
            if entry.message_len != message_len || entry.fragment_count != packet.fragment_count {
                metrics.record_drop();
                return Ok(Self::empty_output());
            }
        } else {
            if self.reassembly.len() >= config.max_reassembly_messages.get() {
                metrics.record_reassembly_limit();
                return Ok(Self::empty_output());
            }
            let reserved = self
                .reserved_reassembly_bytes
                .checked_add(message_len)
                .ok_or(Error::ReassemblyLimit)?;
            if reserved > config.max_reassembly_bytes.get() {
                metrics.record_reassembly_limit();
                return Ok(Self::empty_output());
            }
            let bitmap_len = usize::try_from(packet.fragment_count)
                .ok()
                .and_then(|count| count.checked_add(63))
                .map(|count| count / 64)
                .ok_or(Error::InvalidFragment)?;
            let capacity = NonZeroUsize::new(message_len.max(1)).ok_or(Error::ReassemblyLimit)?;
            let buffer = allocator
                .alloc_packet_buf(capacity, message_len)
                .map_err(Error::from)?;
            self.reassembly.insert(
                message_id,
                ReassemblyEntry {
                    message_len,
                    fragment_count: packet.fragment_count,
                    buffer,
                    received_bitmap: vec![0; bitmap_len],
                    received_fragments: 0,
                    received_bytes: 0,
                    complete_pending_delivery: false,
                },
            );
            self.reserved_reassembly_bytes = reserved;
            arm_reassembly.push(message_id);
            created_reassembly = true;
        }

        let observation = self.recv_ack.observe(sequence);
        if matches!(observation, AckObserve::Duplicate | AckObserve::TooOld) {
            if created_reassembly && let Some(entry) = self.reassembly.remove(&message_id) {
                self.reserved_reassembly_bytes = self
                    .reserved_reassembly_bytes
                    .saturating_sub(entry.message_len);
            }
            metrics.record_duplicate();
            return Ok(InboundOutput {
                decision: AckDecision::Immediate,
                delivered: 0,
                message_acks: Vec::new(),
                arm_reassembly: Vec::new(),
                cancel_reassembly: Vec::new(),
            });
        }
        self.recv_reorder.insert(
            sequence,
            InboundFragment {
                datagram,
                meta: packet,
            },
        );
        let mut output = InboundOutput {
            decision: AckDecision::Immediate,
            delivered: 0,
            message_acks: Vec::new(),
            arm_reassembly,
            cancel_reassembly: Vec::new(),
        };
        self.process_contiguous(config, metrics, &mut output);
        output.delivered = output.message_acks.len();
        self.pending_ack_packets = self.pending_ack_packets.saturating_add(1);
        let distance = FrameSequence::forward_distance(self.next_deliver_frame, sequence);
        if distance == Some(0)
            && output.delivered > 0
            && self.pending_ack_packets < config.ack_batch_size.get()
        {
            output.decision = AckDecision::Delayed;
        }
        if !output.message_acks.is_empty() {
            output.decision = AckDecision::None;
        }
        trace!(
            target: "veloq_reliable_udp::session",
            connection_id = connection_id.get(),
            frame_sequence = sequence.get(),
            message_id = message_id.get(),
            observation = ?observation,
            delivered = output.delivered,
            "session accepted data fragment"
        );
        Ok(output)
    }

    pub(super) fn pop_message(
        &mut self,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> (Option<Message>, Vec<MessageId>, Vec<MessageId>) {
        let message = self.recv_ready.pop_front();
        if let Some(message) = &message {
            self.buffered_inbound_bytes = self
                .buffered_inbound_bytes
                .saturating_sub(message.payload.len());
        }
        let mut output = InboundOutput {
            decision: AckDecision::None,
            delivered: 0,
            message_acks: Vec::new(),
            arm_reassembly: Vec::new(),
            cancel_reassembly: Vec::new(),
        };
        self.promote_ready(config, metrics, &mut output);
        (message, output.message_acks, output.cancel_reassembly)
    }

    pub(super) fn buffered_messages(&self) -> usize {
        self.recv_ready.len() + self.recv_reorder.len() + self.reassembly.len()
    }

    pub(super) fn reassembly_messages(&self) -> usize {
        self.reassembly.len()
    }

    pub(super) fn reassembly_bytes(&self) -> usize {
        self.reserved_reassembly_bytes
    }

    pub(super) fn available_window(&self, config: &Config) -> usize {
        config
            .receive_window
            .get()
            .saturating_sub(self.recv_ready.len() + self.recv_reorder.len())
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

    pub(super) fn on_reassembly_timeout(
        &mut self,
        message_id: MessageId,
        metrics: &mut SessionMetrics,
    ) -> bool {
        let Some(entry) = self.reassembly.remove(&message_id) else {
            return false;
        };
        self.reserved_reassembly_bytes = self
            .reserved_reassembly_bytes
            .saturating_sub(entry.message_len);
        metrics.record_reassembly_timeout();
        true
    }

    pub(super) fn clear(&mut self) -> Vec<MessageId> {
        let ids = self.reassembly.keys().copied().collect();
        self.recv_reorder.clear();
        self.reassembly.clear();
        self.recv_ready.clear();
        self.recently_completed.clear();
        self.reserved_reassembly_bytes = 0;
        self.buffered_inbound_bytes = 0;
        ids
    }

    fn process_contiguous(
        &mut self,
        config: &Config,
        metrics: &mut SessionMetrics,
        output: &mut InboundOutput,
    ) {
        while let Some(fragment) = self.recv_reorder.remove(&self.next_deliver_frame) {
            let sequence = self.next_deliver_frame;
            self.next_deliver_frame = sequence.next();
            let message_id = MessageId::new(fragment.meta.message_id)
                .expect("validated data metadata has a message ID");
            let Some(entry) = self.reassembly.get_mut(&message_id) else {
                metrics.record_drop();
                continue;
            };
            let index = usize::try_from(fragment.meta.fragment_index).unwrap_or(usize::MAX);
            let max_fragment_payload = config.max_fragment_payload();
            let Some(offset) = index.checked_mul(max_fragment_payload) else {
                metrics.record_drop();
                continue;
            };
            let payload = &fragment.datagram.as_slice()[fragment.meta.payload_range];
            let Some(end) = offset.checked_add(payload.len()) else {
                metrics.record_drop();
                continue;
            };
            if end > entry.message_len {
                metrics.record_drop();
                continue;
            }
            let bitmap_index = index / 64;
            let bitmap_bit = 1u64 << (index % 64);
            if entry.received_bitmap[bitmap_index] & bitmap_bit != 0 {
                metrics.record_duplicate_fragment();
                continue;
            }
            entry.buffer.as_slice_mut()[offset..end].copy_from_slice(payload);
            entry.received_bitmap[bitmap_index] |= bitmap_bit;
            entry.received_fragments = entry.received_fragments.saturating_add(1);
            entry.received_bytes = entry.received_bytes.saturating_add(payload.len());
            if entry.received_fragments == entry.fragment_count
                && entry.received_bytes == entry.message_len
            {
                entry.complete_pending_delivery = true;
                self.promote_ready(config, metrics, output);
            }
        }
        self.promote_ready(config, metrics, output);
    }

    fn promote_ready(
        &mut self,
        config: &Config,
        metrics: &mut SessionMetrics,
        output: &mut InboundOutput,
    ) {
        loop {
            let message_id = self.next_deliver_message;
            let Some(entry) = self.reassembly.get(&message_id) else {
                break;
            };
            if !entry.complete_pending_delivery
                || self.recv_ready.len() >= config.receive_window.get()
                || self
                    .buffered_inbound_bytes
                    .checked_add(entry.message_len)
                    .is_none_or(|bytes| bytes > config.max_inbound_bytes.get())
            {
                break;
            }
            let entry = self
                .reassembly
                .remove(&message_id)
                .expect("reassembly entry was checked above");
            self.reserved_reassembly_bytes = self
                .reserved_reassembly_bytes
                .saturating_sub(entry.message_len);
            self.buffered_inbound_bytes = self
                .buffered_inbound_bytes
                .saturating_add(entry.message_len);
            self.recv_ready.push_back(Message {
                message_id,
                payload: entry.buffer,
            });
            self.recently_completed.push_back(message_id);
            while self.recently_completed.len() > config.receive_window.get().max(64) {
                self.recently_completed.pop_front();
            }
            self.next_deliver_message = message_id.next();
            output.message_acks.push(message_id);
            output.cancel_reassembly.push(message_id);
            metrics.record_completed_message();
        }
    }

    fn empty_output() -> InboundOutput {
        InboundOutput {
            decision: AckDecision::None,
            delivered: 0,
            message_acks: Vec::new(),
            arm_reassembly: Vec::new(),
            cancel_reassembly: Vec::new(),
        }
    }
}
