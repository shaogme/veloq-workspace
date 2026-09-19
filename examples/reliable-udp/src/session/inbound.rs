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
        PacketMeta, StreamId, StreamSequence, non_zero_sequence,
    },
};

use super::StreamMessage;
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
    message_acks: Vec<(StreamId, MessageId)>,
    arm_reassembly: Vec<(StreamId, MessageId)>,
    cancel_reassembly: Vec<(StreamId, MessageId)>,
}

impl InboundOutput {
    pub(super) fn ack(&self) -> AckDecision {
        self.decision
    }

    pub(super) fn delivered(&self) -> usize {
        self.delivered
    }

    pub(super) fn message_acks(&self) -> &[(StreamId, MessageId)] {
        &self.message_acks
    }

    pub(super) fn arm_reassembly(&self) -> &[(StreamId, MessageId)] {
        &self.arm_reassembly
    }

    pub(super) fn cancel_reassembly(&self) -> &[(StreamId, MessageId)] {
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
    first_sequence: StreamSequence,
}

struct StreamInboundState {
    recv_reorder: HashMap<StreamSequence, InboundFragment>,
    reassembly: HashMap<MessageId, ReassemblyEntry>,
    recv_ready: VecDeque<StreamMessage>,
    recently_completed: VecDeque<MessageId>,
    next_deliver_sequence: StreamSequence,
    next_deliver_message: MessageId,
    reserved_reassembly_bytes: usize,
    buffered_inbound_bytes: usize,
}

impl StreamInboundState {
    fn new() -> Self {
        Self {
            recv_reorder: HashMap::default(),
            reassembly: HashMap::default(),
            recv_ready: VecDeque::new(),
            recently_completed: VecDeque::new(),
            next_deliver_sequence: StreamSequence::new(1).expect("one is a valid stream sequence"),
            next_deliver_message: MessageId::new(1).expect("one is a valid message ID"),
            reserved_reassembly_bytes: 0,
            buffered_inbound_bytes: 0,
        }
    }
}

pub(super) struct InboundState {
    recv_ack: AckWindow,
    streams: HashMap<StreamId, StreamInboundState>,
    pending_ack_packets: usize,
}

type ReceiveOutput = (
    Option<StreamMessage>,
    Vec<(StreamId, MessageId)>,
    Vec<(StreamId, MessageId)>,
);

impl InboundState {
    pub(super) fn new() -> Self {
        Self {
            recv_ack: AckWindow::new(),
            streams: HashMap::default(),
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
        let total_reassembly_messages = self.total_reassembly_messages();
        let total_reassembly_bytes = self.total_reassembly_bytes();
        let total_buffered_bytes = self.total_buffered_bytes();
        let frame_sequence = non_zero_sequence(packet.frame_sequence)?;
        let stream_id = StreamId::new(packet.stream_id).ok_or(Error::InvalidStreamId)?;
        let stream_sequence =
            StreamSequence::new(packet.stream_sequence).ok_or(Error::InvalidFragment)?;
        let message_id = MessageId::new(packet.message_id).ok_or(Error::InvalidFragment)?;
        let frame_duplicate = self.recv_ack.contains(frame_sequence);
        let stream_duplicate = self
            .streams
            .get(&stream_id)
            .is_some_and(|stream| stream.recv_reorder.contains_key(&stream_sequence));
        let recently_completed = self
            .streams
            .get(&stream_id)
            .is_some_and(|stream| stream.recently_completed.contains(&message_id));
        if !frame_duplicate && (stream_duplicate || recently_completed) {
            self.recv_ack.observe(frame_sequence);
        }
        let stream = self
            .streams
            .entry(stream_id)
            .or_insert_with(StreamInboundState::new);

        if frame_duplicate || stream_duplicate {
            metrics.record_duplicate();
            let message_acks = stream
                .recently_completed
                .contains(&message_id)
                .then_some((stream_id, message_id))
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
        if stream.recently_completed.contains(&message_id) {
            metrics.record_duplicate_fragment();
            return Ok(InboundOutput {
                decision: AckDecision::Immediate,
                delivered: 0,
                message_acks: vec![(stream_id, message_id)],
                arm_reassembly: Vec::new(),
                cancel_reassembly: Vec::new(),
            });
        }
        let Some(distance) =
            StreamSequence::forward_distance(stream.next_deliver_sequence, stream_sequence)
        else {
            metrics.record_drop();
            return Ok(Self::empty_output());
        };
        if distance >= config.stream_receive_window_frames.get() as u64
            || stream.recv_reorder.len() >= config.stream_receive_window_frames.get()
        {
            metrics.record_receive_window_drop();
            return Ok(Self::empty_output());
        }

        let message_len =
            usize::try_from(packet.message_len).map_err(|_| Error::InvalidFragment)?;
        let mut arm_reassembly = Vec::new();
        let mut created_reassembly = false;
        if let Some(entry) = stream.reassembly.get(&message_id) {
            if entry.message_len != message_len || entry.fragment_count != packet.fragment_count {
                metrics.record_drop();
                return Ok(Self::empty_output());
            }
        } else {
            if stream.reassembly.len() >= config.max_reassembly_messages.get()
                || total_reassembly_messages >= config.max_reassembly_messages.get()
            {
                metrics.record_reassembly_limit();
                return Ok(Self::empty_output());
            }
            let reserved = stream
                .reserved_reassembly_bytes
                .checked_add(message_len)
                .ok_or(Error::ReassemblyLimit)?;
            let connection_reserved = total_reassembly_bytes
                .checked_add(message_len)
                .ok_or(Error::ReassemblyLimit)?;
            if reserved > config.max_stream_receive_bytes.get()
                || connection_reserved > config.max_connection_receive_bytes.get()
                || reserved > config.max_reassembly_bytes.get()
            {
                metrics.record_reassembly_limit();
                return Ok(Self::empty_output());
            }
            if total_buffered_bytes
                .checked_add(message_len)
                .is_none_or(|bytes| bytes > config.max_connection_receive_bytes.get())
            {
                metrics.record_receive_window_drop();
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
            stream.reassembly.insert(
                message_id,
                ReassemblyEntry {
                    message_len,
                    fragment_count: packet.fragment_count,
                    buffer,
                    received_bitmap: vec![0; bitmap_len],
                    received_fragments: 0,
                    received_bytes: 0,
                    complete_pending_delivery: false,
                    first_sequence: stream_sequence,
                },
            );
            stream.reserved_reassembly_bytes = reserved;
            arm_reassembly.push((stream_id, message_id));
            created_reassembly = true;
        }

        let observation = self.recv_ack.observe(frame_sequence);
        if matches!(observation, AckObserve::Duplicate | AckObserve::TooOld) {
            if created_reassembly && let Some(entry) = stream.reassembly.remove(&message_id) {
                stream.reserved_reassembly_bytes = stream
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
        stream.recv_reorder.insert(
            stream_sequence,
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
        Self::process_contiguous(stream_id, stream, config, metrics, &mut output);
        output.delivered = output.message_acks.len();
        self.pending_ack_packets = self.pending_ack_packets.saturating_add(1);
        if distance == 0
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
            frame_sequence = frame_sequence.get(),
            stream_id = stream_id.get(),
            stream_sequence = stream_sequence.get(),
            message_id = message_id.get(),
            observation = ?observation,
            delivered = output.delivered,
            "session accepted data fragment"
        );
        Ok(output)
    }

    pub(super) fn pop_message(
        &mut self,
        stream_id: StreamId,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> ReceiveOutput {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return (None, Vec::new(), Vec::new());
        };
        let message = stream.recv_ready.pop_front();
        if let Some(message) = &message {
            stream.buffered_inbound_bytes = stream
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
        Self::promote_ready(stream_id, stream, config, metrics, &mut output);
        (message, output.message_acks, output.cancel_reassembly)
    }

    pub(super) fn buffered_messages(&self) -> usize {
        self.streams
            .values()
            .map(|stream| {
                stream.recv_ready.len() + stream.recv_reorder.len() + stream.reassembly.len()
            })
            .fold(0, usize::saturating_add)
    }

    pub(super) fn reassembly_messages(&self) -> usize {
        self.streams
            .values()
            .map(|stream| stream.reassembly.len())
            .fold(0, usize::saturating_add)
    }

    pub(super) fn reassembly_bytes(&self) -> usize {
        self.total_reassembly_bytes()
    }

    pub(super) fn available_window(&self, config: &Config) -> usize {
        config
            .connection_receive_window_frames
            .get()
            .saturating_sub(self.buffered_messages())
    }

    pub(super) fn ack_snapshot(&self) -> AckWindow {
        self.recv_ack
    }

    pub(super) fn observe_reliable_frame(&mut self, sequence: FrameSequence) -> AckObserve {
        self.recv_ack.observe(sequence)
    }

    pub(super) fn ack_is_empty(&self) -> bool {
        self.recv_ack.is_empty()
    }

    pub(super) fn mark_ack_sent(&mut self) {
        self.pending_ack_packets = 0;
    }

    pub(super) fn on_reassembly_timeout(
        &mut self,
        stream_id: StreamId,
        message_id: MessageId,
        metrics: &mut SessionMetrics,
    ) -> bool {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return false;
        };
        let Some(entry) = stream.reassembly.remove(&message_id) else {
            return false;
        };
        stream.reserved_reassembly_bytes = stream
            .reserved_reassembly_bytes
            .saturating_sub(entry.message_len);
        metrics.record_reassembly_timeout();
        true
    }

    pub(super) fn clear(&mut self) -> Vec<(StreamId, MessageId)> {
        let mut ids = Vec::new();
        for (stream_id, stream) in &mut self.streams {
            ids.extend(
                stream
                    .reassembly
                    .keys()
                    .copied()
                    .map(|message_id| (*stream_id, message_id)),
            );
            stream.recv_reorder.clear();
            stream.reassembly.clear();
            stream.recv_ready.clear();
            stream.recently_completed.clear();
            stream.reserved_reassembly_bytes = 0;
            stream.buffered_inbound_bytes = 0;
        }
        self.recv_ack = AckWindow::new();
        ids
    }

    pub(super) fn reset_stream(&mut self, stream_id: StreamId) -> Vec<MessageId> {
        self.streams
            .remove(&stream_id)
            .map(|stream| stream.reassembly.into_keys().collect())
            .unwrap_or_default()
    }

    fn process_contiguous(
        stream_id: StreamId,
        stream: &mut StreamInboundState,
        config: &Config,
        metrics: &mut SessionMetrics,
        output: &mut InboundOutput,
    ) {
        while let Some(fragment) = stream.recv_reorder.remove(&stream.next_deliver_sequence) {
            let sequence = stream.next_deliver_sequence;
            stream.next_deliver_sequence = sequence.next();
            let message_id = MessageId::new(fragment.meta.message_id)
                .expect("validated data metadata has a message ID");
            let Some(entry) = stream.reassembly.get_mut(&message_id) else {
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
                Self::promote_ready(stream_id, stream, config, metrics, output);
            }
        }
        Self::promote_ready(stream_id, stream, config, metrics, output);
    }

    fn promote_ready(
        stream_id: StreamId,
        stream: &mut StreamInboundState,
        config: &Config,
        metrics: &mut SessionMetrics,
        output: &mut InboundOutput,
    ) {
        loop {
            let message_id = stream.next_deliver_message;
            let Some(entry) = stream.reassembly.get(&message_id) else {
                break;
            };
            if !entry.complete_pending_delivery
                || stream.recv_ready.len() >= config.stream_inbound_capacity.get()
                || stream
                    .buffered_inbound_bytes
                    .checked_add(entry.message_len)
                    .is_none_or(|bytes| bytes > config.max_stream_receive_bytes.get())
            {
                break;
            }
            let entry = stream
                .reassembly
                .remove(&message_id)
                .expect("reassembly entry was checked above");
            stream.reserved_reassembly_bytes = stream
                .reserved_reassembly_bytes
                .saturating_sub(entry.message_len);
            stream.buffered_inbound_bytes = stream
                .buffered_inbound_bytes
                .saturating_add(entry.message_len);
            stream.recv_ready.push_back(StreamMessage {
                stream_id,
                stream_sequence: entry.first_sequence,
                message_id,
                payload: entry.buffer,
            });
            stream.recently_completed.push_back(message_id);
            while stream.recently_completed.len()
                > config.stream_receive_window_frames.get().max(64)
            {
                stream.recently_completed.pop_front();
            }
            stream.next_deliver_message = message_id.next();
            output.message_acks.push((stream_id, message_id));
            output.cancel_reassembly.push((stream_id, message_id));
            metrics.record_completed_message();
        }
    }

    fn total_reassembly_messages(&self) -> usize {
        self.streams
            .values()
            .map(|stream| stream.reassembly.len())
            .sum()
    }

    fn total_reassembly_bytes(&self) -> usize {
        self.streams
            .values()
            .map(|stream| stream.reserved_reassembly_bytes)
            .fold(0, usize::saturating_add)
    }

    fn total_buffered_bytes(&self) -> usize {
        self.streams
            .values()
            .map(|stream| {
                stream
                    .reserved_reassembly_bytes
                    .saturating_add(stream.buffered_inbound_bytes)
            })
            .fold(0, usize::saturating_add)
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
