use tracing::debug;
use veloq::{
    buf::FixedBuf,
    std::{
        collections::{HashMap, VecDeque},
        time::Duration,
        vec::Vec,
    },
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{
        Ack, AckWindow, COOKIE_LEN, ConnectionId, FrameSequence, FrameType, HEADER_LEN, MessageId,
        Packet, PacketBufAllocator, PacketMeta, StreamDataPacket, StreamId, StreamSequence,
    },
    timer::TimerKind,
};

use super::metrics::{OutboundView, SessionMetrics};
use super::{SendReceipt, SendToken};

pub(super) struct PendingMessage {
    stream_id: StreamId,
    token: SendToken,
    message_id: MessageId,
    payload: FixedBuf,
    fragment_count: u32,
    next_fragment: u32,
    next_stream_sequence: StreamSequence,
}

struct MessageTxState {
    stream_id: StreamId,
    payload_len: usize,
    token: SendToken,
    message_id: MessageId,
    fragment_count: u32,
    emitted_fragments: u32,
    acked_frame_count: u32,
    final_probe: Option<FixedBuf>,
    message_ack_retries: u8,
    retransmissions: u64,
    first_sent_at: Option<Duration>,
}

struct TxFrame {
    stream_id: StreamId,
    message_id: MessageId,
    fragment_index: u32,
    datagram: Option<FixedBuf>,
    sent_at: Duration,
    rto: Duration,
    retries: u8,
    retransmitted: bool,
    acknowledged: bool,
}

pub(super) enum OutboundAction {
    Datagram {
        datagram: FixedBuf,
        sequence: Option<FrameSequence>,
    },
    ArmTimer {
        kind: TimerKind,
        delay: Duration,
    },
    CancelTimer(TimerKind),
    SendAcked(SendReceipt),
    SendFailed {
        token: SendToken,
        message_id: Option<MessageId>,
        error: Error,
    },
}

pub(super) struct OutboundOutput {
    actions: Vec<OutboundAction>,
    ack_consumed: bool,
    terminal_error: Option<Error>,
    stream_failure: Option<(StreamId, Error)>,
}

impl OutboundOutput {
    pub(super) fn new() -> Self {
        Self {
            actions: Vec::new(),
            ack_consumed: false,
            terminal_error: None,
            stream_failure: None,
        }
    }

    pub(super) fn into_actions(self) -> Vec<OutboundAction> {
        self.actions
    }

    pub(super) fn ack_consumed(&self) -> bool {
        self.ack_consumed
    }

    pub(super) fn terminal_error(&self) -> Option<Error> {
        self.terminal_error
    }

    pub(super) fn stream_failure(&self) -> Option<(StreamId, Error)> {
        self.stream_failure
    }
}

pub(super) struct FlushContext<'a, A: PacketBufAllocator + ?Sized> {
    connection_id: ConnectionId,
    config: &'a Config,
    allocator: &'a A,
    now: Duration,
    ack: AckWindow,
    receive_window: usize,
}

impl<'a, A: PacketBufAllocator + ?Sized> FlushContext<'a, A> {
    pub(super) fn new(
        connection_id: ConnectionId,
        config: &'a Config,
        allocator: &'a A,
        now: Duration,
        ack: AckWindow,
        receive_window: usize,
    ) -> Self {
        Self {
            connection_id,
            config,
            allocator,
            now,
            ack,
            receive_window,
        }
    }
}

pub(super) struct MessageAckContext<'a, A: PacketBufAllocator + ?Sized> {
    pub(super) connection_id: ConnectionId,
    pub(super) config: &'a Config,
    pub(super) allocator: &'a A,
    pub(super) message_id: MessageId,
    pub(super) stream_id: StreamId,
    pub(super) ack: AckWindow,
    pub(super) receive_window: usize,
    pub(super) metrics: &'a mut SessionMetrics,
}

pub(super) struct OutboundState {
    send_next_frame: FrameSequence,
    next_stream_messages: HashMap<StreamId, MessageId>,
    next_stream_sequences: HashMap<StreamId, StreamSequence>,
    pending_stream_bytes: HashMap<StreamId, usize>,
    pending_connection_bytes: usize,
    peer_stream_windows: HashMap<StreamId, usize>,
    peer_stream_bytes: HashMap<StreamId, usize>,
    stream_bytes_sent: HashMap<StreamId, usize>,
    send_frames: HashMap<FrameSequence, TxFrame>,
    messages: HashMap<(StreamId, MessageId), MessageTxState>,
    pending_send: VecDeque<PendingMessage>,
    next_token: u64,
    peer_receive_window: usize,
    congestion_window: usize,
    slow_start_threshold: usize,
    congestion_credit: usize,
}

impl OutboundState {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            send_next_frame: FrameSequence::new(1).expect("one is a valid frame sequence"),
            next_stream_messages: HashMap::default(),
            next_stream_sequences: HashMap::default(),
            pending_stream_bytes: HashMap::default(),
            pending_connection_bytes: 0,
            peer_stream_windows: HashMap::default(),
            peer_stream_bytes: HashMap::default(),
            stream_bytes_sent: HashMap::default(),
            send_frames: HashMap::default(),
            messages: HashMap::default(),
            pending_send: VecDeque::new(),
            next_token: 1,
            peer_receive_window: config.send_window.get(),
            congestion_window: config.send_window.get().min(2),
            slow_start_threshold: config.send_window.get(),
            congestion_credit: 0,
        }
    }

    pub(super) fn queue_stream_send(
        &mut self,
        stream_id: StreamId,
        payload: FixedBuf,
        config: &Config,
    ) -> Result<SendToken> {
        if payload.len() > config.max_message_size.get() {
            return Err(Error::MessageTooLarge);
        }
        let max_fragment_payload = config.max_fragment_payload();
        let fragment_count = if payload.is_empty() {
            1
        } else {
            payload
                .len()
                .checked_add(max_fragment_payload - 1)
                .ok_or(Error::FragmentCountExceeded)?
                / max_fragment_payload
        };
        let fragment_count =
            u32::try_from(fragment_count).map_err(|_| Error::FragmentCountExceeded)?;
        if fragment_count > config.max_fragments_per_message.get() {
            return Err(Error::FragmentCountExceeded);
        }
        if self.pending_send.len() + self.messages.len() >= config.pending_send_capacity.get() {
            return Err(Error::SendWindowClosed);
        }
        let stream_pending = self
            .pending_send
            .iter()
            .filter(|pending| pending.stream_id == stream_id)
            .count()
            + self
                .messages
                .keys()
                .filter(|(current, _)| *current == stream_id)
                .count();
        if stream_pending >= config.stream_pending_send_capacity.get() {
            return Err(Error::StreamSendWindowClosed);
        }
        let stream_bytes = self
            .pending_stream_bytes
            .get(&stream_id)
            .copied()
            .unwrap_or(0);
        let stream_bytes_sent = self.stream_bytes_sent.get(&stream_id).copied().unwrap_or(0);
        if self
            .peer_stream_bytes
            .get(&stream_id)
            .is_some_and(|credit| {
                stream_bytes_sent
                    .checked_add(stream_bytes)
                    .and_then(|bytes| bytes.checked_add(payload.len()))
                    .is_none_or(|bytes| bytes > *credit)
            })
        {
            return Err(Error::StreamFlowControlExceeded);
        }
        if stream_bytes
            .checked_add(payload.len())
            .is_none_or(|bytes| bytes > config.max_stream_send_bytes.get())
            || self
                .pending_connection_bytes
                .checked_add(payload.len())
                .is_none_or(|bytes| bytes > config.max_connection_send_bytes.get())
        {
            return Err(Error::ConnectionFlowControlExceeded);
        }
        let token = SendToken(self.next_token);
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let message_id = *self
            .next_stream_messages
            .entry(stream_id)
            .or_insert_with(|| MessageId::new(1).expect("one is a valid message ID"));
        self.next_stream_messages
            .insert(stream_id, message_id.next());
        self.pending_stream_bytes
            .insert(stream_id, stream_bytes.saturating_add(payload.len()));
        self.pending_connection_bytes = self.pending_connection_bytes.saturating_add(payload.len());
        self.stream_bytes_sent
            .insert(stream_id, stream_bytes_sent.saturating_add(payload.len()));
        self.pending_send.push_back(PendingMessage {
            stream_id,
            token,
            message_id,
            payload,
            fragment_count,
            next_fragment: 0,
            next_stream_sequence: *self
                .next_stream_sequences
                .entry(stream_id)
                .or_insert_with(|| StreamSequence::new(1).expect("one is valid")),
        });
        Ok(token)
    }

    pub(super) fn set_peer_receive_window(&mut self, window: u16) {
        self.peer_receive_window = usize::from(window);
    }

    pub(super) fn set_peer_stream_window(
        &mut self,
        stream_id: StreamId,
        window: u16,
        byte_credit: u64,
    ) {
        let window = usize::from(window);
        self.peer_stream_windows
            .entry(stream_id)
            .and_modify(|current| *current = (*current).max(window))
            .or_insert(window);
        let byte_credit = usize::try_from(byte_credit).unwrap_or(usize::MAX);
        self.peer_stream_bytes
            .entry(stream_id)
            .and_modify(|current| *current = (*current).max(byte_credit))
            .or_insert(byte_credit);
    }

    pub(super) fn apply_ack(
        &mut self,
        packet: &PacketMeta,
        now: Duration,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        if !packet.has_ack || packet.ack_largest == 0 {
            return Ok(output);
        }
        let ack = packet.ack()?;
        let acked: Vec<FrameSequence> = self
            .send_frames
            .keys()
            .copied()
            .filter(|sequence| ack.acknowledges(*sequence))
            .collect();
        if acked.is_empty() {
            metrics.record_duplicate_ack();
        }
        for sequence in acked {
            let Some(frame) = self.send_frames.get_mut(&sequence) else {
                continue;
            };
            if frame.acknowledged {
                continue;
            }
            frame.acknowledged = true;
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
            if frame.datagram.is_some() {
                self.finalize_frame(sequence, now, config, metrics, &mut output);
            }
        }
        Ok(output)
    }

    pub(super) fn apply_message_ack(
        &mut self,
        stream_id: StreamId,
        message_id: MessageId,
        now: Duration,
        config: &Config,
    ) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let Some(state) = self.messages.remove(&(stream_id, message_id)) else {
            return output;
        };
        self.release_pending_bytes(stream_id, state.payload_len);
        output
            .actions
            .push(OutboundAction::CancelTimer(TimerKind::MessageAckRetry {
                stream_id,
                message_id,
            }));
        let frame_sequences: Vec<FrameSequence> = self
            .send_frames
            .iter()
            .filter_map(|(sequence, frame)| {
                (frame.stream_id == stream_id && frame.message_id == message_id)
                    .then_some(*sequence)
            })
            .collect();
        for sequence in frame_sequences {
            self.send_frames.remove(&sequence);
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
        }
        let rtt = state
            .first_sent_at
            .map(|sent_at| now.saturating_sub(sent_at));
        output.actions.push(OutboundAction::SendAcked(SendReceipt {
            token: state.token,
            stream_id,
            message_id,
            fragment_count: state.fragment_count,
            rtt,
            retransmissions: state.retransmissions,
        }));
        let _ = config;
        output
    }

    pub(super) fn on_retransmit(
        &mut self,
        sequence: FrameSequence,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let Some(entry) = self.send_frames.get(&sequence) else {
            return Ok(output);
        };
        if entry.retries >= config.max_retries {
            output.stream_failure = Some((entry.stream_id, Error::RetransmitExhausted));
            return Ok(output);
        }
        let (datagram, rto) = {
            let entry = self.send_frames.get_mut(&sequence).expect("frame exists");
            let Some(datagram) = entry.datagram.take() else {
                return Ok(output);
            };
            entry.retries = entry.retries.saturating_add(1);
            entry.retransmitted = true;
            entry.rto = entry.rto.saturating_mul(2).min(config.max_rto);
            (datagram, entry.rto)
        };
        metrics.record_retransmission();
        self.on_congestion_timeout();
        if let Some(frame) = self.send_frames.get(&sequence)
            && let Some(message) = self.messages.get_mut(&(frame.stream_id, frame.message_id))
        {
            message.retransmissions = message.retransmissions.saturating_add(1);
        }
        output.actions.push(OutboundAction::Datagram {
            datagram,
            sequence: Some(sequence),
        });
        output.actions.push(OutboundAction::ArmTimer {
            kind: TimerKind::Retransmit { sequence },
            delay: rto,
        });
        Ok(output)
    }

    pub(super) fn on_message_ack_retry<A: PacketBufAllocator + ?Sized>(
        &mut self,
        stream_id: StreamId,
        message_id: MessageId,
        config: &Config,
        allocator: &A,
        metrics: &mut SessionMetrics,
    ) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let Some(message) = self.messages.get_mut(&(stream_id, message_id)) else {
            return output;
        };
        if message.message_ack_retries >= config.message_ack_max_retries {
            output.stream_failure = Some((stream_id, Error::MessageAckTimeout));
            return output;
        }
        message.message_ack_retries = message.message_ack_retries.saturating_add(1);
        metrics.record_message_ack_retry();
        if let Some(probe) = message.final_probe.as_ref() {
            match allocator.alloc_packet_buf(config.max_datagram_size, probe.len()) {
                Ok(mut datagram) => {
                    datagram.as_slice_mut().copy_from_slice(probe.as_slice());
                    output.actions.push(OutboundAction::Datagram {
                        datagram,
                        sequence: None,
                    });
                }
                Err(_) => output.terminal_error = Some(Error::Io),
            }
        }
        output.actions.push(OutboundAction::ArmTimer {
            kind: TimerKind::MessageAckRetry {
                stream_id,
                message_id,
            },
            delay: config.message_ack_timeout,
        });
        output
    }

    pub(super) fn on_send_completed(
        &mut self,
        sequence: FrameSequence,
        sent_at: Duration,
        datagram: FixedBuf,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let Some(entry) = self.send_frames.get_mut(&sequence) else {
            return output;
        };
        if entry.datagram.is_some() {
            return output;
        }
        entry.sent_at = sent_at;
        entry.datagram = Some(datagram);
        if entry.acknowledged {
            self.finalize_frame(sequence, sent_at, config, metrics, &mut output);
        } else {
            output.actions.push(OutboundAction::ArmTimer {
                kind: TimerKind::Retransmit { sequence },
                delay: entry.rto,
            });
        }
        if let Some(frame) = self.send_frames.get(&sequence)
            && let Some(message) = self.messages.get_mut(&(frame.stream_id, frame.message_id))
            && message.first_sent_at.is_none()
        {
            message.first_sent_at = Some(sent_at);
        }
        output
    }

    pub(super) fn flush_pending<A: PacketBufAllocator + ?Sized>(
        &mut self,
        context: FlushContext<'_, A>,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let limit = self.send_limit(context.config);
        let mut current_ack = context.ack.ack();
        let rto = metrics.rto();
        let mut blocked_streams: usize = 0;
        while self.send_frames.len() < limit && !self.pending_send.is_empty() {
            let Some(pending) = self.pending_send.pop_front() else {
                break;
            };
            let stream_in_flight = self
                .send_frames
                .values()
                .filter(|frame| frame.stream_id == pending.stream_id)
                .count();
            if stream_in_flight
                >= context.config.stream_send_window_frames.get().min(
                    self.peer_stream_windows
                        .get(&pending.stream_id)
                        .copied()
                        .unwrap_or(context.config.stream_send_window_frames.get()),
                )
            {
                self.pending_send.push_back(pending);
                blocked_streams = blocked_streams.saturating_add(1);
                if blocked_streams >= self.pending_send.len() {
                    break;
                }
                continue;
            }
            blocked_streams = 0;
            let use_ack = current_ack.largest().is_some();
            self.emit_fragment(pending, &context, current_ack, rto, metrics, &mut output)?;
            if use_ack {
                output.ack_consumed = true;
                current_ack = Ack::empty();
            }
        }
        Ok(output)
    }

    pub(super) fn emit_control<A: PacketBufAllocator + ?Sized>(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        allocator: &A,
        frame_type: FrameType,
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let frame_sequence = matches!(frame_type, FrameType::Fin | FrameType::FinAck).then(|| {
            let sequence = self.send_next_frame;
            self.send_next_frame = sequence.next();
            sequence
        });
        let packet = Packet::encode_frame(
            allocator,
            config.max_datagram_size,
            frame_type,
            false,
            connection_id,
            frame_sequence,
            None,
            Ack::empty(),
            receive_window as u16,
            &[],
        );
        output.actions.push(OutboundAction::Datagram {
            datagram: packet?.into_fixed_buf(),
            sequence: None,
        });
        metrics.record_sent();
        Ok(output)
    }

    pub(super) fn emit_handshake_cookie<A: PacketBufAllocator + ?Sized>(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        allocator: &A,
        cookie: &[u8; COOKIE_LEN],
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let packet = Packet::encode_frame(
            allocator,
            config.max_datagram_size,
            FrameType::HandshakeAck,
            false,
            connection_id,
            None,
            None,
            Ack::empty(),
            receive_window as u16,
            cookie,
        )?;
        let mut output = OutboundOutput::new();
        output.actions.push(OutboundAction::Datagram {
            datagram: packet.into_fixed_buf(),
            sequence: None,
        });
        metrics.record_ack_sent();
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn emit_stream_control<A: PacketBufAllocator + ?Sized>(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        allocator: &A,
        frame_type: FrameType,
        stream_id: StreamId,
        payload: &[u8],
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let sequence = self.send_next_frame;
        self.send_next_frame = sequence.next();
        let packet = Packet::encode_frame(
            allocator,
            config.max_datagram_size,
            frame_type,
            false,
            connection_id,
            Some(sequence),
            Some(stream_id),
            Ack::empty(),
            receive_window as u16,
            payload,
        )?;
        let mut output = OutboundOutput::new();
        output.actions.push(OutboundAction::Datagram {
            datagram: packet.into_fixed_buf(),
            sequence: Some(sequence),
        });
        metrics.record_sent();
        Ok(output)
    }

    pub(super) fn emit_ack<A: PacketBufAllocator + ?Sized>(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        allocator: &A,
        ack: AckWindow,
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let packet = Packet::encode_frame(
            allocator,
            config.max_datagram_size,
            FrameType::Ack,
            true,
            connection_id,
            None,
            None,
            ack.ack(),
            receive_window as u16,
            &[],
        );
        output
            .actions
            .push(OutboundAction::CancelTimer(TimerKind::AckDelay));
        output.actions.push(OutboundAction::Datagram {
            datagram: packet?.into_fixed_buf(),
            sequence: None,
        });
        metrics.record_ack_sent();
        Ok(output)
    }

    pub(super) fn emit_message_ack<A: PacketBufAllocator + ?Sized>(
        &mut self,
        context: MessageAckContext<'_, A>,
    ) -> Result<OutboundOutput> {
        let packet = Packet::encode_stream_message_ack_into_with_limit(
            context.allocator,
            context.config.max_datagram_size,
            context.connection_id,
            context.stream_id,
            context.ack.ack(),
            context.receive_window as u16,
            context.message_id,
        );
        let mut output = OutboundOutput::new();
        output.actions.push(OutboundAction::Datagram {
            datagram: packet?.into_fixed_buf(),
            sequence: None,
        });
        context.metrics.record_ack_sent();
        Ok(output)
    }

    pub(super) fn fail_all(&mut self, error: Error, _generation: u64) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        for (sequence, _) in self.send_frames.drain() {
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
        }
        for (_, state) in self.messages.drain() {
            output.actions.push(OutboundAction::SendFailed {
                token: state.token,
                message_id: Some(state.message_id),
                error,
            });
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::MessageAckRetry {
                    stream_id: state.stream_id,
                    message_id: state.message_id,
                }));
        }
        for pending in self.pending_send.drain(..) {
            output.actions.push(OutboundAction::SendFailed {
                token: pending.token,
                message_id: Some(pending.message_id),
                error,
            });
        }
        self.peer_stream_windows.clear();
        self.peer_stream_bytes.clear();
        self.stream_bytes_sent.clear();
        output
    }

    pub(super) fn fail_stream(&mut self, stream_id: StreamId, error: Error) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let frame_sequences: Vec<FrameSequence> = self
            .send_frames
            .iter()
            .filter_map(|(sequence, frame)| (frame.stream_id == stream_id).then_some(*sequence))
            .collect();
        for sequence in frame_sequences {
            self.send_frames.remove(&sequence);
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
        }

        let mut pending = VecDeque::new();
        while let Some(message) = self.pending_send.pop_front() {
            if message.stream_id == stream_id {
                if !self.messages.contains_key(&(stream_id, message.message_id)) {
                    output.actions.push(OutboundAction::SendFailed {
                        token: message.token,
                        message_id: Some(message.message_id),
                        error,
                    });
                    self.release_pending_bytes(stream_id, message.payload.len());
                }
            } else {
                pending.push_back(message);
            }
        }
        self.pending_send = pending;

        let message_keys: Vec<(StreamId, MessageId)> = self
            .messages
            .keys()
            .copied()
            .filter(|(current_stream, _)| *current_stream == stream_id)
            .collect();
        for key @ (_, message_id) in message_keys {
            if let Some(message) = self.messages.remove(&key) {
                self.release_pending_bytes(stream_id, message.payload_len);
                output.actions.push(OutboundAction::SendFailed {
                    token: message.token,
                    message_id: Some(message_id),
                    error,
                });
                output
                    .actions
                    .push(OutboundAction::CancelTimer(TimerKind::MessageAckRetry {
                        stream_id,
                        message_id,
                    }));
            }
        }
        self.peer_stream_windows.remove(&stream_id);
        self.peer_stream_bytes.remove(&stream_id);
        self.stream_bytes_sent.remove(&stream_id);
        output
    }

    pub(super) fn send_limit(&self, config: &Config) -> usize {
        config
            .send_window
            .get()
            .min(self.peer_receive_window)
            .min(self.congestion_window)
    }

    pub(super) fn in_flight(&self) -> usize {
        self.send_frames.len()
    }

    pub(super) fn pending_sends(&self) -> usize {
        self.pending_send.len() + self.messages.len()
    }

    pub(super) fn congestion_window(&self) -> usize {
        self.congestion_window
    }

    pub(super) fn view(&self, config: &Config) -> OutboundView {
        OutboundView::new(
            config.send_window.get(),
            self.congestion_window,
            self.slow_start_threshold,
            self.peer_receive_window,
        )
    }

    fn emit_fragment<A: PacketBufAllocator + ?Sized>(
        &mut self,
        mut pending: PendingMessage,
        context: &FlushContext<'_, A>,
        ack: Ack,
        rto: Duration,
        metrics: &mut SessionMetrics,
        output: &mut OutboundOutput,
    ) -> Result<()> {
        let frame_sequence = self.send_next_frame;
        self.send_next_frame = frame_sequence.next();
        if ack.largest().is_some() {
            metrics.record_piggybacked_ack();
        }
        let max_fragment_payload = context.config.max_fragment_payload();
        let offset = usize::try_from(pending.next_fragment)
            .ok()
            .and_then(|index| index.checked_mul(max_fragment_payload))
            .ok_or(Error::FragmentCountExceeded)?;
        let remaining = pending.payload.len().saturating_sub(offset);
        let fragment_len = remaining.min(max_fragment_payload);
        let payload = &pending.payload.as_slice()[offset..offset + fragment_len];
        let stream_sequence = pending.next_stream_sequence;
        self.next_stream_sequences
            .insert(pending.stream_id, stream_sequence.next());
        let packet = Packet::encode_stream_data_into_with_limit(
            context.allocator,
            context.config.max_datagram_size,
            StreamDataPacket {
                connection_id: context.connection_id,
                frame_sequence,
                stream_id: pending.stream_id,
                stream_sequence,
                ack,
                receive_window: context.receive_window as u16,
                message_id: pending.message_id,
                fragment_index: pending.next_fragment,
                fragment_count: pending.fragment_count,
                message_len: pending.payload.len() as u64,
                payload,
                max_fragment_payload,
            },
        )?;
        let datagram = packet.into_fixed_buf();
        self.send_frames.insert(
            frame_sequence,
            TxFrame {
                stream_id: pending.stream_id,
                message_id: pending.message_id,
                fragment_index: pending.next_fragment,
                datagram: None,
                sent_at: context.now,
                rto,
                retries: 0,
                retransmitted: false,
                acknowledged: false,
            },
        );
        let entry = self
            .messages
            .entry((pending.stream_id, pending.message_id))
            .or_insert(MessageTxState {
                stream_id: pending.stream_id,
                payload_len: pending.payload.len(),
                token: pending.token,
                message_id: pending.message_id,
                fragment_count: pending.fragment_count,
                emitted_fragments: 0,
                acked_frame_count: 0,
                final_probe: None,
                message_ack_retries: 0,
                retransmissions: 0,
                first_sent_at: None,
            });
        entry.emitted_fragments = entry.emitted_fragments.saturating_add(1);
        pending.next_fragment = pending.next_fragment.saturating_add(1);
        pending.next_stream_sequence = stream_sequence.next();
        if pending.next_fragment < pending.fragment_count {
            self.pending_send.push_back(pending);
        }
        metrics.record_data_sent();
        debug!(
            target: "veloq_reliable_udp::session",
            frame_sequence = frame_sequence.get(),
            message_id = entry.message_id.get(),
            fragment_index = entry.emitted_fragments - 1,
            fragment_count = entry.fragment_count,
            payload_len = datagram.len().saturating_sub(HEADER_LEN),
            "session emitted data fragment"
        );
        output.actions.push(OutboundAction::Datagram {
            datagram,
            sequence: Some(frame_sequence),
        });
        Ok(())
    }

    fn finalize_frame(
        &mut self,
        sequence: FrameSequence,
        now: Duration,
        config: &Config,
        metrics: &mut SessionMetrics,
        output: &mut OutboundOutput,
    ) {
        let Some(mut frame) = self.send_frames.remove(&sequence) else {
            return;
        };
        let message_id = frame.message_id;
        let Some(message) = self.messages.get_mut(&(frame.stream_id, message_id)) else {
            return;
        };
        message.acked_frame_count = message.acked_frame_count.saturating_add(1);
        let rtt = (!frame.retransmitted).then(|| now.saturating_sub(frame.sent_at));
        if let Some(sample) = rtt {
            metrics.record_rtt(sample, config);
        }
        if frame.fragment_index + 1 == message.fragment_count {
            message.final_probe = frame.datagram.take();
        }
        let complete = message.acked_frame_count == message.fragment_count;
        if complete {
            output.actions.push(OutboundAction::ArmTimer {
                kind: TimerKind::MessageAckRetry {
                    stream_id: frame.stream_id,
                    message_id: message.message_id,
                },
                delay: config.message_ack_timeout,
            });
        }
        self.increase_congestion_window(1, config);
    }

    fn increase_congestion_window(&mut self, acknowledged: usize, config: &Config) {
        let max_window = config.send_window.get();
        for _ in 0..acknowledged {
            if self.congestion_window >= max_window {
                break;
            }
            if self.congestion_window < self.slow_start_threshold {
                self.congestion_window = self.congestion_window.saturating_add(1).min(max_window);
                continue;
            }
            self.congestion_credit = self.congestion_credit.saturating_add(1);
            if self.congestion_credit >= self.congestion_window {
                self.congestion_credit -= self.congestion_window;
                self.congestion_window = self.congestion_window.saturating_add(1).min(max_window);
            }
        }
    }

    pub(super) fn on_congestion_timeout(&mut self) {
        self.slow_start_threshold = (self.congestion_window / 2).max(1);
        self.congestion_window = 1;
        self.congestion_credit = 0;
    }

    fn release_pending_bytes(&mut self, stream_id: StreamId, payload_len: usize) {
        if let Some(bytes) = self.pending_stream_bytes.get_mut(&stream_id) {
            *bytes = bytes.saturating_sub(payload_len);
            if *bytes == 0 {
                self.pending_stream_bytes.remove(&stream_id);
            }
        }
        self.pending_connection_bytes = self.pending_connection_bytes.saturating_sub(payload_len);
    }
}
