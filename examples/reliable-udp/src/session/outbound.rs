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
        Ack, AckWindow, ConnectionId, DataPacket, Flags, FrameSequence, HEADER_LEN, MessageId,
        Packet, PacketBufAllocator, PacketMeta,
    },
    timer::TimerKind,
};

use super::metrics::{OutboundView, SessionMetrics};
use super::{SendReceipt, SendToken};

pub(super) struct PendingMessage {
    token: SendToken,
    message_id: MessageId,
    payload: FixedBuf,
    fragment_count: u32,
    next_fragment: u32,
}

struct MessageTxState {
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
}

impl OutboundOutput {
    pub(super) fn new() -> Self {
        Self {
            actions: Vec::new(),
            ack_consumed: false,
            terminal_error: None,
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
    pub(super) ack: AckWindow,
    pub(super) receive_window: usize,
    pub(super) metrics: &'a mut SessionMetrics,
}

pub(super) struct OutboundState {
    send_next_frame: FrameSequence,
    send_next_message: MessageId,
    send_frames: HashMap<FrameSequence, TxFrame>,
    messages: HashMap<MessageId, MessageTxState>,
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
            send_next_message: MessageId::new(1).expect("one is a valid message ID"),
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

    pub(super) fn queue_send(&mut self, payload: FixedBuf, config: &Config) -> Result<SendToken> {
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
        let token = SendToken(self.next_token);
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let message_id = self.send_next_message;
        self.send_next_message = message_id.next();
        self.pending_send.push_back(PendingMessage {
            token,
            message_id,
            payload,
            fragment_count,
            next_fragment: 0,
        });
        Ok(token)
    }

    pub(super) fn set_peer_receive_window(&mut self, window: u16) {
        self.peer_receive_window = usize::from(window);
    }

    pub(super) fn apply_ack(
        &mut self,
        packet: &PacketMeta,
        now: Duration,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        if !packet.flags.contains(Flags::ACK) || packet.ack_largest == 0 {
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
        message_id: MessageId,
        now: Duration,
        config: &Config,
    ) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let Some(state) = self.messages.remove(&message_id) else {
            return output;
        };
        output
            .actions
            .push(OutboundAction::CancelTimer(TimerKind::MessageAckRetry {
                message_id,
            }));
        let frame_sequences: Vec<FrameSequence> = self
            .send_frames
            .iter()
            .filter_map(|(sequence, frame)| (frame.message_id == message_id).then_some(*sequence))
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
            output.terminal_error = Some(Error::RetransmitExhausted);
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
            && let Some(message) = self.messages.get_mut(&frame.message_id)
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
        message_id: MessageId,
        config: &Config,
        allocator: &A,
        metrics: &mut SessionMetrics,
    ) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        let Some(message) = self.messages.get_mut(&message_id) else {
            return output;
        };
        if message.message_ack_retries >= config.message_ack_max_retries {
            output.terminal_error = Some(Error::MessageAckTimeout);
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
            kind: TimerKind::MessageAckRetry { message_id },
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
            && let Some(message) = self.messages.get_mut(&frame.message_id)
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
        while self.send_frames.len() < limit {
            let Some(pending) = self.pending_send.pop_front() else {
                break;
            };
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
        flags: Flags,
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let packet = Packet::encode_control_into_with_limit(
            allocator,
            config.max_datagram_size,
            flags,
            connection_id,
            Ack::empty(),
            receive_window as u16,
        );
        output.actions.push(OutboundAction::Datagram {
            datagram: packet?.into_fixed_buf(),
            sequence: None,
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
        let packet = Packet::encode_control_into_with_limit(
            allocator,
            config.max_datagram_size,
            Flags::ACK,
            connection_id,
            ack.ack(),
            receive_window as u16,
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
        let packet = Packet::encode_message_ack_into_with_limit(
            context.allocator,
            context.config.max_datagram_size,
            context.connection_id,
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
        let flags = if ack.largest().is_some() {
            metrics.record_piggybacked_ack();
            Flags::DATA | Flags::ACK
        } else {
            Flags::DATA
        };
        let max_fragment_payload = context.config.max_fragment_payload();
        let offset = usize::try_from(pending.next_fragment)
            .ok()
            .and_then(|index| index.checked_mul(max_fragment_payload))
            .ok_or(Error::FragmentCountExceeded)?;
        let remaining = pending.payload.len().saturating_sub(offset);
        let fragment_len = remaining.min(max_fragment_payload);
        let payload = &pending.payload.as_slice()[offset..offset + fragment_len];
        let packet = Packet::encode_data_into_with_limit(
            context.allocator,
            context.config.max_datagram_size,
            DataPacket {
                flags,
                connection_id: context.connection_id,
                frame_sequence,
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
            .entry(pending.message_id)
            .or_insert(MessageTxState {
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
        if pending.next_fragment < pending.fragment_count {
            self.pending_send.push_front(pending);
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
        let Some(message) = self.messages.get_mut(&message_id) else {
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
}
