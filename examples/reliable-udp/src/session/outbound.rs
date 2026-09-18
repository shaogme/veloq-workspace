use tracing::debug;
use veloq::std::{
    collections::{HashMap, VecDeque},
    time::Duration,
    vec::Vec,
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{Ack, AckWindow, ConnectionId, Flags, HEADER_LEN, MessageSequence, Packet, PacketRef},
    timer::TimerKind,
};

use super::metrics::{OutboundView, SessionMetrics};
use super::{SendReceipt, SendToken};

pub(super) struct PendingSend {
    token: SendToken,
    payload: Vec<u8>,
}

struct TxEntry {
    token: SendToken,
    datagram: Vec<u8>,
    sent_at: Duration,
    rto: Duration,
    retransmitted: bool,
    retries: u8,
}

pub(super) enum OutboundAction {
    Datagram(Vec<u8>),
    ArmTimer {
        kind: TimerKind,
        delay: Duration,
    },
    CancelTimer(TimerKind),
    SendAcked(SendReceipt),
    SendFailed {
        token: SendToken,
        sequence: Option<MessageSequence>,
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

pub(super) struct FlushContext<'a> {
    connection_id: ConnectionId,
    config: &'a Config,
    now: Duration,
    ack: AckWindow,
    receive_window: usize,
}

impl<'a> FlushContext<'a> {
    pub(super) fn new(
        connection_id: ConnectionId,
        config: &'a Config,
        now: Duration,
        ack: AckWindow,
        receive_window: usize,
    ) -> Self {
        Self {
            connection_id,
            config,
            now,
            ack,
            receive_window,
        }
    }
}

pub(super) struct OutboundState {
    send_next_seq: MessageSequence,
    send_unacked: HashMap<MessageSequence, TxEntry>,
    pending_send: VecDeque<PendingSend>,
    next_token: u64,
    peer_receive_window: usize,
    congestion_window: usize,
    slow_start_threshold: usize,
    congestion_credit: usize,
}

impl OutboundState {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            send_next_seq: MessageSequence::new(1).expect("one is a valid message sequence"),
            send_unacked: HashMap::default(),
            pending_send: VecDeque::new(),
            next_token: 1,
            peer_receive_window: config.send_window.get(),
            congestion_window: config.send_window.get().min(2),
            slow_start_threshold: config.send_window.get(),
            congestion_credit: 0,
        }
    }

    pub(super) fn queue_send(
        &mut self,
        payload: Vec<u8>,
        max_payload: usize,
        capacity: usize,
    ) -> Result<SendToken> {
        if payload.len() > max_payload {
            return Err(Error::MessageTooLarge);
        }
        if self.pending_send.len() >= capacity {
            return Err(Error::SendWindowClosed);
        }
        let token = SendToken(self.next_token);
        self.next_token = self.next_token.wrapping_add(1);
        if self.next_token == 0 {
            self.next_token = 1;
        }
        self.pending_send.push_back(PendingSend { token, payload });
        Ok(token)
    }

    pub(super) fn set_peer_receive_window(&mut self, window: u16) {
        self.peer_receive_window = usize::from(window);
    }

    pub(super) fn apply_ack(
        &mut self,
        packet: &PacketRef<'_>,
        now: Duration,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        if !packet.flags.contains(Flags::ACK) || packet.ack_largest == 0 {
            return Ok(output);
        }
        let ack = packet.ack()?;
        let acked: Vec<MessageSequence> = self
            .send_unacked
            .keys()
            .copied()
            .filter(|sequence| ack.acknowledges(*sequence))
            .collect();
        if acked.is_empty() {
            metrics.record_duplicate_ack();
        }
        for sequence in acked {
            let Some(entry) = self.send_unacked.remove(&sequence) else {
                continue;
            };
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
            let rtt = (!entry.retransmitted).then(|| now.saturating_sub(entry.sent_at));
            if let Some(sample) = rtt {
                metrics.record_rtt(sample, config);
            }
            self.increase_congestion_window(1, config);
            debug!(
                target: "veloq_reliable_udp::session",
                sequence = sequence.get(),
                token = entry.token.get(),
                retransmissions = entry.retries,
                rtt = ?rtt,
                "session acknowledged data"
            );
            output.actions.push(OutboundAction::SendAcked(SendReceipt {
                token: entry.token,
                sequence,
                rtt,
                retransmissions: entry.retries,
            }));
        }
        Ok(output)
    }

    pub(super) fn on_retransmit(
        &mut self,
        sequence: MessageSequence,
        config: &Config,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let Some(entry) = self.send_unacked.get(&sequence) else {
            return Ok(output);
        };
        if entry.retries >= config.max_retries {
            output.terminal_error = Some(Error::RetransmitExhausted);
            return Ok(output);
        }
        let (datagram, rto) = {
            let entry = self
                .send_unacked
                .get_mut(&sequence)
                .expect("entry was checked above");
            entry.retries = entry.retries.saturating_add(1);
            entry.retransmitted = true;
            entry.rto = entry.rto.saturating_mul(2).min(config.max_rto);
            (entry.datagram.clone(), entry.rto)
        };
        metrics.record_retransmission();
        self.on_congestion_timeout();
        debug!(
            target: "veloq_reliable_udp::session",
            sequence = sequence.get(),
            retries = self.send_unacked.get(&sequence).map_or(0, |entry| entry.retries),
            next_rto = ?rto,
            "session retransmitting data"
        );
        output.actions.push(OutboundAction::ArmTimer {
            kind: TimerKind::Retransmit { sequence },
            delay: rto,
        });
        output.actions.push(OutboundAction::Datagram(datagram));
        Ok(output)
    }

    pub(super) fn flush_pending(
        &mut self,
        context: FlushContext<'_>,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let limit = self.send_limit(context.config);
        let mut current_ack = context.ack.ack();
        let rto = metrics.rto();
        while self.send_unacked.len() < limit {
            let Some(pending) = self.pending_send.pop_front() else {
                break;
            };
            let use_ack = current_ack.largest().is_some();
            self.emit_data(pending, &context, current_ack, rto, metrics, &mut output)?;
            if use_ack {
                output.ack_consumed = true;
                current_ack = Ack::empty();
            }
        }
        Ok(output)
    }

    pub(super) fn emit_control(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        flags: Flags,
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let packet = Packet::new(
            flags,
            connection_id,
            0,
            Ack::empty(),
            receive_window as u16,
            Vec::new(),
        );
        output.actions.push(OutboundAction::Datagram(
            packet.encode_with_limit(config.max_datagram_size.get())?,
        ));
        metrics.record_sent();
        Ok(output)
    }

    pub(super) fn emit_ack(
        &mut self,
        connection_id: ConnectionId,
        config: &Config,
        ack: AckWindow,
        receive_window: usize,
        metrics: &mut SessionMetrics,
    ) -> Result<OutboundOutput> {
        let mut output = OutboundOutput::new();
        let packet = Packet::new(
            Flags::ACK,
            connection_id,
            0,
            ack.ack(),
            receive_window as u16,
            Vec::new(),
        );
        output
            .actions
            .push(OutboundAction::CancelTimer(TimerKind::AckDelay));
        output.actions.push(OutboundAction::Datagram(
            packet.encode_with_limit(config.max_datagram_size.get())?,
        ));
        metrics.record_ack_sent();
        Ok(output)
    }

    pub(super) fn fail_all(&mut self, error: Error, _generation: u64) -> OutboundOutput {
        let mut output = OutboundOutput::new();
        for (sequence, entry) in self.send_unacked.drain() {
            output
                .actions
                .push(OutboundAction::CancelTimer(TimerKind::Retransmit {
                    sequence,
                }));
            output.actions.push(OutboundAction::SendFailed {
                token: entry.token,
                sequence: Some(sequence),
                error,
            });
        }
        for pending in self.pending_send.drain(..) {
            output.actions.push(OutboundAction::SendFailed {
                token: pending.token,
                sequence: None,
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
        self.send_unacked.len()
    }

    pub(super) fn pending_sends(&self) -> usize {
        self.pending_send.len()
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

    fn emit_data(
        &mut self,
        pending: PendingSend,
        context: &FlushContext<'_>,
        ack: Ack,
        rto: Duration,
        metrics: &mut SessionMetrics,
        output: &mut OutboundOutput,
    ) -> Result<()> {
        let sequence = self.send_next_seq;
        self.send_next_seq = sequence.next();
        let flags = if ack.largest().is_some() {
            metrics.record_piggybacked_ack();
            Flags::DATA | Flags::ACK
        } else {
            Flags::DATA
        };
        let packet = Packet::new(
            flags,
            context.connection_id,
            sequence.get(),
            ack,
            context.receive_window as u16,
            pending.payload,
        );
        let datagram = packet.encode_with_limit(context.config.max_datagram_size.get())?;
        output.actions.push(OutboundAction::ArmTimer {
            kind: TimerKind::Retransmit { sequence },
            delay: rto,
        });
        self.send_unacked.insert(
            sequence,
            TxEntry {
                token: pending.token,
                datagram: datagram.clone(),
                sent_at: context.now,
                rto,
                retransmitted: false,
                retries: 0,
            },
        );
        metrics.record_data_sent();
        debug!(
            target: "veloq_reliable_udp::session",
            sequence = sequence.get(),
            token = pending.token.get(),
            payload_len = datagram.len().saturating_sub(HEADER_LEN),
            rto = ?rto,
            "session emitted data"
        );
        output.actions.push(OutboundAction::Datagram(datagram));
        Ok(())
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
