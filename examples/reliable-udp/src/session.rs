use tracing::{debug, trace};
use veloq_std::{
    collections::{HashMap, VecDeque},
    time::Duration,
    vec::Vec,
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{
        Ack, AckObserve, AckWindow, ConnectionId, Flags, HEADER_LEN, MessageSequence, Packet,
        non_zero_sequence,
    },
    timer::{TimerCommand, TimerKind},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Listen,
    SynSent,
    SynReceived,
    Established,
    FinWait,
    CloseWait,
    Closed,
    Failed,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub sequence: MessageSequence,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn as_slice(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SendToken(u64);

impl SendToken {
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendReceipt {
    pub token: SendToken,
    pub sequence: MessageSequence,
    pub rtt: Option<Duration>,
    pub retransmissions: u8,
}

/// 累计协议计数器与当前发送状态的快照。
///
/// 计数器只在 `Session` 所属的端点驱动任务内更新，因此协议热路径不需要
/// 锁；端点可以在事件处理后把快照增量合并到跨 Worker 的原子指标中。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionStatsSnapshot {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub data_packets: u64,
    pub ack_packets: u64,
    pub retransmissions: u64,
    pub duplicate_packets: u64,
    pub dropped_packets: u64,
    pub receive_window_drops: u64,
    pub out_of_window_drops: u64,
    pub duplicate_acks: u64,
    pub rtt_samples: u64,
    pub latest_rtt: Option<Duration>,
    pub min_rtt: Option<Duration>,
    pub max_rtt: Option<Duration>,
    pub srtt: Option<Duration>,
    pub rto: Duration,
    pub send_window: usize,
    pub congestion_window: usize,
    pub slow_start_threshold: usize,
    pub peer_receive_window: usize,
    pub receive_window: usize,
    pub ack_delayed: u64,
    pub piggybacked_acks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    Outbound(Vec<u8>),
    ArmTimer(TimerCommand),
    CancelTimer(TimerCommand),
    MessageAvailable,
    SendAcked(SendReceipt),
    SendFailed {
        token: SendToken,
        sequence: Option<MessageSequence>,
        error: Error,
    },
    StateChanged(SessionState),
    Failed(Error),
}

struct PendingSend {
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

#[derive(Debug, Clone, Copy, Default)]
struct SessionStats {
    packets_sent: u64,
    packets_received: u64,
    data_packets: u64,
    ack_packets: u64,
    retransmissions: u64,
    duplicate_packets: u64,
    dropped_packets: u64,
    receive_window_drops: u64,
    out_of_window_drops: u64,
    duplicate_acks: u64,
    rtt_samples: u64,
    latest_rtt: Option<Duration>,
    min_rtt: Option<Duration>,
    max_rtt: Option<Duration>,
    ack_delayed: u64,
    piggybacked_acks: u64,
}

#[derive(Debug, Clone, Copy)]
struct RttEstimator {
    srtt: Option<Duration>,
    rttvar: Option<Duration>,
    rto: Duration,
}

impl RttEstimator {
    fn new(config: &Config) -> Self {
        Self {
            srtt: None,
            rttvar: None,
            rto: config.initial_rto,
        }
    }

    fn sample(&mut self, sample: Duration, config: &Config) {
        match (self.srtt, self.rttvar) {
            (None, None) => {
                self.srtt = Some(sample);
                self.rttvar = Some(half(sample));
            }
            (Some(srtt), Some(rttvar)) => {
                let variation = sample.abs_diff(srtt);
                self.rttvar = Some(weighted_duration(rttvar, 3, variation, 1, 4));
                self.srtt = Some(weighted_duration(srtt, 7, sample, 1, 8));
            }
            _ => {
                self.srtt = Some(sample);
                self.rttvar = Some(half(sample));
            }
        }

        let srtt = self.srtt.expect("RTT sample sets srtt");
        let rttvar = self.rttvar.expect("RTT sample sets rttvar");
        let estimate = srtt
            .checked_add(rttvar.saturating_mul(4))
            .unwrap_or(config.max_rto);
        self.rto = estimate.clamp(config.min_rto, config.max_rto);
    }
}

/// A single-connection protocol state machine.
///
/// `Session` has no socket dependency. It accepts complete datagrams and
/// emits complete datagrams through [`SessionEvent::Outbound`], which makes
/// loss, duplication, delay, reordering, and clock progression testable in
/// isolation from the Veloq endpoint adapter.
pub struct Session {
    role: Role,
    state: SessionState,
    connection_id: ConnectionId,
    config: Config,
    generation: u64,
    now: Duration,
    handshake_timer_armed: bool,
    handshake_started: Option<Duration>,
    handshake_rto: Duration,
    handshake_retries: u8,
    fin_timer_armed: bool,
    fin_retries: u8,
    send_next_seq: MessageSequence,
    send_unacked: HashMap<MessageSequence, TxEntry>,
    pending_send: VecDeque<PendingSend>,
    next_token: u64,
    peer_receive_window: usize,
    congestion_window: usize,
    slow_start_threshold: usize,
    congestion_credit: usize,
    recv_ack: AckWindow,
    recv_reorder: HashMap<MessageSequence, Vec<u8>>,
    recv_ready: VecDeque<Message>,
    next_deliver_seq: MessageSequence,
    ack_timer_armed: bool,
    events: VecDeque<SessionEvent>,
    rtt: RttEstimator,
    stats: SessionStats,
    pending_ack_packets: usize,
}

impl Session {
    pub fn new_client(connection_id: ConnectionId, config: Config) -> Result<Self> {
        Self::new(Role::Client, SessionState::SynSent, connection_id, config)
    }

    pub fn new_server(connection_id: ConnectionId, config: Config) -> Result<Self> {
        Self::new(Role::Server, SessionState::Listen, connection_id, config)
    }

    fn new(
        role: Role,
        state: SessionState,
        connection_id: ConnectionId,
        config: Config,
    ) -> Result<Self> {
        config.validate()?;
        let peer_receive_window = config.send_window.get();
        let initial_congestion_window = config.send_window.get().min(2);
        let slow_start_threshold = config.send_window.get();
        let handshake_initial_rto = config.handshake_initial_rto;
        let rtt = RttEstimator::new(&config);
        Ok(Self {
            role,
            state,
            connection_id,
            config,
            generation: 1,
            now: Duration::ZERO,
            handshake_timer_armed: false,
            handshake_started: None,
            handshake_rto: handshake_initial_rto,
            handshake_retries: 0,
            fin_timer_armed: false,
            fin_retries: 0,
            send_next_seq: MessageSequence::new(1).expect("one is a valid message sequence"),
            send_unacked: HashMap::default(),
            pending_send: VecDeque::new(),
            next_token: 1,
            peer_receive_window,
            congestion_window: initial_congestion_window,
            slow_start_threshold,
            congestion_credit: 0,
            recv_ack: AckWindow::new(),
            recv_reorder: HashMap::default(),
            recv_ready: VecDeque::new(),
            next_deliver_seq: MessageSequence::new(1).expect("one is a valid message sequence"),
            ack_timer_armed: false,
            events: VecDeque::new(),
            rtt,
            stats: SessionStats::default(),
            pending_ack_packets: 0,
        })
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn now(&self) -> Duration {
        self.now
    }

    pub fn rto(&self) -> Duration {
        self.rtt.rto
    }

    pub fn congestion_window(&self) -> usize {
        self.congestion_window
    }

    pub fn effective_send_window(&self) -> usize {
        self.send_limit()
    }

    pub fn stats(&self) -> SessionStatsSnapshot {
        SessionStatsSnapshot {
            packets_sent: self.stats.packets_sent,
            packets_received: self.stats.packets_received,
            data_packets: self.stats.data_packets,
            ack_packets: self.stats.ack_packets,
            retransmissions: self.stats.retransmissions,
            duplicate_packets: self.stats.duplicate_packets,
            dropped_packets: self.stats.dropped_packets,
            receive_window_drops: self.stats.receive_window_drops,
            out_of_window_drops: self.stats.out_of_window_drops,
            duplicate_acks: self.stats.duplicate_acks,
            rtt_samples: self.stats.rtt_samples,
            latest_rtt: self.stats.latest_rtt,
            min_rtt: self.stats.min_rtt,
            max_rtt: self.stats.max_rtt,
            srtt: self.rtt.srtt,
            rto: self.rtt.rto,
            send_window: self.config.send_window.get(),
            congestion_window: self.congestion_window,
            slow_start_threshold: self.slow_start_threshold,
            peer_receive_window: self.peer_receive_window,
            receive_window: self.available_receive_window(),
            ack_delayed: self.stats.ack_delayed,
            piggybacked_acks: self.stats.piggybacked_acks,
        }
    }

    pub fn in_flight(&self) -> usize {
        self.send_unacked.len()
    }

    pub fn pending_sends(&self) -> usize {
        self.pending_send.len()
    }

    pub fn buffered_messages(&self) -> usize {
        self.recv_ready.len() + self.recv_reorder.len()
    }

    pub fn available_receive_window(&self) -> usize {
        self.config
            .receive_window
            .get()
            .saturating_sub(self.buffered_messages())
    }

    pub fn start(&mut self, now: Duration) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        if self.role != Role::Client || self.state != SessionState::SynSent {
            return Err(Error::InvalidState);
        }
        self.emit_control(Flags::SYN)?;
        self.begin_handshake();
        self.schedule_handshake_timer()?;
        Ok(self.drain_events())
    }

    pub fn queue_send(&mut self, now: Duration, payload: Vec<u8>) -> Result<SendToken> {
        self.sync_now(now);
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Err(match self.state {
                SessionState::Reset => Error::ConnectionReset,
                _ => Error::ConnectionClosed,
            });
        }
        if payload.len() > self.config.max_payload() {
            return Err(Error::MessageTooLarge);
        }
        if self.pending_send.len() >= self.config.pending_send_capacity.get() {
            return Err(Error::SendWindowClosed);
        }

        let token = SendToken(self.next_token);
        self.next_token = self.next_token.wrapping_add(1);
        if self.next_token == 0 {
            self.next_token = 1;
        }
        self.pending_send.push_back(PendingSend { token, payload });
        self.flush_pending()?;
        Ok(token)
    }

    pub fn receive(&mut self, now: Duration, datagram: &[u8]) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Err(match self.state {
                SessionState::Reset => Error::ConnectionReset,
                _ => Error::ConnectionClosed,
            });
        }
        let packet = Packet::decode(datagram)?;
        if packet.connection_id != self.connection_id {
            return Err(Error::UnknownConnection);
        }

        self.stats.packets_received = self.stats.packets_received.saturating_add(1);
        if packet.flags.contains(Flags::DATA) {
            self.stats.data_packets = self.stats.data_packets.saturating_add(1);
        }
        if packet.flags.contains(Flags::ACK) {
            self.stats.ack_packets = self.stats.ack_packets.saturating_add(1);
        }

        trace!(
            target: "veloq_reliable_udp::session",
            role = ?self.role,
            state = ?self.state,
            connection_id = self.connection_id.get(),
            flags = packet.flags.bits(),
            sequence = packet.sequence,
            ack_largest = packet.ack_largest,
            ack_bitmap = packet.ack_bitmap,
            payload_len = packet.payload.len(),
            logical_now = ?self.now,
            "session received packet"
        );

        self.peer_receive_window = usize::from(packet.receive_window);
        self.apply_ack(&packet)?;

        if packet.flags.contains(Flags::RST) {
            self.terminate(Error::ConnectionReset, SessionState::Reset);
            return Ok(self.drain_events());
        }

        if packet.flags.contains(Flags::SYN) {
            self.handle_syn(packet.receive_window)?;
        }
        if packet.flags.contains(Flags::SYN_ACK) {
            self.handle_syn_ack(packet.receive_window)?;
        }
        if packet.flags.contains(Flags::ACK)
            && self.role == Role::Server
            && self.state == SessionState::SynReceived
            && !packet.flags.contains(Flags::DATA)
        {
            self.establish();
        }
        if packet.flags.contains(Flags::FIN) {
            self.handle_fin()?;
        }
        if packet.flags.contains(Flags::FIN_ACK) && self.state == SessionState::FinWait {
            self.cancel_fin_timer();
            self.transition(SessionState::Closed);
        }
        if packet.flags.contains(Flags::DATA) {
            if self.role == Role::Server && self.state == SessionState::SynReceived {
                self.establish();
            }
            self.handle_data(packet)?;
        }

        self.flush_pending()?;
        Ok(self.drain_events())
    }

    pub fn on_timer(
        &mut self,
        now: Duration,
        kind: TimerKind,
        generation: u64,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Ok(Vec::new());
        }
        if generation != self.generation {
            return Ok(Vec::new());
        }
        self.handle_timer(kind)?;
        Ok(self.drain_events())
    }

    pub fn recv(&mut self, now: Duration) -> Option<Message> {
        self.sync_now(now);
        let message = self.recv_ready.pop_front();
        if message.is_some() {
            let _ = self.emit_ack();
        }
        message
    }

    pub fn drain_events(&mut self) -> Vec<SessionEvent> {
        self.events.drain(..).collect()
    }

    pub fn close(&mut self, now: Duration) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        match self.state {
            SessionState::Established | SessionState::CloseWait => {
                self.emit_control(Flags::FIN)?;
                self.transition(SessionState::FinWait);
                self.schedule_fin_timer()?;
                Ok(self.drain_events())
            }
            SessionState::Closed => Ok(Vec::new()),
            SessionState::Failed | SessionState::Reset => Err(Error::ConnectionClosed),
            _ => {
                self.cancel_handshake_timer();
                self.cancel_fin_timer();
                self.cancel_ack_timer();
                self.transition(SessionState::Closed);
                Ok(self.drain_events())
            }
        }
    }

    pub fn abort(&mut self, now: Duration, error: Error) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Ok(Vec::new());
        }
        self.emit_control(Flags::RST)?;
        self.terminate(error, SessionState::Failed);
        Ok(self.drain_events())
    }

    fn sync_now(&mut self, now: Duration) {
        self.now = self.now.max(now);
    }

    fn handle_syn(&mut self, receive_window: u16) -> Result<()> {
        if self.role != Role::Server {
            return Ok(());
        }
        self.peer_receive_window = usize::from(receive_window);
        match self.state {
            SessionState::Listen => {
                self.transition(SessionState::SynReceived);
                self.begin_handshake();
                self.emit_control(Flags::SYN_ACK)?;
                self.schedule_handshake_timer()?;
            }
            SessionState::SynReceived => {
                self.emit_control(Flags::SYN_ACK)?;
            }
            SessionState::Established => {
                self.emit_control(Flags::SYN_ACK)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_syn_ack(&mut self, receive_window: u16) -> Result<()> {
        if self.role != Role::Client {
            return Ok(());
        }
        self.peer_receive_window = usize::from(receive_window);
        match self.state {
            SessionState::SynSent => {
                self.cancel_handshake_timer();
                self.establish();
                self.emit_control(Flags::ACK)?;
            }
            SessionState::Established => {
                self.emit_control(Flags::ACK)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_data(&mut self, packet: Packet) -> Result<()> {
        if self.state != SessionState::Established {
            return Ok(());
        }
        let sequence = non_zero_sequence(packet.sequence)?;
        if self.recv_ack.contains(sequence)
            || self.recv_reorder.contains_key(&sequence)
            || self
                .recv_ready
                .iter()
                .any(|message| message.sequence == sequence)
        {
            self.stats.duplicate_packets = self.stats.duplicate_packets.saturating_add(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                "session suppressed duplicate data and emitted ACK"
            );
            self.emit_ack()?;
            return Ok(());
        }

        let Some(distance) = MessageSequence::forward_distance(self.next_deliver_seq, sequence)
        else {
            self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                "session suppressed data outside sequence space"
            );
            self.emit_ack_if_available()?;
            return Ok(());
        };
        if distance >= self.config.receive_window.get() as u64 {
            self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
            self.stats.out_of_window_drops = self.stats.out_of_window_drops.saturating_add(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                distance,
                "session suppressed data outside receive window"
            );
            self.emit_ack_if_available()?;
            return Ok(());
        }
        if self.buffered_messages() >= self.config.receive_window.get() {
            self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
            self.stats.receive_window_drops = self.stats.receive_window_drops.saturating_add(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                "session suppressed data because receive window is full"
            );
            return Ok(());
        }

        let observation = self.recv_ack.observe(sequence);
        if matches!(observation, AckObserve::Duplicate | AckObserve::TooOld) {
            self.stats.duplicate_packets = self.stats.duplicate_packets.saturating_add(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                observation = ?observation,
                "session suppressed data after ACK-window observation"
            );
            self.emit_ack_if_available()?;
            return Ok(());
        }
        self.recv_reorder.insert(sequence, packet.payload);
        let delivered = self.deliver_ready();
        debug!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            sequence = sequence.get(),
            distance,
            observation = ?observation,
            delivered,
            "session accepted data"
        );
        self.pending_ack_packets = self.pending_ack_packets.saturating_add(1);
        if distance == 0
            && delivered > 0
            && self.pending_ack_packets < self.config.ack_batch_size.get()
        {
            self.stats.ack_delayed = self.stats.ack_delayed.saturating_add(1);
            self.schedule_ack_timer()?;
        } else {
            self.emit_ack()?;
        }
        Ok(())
    }

    fn deliver_ready(&mut self) -> usize {
        let mut delivered = 0;
        while let Some(payload) = self.recv_reorder.remove(&self.next_deliver_seq) {
            if self.recv_ready.len() >= self.config.receive_window.get() {
                self.recv_reorder.insert(self.next_deliver_seq, payload);
                break;
            }
            let sequence = self.next_deliver_seq;
            self.recv_ready.push_back(Message { sequence, payload });
            self.next_deliver_seq = sequence.next();
            self.events.push_back(SessionEvent::MessageAvailable);
            delivered += 1;
        }
        delivered
    }

    fn apply_ack(&mut self, packet: &Packet) -> Result<()> {
        if !packet.flags.contains(Flags::ACK) || packet.ack_largest == 0 {
            return Ok(());
        }
        let ack = packet.ack()?;
        let acked: Vec<MessageSequence> = self
            .send_unacked
            .keys()
            .copied()
            .filter(|sequence| ack.acknowledges(*sequence))
            .collect();
        let acked_count = acked.len();
        for sequence in acked {
            let Some(entry) = self.send_unacked.remove(&sequence) else {
                continue;
            };
            self.cancel_retransmit(sequence);
            let rtt = (!entry.retransmitted).then(|| self.now.saturating_sub(entry.sent_at));
            if let Some(sample) = rtt {
                self.rtt.sample(sample, &self.config);
                self.stats.rtt_samples = self.stats.rtt_samples.saturating_add(1);
                self.stats.latest_rtt = Some(sample);
                self.stats.min_rtt = Some(
                    self.stats
                        .min_rtt
                        .map_or(sample, |current| current.min(sample)),
                );
                self.stats.max_rtt = Some(
                    self.stats
                        .max_rtt
                        .map_or(sample, |current| current.max(sample)),
                );
            }
            self.increase_congestion_window(1);
            debug!(
                target: "veloq_reliable_udp::session",
                connection_id = self.connection_id.get(),
                sequence = sequence.get(),
                token = entry.token.get(),
                retransmissions = entry.retries,
                rtt = ?rtt,
                "session acknowledged data"
            );
            self.events.push_back(SessionEvent::SendAcked(SendReceipt {
                token: entry.token,
                sequence,
                rtt,
                retransmissions: entry.retries,
            }));
        }
        if acked_count == 0 {
            self.stats.duplicate_acks = self.stats.duplicate_acks.saturating_add(1);
        }
        Ok(())
    }

    fn handle_fin(&mut self) -> Result<()> {
        if matches!(
            self.state,
            SessionState::Established | SessionState::SynReceived
        ) {
            self.cancel_handshake_timer();
            self.transition(SessionState::CloseWait);
            self.emit_control(Flags::FIN_ACK)?;
            self.transition(SessionState::Closed);
        }
        Ok(())
    }

    fn handle_timer(&mut self, kind: TimerKind) -> Result<()> {
        match kind {
            TimerKind::Retransmit { sequence } => self.handle_retransmit(sequence)?,
            TimerKind::HandshakeRetry => {
                self.handshake_timer_armed = false;
                self.handle_handshake_timeout()?;
            }
            TimerKind::AckDelay => {
                self.ack_timer_armed = false;
                self.emit_ack_if_available()?;
            }
            TimerKind::FinRetry => {
                self.fin_timer_armed = false;
                self.handle_fin_timeout()?;
            }
            TimerKind::IdleTimeout | TimerKind::KeepAlive => {}
        }
        Ok(())
    }

    fn handle_retransmit(&mut self, sequence: MessageSequence) -> Result<()> {
        let Some(entry) = self.send_unacked.get(&sequence) else {
            return Ok(());
        };
        if entry.retries >= self.config.max_retries {
            self.terminate(Error::RetransmitExhausted, SessionState::Failed);
            return Ok(());
        }

        let (datagram, rto) = {
            let entry = self
                .send_unacked
                .get_mut(&sequence)
                .expect("entry was checked above");
            entry.retries = entry.retries.saturating_add(1);
            entry.retransmitted = true;
            entry.rto = entry.rto.saturating_mul(2).min(self.config.max_rto);
            (entry.datagram.clone(), entry.rto)
        };
        self.stats.retransmissions = self.stats.retransmissions.saturating_add(1);
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        self.stats.data_packets = self.stats.data_packets.saturating_add(1);
        self.on_congestion_timeout();
        debug!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            sequence = sequence.get(),
            retries = self
                .send_unacked
                .get(&sequence)
                .map_or(0, |entry| entry.retries),
            next_rto = ?rto,
            "session retransmitting data"
        );
        self.arm_timer(TimerKind::Retransmit { sequence }, rto);
        self.events.push_back(SessionEvent::Outbound(datagram));
        Ok(())
    }

    fn handle_handshake_timeout(&mut self) -> Result<()> {
        if !matches!(
            self.state,
            SessionState::SynSent | SessionState::SynReceived
        ) {
            return Ok(());
        }
        let elapsed = self
            .handshake_started
            .map_or(self.config.handshake_deadline, |started| {
                self.now.saturating_sub(started)
            });
        if elapsed >= self.config.handshake_deadline
            || self.handshake_retries >= self.config.handshake_max_retries
        {
            self.terminate(Error::HandshakeTimeout, SessionState::Failed);
            return Ok(());
        }
        self.handshake_retries = self.handshake_retries.saturating_add(1);
        self.handshake_rto = self
            .handshake_rto
            .saturating_mul(2)
            .min(self.config.handshake_max_rto);
        let flags = match self.role {
            Role::Client => Flags::SYN,
            Role::Server => Flags::SYN_ACK,
        };
        self.emit_control(flags)?;
        self.schedule_handshake_timer()
    }

    fn handle_fin_timeout(&mut self) -> Result<()> {
        if self.state != SessionState::FinWait {
            return Ok(());
        }
        if self.fin_retries >= self.config.max_retries {
            self.emit_control(Flags::RST)?;
            self.terminate(Error::CloseTimeout, SessionState::Failed);
            return Ok(());
        }
        self.fin_retries = self.fin_retries.saturating_add(1);
        self.on_congestion_timeout();
        self.emit_control(Flags::FIN)?;
        self.schedule_fin_timer()
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.state != SessionState::Established {
            return Ok(());
        }
        let limit = self.send_limit();
        while self.send_unacked.len() < limit {
            let Some(pending) = self.pending_send.pop_front() else {
                break;
            };
            self.emit_data(pending)?;
        }
        Ok(())
    }

    fn emit_data(&mut self, pending: PendingSend) -> Result<()> {
        let sequence = self.send_next_seq;
        self.send_next_seq = sequence.next();
        let ack = self.recv_ack.ack();
        let flags = if ack.largest().is_some() {
            self.stats.piggybacked_acks = self.stats.piggybacked_acks.saturating_add(1);
            self.pending_ack_packets = 0;
            Flags::DATA | Flags::ACK
        } else {
            Flags::DATA
        };
        let packet = Packet::new(
            flags,
            self.connection_id,
            sequence.get(),
            ack,
            self.available_receive_window() as u16,
            pending.payload,
        );
        let datagram = packet.encode_with_limit(self.config.max_datagram_size.get())?;
        self.cancel_ack_timer();
        self.arm_timer(TimerKind::Retransmit { sequence }, self.rtt.rto);
        self.send_unacked.insert(
            sequence,
            TxEntry {
                token: pending.token,
                datagram: datagram.clone(),
                sent_at: self.now,
                rto: self.rtt.rto,
                retransmitted: false,
                retries: 0,
            },
        );
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        self.stats.data_packets = self.stats.data_packets.saturating_add(1);
        debug!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            sequence = sequence.get(),
            token = pending.token.get(),
            payload_len = datagram.len().saturating_sub(HEADER_LEN),
            rto = ?self.rtt.rto,
            "session emitted data"
        );
        self.events.push_back(SessionEvent::Outbound(datagram));
        Ok(())
    }

    fn emit_control(&mut self, flags: Flags) -> Result<()> {
        let packet = Packet::new(
            flags,
            self.connection_id,
            0,
            Ack::empty(),
            self.available_receive_window() as u16,
            Vec::new(),
        );
        let datagram = packet.encode_with_limit(self.config.max_datagram_size.get())?;
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        debug!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            flags = flags.bits(),
            "session emitted control packet"
        );
        self.events.push_back(SessionEvent::Outbound(datagram));
        Ok(())
    }

    fn emit_ack(&mut self) -> Result<()> {
        self.cancel_ack_timer();
        let ack = self.recv_ack.ack();
        let packet = Packet::new(
            Flags::ACK,
            self.connection_id,
            0,
            ack,
            self.available_receive_window() as u16,
            Vec::new(),
        );
        let datagram = packet.encode_with_limit(self.config.max_datagram_size.get())?;
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        self.stats.ack_packets = self.stats.ack_packets.saturating_add(1);
        self.pending_ack_packets = 0;
        debug!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            ack_largest = packet.ack_largest,
            ack_bitmap = packet.ack_bitmap,
            "session emitted ACK"
        );
        self.events.push_back(SessionEvent::Outbound(datagram));
        Ok(())
    }

    fn emit_ack_if_available(&mut self) -> Result<()> {
        if self.recv_ack.is_empty() {
            Ok(())
        } else {
            self.emit_ack()
        }
    }

    fn send_limit(&self) -> usize {
        self.config
            .send_window
            .get()
            .min(self.peer_receive_window)
            .min(self.congestion_window)
    }

    fn increase_congestion_window(&mut self, acknowledged: usize) {
        let max_window = self.config.send_window.get();
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

    fn on_congestion_timeout(&mut self) {
        self.slow_start_threshold = (self.congestion_window / 2).max(1);
        self.congestion_window = 1;
        self.congestion_credit = 0;
    }

    fn schedule_handshake_timer(&mut self) -> Result<()> {
        self.cancel_handshake_timer();
        let remaining = self
            .handshake_started
            .map(|started| {
                self.config
                    .handshake_deadline
                    .saturating_sub(self.now.saturating_sub(started))
            })
            .unwrap_or(self.config.handshake_deadline);
        let delay = self.handshake_rto.min(remaining);
        self.handshake_timer_armed = true;
        self.arm_timer(TimerKind::HandshakeRetry, delay);
        Ok(())
    }

    fn schedule_ack_timer(&mut self) -> Result<()> {
        if self.ack_timer_armed || self.recv_ack.is_empty() {
            return Ok(());
        }
        self.ack_timer_armed = true;
        self.arm_timer(TimerKind::AckDelay, self.config.ack_delay);
        Ok(())
    }

    fn schedule_fin_timer(&mut self) -> Result<()> {
        self.cancel_fin_timer();
        self.fin_timer_armed = true;
        self.arm_timer(TimerKind::FinRetry, self.config.close_timeout);
        Ok(())
    }

    fn cancel_handshake_timer(&mut self) {
        if self.handshake_timer_armed {
            self.handshake_timer_armed = false;
            self.cancel_timer(TimerKind::HandshakeRetry);
        }
    }

    fn cancel_fin_timer(&mut self) {
        if self.fin_timer_armed {
            self.fin_timer_armed = false;
            self.cancel_timer(TimerKind::FinRetry);
        }
    }

    fn cancel_ack_timer(&mut self) {
        if self.ack_timer_armed {
            self.ack_timer_armed = false;
            self.cancel_timer(TimerKind::AckDelay);
        }
    }

    fn arm_timer(&mut self, kind: TimerKind, delay: Duration) {
        trace!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            kind = ?kind,
            generation = self.generation,
            delay = ?delay,
            "session armed timer"
        );
        self.events
            .push_back(SessionEvent::ArmTimer(TimerCommand::Arm {
                kind,
                generation: self.generation,
                delay,
            }));
    }

    fn cancel_timer(&mut self, kind: TimerKind) {
        trace!(
            target: "veloq_reliable_udp::session",
            connection_id = self.connection_id.get(),
            kind = ?kind,
            generation = self.generation,
            "session cancelled timer"
        );
        self.events
            .push_back(SessionEvent::CancelTimer(TimerCommand::Cancel {
                kind,
                generation: self.generation,
            }));
    }

    fn cancel_retransmit(&mut self, sequence: MessageSequence) {
        self.cancel_timer(TimerKind::Retransmit { sequence });
    }

    fn establish(&mut self) {
        if self.state != SessionState::Established {
            self.cancel_handshake_timer();
            self.handshake_started = None;
            self.transition(SessionState::Established);
        }
    }

    fn begin_handshake(&mut self) {
        self.handshake_started = Some(self.now);
        self.handshake_rto = self.config.handshake_initial_rto;
        self.handshake_retries = 0;
    }

    fn transition(&mut self, state: SessionState) {
        if self.state != state {
            self.state = state;
            self.events.push_back(SessionEvent::StateChanged(state));
        }
    }

    fn terminate(&mut self, error: Error, state: SessionState) {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return;
        }
        let generation = self.generation;
        self.cancel_handshake_timer();
        self.handshake_started = None;
        self.cancel_fin_timer();
        self.cancel_ack_timer();
        for (sequence, entry) in self.send_unacked.drain() {
            self.events
                .push_back(SessionEvent::CancelTimer(TimerCommand::Cancel {
                    kind: TimerKind::Retransmit { sequence },
                    generation,
                }));
            self.events.push_back(SessionEvent::SendFailed {
                token: entry.token,
                sequence: Some(sequence),
                error,
            });
        }
        self.generation = self.generation.wrapping_add(1);
        for pending in self.pending_send.drain(..) {
            self.events.push_back(SessionEvent::SendFailed {
                token: pending.token,
                sequence: None,
                error,
            });
        }
        self.state = state;
        self.events.push_back(SessionEvent::StateChanged(state));
        self.events.push_back(SessionEvent::Failed(error));
    }
}

fn half(duration: Duration) -> Duration {
    duration / 2
}

fn weighted_duration(
    first: Duration,
    first_weight: u32,
    second: Duration,
    second_weight: u32,
    divisor: u32,
) -> Duration {
    let first_nanos = first.as_nanos().saturating_mul(u128::from(first_weight));
    let second_nanos = second.as_nanos().saturating_mul(u128::from(second_weight));
    let nanos = first_nanos.saturating_add(second_nanos) / u128::from(divisor);
    let nanos = nanos.min(u128::from(u64::MAX) * 1_000_000_000 + 999_999_999);
    Duration::from_nanos(
        u64::try_from(nanos / 1_000_000_000)
            .unwrap_or(u64::MAX)
            .saturating_mul(1_000_000_000)
            .saturating_add(u64::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999)),
    )
}
