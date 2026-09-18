mod inbound;
mod lifecycle;
mod metrics;
mod outbound;
mod timers;

use veloq::{
    buf::FixedBuf,
    std::{collections::VecDeque, time::Duration, vec::Vec},
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{ConnectionId, Flags, MessageSequence, PacketBufAllocator, PacketRef},
    timer::{TimerCommand, TimerKind},
};

use self::{
    inbound::InboundState,
    lifecycle::{LifecycleAction, LifecycleState},
    metrics::SessionMetrics,
    outbound::{FlushContext, OutboundAction, OutboundState},
    timers::TimerState,
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

#[derive(Debug)]
pub struct Message {
    pub sequence: MessageSequence,
    pub payload: FixedBuf,
}

impl Message {
    pub fn as_slice(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn into_fixed_buf(self) -> FixedBuf {
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

#[derive(Debug)]
pub enum SessionEvent {
    Outbound {
        datagram: FixedBuf,
        retain_for_retransmit: Option<MessageSequence>,
    },
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

impl PartialEq for SessionEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Outbound {
                    retain_for_retransmit: left,
                    ..
                },
                Self::Outbound {
                    retain_for_retransmit: right,
                    ..
                },
            ) => left == right,
            (Self::ArmTimer(left), Self::ArmTimer(right))
            | (Self::CancelTimer(left), Self::CancelTimer(right)) => left == right,
            (Self::MessageAvailable, Self::MessageAvailable) => true,
            (Self::SendAcked(left), Self::SendAcked(right)) => left == right,
            (
                Self::SendFailed {
                    token: left_token,
                    sequence: left_sequence,
                    error: left_error,
                },
                Self::SendFailed {
                    token: right_token,
                    sequence: right_sequence,
                    error: right_error,
                },
            ) => {
                left_token == right_token
                    && left_sequence == right_sequence
                    && left_error == right_error
            }
            (Self::StateChanged(left), Self::StateChanged(right)) => left == right,
            (Self::Failed(left), Self::Failed(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for SessionEvent {}

/// 单连接可靠 UDP 协议状态机。
///
/// `Session` 是组件组合根，不持有 socket。协议算法分别由生命周期、收发窗口、
/// 指标和逻辑定时器组件拥有；本模块只固定组件调用顺序并统一收集事件。
pub struct Session {
    config: Config,
    now: Duration,
    lifecycle: LifecycleState,
    inbound: InboundState,
    outbound: OutboundState,
    metrics: SessionMetrics,
    timers: TimerState,
    events: VecDeque<SessionEvent>,
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
        Ok(Self {
            lifecycle: LifecycleState::new(role, state, connection_id, &config),
            inbound: InboundState::new(),
            outbound: OutboundState::new(&config),
            metrics: SessionMetrics::new(&config),
            timers: TimerState::new(),
            config,
            now: Duration::ZERO,
            events: VecDeque::new(),
        })
    }

    pub fn role(&self) -> Role {
        self.lifecycle.role()
    }

    pub fn state(&self) -> SessionState {
        self.lifecycle.state()
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.lifecycle.connection_id()
    }

    pub fn now(&self) -> Duration {
        self.now
    }

    pub fn rto(&self) -> Duration {
        self.metrics.rto()
    }

    pub fn congestion_window(&self) -> usize {
        self.outbound.congestion_window()
    }

    pub fn effective_send_window(&self) -> usize {
        self.outbound.send_limit(&self.config)
    }

    pub fn in_flight(&self) -> usize {
        self.outbound.in_flight()
    }

    pub fn pending_sends(&self) -> usize {
        self.outbound.pending_sends()
    }

    pub fn buffered_messages(&self) -> usize {
        self.inbound.buffered_messages()
    }

    pub fn available_receive_window(&self) -> usize {
        self.inbound.available_window(&self.config)
    }

    pub fn stats(&self) -> SessionStatsSnapshot {
        self.metrics.snapshot(
            &self.config,
            self.outbound.view(&self.config),
            self.available_receive_window(),
        )
    }

    pub(crate) fn take_events(&mut self) -> Vec<SessionEvent> {
        self.events.drain(..).collect()
    }

    pub fn start<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let actions = self.lifecycle.start(self.now, &self.config)?;
        self.apply_lifecycle(actions, allocator)?;
        Ok(self.take_events())
    }

    pub fn receive<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        datagram: FixedBuf,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let packet = PacketRef::decode(datagram.as_slice())?;
        let meta = packet.meta();
        self.ensure_receivable(meta.connection_id)?;
        self.metrics.record_received(
            meta.flags.contains(Flags::DATA),
            meta.flags.contains(Flags::ACK),
        );
        self.outbound.set_peer_receive_window(meta.receive_window);

        let outbound = self
            .outbound
            .apply_ack(&meta, self.now, &self.config, &mut self.metrics)?;
        self.apply_outbound(outbound);

        if meta.flags.contains(Flags::RST) {
            let actions = self
                .lifecycle
                .request_terminate(Error::ConnectionReset, SessionState::Reset);
            self.apply_lifecycle(actions, allocator)?;
            return Ok(self.take_events());
        }
        if meta.flags.contains(Flags::SYN) {
            let actions = self
                .lifecycle
                .on_syn(self.now, meta.receive_window, &self.config);
            self.apply_lifecycle(actions, allocator)?;
        }
        if meta.flags.contains(Flags::SYN_ACK) {
            let actions = self.lifecycle.on_syn_ack(meta.receive_window);
            self.apply_lifecycle(actions, allocator)?;
        }
        if meta.flags.contains(Flags::ACK)
            && self.role() == Role::Server
            && self.state() == SessionState::SynReceived
            && !meta.flags.contains(Flags::DATA)
        {
            let actions = self.lifecycle.establish();
            self.apply_lifecycle(actions, allocator)?;
        }
        if meta.flags.contains(Flags::FIN) {
            let actions = self.lifecycle.on_fin();
            self.apply_lifecycle(actions, allocator)?;
        }
        if packet.flags.contains(Flags::FIN_ACK) && self.state() == SessionState::FinWait {
            let actions = self.lifecycle.on_fin_ack();
            self.apply_lifecycle(actions, allocator)?;
        }
        if meta.flags.contains(Flags::DATA) {
            if self.role() == Role::Server && self.state() == SessionState::SynReceived {
                let actions = self.lifecycle.establish();
                self.apply_lifecycle(actions, allocator)?;
            }
            let output = self.inbound.on_data(
                datagram,
                meta,
                &self.config,
                self.connection_id(),
                &mut self.metrics,
            )?;
            for _ in 0..output.delivered() {
                self.events.push_back(SessionEvent::MessageAvailable);
            }
            self.apply_inbound_ack(output.ack(), allocator)?;
        }

        self.flush_pending(allocator)?;
        Ok(self.take_events())
    }

    pub fn queue_send<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        payload: FixedBuf,
        allocator: &A,
    ) -> Result<SendToken> {
        self.sync_now(now);
        self.ensure_open_for_send()?;
        let token = self.outbound.queue_send(
            payload,
            self.config.max_payload(),
            self.config.pending_send_capacity.get(),
        )?;
        let _ = self.flush_pending(allocator)?;
        Ok(token)
    }

    pub fn recv<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        allocator: &A,
    ) -> Option<Message> {
        self.sync_now(now);
        let message = self.inbound.pop_message();
        if message.is_some() {
            let _ = self.emit_ack(allocator);
        }
        message
    }

    pub fn close<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let actions = self.lifecycle.request_close(self.now, &self.config)?;
        self.apply_lifecycle(actions, allocator)?;
        if self.state() == SessionState::Closed {
            self.cancel_timer(TimerKind::AckDelay);
        }
        Ok(self.take_events())
    }

    pub fn abort<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        error: Error,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let actions = self.lifecycle.request_abort(error)?;
        self.apply_lifecycle(actions, allocator)?;
        Ok(self.take_events())
    }

    pub fn on_timer<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        kind: TimerKind,
        generation: u64,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        if self.is_terminal() || generation != self.lifecycle.generation() {
            return Ok(Vec::new());
        }
        self.timers.mark_expired(kind, generation);
        match kind {
            TimerKind::Retransmit { sequence } => {
                let output =
                    self.outbound
                        .on_retransmit(sequence, &self.config, &mut self.metrics)?;
                let terminal = output.terminal_error();
                self.apply_outbound(output);
                if let Some(error) = terminal {
                    let actions = self
                        .lifecycle
                        .request_terminate(error, SessionState::Failed);
                    self.apply_lifecycle(actions, allocator)?;
                }
            }
            TimerKind::HandshakeRetry => {
                let actions = self
                    .lifecycle
                    .on_handshake_timeout(self.now, &self.config)?;
                self.apply_lifecycle(actions, allocator)?;
            }
            TimerKind::AckDelay => {
                if !self.inbound.ack_is_empty() {
                    self.emit_ack(allocator)?;
                }
            }
            TimerKind::FinRetry => {
                let actions = self.lifecycle.on_fin_timeout(&self.config)?;
                self.apply_lifecycle(actions, allocator)?;
            }
        }
        Ok(self.take_events())
    }

    pub fn on_send_completed(
        &mut self,
        now: Duration,
        sequence: MessageSequence,
        datagram: FixedBuf,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let output = self
            .outbound
            .on_send_completed(sequence, self.now, datagram);
        self.apply_outbound(output);
        Ok(self.take_events())
    }

    fn flush_pending<A: PacketBufAllocator + ?Sized>(&mut self, allocator: &A) -> Result<bool> {
        if self.state() != SessionState::Established {
            return Ok(false);
        }
        let output = self.outbound.flush_pending(
            FlushContext::new(
                self.connection_id(),
                &self.config,
                allocator,
                self.now,
                self.inbound.ack_snapshot(),
                self.inbound.available_window(&self.config),
            ),
            &mut self.metrics,
        )?;
        let ack_consumed = output.ack_consumed();
        self.apply_outbound(output);
        if ack_consumed {
            self.inbound.mark_ack_sent();
        }
        Ok(true)
    }

    fn emit_ack<A: PacketBufAllocator + ?Sized>(&mut self, allocator: &A) -> Result<()> {
        let output = self.outbound.emit_ack(
            self.connection_id(),
            &self.config,
            allocator,
            self.inbound.ack_snapshot(),
            self.inbound.available_window(&self.config),
            &mut self.metrics,
        )?;
        self.apply_outbound(output);
        self.inbound.mark_ack_sent();
        Ok(())
    }

    fn apply_inbound_ack<A: PacketBufAllocator + ?Sized>(
        &mut self,
        ack: inbound::AckDecision,
        allocator: &A,
    ) -> Result<()> {
        match ack {
            inbound::AckDecision::None => {}
            inbound::AckDecision::Immediate => self.emit_ack(allocator)?,
            inbound::AckDecision::Delayed => {
                self.metrics.record_ack_delayed();
                self.arm_timer(TimerKind::AckDelay, self.config.ack_delay);
            }
        }
        Ok(())
    }

    fn apply_lifecycle<A: PacketBufAllocator + ?Sized>(
        &mut self,
        actions: Vec<LifecycleAction>,
        allocator: &A,
    ) -> Result<()> {
        for action in actions {
            match action {
                LifecycleAction::EmitControl(flags) => {
                    let output = self.outbound.emit_control(
                        self.connection_id(),
                        &self.config,
                        allocator,
                        flags,
                        self.inbound.available_window(&self.config),
                        &mut self.metrics,
                    )?;
                    self.apply_outbound(output);
                }
                LifecycleAction::StateChanged(state) => {
                    self.events.push_back(SessionEvent::StateChanged(state));
                }
                LifecycleAction::ArmTimer { kind, delay } => self.arm_timer(kind, delay),
                LifecycleAction::CancelTimer(kind) => self.cancel_timer(kind),
                LifecycleAction::Terminate {
                    error,
                    state,
                    generation,
                } => {
                    for command in self.timers.cancel_all(generation).into_iter().flatten() {
                        self.events.push_back(SessionEvent::CancelTimer(command));
                    }
                    let output = self.outbound.fail_all(error, generation);
                    self.apply_outbound(output);
                    self.events.push_back(SessionEvent::StateChanged(state));
                    self.events.push_back(SessionEvent::Failed(error));
                }
            }
        }
        Ok(())
    }

    fn apply_outbound(&mut self, output: outbound::OutboundOutput) {
        for action in output.into_actions() {
            match action {
                OutboundAction::Datagram { datagram, sequence } => {
                    self.events.push_back(SessionEvent::Outbound {
                        datagram,
                        retain_for_retransmit: sequence,
                    });
                }
                OutboundAction::ArmTimer { kind, delay } => self.arm_timer(kind, delay),
                OutboundAction::CancelTimer(kind) => self.cancel_timer(kind),
                OutboundAction::SendAcked(receipt) => {
                    self.events.push_back(SessionEvent::SendAcked(receipt));
                }
                OutboundAction::SendFailed {
                    token,
                    sequence,
                    error,
                } => self.events.push_back(SessionEvent::SendFailed {
                    token,
                    sequence,
                    error,
                }),
            }
        }
    }

    fn arm_timer(&mut self, kind: TimerKind, delay: Duration) {
        let command = self.timers.arm(kind, self.lifecycle.generation(), delay);
        self.events.push_back(SessionEvent::ArmTimer(command));
    }

    fn cancel_timer(&mut self, kind: TimerKind) {
        if let Some(command) = self.timers.cancel(kind, self.lifecycle.generation()) {
            self.events.push_back(SessionEvent::CancelTimer(command));
        }
    }

    fn sync_now(&mut self, now: Duration) {
        self.now = self.now.max(now);
    }

    fn ensure_receivable(&self, connection_id: ConnectionId) -> Result<()> {
        if self.is_terminal() {
            return Err(match self.state() {
                SessionState::Reset => Error::ConnectionReset,
                _ => Error::ConnectionClosed,
            });
        }
        if connection_id != self.connection_id() {
            return Err(Error::UnknownConnection);
        }
        Ok(())
    }

    fn ensure_open_for_send(&self) -> Result<()> {
        if self.is_terminal() {
            return Err(match self.state() {
                SessionState::Reset => Error::ConnectionReset,
                _ => Error::ConnectionClosed,
            });
        }
        Ok(())
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.state(),
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        )
    }
}
