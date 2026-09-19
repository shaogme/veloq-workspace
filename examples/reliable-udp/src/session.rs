mod inbound;
mod lifecycle;
mod metrics;
mod outbound;
mod stream_state;
mod timers;

use veloq::{
    buf::FixedBuf,
    std::{collections::VecDeque, time::Duration, vec::Vec},
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{
        ConnectionId, FrameSequence, FrameType, MessageId, PacketBufAllocator, PacketRef, StreamId,
        StreamOpenPayload, StreamSequence,
    },
    timer::{TimerCommand, TimerKind},
};

use self::{
    inbound::InboundState,
    lifecycle::{LifecycleAction, LifecycleState},
    metrics::SessionMetrics,
    outbound::{FlushContext, MessageAckContext, OutboundAction, OutboundState},
    stream_state::{StreamLifecycle, StreamRegistry},
    timers::TimerState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    SynSent,
    CookieSent,
    Established,
    FinWait,
    CloseWait,
    Closed,
    Failed,
    Reset,
}

#[derive(Debug)]
pub struct StreamMessage {
    pub stream_id: StreamId,
    pub stream_sequence: StreamSequence,
    pub message_id: MessageId,
    pub payload: FixedBuf,
}

impl StreamMessage {
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
    pub stream_id: StreamId,
    pub message_id: MessageId,
    pub fragment_count: u32,
    pub rtt: Option<Duration>,
    pub retransmissions: u64,
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
    pub reassembly_messages: usize,
    pub reassembly_bytes: usize,
    pub completed_messages: u64,
    pub duplicate_fragments: u64,
    pub message_ack_retries: u64,
    pub reassembly_timeouts: u64,
}

#[derive(Debug)]
pub enum SessionEvent {
    Outbound {
        datagram: FixedBuf,
        frame_sequence: Option<FrameSequence>,
    },
    ArmTimer(TimerCommand),
    CancelTimer(TimerCommand),
    StreamMessageAvailable(StreamId),
    StreamAvailable(StreamId),
    StreamOpened(StreamId),
    StreamOpenFailed(StreamId, Error),
    StreamClosed(StreamId),
    StreamReset(StreamId),
    SendAcked(SendReceipt),
    SendFailed {
        token: SendToken,
        message_id: Option<MessageId>,
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
                    frame_sequence: left,
                    ..
                },
                Self::Outbound {
                    frame_sequence: right,
                    ..
                },
            ) => left == right,
            (Self::ArmTimer(left), Self::ArmTimer(right))
            | (Self::CancelTimer(left), Self::CancelTimer(right)) => left == right,
            (Self::StreamMessageAvailable(left), Self::StreamMessageAvailable(right))
            | (Self::StreamAvailable(left), Self::StreamAvailable(right))
            | (Self::StreamOpened(left), Self::StreamOpened(right))
            | (Self::StreamClosed(left), Self::StreamClosed(right))
            | (Self::StreamReset(left), Self::StreamReset(right)) => left == right,
            (
                Self::StreamOpenFailed(left_id, left_error),
                Self::StreamOpenFailed(right_id, right_error),
            ) => left_id == right_id && left_error == right_error,
            (Self::SendAcked(left), Self::SendAcked(right)) => left == right,
            (
                Self::SendFailed {
                    token: left_token,
                    message_id: left_message_id,
                    error: left_error,
                },
                Self::SendFailed {
                    token: right_token,
                    message_id: right_message_id,
                    error: right_error,
                },
            ) => {
                left_token == right_token
                    && left_message_id == right_message_id
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
    streams: StreamRegistry,
    metrics: SessionMetrics,
    timers: TimerState,
    events: VecDeque<SessionEvent>,
}

impl Session {
    pub fn new_client(connection_id: ConnectionId, config: Config) -> Result<Self> {
        Self::new(Role::Client, SessionState::SynSent, connection_id, config)
    }

    pub fn new_server_established(
        connection_id: ConnectionId,
        config: Config,
        peer_receive_window: u16,
    ) -> Result<Self> {
        let mut session = Self::new(
            Role::Server,
            SessionState::Established,
            connection_id,
            config,
        )?;
        session
            .outbound
            .set_peer_receive_window(peer_receive_window);
        Ok(session)
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
            streams: StreamRegistry::new(role == Role::Client, &config),
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
            self.inbound.reassembly_messages(),
            self.inbound.reassembly_bytes(),
        )
    }

    pub fn take_events(&mut self) -> Vec<SessionEvent> {
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
        let packet = PacketRef::decode_with_constraints(
            datagram.as_slice(),
            self.config.max_fragment_payload(),
            self.config.max_message_size.get() as u64,
            self.config.max_fragments_per_message.get(),
        )?;
        let meta = packet.meta();
        self.ensure_receivable(meta.connection_id)?;
        if meta.frame_type == FrameType::Rst {
            let actions = self
                .lifecycle
                .request_terminate(Error::ConnectionReset, SessionState::Reset);
            self.apply_lifecycle(actions, allocator)?;
            return Ok(self.take_events());
        }
        if meta.frame_type == FrameType::SynAck {
            if self.role() != Role::Client
                || !matches!(
                    self.state(),
                    SessionState::SynSent | SessionState::CookieSent
                )
            {
                return Err(Error::InvalidState);
            }
            let cookie = packet.handshake_cookie()?;
            self.outbound.set_peer_receive_window(meta.receive_window);
            let actions = self.lifecycle.on_syn_ack(cookie);
            self.apply_lifecycle(actions, allocator)?;
            return Ok(self.take_events());
        }
        if meta.frame_type == FrameType::Ack && !packet.payload.is_empty() {
            return Err(Error::InvalidState);
        }
        if self.role() == Role::Client
            && self.state() == SessionState::CookieSent
            && meta.frame_type == FrameType::Ack
            && packet.payload.is_empty()
        {
            if meta.ack_largest != 0 || meta.ack_bitmap != 0 {
                return Err(Error::InvalidState);
            }
            let actions = self.lifecycle.on_handshake_confirmation();
            self.apply_lifecycle(actions, allocator)?;
            return Ok(self.take_events());
        }
        if self.state() == SessionState::FinWait && meta.frame_type == FrameType::FinAck {
            let actions = self.lifecycle.on_fin_ack();
            self.apply_lifecycle(actions, allocator)?;
            return Ok(self.take_events());
        }
        if self.state() != SessionState::Established {
            return Err(Error::InvalidState);
        }
        self.metrics
            .record_received(meta.frame_type == FrameType::Data, meta.has_ack);
        self.outbound.set_peer_receive_window(meta.receive_window);

        let outbound = self
            .outbound
            .apply_ack(&meta, self.now, &self.config, &mut self.metrics)?;
        self.apply_outbound(outbound);

        if meta.frame_type == FrameType::MessageAck {
            let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
            let message_id = MessageId::new(meta.message_id).ok_or(Error::InvalidFragment)?;
            let output =
                self.outbound
                    .apply_message_ack(stream_id, message_id, self.now, &self.config);
            self.apply_outbound(output);
        }
        if meta.frame_type.is_reliable()
            && meta.frame_type != FrameType::Data
            && let Some(sequence) = FrameSequence::new(meta.frame_sequence)
        {
            let _ = self.inbound.observe_reliable_frame(sequence);
            self.emit_ack(allocator)?;
        }

        match meta.frame_type {
            FrameType::StreamOpen => {
                let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
                let remote_is_client = self.role() == Role::Server;
                if stream_id.is_client_initiated() != remote_is_client {
                    return Err(Error::InvalidStreamId);
                }
                let open = StreamOpenPayload::decode(packet.payload)?;
                let open_ack = StreamOpenPayload {
                    frame_window: self.config.stream_receive_window_frames.get() as u16,
                    byte_credit: self.config.max_stream_receive_bytes.get() as u64,
                }
                .encode();
                if self.streams.create_remote(stream_id, &self.config).is_err() {
                    if self.streams.contains(stream_id) {
                        self.outbound.set_peer_stream_window(
                            stream_id,
                            open.frame_window,
                            open.byte_credit,
                        );
                        let output = self.outbound.emit_stream_control(
                            self.connection_id(),
                            &self.config,
                            allocator,
                            FrameType::StreamOpenAck,
                            stream_id,
                            &open_ack,
                            self.inbound.available_window(&self.config),
                            &mut self.metrics,
                        )?;
                        self.apply_outbound(output);
                    }
                } else {
                    self.outbound.set_peer_stream_window(
                        stream_id,
                        open.frame_window,
                        open.byte_credit,
                    );
                    let output = self.outbound.emit_stream_control(
                        self.connection_id(),
                        &self.config,
                        allocator,
                        FrameType::StreamOpenAck,
                        stream_id,
                        &open_ack,
                        self.inbound.available_window(&self.config),
                        &mut self.metrics,
                    )?;
                    self.apply_outbound(output);
                    self.events
                        .push_back(SessionEvent::StreamAvailable(stream_id));
                }
            }
            FrameType::StreamOpenAck => {
                let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
                let open_ack = StreamOpenPayload::decode(packet.payload)?;
                self.outbound.set_peer_stream_window(
                    stream_id,
                    open_ack.frame_window,
                    open_ack.byte_credit,
                );
                if self.streams.mark_open(stream_id).is_ok() {
                    self.cancel_timer(TimerKind::StreamOpenRetry { stream_id });
                    self.events.push_back(SessionEvent::StreamOpened(stream_id));
                }
            }
            FrameType::StreamFin => {
                let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.close_remote();
                    self.events.push_back(SessionEvent::StreamClosed(stream_id));
                }
            }
            FrameType::StreamReset => {
                let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
                if self.streams.mark_reset(stream_id).is_ok() {
                    self.cancel_stream_reassembly(stream_id);
                    let output = self.outbound.fail_stream(stream_id, Error::StreamReset);
                    self.apply_outbound(output);
                    self.events.push_back(SessionEvent::StreamReset(stream_id));
                }
            }
            _ => {}
        }

        if meta.frame_type == FrameType::Fin {
            let actions = self.lifecycle.on_fin();
            self.apply_lifecycle(actions, allocator)?;
        }
        if packet.frame_type == FrameType::FinAck && self.state() == SessionState::FinWait {
            let actions = self.lifecycle.on_fin_ack();
            self.apply_lifecycle(actions, allocator)?;
        }
        if meta.frame_type == FrameType::Data {
            if self.state() != SessionState::Established {
                return Err(Error::InvalidState);
            }
            let stream_id = StreamId::new(meta.stream_id).ok_or(Error::InvalidStreamId)?;
            if !self.streams.contains(stream_id) {
                self.flush_pending(allocator)?;
                return Ok(self.take_events());
            }
            let output = self.inbound.on_data(
                datagram,
                meta,
                &self.config,
                self.connection_id(),
                allocator,
                &mut self.metrics,
            )?;
            for (stream_id, _) in output.message_acks().iter().take(output.delivered()) {
                self.events
                    .push_back(SessionEvent::StreamMessageAvailable(*stream_id));
            }
            for (stream_id, message_id) in output.arm_reassembly() {
                self.arm_timer(
                    TimerKind::ReassemblyTimeout {
                        stream_id: *stream_id,
                        message_id: *message_id,
                    },
                    self.config.reassembly_timeout,
                );
            }
            for (stream_id, message_id) in output.cancel_reassembly() {
                self.cancel_timer(TimerKind::ReassemblyTimeout {
                    stream_id: *stream_id,
                    message_id: *message_id,
                });
            }
            self.emit_message_acks(output.message_acks(), allocator)?;
            self.apply_inbound_ack(output.ack(), allocator)?;
        }

        self.flush_pending(allocator)?;
        Ok(self.take_events())
    }

    pub fn open_stream<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        allocator: &A,
    ) -> Result<StreamId> {
        self.sync_now(now);
        if self.state() != SessionState::Established {
            return Err(Error::ConnectionClosing);
        }
        let stream_id = self.streams.create_local(&self.config)?;
        let payload = StreamOpenPayload {
            frame_window: self.config.stream_receive_window_frames.get() as u16,
            byte_credit: self.config.max_stream_receive_bytes.get() as u64,
        }
        .encode();
        let output = self.outbound.emit_stream_control(
            self.connection_id(),
            &self.config,
            allocator,
            FrameType::StreamOpen,
            stream_id,
            &payload,
            self.inbound.available_window(&self.config),
            &mut self.metrics,
        )?;
        self.apply_outbound(output);
        self.arm_timer(
            TimerKind::StreamOpenRetry { stream_id },
            self.config.stream_open_deadline,
        );
        Ok(stream_id)
    }

    pub fn accept_stream(&mut self) -> Option<StreamId> {
        self.streams.pop_accept()
    }

    pub fn stream_is_open(&self, stream_id: StreamId) -> bool {
        self.streams
            .get(stream_id)
            .is_some_and(|stream| matches!(stream.lifecycle, StreamLifecycle::Open))
    }

    pub fn queue_stream_send<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        stream_id: StreamId,
        payload: FixedBuf,
        allocator: &A,
    ) -> Result<SendToken> {
        self.sync_now(now);
        if let Some(stream) = self.streams.get(stream_id) {
            stream.ensure_sendable()?;
        } else {
            return Err(Error::InvalidStreamId);
        }
        let token = self
            .outbound
            .queue_stream_send(stream_id, payload, &self.config)?;
        let _ = self.flush_pending(allocator)?;
        Ok(token)
    }

    pub fn recv_stream<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        stream_id: StreamId,
        allocator: &A,
    ) -> Option<StreamMessage> {
        self.sync_now(now);
        let (message, message_acks, cancel_reassembly) =
            self.inbound
                .pop_message(stream_id, &self.config, &mut self.metrics);
        for (stream_id, message_id) in cancel_reassembly {
            self.cancel_timer(TimerKind::ReassemblyTimeout {
                stream_id,
                message_id,
            });
        }
        let _ = self.emit_message_acks(&message_acks, allocator);
        if message.is_some() {
            let _ = self.emit_ack(allocator);
        }
        message
    }

    pub fn close_stream<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        stream_id: StreamId,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let state = self
            .streams
            .get_mut(stream_id)
            .ok_or(Error::InvalidStreamId)?;
        state.ensure_sendable()?;
        state.close_local();
        let output = self.outbound.emit_stream_control(
            self.connection_id(),
            &self.config,
            allocator,
            FrameType::StreamFin,
            stream_id,
            &[],
            self.inbound.available_window(&self.config),
            &mut self.metrics,
        )?;
        self.apply_outbound(output);
        self.events.push_back(SessionEvent::StreamClosed(stream_id));
        Ok(self.take_events())
    }

    pub fn reset_stream<A: PacketBufAllocator + ?Sized>(
        &mut self,
        now: Duration,
        stream_id: StreamId,
        allocator: &A,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        self.streams.mark_reset(stream_id)?;
        self.cancel_stream_reassembly(stream_id);
        let failed = self.outbound.fail_stream(stream_id, Error::StreamReset);
        self.apply_outbound(failed);
        let output = self.outbound.emit_stream_control(
            self.connection_id(),
            &self.config,
            allocator,
            FrameType::StreamReset,
            stream_id,
            &[],
            self.inbound.available_window(&self.config),
            &mut self.metrics,
        )?;
        self.apply_outbound(output);
        self.events.push_back(SessionEvent::StreamReset(stream_id));
        Ok(self.take_events())
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
                let stream_failure = output.stream_failure();
                self.apply_outbound(output);
                if let Some(error) = terminal {
                    let actions = self
                        .lifecycle
                        .request_terminate(error, SessionState::Failed);
                    self.apply_lifecycle(actions, allocator)?;
                }
                if let Some((stream_id, _error)) = stream_failure {
                    let events = self.reset_stream(self.now, stream_id, allocator)?;
                    self.events.extend(events);
                }
            }
            TimerKind::MessageAckRetry {
                stream_id,
                message_id,
            } => {
                let output = self.outbound.on_message_ack_retry(
                    stream_id,
                    message_id,
                    &self.config,
                    allocator,
                    &mut self.metrics,
                );
                let terminal = output.terminal_error();
                let stream_failure = output.stream_failure();
                self.apply_outbound(output);
                if let Some(error) = terminal {
                    let actions = self
                        .lifecycle
                        .request_terminate(error, SessionState::Failed);
                    self.apply_lifecycle(actions, allocator)?;
                }
                if let Some((stream_id, _error)) = stream_failure {
                    let events = self.reset_stream(self.now, stream_id, allocator)?;
                    self.events.extend(events);
                }
            }
            TimerKind::ReassemblyTimeout {
                stream_id,
                message_id,
            } => {
                if self
                    .inbound
                    .on_reassembly_timeout(stream_id, message_id, &mut self.metrics)
                {
                    let events = self.reset_stream(self.now, stream_id, allocator)?;
                    self.events.extend(events);
                }
            }
            TimerKind::StreamOpenRetry { stream_id } => {
                let retry = self
                    .streams
                    .get_mut(stream_id)
                    .is_some_and(|stream| stream.retry_open(self.config.max_retries));
                if retry {
                    let payload = StreamOpenPayload {
                        frame_window: self.config.stream_receive_window_frames.get() as u16,
                        byte_credit: self.config.max_stream_receive_bytes.get() as u64,
                    }
                    .encode();
                    let output = self.outbound.emit_stream_control(
                        self.connection_id(),
                        &self.config,
                        allocator,
                        FrameType::StreamOpen,
                        stream_id,
                        &payload,
                        self.inbound.available_window(&self.config),
                        &mut self.metrics,
                    )?;
                    self.apply_outbound(output);
                    self.arm_timer(
                        TimerKind::StreamOpenRetry { stream_id },
                        self.config.stream_open_deadline,
                    );
                } else if self.streams.mark_reset(stream_id).is_ok() {
                    self.events.push_back(SessionEvent::StreamOpenFailed(
                        stream_id,
                        Error::StreamOpenTimeout,
                    ));
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
        sequence: FrameSequence,
        datagram: FixedBuf,
    ) -> Result<Vec<SessionEvent>> {
        self.sync_now(now);
        let output = self.outbound.on_send_completed(
            sequence,
            self.now,
            datagram,
            &self.config,
            &mut self.metrics,
        );
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

    fn emit_message_acks<A: PacketBufAllocator + ?Sized>(
        &mut self,
        message_ids: &[(StreamId, MessageId)],
        allocator: &A,
    ) -> Result<()> {
        for (stream_id, message_id) in message_ids {
            let output = self.outbound.emit_message_ack(MessageAckContext {
                connection_id: self.connection_id(),
                config: &self.config,
                allocator,
                stream_id: *stream_id,
                message_id: *message_id,
                ack: self.inbound.ack_snapshot(),
                receive_window: self.inbound.available_window(&self.config),
                metrics: &mut self.metrics,
            })?;
            self.apply_outbound(output);
            self.inbound.mark_ack_sent();
            self.cancel_timer(TimerKind::AckDelay);
        }
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
                LifecycleAction::EmitCookieProof(cookie) => {
                    let output = self.outbound.emit_handshake_cookie(
                        self.connection_id(),
                        &self.config,
                        allocator,
                        &cookie,
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
                    for (stream_id, message_id) in self.inbound.clear() {
                        if let Some(command) = self.timers.cancel(
                            TimerKind::ReassemblyTimeout {
                                stream_id,
                                message_id,
                            },
                            generation,
                        ) {
                            self.events.push_back(SessionEvent::CancelTimer(command));
                        }
                    }
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
                        frame_sequence: sequence,
                    });
                }
                OutboundAction::ArmTimer { kind, delay } => self.arm_timer(kind, delay),
                OutboundAction::CancelTimer(kind) => self.cancel_timer(kind),
                OutboundAction::SendAcked(receipt) => {
                    self.events.push_back(SessionEvent::SendAcked(receipt));
                }
                OutboundAction::SendFailed {
                    token,
                    message_id,
                    error,
                } => self.events.push_back(SessionEvent::SendFailed {
                    token,
                    message_id,
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

    fn cancel_stream_reassembly(&mut self, stream_id: StreamId) {
        for message_id in self.inbound.reset_stream(stream_id) {
            self.cancel_timer(TimerKind::ReassemblyTimeout {
                stream_id,
                message_id,
            });
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

    fn is_terminal(&self) -> bool {
        matches!(
            self.state(),
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        )
    }
}
