use tracing::trace;
use veloq::{
    buf::FixedBuf,
    runtime::context::Ctx,
    std::num::NonZeroUsize,
    sync::{
        TrySendError,
        mpmc::{BoundedReceiver, BoundedSender},
    },
};

use crate::{
    connection::{
        Connection, ConnectionErrorCode, ConnectionReply, SendPayload, Shutdown, StreamMessage,
    },
    cookie::{
        CookieInput, CookieInvalidReason, CookieKeyRing, CookieToken, CookieValidation,
        issue_cookie, validate_cookie,
    },
    error::{Error, Result},
    packet::{Ack, COOKIE_LEN, FrameSequence, FrameType, Packet, PacketRef, StreamId},
    session::{Role, SendReceipt, Session, SessionState},
};

use super::event::EventRouter;
use super::io::{InboundDatagram, OutboundDatagram, OutboundSender, PumpSender, SendTicket};
use super::state::{ProtocolState, SessionEntry, key_from_packet, stream_message_from_session};
use super::{ConnectionKey, Reply};

pub(super) type CommandSender = BoundedSender<Command>;
pub(super) type CommandReceiver = BoundedReceiver<Command>;

pub(crate) enum Command {
    Connect {
        key: ConnectionKey,
        reply: ConnectionReply,
    },
    OpenStream {
        key: ConnectionKey,
        reply: Reply<StreamId>,
    },
    AcceptStream {
        key: ConnectionKey,
        reply: Reply<StreamId>,
    },
    StreamSend {
        key: ConnectionKey,
        stream_id: StreamId,
        payload: SendPayload,
        reply: Reply<SendReceipt>,
    },
    StreamRecv {
        key: ConnectionKey,
        stream_id: StreamId,
        reply: Reply<StreamMessage>,
    },
    StreamShutdown {
        key: ConnectionKey,
        stream_id: StreamId,
        how: Shutdown,
        reply: Reply<()>,
    },
    StreamClose {
        key: ConnectionKey,
        stream_id: StreamId,
        reply: Reply<()>,
    },
    StreamDrop {
        key: ConnectionKey,
        stream_id: StreamId,
    },
    Abort {
        key: ConnectionKey,
        code: ConnectionErrorCode,
        reply: Reply<()>,
    },
    Close {
        key: ConnectionKey,
        reply: Reply<()>,
    },
    Drop {
        key: ConnectionKey,
    },
    CloseEndpoint {
        reply: Reply<()>,
    },
    RotateCookieKeys {
        cookie_keys: CookieKeyRing,
        reply: Reply<()>,
    },
}

pub(super) enum CommandOutcome {
    Continue,
    Close(Reply<()>),
}

pub(super) struct CommandPorts<'a, 'rt> {
    ctx: Ctx<'rt>,
    command: &'a CommandSender,
    outbound: &'a OutboundSender,
    pump_events: &'a PumpSender,
}

impl<'a, 'rt> CommandPorts<'a, 'rt> {
    pub(super) fn new(
        ctx: Ctx<'rt>,
        command: &'a CommandSender,
        outbound: &'a OutboundSender,
        pump_events: &'a PumpSender,
    ) -> Self {
        Self {
            ctx,
            command,
            outbound,
            pump_events,
        }
    }

    pub(super) fn ctx(&self) -> Ctx<'rt> {
        self.ctx
    }

    pub(super) fn command(&self) -> &CommandSender {
        self.command
    }

    pub(super) fn outbound(&self) -> &OutboundSender {
        self.outbound
    }

    pub(super) fn pump_events(&self) -> &PumpSender {
        self.pump_events
    }
}

pub(super) struct CommandService;

impl CommandService {
    pub(super) fn handle<'a, 'rt>(
        command: Command,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<CommandOutcome> {
        match command {
            Command::Connect { key, reply } => {
                Self::start_client(key, reply, state, ports)?;
            }
            Command::OpenStream { key, reply } => {
                Self::handle_open_stream(key, reply, state, ports)?;
            }
            Command::AcceptStream { key, reply } => {
                Self::handle_accept_stream(key, reply, state, ports)?;
            }
            Command::StreamSend {
                key,
                stream_id,
                payload,
                reply,
            } => {
                Self::handle_stream_send(key, stream_id, payload, reply, state, ports)?;
            }
            Command::StreamRecv {
                key,
                stream_id,
                reply,
            } => {
                Self::handle_stream_recv(key, stream_id, reply, state, ports)?;
            }
            Command::StreamShutdown {
                key,
                stream_id,
                how,
                reply,
            } => {
                Self::handle_stream_shutdown(key, stream_id, how, reply, state, ports)?;
            }
            Command::StreamClose {
                key,
                stream_id,
                reply,
            } => {
                Self::handle_stream_close(key, stream_id, reply, state, ports)?;
            }
            Command::StreamDrop { key, stream_id } => {
                Self::handle_stream_drop(key, stream_id, state, ports)?;
            }
            Command::Abort { key, code, reply } => {
                Self::handle_abort(key, code, reply, state, ports)?;
            }
            Command::Close { key, reply } => {
                Self::begin_close(key, reply, state, ports)?;
            }
            Command::Drop { key } => {
                Self::handle_drop(key, state, ports)?;
            }
            Command::CloseEndpoint { reply } => {
                EventRouter::shutdown(
                    state,
                    Error::EndpointClosed,
                    ports.command(),
                    ports.outbound(),
                    ports.pump_events(),
                )?;
                return Ok(CommandOutcome::Close(reply));
            }
            Command::RotateCookieKeys { cookie_keys, reply } => {
                state.rotate_cookie_keys(cookie_keys);
                let _ = reply.send(Ok(()));
            }
        }
        Ok(CommandOutcome::Continue)
    }

    fn start_client<'a, 'rt>(
        key: ConnectionKey,
        reply: ConnectionReply,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        if state.contains(&key) {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        if state.sessions_len() >= state.config().max_connections.get() {
            let _ = reply.send(Err(Error::TooManyConnections));
            return Ok(());
        }
        let mut session = Session::new_client(key.connection_id(), state.config().clone())?;
        let ctx = ports.ctx();
        let events = session.start(state.now(), &ctx)?;
        state.insert(key, SessionEntry::new(session, Some(reply)));
        state.stats().connection_started();
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn handle_open_stream<'a, 'rt>(
        key: ConnectionKey,
        reply: Reply<StreamId>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        let stream_id = match entry.session_mut().open_stream(now, &ctx) {
            Ok(stream_id) => stream_id,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        entry.put_pending_stream_open(stream_id, reply);
        let events = entry.session_mut().take_events();
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn handle_accept_stream<'a, 'rt>(
        key: ConnectionKey,
        reply: Reply<StreamId>,
        state: &mut ProtocolState<'rt>,
        _ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if let Some(stream_id) = entry.session_mut().accept_stream() {
            let _ = reply.send(Ok(stream_id));
        } else if !entry.has_pending_stream_accept() {
            entry.put_pending_stream_accept(reply);
        } else {
            let _ = reply.send(Err(Error::InvalidState));
        }
        Ok(())
    }

    fn handle_stream_send<'a, 'rt>(
        key: ConnectionKey,
        stream_id: StreamId,
        payload: SendPayload,
        reply: Reply<SendReceipt>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let ctx = ports.ctx();
        let payload = match payload {
            SendPayload::Bytes(bytes) => {
                let length = bytes.len();
                let mut payload = ctx
                    .try_alloc(
                        NonZeroUsize::new(length.max(1)).expect("message capacity is non-zero"),
                        length,
                    )
                    .map_err(|_| Error::Io)?;
                payload.spare_capacity_mut()[..length].copy_from_slice(&bytes);
                payload.set_len(length);
                payload
            }
            SendPayload::Buffer(payload) => payload,
        };
        let now = state.now();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        let token = match entry
            .session_mut()
            .queue_stream_send(now, stream_id, payload, &ctx)
        {
            Ok(token) => token,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        entry.insert_pending_send(token, reply);
        let events = entry.session_mut().take_events();
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn handle_stream_recv<'a, 'rt>(
        key: ConnectionKey,
        stream_id: StreamId,
        reply: Reply<StreamMessage>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if entry.has_pending_stream_recv(stream_id) {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        if let Some(message) = entry.session_mut().recv_stream(now, stream_id, &ctx) {
            let _ = reply.send(Ok(stream_message_from_session(message)));
            let events = entry.session_mut().take_events();
            EventRouter::record_events(
                state,
                key,
                events,
                ports.command(),
                ports.outbound(),
                ports.pump_events(),
            )
        } else {
            entry.put_pending_stream_recv(stream_id, reply);
            Ok(())
        }
    }

    fn handle_stream_shutdown<'a, 'rt>(
        key: ConnectionKey,
        stream_id: StreamId,
        how: Shutdown,
        reply: Reply<()>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        if matches!(how, Shutdown::Write) {
            return Self::handle_stream_close(key, stream_id, reply, state, ports);
        }
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if let Some(pending) = entry.take_pending_stream_recv(stream_id) {
            let _ = pending.send(Err(Error::StreamClosed));
        }
        match entry.session_mut().reset_stream(now, stream_id, &ctx) {
            Ok(events) => {
                let _ = reply.send(Ok(()));
                EventRouter::record_events(
                    state,
                    key,
                    events,
                    ports.command(),
                    ports.outbound(),
                    ports.pump_events(),
                )
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                Ok(())
            }
        }
    }

    fn handle_stream_close<'a, 'rt>(
        key: ConnectionKey,
        stream_id: StreamId,
        reply: Reply<()>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        match entry.session_mut().close_stream(now, stream_id, &ctx) {
            Ok(events) => {
                let _ = reply.send(Ok(()));
                EventRouter::record_events(
                    state,
                    key,
                    events,
                    ports.command(),
                    ports.outbound(),
                    ports.pump_events(),
                )
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                Ok(())
            }
        }
    }

    fn handle_stream_drop<'a, 'rt>(
        key: ConnectionKey,
        stream_id: StreamId,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            return Ok(());
        };
        let events = entry
            .session_mut()
            .reset_stream(now, stream_id, &ctx)
            .unwrap_or_default();
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn handle_abort<'a, 'rt>(
        key: ConnectionKey,
        code: ConnectionErrorCode,
        reply: Reply<()>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        let error = match code {
            ConnectionErrorCode::Protocol => Error::InvalidState,
            ConnectionErrorCode::Application(_) => Error::ConnectionReset,
        };
        let events = entry.session_mut().abort(now, error, &ports.ctx())?;
        let _ = reply.send(Ok(()));
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn begin_close<'a, 'rt>(
        key: ConnectionKey,
        reply: Reply<()>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if entry.has_pending_close() {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        let ctx = ports.ctx();
        match entry.session_mut().close(now, &ctx) {
            Ok(events) => {
                entry.set_pending_close(reply);
                EventRouter::record_events(
                    state,
                    key,
                    events,
                    ports.command(),
                    ports.outbound(),
                    ports.pump_events(),
                )
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                Ok(())
            }
        }
    }

    fn handle_drop<'a, 'rt>(
        key: ConnectionKey,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let Some(entry) = state.entry_mut(&key) else {
            return Ok(());
        };
        if entry.has_pending_close() {
            return Ok(());
        }
        let ctx = ports.ctx();
        let events = entry.session_mut().close(now, &ctx).unwrap_or_default();
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    pub(super) fn handle_packet<'a, 'rt>(
        datagram: InboundDatagram,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        if datagram.len() > state.config().max_datagram_size.get() {
            state.stats().record_oversized();
            return Ok(());
        }
        let peer = datagram.peer();
        let packet = match PacketRef::decode_with_constraints(
            datagram.bytes(),
            state.config().max_fragment_payload(),
            state.config().max_message_size.get() as u64,
            state.config().max_fragments_per_message.get(),
        ) {
            Ok(packet) => packet,
            Err(error) => {
                state.stats().record_malformed();
                trace!(
                    target: "veloq_reliable_udp::endpoint",
                    peer = ?peer,
                    error = ?error,
                    "protocol loop ignored undecodable datagram"
                );
                return Ok(());
            }
        };
        let key = key_from_packet(peer, packet.connection_id);
        trace!(
            target: "veloq_reliable_udp::endpoint",
            peer = ?peer,
            connection_id = packet.connection_id.get(),
            frame_type = ?packet.frame_type,
            frame_sequence = packet.frame_sequence,
            stream_id = packet.stream_id,
            "protocol loop dispatching datagram"
        );
        if !state.contains(&key) {
            if packet.frame_type == FrameType::Syn {
                Self::issue_challenge(key, packet.receive_window, state, ports)?;
                return Ok(());
            }
            if packet.frame_type == FrameType::Fin {
                return Self::send_fin_ack(key, packet.frame_sequence, state, ports);
            }
            if packet.frame_type == FrameType::HandshakeAck && packet.payload.len() == COOKIE_LEN {
                return Self::admit_proof(key, packet, state, ports);
            }
            state.stats().record_unknown();
            return Ok(());
        }

        if packet.frame_type == FrameType::HandshakeAck && packet.payload.len() == COOKIE_LEN {
            if state.entry(&key).is_some_and(|entry| {
                entry.session().role() == Role::Server
                    && entry.session().state() == SessionState::Established
            }) {
                state.stats().record_cookie_duplicate();
                return Self::send_confirmation(key, state, ports);
            }
            state.stats().record_unknown();
            return Ok(());
        }

        if packet.frame_type == FrameType::Syn {
            state.stats().record_unknown();
            return Ok(());
        }

        if packet.frame_type == FrameType::Ack && !packet.payload.is_empty() {
            state.stats().record_unknown();
            return Ok(());
        }

        let now = state.now();
        let ctx = ports.ctx();
        let datagram = datagram.into_datagram();
        let events = {
            let Some(entry) = state.entry_mut(&key) else {
                return Ok(());
            };
            match entry.session_mut().receive(now, datagram, &ctx) {
                Ok(events) => {
                    trace!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?peer,
                        connection_id = key.connection_id().get(),
                        event_count = events.len(),
                        "session processed datagram"
                    );
                    events
                }
                Err(error) => {
                    trace!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?peer,
                        connection_id = key.connection_id().get(),
                        state = ?entry.session().state(),
                        error = ?error,
                        "session rejected datagram"
                    );
                    if error == Error::UnknownConnection {
                        state.stats().record_unknown();
                    } else {
                        state.stats().record_protocol_drop();
                    }
                    return Ok(());
                }
            }
        };
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn issue_challenge<'a, 'rt>(
        key: ConnectionKey,
        receive_window: u16,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let input = CookieInput {
            source: key.peer(),
            connection_id: key.connection_id(),
            client_receive_window: receive_window,
        };
        let cookie = issue_cookie(state.cookie_keys(), input, state.now());
        let packet = Packet::encode_frame(
            &ports.ctx(),
            state.config().max_datagram_size,
            FrameType::SynAck,
            false,
            key.connection_id(),
            None,
            None,
            Ack::empty(),
            state.config().receive_window.get() as u16,
            cookie.as_bytes(),
        )?;
        state.stats().record_cookie_challenge();
        Self::enqueue_stateless(key, packet.into_fixed_buf(), state, ports, true);
        Ok(())
    }

    fn admit_proof<'a, 'rt>(
        key: ConnectionKey,
        packet: PacketRef<'_>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        state.stats().record_cookie_proof_received();
        let token = match packet
            .handshake_cookie()
            .ok()
            .and_then(|payload| CookieToken::parse(payload).ok())
        {
            Some(token) => token,
            None => {
                state.stats().record_cookie_wrong_parameters();
                return Ok(());
            }
        };
        let input = CookieInput {
            source: key.peer(),
            connection_id: key.connection_id(),
            client_receive_window: packet.receive_window,
        };
        match validate_cookie(
            state.cookie_keys(),
            state.cookie_config(),
            input,
            token,
            state.now(),
        ) {
            CookieValidation::Valid {
                client_receive_window,
                ..
            } => {
                if !state.reserve_accept() {
                    state.stats().record_cookie_admission_drop();
                    return Ok(());
                }
                let session = match Session::new_server_established(
                    key.connection_id(),
                    state.config().clone(),
                    client_receive_window,
                ) {
                    Ok(session) => session,
                    Err(error) => {
                        state.release_accept();
                        return Err(error);
                    }
                };
                state.insert(key, SessionEntry::new(session, None));
                state.stats().connection_started();
                let connection = Connection::new(
                    ports.command().clone(),
                    key,
                    state.config().max_message_size.get(),
                );
                match state.try_accept(connection) {
                    Ok(()) => {
                        state.release_accept();
                        if let Some(entry) = state.entry_mut(&key) {
                            entry.mark_accepted();
                        }
                        state.stats().record_cookie_proof_accepted();
                        Self::send_confirmation(key, state, ports)
                    }
                    Err(TrySendError::Full(mut returned))
                    | Err(TrySendError::Closed(mut returned)) => {
                        returned.suppress_drop();
                        drop(returned);
                        state.release_accept();
                        state.remove(&key);
                        state.stats().connection_finished();
                        state.stats().record_cookie_admission_drop();
                        Ok(())
                    }
                }
            }
            CookieValidation::Invalid(reason) => {
                Self::record_cookie_failure(state, reason);
                Ok(())
            }
        }
    }

    fn send_confirmation<'a, 'rt>(
        key: ConnectionKey,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let packet = Packet::encode_frame(
            &ports.ctx(),
            state.config().max_datagram_size,
            FrameType::Ack,
            true,
            key.connection_id(),
            None,
            None,
            Ack::empty(),
            state.config().receive_window.get() as u16,
            &[],
        )?;
        Self::enqueue_stateless(key, packet.into_fixed_buf(), state, ports, false);
        Ok(())
    }

    fn send_fin_ack<'a, 'rt>(
        key: ConnectionKey,
        frame_sequence: u64,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let frame_sequence = FrameSequence::new(frame_sequence).ok_or(Error::InvalidState)?;
        let packet = Packet::encode_frame(
            &ports.ctx(),
            state.config().max_datagram_size,
            FrameType::FinAck,
            false,
            key.connection_id(),
            Some(frame_sequence),
            None,
            Ack::empty(),
            state.config().receive_window.get() as u16,
            &[],
        )?;
        Self::enqueue_stateless(key, packet.into_fixed_buf(), state, ports, false);
        Ok(())
    }

    fn enqueue_stateless<'a, 'rt>(
        key: ConnectionKey,
        datagram: FixedBuf,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
        challenge: bool,
    ) {
        let item = OutboundDatagram::new(key, datagram, SendTicket::DropAfterSend);
        if matches!(
            ports.outbound().try_send(item),
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_))
        ) {
            state.stats().record_outbound_drop();
            if challenge {
                state.stats().record_cookie_challenge_send_drop();
            }
        }
    }

    fn record_cookie_failure(state: &ProtocolState<'_>, reason: CookieInvalidReason) {
        match reason {
            CookieInvalidReason::Expired | CookieInvalidReason::Future => {
                state.stats().record_cookie_expired()
            }
            CookieInvalidReason::WrongParameters => state.stats().record_cookie_wrong_parameters(),
            CookieInvalidReason::InvalidMac | CookieInvalidReason::KeyUnavailable => {
                state.stats().record_cookie_invalid_mac()
            }
            CookieInvalidReason::Malformed => state.stats().record_cookie_wrong_parameters(),
        }
    }
}
