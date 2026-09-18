use tracing::trace;
use veloq::{
    runtime::context::Ctx,
    std::{num::NonZeroUsize, vec::Vec},
    sync::mpmc::{BoundedOwnedReceiver, BoundedOwnedSender},
};

use crate::{
    connection::{ConnectionReply, Message, SendPayload, Shutdown},
    error::{Error, Result},
    packet::{Flags, PacketRef},
    session::{SendReceipt, Session, SessionEvent},
};

use super::event::EventRouter;
use super::io::{InboundDatagram, OutboundSender, PumpSender};
use super::state::{ProtocolState, SessionEntry, key_from_packet, message_from_session};
use super::{ConnectionKey, Reply};

pub(super) type CommandSender = BoundedOwnedSender<Command>;
pub(super) type CommandReceiver = BoundedOwnedReceiver<Command>;

pub(crate) enum Command {
    Connect {
        key: ConnectionKey,
        reply: ConnectionReply,
    },
    Send {
        key: ConnectionKey,
        payload: SendPayload,
        reply: Reply<SendReceipt>,
    },
    Recv {
        key: ConnectionKey,
        reply: Reply<Message>,
    },
    Shutdown {
        key: ConnectionKey,
        how: Shutdown,
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
            Command::Send {
                key,
                payload,
                reply,
            } => {
                Self::handle_send(key, payload, reply, state, ports)?;
            }
            Command::Recv { key, reply } => {
                Self::handle_recv(key, reply, state, ports)?;
            }
            Command::Shutdown { key, how, reply } => {
                Self::handle_shutdown(key, how, reply, state, ports)?;
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

    fn handle_send<'a, 'rt>(
        key: ConnectionKey,
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
        let token = match entry.session_mut().queue_send(now, payload, &ctx) {
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

    fn handle_recv<'a, 'rt>(
        key: ConnectionKey,
        reply: Reply<Message>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        let now = state.now();
        let ctx = ports.ctx();
        let Some(entry) = state.entry_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if entry.read_shutdown() {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        }
        if entry.has_pending_recv() {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        let message = entry.session_mut().recv(now, &ctx);
        if let Some(message) = message {
            let _ = reply.send(Ok(message_from_session(message)));
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
            entry.put_pending_recv(reply);
            Ok(())
        }
    }

    fn handle_shutdown<'a, 'rt>(
        key: ConnectionKey,
        how: Shutdown,
        reply: Reply<()>,
        state: &mut ProtocolState<'rt>,
        ports: &CommandPorts<'a, 'rt>,
    ) -> Result<()> {
        if matches!(how, Shutdown::Read) {
            let Some(entry) = state.entry_mut(&key) else {
                let _ = reply.send(Err(Error::ConnectionClosed));
                return Ok(());
            };
            entry.mark_read_shutdown();
            if let Some(pending) = entry.take_pending_recv() {
                let _ = pending.send(Err(Error::ConnectionClosed));
            }
            let _ = reply.send(Ok(()));
            return Ok(());
        }
        Self::begin_close(key, reply, state, ports)
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
        let (connection_id, flags) = match PacketRef::decode_with_constraints(
            datagram.bytes(),
            state.config().max_fragment_payload(),
            state.config().max_message_size.get() as u64,
            state.config().max_fragments_per_message.get(),
        ) {
            Ok(packet) => (packet.connection_id, packet.flags),
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
        let key = key_from_packet(peer, connection_id);
        if !state.contains(&key) {
            if !flags.contains(Flags::SYN) || !state.can_accept_new_session() {
                state.stats().record_unknown();
                return Ok(());
            }
            Self::start_server(key, state)?;
        }

        let now = state.now();
        let ctx = ports.ctx();
        let datagram = datagram.into_datagram();
        let events = {
            let Some(entry) = state.entry_mut(&key) else {
                return Ok(());
            };
            match entry.session_mut().receive(now, datagram, &ctx) {
                Ok(events) => events,
                Err(error) => {
                    if error == Error::UnknownConnection {
                        state.stats().record_unknown();
                    } else {
                        state.stats().record_protocol_drop();
                    }
                    return Ok(());
                }
            }
        };
        let mut events = events;
        events.extend(Self::complete_pending_recv(key, state, ctx)?);
        EventRouter::record_events(
            state,
            key,
            events,
            ports.command(),
            ports.outbound(),
            ports.pump_events(),
        )
    }

    fn start_server(key: ConnectionKey, state: &mut ProtocolState<'_>) -> Result<()> {
        let session = Session::new_server(key.connection_id(), state.config().clone())?;
        state.insert(key, SessionEntry::new(session, None));
        state.stats().connection_started();
        Ok(())
    }

    fn complete_pending_recv(
        key: ConnectionKey,
        state: &mut ProtocolState<'_>,
        ctx: Ctx<'_>,
    ) -> Result<Vec<SessionEvent>> {
        let now = state.now();
        let Some(entry) = state.entry_mut(&key) else {
            return Ok(Vec::new());
        };
        let Some(reply) = entry.take_pending_recv() else {
            return Ok(Vec::new());
        };
        if reply.is_closed() {
            return Ok(Vec::new());
        }
        let Some(message) = entry.session_mut().recv(now, &ctx) else {
            entry.put_pending_recv(reply);
            return Ok(Vec::new());
        };
        let _ = reply.send(Ok(message_from_session(message)));
        Ok(entry.session_mut().take_events())
    }
}
