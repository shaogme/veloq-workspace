use veloq::{
    runtime::context::Ctx,
    std::{
        collections::HashMap, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration,
        vec::Vec,
    },
    sync::{
        TrySendError,
        mpmc::{BoundedOwnedReceiver, BoundedOwnedSender},
        oneshot,
    },
};

use crate::{
    Config,
    connection::{Connection, ConnectionReply, Message},
    error::{Error, Result},
    packet::ConnectionId,
    session::{
        Message as SessionMessage, SendReceipt, SendToken, Session, SessionState,
        SessionStatsSnapshot,
    },
};

use super::io::CommandSender;
use super::{ConnectionKey, Reply};
use super::{stats::EndpointStats, timers::EndpointClock};

pub(super) struct SessionEntry {
    session: Session,
    observed_stats: SessionStatsSnapshot,
    connect_reply: Option<ConnectionReply>,
    connect_send_pending: bool,
    connect_completion_pending: bool,
    pending_send: HashMap<SendToken, Reply<SendReceipt>>,
    pending_recv: Option<Reply<Message>>,
    pending_close: Option<Reply<()>>,
    accepted: bool,
    read_shutdown: bool,
}

impl SessionEntry {
    pub(super) fn new(session: Session, connect_reply: Option<ConnectionReply>) -> Self {
        Self {
            session,
            observed_stats: SessionStatsSnapshot::default(),
            connect_reply,
            connect_send_pending: false,
            connect_completion_pending: false,
            pending_send: HashMap::default(),
            pending_recv: None,
            pending_close: None,
            accepted: false,
            read_shutdown: false,
        }
    }

    pub(super) fn session(&self) -> &Session {
        &self.session
    }

    pub(super) fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }

    pub(super) fn observed_stats(&self) -> SessionStatsSnapshot {
        self.observed_stats
    }

    pub(super) fn set_observed_stats(&mut self, snapshot: SessionStatsSnapshot) {
        self.observed_stats = snapshot;
    }

    pub(super) fn insert_pending_send(&mut self, token: SendToken, reply: Reply<SendReceipt>) {
        self.pending_send.insert(token, reply);
    }

    pub(super) fn take_pending_send(&mut self, token: SendToken) -> Option<Reply<SendReceipt>> {
        self.pending_send.remove(&token)
    }

    pub(super) fn take_pending_recv(&mut self) -> Option<Reply<Message>> {
        self.pending_recv.take()
    }

    pub(super) fn put_pending_recv(&mut self, reply: Reply<Message>) {
        self.pending_recv = Some(reply);
    }

    pub(super) fn has_pending_recv(&self) -> bool {
        self.pending_recv.is_some()
    }

    pub(super) fn clear_closed_waiters(&mut self) {
        if self
            .pending_recv
            .as_ref()
            .is_some_and(|reply| reply.is_closed())
        {
            self.pending_recv = None;
        }
        if self
            .pending_close
            .as_ref()
            .is_some_and(|reply| reply.is_closed())
        {
            self.pending_close = None;
        }
    }

    pub(super) fn set_pending_close(&mut self, reply: Reply<()>) {
        self.pending_close = Some(reply);
    }

    pub(super) fn has_pending_close(&self) -> bool {
        self.pending_close.is_some()
    }

    pub(super) fn connect_reply_closed(&self) -> bool {
        self.connect_reply
            .as_ref()
            .is_some_and(|reply| reply.is_closed())
    }

    pub(super) fn take_connect_reply(&mut self) -> Option<ConnectionReply> {
        self.connect_reply.take()
    }

    pub(super) fn has_connect_reply(&self) -> bool {
        self.connect_reply.is_some()
    }

    pub(super) fn mark_connect_send_pending(&mut self) {
        self.connect_send_pending = true;
    }

    pub(super) fn connect_send_pending(&self) -> bool {
        self.connect_send_pending
    }

    pub(super) fn take_connect_send_pending(&mut self) -> bool {
        let pending = self.connect_send_pending;
        self.connect_send_pending = false;
        pending
    }

    pub(super) fn mark_connect_completion_pending(&mut self) {
        self.connect_completion_pending = true;
    }

    pub(super) fn take_connect_completion_pending(&mut self) -> bool {
        let pending = self.connect_completion_pending;
        self.connect_completion_pending = false;
        pending
    }

    pub(super) fn is_accepted(&self) -> bool {
        self.accepted
    }

    pub(super) fn mark_accepted(&mut self) {
        self.accepted = true;
    }

    pub(super) fn read_shutdown(&self) -> bool {
        self.read_shutdown
    }

    pub(super) fn mark_read_shutdown(&mut self) {
        self.read_shutdown = true;
    }

    pub(super) fn finish(&mut self, error: Error) {
        if let Some(reply) = self.connect_reply.take() {
            let _ = reply.send(Err(error));
        }
        if let Some(reply) = self.pending_recv.take() {
            let _ = reply.send(Err(error));
        }
        if let Some(reply) = self.pending_close.take() {
            let _ = reply.send(if error == Error::ConnectionClosed {
                Ok(())
            } else {
                Err(error)
            });
        }
        for (_, reply) in self.pending_send.drain() {
            let _ = reply.send(Err(error));
        }
    }
}

pub(super) struct ProtocolState<'rt> {
    config: Config,
    sessions: HashMap<ConnectionKey, SessionEntry>,
    clock: EndpointClock,
    stats: Arc<EndpointStats>,
    command: CommandSender,
    accept_tx: BoundedOwnedSender<Connection<'rt>>,
    accept: BoundedOwnedReceiver<Connection<'rt>>,
}

impl<'rt> ProtocolState<'rt> {
    pub(super) fn new(
        config: Config,
        stats: Arc<EndpointStats>,
        command: CommandSender,
        accept_tx: BoundedOwnedSender<Connection<'rt>>,
        accept: BoundedOwnedReceiver<Connection<'rt>>,
    ) -> Self {
        Self {
            clock: EndpointClock::new(&config),
            config,
            sessions: HashMap::default(),
            stats,
            command,
            accept_tx,
            accept,
        }
    }

    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn now(&self) -> Duration {
        self.clock.now()
    }

    pub(super) fn clock(&self) -> &EndpointClock {
        &self.clock
    }

    pub(super) fn clock_mut(&mut self) -> &mut EndpointClock {
        &mut self.clock
    }

    pub(super) fn stats(&self) -> &EndpointStats {
        &self.stats
    }

    pub(super) fn stats_arc(&self) -> Arc<EndpointStats> {
        self.stats.clone()
    }

    pub(super) fn command_sender(&self) -> CommandSender {
        self.command.clone()
    }

    pub(super) fn sessions_len(&self) -> usize {
        self.sessions.len()
    }

    pub(super) fn contains(&self, key: &ConnectionKey) -> bool {
        self.sessions.contains_key(key)
    }

    pub(super) fn entry(&self, key: &ConnectionKey) -> Option<&SessionEntry> {
        self.sessions.get(key)
    }

    pub(super) fn entry_mut(&mut self, key: &ConnectionKey) -> Option<&mut SessionEntry> {
        self.sessions.get_mut(key)
    }

    pub(super) fn insert(&mut self, key: ConnectionKey, entry: SessionEntry) {
        self.sessions.insert(key, entry);
    }

    pub(super) fn remove(&mut self, key: &ConnectionKey) -> Option<SessionEntry> {
        self.sessions.remove(key)
    }

    pub(super) fn keys(&self) -> Vec<ConnectionKey> {
        self.sessions.keys().copied().collect()
    }

    pub(super) fn clear_sessions(&mut self) {
        self.sessions.clear();
    }

    pub(super) fn can_accept_new_session(&self) -> bool {
        if self.sessions_len() >= self.config.max_connections.get() {
            return false;
        }
        let half_open = self
            .sessions
            .values()
            .filter(|entry| {
                matches!(
                    entry.session().state(),
                    SessionState::Listen | SessionState::SynReceived | SessionState::SynSent
                )
            })
            .count();
        half_open < self.config.accept_capacity.get()
    }

    pub(super) fn try_accept(
        &mut self,
        connection: Connection<'rt>,
    ) -> core::result::Result<(), TrySendError<Connection<'rt>>> {
        self.accept_tx.try_send(connection)
    }

    pub(super) fn drain_accept(&mut self) {
        while let Ok(mut connection) = self.accept.try_recv() {
            connection.suppress_drop();
        }
    }
}

pub(super) fn message_from_session<'rt>(
    ctx: Ctx<'rt>,
    max_datagram_size: NonZeroUsize,
    message: SessionMessage,
) -> Result<Message> {
    let length = message.payload.len();
    let mut payload = ctx
        .try_alloc(max_datagram_size, length)
        .map_err(|_| Error::Io)?;
    payload.spare_capacity_mut()[..length].copy_from_slice(&message.payload);
    payload.set_len(length);
    Ok(Message {
        sequence: message.sequence,
        payload,
    })
}

pub(super) fn key_from_packet(peer: SocketAddr, connection_id: ConnectionId) -> ConnectionKey {
    ConnectionKey::new(peer, connection_id)
}

pub(super) type ReadySender = oneshot::OwnedSender<Result<()>>;
