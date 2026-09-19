use veloq::{
    runtime::context::Ctx,
    std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration, vec::Vec},
    sync::{
        TrySendError,
        mpmc::{BoundedOwnedReceiver, BoundedOwnedSender},
        oneshot,
    },
};

use crate::{
    Config,
    connection::{Connection, ConnectionReply, StreamMessage as ConnectionStreamMessage},
    cookie::{CookieConfig, CookieKeyRing},
    error::{Error, Result},
    packet::{ConnectionId, StreamId},
    session::{SendReceipt, SendToken, Session, SessionStatsSnapshot, StreamMessage},
};

use super::io::CommandSender;
use super::{ConnectionKey, Reply};
use super::{stats::EndpointStats, timers::EndpointClock};

pub(super) struct SessionEntry {
    session: Session,
    observed_stats: SessionStatsSnapshot,
    connect_reply: Option<ConnectionReply>,
    pending_send: HashMap<SendToken, Reply<SendReceipt>>,
    pending_stream_recv: HashMap<StreamId, Reply<ConnectionStreamMessage>>,
    pending_stream_open: HashMap<StreamId, Reply<StreamId>>,
    pending_stream_accept: Option<Reply<StreamId>>,
    pending_close: Option<Reply<()>>,
    accepted: bool,
}

impl SessionEntry {
    pub(super) fn new(session: Session, connect_reply: Option<ConnectionReply>) -> Self {
        Self {
            session,
            observed_stats: SessionStatsSnapshot::default(),
            connect_reply,
            pending_send: HashMap::default(),
            pending_stream_recv: HashMap::default(),
            pending_stream_open: HashMap::default(),
            pending_stream_accept: None,
            pending_close: None,
            accepted: false,
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

    pub(super) fn take_pending_stream_recv(
        &mut self,
        stream_id: StreamId,
    ) -> Option<Reply<ConnectionStreamMessage>> {
        self.pending_stream_recv.remove(&stream_id)
    }

    pub(super) fn put_pending_stream_recv(
        &mut self,
        stream_id: StreamId,
        reply: Reply<ConnectionStreamMessage>,
    ) {
        self.pending_stream_recv.insert(stream_id, reply);
    }

    pub(super) fn has_pending_stream_recv(&self, stream_id: StreamId) -> bool {
        self.pending_stream_recv.contains_key(&stream_id)
    }

    pub(super) fn put_pending_stream_open(&mut self, stream_id: StreamId, reply: Reply<StreamId>) {
        self.pending_stream_open.insert(stream_id, reply);
    }

    pub(super) fn take_pending_stream_open(
        &mut self,
        stream_id: StreamId,
    ) -> Option<Reply<StreamId>> {
        self.pending_stream_open.remove(&stream_id)
    }

    pub(super) fn pending_stream_open(&self, stream_id: StreamId) -> bool {
        self.pending_stream_open.contains_key(&stream_id)
    }

    pub(super) fn put_pending_stream_accept(&mut self, reply: Reply<StreamId>) {
        self.pending_stream_accept = Some(reply);
    }

    pub(super) fn has_pending_stream_accept(&self) -> bool {
        self.pending_stream_accept.is_some()
    }

    pub(super) fn take_pending_stream_accept(&mut self) -> Option<Reply<StreamId>> {
        self.pending_stream_accept.take()
    }

    pub(super) fn clear_closed_waiters(&mut self) {
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

    pub(super) fn is_accepted(&self) -> bool {
        self.accepted
    }

    pub(super) fn mark_accepted(&mut self) {
        self.accepted = true;
    }

    pub(super) fn finish(&mut self, error: Error) {
        if let Some(reply) = self.connect_reply.take() {
            let _ = reply.send(Err(error));
        }
        for (_, reply) in self.pending_stream_recv.drain() {
            let _ = reply.send(Err(error));
        }
        for (_, reply) in self.pending_stream_open.drain() {
            let _ = reply.send(Err(error));
        }
        if let Some(reply) = self.pending_stream_accept.take() {
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
    ctx: Ctx<'rt>,
    config: Config,
    sessions: HashMap<ConnectionKey, SessionEntry>,
    clock: EndpointClock,
    stats: Arc<EndpointStats>,
    command: CommandSender,
    accept_tx: BoundedOwnedSender<Connection<'rt>>,
    accept: BoundedOwnedReceiver<Connection<'rt>>,
    cookie_keys: CookieKeyRing,
    accept_reservations: usize,
}

impl<'rt> ProtocolState<'rt> {
    pub(super) fn new(
        ctx: Ctx<'rt>,
        config: Config,
        stats: Arc<EndpointStats>,
        command: CommandSender,
        accept_tx: BoundedOwnedSender<Connection<'rt>>,
        accept: BoundedOwnedReceiver<Connection<'rt>>,
        cookie_keys: CookieKeyRing,
    ) -> Self {
        Self {
            ctx,
            clock: EndpointClock::new(&config),
            config,
            sessions: HashMap::default(),
            stats,
            command,
            accept_tx,
            accept,
            cookie_keys,
            accept_reservations: 0,
        }
    }

    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn ctx(&self) -> Ctx<'rt> {
        self.ctx
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
        self.accept_reservations < self.config.accept_capacity.get()
    }

    pub(super) fn cookie_config(&self) -> CookieConfig {
        self.config.cookie
    }

    pub(super) fn cookie_keys(&self) -> &CookieKeyRing {
        &self.cookie_keys
    }

    pub(super) fn rotate_cookie_keys(&mut self, keys: CookieKeyRing) {
        self.cookie_keys = keys;
    }

    pub(super) fn reserve_accept(&mut self) -> bool {
        if !self.can_accept_new_session() {
            return false;
        }
        self.accept_reservations += 1;
        true
    }

    pub(super) fn release_accept(&mut self) {
        self.accept_reservations = self.accept_reservations.saturating_sub(1);
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

pub(super) fn stream_message_from_session(message: StreamMessage) -> ConnectionStreamMessage {
    ConnectionStreamMessage {
        stream_id: message.stream_id,
        stream_sequence: message.stream_sequence,
        message_id: message.message_id,
        payload: message.into_fixed_buf(),
    }
}

pub(super) fn key_from_packet(peer: SocketAddr, connection_id: ConnectionId) -> ConnectionKey {
    ConnectionKey::new(peer, connection_id)
}

pub(super) type ReadySender = oneshot::OwnedSender<Result<()>>;
