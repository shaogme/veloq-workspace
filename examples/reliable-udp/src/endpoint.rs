mod command;
mod driver;
mod event;
mod io;
mod state;
mod stats;
mod timers;

pub(crate) use command::Command;

use veloq::{
    net::UdpSocket,
    runtime::context::Ctx,
    std::{
        marker::PhantomData,
        net::{SocketAddr, ToSocketAddrs},
        sync::{
            Arc,
            atomic::{NativeAtomicU64, Ordering},
        },
        time::Duration,
    },
    sync::{
        mpmc::{BoundedOwnedReceiver, owned_bounded},
        oneshot,
    },
};

use crate::{
    Config,
    connection::Connection,
    error::{Error, Result},
    packet::ConnectionId,
};

use self::{io::CommandSender, state::ProtocolState, stats::EndpointStats};

static NEXT_CONNECTION_ID: NativeAtomicU64 = NativeAtomicU64::new(1);

pub(crate) type ConnectionCommandSender = CommandSender;
pub(crate) type Reply<T> = oneshot::OwnedSender<Result<T>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ConnectionKey {
    peer: SocketAddr,
    connection_id: ConnectionId,
}

impl ConnectionKey {
    pub(crate) const fn new(peer: SocketAddr, connection_id: ConnectionId) -> Self {
        Self {
            peer,
            connection_id,
        }
    }

    pub(crate) const fn peer(self) -> SocketAddr {
        self.peer
    }

    pub(crate) const fn connection_id(self) -> ConnectionId {
        self.connection_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointStatsSnapshot {
    pub active_connections: u64,
    pub total_connections: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub data_packets: u64,
    pub ack_packets: u64,
    pub retransmissions: u64,
    pub duplicate_packets: u64,
    pub dropped_packets: u64,
    pub malformed_packets: u64,
    pub oversized_datagrams: u64,
    pub unknown_connections: u64,
    pub receive_window_drops: u64,
    pub out_of_window_drops: u64,
    pub duplicate_acks: u64,
    pub rtt_samples: u64,
    pub latest_rtt: Option<Duration>,
    pub min_rtt: Option<Duration>,
    pub max_rtt: Option<Duration>,
    pub rto: Duration,
    pub congestion_window: u64,
    pub send_window: u64,
    pub peer_receive_window: u64,
    pub receive_window: u64,
    pub ack_delayed: u64,
    pub piggybacked_acks: u64,
    pub timer_expirations: u64,
    pub timer_delay_samples: u64,
    pub timer_delay_total: Duration,
    pub timer_delay_max: Duration,
    pub inbound_queue_drops: u64,
    pub outbound_queue_drops: u64,
}

pub struct Endpoint<'rt> {
    command: ConnectionCommandSender,
    accept: BoundedOwnedReceiver<Connection<'rt>>,
    local_addr: SocketAddr,
    max_payload: usize,
    stats: Arc<EndpointStats>,
    marker: PhantomData<&'rt ()>,
}

impl<'rt> Clone for Endpoint<'rt> {
    fn clone(&self) -> Self {
        Self {
            command: self.command.clone(),
            accept: self.accept.clone(),
            local_addr: self.local_addr,
            max_payload: self.max_payload,
            stats: self.stats.clone(),
            marker: PhantomData,
        }
    }
}

impl<'rt> Endpoint<'rt> {
    pub fn bind(
        ctx: Ctx<'rt>,
        addr: impl ToSocketAddrs,
        config: Config,
    ) -> Result<(Self, EndpointDriver<'rt>, EndpointReady)> {
        config.validate()?;
        let socket = UdpSocket::bind(ctx, addr).map_err(|_| Error::Io)?;
        let local_addr = socket.local_addr().map_err(|_| Error::Io)?;
        let (command, command_rx) = io::channel(config.command_capacity.get());
        let (accept_tx, accept_rx) = owned_bounded(config.accept_capacity.get());
        let (ready, ready_receiver) = oneshot::owned_channel();
        let stats = Arc::new(EndpointStats::new());
        let endpoint = Self {
            command: command.clone(),
            accept: accept_rx.clone(),
            local_addr,
            max_payload: config.max_payload(),
            stats: stats.clone(),
            marker: PhantomData,
        };
        let state =
            ProtocolState::new(config.clone(), stats, command.clone(), accept_tx, accept_rx);
        let driver = EndpointDriver {
            inner: driver::Driver::new(ctx, socket, command_rx, state, ready),
        };
        Ok((
            endpoint,
            driver,
            EndpointReady {
                receiver: Some(ready_receiver),
            },
        ))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn inbound_dropped(&self) -> u64 {
        self.stats.inbound_dropped()
    }

    pub fn outbound_dropped(&self) -> u64 {
        self.stats.outbound_dropped()
    }

    pub fn stats(&self) -> EndpointStatsSnapshot {
        self.stats.snapshot()
    }

    pub async fn connect(&self, peer: SocketAddr) -> Result<Connection<'rt>> {
        let connection_id = next_connection_id();
        let key = ConnectionKey::new(peer, connection_id);
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Connect { key, reply })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)??;
        Ok(Connection::new(self.command.clone(), key, self.max_payload))
    }

    pub async fn accept(&self) -> Result<Connection<'rt>> {
        self.accept.recv().await.map_err(|_| Error::EndpointClosed)
    }

    pub async fn close(&self) -> Result<()> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::CloseEndpoint { reply })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }
}

pub struct EndpointDriver<'rt> {
    inner: driver::Driver<'rt>,
}

impl<'rt> EndpointDriver<'rt> {
    pub async fn run(self) -> Result<()> {
        self.inner.run().await
    }
}

pub struct EndpointReady {
    receiver: Option<oneshot::OwnedReceiver<Result<()>>>,
}

impl EndpointReady {
    pub async fn wait(&mut self) -> Result<()> {
        let receiver = self.receiver.take().ok_or(Error::EndpointClosed)?;
        receiver.await.map_err(|_| Error::EndpointClosed)?
    }
}

fn next_connection_id() -> ConnectionId {
    loop {
        let value = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        if let Some(connection_id) = ConnectionId::new(value) {
            return connection_id;
        }
    }
}
