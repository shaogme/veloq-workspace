use tracing::{debug, trace, warn};
use veloq::{
    net::{PreparedUdpRecv, UdpSocket},
    runtime::context::Ctx,
    time::sleep,
};
use veloq_buf::FixedBuf;
use veloq_runtime::{Outcome, scope, select};
use veloq_std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{NativeAtomicU64, Ordering},
    },
    time::{Duration, Instant},
    vec::Vec,
};
use veloq_sync::{
    TrySendError,
    mpmc::{BoundedOwnedReceiver, BoundedOwnedSender},
    oneshot,
};
use veloq_wheel::{Expired, TimerId, Wheel};

use crate::{
    Config,
    connection::{Connection, ConnectionReply, Message, SendPayload, Shutdown},
    error::{Error, Result},
    packet::{ConnectionId, Flags, Packet, PacketRef},
    session::{
        Message as SessionMessage, Role, SendReceipt, SendToken, Session, SessionEvent,
        SessionState, SessionStatsSnapshot,
    },
    timer::{TimerCommand, TimerKind},
};

static NEXT_CONNECTION_ID: NativeAtomicU64 = NativeAtomicU64::new(1);

type CommandSender = BoundedOwnedSender<Command>;
type CommandReceiver = BoundedOwnedReceiver<Command>;
type Reply<T> = oneshot::OwnedSender<Result<T>>;
type PumpSender = BoundedOwnedSender<PumpEvent>;
type PumpReceiver = BoundedOwnedReceiver<PumpEvent>;
type InboundSender = BoundedOwnedSender<InboundDatagram>;
type InboundReceiver = BoundedOwnedReceiver<InboundDatagram>;
type OutboundSender = BoundedOwnedSender<OutboundDatagram>;
type OutboundReceiver = BoundedOwnedReceiver<OutboundDatagram>;
type ReadySender = oneshot::OwnedSender<Result<()>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ConnectionKey {
    pub(crate) peer: SocketAddr,
    pub(crate) connection_id: ConnectionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TimerSlotKey {
    connection: ConnectionKey,
    kind: TimerKind,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointTimer {
    slot: TimerSlotKey,
    deadline: Duration,
}

struct EndpointTimerRegistry {
    slots: HashMap<TimerSlotKey, TimerId>,
}

impl EndpointTimerRegistry {
    fn new() -> Self {
        Self {
            slots: HashMap::default(),
        }
    }

    fn arm(
        &mut self,
        wheel: &mut Wheel<EndpointTimer>,
        connection: ConnectionKey,
        kind: TimerKind,
        generation: u64,
        delay: Duration,
        now: Duration,
    ) -> Result<()> {
        let slot = TimerSlotKey {
            connection,
            kind,
            generation,
        };
        if let Some(timer_id) = self.slots.remove(&slot) {
            let _ = wheel.cancel(timer_id);
        }
        let deadline = now.checked_add(delay).unwrap_or(Duration::MAX);
        let timer_id = wheel.insert(EndpointTimer { slot, deadline }, delay)?;
        self.slots.insert(slot, timer_id);
        Ok(())
    }

    fn cancel(
        &mut self,
        wheel: &mut Wheel<EndpointTimer>,
        connection: ConnectionKey,
        kind: TimerKind,
        generation: u64,
    ) {
        let slot = TimerSlotKey {
            connection,
            kind,
            generation,
        };
        if let Some(timer_id) = self.slots.remove(&slot) {
            let _ = wheel.cancel(timer_id);
        }
    }

    fn cancel_connection(&mut self, wheel: &mut Wheel<EndpointTimer>, connection: ConnectionKey) {
        let slots: Vec<TimerSlotKey> = self
            .slots
            .keys()
            .copied()
            .filter(|slot| slot.connection == connection)
            .collect();
        for slot in slots {
            self.cancel(wheel, connection, slot.kind, slot.generation);
        }
    }

    fn take_expired(
        &mut self,
        expired: &Expired<EndpointTimer>,
    ) -> Option<(TimerSlotKey, Duration)> {
        if self.slots.get(&expired.item.slot) != Some(&expired.id) {
            return None;
        }
        self.slots.remove(&expired.item.slot);
        Some((expired.item.slot, expired.item.deadline))
    }

    fn clear(&mut self, wheel: &mut Wheel<EndpointTimer>) {
        let mut discarded = Vec::new();
        wheel.clear(&mut discarded);
        self.slots.clear();
    }
}

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

struct InboundDatagram {
    peer: SocketAddr,
    datagram: FixedBuf,
}

struct OutboundDatagram {
    key: ConnectionKey,
    datagram: Vec<u8>,
    completion: Option<PumpSender>,
}

enum PumpEvent {
    ReceiveReady,
    SendReady,
    ReceiveFailed(Error),
    SendCompleted {
        key: ConnectionKey,
        result: Result<()>,
    },
    SendFailed {
        key: ConnectionKey,
        error: Error,
    },
}

struct EndpointStats {
    active_connections: NativeAtomicU64,
    total_connections: NativeAtomicU64,
    packets_sent: NativeAtomicU64,
    packets_received: NativeAtomicU64,
    data_packets: NativeAtomicU64,
    ack_packets: NativeAtomicU64,
    retransmissions: NativeAtomicU64,
    duplicate_packets: NativeAtomicU64,
    dropped_packets: NativeAtomicU64,
    malformed_packets: NativeAtomicU64,
    oversized_datagrams: NativeAtomicU64,
    unknown_connections: NativeAtomicU64,
    receive_window_drops: NativeAtomicU64,
    out_of_window_drops: NativeAtomicU64,
    duplicate_acks: NativeAtomicU64,
    rtt_samples: NativeAtomicU64,
    latest_rtt_nanos: NativeAtomicU64,
    min_rtt_nanos: NativeAtomicU64,
    max_rtt_nanos: NativeAtomicU64,
    rto_nanos: NativeAtomicU64,
    congestion_window: NativeAtomicU64,
    send_window: NativeAtomicU64,
    peer_receive_window: NativeAtomicU64,
    receive_window: NativeAtomicU64,
    ack_delayed: NativeAtomicU64,
    piggybacked_acks: NativeAtomicU64,
    timer_expirations: NativeAtomicU64,
    timer_delay_samples: NativeAtomicU64,
    timer_delay_total_nanos: NativeAtomicU64,
    timer_delay_max_nanos: NativeAtomicU64,
    inbound_dropped: NativeAtomicU64,
    outbound_dropped: NativeAtomicU64,
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

impl EndpointStats {
    fn new() -> Self {
        Self {
            active_connections: NativeAtomicU64::new(0),
            total_connections: NativeAtomicU64::new(0),
            packets_sent: NativeAtomicU64::new(0),
            packets_received: NativeAtomicU64::new(0),
            data_packets: NativeAtomicU64::new(0),
            ack_packets: NativeAtomicU64::new(0),
            retransmissions: NativeAtomicU64::new(0),
            duplicate_packets: NativeAtomicU64::new(0),
            dropped_packets: NativeAtomicU64::new(0),
            malformed_packets: NativeAtomicU64::new(0),
            oversized_datagrams: NativeAtomicU64::new(0),
            unknown_connections: NativeAtomicU64::new(0),
            receive_window_drops: NativeAtomicU64::new(0),
            out_of_window_drops: NativeAtomicU64::new(0),
            duplicate_acks: NativeAtomicU64::new(0),
            rtt_samples: NativeAtomicU64::new(0),
            latest_rtt_nanos: NativeAtomicU64::new(0),
            min_rtt_nanos: NativeAtomicU64::new(u64::MAX),
            max_rtt_nanos: NativeAtomicU64::new(0),
            rto_nanos: NativeAtomicU64::new(0),
            congestion_window: NativeAtomicU64::new(0),
            send_window: NativeAtomicU64::new(0),
            peer_receive_window: NativeAtomicU64::new(0),
            receive_window: NativeAtomicU64::new(0),
            ack_delayed: NativeAtomicU64::new(0),
            piggybacked_acks: NativeAtomicU64::new(0),
            timer_expirations: NativeAtomicU64::new(0),
            timer_delay_samples: NativeAtomicU64::new(0),
            timer_delay_total_nanos: NativeAtomicU64::new(0),
            timer_delay_max_nanos: NativeAtomicU64::new(0),
            inbound_dropped: NativeAtomicU64::new(0),
            outbound_dropped: NativeAtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> EndpointStatsSnapshot {
        let min_rtt_nanos = self.min_rtt_nanos.load(Ordering::Relaxed);
        let rtt_samples = self.rtt_samples.load(Ordering::Relaxed);
        EndpointStatsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            data_packets: self.data_packets.load(Ordering::Relaxed),
            ack_packets: self.ack_packets.load(Ordering::Relaxed),
            retransmissions: self.retransmissions.load(Ordering::Relaxed),
            duplicate_packets: self.duplicate_packets.load(Ordering::Relaxed),
            dropped_packets: self.dropped_packets.load(Ordering::Relaxed),
            malformed_packets: self.malformed_packets.load(Ordering::Relaxed),
            oversized_datagrams: self.oversized_datagrams.load(Ordering::Relaxed),
            unknown_connections: self.unknown_connections.load(Ordering::Relaxed),
            receive_window_drops: self.receive_window_drops.load(Ordering::Relaxed),
            out_of_window_drops: self.out_of_window_drops.load(Ordering::Relaxed),
            duplicate_acks: self.duplicate_acks.load(Ordering::Relaxed),
            rtt_samples,
            latest_rtt: (rtt_samples > 0)
                .then(|| duration_from_nanos(self.latest_rtt_nanos.load(Ordering::Relaxed))),
            min_rtt: (rtt_samples > 0 && min_rtt_nanos != u64::MAX)
                .then(|| duration_from_nanos(min_rtt_nanos)),
            max_rtt: (rtt_samples > 0)
                .then(|| duration_from_nanos(self.max_rtt_nanos.load(Ordering::Relaxed))),
            rto: duration_from_nanos(self.rto_nanos.load(Ordering::Relaxed)),
            congestion_window: self.congestion_window.load(Ordering::Relaxed),
            send_window: self.send_window.load(Ordering::Relaxed),
            peer_receive_window: self.peer_receive_window.load(Ordering::Relaxed),
            receive_window: self.receive_window.load(Ordering::Relaxed),
            ack_delayed: self.ack_delayed.load(Ordering::Relaxed),
            piggybacked_acks: self.piggybacked_acks.load(Ordering::Relaxed),
            timer_expirations: self.timer_expirations.load(Ordering::Relaxed),
            timer_delay_samples: self.timer_delay_samples.load(Ordering::Relaxed),
            timer_delay_total: duration_from_nanos(
                self.timer_delay_total_nanos.load(Ordering::Relaxed),
            ),
            timer_delay_max: duration_from_nanos(
                self.timer_delay_max_nanos.load(Ordering::Relaxed),
            ),
            inbound_queue_drops: self.inbound_dropped.load(Ordering::Relaxed),
            outbound_queue_drops: self.outbound_dropped.load(Ordering::Relaxed),
        }
    }
}

pub struct Endpoint<'rt> {
    command: CommandSender,
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
    pub fn bind<A: ToSocketAddrs>(
        ctx: Ctx<'rt>,
        addr: A,
        config: Config,
    ) -> Result<(Self, EndpointDriver<'rt>, EndpointReady)> {
        config.validate()?;
        let socket = UdpSocket::bind(ctx, addr).map_err(|_| Error::Io)?;
        let local_addr = socket.local_addr().map_err(|_| Error::Io)?;
        let (command, command_rx) = veloq_sync::mpmc::owned_bounded(config.command_capacity.get());
        let (accept_tx, accept_rx) = veloq_sync::mpmc::owned_bounded(config.accept_capacity.get());
        let (ready, ready_receiver) = oneshot::owned_channel();
        let wheel = Wheel::new(config.wheel.clone());
        let stats = Arc::new(EndpointStats::new());
        let endpoint = Self {
            command: command.clone(),
            accept: accept_rx.clone(),
            local_addr,
            max_payload: config.max_payload(),
            stats: stats.clone(),
            marker: PhantomData,
        };
        let driver = EndpointDriver {
            ctx,
            socket: Some(socket),
            command,
            commands: command_rx,
            accept_tx,
            accept: accept_rx,
            config,
            sessions: HashMap::default(),
            wheel,
            timer_registry: EndpointTimerRegistry::new(),
            logical_now: Duration::ZERO,
            last_advanced: Instant::now(),
            stats,
            endpoint_close_reply: None,
            ready: Some(ready),
            marker: PhantomData,
        };
        Ok((
            endpoint,
            driver,
            EndpointReady {
                receiver: Some(ready_receiver),
            },
        ))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.local_addr)
    }

    pub fn inbound_dropped(&self) -> u64 {
        self.stats.inbound_dropped.load(Ordering::Relaxed)
    }

    pub fn outbound_dropped(&self) -> u64 {
        self.stats.outbound_dropped.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> EndpointStatsSnapshot {
        self.stats.snapshot()
    }

    pub async fn connect(&self, peer: SocketAddr) -> Result<Connection<'rt>> {
        let connection_id = next_connection_id();
        let key = ConnectionKey {
            peer,
            connection_id,
        };
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
    ctx: Ctx<'rt>,
    socket: Option<UdpSocket<'rt>>,
    command: CommandSender,
    commands: CommandReceiver,
    accept_tx: BoundedOwnedSender<Connection<'rt>>,
    accept: BoundedOwnedReceiver<Connection<'rt>>,
    config: Config,
    sessions: HashMap<ConnectionKey, SessionEntry>,
    wheel: Wheel<EndpointTimer>,
    timer_registry: EndpointTimerRegistry,
    logical_now: Duration,
    last_advanced: Instant,
    stats: Arc<EndpointStats>,
    endpoint_close_reply: Option<Reply<()>>,
    ready: Option<ReadySender>,
    marker: PhantomData<&'rt ()>,
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

struct SessionEntry {
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

#[derive(Default)]
struct EventBatch {
    datagrams: VecDeque<Vec<u8>>,
    timer_commands: Vec<TimerCommand>,
    terminal_error: Option<Error>,
    reject_accept: bool,
}

enum DriverEvent {
    Command(core::result::Result<Command, veloq_sync::TryRecvError>),
    Packet(core::result::Result<InboundDatagram, veloq_sync::TryRecvError>),
    Pump(core::result::Result<PumpEvent, veloq_sync::TryRecvError>),
    Timer,
}

impl<'rt> EndpointDriver<'rt> {
    pub async fn run(mut self) -> Result<()> {
        let socket = match self.socket.take() {
            Some(socket) => socket,
            None => {
                self.send_ready(Err(Error::EndpointClosed));
                return Err(Error::EndpointClosed);
            }
        };
        trace!(
            target: "veloq_reliable_udp::endpoint",
            "endpoint driver started"
        );
        let config = self.config.clone();
        let (inbound_tx, inbound_rx) =
            veloq_sync::mpmc::owned_bounded(config.inbound_capacity.get());
        let (outbound_tx, outbound_rx) =
            veloq_sync::mpmc::owned_bounded(config.outbound_capacity.get());
        let event_capacity = config
            .outbound_capacity
            .get()
            .checked_add(config.max_connections.get())
            .ok_or(Error::Io)?;
        let (pump_tx, pump_rx) = veloq_sync::mpmc::owned_bounded(event_capacity);
        let ctx = self.ctx;
        let receive_socket = socket.clone();
        let send_socket = socket.clone();
        let stats = self.stats.clone();

        let scoped = scope!(ctx, async |scope| {
            let mut receive_task = scope.spawn_boxed(receive_pump(
                ctx,
                receive_socket,
                config.clone(),
                inbound_tx,
                pump_tx.clone(),
                stats,
            ));
            let mut send_task = scope.spawn_boxed(send_pump(
                ctx,
                send_socket,
                config,
                outbound_rx,
                pump_tx.clone(),
            ));

            let result = self
                .run_protocol(inbound_rx, outbound_tx, pump_tx, pump_rx)
                .await;
            let graceful = matches!(result, Ok(true));

            receive_task.cancel();
            if !graceful {
                send_task.cancel();
            }
            let _ = receive_task.await;
            let _ = send_task.await;
            result
        })
        .await
        .map_err(|_| Error::Io)?;

        let result = match scoped {
            Outcome::Ok(_) => Ok(()),
            Outcome::Err(error) => Err(error),
        };
        if let Err(error) = result {
            self.send_ready(Err(error));
        }
        let close_result = socket.close().await.map_err(|_| Error::Io);
        let result = result.and(close_result);
        if let Some(reply) = self.endpoint_close_reply.take() {
            let _ = reply.send(result);
        }
        result
    }

    async fn run_protocol(
        &mut self,
        inbound: InboundReceiver,
        outbound: OutboundSender,
        pump_sender: PumpSender,
        mut pump_events: PumpReceiver,
    ) -> Result<bool> {
        let result = match self.wait_for_pumps(&mut pump_events).await {
            Ok(()) => {
                self.send_ready(Ok(()));
                self.run_protocol_loop(inbound, &outbound, &pump_sender, pump_events)
                    .await
            }
            Err(error) => {
                self.send_ready(Err(error));
                Err(error)
            }
        };
        if let Err(error) = result {
            let _ = self.shutdown(error, &outbound, &pump_sender);
        }
        result
    }

    async fn wait_for_pumps(&self, pump_events: &mut PumpReceiver) -> Result<()> {
        let mut receive_ready = false;
        let mut send_ready = false;
        while !(receive_ready && send_ready) {
            match pump_events.recv().await {
                Ok(PumpEvent::ReceiveReady) => receive_ready = true,
                Ok(PumpEvent::SendReady) => send_ready = true,
                Ok(PumpEvent::ReceiveFailed(error)) => return Err(error),
                Ok(PumpEvent::SendFailed { error, .. }) => return Err(error),
                Ok(PumpEvent::SendCompleted { result, .. }) => result?,
                Err(_) => return Err(Error::EndpointClosed),
            }
        }
        Ok(())
    }

    fn send_ready(&mut self, result: Result<()>) {
        if let Some(sender) = self.ready.take() {
            let _ = sender.send(result);
        }
    }

    async fn run_protocol_loop(
        &mut self,
        inbound: InboundReceiver,
        outbound: &OutboundSender,
        pump_sender: &PumpSender,
        pump_events: PumpReceiver,
    ) -> Result<bool> {
        loop {
            self.advance_endpoint_clock(outbound, pump_sender)?;
            let event = match self.wheel.next_deadline()? {
                Some(delay) => select! {
                    self.ctx;
                    command = self.commands.recv() => DriverEvent::Command(command),
                    packet = inbound.recv() => DriverEvent::Packet(packet),
                    pump = pump_events.recv() => DriverEvent::Pump(pump),
                    _ = sleep(self.ctx, delay) => DriverEvent::Timer,
                },
                None => select! {
                    self.ctx;
                    command = self.commands.recv() => DriverEvent::Command(command),
                    packet = inbound.recv() => DriverEvent::Packet(packet),
                    pump = pump_events.recv() => DriverEvent::Pump(pump),
                },
            };

            match event {
                DriverEvent::Command(Ok(command)) => {
                    if self.handle_command(command, outbound, pump_sender)? {
                        return Ok(true);
                    }
                }
                DriverEvent::Command(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Ok(true);
                }
                DriverEvent::Packet(Ok(packet)) => {
                    self.handle_packet(
                        packet.peer,
                        packet.datagram.as_slice(),
                        outbound,
                        pump_sender,
                    )?;
                }
                DriverEvent::Packet(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Ok(true);
                }
                DriverEvent::Pump(Ok(PumpEvent::ReceiveFailed(error))) => {
                    self.shutdown(error, outbound, pump_sender)?;
                    return Err(error);
                }
                DriverEvent::Pump(Ok(PumpEvent::ReceiveReady | PumpEvent::SendReady)) => {}
                DriverEvent::Pump(Ok(PumpEvent::SendCompleted { key, result })) => {
                    self.handle_send_completion(key, result, outbound, pump_sender)?;
                }
                DriverEvent::Pump(Ok(PumpEvent::SendFailed { key, error })) => {
                    self.handle_send_error(key, error, outbound, pump_sender)?;
                }
                DriverEvent::Pump(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Err(Error::Io);
                }
                DriverEvent::Timer => {}
            }
        }
    }

    fn handle_command(
        &mut self,
        command: Command,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<bool> {
        match command {
            Command::Connect { key, reply } => {
                self.start_client(key, reply, outbound, pump_events)?;
            }
            Command::Send {
                key,
                payload,
                reply,
            } => {
                self.handle_send(key, payload, reply, outbound, pump_events)?;
            }
            Command::Recv { key, reply } => {
                self.handle_recv(key, reply, outbound, pump_events)?;
            }
            Command::Shutdown { key, how, reply } => {
                self.handle_shutdown(key, how, reply, outbound, pump_events)?;
            }
            Command::Close { key, reply } => {
                self.handle_close(key, reply, outbound, pump_events)?;
            }
            Command::Drop { key } => {
                self.handle_drop(key, outbound, pump_events)?;
            }
            Command::CloseEndpoint { reply } => {
                self.shutdown(Error::EndpointClosed, outbound, pump_events)?;
                self.endpoint_close_reply = Some(reply);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn start_client(
        &mut self,
        key: ConnectionKey,
        reply: ConnectionReply,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        if self.sessions.contains_key(&key) {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        if self.sessions.len() >= self.config.max_connections.get() {
            let _ = reply.send(Err(Error::TooManyConnections));
            return Ok(());
        }
        let config = self.config.clone();
        let mut session = Session::new_client(key.connection_id, config)?;
        let events = session.start(self.logical_now)?;
        self.sessions.insert(
            key,
            SessionEntry {
                session,
                observed_stats: SessionStatsSnapshot::default(),
                connect_reply: Some(reply),
                connect_send_pending: false,
                connect_completion_pending: false,
                pending_send: HashMap::default(),
                pending_recv: None,
                pending_close: None,
                accepted: false,
                read_shutdown: false,
            },
        );
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.record_events(key, events, outbound, pump_events)
    }

    fn handle_send(
        &mut self,
        key: ConnectionKey,
        payload: SendPayload,
        reply: Reply<SendReceipt>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let payload = match payload {
            SendPayload::Bytes(payload) => payload,
            SendPayload::Buffer(payload) => payload.as_slice().to_vec(),
        };
        let Some(entry) = self.sessions.get_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        let token = match entry.session.queue_send(self.logical_now, payload) {
            Ok(token) => token,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        entry.pending_send.insert(token, reply);
        let events = entry.session.drain_events();
        self.record_events(key, events, outbound, pump_events)
    }

    fn handle_recv(
        &mut self,
        key: ConnectionKey,
        reply: Reply<Message>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let ctx = self.ctx;
        let max_datagram_size = self.config.max_datagram_size;
        let Some(entry) = self.sessions.get_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if entry.read_shutdown {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        }
        if entry.pending_recv.is_some() {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        if let Some(message) = entry.session.recv(self.logical_now) {
            let response = Self::message_from_session(ctx, max_datagram_size, message);
            let _ = reply.send(response);
            let events = entry.session.drain_events();
            self.record_events(key, events, outbound, pump_events)
        } else {
            entry.pending_recv = Some(reply);
            Ok(())
        }
    }

    fn handle_shutdown(
        &mut self,
        key: ConnectionKey,
        how: Shutdown,
        reply: Reply<()>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        if matches!(how, Shutdown::Read) {
            let Some(entry) = self.sessions.get_mut(&key) else {
                let _ = reply.send(Err(Error::ConnectionClosed));
                return Ok(());
            };
            entry.read_shutdown = true;
            if let Some(pending) = entry.pending_recv.take() {
                let _ = pending.send(Err(Error::ConnectionClosed));
            }
            let _ = reply.send(Ok(()));
            return Ok(());
        }
        self.begin_close(key, reply, outbound, pump_events)
    }

    fn handle_close(
        &mut self,
        key: ConnectionKey,
        reply: Reply<()>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        self.begin_close(key, reply, outbound, pump_events)
    }

    fn begin_close(
        &mut self,
        key: ConnectionKey,
        reply: Reply<()>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let Some(entry) = self.sessions.get_mut(&key) else {
            let _ = reply.send(Err(Error::ConnectionClosed));
            return Ok(());
        };
        if entry.pending_close.is_some() {
            let _ = reply.send(Err(Error::InvalidState));
            return Ok(());
        }
        match entry.session.close(self.logical_now) {
            Ok(events) => {
                entry.pending_close = Some(reply);
                self.record_events(key, events, outbound, pump_events)
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                Ok(())
            }
        }
    }

    fn handle_drop(
        &mut self,
        key: ConnectionKey,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let Some(entry) = self.sessions.get_mut(&key) else {
            return Ok(());
        };
        if entry.pending_close.is_some() {
            return Ok(());
        }
        let events = entry.session.close(self.logical_now).unwrap_or_default();
        self.record_events(key, events, outbound, pump_events)
    }

    fn handle_packet(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        if datagram.len() > self.config.max_datagram_size.get() {
            self.stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
            self.stats
                .oversized_datagrams
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let packet = match PacketRef::decode(datagram) {
            Ok(packet) => packet,
            Err(error) => {
                self.stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                self.stats.malformed_packets.fetch_add(1, Ordering::Relaxed);
                trace!(
                    target: "veloq_reliable_udp::endpoint",
                    peer = ?peer,
                    datagram_len = datagram.len(),
                    error = ?error,
                    "protocol loop ignored undecodable datagram"
                );
                return Ok(());
            }
        };
        trace!(
            target: "veloq_reliable_udp::endpoint",
            peer = ?peer,
            connection_id = packet.connection_id.get(),
            flags = packet.flags.bits(),
            sequence = packet.sequence,
            ack_largest = packet.ack_largest,
            payload_len = packet.payload.len(),
            "protocol loop received datagram"
        );
        let key = ConnectionKey {
            peer,
            connection_id: packet.connection_id,
        };
        if !self.sessions.contains_key(&key) {
            if !packet.flags.contains(Flags::SYN) || !self.can_accept_new_session() {
                self.stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .unknown_connections
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            self.start_server(key)?;
        }

        let events = {
            let Some(entry) = self.sessions.get_mut(&key) else {
                return Ok(());
            };
            match entry.session.receive(self.logical_now, datagram) {
                Ok(events) => events,
                Err(error) => {
                    if error == Error::UnknownConnection {
                        self.stats
                            .unknown_connections
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }
        };
        let mut events = events;
        events.extend(self.complete_pending_recv(key)?);
        self.record_events(key, events, outbound, pump_events)
    }

    fn start_server(&mut self, key: ConnectionKey) -> Result<()> {
        let session = Session::new_server(key.connection_id, self.config.clone())?;
        self.sessions.insert(
            key,
            SessionEntry {
                session,
                observed_stats: SessionStatsSnapshot::default(),
                connect_reply: None,
                connect_send_pending: false,
                connect_completion_pending: false,
                pending_send: HashMap::default(),
                pending_recv: None,
                pending_close: None,
                accepted: false,
                read_shutdown: false,
            },
        );
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn can_accept_new_session(&self) -> bool {
        if self.sessions.len() >= self.config.max_connections.get() {
            return false;
        }
        let half_open = self
            .sessions
            .values()
            .filter(|entry| {
                matches!(
                    entry.session.state(),
                    SessionState::Listen | SessionState::SynReceived | SessionState::SynSent
                )
            })
            .count();
        half_open < self.config.accept_capacity.get()
    }

    fn complete_pending_recv(&mut self, key: ConnectionKey) -> Result<Vec<SessionEvent>> {
        let ctx = self.ctx;
        let max_datagram_size = self.config.max_datagram_size;
        let Some(entry) = self.sessions.get_mut(&key) else {
            return Ok(Vec::new());
        };
        let Some(reply) = entry.pending_recv.take() else {
            return Ok(Vec::new());
        };
        if reply.is_closed() {
            return Ok(Vec::new());
        }
        let Some(message) = entry.session.recv(self.logical_now) else {
            entry.pending_recv = Some(reply);
            return Ok(Vec::new());
        };
        let response = Self::message_from_session(ctx, max_datagram_size, message);
        let _ = reply.send(response);
        Ok(entry.session.drain_events())
    }

    fn message_from_session(
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

    fn record_events(
        &mut self,
        key: ConnectionKey,
        events: Vec<SessionEvent>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        self.update_session_stats(key);
        let mut batch = self.collect_events(key, events)?;
        if batch.reject_accept {
            let abort_events = self
                .sessions
                .get_mut(&key)
                .ok_or(Error::ConnectionClosed)?
                .session
                .abort(self.logical_now, Error::TooManyConnections)?;
            let abort_batch = self.collect_events(key, abort_events)?;
            batch.datagrams.extend(abort_batch.datagrams);
            batch.timer_commands.extend(abort_batch.timer_commands);
            batch.terminal_error = Some(Error::TooManyConnections);
        }

        let session_state = self.sessions.get(&key).map(|entry| entry.session.state());
        let mut terminal = batch.terminal_error.or_else(|| {
            session_state
                .filter(|state| {
                    matches!(
                        state,
                        SessionState::Closed | SessionState::Failed | SessionState::Reset
                    )
                })
                .map(|_| Error::ConnectionClosed)
        });
        if terminal.is_some() {
            self.timer_registry.cancel_connection(&mut self.wheel, key);
        } else {
            self.apply_timer_commands(key, batch.timer_commands)?;
        }

        if self.enqueue_datagrams(key, batch.datagrams, outbound, pump_events) {
            terminal = Some(Error::OutboundQueueFull);
            let reset_events = self
                .sessions
                .get_mut(&key)
                .ok_or(Error::ConnectionClosed)?
                .session
                .abort(self.logical_now, Error::OutboundQueueFull)?;
            self.enqueue_reset(key, reset_events, outbound);
        }

        if let Some(error) = terminal {
            self.timer_registry.cancel_connection(&mut self.wheel, key);
            self.update_session_stats(key);
            if let Some(entry) = self.sessions.get_mut(&key) {
                remove_session_gauges(&self.stats, entry.observed_stats);
                finish_session(entry, error);
            }
            self.sessions.remove(&key);
            self.stats
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn update_session_stats(&mut self, key: ConnectionKey) {
        let Some(entry) = self.sessions.get_mut(&key) else {
            return;
        };
        let current = entry.session.stats();
        let previous = entry.observed_stats;
        add_delta(
            &self.stats.packets_sent,
            previous.packets_sent,
            current.packets_sent,
        );
        add_delta(
            &self.stats.packets_received,
            previous.packets_received,
            current.packets_received,
        );
        add_delta(
            &self.stats.data_packets,
            previous.data_packets,
            current.data_packets,
        );
        add_delta(
            &self.stats.ack_packets,
            previous.ack_packets,
            current.ack_packets,
        );
        add_delta(
            &self.stats.retransmissions,
            previous.retransmissions,
            current.retransmissions,
        );
        add_delta(
            &self.stats.duplicate_packets,
            previous.duplicate_packets,
            current.duplicate_packets,
        );
        add_delta(
            &self.stats.dropped_packets,
            previous.dropped_packets,
            current.dropped_packets,
        );
        add_delta(
            &self.stats.receive_window_drops,
            previous.receive_window_drops,
            current.receive_window_drops,
        );
        add_delta(
            &self.stats.out_of_window_drops,
            previous.out_of_window_drops,
            current.out_of_window_drops,
        );
        add_delta(
            &self.stats.duplicate_acks,
            previous.duplicate_acks,
            current.duplicate_acks,
        );
        add_delta(
            &self.stats.rtt_samples,
            previous.rtt_samples,
            current.rtt_samples,
        );
        add_delta(
            &self.stats.ack_delayed,
            previous.ack_delayed,
            current.ack_delayed,
        );
        add_delta(
            &self.stats.piggybacked_acks,
            previous.piggybacked_acks,
            current.piggybacked_acks,
        );
        if current.latest_rtt != previous.latest_rtt
            && let Some(rtt) = current.latest_rtt
        {
            self.stats
                .latest_rtt_nanos
                .store(duration_nanos(rtt), Ordering::Relaxed);
        }
        if let Some(rtt) = current.min_rtt {
            self.stats
                .min_rtt_nanos
                .fetch_min(duration_nanos(rtt), Ordering::Relaxed);
        }
        if let Some(rtt) = current.max_rtt {
            self.stats
                .max_rtt_nanos
                .fetch_max(duration_nanos(rtt), Ordering::Relaxed);
        }
        adjust_gauge(
            &self.stats.rto_nanos,
            duration_nanos(previous.rto),
            duration_nanos(current.rto),
        );
        adjust_gauge(
            &self.stats.congestion_window,
            previous.congestion_window as u64,
            current.congestion_window as u64,
        );
        adjust_gauge(
            &self.stats.send_window,
            previous.send_window as u64,
            current.send_window as u64,
        );
        adjust_gauge(
            &self.stats.peer_receive_window,
            previous.peer_receive_window as u64,
            current.peer_receive_window as u64,
        );
        adjust_gauge(
            &self.stats.receive_window,
            previous.receive_window as u64,
            current.receive_window as u64,
        );
        entry.observed_stats = current;
    }

    fn collect_events(
        &mut self,
        key: ConnectionKey,
        events: Vec<SessionEvent>,
    ) -> Result<EventBatch> {
        let command = self.command.clone();
        let max_payload = self.config.max_payload();
        let mut batch = EventBatch::default();
        for event in events {
            let Some(entry) = self.sessions.get_mut(&key) else {
                return Ok(batch);
            };
            match event {
                SessionEvent::Outbound(datagram) => batch.datagrams.push_back(datagram),
                SessionEvent::ArmTimer(command) | SessionEvent::CancelTimer(command) => {
                    trace!(
                        target: "veloq_reliable_udp::endpoint",
                        connection_id = key.connection_id.get(),
                        command = ?command,
                        "protocol loop received timer command"
                    );
                    batch.timer_commands.push(command);
                }
                SessionEvent::SendAcked(receipt) => {
                    debug!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?key.peer,
                        connection_id = key.connection_id.get(),
                        token = receipt.token.get(),
                        sequence = receipt.sequence.get(),
                        retransmissions = receipt.retransmissions,
                        rtt = ?receipt.rtt,
                        "protocol loop received SendAcked"
                    );
                    if let Some(reply) = entry.pending_send.remove(&receipt.token) {
                        let _ = reply.send(Ok(receipt));
                    }
                }
                SessionEvent::SendFailed { token, error, .. } => {
                    if let Some(reply) = entry.pending_send.remove(&token) {
                        let _ = reply.send(Err(error));
                    }
                }
                SessionEvent::StateChanged(SessionState::Established) => {
                    debug!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?key.peer,
                        connection_id = key.connection_id.get(),
                        "protocol loop observed established state"
                    );
                    if entry.session.role() == Role::Client && entry.connect_reply.is_some() {
                        entry.connect_send_pending = true;
                    } else if !entry.accepted {
                        let connection = Connection::new(command.clone(), key, max_payload);
                        match self.accept_tx.try_send(connection) {
                            Ok(()) => entry.accepted = true,
                            Err(TrySendError::Full(mut returned))
                            | Err(TrySendError::Closed(mut returned)) => {
                                returned.suppress_drop();
                                drop(returned);
                                batch.reject_accept = true;
                            }
                        }
                    }
                }
                SessionEvent::StateChanged(SessionState::Closed)
                | SessionEvent::StateChanged(SessionState::Failed) => {
                    batch.terminal_error = Some(Error::ConnectionClosed);
                }
                SessionEvent::StateChanged(SessionState::Reset) => {
                    batch.terminal_error = Some(Error::ConnectionReset);
                }
                SessionEvent::Failed(error) => batch.terminal_error = Some(error),
                SessionEvent::MessageAvailable | SessionEvent::StateChanged(_) => {}
            }
        }
        Ok(batch)
    }

    fn apply_timer_commands(
        &mut self,
        key: ConnectionKey,
        commands: Vec<TimerCommand>,
    ) -> Result<()> {
        for command in commands {
            match command {
                TimerCommand::Cancel {
                    kind, generation, ..
                } => self
                    .timer_registry
                    .cancel(&mut self.wheel, key, kind, generation),
                TimerCommand::Arm {
                    kind,
                    generation,
                    delay,
                } => self.timer_registry.arm(
                    &mut self.wheel,
                    key,
                    kind,
                    generation,
                    delay,
                    self.logical_now,
                )?,
            }
        }
        Ok(())
    }

    fn enqueue_datagrams(
        &mut self,
        key: ConnectionKey,
        mut datagrams: VecDeque<Vec<u8>>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> bool {
        let Some(entry) = self.sessions.get_mut(&key) else {
            return false;
        };
        let mut queue_full = false;
        while let Some(datagram) = datagrams.pop_front() {
            let completion = entry.connect_send_pending.then(|| pump_events.clone());
            if let Ok(packet) = Packet::decode(&datagram) {
                trace!(
                    target: "veloq_reliable_udp::endpoint",
                    peer = ?key.peer,
                    connection_id = packet.connection_id.get(),
                    flags = packet.flags.bits(),
                    sequence = packet.sequence,
                    ack_largest = packet.ack_largest,
                    payload_len = packet.payload.len(),
                    completion = completion.is_some(),
                    "protocol loop enqueuing outbound datagram"
                );
            }
            let item = OutboundDatagram {
                key,
                datagram,
                completion,
            };
            match outbound.try_send(item) {
                Ok(()) => {
                    if entry.connect_send_pending {
                        entry.connect_send_pending = false;
                        entry.connect_completion_pending = true;
                    }
                }
                Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                    self.stats.outbound_dropped.fetch_add(1, Ordering::Relaxed);
                    self.stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                    queue_full = true;
                    warn!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?key.peer,
                        connection_id = key.connection_id.get(),
                        "protocol loop dropped outbound datagram because queue is full or closed"
                    );
                }
            }
        }
        queue_full
    }

    fn enqueue_reset(
        &mut self,
        key: ConnectionKey,
        events: Vec<SessionEvent>,
        outbound: &OutboundSender,
    ) {
        for event in events {
            if let SessionEvent::Outbound(datagram) = event {
                let item = OutboundDatagram {
                    key,
                    datagram,
                    completion: None,
                };
                let _ = outbound.try_send(item);
            }
        }
    }

    fn handle_send_completion(
        &mut self,
        key: ConnectionKey,
        result: Result<()>,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let Some(entry) = self.sessions.get_mut(&key) else {
            return Ok(());
        };
        if !entry.connect_completion_pending {
            return Ok(());
        }
        entry.connect_completion_pending = false;
        match result {
            Ok(()) => {
                if let Some(reply) = entry.connect_reply.take() {
                    let _ = reply.send(Ok(()));
                }
            }
            Err(error) => {
                let events = entry.session.abort(self.logical_now, error)?;
                self.record_events(key, events, outbound, pump_events)?;
            }
        }
        Ok(())
    }

    fn handle_send_error(
        &mut self,
        key: ConnectionKey,
        error: Error,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let Some(entry) = self.sessions.get_mut(&key) else {
            return Ok(());
        };
        let events = entry.session.abort(self.logical_now, error)?;
        self.record_events(key, events, outbound, pump_events)
    }

    fn advance_endpoint_clock(
        &mut self,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_advanced);
        self.last_advanced = now;
        self.logical_now = self.logical_now.saturating_add(elapsed);
        let mut expired = Vec::new();
        self.wheel.advance_by(elapsed, &mut expired)?;
        if !expired.is_empty() {
            trace!(
                target: "veloq_reliable_udp::endpoint",
                elapsed = ?elapsed,
                logical_now = ?self.logical_now,
                expired = expired.len(),
                "endpoint timer wheel produced expired entries"
            );
        }
        for timer in expired {
            let Some((slot, deadline)) = self.timer_registry.take_expired(&timer) else {
                continue;
            };
            let delay = self.logical_now.saturating_sub(deadline);
            self.stats.timer_expirations.fetch_add(1, Ordering::Relaxed);
            self.stats
                .timer_delay_samples
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .timer_delay_total_nanos
                .fetch_add(duration_nanos(delay), Ordering::Relaxed);
            self.stats
                .timer_delay_max_nanos
                .fetch_max(duration_nanos(delay), Ordering::Relaxed);
            trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?slot.connection.peer,
                connection_id = slot.connection.connection_id.get(),
                kind = ?slot.kind,
                generation = slot.generation,
                "endpoint dispatching timer"
            );
            let events = {
                let Some(entry) = self.sessions.get_mut(&slot.connection) else {
                    continue;
                };
                entry
                    .session
                    .on_timer(self.logical_now, slot.kind, slot.generation)?
            };
            if !events.is_empty() {
                self.record_events(slot.connection, events, outbound, pump_events)?;
            }
        }
        let keys: Vec<ConnectionKey> = self.sessions.keys().copied().collect();
        for key in keys {
            let Some(entry) = self.sessions.get_mut(&key) else {
                continue;
            };
            if entry
                .pending_recv
                .as_ref()
                .is_some_and(|reply| reply.is_closed())
            {
                entry.pending_recv = None;
            }
            if entry
                .pending_close
                .as_ref()
                .is_some_and(|reply| reply.is_closed())
            {
                entry.pending_close = None;
            }
            if entry
                .connect_reply
                .as_ref()
                .is_some_and(|reply| reply.is_closed())
            {
                let events = entry
                    .session
                    .abort(self.logical_now, Error::ConnectionClosed)?;
                self.record_events(key, events, outbound, pump_events)?;
            }
        }
        Ok(())
    }

    fn shutdown(
        &mut self,
        error: Error,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let keys: Vec<ConnectionKey> = self.sessions.keys().copied().collect();
        for key in keys {
            let events = match self.sessions.get_mut(&key) {
                Some(entry) => entry
                    .session
                    .abort(self.logical_now, error)
                    .unwrap_or_default(),
                None => continue,
            };
            self.record_events(key, events, outbound, pump_events)?;
        }
        self.sessions.clear();
        self.timer_registry.clear(&mut self.wheel);
        while let Ok(mut connection) = self.accept.try_recv() {
            connection.suppress_drop();
        }
        Ok(())
    }
}

fn finish_session(entry: &mut SessionEntry, error: Error) {
    if let Some(reply) = entry.connect_reply.take() {
        let _ = reply.send(Err(error));
    }
    if let Some(reply) = entry.pending_recv.take() {
        let _ = reply.send(Err(error));
    }
    if let Some(reply) = entry.pending_close.take() {
        let _ = reply.send(if error == Error::ConnectionClosed {
            Ok(())
        } else {
            Err(error)
        });
    }
    for (_, reply) in entry.pending_send.drain() {
        let _ = reply.send(Err(error));
    }
}

async fn receive_pump<'rt>(
    ctx: Ctx<'rt>,
    socket: UdpSocket<'rt>,
    config: Config,
    inbound: InboundSender,
    pump_events: PumpSender,
    stats: Arc<EndpointStats>,
) -> Result<()> {
    trace!(
        target: "veloq_reliable_udp::endpoint",
        "receive pump started"
    );
    let mut recv = match prepare_recv(ctx, &socket, &config) {
        Ok(recv) => recv,
        Err(error) => {
            report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(error)).await;
            return Ok(());
        }
    };
    if let Err(error) = recv.arm().await.map_err(|_| Error::Io) {
        report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(error)).await;
        return Ok(());
    }
    trace!(
        target: "veloq_reliable_udp::endpoint",
        "receive pump armed"
    );
    if pump_events.send(PumpEvent::ReceiveReady).await.is_err() {
        return Ok(());
    }

    loop {
        let packet = match recv.await {
            Ok(packet) => packet,
            Err(_) => {
                report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(Error::Io)).await;
                return Ok(());
            }
        };
        let peer = packet.addr;
        let datagram = match packet.buf.into_fixed_buf() {
            Some(datagram) => datagram,
            None => {
                report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(Error::Io)).await;
                return Ok(());
            }
        };
        if let Ok(packet) = PacketRef::decode(datagram.as_slice()) {
            trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?peer,
                connection_id = packet.connection_id.get(),
                flags = packet.flags.bits(),
                sequence = packet.sequence,
                ack_largest = packet.ack_largest,
                payload_len = packet.payload.len(),
                "receive pump completed datagram"
            );
        } else {
            trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?peer,
                datagram_len = datagram.len(),
                "receive pump completed undecodable datagram"
            );
        }

        recv = match prepare_recv(ctx, &socket, &config) {
            Ok(recv) => recv,
            Err(error) => {
                report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(error)).await;
                return Ok(());
            }
        };
        if let Err(error) = recv.arm().await.map_err(|_| Error::Io) {
            report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(error)).await;
            return Ok(());
        }
        trace!(
            target: "veloq_reliable_udp::endpoint",
            peer = ?peer,
            "receive pump re-armed before enqueue"
        );

        let packet = InboundDatagram { peer, datagram };
        match inbound.try_send(packet) {
            Ok(()) => trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?peer,
                "receive pump enqueued datagram"
            ),
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                stats.inbound_dropped.fetch_add(1, Ordering::Relaxed);
                stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "veloq_reliable_udp::endpoint",
                    peer = ?peer,
                    "receive pump dropped datagram because inbound queue is full or closed"
                );
            }
        }
    }
}

async fn send_pump<'rt>(
    ctx: Ctx<'rt>,
    socket: UdpSocket<'rt>,
    config: Config,
    outbound: OutboundReceiver,
    pump_events: PumpSender,
) -> Result<()> {
    trace!(
        target: "veloq_reliable_udp::endpoint",
        "send pump started"
    );
    if pump_events.send(PumpEvent::SendReady).await.is_err() {
        return Ok(());
    }
    while let Ok(item) = outbound.recv().await {
        if let Ok(packet) = Packet::decode(&item.datagram) {
            trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?item.key.peer,
                connection_id = packet.connection_id.get(),
                flags = packet.flags.bits(),
                sequence = packet.sequence,
                ack_largest = packet.ack_largest,
                payload_len = packet.payload.len(),
                completion = item.completion.is_some(),
                "send pump submitting datagram"
            );
        }
        let result = send_datagram(
            ctx,
            &socket,
            config.max_datagram_size,
            item.key.peer,
            item.datagram,
        )
        .await;
        match &result {
            Ok(()) => trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?item.key.peer,
                completion = item.completion.is_some(),
                "send pump completed datagram"
            ),
            Err(error) => warn!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?item.key.peer,
                error = ?error,
                "send pump failed datagram"
            ),
        }
        match item.completion {
            Some(completion) => {
                if completion
                    .send(PumpEvent::SendCompleted {
                        key: item.key,
                        result,
                    })
                    .await
                    .is_err()
                {
                    return Err(Error::Io);
                }
            }
            None => {
                if let Err(error) = result
                    && pump_events
                        .send(PumpEvent::SendFailed {
                            key: item.key,
                            error,
                        })
                        .await
                        .is_err()
                {
                    return Err(Error::Io);
                }
            }
        }
    }
    Ok(())
}

async fn report_pump_failure(pump_events: &PumpSender, event: PumpEvent) {
    let _ = pump_events.send(event).await;
}

fn prepare_recv<'rt>(
    ctx: Ctx<'rt>,
    socket: &UdpSocket<'rt>,
    config: &Config,
) -> Result<PreparedUdpRecv<'rt>> {
    let buffer = ctx
        .try_alloc_full(config.max_datagram_size)
        .map_err(|_| Error::Io)?;
    Ok(socket.prepare_recv_from(buffer))
}

async fn send_datagram<'rt>(
    ctx: Ctx<'rt>,
    socket: &UdpSocket<'rt>,
    max_datagram_size: NonZeroUsize,
    peer: SocketAddr,
    datagram: Vec<u8>,
) -> Result<()> {
    let length = datagram.len();
    let mut buffer = ctx
        .try_alloc(max_datagram_size, length)
        .map_err(|_| Error::Io)?;
    buffer.spare_capacity_mut()[..length].copy_from_slice(&datagram);
    buffer.set_len(length);
    socket.send_to(buffer, peer).await.map_err(|_| Error::Io)?;
    Ok(())
}

fn next_connection_id() -> ConnectionId {
    loop {
        let value = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        if let Some(connection_id) = ConnectionId::new(value) {
            return connection_id;
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn duration_from_nanos(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}

fn add_delta(counter: &NativeAtomicU64, previous: u64, current: u64) {
    counter.fetch_add(current.saturating_sub(previous), Ordering::Relaxed);
}

fn adjust_gauge(counter: &NativeAtomicU64, previous: u64, current: u64) {
    if current >= previous {
        counter.fetch_add(current - previous, Ordering::Relaxed);
    } else {
        counter.fetch_sub(previous - current, Ordering::Relaxed);
    }
}

fn remove_session_gauges(stats: &EndpointStats, snapshot: SessionStatsSnapshot) {
    adjust_gauge(&stats.rto_nanos, duration_nanos(snapshot.rto), 0);
    adjust_gauge(
        &stats.congestion_window,
        snapshot.congestion_window as u64,
        0,
    );
    adjust_gauge(&stats.send_window, snapshot.send_window as u64, 0);
    adjust_gauge(
        &stats.peer_receive_window,
        snapshot.peer_receive_window as u64,
        0,
    );
    adjust_gauge(&stats.receive_window, snapshot.receive_window as u64, 0);
}

#[cfg(test)]
mod tests {
    use veloq_std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        time::Duration,
        vec::Vec,
    };
    use veloq_wheel::Wheel;

    use super::{ConnectionKey, EndpointTimerRegistry};
    use crate::{
        Config,
        packet::{ConnectionId, MessageSequence},
        timer::TimerKind,
    };

    fn key(port: u16, connection_id: u64) -> ConnectionKey {
        ConnectionKey {
            peer: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            connection_id: ConnectionId::new(connection_id).expect("connection ID"),
        }
    }

    #[test]
    fn timer_registry_replaces_and_cancels_one_logical_slot() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let connection = key(10_001, 1);
        let kind = TimerKind::Retransmit {
            sequence: MessageSequence::new(1).expect("sequence"),
        };

        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                1,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("first arm");
        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                1,
                Duration::from_millis(30),
                Duration::ZERO,
            )
            .expect("replacement arm");
        assert_eq!(wheel.len(), 1);

        registry.cancel(&mut wheel, connection, kind, 1);
        assert!(wheel.is_empty());
    }

    #[test]
    fn timer_registry_accepts_only_the_current_expired_id() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let connection = key(10_002, 2);
        let kind = TimerKind::AckDelay;
        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                7,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("arm");

        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(20), &mut expired)
            .expect("advance");
        assert_eq!(expired.len(), 1);
        assert!(registry.take_expired(&expired[0]).is_some());
        assert!(registry.take_expired(&expired[0]).is_none());
    }

    #[test]
    fn timer_registry_cleanup_keeps_other_connections_live() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let first = key(10_003, 3);
        let second = key(10_004, 4);
        registry
            .arm(
                &mut wheel,
                first,
                TimerKind::HandshakeRetry,
                1,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("first arm");
        registry
            .arm(
                &mut wheel,
                second,
                TimerKind::HandshakeRetry,
                1,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("second arm");

        registry.cancel_connection(&mut wheel, first);
        assert_eq!(wheel.len(), 1);
        registry.cancel_connection(&mut wheel, second);
        assert!(wheel.is_empty());
    }
}
