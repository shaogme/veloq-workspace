#![allow(dead_code)]

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tracing::{debug, trace, warn};
use veloq::{
    net::{PreparedUdpRecv, UdpSocket},
    runtime::{
        Outcome,
        context::Ctx,
        scope,
        scope::{JoinHandle, ScopeProvider},
        select,
        task::TaskHandleRef,
    },
    std::{
        net::SocketAddr,
        num::NonZeroUsize,
        result::Result as StdResult,
        time::{Duration, Instant},
        vec::Vec,
    },
    sync::{
        TryRecvError, TrySendError,
        mpmc::{BoundedOwnedReceiver, BoundedOwnedSender, owned_bounded},
        oneshot,
    },
    time::sleep,
};

use veloq_reliable_udp::{Flags, MessageSequence, PacketRef};

const CHANNEL_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 32;
const MAX_PENDING_DATAGRAMS: usize = 64;
const MAX_COORDINATOR_BATCH: usize = 16;

type DatagramSender = BoundedOwnedSender<ReceivedDatagram>;
type DatagramReceiver = BoundedOwnedReceiver<ReceivedDatagram>;
type ForwardSender = BoundedOwnedSender<ForwardDatagram>;
type ForwardReceiver = BoundedOwnedReceiver<ForwardDatagram>;
type CompletionSender = BoundedOwnedSender<SendCompleted>;
type CompletionReceiver = BoundedOwnedReceiver<SendCompleted>;
type DriverEventSender = BoundedOwnedSender<DriverEvent>;
type DriverEventReceiver = BoundedOwnedReceiver<DriverEvent>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyDirection {
    ClientToServer,
    ServerToClient,
}

impl ProxyDirection {
    fn index(self) -> usize {
        match self {
            Self::ClientToServer => 0,
            Self::ServerToClient => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketMatcher {
    Any,
    Syn,
    SynAck,
    AckOnly,
    Data { sequence: Option<MessageSequence> },
    Fin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyAction {
    Forward,
    Drop,
    Duplicate,
    Delay(Duration),
    Reorder { count: NonZeroUsize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyStatsSnapshot {
    pub receive_started: u64,
    pub receive_armed: u64,
    pub receive_completed: u64,
    pub receive_rearmed: u64,
    pub receive_failed: u64,
    pub receive_cancelled: u64,
    pub unexpected_source: u64,
    pub inbound_queue_full: u64,
    pub actions_seen: u64,
    pub forwarded: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub duplicate_outputs: u64,
    pub delayed: u64,
    pub reordered: u64,
    pub pending_high_watermark: u64,
    pub send_submitted: u64,
    pub send_completed: u64,
    pub send_failed: u64,
    pub outbound_queue_full: u64,
    // Compatibility alias for the original test-only proxy API.
    pub seen: u64,
}

pub struct ProxyStats {
    receive_started: AtomicU64,
    receive_armed: AtomicU64,
    receive_completed: AtomicU64,
    receive_rearmed: AtomicU64,
    receive_failed: AtomicU64,
    receive_cancelled: AtomicU64,
    unexpected_source: AtomicU64,
    inbound_queue_full: AtomicU64,
    actions_seen: AtomicU64,
    forwarded: AtomicU64,
    dropped: AtomicU64,
    duplicated: AtomicU64,
    duplicate_outputs: AtomicU64,
    delayed: AtomicU64,
    reordered: AtomicU64,
    pending_high_watermark: AtomicU64,
    send_submitted: AtomicU64,
    send_completed: AtomicU64,
    send_failed: AtomicU64,
    outbound_queue_full: AtomicU64,
}

impl ProxyStats {
    fn new() -> Self {
        Self {
            receive_started: AtomicU64::new(0),
            receive_armed: AtomicU64::new(0),
            receive_completed: AtomicU64::new(0),
            receive_rearmed: AtomicU64::new(0),
            receive_failed: AtomicU64::new(0),
            receive_cancelled: AtomicU64::new(0),
            unexpected_source: AtomicU64::new(0),
            inbound_queue_full: AtomicU64::new(0),
            actions_seen: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            duplicated: AtomicU64::new(0),
            duplicate_outputs: AtomicU64::new(0),
            delayed: AtomicU64::new(0),
            reordered: AtomicU64::new(0),
            pending_high_watermark: AtomicU64::new(0),
            send_submitted: AtomicU64::new(0),
            send_completed: AtomicU64::new(0),
            send_failed: AtomicU64::new(0),
            outbound_queue_full: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> ProxyStatsSnapshot {
        let actions_seen = self.actions_seen.load(Ordering::Relaxed);
        ProxyStatsSnapshot {
            receive_started: self.receive_started.load(Ordering::Relaxed),
            receive_armed: self.receive_armed.load(Ordering::Relaxed),
            receive_completed: self.receive_completed.load(Ordering::Relaxed),
            receive_rearmed: self.receive_rearmed.load(Ordering::Relaxed),
            receive_failed: self.receive_failed.load(Ordering::Relaxed),
            receive_cancelled: self.receive_cancelled.load(Ordering::Relaxed),
            unexpected_source: self.unexpected_source.load(Ordering::Relaxed),
            inbound_queue_full: self.inbound_queue_full.load(Ordering::Relaxed),
            actions_seen,
            forwarded: self.forwarded.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            duplicated: self.duplicated.load(Ordering::Relaxed),
            duplicate_outputs: self.duplicate_outputs.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            reordered: self.reordered.load(Ordering::Relaxed),
            pending_high_watermark: self.pending_high_watermark.load(Ordering::Relaxed),
            send_submitted: self.send_submitted.load(Ordering::Relaxed),
            send_completed: self.send_completed.load(Ordering::Relaxed),
            send_failed: self.send_failed.load(Ordering::Relaxed),
            outbound_queue_full: self.outbound_queue_full.load(Ordering::Relaxed),
            seen: actions_seen,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyError {
    Io,
    QueueFull,
    Shutdown,
}

struct ProxyRule {
    matcher: PacketMatcher,
    action: ProxyAction,
}

pub struct SocketProxy<'rt> {
    ctx: Ctx<'rt>,
    max_datagram_size: NonZeroUsize,
    client_side: UdpSocket<'rt>,
    server_side: UdpSocket<'rt>,
    client_to_server: VecDeque<ProxyRule>,
    server_to_client: VecDeque<ProxyRule>,
    stats: [Arc<ProxyStats>; 2],
    action_observed: BoundedOwnedSender<()>,
    action_receiver: Option<BoundedOwnedReceiver<()>>,
}

pub struct ProxyDriver<'rt> {
    ctx: Ctx<'rt>,
    max_datagram_size: NonZeroUsize,
    client_side: UdpSocket<'rt>,
    server_side: UdpSocket<'rt>,
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    client_to_server: VecDeque<ProxyRule>,
    server_to_client: VecDeque<ProxyRule>,
    stats: [Arc<ProxyStats>; 2],
    action_observed: BoundedOwnedSender<()>,
    ready: oneshot::OwnedSender<Result<(), ProxyError>>,
    shutdown: oneshot::OwnedReceiver<()>,
}

pub struct ProxyHandle {
    ready: Option<oneshot::OwnedReceiver<Result<(), ProxyError>>>,
    action_observed: Option<BoundedOwnedReceiver<()>>,
    shutdown: Option<oneshot::OwnedSender<()>>,
}

impl ProxyHandle {
    pub async fn wait_ready(&mut self) -> Result<(), ProxyError> {
        let receiver = self.ready.take().ok_or(ProxyError::Shutdown)?;
        receiver.await.map_err(|_| ProxyError::Shutdown)?
    }

    pub async fn wait_action_observed(&mut self) -> Result<(), ProxyError> {
        let receiver = self.action_observed.take().ok_or(ProxyError::Shutdown)?;
        receiver
            .recv()
            .await
            .map(|_| ())
            .map_err(|_| ProxyError::Shutdown)
    }

    pub fn shutdown(&mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }
    }
}

impl<'rt> SocketProxy<'rt> {
    pub fn bind(ctx: Ctx<'rt>, max_datagram_size: NonZeroUsize) -> Result<Self, ProxyError> {
        let client_side = UdpSocket::bind(ctx, "127.0.0.1:0").map_err(|_| ProxyError::Io)?;
        let server_side = UdpSocket::bind(ctx, "127.0.0.1:0").map_err(|_| ProxyError::Io)?;
        let (action_observed, action_receiver) = owned_bounded(1);
        Ok(Self {
            ctx,
            max_datagram_size,
            client_side,
            server_side,
            client_to_server: VecDeque::new(),
            server_to_client: VecDeque::new(),
            stats: [Arc::new(ProxyStats::new()), Arc::new(ProxyStats::new())],
            action_observed,
            action_receiver: Some(action_receiver),
        })
    }

    pub fn client_side_addr(&self) -> Result<SocketAddr, ProxyError> {
        self.client_side.local_addr().map_err(|_| ProxyError::Io)
    }

    pub fn server_side_addr(&self) -> Result<SocketAddr, ProxyError> {
        self.server_side.local_addr().map_err(|_| ProxyError::Io)
    }

    /// 返回客户端到服务端方向的统计，保留原有测试代理 API。
    pub fn stats(&self) -> Arc<ProxyStats> {
        self.stats_for(ProxyDirection::ClientToServer)
    }

    pub fn stats_for(&self, direction: ProxyDirection) -> Arc<ProxyStats> {
        self.stats[direction.index()].clone()
    }

    pub fn push_action(
        &mut self,
        direction: ProxyDirection,
        matcher: PacketMatcher,
        action: ProxyAction,
    ) {
        let rules = match direction {
            ProxyDirection::ClientToServer => &mut self.client_to_server,
            ProxyDirection::ServerToClient => &mut self.server_to_client,
        };
        rules.push_back(ProxyRule { matcher, action });
    }

    pub fn start(
        self,
        server_addr: SocketAddr,
        client_addr: SocketAddr,
    ) -> (ProxyDriver<'rt>, ProxyHandle) {
        let (shutdown, shutdown_receiver) = oneshot::owned_channel();
        let (ready, ready_receiver) = oneshot::owned_channel();
        (
            ProxyDriver {
                ctx: self.ctx,
                max_datagram_size: self.max_datagram_size,
                client_side: self.client_side,
                server_side: self.server_side,
                server_addr,
                client_addr,
                client_to_server: self.client_to_server,
                server_to_client: self.server_to_client,
                stats: self.stats,
                action_observed: self.action_observed,
                ready,
                shutdown: shutdown_receiver,
            },
            ProxyHandle {
                ready: Some(ready_receiver),
                action_observed: self.action_receiver,
                shutdown: Some(shutdown),
            },
        )
    }
}

impl ProxyDriver<'_> {
    pub async fn run(self) -> Result<(), ProxyError> {
        let ProxyDriver {
            ctx,
            max_datagram_size,
            client_side,
            server_side,
            server_addr,
            client_addr,
            client_to_server,
            server_to_client,
            stats,
            action_observed,
            ready,
            mut shutdown,
        } = self;

        let result = {
            let (driver_events, mut driver_event_receiver) = owned_bounded(EVENT_CAPACITY);
            let (client_inbound, client_inbound_receiver) = owned_bounded(CHANNEL_CAPACITY);
            let (client_outbound, client_outbound_receiver) = owned_bounded(CHANNEL_CAPACITY);
            let (client_completions, client_completion_receiver) = owned_bounded(CHANNEL_CAPACITY);
            let (server_inbound, server_inbound_receiver) = owned_bounded(CHANNEL_CAPACITY);
            let (server_outbound, server_outbound_receiver) = owned_bounded(CHANNEL_CAPACITY);
            let (server_completions, server_completion_receiver) = owned_bounded(CHANNEL_CAPACITY);

            let scoped = scope!(ctx, async |scope| {
                let mut client_receive = scope.spawn_boxed(receive_pump(
                    ctx,
                    client_side.clone(),
                    max_datagram_size,
                    client_addr,
                    client_inbound,
                    driver_events.clone(),
                    stats[ProxyDirection::ClientToServer.index()].clone(),
                    ProxyDirection::ClientToServer,
                ));
                let mut client_send = scope.spawn_boxed(send_pump(
                    ctx,
                    server_side.clone(),
                    max_datagram_size,
                    server_addr,
                    client_outbound_receiver,
                    client_completions.clone(),
                    driver_events.clone(),
                    stats[ProxyDirection::ClientToServer.index()].clone(),
                    ProxyDirection::ClientToServer,
                ));
                let mut client_coordinator = scope.spawn_boxed(coordinator(
                    ctx,
                    max_datagram_size,
                    client_to_server,
                    client_inbound_receiver,
                    client_outbound,
                    client_completion_receiver,
                    client_completions,
                    driver_events.clone(),
                    action_observed.clone(),
                    stats[ProxyDirection::ClientToServer.index()].clone(),
                    ProxyDirection::ClientToServer,
                ));

                let mut server_receive = scope.spawn_boxed(receive_pump(
                    ctx,
                    server_side.clone(),
                    max_datagram_size,
                    server_addr,
                    server_inbound,
                    driver_events.clone(),
                    stats[ProxyDirection::ServerToClient.index()].clone(),
                    ProxyDirection::ServerToClient,
                ));
                let mut server_send = scope.spawn_boxed(send_pump(
                    ctx,
                    client_side.clone(),
                    max_datagram_size,
                    client_addr,
                    server_outbound_receiver,
                    server_completions.clone(),
                    driver_events.clone(),
                    stats[ProxyDirection::ServerToClient.index()].clone(),
                    ProxyDirection::ServerToClient,
                ));
                let mut server_coordinator = scope.spawn_boxed(coordinator(
                    ctx,
                    max_datagram_size,
                    server_to_client,
                    server_inbound_receiver,
                    server_outbound,
                    server_completion_receiver,
                    server_completions,
                    driver_events.clone(),
                    action_observed.clone(),
                    stats[ProxyDirection::ServerToClient.index()].clone(),
                    ProxyDirection::ServerToClient,
                ));
                drop(driver_events);

                let startup =
                    wait_for_startup(ctx, &mut driver_event_receiver, &mut shutdown).await;
                if let Err(error) = startup {
                    let _ = ready.send(Err(error));
                    cancel_proxy_tasks(
                        &mut client_receive,
                        &mut client_send,
                        &mut client_coordinator,
                        &mut server_receive,
                        &mut server_send,
                        &mut server_coordinator,
                    );
                    let _ = scope.wait_all().await;
                    return Err(error);
                }
                if ready.send(Ok(())).is_err() {
                    cancel_proxy_tasks(
                        &mut client_receive,
                        &mut client_send,
                        &mut client_coordinator,
                        &mut server_receive,
                        &mut server_send,
                        &mut server_coordinator,
                    );
                    let _ = scope.wait_all().await;
                    return Ok(());
                }

                let result = select! {
                    ctx;
                    event = driver_event_receiver.recv() => match event {
                        Ok(DriverEvent::Failure(error)) => {
                            warn!(
                                target: "veloq_reliable_udp::socket_proxy",
                                error = ?error,
                                "proxy driver observed child failure"
                            );
                            Err(error)
                        }
                        Ok(DriverEvent::CoordinatorStopped) => {
                            warn!(
                                target: "veloq_reliable_udp::socket_proxy",
                                "proxy coordinator stopped unexpectedly"
                            );
                            Err(ProxyError::Io)
                        }
                        Err(_) => {
                            warn!(
                                target: "veloq_reliable_udp::socket_proxy",
                                "proxy driver event channel disconnected"
                            );
                            Err(ProxyError::Io)
                        }
                        Ok(DriverEvent::Ready(_)) => Err(ProxyError::Io),
                    },
                    _ = &mut shutdown => {
                        trace!(
                            target: "veloq_reliable_udp::socket_proxy",
                            "proxy shutdown requested"
                        );
                        Ok(())
                    },
                };
                cancel_proxy_tasks(
                    &mut client_receive,
                    &mut client_send,
                    &mut client_coordinator,
                    &mut server_receive,
                    &mut server_send,
                    &mut server_coordinator,
                );
                let _ = scope.wait_all().await;
                result
            })
            .await
            .map_err(|_| ProxyError::Io)?;
            match scoped {
                Outcome::Ok(_) => Ok(()),
                Outcome::Err(error) => Err(error),
            }
        };

        let _ = client_side.close().await;
        let _ = server_side.close().await;
        result
    }
}

fn cancel_proxy_tasks<CR, CS, CC, SR, SS, SC>(
    client_receive: &mut CR,
    client_send: &mut CS,
    client_coordinator: &mut CC,
    server_receive: &mut SR,
    server_send: &mut SS,
    server_coordinator: &mut SC,
) where
    CR: ProxyCancel,
    CS: ProxyCancel,
    CC: ProxyCancel,
    SR: ProxyCancel,
    SS: ProxyCancel,
    SC: ProxyCancel,
{
    client_receive.cancel_task();
    client_send.cancel_task();
    client_coordinator.cancel_task();
    server_receive.cancel_task();
    server_send.cancel_task();
    server_coordinator.cancel_task();
}

trait ProxyCancel {
    fn cancel_task(&mut self);
}

impl<'scope_ref, T, R, S> ProxyCancel for JoinHandle<'scope_ref, T, R, S>
where
    R: TaskHandleRef,
    S: ScopeProvider + 'scope_ref,
    S::Arena: 'scope_ref,
{
    fn cancel_task(&mut self) {
        self.cancel();
    }
}

#[derive(Debug, Clone, Copy)]
enum StartupKind {
    Receive(ProxyDirection),
    Send(ProxyDirection),
    Coordinator(ProxyDirection),
}

#[derive(Debug, Clone, Copy)]
enum DriverEvent {
    Ready(StartupKind),
    Failure(ProxyError),
    CoordinatorStopped,
}

async fn wait_for_startup(
    ctx: Ctx<'_>,
    events: &mut DriverEventReceiver,
    shutdown: &mut oneshot::OwnedReceiver<()>,
) -> Result<(), ProxyError> {
    let mut ready = [[false; 3]; 2];
    while !(ready[0].iter().all(|value| *value) && ready[1].iter().all(|value| *value)) {
        let event = select! {
            ctx;
            event = events.recv() => StartupWait::Event(event),
            _ = &mut *shutdown => StartupWait::Shutdown,
        };
        let event = match event {
            StartupWait::Event(event) => event,
            StartupWait::Shutdown => return Err(ProxyError::Shutdown),
        };
        let event = match event {
            Ok(event) => event,
            Err(_) => return Err(ProxyError::Shutdown),
        };
        match event {
            DriverEvent::Ready(kind) => {
                let (direction, slot) = match kind {
                    StartupKind::Receive(direction) => (direction, 0),
                    StartupKind::Send(direction) => (direction, 1),
                    StartupKind::Coordinator(direction) => (direction, 2),
                };
                ready[direction.index()][slot] = true;
            }
            DriverEvent::Failure(error) => return Err(error),
            DriverEvent::CoordinatorStopped => return Err(ProxyError::Io),
        }
    }
    Ok(())
}

enum StartupWait {
    Event(StdResult<DriverEvent, TryRecvError>),
    Shutdown,
}

struct ReceivedDatagram {
    peer: SocketAddr,
    datagram: Vec<u8>,
    operation_id: u64,
}

struct ForwardDatagram {
    order: u64,
    datagram: Vec<u8>,
}

struct SendCompleted {
    order: u64,
    result: Result<(), ProxyError>,
}

#[allow(clippy::too_many_arguments)]
async fn receive_pump<'rt>(
    ctx: Ctx<'rt>,
    socket: UdpSocket<'rt>,
    max_datagram_size: NonZeroUsize,
    expected_peer: SocketAddr,
    inbound: DatagramSender,
    events: DriverEventSender,
    stats: Arc<ProxyStats>,
    direction: ProxyDirection,
) {
    stats.receive_started.fetch_add(1, Ordering::Relaxed);
    let mut operation_id: u64 = 0;
    let mut recv = match prepare_recv(ctx, &socket, max_datagram_size) {
        Ok(recv) => recv,
        Err(error) => {
            report_receive_failure(&events, &stats, error).await;
            return;
        }
    };
    if recv.arm().await.is_err() {
        report_receive_failure(&events, &stats, ProxyError::Io).await;
        return;
    }
    stats.receive_armed.fetch_add(1, Ordering::Relaxed);
    if report_event(&events, DriverEvent::Ready(StartupKind::Receive(direction)))
        .await
        .is_err()
    {
        return;
    }

    loop {
        let packet = match recv.await {
            Ok(packet) => packet,
            Err(_) => {
                report_receive_failure(&events, &stats, ProxyError::Io).await;
                return;
            }
        };
        stats.receive_completed.fetch_add(1, Ordering::Relaxed);
        operation_id = operation_id.wrapping_add(1);
        let peer = packet.addr;
        let datagram = packet.buf.as_slice().to_vec();

        // Re-arm before validating the source or touching the coordinator. This is the
        // invariant that prevents delayed forwarding from stealing the receive slot.
        recv = match prepare_recv(ctx, &socket, max_datagram_size) {
            Ok(recv) => recv,
            Err(error) => {
                report_receive_failure(&events, &stats, error).await;
                return;
            }
        };
        if recv.arm().await.is_err() {
            report_receive_failure(&events, &stats, ProxyError::Io).await;
            return;
        }
        stats.receive_rearmed.fetch_add(1, Ordering::Relaxed);

        if peer != expected_peer {
            stats.unexpected_source.fetch_add(1, Ordering::Relaxed);
            warn!(
                target: "veloq_reliable_udp::socket_proxy",
                direction = ?direction,
                source = ?peer,
                expected = ?expected_peer,
                operation_id,
                "proxy discarded datagram from unexpected source"
            );
            continue;
        }

        let received = ReceivedDatagram {
            peer,
            datagram,
            operation_id,
        };
        if inbound.try_send(received).is_err() {
            stats.inbound_queue_full.fetch_add(1, Ordering::Relaxed);
            report_receive_failure(&events, &stats, ProxyError::QueueFull).await;
            return;
        }
    }
}

async fn report_receive_failure(events: &DriverEventSender, stats: &ProxyStats, error: ProxyError) {
    stats.receive_failed.fetch_add(1, Ordering::Relaxed);
    warn!(
        target: "veloq_reliable_udp::socket_proxy",
        error = ?error,
        "proxy receive pump failed"
    );
    let _ = report_event(events, DriverEvent::Failure(error)).await;
}

#[allow(clippy::too_many_arguments)]
async fn send_pump<'rt>(
    ctx: Ctx<'rt>,
    socket: UdpSocket<'rt>,
    max_datagram_size: NonZeroUsize,
    target: SocketAddr,
    outbound: ForwardReceiver,
    completions: CompletionSender,
    events: DriverEventSender,
    stats: Arc<ProxyStats>,
    direction: ProxyDirection,
) {
    if report_event(&events, DriverEvent::Ready(StartupKind::Send(direction)))
        .await
        .is_err()
    {
        return;
    }
    while let Ok(item) = outbound.recv().await {
        stats.send_submitted.fetch_add(1, Ordering::Relaxed);
        let order = item.order;
        let result = send_datagram(ctx, &socket, max_datagram_size, target, item.datagram).await;
        match result {
            Ok(()) => {
                stats.send_completed.fetch_add(1, Ordering::Relaxed);
                trace!(
                    target: "veloq_reliable_udp::socket_proxy",
                    direction = ?direction,
                    order,
                    "proxy send completed"
                );
                if completions
                    .send(SendCompleted {
                        order,
                        result: Ok(()),
                    })
                    .await
                    .is_err()
                {
                    let _ = report_event(&events, DriverEvent::Failure(ProxyError::Io)).await;
                    return;
                }
            }
            Err(error) => {
                stats.send_failed.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "veloq_reliable_udp::socket_proxy",
                    direction = ?direction,
                    order,
                    error = ?error,
                    "proxy send failed"
                );
                let _ = completions
                    .send(SendCompleted {
                        order,
                        result: Err(error),
                    })
                    .await;
                let _ = report_event(&events, DriverEvent::Failure(error)).await;
                return;
            }
        }
    }
    trace!(
        target: "veloq_reliable_udp::socket_proxy",
        direction = ?direction,
        "proxy send pump stopped because outbound channel closed"
    );
}

struct PendingDatagram {
    deadline: Instant,
    order: u64,
    datagram: Vec<u8>,
}

struct ReorderBuffer {
    expected: usize,
    datagrams: Vec<Vec<u8>>,
}

struct Coordinator<'rt> {
    ctx: Ctx<'rt>,
    max_datagram_size: NonZeroUsize,
    rules: VecDeque<ProxyRule>,
    pending: Vec<PendingDatagram>,
    reorder: Option<ReorderBuffer>,
    next_order: u64,
    next_event_id: u64,
    events: DriverEventSender,
    action_observed: BoundedOwnedSender<()>,
    stats: Arc<ProxyStats>,
    direction: ProxyDirection,
}

#[allow(clippy::too_many_arguments)]
async fn coordinator<'rt>(
    ctx: Ctx<'rt>,
    max_datagram_size: NonZeroUsize,
    rules: VecDeque<ProxyRule>,
    inbound: DatagramReceiver,
    outbound: ForwardSender,
    completions: CompletionReceiver,
    _completion_sender: CompletionSender,
    events: DriverEventSender,
    action_observed: BoundedOwnedSender<()>,
    stats: Arc<ProxyStats>,
    direction: ProxyDirection,
) {
    let mut coordinator = Coordinator {
        ctx,
        max_datagram_size,
        rules,
        pending: Vec::new(),
        reorder: None,
        next_order: 0,
        next_event_id: 0,
        events: events.clone(),
        action_observed,
        stats,
        direction,
    };
    if report_event(
        &events,
        DriverEvent::Ready(StartupKind::Coordinator(direction)),
    )
    .await
    .is_err()
    {
        return;
    }
    let result = coordinator.run_inner(inbound, outbound, completions).await;
    if let Err(error) = result {
        warn!(
            target: "veloq_reliable_udp::socket_proxy",
            direction = ?direction,
            error = ?error,
            "proxy coordinator failed"
        );
        let _ = report_event(&events, DriverEvent::Failure(error)).await;
    } else {
        let _ = report_event(&events, DriverEvent::CoordinatorStopped).await;
    }
}

impl Coordinator<'_> {
    async fn run_inner(
        &mut self,
        inbound: DatagramReceiver,
        outbound: ForwardSender,
        completions: CompletionReceiver,
    ) -> Result<(), ProxyError> {
        loop {
            let mut progressed = false;
            for _ in 0..MAX_COORDINATOR_BATCH {
                if !self.send_one_due(&outbound)? {
                    break;
                }
                progressed = true;
            }
            for _ in 0..MAX_COORDINATOR_BATCH {
                match inbound.try_recv() {
                    Ok(datagram) => {
                        self.handle_datagram(datagram)?;
                        progressed = true;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Err(ProxyError::Io),
                }
            }
            for _ in 0..MAX_COORDINATOR_BATCH {
                match completions.try_recv() {
                    Ok(completion) => {
                        self.handle_completion(completion)?;
                        progressed = true;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Err(ProxyError::Io),
                }
            }
            if progressed {
                continue;
            }

            let next_deadline = self
                .pending
                .iter()
                .map(|item| item.deadline)
                .min_by_key(|deadline| *deadline);
            match next_deadline {
                Some(deadline) => {
                    let delay = deadline.saturating_duration_since(Instant::now());
                    let event = select! {
                        self.ctx;
                        datagram = inbound.recv() => CoordinatorEvent::Datagram(datagram.ok()),
                        completion = completions.recv() => CoordinatorEvent::Completion(completion.ok()),
                        _ = sleep(self.ctx, delay) => CoordinatorEvent::Due,
                    };
                    self.handle_event(event)?;
                }
                None => {
                    let event = select! {
                        self.ctx;
                        datagram = inbound.recv() => CoordinatorEvent::Datagram(datagram.ok()),
                        completion = completions.recv() => CoordinatorEvent::Completion(completion.ok()),
                    };
                    self.handle_event(event)?;
                }
            }
        }
    }

    fn handle_event(&mut self, event: CoordinatorEvent) -> Result<(), ProxyError> {
        match event {
            CoordinatorEvent::Datagram(Some(datagram)) => self.handle_datagram(datagram),
            CoordinatorEvent::Datagram(None) => Err(ProxyError::Io),
            CoordinatorEvent::Completion(Some(completion)) => self.handle_completion(completion),
            CoordinatorEvent::Completion(None) => Err(ProxyError::Io),
            CoordinatorEvent::Due => Ok(()),
        }
    }

    fn handle_completion(&mut self, completion: SendCompleted) -> Result<(), ProxyError> {
        self.next_event_id = self.next_event_id.wrapping_add(1);
        trace!(
            target: "veloq_reliable_udp::socket_proxy",
            direction = ?self.direction,
            event_id = self.next_event_id,
            order = completion.order,
            pending = self.pending.len(),
            "proxy observed send completion"
        );
        completion.result
    }

    fn handle_datagram(&mut self, received: ReceivedDatagram) -> Result<(), ProxyError> {
        self.next_event_id = self.next_event_id.wrapping_add(1);
        let ReceivedDatagram {
            peer,
            datagram,
            operation_id,
        } = received;
        trace_datagram(
            self.direction,
            peer,
            &datagram,
            operation_id,
            self.next_event_id,
        );

        if let Some(reorder) = &self.reorder {
            let complete = reorder.datagrams.len() + 1 == reorder.expected;
            if !complete {
                self.reorder
                    .as_mut()
                    .expect("reorder buffer exists")
                    .datagrams
                    .push(datagram);
                return Ok(());
            }
            if self.pending.len().saturating_add(reorder.expected) > MAX_PENDING_DATAGRAMS {
                return Err(ProxyError::QueueFull);
            }
            let mut datagrams = self
                .reorder
                .take()
                .expect("reorder buffer exists")
                .datagrams;
            datagrams.push(datagram);
            datagrams.reverse();
            self.schedule_vec(datagrams, Duration::ZERO)?;
            self.stats.reordered.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let packet = PacketRef::decode(&datagram).ok();
        let selected = self
            .rules
            .iter()
            .position(|rule| rule.matcher.matches(packet.as_ref()));
        let action = selected
            .and_then(|index| self.rules.get(index).map(|rule| rule.action))
            .unwrap_or(ProxyAction::Forward);
        self.stats.actions_seen.fetch_add(1, Ordering::Relaxed);
        if selected.is_some() {
            let _ = self.action_observed.try_send(());
        }
        debug!(
            target: "veloq_reliable_udp::socket_proxy",
            direction = ?self.direction,
            event_id = self.next_event_id,
            operation_id,
            action = ?action,
            pending = self.pending.len(),
            "proxy selected action"
        );

        if self.action_requires_capacity(action) {
            return Err(ProxyError::QueueFull);
        }
        if let Some(index) = selected {
            let _ = self.rules.remove(index);
        }

        match action {
            ProxyAction::Forward => {
                self.schedule_batch([datagram], Duration::ZERO)?;
            }
            ProxyAction::Drop => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
            ProxyAction::Duplicate => {
                let copy = datagram.clone();
                self.schedule_batch([copy, datagram], Duration::ZERO)?;
                self.stats.duplicated.fetch_add(1, Ordering::Relaxed);
                self.stats.duplicate_outputs.fetch_add(2, Ordering::Relaxed);
            }
            ProxyAction::Delay(delay) => {
                self.schedule_batch([datagram], delay)?;
                self.stats.delayed.fetch_add(1, Ordering::Relaxed);
            }
            ProxyAction::Reorder { count } => {
                self.start_reorder(datagram, count.get())?;
            }
        }
        Ok(())
    }

    fn action_requires_capacity(&self, action: ProxyAction) -> bool {
        match action {
            ProxyAction::Forward | ProxyAction::Delay(_) => {
                self.pending.len() >= MAX_PENDING_DATAGRAMS
            }
            ProxyAction::Duplicate => self.pending.len() + 2 > MAX_PENDING_DATAGRAMS,
            ProxyAction::Drop => false,
            ProxyAction::Reorder { count } => {
                count.get() > MAX_PENDING_DATAGRAMS
                    || self.pending.len().saturating_add(count.get()) > MAX_PENDING_DATAGRAMS
            }
        }
    }

    fn start_reorder(&mut self, datagram: Vec<u8>, expected: usize) -> Result<(), ProxyError> {
        if expected == 1 {
            self.schedule_batch([datagram], Duration::ZERO)?;
            return Ok(());
        }
        self.reorder = Some(ReorderBuffer {
            expected,
            datagrams: vec![datagram],
        });
        Ok(())
    }

    fn schedule_batch<const N: usize>(
        &mut self,
        datagrams: [Vec<u8>; N],
        delay: Duration,
    ) -> Result<(), ProxyError> {
        if self.pending.len().saturating_add(N) > MAX_PENDING_DATAGRAMS {
            return Err(ProxyError::QueueFull);
        }
        let deadline = Instant::now() + delay;
        for datagram in datagrams {
            self.pending.push(PendingDatagram {
                deadline,
                order: self.next_order,
                datagram,
            });
            self.next_order = self.next_order.wrapping_add(1);
        }
        self.stats
            .pending_high_watermark
            .fetch_max(self.pending.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn schedule_vec(&mut self, datagrams: Vec<Vec<u8>>, delay: Duration) -> Result<(), ProxyError> {
        if self.pending.len().saturating_add(datagrams.len()) > MAX_PENDING_DATAGRAMS {
            return Err(ProxyError::QueueFull);
        }
        let deadline = Instant::now() + delay;
        for datagram in datagrams {
            self.pending.push(PendingDatagram {
                deadline,
                order: self.next_order,
                datagram,
            });
            self.next_order = self.next_order.wrapping_add(1);
        }
        self.stats
            .pending_high_watermark
            .fetch_max(self.pending.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn send_one_due(&mut self, outbound: &ForwardSender) -> Result<bool, ProxyError> {
        let now = Instant::now();
        let Some(index) = self
            .pending
            .iter()
            .enumerate()
            .min_by_key(|(_, item)| (item.deadline, item.order))
            .and_then(|(index, item)| (item.deadline <= now).then_some(index))
        else {
            return Ok(false);
        };
        let item = self.pending.remove(index);
        let forward = ForwardDatagram {
            order: item.order,
            datagram: item.datagram,
        };
        match outbound.try_send(forward) {
            Ok(()) => Ok(true),
            Err(TrySendError::Full(forward) | TrySendError::Closed(forward)) => {
                self.pending.insert(
                    index,
                    PendingDatagram {
                        deadline: item.deadline,
                        order: forward.order,
                        datagram: forward.datagram,
                    },
                );
                self.stats
                    .outbound_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                Err(ProxyError::QueueFull)
            }
        }
    }
}

enum CoordinatorEvent {
    Datagram(Option<ReceivedDatagram>),
    Completion(Option<SendCompleted>),
    Due,
}

async fn report_event(events: &DriverEventSender, event: DriverEvent) -> Result<(), ProxyError> {
    events.send(event).await.map_err(|_| ProxyError::Shutdown)
}

async fn send_datagram<'rt>(
    ctx: Ctx<'rt>,
    socket: &UdpSocket<'rt>,
    max_datagram_size: NonZeroUsize,
    target: SocketAddr,
    datagram: Vec<u8>,
) -> Result<(), ProxyError> {
    let length = datagram.len();
    let mut buffer = ctx
        .try_alloc(max_datagram_size, length)
        .map_err(|_| ProxyError::Io)?;
    buffer.spare_capacity_mut()[..length].copy_from_slice(&datagram);
    buffer.set_len(length);
    socket
        .send_to(buffer, target)
        .await
        .map_err(|_| ProxyError::Io)?;
    Ok(())
}

fn prepare_recv<'rt>(
    ctx: Ctx<'rt>,
    socket: &UdpSocket<'rt>,
    max_datagram_size: NonZeroUsize,
) -> Result<PreparedUdpRecv<'rt>, ProxyError> {
    let buffer = ctx
        .try_alloc_full(max_datagram_size)
        .map_err(|_| ProxyError::Io)?;
    Ok(socket.prepare_recv_from(buffer))
}

fn trace_datagram(
    direction: ProxyDirection,
    source: SocketAddr,
    datagram: &[u8],
    operation_id: u64,
    event_id: u64,
) {
    if let Ok(packet) = PacketRef::decode(datagram) {
        trace!(
            target: "veloq_reliable_udp::socket_proxy",
            direction = ?direction,
            source = ?source,
            operation_id,
            event_id,
            connection_id = packet.connection_id.get(),
            flags = packet.flags.bits(),
            sequence = packet.sequence,
            ack_largest = packet.ack_largest,
            payload_len = packet.payload.len(),
            "proxy received datagram"
        );
    } else {
        trace!(
            target: "veloq_reliable_udp::socket_proxy",
            direction = ?direction,
            source = ?source,
            operation_id,
            event_id,
            datagram_len = datagram.len(),
            "proxy received undecodable datagram"
        );
    }
}

impl PacketMatcher {
    fn matches(self, packet: Option<&PacketRef<'_>>) -> bool {
        let Some(packet) = packet else {
            return matches!(self, Self::Any);
        };
        match self {
            Self::Any => true,
            Self::Syn => packet.flags == Flags::SYN,
            Self::SynAck => packet.flags == Flags::SYN_ACK,
            Self::AckOnly => packet.flags == Flags::ACK,
            Self::Data { sequence } => {
                packet.flags.contains(Flags::DATA)
                    && sequence.is_none_or(|sequence| packet.sequence == sequence.get())
            }
            Self::Fin => packet.flags.contains(Flags::FIN),
        }
    }
}
