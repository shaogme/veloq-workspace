use tracing::{trace, warn};
use veloq::{
    buf::FixedBuf,
    net::{UdpReceiveConfig, UdpSocket},
    nz,
    std::{net::SocketAddr, sync::Arc},
    sync::{
        TrySendError,
        mpmc::{BoundedOwnedReceiver, BoundedOwnedSender, owned_bounded},
    },
};

use crate::{
    Config,
    error::{Error, Result},
    packet::{FrameSequence, PacketRef},
};

use super::{Command, ConnectionKey, stats::EndpointStats};

pub(super) type CommandSender = BoundedOwnedSender<Command>;
pub(super) type CommandReceiver = BoundedOwnedReceiver<Command>;
pub(super) type PumpSender = BoundedOwnedSender<PumpEvent>;
pub(super) type PumpReceiver = BoundedOwnedReceiver<PumpEvent>;
pub(super) type InboundSender = BoundedOwnedSender<InboundDatagram>;
pub(super) type InboundReceiver = BoundedOwnedReceiver<InboundDatagram>;
pub(super) type OutboundSender = BoundedOwnedSender<OutboundDatagram>;
pub(super) type OutboundReceiver = BoundedOwnedReceiver<OutboundDatagram>;

pub(super) fn channel(capacity: usize) -> (CommandSender, CommandReceiver) {
    owned_bounded(capacity)
}

pub(super) struct InboundDatagram {
    peer: SocketAddr,
    datagram: FixedBuf,
}

impl InboundDatagram {
    pub(super) fn new(peer: SocketAddr, datagram: FixedBuf) -> Self {
        Self { peer, datagram }
    }

    pub(super) fn peer(&self) -> SocketAddr {
        self.peer
    }

    pub(super) fn len(&self) -> usize {
        self.datagram.len()
    }

    pub(super) fn bytes(&self) -> &[u8] {
        self.datagram.as_slice()
    }

    pub(super) fn into_datagram(self) -> FixedBuf {
        self.datagram
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendTicket {
    DropAfterSend,
    ReturnToSession(FrameSequence),
}

pub(super) struct OutboundDatagram {
    key: ConnectionKey,
    datagram: FixedBuf,
    ticket: SendTicket,
}

impl OutboundDatagram {
    pub(super) fn new(key: ConnectionKey, datagram: FixedBuf, ticket: SendTicket) -> Self {
        Self {
            key,
            datagram,
            ticket,
        }
    }

    pub(super) fn key(&self) -> ConnectionKey {
        self.key
    }

    pub(super) fn datagram(&self) -> &[u8] {
        self.datagram.as_slice()
    }

    pub(super) fn has_completion(&self) -> bool {
        !matches!(self.ticket, SendTicket::DropAfterSend)
    }

    pub(super) fn into_parts(self) -> (ConnectionKey, FixedBuf, SendTicket) {
        (self.key, self.datagram, self.ticket)
    }
}

pub(super) enum PumpEvent {
    ReceiveReady,
    SendReady,
    ReceiveFailed(Error),
    SendCompleted {
        key: ConnectionKey,
        ticket: SendTicket,
        result: Result<()>,
        datagram: Option<FixedBuf>,
    },
    SendFailed {
        key: ConnectionKey,
        error: Error,
    },
}

pub(super) async fn receive_pump<'rt>(
    socket: UdpSocket<'rt>,
    config: Config,
    inbound: InboundSender,
    pump_events: PumpSender,
    stats: Arc<EndpointStats>,
) -> Result<()> {
    trace!(target: "veloq_reliable_udp::endpoint", "receive pump started");
    let mut recv = match socket.receiver(UdpReceiveConfig {
        kernel_capacity: nz!(1),
        queue_capacity: config.inbound_capacity,
        datagram_capacity: config.max_datagram_size,
        close_timeout: config.close_timeout,
    }) {
        Ok(recv) => recv,
        Err(_) => {
            report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(Error::Io)).await;
            return Ok(());
        }
    };
    if let Err(error) = recv.ready().await {
        warn!(target: "veloq_reliable_udp::endpoint", ?error, "receive receiver failed to become ready");
        report_pump_failure(&pump_events, PumpEvent::ReceiveFailed(Error::Io)).await;
        return Ok(());
    }
    if pump_events.send(PumpEvent::ReceiveReady).await.is_err() {
        return Ok(());
    }

    loop {
        let packet = match recv.recv().await {
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
        trace_received(peer, &datagram);

        let item = InboundDatagram::new(peer, datagram);
        match inbound.try_send(item) {
            Ok(()) => {
                trace!(target: "veloq_reliable_udp::endpoint", peer = ?peer, "receive pump enqueued datagram")
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                stats.record_inbound_drop();
                warn!(
                    target: "veloq_reliable_udp::endpoint",
                    peer = ?peer,
                    "receive pump dropped datagram because inbound queue is full or closed"
                );
            }
        }
    }
}

pub(super) async fn send_pump<'rt>(
    socket: UdpSocket<'rt>,
    outbound: OutboundReceiver,
    pump_events: PumpSender,
) -> Result<()> {
    trace!(target: "veloq_reliable_udp::endpoint", "send pump started");
    if pump_events.send(PumpEvent::SendReady).await.is_err() {
        return Ok(());
    }
    while let Ok(item) = outbound.recv().await {
        trace_submitted(&item);
        let (key, datagram, ticket) = item.into_parts();
        let (result, returned) = match socket.send_to(datagram, key.peer()).await {
            Ok((sent, returned)) if sent == returned.len() => (Ok(()), Some(returned)),
            Ok((_, returned)) => {
                drop(returned);
                (Err(Error::Io), None)
            }
            Err(_) => (Err(Error::Io), None),
        };
        match ticket {
            SendTicket::DropAfterSend => {
                if let Err(error) = result
                    && pump_events
                        .send(PumpEvent::SendFailed { key, error })
                        .await
                        .is_err()
                {
                    return Err(Error::Io);
                }
            }
            SendTicket::ReturnToSession(_) => {
                if pump_events
                    .send(PumpEvent::SendCompleted {
                        key,
                        ticket,
                        result,
                        datagram: returned,
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

fn trace_received(peer: SocketAddr, datagram: &FixedBuf) {
    trace!(
        target: "veloq_reliable_udp::endpoint",
        peer = ?peer,
        datagram_len = datagram.len(),
        "receive pump completed datagram"
    );
}

fn trace_submitted(item: &OutboundDatagram) {
    if let Ok(packet) = PacketRef::decode(item.datagram()) {
        trace!(
            target: "veloq_reliable_udp::endpoint",
            peer = ?item.key().peer(),
            connection_id = packet.connection_id.get(),
            frame_type = ?packet.frame_type,
            has_ack = packet.has_ack,
            frame_sequence = packet.frame_sequence,
            ack_largest = packet.ack_largest,
            payload_len = packet.payload.len(),
            completion = item.has_completion(),
            "send pump submitting datagram"
        );
    }
}
