#![no_std]
#![deny(warnings)]

mod config;
mod connection;
mod endpoint;
mod error;
mod harness;
mod packet;
mod session;
mod timer;

pub use config::{Config, ConfigBuilder, ConfigError};
pub use connection::{Connection, Message, Shutdown};
pub use endpoint::{Endpoint, EndpointDriver, EndpointReady, EndpointStatsSnapshot};
pub use error::{Error, Result};
pub use harness::{DatagramAction, VirtualDatagram, VirtualDatagramHarness};
pub use packet::{
    Ack, AckObserve, AckWindow, ConnectionId, Flags, HEADER_LEN, HeapPacketBufAllocator, MAGIC,
    MessageSequence, Packet, PacketBufAllocator, PacketError, PacketErrorKind, PacketMeta,
    PacketRef, VERSION,
};
pub use session::{
    Message as SessionMessage, Role, SendReceipt, SendToken, Session, SessionEvent, SessionState,
    SessionStatsSnapshot,
};
pub use timer::{TimerCommand, TimerKind};
pub use veloq::buf::FixedBuf;

#[cfg(test)]
mod tests {
    use veloq::std::{num::NonZeroUsize, time::Duration, vec::Vec};

    use super::*;

    static ALLOCATOR: HeapPacketBufAllocator = HeapPacketBufAllocator;

    fn connection_id() -> ConnectionId {
        ConnectionId::new(7).expect("test connection ID")
    }

    fn sequence(value: u64) -> MessageSequence {
        MessageSequence::new(value).expect("test sequence")
    }

    fn buffer(bytes: &[u8]) -> FixedBuf {
        let capacity = NonZeroUsize::new(bytes.len().max(1)).expect("non-zero capacity");
        let mut buffer = FixedBuf::alloc_heap(capacity, bytes.len()).expect("buffer allocation");
        buffer.as_slice_mut().copy_from_slice(bytes);
        buffer
    }

    fn duplicate(source: &FixedBuf) -> FixedBuf {
        buffer(source.as_slice())
    }

    fn outbound(events: Vec<SessionEvent>) -> Vec<FixedBuf> {
        events
            .into_iter()
            .filter_map(|event| match event {
                SessionEvent::Outbound { datagram, .. } => Some(datagram),
                _ => None,
            })
            .collect()
    }

    fn encode(flags: Flags, sequence: u64, ack: Ack, payload: &[u8]) -> FixedBuf {
        Packet::encode_into(
            &ALLOCATOR,
            flags,
            connection_id(),
            sequence,
            ack,
            32,
            payload,
        )
        .expect("packet encode")
        .into_fixed_buf()
    }

    fn establish_pair() -> (Session, Session) {
        let config = Config::default();
        let mut client = Session::new_client(connection_id(), config.clone()).expect("client");
        let mut server = Session::new_server(connection_id(), config).expect("server");

        let mut syn = outbound(
            client
                .start(Duration::ZERO, &ALLOCATOR)
                .expect("client start"),
        );
        let mut syn_ack = outbound(
            server
                .receive(Duration::ZERO, syn.remove(0), &ALLOCATOR)
                .expect("server SYN"),
        );
        let mut ack = outbound(
            client
                .receive(Duration::ZERO, syn_ack.remove(0), &ALLOCATOR)
                .expect("client SYN-ACK"),
        );
        let server_events = server
            .receive(Duration::ZERO, ack.remove(0), &ALLOCATOR)
            .expect("server final ACK");
        assert!(server_events.contains(&SessionEvent::StateChanged(SessionState::Established,)));
        assert_eq!(client.state(), SessionState::Established);
        assert_eq!(server.state(), SessionState::Established);
        (client, server)
    }

    #[test]
    fn packet_ref_decodes_without_copying_payload() {
        let datagram = encode(Flags::DATA, 1, Ack::empty(), b"borrowed");
        let view = PacketRef::decode(datagram.as_slice()).expect("decode borrowed packet");
        assert_eq!(view.payload, b"borrowed");
        assert_eq!(view.payload.as_ptr(), unsafe {
            datagram.as_ptr().add(HEADER_LEN)
        });
    }

    #[test]
    fn packet_round_trip_preserves_wire_fields() {
        let datagram = encode(
            Flags::DATA | Flags::ACK,
            9,
            Ack::new(Some(sequence(8)), 0b101).expect("valid ACK"),
            b"payload",
        );
        let packet = Packet::from_fixed_buf(datagram).expect("owned packet");
        assert_eq!(packet.as_slice().len(), HEADER_LEN + 7);
        assert_eq!(packet.as_ref().payload, b"payload");
        assert_eq!(packet.meta().payload_range, HEADER_LEN..HEADER_LEN + 7);
    }

    #[test]
    fn packet_parser_rejects_truncation_and_invalid_lengths() {
        let mut datagram = encode(Flags::DATA, 1, Ack::empty(), &[]);
        datagram.as_slice_mut()[40] = 1;
        assert_eq!(
            PacketRef::decode(datagram.as_slice())
                .expect_err("length mismatch")
                .kind,
            PacketErrorKind::InvalidPayloadLength
        );
        assert_eq!(
            PacketRef::decode(&datagram.as_slice()[..HEADER_LEN - 1])
                .expect_err("truncated header")
                .kind,
            PacketErrorKind::Truncated
        );
        let mut datagram = encode(Flags::DATA, 1, Ack::empty(), &[]);
        datagram.as_slice_mut()[3] = Flags::SYN.bits() | Flags::DATA.bits();
        assert_eq!(
            PacketRef::decode(datagram.as_slice())
                .expect_err("invalid flags")
                .kind,
            PacketErrorKind::InvalidFlags
        );
    }

    #[test]
    fn ack_batch_emits_one_ack_for_two_contiguous_packets() {
        let (mut client, mut server) = establish_pair();
        let first_events = server
            .receive(
                Duration::ZERO,
                encode(Flags::DATA, 1, Ack::empty(), b"first"),
                &ALLOCATOR,
            )
            .expect("first data");
        assert!(first_events.contains(&SessionEvent::MessageAvailable));
        let second_events = server
            .receive(
                Duration::ZERO,
                encode(Flags::DATA, 2, Ack::empty(), b"second"),
                &ALLOCATOR,
            )
            .expect("second data");
        assert_eq!(outbound(second_events).len(), 1);
        assert_eq!(server.stats().ack_delayed, 1);
        let _ = client.take_events();
    }

    #[test]
    fn reliable_send_reuses_buffer_after_completion_and_retransmits_once() {
        let (mut client, mut server) = establish_pair();
        let token = client
            .queue_send(Duration::ZERO, buffer(b"reliable"), &ALLOCATOR)
            .expect("queue message");
        let mut data = outbound(client.take_events());
        let send_buffer = data.remove(0);
        let retransmit_source = duplicate(&send_buffer);
        let client_events = client
            .on_send_completed(Duration::ZERO, sequence(1), send_buffer)
            .expect("send completion");
        assert!(client_events.iter().any(|event| {
            matches!(event, SessionEvent::ArmTimer(TimerCommand::Arm {
                kind: TimerKind::Retransmit { sequence: value }, ..
            }) if *value == sequence(1))
        }));
        let server_events = server
            .receive(Duration::ZERO, retransmit_source, &ALLOCATOR)
            .expect("initial data");
        assert!(server_events.contains(&SessionEvent::MessageAvailable));
        let mut retransmit = outbound(
            client
                .on_timer(
                    Config::default().initial_rto,
                    TimerKind::Retransmit {
                        sequence: sequence(1),
                    },
                    1,
                    &ALLOCATOR,
                )
                .expect("retransmit timeout"),
        );
        assert_eq!(retransmit.len(), 1);
        let duplicate_events = server
            .receive(Duration::ZERO, retransmit.remove(0), &ALLOCATOR)
            .expect("duplicate data");
        assert!(!duplicate_events.contains(&SessionEvent::MessageAvailable));
        let message = server.recv(Duration::ZERO, &ALLOCATOR).expect("message");
        assert_eq!(message.as_slice(), b"reliable");
        assert_eq!(token.get(), 1);
    }

    #[test]
    fn ack_before_completion_drops_stale_buffer_without_rearming_timer() {
        let (mut client, mut server) = establish_pair();
        let token = client
            .queue_send(Duration::ZERO, buffer(b"stale"), &ALLOCATOR)
            .expect("queue message");
        let mut data = outbound(client.take_events());
        let stale = data.remove(0);
        let ack = encode(
            Flags::ACK,
            0,
            Ack::new(Some(sequence(1)), 0).expect("ACK"),
            &[],
        );
        let events = client
            .receive(Duration::ZERO, ack, &ALLOCATOR)
            .expect("ACK before completion");
        assert!(events.iter().any(|event| {
            matches!(event, SessionEvent::SendAcked(receipt) if receipt.token == token)
        }));
        let stale_events = client
            .on_send_completed(Duration::ZERO, sequence(1), stale)
            .expect("stale completion");
        assert!(stale_events.iter().all(|event| {
            !matches!(
                event,
                SessionEvent::ArmTimer(TimerCommand::Arm {
                    kind: TimerKind::Retransmit { .. },
                    ..
                })
            )
        }));
        let _ = server.take_events();
    }

    #[test]
    fn ack_window_tracks_duplicates_old_packets_and_wraparound() {
        let mut window = AckWindow::new();
        assert_eq!(window.observe(sequence(1)), AckObserve::NewLargest);
        assert_eq!(window.observe(sequence(3)), AckObserve::NewLargest);
        assert_eq!(window.observe(sequence(2)), AckObserve::NewOutOfOrder);
        assert_eq!(window.observe(sequence(2)), AckObserve::Duplicate);
        assert!(window.contains(sequence(1)));
        assert!(window.contains(sequence(2)));
        assert!(window.contains(sequence(3)));

        let mut wrapped = AckWindow::new();
        assert_eq!(wrapped.observe(sequence(u64::MAX)), AckObserve::NewLargest);
        assert_eq!(wrapped.observe(sequence(1)), AckObserve::NewLargest);
        assert!(wrapped.contains(sequence(u64::MAX)));
        assert_eq!(sequence(u64::MAX).next(), sequence(1));
    }

    #[test]
    fn virtual_harness_uses_owned_buffers_for_duplicate_and_delay() {
        let mut harness = VirtualDatagramHarness::new();
        harness.push_action(DatagramAction::Duplicate);
        harness.send(0, 1, buffer(b"duplicate"));
        assert_eq!(harness.pending_len(), 2);
        assert_eq!(
            harness.recv().expect("first duplicate").payload.as_slice(),
            b"duplicate"
        );
        assert_eq!(
            harness.recv().expect("second duplicate").payload.as_slice(),
            b"duplicate"
        );

        harness.push_action(DatagramAction::Delay(Duration::from_millis(5)));
        harness.send(0, 1, buffer(b"delayed"));
        assert!(harness.recv().is_none());
        harness.advance(Duration::from_millis(5));
        assert_eq!(
            harness.recv().expect("delayed packet").payload.as_slice(),
            b"delayed"
        );
    }

    #[test]
    fn handshake_retries_back_off_and_stop_at_deadline() {
        let config = Config::builder()
            .handshake_initial_rto(Duration::from_millis(50))
            .handshake_max_rto(Duration::from_millis(200))
            .handshake_deadline(Duration::from_millis(600))
            .handshake_max_retries(4)
            .build()
            .expect("handshake configuration");
        let mut client = Session::new_client(connection_id(), config).expect("client");
        assert!(client.start(Duration::ZERO, &ALLOCATOR).is_ok());
        for now in [50, 150, 350, 550] {
            assert_eq!(
                outbound(
                    client
                        .on_timer(
                            Duration::from_millis(now),
                            TimerKind::HandshakeRetry,
                            1,
                            &ALLOCATOR,
                        )
                        .expect("retry")
                )
                .len(),
                1
            );
        }
        let events = client
            .on_timer(
                Duration::from_millis(600),
                TimerKind::HandshakeRetry,
                1,
                &ALLOCATOR,
            )
            .expect("deadline");
        assert!(events.contains(&SessionEvent::StateChanged(SessionState::Failed)));
        assert!(events.contains(&SessionEvent::Failed(Error::HandshakeTimeout)));
    }

    #[test]
    fn configuration_rejects_invalid_limits() {
        let oversized_window = Config::builder()
            .send_window(NonZeroUsize::new(65).expect("non-zero"))
            .build()
            .expect_err("send window limit");
        assert!(matches!(
            oversized_window,
            ConfigError::SendWindowTooLarge { .. }
        ));
        let too_small = Config::builder()
            .max_datagram_size(NonZeroUsize::new(HEADER_LEN).expect("non-zero"))
            .build()
            .expect_err("header-only datagram");
        assert!(matches!(too_small, ConfigError::DatagramTooSmall { .. }));
    }
}
