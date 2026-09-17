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
    Ack, AckObserve, AckWindow, ConnectionId, Flags, HEADER_LEN, MAGIC, MessageSequence, Packet,
    PacketError, PacketErrorKind, PacketRef, VERSION,
};
pub use session::{
    Message as SessionMessage, Role, SendReceipt, SendToken, Session, SessionEvent, SessionState,
    SessionStatsSnapshot,
};
pub use timer::{TimerCommand, TimerKind};
pub use veloq::buf::FixedBuf;

#[cfg(test)]
mod tests {
    use veloq::std::{collections::VecDeque, num::NonZeroUsize, time::Duration, vec::Vec};

    use super::*;

    fn connection_id() -> ConnectionId {
        ConnectionId::new(7).expect("test connection ID")
    }

    fn sequence(value: u64) -> MessageSequence {
        MessageSequence::new(value).expect("test sequence")
    }

    fn outbound(events: Vec<SessionEvent>) -> Vec<Vec<u8>> {
        events
            .into_iter()
            .filter_map(|event| match event {
                SessionEvent::Outbound(datagram) => Some(datagram),
                _ => None,
            })
            .collect()
    }

    fn establish_pair() -> (Session, Session) {
        let config = Config::default();
        let mut client = Session::new_client(connection_id(), config.clone()).expect("client");
        let mut server = Session::new_server(connection_id(), config).expect("server");

        let syn = outbound(client.start(Duration::ZERO).expect("client start"));
        assert_eq!(syn.len(), 1);
        let syn_ack = outbound(server.receive(Duration::ZERO, &syn[0]).expect("server SYN"));
        assert_eq!(syn_ack.len(), 1);
        let ack = outbound(
            client
                .receive(Duration::ZERO, &syn_ack[0])
                .expect("client SYN-ACK"),
        );
        assert_eq!(ack.len(), 1);
        let server_events = server
            .receive(Duration::ZERO, &ack[0])
            .expect("server final ACK");
        assert!(server_events.contains(&SessionEvent::StateChanged(SessionState::Established)));
        assert_eq!(client.state(), SessionState::Established);
        assert_eq!(server.state(), SessionState::Established);
        (client, server)
    }

    #[test]
    fn packet_ref_decodes_without_copying_payload() {
        let packet = Packet::new(
            Flags::DATA,
            connection_id(),
            1,
            Ack::empty(),
            32,
            b"borrowed".to_vec(),
        );
        let datagram = packet.encode().expect("encode");
        let view = PacketRef::decode(&datagram).expect("decode borrowed packet");
        assert_eq!(view.payload, b"borrowed");
        assert_eq!(view.payload.as_ptr(), datagram[HEADER_LEN..].as_ptr());
    }

    #[test]
    fn ack_batch_emits_one_ack_for_two_contiguous_packets() {
        let (mut client, mut server) = establish_pair();
        let first = Packet::new(
            Flags::DATA,
            connection_id(),
            1,
            Ack::empty(),
            32,
            b"first".to_vec(),
        )
        .encode()
        .expect("first packet");
        let second = Packet::new(
            Flags::DATA,
            connection_id(),
            2,
            Ack::empty(),
            32,
            b"second".to_vec(),
        )
        .encode()
        .expect("second packet");
        let first_events = server.receive(Duration::ZERO, &first).expect("first data");
        assert!(first_events.contains(&SessionEvent::MessageAvailable));
        let second_events = server
            .receive(Duration::ZERO, &second)
            .expect("second data");
        assert_eq!(outbound(second_events).len(), 1);
        assert_eq!(server.stats().ack_delayed, 1);
        let _ = client.drain_events();
    }

    #[test]
    fn congestion_window_grows_after_a_clean_ack() {
        let (mut client, mut server) = establish_pair();
        assert_eq!(client.congestion_window(), 2);
        let token = client
            .queue_send(Duration::ZERO, b"cwnd".to_vec())
            .expect("queue message");
        let data = outbound(client.drain_events());
        let server_events = server.receive(Duration::ZERO, &data[0]).expect("data");
        let generation = server_events
            .iter()
            .find_map(|event| match event {
                SessionEvent::ArmTimer(TimerCommand::Arm {
                    kind: TimerKind::AckDelay,
                    generation,
                    ..
                }) => Some(*generation),
                _ => None,
            })
            .expect("delayed ACK timer");
        let ack = outbound(
            server
                .on_timer(Config::default().ack_delay, TimerKind::AckDelay, generation)
                .expect("ACK timer"),
        );
        let client_events = client
            .receive(Duration::from_millis(5), &ack[0])
            .expect("ACK");
        assert!(client_events.iter().any(|event| {
            matches!(event, SessionEvent::SendAcked(receipt) if receipt.token == token)
        }));
        assert_eq!(client.congestion_window(), 3);
        assert_eq!(client.stats().rtt_samples, 1);
    }

    #[test]
    fn packet_round_trip_preserves_wire_fields() {
        let packet = Packet::new(
            Flags::DATA | Flags::ACK,
            connection_id(),
            9,
            Ack::new(Some(sequence(8)), 0b101).expect("valid ACK"),
            31,
            b"payload".to_vec(),
        );
        let datagram = packet.encode().expect("encode");
        assert_eq!(datagram.len(), HEADER_LEN + 7);
        assert_eq!(Packet::decode(&datagram).expect("decode"), packet);
    }

    #[test]
    fn packet_parser_rejects_truncation_and_invalid_lengths() {
        let packet = Packet::new(
            Flags::DATA,
            connection_id(),
            1,
            Ack::empty(),
            32,
            Vec::new(),
        );
        let mut datagram = packet.encode().expect("encode");
        datagram[40] = 1;
        assert_eq!(
            Packet::decode(&datagram).expect_err("length mismatch").kind,
            PacketErrorKind::InvalidPayloadLength
        );
        assert_eq!(
            Packet::decode(&datagram[..HEADER_LEN - 1])
                .expect_err("truncated header")
                .kind,
            PacketErrorKind::Truncated
        );
        datagram = packet.encode().expect("encode");
        datagram[3] = Flags::SYN.bits() | Flags::DATA.bits();
        assert_eq!(
            Packet::decode(&datagram).expect_err("invalid flags").kind,
            PacketErrorKind::InvalidFlags
        );
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
    fn virtual_harness_can_drop_duplicate_delay_and_reorder_datagrams() {
        let mut harness = VirtualDatagramHarness::new();
        harness.push_action(DatagramAction::Drop);
        harness.send(0, 1, b"drop".to_vec());
        assert_eq!(harness.pending_len(), 0);

        harness.push_action(DatagramAction::Duplicate);
        harness.send(0, 1, b"duplicate".to_vec());
        assert_eq!(harness.pending_len(), 2);
        assert_eq!(
            harness.recv().expect("first duplicate").payload,
            b"duplicate"
        );
        assert_eq!(
            harness.recv().expect("second duplicate").payload,
            b"duplicate"
        );

        harness.push_action(DatagramAction::Delay(Duration::from_millis(5)));
        harness.send(0, 1, b"delayed".to_vec());
        assert!(harness.recv().is_none());
        harness.advance(Duration::from_millis(5));
        assert_eq!(harness.recv().expect("delayed packet").payload, b"delayed");

        harness.send(0, 1, b"first".to_vec());
        harness.send(0, 1, b"second".to_vec());
        harness.reorder_pending();
        assert_eq!(harness.recv().expect("reordered packet").payload, b"second");
        assert_eq!(harness.recv().expect("reordered packet").payload, b"first");
    }

    #[test]
    fn dropped_data_is_retransmitted_and_delivered_once_after_ack() {
        let (mut client, mut server) = establish_pair();
        let token = client
            .queue_send(Duration::ZERO, b"reliable".to_vec())
            .expect("queue message");
        let data = outbound(client.drain_events());
        assert_eq!(data.len(), 1);

        let mut link = VecDeque::from(data);
        let dropped = link.pop_front().expect("data packet");
        drop(dropped);
        assert_eq!(server.buffered_messages(), 0);

        let retransmission = outbound(
            client
                .on_timer(
                    Config::default().initial_rto,
                    TimerKind::Retransmit {
                        sequence: sequence(1),
                    },
                    1,
                )
                .expect("retransmit timeout"),
        );
        assert_eq!(retransmission.len(), 1);
        let server_events = server
            .receive(Config::default().initial_rto, &retransmission[0])
            .expect("retransmitted data");
        assert!(server_events.contains(&SessionEvent::MessageAvailable));
        let server_generation = server_events
            .iter()
            .find_map(|event| match event {
                SessionEvent::ArmTimer(TimerCommand::Arm {
                    kind: TimerKind::AckDelay,
                    generation,
                    ..
                }) => Some(*generation),
                _ => None,
            })
            .expect("delayed ACK timer");
        let ack = outbound(
            server
                .on_timer(
                    Config::default().initial_rto + Config::default().wheel.base_tick(),
                    TimerKind::AckDelay,
                    server_generation,
                )
                .expect("delayed ACK"),
        );
        assert_eq!(ack.len(), 1);
        let client_events = client
            .receive(
                Config::default().initial_rto + Config::default().wheel.base_tick(),
                &ack[0],
            )
            .expect("ACK");
        assert!(client_events.iter().any(|event| {
            matches!(event, SessionEvent::SendAcked(receipt) if receipt.token == token
                && receipt.retransmissions == 1
                && receipt.rtt.is_none())
        }));
        assert_eq!(
            server
                .recv(Config::default().initial_rto)
                .expect("message")
                .as_slice(),
            b"reliable"
        );
        assert!(server.recv(Config::default().initial_rto).is_none());
    }

    #[test]
    fn session_ack_at_the_same_logical_time_has_no_retransmission_and_deduplicates_data() {
        let (mut client, mut server) = establish_pair();
        let token = client
            .queue_send(Duration::ZERO, b"stable-duplicate".to_vec())
            .expect("queue message");
        let data = outbound(client.drain_events());
        assert_eq!(data.len(), 1);

        let events = server
            .receive(Duration::ZERO, &data[0])
            .expect("first data");
        assert!(events.contains(&SessionEvent::MessageAvailable));
        let message = server.recv(Duration::ZERO).expect("first message");
        assert_eq!(message.as_slice(), b"stable-duplicate");
        let ack = outbound(server.drain_events());
        assert_eq!(ack.len(), 1);

        let duplicate_events = server
            .receive(Duration::ZERO, &data[0])
            .expect("duplicate data");
        assert!(!duplicate_events.contains(&SessionEvent::MessageAvailable));
        assert!(server.recv(Duration::ZERO).is_none());

        let client_events = client
            .receive(Duration::ZERO, &ack[0])
            .expect("same-time ACK");
        assert!(client_events.iter().any(|event| {
            matches!(
                event,
                SessionEvent::SendAcked(receipt)
                    if receipt.token == token && receipt.retransmissions == 0
            )
        }));
    }

    #[test]
    fn handshake_retries_back_off_and_stop_at_the_protocol_deadline() {
        let config = Config::builder()
            .handshake_initial_rto(Duration::from_millis(50))
            .handshake_max_rto(Duration::from_millis(200))
            .handshake_deadline(Duration::from_millis(600))
            .handshake_max_retries(4)
            .build()
            .expect("handshake configuration");
        let mut client = Session::new_client(connection_id(), config).expect("client");
        let start_events = client.start(Duration::ZERO).expect("start handshake");
        assert!(start_events.iter().any(|event| {
            matches!(
                event,
                SessionEvent::ArmTimer(TimerCommand::Arm {
                    kind: TimerKind::HandshakeRetry,
                    delay,
                    ..
                }) if *delay == Duration::from_millis(50)
            )
        }));
        assert_eq!(
            outbound(
                client
                    .on_timer(Duration::from_millis(50), TimerKind::HandshakeRetry, 1,)
                    .expect("retry")
            )
            .len(),
            1
        );
        assert_eq!(
            outbound(
                client
                    .on_timer(Duration::from_millis(150), TimerKind::HandshakeRetry, 1,)
                    .expect("retry")
            )
            .len(),
            1
        );
        assert_eq!(
            outbound(
                client
                    .on_timer(Duration::from_millis(350), TimerKind::HandshakeRetry, 1,)
                    .expect("retry")
            )
            .len(),
            1
        );
        assert_eq!(
            outbound(
                client
                    .on_timer(Duration::from_millis(550), TimerKind::HandshakeRetry, 1,)
                    .expect("retry")
            )
            .len(),
            1
        );
        let events = client
            .on_timer(Duration::from_millis(600), TimerKind::HandshakeRetry, 1)
            .expect("deadline");
        assert!(events.contains(&SessionEvent::StateChanged(SessionState::Failed)));
        assert!(events.contains(&SessionEvent::Failed(Error::HandshakeTimeout)));
    }

    #[test]
    fn configuration_rejects_handshake_retry_budget_beyond_deadline() {
        let error = Config::builder()
            .handshake_initial_rto(Duration::from_millis(50))
            .handshake_max_rto(Duration::from_millis(200))
            .handshake_deadline(Duration::from_millis(100))
            .handshake_max_retries(2)
            .build()
            .expect_err("retry budget must fit the deadline");
        assert_eq!(error, ConfigError::HandshakeRetryBudgetOverflow);
    }

    #[test]
    fn configuration_rejects_unsafe_windows_and_datagrams() {
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
