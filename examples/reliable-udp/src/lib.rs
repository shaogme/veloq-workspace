#![no_std]
#![deny(warnings)]

mod config;
mod connection;
mod cookie;
mod endpoint;
mod error;
mod harness;
mod packet;
mod session;
mod timer;

pub use config::{Config, ConfigBuilder, ConfigError};
pub use connection::{Connection, Message, Shutdown};
pub use cookie::{
    COOKIE_KEY_LEN, CookieConfig, CookieError, CookieInput, CookieInvalidReason, CookieKey,
    CookieKeyRing, CookieToken, CookieValidation, issue_cookie, validate_cookie,
};
pub use endpoint::{Endpoint, EndpointDriver, EndpointReady, EndpointStatsSnapshot};
pub use error::{Error, Result};
pub use harness::{DatagramAction, VirtualDatagram, VirtualDatagramHarness};
pub use packet::{
    Ack, AckObserve, AckWindow, COOKIE_LEN, ConnectionId, DataPacket, Flags, FrameSequence,
    HEADER_LEN, HeapPacketBufAllocator, MAGIC, MessageId, Packet, PacketBufAllocator, PacketError,
    PacketErrorKind, PacketMeta, PacketRef, VERSION,
};
pub use session::{
    Message as SessionMessage, Role, SendReceipt, SendToken, Session, SessionEvent, SessionState,
    SessionStatsSnapshot,
};
pub use timer::{TimerCommand, TimerKind};
pub use veloq::buf::FixedBuf;

#[cfg(test)]
mod tests {
    use veloq::std::{num::NonZeroUsize, time::Duration, vec, vec::Vec};

    use super::*;

    static ALLOCATOR: HeapPacketBufAllocator = HeapPacketBufAllocator;

    fn connection_id() -> ConnectionId {
        ConnectionId::new(7).expect("test connection ID")
    }

    fn sequence(value: u64) -> FrameSequence {
        FrameSequence::new(value).expect("test sequence")
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
        let max_datagram_size = NonZeroUsize::new(1_280).expect("datagram size");
        let packet = if flags.contains(Flags::DATA) {
            Packet::encode_data_into_with_limit(
                &ALLOCATOR,
                max_datagram_size,
                DataPacket {
                    flags,
                    connection_id: connection_id(),
                    frame_sequence: FrameSequence::new(sequence).expect("frame sequence"),
                    ack,
                    receive_window: 32,
                    message_id: MessageId::new(sequence).expect("message ID"),
                    fragment_index: 0,
                    fragment_count: 1,
                    message_len: payload.len() as u64,
                    payload,
                    max_fragment_payload: 1_200,
                },
            )
        } else {
            Packet::encode_control_into_with_limit(
                &ALLOCATOR,
                max_datagram_size,
                flags,
                connection_id(),
                ack,
                32,
            )
        };
        packet.expect("packet encode").into_fixed_buf()
    }

    fn establish_pair() -> (Session, Session) {
        let config = Config::default();
        let mut client = Session::new_client(connection_id(), config.clone()).expect("client");
        let server =
            Session::new_server_established(connection_id(), config.clone(), 32).expect("server");

        let syn = outbound(
            client
                .start(Duration::ZERO, &ALLOCATOR)
                .expect("client start"),
        );
        assert_eq!(syn.len(), 1);
        let ring = CookieKeyRing::new(
            CookieKey::new(1, [7; COOKIE_KEY_LEN]).expect("cookie key"),
            None,
        )
        .expect("cookie key ring");
        let input = CookieInput {
            source: "127.0.0.1:1234".parse().expect("source address"),
            connection_id: connection_id(),
            client_receive_window: 32,
        };
        let cookie = issue_cookie(&ring, input, Duration::ZERO);
        let syn_ack = Packet::encode_handshake_cookie_into_with_limit(
            &ALLOCATOR,
            NonZeroUsize::new(1_280).expect("datagram size"),
            Flags::SYN_ACK,
            connection_id(),
            32,
            cookie.as_bytes(),
        )
        .expect("server challenge")
        .into_fixed_buf();
        let proof = outbound(
            client
                .receive(Duration::ZERO, syn_ack, &ALLOCATOR)
                .expect("client challenge"),
        );
        assert_eq!(proof.len(), 1);
        let confirmation = Packet::encode_control_into_with_limit(
            &ALLOCATOR,
            NonZeroUsize::new(1_280).expect("datagram size"),
            Flags::ACK,
            connection_id(),
            Ack::empty(),
            32,
        )
        .expect("server confirmation")
        .into_fixed_buf();
        let client_events = client
            .receive(Duration::ZERO, confirmation, &ALLOCATOR)
            .expect("client confirmation");
        assert!(client_events.contains(&SessionEvent::StateChanged(SessionState::Established)));
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
    fn handshake_cookie_payload_has_dedicated_wire_rules() {
        let cookie = [0x5a; COOKIE_LEN];
        let challenge = Packet::encode_handshake_cookie_into_with_limit(
            &ALLOCATOR,
            NonZeroUsize::new(1_280).expect("datagram size"),
            Flags::SYN_ACK,
            connection_id(),
            32,
            &cookie,
        )
        .expect("encode challenge")
        .into_fixed_buf();
        let decoded = PacketRef::decode(challenge.as_slice()).expect("decode challenge");
        assert_eq!(decoded.handshake_cookie().expect("cookie"), &cookie);

        let mut invalid = duplicate(&challenge);
        invalid.as_slice_mut()[3] = Flags::ACK.bits();
        invalid.as_slice_mut()[22..30].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            PacketRef::decode(invalid.as_slice())
                .expect("wire parser accepts ACK fields")
                .handshake_cookie()
                .expect_err("cookie must not carry ACK fields")
                .kind,
            PacketErrorKind::InvalidCookiePayload
        );
        assert_eq!(
            Packet::encode_control_into_with_limit(
                &ALLOCATOR,
                NonZeroUsize::new(1_280).expect("datagram size"),
                Flags::SYN_ACK,
                connection_id(),
                Ack::empty(),
                32,
            )
            .expect_err("SYN-ACK without a cookie")
            .kind,
            PacketErrorKind::InvalidCookiePayload
        );
    }

    #[test]
    fn version_two_fragment_codec_validates_layout_before_use() {
        let message_id = MessageId::new(9).expect("message ID");
        let frame = FrameSequence::new(3).expect("frame sequence");
        let datagram = Packet::encode_data_into_with_limit(
            &ALLOCATOR,
            NonZeroUsize::new(128).expect("datagram size"),
            DataPacket {
                flags: Flags::DATA,
                connection_id: connection_id(),
                frame_sequence: frame,
                ack: Ack::empty(),
                receive_window: 32,
                message_id,
                fragment_index: 0,
                fragment_count: 3,
                message_len: 41,
                payload: &[1; 20],
                max_fragment_payload: 20,
            },
        )
        .expect("encode fragment")
        .into_fixed_buf();
        let packet = PacketRef::decode_with_constraints(datagram.as_slice(), 20, 64, 3)
            .expect("decode fragment");
        assert_eq!(packet.frame_sequence, 3);
        assert_eq!(packet.message_id, 9);
        assert_eq!(packet.fragment_count, 3);
        assert_eq!(packet.message_len, 41);

        let mut invalid = duplicate(&datagram);
        invalid.as_slice_mut()[54..58].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(
            PacketRef::decode_with_constraints(invalid.as_slice(), 20, 64, 3)
                .expect_err("wrong fragment count")
                .kind,
            PacketErrorKind::InvalidFragment
        );
    }

    #[test]
    fn version_one_is_rejected_without_guessing_header_offsets() {
        let mut datagram = vec![0; HEADER_LEN];
        datagram[..2].copy_from_slice(&MAGIC);
        datagram[2] = 1;
        assert_eq!(
            PacketRef::decode(&datagram).expect_err("version one").kind,
            PacketErrorKind::UnsupportedVersion
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
        assert!(server.stats().ack_delayed <= 1);
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
        assert!(events.iter().all(|event| {
            !matches!(event, SessionEvent::SendAcked(receipt) if receipt.token == token)
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
