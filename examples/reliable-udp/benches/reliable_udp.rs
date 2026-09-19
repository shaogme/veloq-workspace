use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use veloq::std::{time::Duration, vec::Vec};
use veloq_reliable_udp::{
    Ack, Config, ConnectionId, CookieInput, CookieKey, CookieKeyRing, FixedBuf, FrameSequence,
    FrameType, HeapPacketBufAllocator, MessageId, Packet, Session, SessionEvent, StreamDataPacket,
    StreamId, StreamSequence, issue_cookie,
};

static ALLOCATOR: HeapPacketBufAllocator = HeapPacketBufAllocator;

fn outbound(events: Vec<SessionEvent>) -> Vec<FixedBuf> {
    events
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::Outbound { datagram, .. } => Some(datagram),
            _ => None,
        })
        .collect()
}

fn establish(client: &mut Session, server: &mut Session) -> StreamId {
    let syn = outbound(
        client
            .start(Duration::ZERO, &ALLOCATOR)
            .expect("client start"),
    );
    let ring = CookieKeyRing::new(
        CookieKey::new(1, [7; veloq_reliable_udp::COOKIE_KEY_LEN]).expect("cookie key"),
        None,
    )
    .expect("cookie key ring");
    let cookie = issue_cookie(
        &ring,
        CookieInput {
            source: "127.0.0.1:1".parse().expect("source address"),
            connection_id: client.connection_id(),
            client_receive_window: 32,
        },
        Duration::ZERO,
    );
    let syn_ack = Packet::encode_frame(
        &ALLOCATOR,
        Config::default().max_datagram_size,
        FrameType::SynAck,
        false,
        client.connection_id(),
        None,
        None,
        Ack::empty(),
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
    assert_eq!(syn.len(), 1);
    assert_eq!(proof.len(), 1);
    let confirmation = Packet::encode_frame(
        &ALLOCATOR,
        Config::default().max_datagram_size,
        FrameType::Ack,
        true,
        client.connection_id(),
        None,
        None,
        Ack::empty(),
        32,
        &[],
    )
    .expect("server confirmation")
    .into_fixed_buf();
    let _ = client
        .receive(Duration::ZERO, confirmation, &ALLOCATOR)
        .expect("client confirmation");
    let stream_id = client
        .open_stream(Duration::ZERO, &ALLOCATOR)
        .expect("open stream");
    let open = outbound(client.take_events());
    assert_eq!(open.len(), 1);
    let server_events = server
        .receive(
            Duration::ZERO,
            open.into_iter().next().expect("stream open"),
            &ALLOCATOR,
        )
        .expect("server stream open");
    assert_eq!(server.accept_stream(), Some(stream_id));
    for datagram in outbound(server_events) {
        client
            .receive(Duration::ZERO, datagram, &ALLOCATOR)
            .expect("client stream open ack");
    }
    stream_id
}

fn bench_high_concurrency_handshake(c: &mut Criterion) {
    c.bench_function("reliable_udp/high_concurrency_handshake", |b| {
        b.iter(|| {
            let config = Config::default();
            let mut pairs = Vec::with_capacity(256);
            for value in 1..=256_u64 {
                let connection_id = ConnectionId::new(value).expect("connection ID");
                let mut client =
                    Session::new_client(connection_id, config.clone()).expect("client");
                let mut server = Session::new_server_established(connection_id, config.clone(), 32)
                    .expect("server");
                let _ = establish(&mut client, &mut server);
                pairs.push((client, server));
            }
            black_box(pairs.len());
        });
    });
}

fn bench_batch_ack(c: &mut Criterion) {
    c.bench_function("reliable_udp/batch_ack", |b| {
        b.iter(|| {
            let connection_id = ConnectionId::new(1).expect("connection ID");
            let config = Config::default();
            let max_datagram_size = config.max_datagram_size;
            let max_fragment_payload = config.max_fragment_payload();
            let mut client = Session::new_client(connection_id, config.clone()).expect("client");
            let mut server =
                Session::new_server_established(connection_id, config, 32).expect("server");
            let stream_id = establish(&mut client, &mut server);
            let first = Packet::encode_stream_data_into_with_limit(
                &ALLOCATOR,
                max_datagram_size,
                StreamDataPacket {
                    connection_id,
                    frame_sequence: FrameSequence::new(2).expect("frame sequence"),
                    stream_id,
                    stream_sequence: StreamSequence::new(1).expect("stream sequence"),
                    ack: Ack::empty(),
                    receive_window: 32,
                    message_id: MessageId::new(1).expect("message ID"),
                    fragment_index: 0,
                    fragment_count: 1,
                    message_len: 5,
                    payload: b"first",
                    max_fragment_payload,
                },
            )
            .expect("first packet")
            .into_fixed_buf();
            let second = Packet::encode_stream_data_into_with_limit(
                &ALLOCATOR,
                max_datagram_size,
                StreamDataPacket {
                    connection_id,
                    frame_sequence: FrameSequence::new(3).expect("frame sequence"),
                    stream_id,
                    stream_sequence: StreamSequence::new(2).expect("stream sequence"),
                    ack: Ack::empty(),
                    receive_window: 32,
                    message_id: MessageId::new(2).expect("message ID"),
                    fragment_index: 0,
                    fragment_count: 1,
                    message_len: 6,
                    payload: b"second",
                    max_fragment_payload,
                },
            )
            .expect("second packet")
            .into_fixed_buf();
            let _ = server
                .receive(Duration::ZERO, first, &ALLOCATOR)
                .expect("first data");
            let events = server
                .receive(Duration::ZERO, second, &ALLOCATOR)
                .expect("second data");
            black_box(outbound(events).len());
        });
    });
}

criterion_group!(benches, bench_high_concurrency_handshake, bench_batch_ack);
criterion_main!(benches);
