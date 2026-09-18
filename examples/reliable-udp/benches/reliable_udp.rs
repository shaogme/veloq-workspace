use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use veloq::std::{time::Duration, vec::Vec};
use veloq_reliable_udp::{
    Ack, Config, ConnectionId, FixedBuf, Flags, HeapPacketBufAllocator, Packet, Session,
    SessionEvent,
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

fn establish(client: &mut Session, server: &mut Session) {
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
    let _ = server
        .receive(Duration::ZERO, ack.remove(0), &ALLOCATOR)
        .expect("server ACK");
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
                let mut server =
                    Session::new_server(connection_id, config.clone()).expect("server");
                establish(&mut client, &mut server);
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
            let mut client = Session::new_client(connection_id, config.clone()).expect("client");
            let mut server = Session::new_server(connection_id, config).expect("server");
            establish(&mut client, &mut server);
            let first = Packet::encode_into(
                &ALLOCATOR,
                Flags::DATA,
                connection_id,
                1,
                Ack::empty(),
                32,
                b"first",
            )
            .expect("first packet")
            .into_fixed_buf();
            let second = Packet::encode_into(
                &ALLOCATOR,
                Flags::DATA,
                connection_id,
                2,
                Ack::empty(),
                32,
                b"second",
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
