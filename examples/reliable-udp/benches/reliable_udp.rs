use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use veloq::std::time::Duration;
use veloq_reliable_udp::{Ack, Config, ConnectionId, Flags, Session, SessionEvent};

fn outbound(events: Vec<SessionEvent>) -> Vec<Vec<u8>> {
    events
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::Outbound(datagram) => Some(datagram),
            _ => None,
        })
        .collect()
}

fn establish(client: &mut Session, server: &mut Session) {
    let syn = outbound(client.start(Duration::ZERO).expect("client start"));
    let syn_ack = outbound(server.receive(Duration::ZERO, &syn[0]).expect("server SYN"));
    let ack = outbound(
        client
            .receive(Duration::ZERO, &syn_ack[0])
            .expect("client SYN-ACK"),
    );
    let _ = server.receive(Duration::ZERO, &ack[0]).expect("server ACK");
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
            let first = veloq_reliable_udp::Packet::new(
                Flags::DATA,
                connection_id,
                1,
                Ack::empty(),
                32,
                b"first".to_vec(),
            )
            .encode()
            .expect("first packet");
            let second = veloq_reliable_udp::Packet::new(
                Flags::DATA,
                connection_id,
                2,
                Ack::empty(),
                32,
                b"second".to_vec(),
            )
            .encode()
            .expect("second packet");
            let _ = server.receive(Duration::ZERO, &first).expect("first data");
            let events = server
                .receive(Duration::ZERO, &second)
                .expect("second data");
            black_box(outbound(events).len());
        });
    });
}

criterion_group!(benches, bench_high_concurrency_handshake, bench_batch_ack);
criterion_main!(benches);
