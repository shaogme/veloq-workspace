use veloq::{
    buf::{UniformSlot, heap::ThreadMemoryMultiplier},
    nz,
    runtime::{Runtime, context::Ctx, scope},
    std::{
        num::{NonZeroU32, NonZeroUsize},
        ops::AsyncFnOnce,
        time::{Duration, Instant},
        vec::Vec,
    },
    time::timeout_at,
};
use veloq_reliable_udp::{COOKIE_KEY_LEN, Config, CookieKey, CookieKeyRing, Endpoint};

fn cookie_keys() -> CookieKeyRing {
    CookieKeyRing::new(
        CookieKey::new(1, [0x42; COOKIE_KEY_LEN]).expect("cookie key"),
        None,
    )
    .expect("cookie key ring")
}

fn run_test<F, R>(workers: NonZeroUsize, f: F) -> R
where
    F: for<'rt> AsyncFnOnce(Ctx<'rt>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(workers))
        .scope(f)
        .expect("runtime scope failed")
}

#[test]
fn endpoint_round_trips_a_fragmented_message() {
    run_test(nz!(1), async |ctx| {
        let config = Config::builder()
            .max_datagram_size(nz!(128))
            .max_message_size(nz!(4096))
            .max_fragments_per_message(NonZeroU32::new(128).expect("fragment limit"))
            .max_reassembly_bytes(nz!(4096))
            .max_inbound_bytes(nz!(8192))
            .build()
            .expect("fragmentation config");
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let payload: Vec<u8> = (0..2_048).map(|value| (value % 251) as u8).collect();
        let deadline = Instant::now() + Duration::from_secs(5);

        scope!(ctx, async |scope| {
            let server_task = scope.spawn_boxed(server_driver.run());
            let client_task = scope.spawn_boxed(client_driver.run());
            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                let client_connection = client.connect(server_addr).await.expect("connect");
                let mut server_connection = server.accept().await.expect("accept");
                let receipt = client_connection.send_bytes(&payload).await.expect("send");
                assert!(receipt.fragment_count > 1);
                let message = server_connection.recv().await.expect("receive");
                assert_eq!(message.as_slice(), payload.as_slice());
                assert_eq!(message.message_id, receipt.message_id);
                client_connection.close().await.expect("close connection");
                drop(server_connection);
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;
            if completed.is_err() {
                panic!(
                    "fragmented round trip timed out: client={:?}, server={:?}",
                    client.stats(),
                    server.stats()
                );
            }
            server_task.await.expect("server task").expect("server run");
            client_task.await.expect("client task").expect("client run");
        })
        .await
        .expect("endpoint scope");
    });
}
