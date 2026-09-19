use veloq::{
    buf::{UniformSlot, heap::ThreadMemoryMultiplier},
    nz,
    runtime::{Runtime, context::Ctx, scope},
    std::{
        ops::AsyncFnOnce,
        time::{Duration, Instant},
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

fn run_test<F, R>(f: F) -> R
where
    F: for<'rt> AsyncFnOnce(Ctx<'rt>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .scope(f)
        .expect("runtime scope failed")
}

#[test]
fn independent_streams_have_independent_message_queues() {
    run_test(async |ctx| {
        let config = Config::default();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind");
        let server_addr = server.local_addr();
        let deadline = Instant::now() + Duration::from_secs(5);

        scope!(ctx, async |scope| {
            let server_task = scope.spawn_boxed(server_driver.run());
            let client_task = scope.spawn_boxed(client_driver.run());
            timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                let client_connection = client.connect(server_addr).await.expect("connect");
                let server_connection = server.accept().await.expect("accept");

                let client_first = client_connection.open_stream().await.expect("open first");
                let client_second = client_connection.open_stream().await.expect("open second");
                let mut server_first = server_connection
                    .accept_stream()
                    .await
                    .expect("accept first");
                let mut server_second = server_connection
                    .accept_stream()
                    .await
                    .expect("accept second");

                assert_ne!(client_first.id(), client_second.id());
                assert!(client_first.id().is_client_initiated());
                assert_eq!(client_first.id(), server_first.id());
                assert_eq!(client_second.id(), server_second.id());

                client_first.send_bytes(b"first").await.expect("first send");
                client_second
                    .send_bytes(b"second")
                    .await
                    .expect("second send");

                assert_eq!(
                    server_first.recv().await.expect("first recv").as_slice(),
                    b"first"
                );
                assert_eq!(
                    server_second.recv().await.expect("second recv").as_slice(),
                    b"second"
                );

                client_first.close().await.expect("close first");
                client_second.close().await.expect("close second");
                client_connection.close().await.expect("close connection");
                let _ = server_connection.close().await;
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await
            .expect("stream round trip timed out");
            server_task.await.expect("server task").expect("server run");
            client_task.await.expect("client task").expect("client run");
        })
        .await
        .expect("endpoint scope");
    });
}
