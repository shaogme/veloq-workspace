use std::sync::{Arc, Once};

use tracing::trace;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};
use veloq::{
    buf::{UniformSlot, heap::ThreadMemoryMultiplier},
    net::{UdpReceiveConfig, UdpSocket},
    nz,
    runtime::{Runtime, context::Ctx, scope},
    std::{
        num::NonZeroUsize,
        ops::AsyncFnOnce,
        time::{Duration, Instant},
    },
    time::timeout_at,
};

use veloq_reliable_udp::{
    Ack, Config, ConnectionId, CookieKey, CookieKeyRing, Endpoint, Error, FrameSequence, FrameType,
    MessageId, Packet, PacketRef, StreamDataPacket, StreamId, StreamOpenPayload, StreamSequence,
};

#[path = "socket_proxy.rs"]
mod socket_proxy;

use socket_proxy::{PacketMatcher, ProxyAction, ProxyDirection, SocketProxy};

const ROUND_TRIP_BUDGET: Duration = Duration::from_millis(1_500);

fn cookie_keys() -> CookieKeyRing {
    CookieKeyRing::new(
        CookieKey::new(1, [0x42; veloq_reliable_udp::COOKIE_KEY_LEN]).expect("cookie key"),
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
fn endpoint_round_trip_and_explicit_connection_close() {
    run_test(nz!(1), async |ctx| {
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", Config::default(), cookie_keys())
                .expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", Config::default(), cookie_keys())
                .expect("bind client");
        let server_addr = server.local_addr();
        let started = Instant::now();
        let deadline = started + ROUND_TRIP_BUDGET;

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut phase = "driver startup";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                phase = "connect";
                let client_connection = client.connect(server_addr).await.expect("connect");
                phase = "accept";
                let server_connection = server.accept().await.expect("accept");
                assert_eq!(client_connection.peer_addr(), server_addr);
                assert_eq!(server_connection.peer_addr(), client.local_addr());
                let mut client_stream = client_connection.open_stream().await.expect("open stream");
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");

                phase = "client-to-server send";
                let receipt = client_stream
                    .send_bytes(b"client-to-server")
                    .await
                    .expect("send");
                phase = "client-to-server receive";
                let message = server_stream.recv().await.expect("receive");
                assert_eq!(message.as_slice(), b"client-to-server");
                assert_eq!(receipt.message_id, message.message_id);
                assert!(receipt.rtt.is_some());

                phase = "server-to-client send";
                let payload = b"server-to-client";
                let mut buffer = ctx.alloc(nz!(256), payload.len());
                buffer.as_slice_mut().copy_from_slice(payload);
                let receipt = server_stream.send(buffer).await.expect("send buffer");
                phase = "server-to-client receive";
                let message = client_stream.recv().await.expect("receive buffer");
                assert_eq!(message.as_slice(), payload);
                assert_eq!(receipt.message_id, message.message_id);

                phase = "connection close";
                client_connection.close().await.expect("close connection");
                phase = "endpoint close";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");

                phase = "driver join";
                server_task
                    .take()
                    .expect("server task")
                    .await
                    .expect("server driver")
                    .expect("server run");
                client_task
                    .take()
                    .expect("client task")
                    .await
                    .expect("client driver")
                    .expect("client run");
            })
            .await;

            if completed.is_err() {
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "round-trip exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }
        })
        .await
        .expect("endpoint scope");
    });
}

#[test]
fn endpoint_routes_commands_and_buffers_across_workers() {
    run_test(nz!(2), async |ctx| {
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", Config::default(), cookie_keys())
                .expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", Config::default(), cookie_keys())
                .expect("bind client");
        let server_addr = server.local_addr();

        scope!(ctx, async |scope| {
            let server_task = scope.spawn_boxed(server_driver.run());
            let client_task = scope.spawn_boxed(client_driver.run());
            server_ready.wait().await.expect("server ready");
            client_ready.wait().await.expect("client ready");
            let client_connection = client.connect(server_addr).await.expect("connect");
            let server_connection = server.accept().await.expect("accept");
            let client_stream = client_connection.open_stream().await.expect("open stream");
            let mut server_stream = server_connection
                .accept_stream()
                .await
                .expect("accept stream");

            let receipt = client_stream
                .send_bytes(b"cross-worker")
                .await
                .expect("send");
            let message = server_stream.recv().await.expect("receive");
            assert_eq!(message.as_slice(), b"cross-worker");
            assert_eq!(receipt.message_id, message.message_id);

            drop(client_connection);
            server.close().await.expect("close server endpoint");
            client.close().await.expect("close client endpoint");
            server_task
                .await
                .expect("server driver")
                .expect("server run");
            client_task
                .await
                .expect("client driver")
                .expect("client run");
        })
        .await
        .expect("endpoint scope");
    });
}

#[test]
fn endpoint_rejects_message_larger_than_configured_payload() {
    run_test(nz!(1), async |ctx| {
        let config = Config::builder()
            .max_datagram_size(nz!(128))
            .max_message_size(nz!(22))
            .build()
            .expect("small datagram config");
        let (endpoint, driver, mut endpoint_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind");
        let endpoint_addr = endpoint.local_addr();

        scope!(ctx, async |scope| {
            let driver_task = scope.spawn_boxed(driver.run());
            let (peer, peer_driver, mut peer_ready) = Endpoint::bind(
                ctx,
                "127.0.0.1:0",
                Config::builder()
                    .max_datagram_size(nz!(128))
                    .max_message_size(nz!(22))
                    .build()
                    .expect("small peer config"),
                cookie_keys(),
            )
            .expect("bind peer");
            let peer_task = scope.spawn_boxed(peer_driver.run());
            endpoint_ready.wait().await.expect("endpoint ready");
            peer_ready.wait().await.expect("peer ready");
            let connection = peer.connect(endpoint_addr).await.expect("connect");
            let endpoint_connection = endpoint.accept().await.expect("accept");
            let stream = connection.open_stream().await.expect("open stream");
            let _endpoint_stream = endpoint_connection
                .accept_stream()
                .await
                .expect("accept stream");
            let error = stream
                .send_bytes(&[0; 23])
                .await
                .expect_err("payload must be rejected");
            assert_eq!(error, Error::MessageTooLarge);

            peer.close().await.expect("close peer");
            endpoint.close().await.expect("close endpoint");
            driver_task
                .await
                .expect("endpoint driver")
                .expect("endpoint run");
            peer_task.await.expect("peer driver").expect("peer run");
        })
        .await
        .expect("endpoint scope");
    });
}

#[test]
fn endpoint_proxy_retransmits_dropped_data_once() {
    run_test(nz!(1), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let client_addr = client.local_addr();
        let mut proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        proxy.push_action(
            ProxyDirection::ClientToServer,
            PacketMatcher::Data {
                frame_sequence: Some(veloq_reliable_udp::FrameSequence::new(2).expect("sequence")),
            },
            ProxyAction::Drop,
        );
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, client_addr);

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + ROUND_TRIP_BUDGET;
            let mut phase = "connect";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                let client_connection = client.connect(proxy_addr).await.expect("connect");
                phase = "accept";
                let server_connection = server.accept().await.expect("accept");
                let client_stream = client_connection.open_stream().await.expect("open stream");
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");
                phase = "send";
                let receipt = client_stream
                    .send_bytes(b"dropped-once")
                    .await
                    .expect("send");
                phase = "receive";
                let message = server_stream.recv().await.expect("receive");
                assert_eq!(message.as_slice(), b"dropped-once");
                assert_eq!(receipt.message_id, message.message_id);
                assert!(receipt.retransmissions >= 1);
                assert_eq!(stats.snapshot().dropped, 1);

                phase = "close connection";
                client_connection.close().await.expect("close connection");
                drop(server_connection);
                proxy_handle.take().expect("proxy handle").shutdown();
                phase = "close endpoints";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "proxy retransmission exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
            client_task
                .take()
                .expect("client task")
                .await
                .expect("client task join")
                .expect("client run");
        })
        .await
        .expect("proxy test scope");
    });
}

#[test]
fn endpoint_proxy_deduplicates_duplicated_data() {
    init_diagnostic_logging();
    run_test(nz!(1), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let client_addr = client.local_addr();
        let mut proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        proxy.push_action(
            ProxyDirection::ClientToServer,
            PacketMatcher::Data {
                frame_sequence: Some(veloq_reliable_udp::FrameSequence::new(2).expect("sequence")),
            },
            ProxyAction::Duplicate,
        );
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, client_addr);

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + ROUND_TRIP_BUDGET;
            let mut phase = "connect";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                let client_connection = client.connect(proxy_addr).await.expect("connect");
                phase = "accept";
                let server_connection = server.accept().await.expect("accept");
                let client_stream = client_connection.open_stream().await.expect("open stream");
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");
                phase = "send";
                let receipt = client_stream
                    .send_bytes(b"duplicated-once")
                    .await
                    .expect("send");
                phase = "receive";
                let message = server_stream.recv().await.expect("receive");
                assert_eq!(message.as_slice(), b"duplicated-once");
                assert_eq!(receipt.message_id, message.message_id);
                let second = timeout_at(
                    ctx,
                    Instant::now() + Duration::from_millis(100),
                    server_stream.recv(),
                )
                .await;
                assert!(second.is_err(), "duplicate became application-visible");
                let snapshot = stats.snapshot();
                assert_eq!(snapshot.duplicated, 1);
                assert_eq!(snapshot.duplicate_outputs, 2);
                assert_eq!(snapshot.receive_failed, 0);
                assert_eq!(snapshot.inbound_queue_full, 0);
                assert_eq!(snapshot.outbound_queue_full, 0);
                assert_eq!(snapshot.receive_cancelled, 0);

                phase = "close connection";
                client_connection.close().await.expect("close connection");
                drop(server_connection);
                proxy_handle.take().expect("proxy handle").shutdown();
                phase = "close endpoints";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "proxy duplicate exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
            client_task
                .take()
                .expect("client task")
                .await
                .expect("client task join")
                .expect("client run");
        })
        .await
        .expect("proxy duplicate scope");
    });
}

#[test]
fn endpoint_proxy_delays_data_without_duplicate_delivery() {
    run_test(nz!(1), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let client_addr = client.local_addr();
        let mut proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        proxy.push_action(
            ProxyDirection::ClientToServer,
            PacketMatcher::Data {
                frame_sequence: Some(veloq_reliable_udp::FrameSequence::new(2).expect("sequence")),
            },
            ProxyAction::Delay(Duration::from_millis(20)),
        );
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, client_addr);

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + ROUND_TRIP_BUDGET;
            let mut phase = "connect";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                let client_connection = client.connect(proxy_addr).await.expect("connect");
                phase = "accept";
                let server_connection = server.accept().await.expect("accept");
                let client_stream = client_connection.open_stream().await.expect("open stream");
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");
                phase = "send";
                let receipt = client_stream
                    .send_bytes(b"delayed-once")
                    .await
                    .expect("send");
                phase = "receive";
                let message = server_stream.recv().await.expect("receive");
                assert_eq!(message.as_slice(), b"delayed-once");
                assert_eq!(receipt.message_id, message.message_id);
                assert_eq!(receipt.retransmissions, 0);
                assert_eq!(stats.snapshot().delayed, 1);

                phase = "close connection";
                client_connection.close().await.expect("close connection");
                drop(server_connection);
                proxy_handle.take().expect("proxy handle").shutdown();
                phase = "close endpoints";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "proxy delay exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
            client_task
                .take()
                .expect("client task")
                .await
                .expect("client task join")
                .expect("client run");
        })
        .await
        .expect("proxy delay scope");
    });
}

#[test]
fn endpoint_proxy_reorders_data_and_delivers_in_order() {
    init_diagnostic_logging();
    run_test(nz!(1), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let client_addr = client.local_addr();
        let mut proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        proxy.push_action(
            ProxyDirection::ClientToServer,
            PacketMatcher::Data {
                frame_sequence: None,
            },
            ProxyAction::Reorder { count: nz!(2) },
        );
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, client_addr);

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + ROUND_TRIP_BUDGET;
            let mut phase = "connect";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                let client_connection =
                    Arc::new(client.connect(proxy_addr).await.expect("connect"));
                phase = "accept";
                let server_connection = server.accept().await.expect("accept");
                let client_stream = client_connection.open_stream().await.expect("open stream");
                let (client_sender, _client_receiver) = client_stream.split();
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");
                phase = "send two frames";
                let first_connection = client_sender.clone();
                let second_connection = client_sender;
                let first_send =
                    scope.spawn_boxed(async move { first_connection.send_bytes(b"first").await });
                let second_send =
                    scope.spawn_boxed(async move { second_connection.send_bytes(b"second").await });
                let first_receipt = first_send
                    .await
                    .expect("first send task")
                    .expect("first send");
                let second_receipt = second_send
                    .await
                    .expect("second send task")
                    .expect("second send");
                phase = "receive two frames";
                let first = server_stream.recv().await.expect("first receive");
                let second = server_stream.recv().await.expect("second receive");
                assert_eq!(first.as_slice(), b"first");
                assert_eq!(second.as_slice(), b"second");
                assert_eq!(first.message_id, first_receipt.message_id);
                assert_eq!(second.message_id, second_receipt.message_id);
                assert_eq!(stats.snapshot().reordered, 1);
                assert_eq!(first_receipt.retransmissions, 0);
                assert_eq!(second_receipt.retransmissions, 0);

                phase = "close connection";
                let client_connection = match Arc::try_unwrap(client_connection) {
                    Ok(connection) => connection,
                    Err(_) => panic!("send task retained client connection"),
                };
                client_connection.close().await.expect("close connection");
                drop(server_connection);
                proxy_handle.take().expect("proxy handle").shutdown();
                phase = "close endpoints";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "proxy reorder exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
            client_task
                .take()
                .expect("client task")
                .await
                .expect("client task join")
                .expect("client run");
        })
        .await
        .expect("proxy reorder scope");
    });
}

#[test]
fn endpoint_proxy_timer_isolation_between_connections() {
    init_diagnostic_logging();
    run_test(nz!(2), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config.clone(), cookie_keys()).expect("bind server");
        let (client, client_driver, mut client_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind client");
        let server_addr = server.local_addr();
        let client_addr = client.local_addr();
        let mut proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        proxy.push_action(
            ProxyDirection::ClientToServer,
            PacketMatcher::Data {
                frame_sequence: Some(veloq_reliable_udp::FrameSequence::new(2).expect("sequence")),
            },
            ProxyAction::Drop,
        );
        proxy.push_action(
            ProxyDirection::ServerToClient,
            PacketMatcher::Frame {
                frame_type: FrameType::FinAck,
                frame_sequence: None,
            },
            ProxyAction::Drop,
        );
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, client_addr);

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut client_task = Some(scope.spawn_boxed(client_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + ROUND_TRIP_BUDGET;
            let mut phase = "connect first";
            trace!(
                target: "veloq_reliable_udp::endpoint_test",
                budget = ?ROUND_TRIP_BUDGET,
                "starting proxy timer isolation test"
            );

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                client_ready.wait().await.expect("client ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    elapsed = ?Instant::now().saturating_duration_since(started),
                    "endpoint and proxy drivers are ready"
                );
                let first_client =
                    Arc::new(client.connect(proxy_addr).await.expect("first connect"));
                let first_server = server.accept().await.expect("first accept");
                let first_stream = first_client.open_stream().await.expect("first open stream");
                let (first_sender, _first_receiver) = first_stream.split();
                let mut first_server_stream = first_server
                    .accept_stream()
                    .await
                    .expect("first accept stream");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    elapsed = ?Instant::now().saturating_duration_since(started),
                    "first connection and stream are ready"
                );
                phase = "connect second";
                let second_client =
                    Arc::new(client.connect(proxy_addr).await.expect("second connect"));
                let second_server = server.accept().await.expect("second accept");
                let second_stream = second_client
                    .open_stream()
                    .await
                    .expect("second open stream");
                let (second_sender, _second_receiver) = second_stream.split();
                let mut second_server_stream = second_server
                    .accept_stream()
                    .await
                    .expect("second accept stream");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    elapsed = ?Instant::now().saturating_duration_since(started),
                    "second connection and stream are ready"
                );

                phase = "drop first data";
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    elapsed = ?Instant::now().saturating_duration_since(started),
                    "sending first data packet expected to be dropped"
                );
                let first_send = scope
                    .spawn_boxed(async move { first_sender.send_bytes(b"connection-a").await });
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_action_observed()
                    .await
                    .expect("drop action observed");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    elapsed = ?Instant::now().saturating_duration_since(started),
                    "proxy observed the first data drop"
                );

                phase = "send second data";
                let second_send = scope
                    .spawn_boxed(async move { second_sender.send_bytes(b"connection-b").await });
                let second_receipt = second_send
                    .await
                    .expect("second send task")
                    .expect("second send");
                let second_message = second_server_stream.recv().await.expect("second receive");
                assert_eq!(second_message.as_slice(), b"connection-b");
                assert_eq!(second_receipt.message_id, second_message.message_id);
                assert_eq!(second_receipt.retransmissions, 0);

                phase = "recover first data";
                let first_receipt = first_send
                    .await
                    .expect("first send task")
                    .expect("first send");
                let first_message = first_server_stream.recv().await.expect("first receive");
                assert_eq!(first_message.as_slice(), b"connection-a");
                assert_eq!(first_receipt.message_id, first_message.message_id);
                assert!(first_receipt.retransmissions >= 1);
                assert_eq!(stats.snapshot().dropped, 1);

                phase = "close connections";
                let first_client = match Arc::try_unwrap(first_client) {
                    Ok(connection) => connection,
                    Err(_) => panic!("first send task retained client connection"),
                };
                first_client.close().await.expect("close first connection");
                let second_client = match Arc::try_unwrap(second_client) {
                    Ok(connection) => connection,
                    Err(_) => panic!("second send task retained client connection"),
                };
                second_client
                    .close()
                    .await
                    .expect("close second connection");
                drop(first_server);
                drop(second_server);
                proxy_handle.take().expect("proxy handle").shutdown();
                phase = "close endpoints";
                client.close().await.expect("close client endpoint");
                server.close().await.expect("close server endpoint");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = client_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "proxy timer isolation exceeded {:?} during {phase} after {:?}",
                    ROUND_TRIP_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
            client_task
                .take()
                .expect("client task")
                .await
                .expect("client task join")
                .expect("client run");
        })
        .await
        .expect("proxy timer isolation scope");
    });
}

fn proxy_config() -> Config {
    Config::builder()
        .initial_rto(Duration::from_millis(80))
        .min_rto(Duration::from_millis(40))
        .max_rto(Duration::from_millis(200))
        .handshake_initial_rto(Duration::from_millis(40))
        .handshake_max_rto(Duration::from_millis(120))
        .handshake_deadline(Duration::from_millis(500))
        .close_timeout(Duration::from_millis(120))
        .build()
        .expect("proxy test configuration")
}

#[test]
fn endpoint_proxy_delivers_single_data_packet_within_15s() {
    const SINGLE_PACKET_BUDGET: Duration = ROUND_TRIP_BUDGET;

    init_diagnostic_logging();
    run_test(nz!(1), async |ctx| {
        let config = proxy_config();
        let (server, server_driver, mut server_ready) =
            Endpoint::bind(ctx, "127.0.0.1:0", config, cookie_keys()).expect("bind server");
        let server_addr = server.local_addr();
        let raw_client = UdpSocket::bind(ctx, "127.0.0.1:0").expect("bind raw client");
        let raw_client_addr = raw_client.local_addr().expect("raw client address");
        let proxy = SocketProxy::bind(ctx, nz!(1_200)).expect("bind proxy");
        let proxy_addr = proxy.client_side_addr().expect("client proxy address");
        let stats = proxy.stats();
        let (proxy_driver, proxy_handle) = proxy.start(server_addr, raw_client_addr);
        let connection_id = ConnectionId::new(1).expect("connection ID");

        scope!(ctx, async |scope| {
            let mut server_task = Some(scope.spawn_boxed(server_driver.run()));
            let mut proxy_task = Some(scope.spawn_boxed(proxy_driver.run()));
            let mut proxy_handle = Some(proxy_handle);
            let started = Instant::now();
            let deadline = started + SINGLE_PACKET_BUDGET;
            let mut phase = "arm handshake receive";

            let completed = timeout_at(ctx, deadline, async {
                server_ready.wait().await.expect("server ready");
                proxy_handle
                    .as_mut()
                    .expect("proxy handle")
                    .wait_ready()
                    .await
                    .expect("proxy ready");
                let mut receive = raw_client
                    .receiver(UdpReceiveConfig {
                        kernel_capacity: nz!(1),
                        queue_capacity: nz!(2),
                        datagram_capacity: nz!(1_200),
                        close_timeout: Duration::from_secs(1),
                    })
                    .expect("create handshake receiver");
                receive.ready().await.expect("arm handshake receive");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    armed = true,
                    "raw client handshake receive armed"
                );
                let syn = Packet::encode_frame(
                    &ctx,
                    nz!(1_200),
                    FrameType::Syn,
                    false,
                    connection_id,
                    None,
                    None,
                    Ack::empty(),
                    32,
                    &[],
                )
                .expect("encode SYN")
                .into_fixed_buf();
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    bytes = syn.len(),
                    "raw client sending SYN"
                );
                let (sent, _) = raw_client.send_to(syn, proxy_addr).await.expect("send SYN");
                trace!(
                    target: "veloq_reliable_udp::endpoint_test",
                    sent,
                    "raw client completed SYN send"
                );

                phase = "receive SYN-ACK";
                let syn_ack = receive.recv().await.expect("receive SYN-ACK");
                let syn_ack_packet =
                    PacketRef::decode(syn_ack.buf.as_slice()).expect("decode SYN-ACK");
                assert_eq!(syn_ack_packet.frame_type, FrameType::SynAck);
                let cookie = *syn_ack_packet.handshake_cookie().expect("handshake cookie");

                let ack = Packet::encode_frame(
                    &ctx,
                    nz!(1_200),
                    FrameType::HandshakeAck,
                    false,
                    connection_id,
                    None,
                    None,
                    Ack::empty(),
                    32,
                    &cookie,
                )
                .expect("encode cookie proof")
                .into_fixed_buf();
                raw_client.send_to(ack, proxy_addr).await.expect("send ACK");

                phase = "accept raw connection";
                let server_connection = server.accept().await.expect("accept");
                let open_payload = StreamOpenPayload {
                    frame_window: 32,
                    byte_credit: 64 * 1024,
                }
                .encode();
                let open = Packet::encode_frame(
                    &ctx,
                    nz!(1_200),
                    FrameType::StreamOpen,
                    false,
                    connection_id,
                    Some(FrameSequence::new(1).expect("frame sequence")),
                    Some(StreamId::new(1).expect("stream ID")),
                    Ack::empty(),
                    32,
                    &open_payload,
                )
                .expect("encode stream open")
                .into_fixed_buf();
                raw_client
                    .send_to(open, proxy_addr)
                    .await
                    .expect("send stream open");
                let mut server_stream = server_connection
                    .accept_stream()
                    .await
                    .expect("accept stream");
                let data = Packet::encode_stream_data_into_with_limit(
                    &ctx,
                    nz!(1_200),
                    StreamDataPacket {
                        connection_id,
                        frame_sequence: FrameSequence::new(2).expect("frame sequence"),
                        stream_id: StreamId::new(1).expect("stream ID"),
                        stream_sequence: StreamSequence::new(1).expect("stream sequence"),
                        ack: Ack::empty(),
                        receive_window: 32,
                        message_id: MessageId::new(1).expect("message ID"),
                        fragment_index: 0,
                        fragment_count: 1,
                        message_len: 11,
                        payload: b"single-shot",
                        max_fragment_payload: 1_134,
                    },
                )
                .expect("encode single DATA")
                .into_fixed_buf();
                raw_client
                    .send_to(data, proxy_addr)
                    .await
                    .expect("send single DATA");

                phase = "receive single DATA";
                let message = server_stream.recv().await.expect("receive single DATA");
                assert_eq!(message.as_slice(), b"single-shot");
                assert_eq!(message.message_id.get(), 1);
                let snapshot = stats.snapshot();
                assert!(snapshot.actions_seen >= 3);
                assert_eq!(snapshot.receive_failed, 0);
                assert_eq!(snapshot.inbound_queue_full, 0);
                assert_eq!(snapshot.outbound_queue_full, 0);

                drop(server_connection);
                proxy_handle.take().expect("proxy handle").shutdown();
                server.close().await.expect("close server endpoint");
                raw_client.close().await.expect("close raw client");
            })
            .await;

            if completed.is_err() {
                if let Some(mut handle) = proxy_handle.take() {
                    handle.shutdown();
                }
                if let Some(mut task) = proxy_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                if let Some(mut task) = server_task.take() {
                    task.cancel();
                    let _ = task.await;
                }
                panic!(
                    "single DATA packet was not received within {:?} during {phase} after {:?}",
                    SINGLE_PACKET_BUDGET,
                    Instant::now().saturating_duration_since(started)
                );
            }

            proxy_task
                .take()
                .expect("proxy task")
                .await
                .expect("proxy task join")
                .expect("proxy run");
            server_task
                .take()
                .expect("server task")
                .await
                .expect("server task join")
                .expect("server run");
        })
        .await
        .expect("single packet scope");
    });
}

fn init_diagnostic_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = Targets::new()
            .with_target("veloq_reliable_udp::endpoint", tracing::Level::TRACE)
            .with_target("veloq_reliable_udp::endpoint_test", tracing::Level::TRACE)
            .with_target("veloq_reliable_udp::session", tracing::Level::TRACE)
            .with_target("veloq_reliable_udp::socket_proxy", tracing::Level::TRACE);
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_test_writer()
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .try_init();
    });
}
