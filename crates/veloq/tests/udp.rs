use std::{
    collections::HashSet,
    net::SocketAddr,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::pin,
    str,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use veloq::{
    io::{AsyncBufRead, AsyncBufWrite},
    net::UdpSocket,
    nz,
    runtime::{Runtime, context::Ctx, scope},
    sync::mpsc,
};
use veloq_buf::{FixedBuf, UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_runtime::{select, task::yield_now};

fn run_test<F, R>(f: F) -> R
where
    F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
{
    run_test_with_workers(nz!(1), f)
}

fn run_test_with_workers<F, R>(worker_threads: NonZeroUsize, f: F) -> R
where
    F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(worker_threads))
        .scope(f)
        .expect("failed to run scope")
}

fn bind_udp_socket<'rt>(ctx: Ctx<'rt>, bind_addr: &str) -> UdpSocket<'rt> {
    UdpSocket::bind(ctx, bind_addr).expect("Failed to bind UDP socket")
}

#[test]
fn udp_bind() {
    run_test(async |ctx| {
        let socket = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind UDP socket");
        let addr = socket.local_addr().expect("Failed to get local address");

        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    });
}

#[test]
fn udp_send_receive() {
    run_test(async |ctx| {
        let socket1 = bind_udp_socket(ctx, "127.0.0.1:0");
        let socket2 = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind socket 2");

        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        let addr2 = socket2.local_addr().expect("Failed to get addr2");
        let state = mpsc::unbounded::<()>();
        let (tx, mut rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let mut recv_prep = socket1.prepare_recv_from(ctx.alloc_full(nz!(1024)));
                recv_prep.arm().await.expect("arm failed");
                tx.send(()).unwrap();

                let datagram = recv_prep.await.expect("recv_from failed");
                assert_eq!(datagram.addr, addr2);
                assert_eq!(
                    &datagram.buf.as_slice()[..b"Hello, UDP!".len()],
                    b"Hello, UDP!"
                );
            });

            s.spawn_boxed(async move {
                rx.recv().await.expect("armed rx closed");
                let data = b"Hello, UDP!";
                let mut send_buf = ctx.alloc(nz!(1024), data.len());
                send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                let (sent, _) = socket2
                    .send_to(send_buf, addr1)
                    .await
                    .expect("send_to failed");
                assert_eq!(sent, data.len());
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn udp_echo() {
    run_test(async |ctx| {
        let server = bind_udp_socket(ctx, "127.0.0.1:0");
        let client = bind_udp_socket(ctx, "127.0.0.1:0");

        let server_addr = server.local_addr().expect("Failed to get server address");
        let (server_tx, mut server_rx) = mpsc::owned_unbounded::<()>();
        let (client_tx, mut client_rx) = mpsc::owned_unbounded::<()>();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let mut server_prep = server.prepare_recv_from(ctx.alloc_full(nz!(1024)));
                server_prep.arm().await.expect("arm failed");
                server_tx.send(()).unwrap();

                let datagram = server_prep.await.expect("Server recv_from failed");
                let from_addr = datagram.addr;
                let bytes = datagram.buf.len();
                let mut echo_buf = ctx.alloc(nz!(1024), bytes);
                echo_buf.spare_capacity_mut()[..bytes]
                    .copy_from_slice(&datagram.buf.as_slice()[..bytes]);
                server
                    .send_to(echo_buf, from_addr)
                    .await
                    .expect("Server send_to failed");
            });

            s.spawn_boxed(async move {
                let recv_client = client.clone();
                scope!(ctx, async |client_scope| {
                    let data = b"Echo this message!";
                    client_scope.spawn_boxed(async move {
                        let mut client_prep =
                            recv_client.prepare_recv_from(ctx.alloc_full(nz!(1024)));
                        client_prep.arm().await.expect("arm failed");
                        client_tx.send(()).unwrap();

                        let datagram = client_prep.await.expect("Client recv_from failed");
                        assert_eq!(datagram.addr, server_addr);
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    client_scope.spawn_boxed(async move {
                        server_rx.recv().await.expect("server rx closed");
                        client_rx.recv().await.expect("client rx closed");

                        let mut send_buf = ctx.alloc(nz!(1024), data.len());
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                        client
                            .send_to(send_buf, server_addr)
                            .await
                            .expect("Client send_to failed");
                    });
                })
                .await
                .unwrap();
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn udp_multiple_messages() {
    run_test(async |ctx| {
        let socket1 = bind_udp_socket(ctx, "127.0.0.1:0");
        let socket2 = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const NUM_MESSAGES: usize = 5;
        let state = mpsc::unbounded::<String>();
        let (msg_tx, mut msg_rx) = state.split();
        let (armed_tx, mut armed_rx) = mpsc::owned_unbounded::<()>();

        scope!(ctx, async |s| {
            for _ in 0..NUM_MESSAGES {
                let recv_socket = socket1.clone();
                let msg_tx = msg_tx.clone();
                let armed_tx = armed_tx.clone();

                s.spawn_boxed(async move {
                    let mut recv_prep = recv_socket.prepare_recv_from(ctx.alloc_full(nz!(1024)));
                    recv_prep.arm().await.expect("arm failed");
                    let _ = armed_tx.send(());

                    let datagram = recv_prep.await.expect("recv_from failed");
                    let msg = str::from_utf8(datagram.buf.as_slice())
                        .expect("udp payload must be utf-8")
                        .to_string();
                    msg_tx.send(msg).expect("message channel closed");
                });
            }

            s.spawn_boxed(async move {
                for _ in 0..NUM_MESSAGES {
                    armed_rx.recv().await.expect("armed channel closed");
                }
                for i in 0..NUM_MESSAGES {
                    let msg = format!("Message {i}");
                    let mut buf = ctx.alloc(nz!(1024), msg.len());
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    socket2.send_to(buf, addr1).await.expect("send_to failed");
                }
            });
        })
        .await
        .unwrap();

        let mut received = Vec::with_capacity(NUM_MESSAGES);
        for _ in 0..NUM_MESSAGES {
            received.push(msg_rx.recv().await.expect("message channel closed"));
        }
        received.sort();
        let mut expected = (0..NUM_MESSAGES)
            .map(|i| format!("Message {i}"))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(received, expected);
    });
}

#[test]
fn udp_large_data() {
    run_test(async |ctx| {
        let socket1 = bind_udp_socket(ctx, "127.0.0.1:0");
        let socket2 = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const DATA_SIZE: usize = 1024;
        let (tx, mut rx) = mpsc::owned_unbounded::<()>();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let mut recv_prep = socket1.prepare_recv_from(ctx.alloc_full(nz!(2048)));
                recv_prep.arm().await.expect("arm failed");
                tx.send(()).unwrap();

                let datagram = recv_prep.await.expect("recv_from failed");
                assert_eq!(datagram.buf.len(), DATA_SIZE);
                for i in 0..DATA_SIZE {
                    assert_eq!(datagram.buf.as_slice()[i], (i % 256) as u8);
                }
            });

            s.spawn_boxed(async move {
                rx.recv().await.expect("armed rx closed");
                let mut buf = ctx.alloc(nz!(2048), DATA_SIZE);
                for i in 0..DATA_SIZE {
                    buf.spare_capacity_mut()[i] = (i % 256) as u8;
                }

                let (bytes, _) = socket2.send_to(buf, addr1).await.expect("send_to failed");
                assert_eq!(bytes, DATA_SIZE);
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn udp_heap_buffer() {
    run_test(async |ctx| {
        let socket1 = bind_udp_socket(ctx, "127.0.0.1:0");
        let socket2 = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        let (tx, mut rx) = mpsc::owned_unbounded::<()>();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let mut recv_prep = socket1.prepare_recv_from(
                    FixedBuf::alloc_heap_full(nz!(1024)).expect("Heap allocation failed"),
                );
                recv_prep.arm().await.expect("arm failed");
                tx.send(()).unwrap();

                let datagram = recv_prep.await.expect("recv_from failed");
                assert_eq!(
                    &datagram.buf.as_slice()[..datagram.buf.len()],
                    b"UDP from heap!"
                );
            });

            s.spawn_boxed(async move {
                rx.recv().await.expect("armed rx closed");
                let data = b"UDP from heap!";
                let mut buf =
                    FixedBuf::alloc_heap(nz!(1024), data.len()).expect("Heap allocation failed");
                buf.as_slice_mut()[..data.len()].copy_from_slice(data);

                socket2.send_to(buf, addr1).await.expect("send_to failed");
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn udp_ipv6() {
    run_test(async |ctx| {
        let socket_result = UdpSocket::bind(ctx, "::1:0");
        if socket_result.is_err() {
            return;
        }

        let socket = socket_result.expect("IPv6 UDP bind unexpectedly failed");
        let addr = socket.local_addr().expect("Failed to get local address");
        assert!(addr.is_ipv6());
    });
}

#[test]
fn udp_cancel_recv_from() {
    run_test(async |ctx| {
        let socket = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind UDP socket");
        let buf = ctx.alloc_full(nz!(1024));

        select! {
            ctx;
            biased;
            _ = socket.recv_from(buf) => {
                panic!("RecvStream should have been cancelled, but it completed (unexpectedly)");
            },
            _ = yield_now() => {
            }
        };
    });
}

#[test]
fn udp_read_exact_write_all() {
    run_test(async |ctx| {
        let socket_server = bind_udp_socket(ctx, "127.0.0.1:0");
        let server_addr = socket_server
            .local_addr()
            .expect("Failed to get server address");
        let socket_client = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind client");
        let (tx, mut rx) = mpsc::owned_unbounded::<()>();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let read_buf = ctx.alloc_full(nz!(16));
                let read_fut = socket_server.read_exact(read_buf);
                let mut read_fut = pin!(read_fut);
                select! {
                    ctx;
                    biased;
                    _ = &mut read_fut => {},
                    _ = yield_now() => {}
                };
                tx.send(()).unwrap();

                let (_, buf) = read_fut.await.expect("Server read_exact failed");
                assert_eq!(buf.as_slice(), b"UDP Exact World!");
            });

            s.spawn_boxed(async move {
                socket_client
                    .connect(server_addr)
                    .await
                    .expect("Client connect failed");

                let mut write_buf = ctx.alloc_full(nz!(16));
                write_buf.as_slice_mut()[..16].copy_from_slice(b"UDP Exact World!");
                rx.recv().await.expect("armed rx closed");

                socket_client
                    .write_all(write_buf)
                    .await
                    .expect("Client write_all failed");
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn multithread_udp_no_echo() {
    run_test_with_workers(nz!(3), async |ctx| {
        const NUM_WORKERS: usize = 3;
        let completed = Arc::new(AtomicUsize::new(0));

        scope!(ctx, async |s| {
            for worker_id in 0..NUM_WORKERS {
                let completed = completed.clone();
                let socket1 = bind_udp_socket(ctx, "127.0.0.1:0");
                let socket2 = UdpSocket::bind(ctx, "127.0.0.1:0").expect("Failed to bind socket 2");
                let addr1 = socket1.local_addr().expect("Failed to get addr1");
                let addr2 = socket2.local_addr().expect("Failed to get addr2");
                let data = format!("Hello from worker {}", worker_id);
                let data_for_recv = data.clone();
                let (ready_tx, mut ready_rx) = mpsc::owned_unbounded::<()>();

                s.spawn_boxed(async move {
                    let mut recv_prep = socket1.prepare_recv_from(ctx.alloc(nz!(1024), 1024));
                    recv_prep.arm().await.expect("arm failed");
                    ready_tx.send(()).unwrap();

                    let datagram = recv_prep.await.expect("recv_from failed");
                    assert_eq!(datagram.addr, addr2);
                    assert_eq!(
                        &datagram.buf.as_slice()[..data_for_recv.len()],
                        data_for_recv.as_bytes()
                    );
                    completed.fetch_add(1, Ordering::SeqCst);
                });

                s.spawn_boxed(async move {
                    ready_rx
                        .recv()
                        .await
                        .expect("receiver readiness channel closed");

                    let mut buf = ctx.alloc(nz!(1024), data.len());
                    buf.spare_capacity_mut()[..data.len()].copy_from_slice(data.as_bytes());

                    let (sent, _) = socket2.send_to(buf, addr1).await.expect("send_to failed");
                    assert_eq!(sent, data.len());
                });
            }
        })
        .await
        .unwrap();

        assert_eq!(completed.load(Ordering::SeqCst), NUM_WORKERS);
    });
}

#[test]
fn multithread_udp_echo() {
    run_test_with_workers(nz!(2), async |ctx| {
        let state = mpsc::unbounded::<SocketAddr>();
        let (addr_tx, mut addr_rx) = state.split();
        let state = mpsc::unbounded::<()>();
        let (done_tx, mut done_rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let socket = bind_udp_socket(ctx, "127.0.0.1:0");
                let server_addr = socket.local_addr().expect("Failed to get server address");
                let mut server_prep = socket.prepare_recv_from(ctx.alloc(nz!(1024), 1024));
                server_prep.arm().await.expect("arm failed");

                addr_tx.send(server_addr).unwrap();
                let datagram = server_prep.await.expect("Server recv_from failed");
                let from_addr = datagram.addr;
                let bytes = datagram.buf.len();
                let mut echo_buf = ctx.alloc(nz!(1024), bytes);
                echo_buf.spare_capacity_mut()[..bytes]
                    .copy_from_slice(&datagram.buf.as_slice()[..bytes]);

                socket
                    .send_to(echo_buf, from_addr)
                    .await
                    .expect("Server send_to failed");

                done_rx.recv().await.expect("Client done channel closed");
            });

            s.spawn_boxed(async move {
                let server_addr = addr_rx.recv().await.expect("Channel closed");
                let client = bind_udp_socket(ctx, "127.0.0.1:0");
                let recv_client = client.clone();
                let (client_tx, mut client_rx) = mpsc::owned_unbounded::<()>();

                scope!(ctx, async |client_scope| {
                    let data = b"Hello from worker 2!";
                    client_scope.spawn_boxed(async move {
                        let mut client_prep =
                            recv_client.prepare_recv_from(ctx.alloc(nz!(1024), 1024));
                        client_prep.arm().await.expect("arm failed");
                        client_tx.send(()).unwrap();

                        let datagram = client_prep.await.expect("Client recv_from failed");
                        assert_eq!(datagram.addr, server_addr);
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    client_scope.spawn_boxed(async move {
                        client_rx
                            .recv()
                            .await
                            .expect("client readiness channel closed");
                        let mut send_buf = ctx.alloc(nz!(1024), data.len());
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                        client
                            .send_to(send_buf, server_addr)
                            .await
                            .expect("Client send_to failed");
                    });
                })
                .await
                .unwrap();

                done_tx.send(()).unwrap();
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn multithread_udp_cross_worker_drop_is_routed() {
    run_test_with_workers(nz!(2), async |ctx| {
        let state = mpsc::unbounded::<UdpSocket<'_>>();
        let (clone_tx, mut clone_rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let socket = bind_udp_socket(ctx, "127.0.0.1:0");
                clone_tx.send(socket.clone()).unwrap();
                // 显式关闭本端的 socket 引用，底层 Token 在跨 Worker 端关闭后将回收解绑
                socket.close().await.expect("socket close failed");

                let probe_server = bind_udp_socket(ctx, "127.0.0.1:0");
                let probe_client =
                    UdpSocket::bind(ctx, "127.0.0.1:0").expect("probe client dummy bind");
                let probe_addr = probe_server
                    .local_addr()
                    .expect("Failed to get probe server address");
                let (probe_ready_tx, mut probe_ready_rx) = mpsc::owned_unbounded::<()>();

                scope!(ctx, async |probe_scope| {
                    let probe_server_task = probe_server.clone();
                    let data = b"probe";
                    probe_scope.spawn_boxed(async move {
                        let mut probe_prep =
                            probe_server_task.prepare_recv_from(ctx.alloc(nz!(1024), 1024));
                        probe_prep.arm().await.expect("arm failed");
                        probe_ready_tx.send(()).unwrap();

                        let datagram = probe_prep.await.expect("probe recv_from failed");
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    probe_scope.spawn_boxed(async move {
                        probe_ready_rx
                            .recv()
                            .await
                            .expect("probe ready channel closed");
                        let mut send_buf = ctx.alloc(nz!(1024), data.len());
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                        probe_client
                            .send_to(send_buf, probe_addr)
                            .await
                            .expect("probe send_to failed");
                    });
                })
                .await
                .unwrap();
            });

            s.spawn_boxed(async move {
                let socket = clone_rx.recv().await.expect("clone channel closed");
                // 在跨 Worker 线程上直接显式 close().await 彻底完成解绑
                socket
                    .close()
                    .await
                    .expect("cross worker socket close failed");
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn multithread_concurrent_udp_clients() {
    run_test_with_workers(nz!(4), async |ctx| {
        const NUM_CLIENTS: usize = 3;
        let completed = Arc::new(AtomicUsize::new(0));
        let mut addr_channels = Vec::with_capacity(NUM_CLIENTS);

        for _ in 0..NUM_CLIENTS {
            addr_channels.push(mpsc::owned_unbounded::<SocketAddr>());
        }

        let server_senders = addr_channels
            .iter()
            .map(|(tx, _)| tx.clone())
            .collect::<Vec<_>>();
        let server = bind_udp_socket(ctx, "127.0.0.1:0");
        let server_addr = server.local_addr().expect("Failed to get server address");
        let state = mpsc::unbounded::<SocketAddr>();
        let (peer_tx, mut peer_rx) = state.split();

        for tx in server_senders {
            tx.send(server_addr).unwrap();
        }

        let mut ready_pairs = Vec::with_capacity(NUM_CLIENTS);
        for _ in 0..NUM_CLIENTS {
            ready_pairs.push(mpsc::owned_unbounded::<()>());
        }

        scope!(ctx, async |s| {
            for (ready_tx, _) in &ready_pairs {
                let recv_socket = server.clone();
                let peer_tx = peer_tx.clone();
                let ready_tx = ready_tx.clone();

                s.spawn_boxed(async move {
                    let mut server_prep = recv_socket.prepare_recv_from(ctx.alloc(nz!(1024), 1024));
                    // 使用真正的异步就绪等待，确保跨 Worker 提交已落地内核
                    server_prep.arm().await.expect("server_prep arm failed");
                    let _ = ready_tx.send(());

                    let datagram = server_prep.await.expect("Server recv_from failed");
                    peer_tx
                        .send(datagram.addr)
                        .expect("peer channel unexpectedly closed");
                });
            }

            for (client_id, ((_tx, mut rx), (_, mut ready_rx))) in
                addr_channels.into_iter().zip(ready_pairs).enumerate()
            {
                s.spawn_boxed(async move {
                    ready_rx.recv().await.expect("server ready channel closed");
                    let server_addr = rx.recv().await.expect("Channel closed");
                    let client = bind_udp_socket(ctx, "127.0.0.1:0");
                    let msg = format!("Hello from client {}", client_id);
                    let mut buf = ctx.alloc(nz!(1024), msg.len());
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());

                    let (sent, _) = client
                        .send_to(buf, server_addr)
                        .await
                        .expect("Client send_to failed");
                    assert_eq!(sent, msg.len());
                });
            }

            // 4. 汇总验证
            let mut unique_peers = HashSet::new();
            for _ in 0..NUM_CLIENTS {
                let peer_addr = peer_rx.recv().await.expect("peer channel closed");
                unique_peers.insert(peer_addr);
                completed.fetch_add(1, Ordering::SeqCst);
            }
            assert_eq!(unique_peers.len(), NUM_CLIENTS);
        })
        .await
        .unwrap();

        assert_eq!(completed.load(Ordering::SeqCst), NUM_CLIENTS);
    });
}
