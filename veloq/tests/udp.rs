use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veloq::net::UdpSocket;
use veloq::runtime::{Runtime, context};
use veloq::sync::mpsc;
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};

fn create_runtime() -> Runtime<UniformSlot> {
    create_runtime_with_workers(1)
}

fn create_runtime_with_workers(worker_threads: usize) -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(NonZeroUsize::new(worker_threads).expect("worker_threads must be > 0"))
        .build()
        .expect("failed to build runtime")
}

fn bind_udp_socket(bind_addr: &str) -> UdpSocket {
    UdpSocket::bind(bind_addr).expect("Failed to bind UDP socket")
}

async fn allow_udp_recv_to_arm() {
    veloq::time::sleep(std::time::Duration::from_millis(5)).await;
}

#[test]
fn udp_bind() {
    let runtime = create_runtime();
    runtime.block_on(async |_| {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
        let addr = socket.local_addr().expect("Failed to get local address");

        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    });
}

#[test]
fn udp_send_receive() {
    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket1 = bind_udp_socket("127.0.0.1:0");
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        let addr2 = socket2.local_addr().expect("Failed to get addr2");

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_from(&ctx, context::alloc(nz!(1024)))
                    .await
                    .expect("recv_from failed");
                assert_eq!(datagram.addr, addr2);
                assert_eq!(
                    &datagram.buf.as_slice()[..b"Hello, UDP!".len()],
                    b"Hello, UDP!"
                );
            });

            s.spawn_boxed(async move {
                let mut send_buf = context::alloc(nz!(1024));
                let data = b"Hello, UDP!";
                send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                send_buf.set_len(data.len());
                allow_udp_recv_to_arm().await;

                let (sent, _) = socket2
                    .send_to(&ctx, send_buf, addr1)
                    .await
                    .expect("send_to failed");
                assert_eq!(sent, data.len());
            });
        })
        .await;
    });
}

#[test]
fn udp_echo() {
    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let server = bind_udp_socket("127.0.0.1:0");
        let client = bind_udp_socket("127.0.0.1:0");

        let server_addr = server.local_addr().expect("Failed to get server address");

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let datagram = server
                    .recv_from(&ctx, context::alloc(nz!(1024)))
                    .await
                    .expect("Server recv_from failed");
                let from_addr = datagram.addr;
                let bytes = datagram.buf.len();
                let mut echo_buf = context::alloc(nz!(1024));
                echo_buf.spare_capacity_mut()[..bytes]
                    .copy_from_slice(&datagram.buf.as_slice()[..bytes]);
                echo_buf.set_len(bytes);
                server
                    .send_to(&ctx, echo_buf, from_addr)
                    .await
                    .expect("Server send_to failed");
            });

            s.spawn_boxed(async move {
                let recv_client = client.clone();
                ctx.scope(async |client_scope| {
                    client_scope.spawn_boxed(async move {
                        let data = b"Echo this message!";
                        let datagram = recv_client
                            .recv_from(&ctx, context::alloc(nz!(1024)))
                            .await
                            .expect("Client recv_from failed");
                        assert_eq!(datagram.addr, server_addr);
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    client_scope.spawn_boxed(async move {
                        let mut send_buf = context::alloc(nz!(1024));
                        let data = b"Echo this message!";
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                        send_buf.set_len(data.len());
                        allow_udp_recv_to_arm().await;
                        client
                            .send_to(&ctx, send_buf, server_addr)
                            .await
                            .expect("Client send_to failed");
                    });
                })
                .await;
            });
        })
        .await;
    });
}

#[test]
fn udp_multiple_messages() {
    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket1 = bind_udp_socket("127.0.0.1:0");
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const NUM_MESSAGES: usize = 5;
        let (msg_tx, mut msg_rx) = mpsc::unbounded::<String>();

        ctx.scope(async |s| {
            for _ in 0..NUM_MESSAGES {
                let recv_socket = socket1.clone();
                let msg_tx = msg_tx.clone();
                s.spawn_boxed(async move {
                    let datagram = recv_socket
                        .recv_from(&ctx, context::alloc(nz!(1024)))
                        .await
                        .expect("recv_from failed");
                    let msg = std::str::from_utf8(datagram.buf.as_slice())
                        .expect("udp payload must be utf-8")
                        .to_string();
                    msg_tx.send(msg).expect("message channel closed");
                });
            }

            s.spawn_boxed(async move {
                allow_udp_recv_to_arm().await;
                for i in 0..NUM_MESSAGES {
                    let mut buf = context::alloc(nz!(1024));
                    let msg = format!("Message {i}");
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    buf.set_len(msg.len());
                    socket2.send_to(&ctx, buf, addr1).await.expect("send_to failed");
                }
            });
        })
        .await;

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
    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket1 = bind_udp_socket("127.0.0.1:0");
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const DATA_SIZE: usize = 1024;

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_from(&ctx, context::alloc(nz!(2048)))
                    .await
                    .expect("recv_from failed");
                assert_eq!(datagram.buf.len(), DATA_SIZE);
                for i in 0..DATA_SIZE {
                    assert_eq!(datagram.buf.as_slice()[i], (i % 256) as u8);
                }
            });

            s.spawn_boxed(async move {
                let mut buf = context::alloc(nz!(2048));
                for i in 0..DATA_SIZE {
                    buf.spare_capacity_mut()[i] = (i % 256) as u8;
                }
                buf.set_len(DATA_SIZE);
                allow_udp_recv_to_arm().await;

                let (bytes, _) = socket2.send_to(&ctx, buf, addr1).await.expect("send_to failed");
                assert_eq!(bytes, DATA_SIZE);
            });
        })
        .await;
    });
}

#[test]
fn udp_heap_buffer() {
    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket1 = bind_udp_socket("127.0.0.1:0");
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_from(
                        &ctx,
                        veloq_buf::FixedBuf::alloc_heap(nz!(1024)).expect("Heap allocation failed"),
                    )
                    .await
                    .expect("recv_from failed");
                assert_eq!(
                    &datagram.buf.as_slice()[..datagram.buf.len()],
                    b"UDP from heap!"
                );
            });

            s.spawn_boxed(async move {
                let mut buf =
                    veloq_buf::FixedBuf::alloc_heap(nz!(1024)).expect("Heap allocation failed");
                let data = b"UDP from heap!";
                buf.as_slice_mut()[..data.len()].copy_from_slice(data);
                buf.set_len(data.len());
                allow_udp_recv_to_arm().await;

                socket2.send_to(&ctx, buf, addr1).await.expect("send_to failed");
            });
        })
        .await;
    });
}

#[test]
fn udp_ipv6() {
    let runtime = create_runtime();
    runtime.block_on(async |_| {
        let socket_result = UdpSocket::bind("::1:0");
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
    use veloq_runtime::task::yield_now;

    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
        let buf = context::alloc(nz!(1024));

        veloq_runtime::select! {
            _ = socket.recv_from(&ctx, buf) => {
                panic!("RecvStream should have been cancelled, but it completed (unexpectedly)");
            },
            _ = yield_now() => {
            }
        };
    });
}

#[test]
fn udp_read_exact_write_all() {

    let runtime = create_runtime();
    runtime.block_on(async |ctx| {
        let socket_server = bind_udp_socket("127.0.0.1:0");
        let server_addr = socket_server
            .local_addr()
            .expect("Failed to get server address");
        let socket_client = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client");

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let mut read_buf = context::alloc(nz!(16));
                read_buf.set_len(16);

                let (_, buf) = socket_server
                    .read_exact(&ctx, read_buf)
                    .await
                    .expect("Server read_exact failed");
                assert_eq!(buf.as_slice(), b"UDP Exact World!");
            });

            s.spawn_boxed(async move {
                socket_client
                    .connect(&ctx, server_addr)
                    .await
                    .expect("Client connect failed");

                let mut write_buf = context::alloc(nz!(16));
                write_buf.as_slice_mut()[..16].copy_from_slice(b"UDP Exact World!");
                write_buf.set_len(16);
                allow_udp_recv_to_arm().await;

                socket_client
                    .write_all(&ctx, write_buf)
                    .await
                    .expect("Client write_all failed");
            });
        })
        .await;
    });
}

#[test]
fn multithread_udp_no_echo() {
    let runtime = create_runtime_with_workers(3);
    runtime.block_on(async |ctx| {
        const NUM_WORKERS: usize = 3;
        let completed = Arc::new(AtomicUsize::new(0));

        ctx.scope(async |s| {
            for worker_id in 0..NUM_WORKERS {
                let completed = completed.clone();
                let socket1 = bind_udp_socket("127.0.0.1:0");
                let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
                let addr1 = socket1.local_addr().expect("Failed to get addr1");
                let addr2 = socket2.local_addr().expect("Failed to get addr2");
                let data = format!("Hello from worker {}", worker_id);
                let data_for_recv = data.clone();
                let (ready_tx, mut ready_rx) = mpsc::unbounded::<()>();

                s.spawn_boxed(async move {
                    ready_tx.send(()).unwrap();
                    let datagram = socket1
                        .recv_from(&ctx, context::alloc(nz!(1024)))
                        .await
                        .expect("recv_from failed");
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
                    allow_udp_recv_to_arm().await;

                    let mut buf = context::alloc(nz!(1024));
                    buf.spare_capacity_mut()[..data.len()].copy_from_slice(data.as_bytes());
                    buf.set_len(data.len());

                    let (sent, _) = socket2.send_to(&ctx, buf, addr1).await.expect("send_to failed");
                    assert_eq!(sent, data.len());
                });
            }
        })
        .await;

        assert_eq!(completed.load(Ordering::SeqCst), NUM_WORKERS);
    });
}

#[test]
fn multithread_udp_echo() {
    let runtime = create_runtime_with_workers(2);
    runtime.block_on(async |ctx| {
        let (addr_tx, mut addr_rx) = mpsc::unbounded::<std::net::SocketAddr>();
        let (done_tx, mut done_rx) = mpsc::unbounded::<()>();

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let socket = bind_udp_socket("127.0.0.1:0");
                let server_addr = socket.local_addr().expect("Failed to get server address");
                addr_tx.send(server_addr).unwrap();
                let datagram = socket
                    .recv_from(&ctx, context::alloc(nz!(1024)))
                    .await
                    .expect("Server recv_from failed");
                let from_addr = datagram.addr;
                let bytes = datagram.buf.len();
                let mut echo_buf = context::alloc(nz!(1024));
                echo_buf.spare_capacity_mut()[..bytes]
                    .copy_from_slice(&datagram.buf.as_slice()[..bytes]);
                echo_buf.set_len(bytes);

                socket
                    .send_to(&ctx, echo_buf, from_addr)
                    .await
                    .expect("Server send_to failed");

                done_rx.recv().await.expect("Client done channel closed");
            });

            s.spawn_boxed(async move {
                let server_addr = addr_rx.recv().await.expect("Channel closed");
                let client = bind_udp_socket("127.0.0.1:0");
                let recv_client = client.clone();
                ctx.scope(async |client_scope| {
                    client_scope.spawn_boxed(async move {
                        let data = b"Hello from worker 2!";
                        let datagram = recv_client
                            .recv_from(&ctx, context::alloc(nz!(1024)))
                            .await
                            .expect("Client recv_from failed");
                        assert_eq!(datagram.addr, server_addr);
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    client_scope.spawn_boxed(async move {
                        let data = b"Hello from worker 2!";
                        let mut send_buf = context::alloc(nz!(1024));
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                        send_buf.set_len(data.len());
                        allow_udp_recv_to_arm().await;
                        client
                            .send_to(&ctx, send_buf, server_addr)
                            .await
                            .expect("Client send_to failed");
                    });
                })
                .await;

                done_tx.send(()).unwrap();
            });
        })
        .await;
    });
}

#[test]
fn multithread_udp_cross_worker_drop_is_routed() {
    use veloq_runtime::task::yield_now;

    let runtime = create_runtime_with_workers(2);
    runtime.block_on(async |ctx| {
        let (clone_tx, mut clone_rx) = mpsc::unbounded::<UdpSocket>();
        let (ready_tx, mut ready_rx) = mpsc::unbounded::<()>();
        let (done_tx, mut done_rx) = mpsc::unbounded::<()>();

        ctx.scope(async |s| {
            s.spawn_boxed(async move {
                let socket = bind_udp_socket("127.0.0.1:0");
                clone_tx.send(socket.clone()).unwrap();
                drop(socket);
                ready_tx.send(()).unwrap();

                done_rx.recv().await.expect("cross-worker drop ack missing");
                yield_now().await;
                yield_now().await;

                let probe_server = bind_udp_socket("127.0.0.1:0");
                let probe_client = UdpSocket::bind("127.0.0.1:0").expect("probe client dummy bind");
                let probe_addr = probe_server
                    .local_addr()
                    .expect("Failed to get probe server address");
                ctx.scope(async |probe_scope| {
                    let probe_server_task = probe_server.clone();
                    probe_scope.spawn_boxed(async move {
                        let data = b"probe";
                        let datagram = probe_server_task
                            .recv_from(&ctx, context::alloc(nz!(1024)))
                            .await
                            .expect("probe recv_from failed");
                        assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
                    });

                    probe_scope.spawn_boxed(async move {
                        let data = b"probe";
                        let mut send_buf = context::alloc(nz!(1024));
                        send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                        send_buf.set_len(data.len());
                        allow_udp_recv_to_arm().await;

                        probe_client
                            .send_to(&ctx, send_buf, probe_addr)
                            .await
                            .expect("probe send_to failed");
                    });
                })
                .await;
            });

            s.spawn_boxed(async move {
                let socket = clone_rx.recv().await.expect("clone channel closed");
                ready_rx.recv().await.expect("ready channel closed");
                drop(socket);
                done_tx.send(()).unwrap();
            });
        })
        .await;
    });
}

#[test]
fn multithread_concurrent_udp_clients() {
    let runtime = create_runtime_with_workers(4);
    runtime.block_on(async |ctx| {
        const NUM_CLIENTS: usize = 3;
        let completed = Arc::new(AtomicUsize::new(0));
        let mut addr_channels = Vec::with_capacity(NUM_CLIENTS);

        for _ in 0..NUM_CLIENTS {
            addr_channels.push(mpsc::unbounded::<std::net::SocketAddr>());
        }

        let server_senders = addr_channels
            .iter()
            .map(|(tx, _)| tx.clone())
            .collect::<Vec<_>>();
        let server = bind_udp_socket("127.0.0.1:0");
        let server_addr = server.local_addr().expect("Failed to get server address");
        let (peer_tx, mut peer_rx) = mpsc::unbounded::<std::net::SocketAddr>();

        for tx in server_senders {
            tx.send(server_addr).unwrap();
        }

        ctx.scope(async |s| {
            for _ in 0..NUM_CLIENTS {
                let recv_socket = server.clone();
                let peer_tx = peer_tx.clone();
                s.spawn_boxed(async move {
                    let datagram = recv_socket
                        .recv_from(&ctx, context::alloc(nz!(1024)))
                        .await
                        .expect("Server recv_from failed");
                    peer_tx
                        .send(datagram.addr)
                        .expect("peer channel unexpectedly closed");
                });
            }

            let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
            for (client_id, (_tx, mut rx)) in addr_channels.into_iter().enumerate() {
                client_handles.push(s.spawn_boxed(async move {
                    let server_addr = rx.recv().await.expect("Channel closed");
                    let client = bind_udp_socket("127.0.0.1:0");
                    let mut buf = context::alloc(nz!(1024));
                    let msg = format!("Hello from client {}", client_id);
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    buf.set_len(msg.len());
                    allow_udp_recv_to_arm().await;

                    let (sent, _) = client
                        .send_to(&ctx, buf, server_addr)
                        .await
                        .expect("Client send_to failed");
                    assert_eq!(sent, msg.len());
                }));
            }

            for handle in client_handles.into_iter() {
                handle.await.expect("client task failed");
            }
        })
        .await;

        let mut peers = std::collections::HashSet::with_capacity(NUM_CLIENTS);
        for _ in 0..NUM_CLIENTS {
            peers.insert(peer_rx.recv().await.expect("peer channel closed"));
        }

        assert_eq!(peers.len(), NUM_CLIENTS);
        completed.fetch_add(1, Ordering::SeqCst);

        assert_eq!(completed.load(Ordering::SeqCst), 1);
    });
}
