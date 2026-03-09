//! UDP network tests - single-threaded and multi-threaded.

use crate::net::udp::UdpSocket;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ============ Helper Functions ============
async fn udp_send_to_with_retry(
    ctx: &str,
    socket: &UdpSocket,
    mut build_buf: impl FnMut() -> veloq_buf::FixedBuf,
    addr: SocketAddr,
    attempts: usize,
) {
    let mut last_err = None;
    for _ in 0..attempts.max(1) {
        let buf = build_buf();
        let (result, _) =
            crate::tests::timeout_op(ctx, "send_to_retry", 5, socket.send_to(buf, addr)).await;
        match result {
            Ok(_) => return,
            Err(e) => last_err = Some(e),
        }
    }
    panic!(
        "UDP send_to_with_retry failed: ctx='{}', attempts={}, last_err={:?}",
        ctx, attempts, last_err
    );
}

async fn udp_recv_unique_peers(
    socket: &UdpSocket,
    size: NonZeroUsize,
    expected_peers: usize,
    max_receives: usize,
    timeout_secs_per_recv: u64,
) -> std::collections::HashSet<SocketAddr> {
    let mut seen = std::collections::HashSet::with_capacity(expected_peers);
    for _ in 0..max_receives {
        if seen.len() >= expected_peers {
            break;
        }
        let buf = crate::runtime::context::alloc(size);
        let datagram = crate::tests::timeout_op(
            "server",
            "recv_unique_peer",
            timeout_secs_per_recv,
            socket.recv_stream(buf),
        )
        .await
        .expect("recv_stream failed while collecting unique peers");
        seen.insert(datagram.addr);
    }
    seen
}

// ============ Single-Thread UDP Tests (using Runtime/spawn) ============

/// Test basic UDP socket binding and local_addr
#[test]
fn test_udp_bind() {
    crate::tests::NetworkTestRunner::new("test_udp_bind")
        .worker_threads(1)
        .run(|_| async move {
            let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");

            let addr = socket.local_addr().expect("Failed to get local address");

            assert_eq!(addr.ip().to_string(), "127.0.0.1");
            assert_ne!(addr.port(), 0);

            println!("UDP socket bound to: {}", addr);
        });
}

/// Test UDP send and receive
#[test]
fn test_udp_send_receive() {
    crate::tests::NetworkTestRunner::new("test_udp_send_receive")
        .worker_threads(1)
        .run(|size| async move {
            let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
            let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

            let addr1 = socket1.local_addr().expect("Failed to get addr1");
            let addr2 = socket2.local_addr().expect("Failed to get addr2");
            println!("Socket 1 bound to: {}", addr1);
            println!("Socket 2 bound to: {}", addr2);

            let socket1_arc = Arc::new(socket1);
            let socket2_arc = Arc::new(socket2);
            let socket1_clone = socket1_arc.clone();

            // Receiver task: socket1 waits for data
            let handler = crate::runtime::context::spawn(async move {
                let buf = crate::runtime::context::alloc(size);
                let datagram = crate::tests::timeout_op(
                    "receiver",
                    "recv_stream",
                    5,
                    socket1_clone.recv_stream(buf),
                )
                .await
                .expect("recv_stream failed");
                let bytes_read = datagram.buf.len();
                let from_addr = datagram.addr;
                println!("Socket 1 received {} bytes from {}", bytes_read, from_addr);
                assert_eq!(from_addr, addr2);
            });

            // Sender: socket2 sends data to socket1
            let mut send_buf = crate::runtime::context::alloc(size);
            let test_data = b"Hello, UDP!";
            send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
            send_buf.set_len(test_data.len());

            let (result, _) = crate::tests::timeout_op(
                "sender",
                "send_to",
                5,
                socket2_arc.send_to(send_buf, addr1),
            )
            .await;
            let bytes_sent = result.expect("send_to failed");
            println!("Socket 2 sent {} bytes to {}", bytes_sent, addr1);

            crate::tests::timeout_op("main", "wait_handler", 5, handler).await;
        });
}

/// Test UDP echo (send and receive response)
#[test]
fn test_udp_echo() {
    crate::tests::NetworkTestRunner::new("test_udp_echo")
        .worker_threads(1)
        .run(|size| async move {
            // Create server and client sockets
            let server = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket");
            let client = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket");

            let server_addr = server.local_addr().expect("Failed to get server address");
            let client_addr = client.local_addr().expect("Failed to get client address");
            println!("Server bound to: {}", server_addr);
            println!("Client bound to: {}", client_addr);

            let server_arc = Arc::new(server);
            let client_arc = Arc::new(client);
            let server_clone = server_arc.clone();

            // Server task: receive and echo back
            let server_h = crate::runtime::context::spawn(async move {
                // Receive data
                let buf = crate::runtime::context::alloc(size);
                let datagram = crate::tests::timeout_op(
                    "server",
                    "recv_stream",
                    5,
                    server_clone.recv_stream(buf),
                )
                .await
                .expect("Server recv_stream failed");
                let bytes_read = datagram.buf.len();
                let from_addr = datagram.addr;
                let buf = datagram.buf;
                println!("Server received {} bytes from {}", bytes_read, from_addr);

                // Echo back
                let mut echo_buf = crate::runtime::context::alloc(size);
                echo_buf.spare_capacity_mut()[..bytes_read]
                    .copy_from_slice(&buf.as_slice()[..bytes_read]);
                echo_buf.set_len(bytes_read);

                let (result, _) = crate::tests::timeout_op(
                    "server",
                    "send_to",
                    5,
                    server_clone.send_to(echo_buf, from_addr),
                )
                .await;
                result.expect("Server send_to failed");
                println!("Server echoed data back to {}", from_addr);
            });

            // Client: send data to server
            let mut send_buf = crate::runtime::context::alloc(size);
            let test_data = b"Echo this message!";
            send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
            send_buf.set_len(test_data.len());

            let (result, _) = crate::tests::timeout_op(
                "client",
                "send_to",
                5,
                client_arc.send_to(send_buf, server_addr),
            )
            .await;
            let bytes_sent = result.expect("Client send_to failed");
            println!("Client sent {} bytes", bytes_sent);

            // Receive echo response
            let recv_buf = crate::runtime::context::alloc(size);
            let datagram = crate::tests::timeout_op(
                "client",
                "recv_stream",
                5,
                client_arc.recv_stream(recv_buf),
            )
            .await
            .expect("Client recv_stream failed");
            let bytes_received = datagram.buf.len();
            let from_addr = datagram.addr;
            let recv_buf = datagram.buf;

            println!(
                "Client received {} bytes from {}",
                bytes_received, from_addr
            );

            // Verify
            assert_eq!(from_addr, server_addr);
            assert_eq!(&recv_buf.as_slice()[..test_data.len()], test_data);
            println!("UDP echo test successful!");

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test multiple UDP messages
#[test]
fn test_udp_multiple_messages() {
    crate::tests::NetworkTestRunner::new("test_udp_multiple_messages")
        .worker_threads(1)
        .run(|size| async move {
            let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
            let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

            let addr1 = socket1.local_addr().expect("Failed to get addr1");
            let _addr2 = socket2.local_addr().expect("Failed to get addr2");

            const NUM_MESSAGES: usize = 5;

            let socket1_arc = Arc::new(socket1);
            let socket2_arc = Arc::new(socket2);
            let socket1_clone = socket1_arc.clone();
            let (ready_tx, mut ready_rx) = crate::sync::mpsc::unbounded::<usize>();

            // Receiver task
            let h_recv = crate::runtime::context::spawn(async move {
                for i in 0..NUM_MESSAGES {
                    ready_tx.send(i).unwrap();
                    let buf = crate::runtime::context::alloc(size);
                    let datagram = crate::tests::timeout_op(
                        "receiver",
                        "recv_stream",
                        5,
                        socket1_clone.recv_stream(buf),
                    )
                    .await
                    .expect("recv_stream failed");
                    let bytes = datagram.buf.len();
                    let from = datagram.addr;
                    println!("Received message {} ({} bytes) from {}", i, bytes, from);
                }
                println!("Received all {} messages", NUM_MESSAGES);
            });

            // Sender
            for i in 0..NUM_MESSAGES {
                let ready_idx =
                    crate::tests::timeout_op("sender", "wait_recv_ready", 5, ready_rx.recv())
                        .await
                        .expect("UDP multiple messages receiver closed readiness channel early");
                assert_eq!(
                    ready_idx, i,
                    "UDP multiple messages receiver/sender iteration mismatch"
                );

                let mut buf = crate::runtime::context::alloc(size);
                let msg = format!("Message {}", i);
                buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                buf.set_len(msg.len());

                let (result, _) = crate::tests::timeout_op(
                    "sender",
                    "send_to",
                    5,
                    socket2_arc.send_to(buf, addr1),
                )
                .await;
                result.expect("send_to failed");
                println!("Sent message {}", i);
            }
            println!("Sent all {} messages", NUM_MESSAGES);

            crate::tests::timeout_op("main", "wait_receiver", 5, h_recv).await;
        });
}

/// Test UDP with large data
#[test]
fn test_udp_large_data() {
    crate::tests::NetworkTestRunner::new("test_udp_large_data")
        .worker_threads(1)
        .run(|size| async move {
            let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
            let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

            let addr1 = socket1.local_addr().expect("Failed to get addr1");

            // UDP datagrams are limited, use a reasonable size (less than MTU)
            const DATA_SIZE: usize = 1024;

            let socket1_arc = Arc::new(socket1);
            let socket2_arc = Arc::new(socket2);
            let socket1_clone = socket1_arc.clone();

            // Receiver task
            let h_recv = crate::runtime::context::spawn(async move {
                let buf = crate::runtime::context::alloc(size);
                let datagram = crate::tests::timeout_op(
                    "receiver",
                    "recv_stream",
                    5,
                    socket1_clone.recv_stream(buf),
                )
                .await
                .expect("recv_stream failed");
                let bytes = datagram.buf.len();
                let buf = datagram.buf;
                println!("Received {} bytes", bytes);

                // Verify data pattern
                for i in 0..DATA_SIZE {
                    assert_eq!(buf.as_slice()[i], (i % 256) as u8);
                }
                println!("Data verification successful!");
            });

            // Sender
            let mut buf = crate::runtime::context::alloc(size);
            for i in 0..DATA_SIZE {
                buf.spare_capacity_mut()[i] = (i % 256) as u8;
            }

            buf.set_len(DATA_SIZE);
            let (result, _) =
                crate::tests::timeout_op("sender", "send_to", 5, socket2_arc.send_to(buf, addr1))
                    .await;
            let bytes = result.expect("send_to failed");
            println!("Sent {} bytes", bytes);

            crate::tests::timeout_op("main", "wait_receiver", 5, h_recv).await;
        });
}

/// Test UDP using heap-allocated FixedBuf
#[test]
fn test_udp_heap_buffer() {
    crate::tests::NetworkTestRunner::new("test_udp_heap_buffer")
        .worker_threads(1)
        .run(|_| async move {
            let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
            let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
            let addr1 = socket1.local_addr().expect("Failed to get addr1");

            // Receiver task: Explicitly use heap buffer
            let h_recv = crate::runtime::context::spawn(async move {
                let buf = veloq_buf::FixedBuf::alloc_heap(veloq_buf::nz!(1024))
                    .expect("Heap allocation failed");
                let datagram = crate::tests::timeout_op(
                    "receiver",
                    "recv_stream",
                    5,
                    socket1.recv_stream(buf),
                )
                .await
                .expect("recv_stream failed");
                let n = datagram.buf.len();
                let buf = datagram.buf;

                assert_eq!(&buf.as_slice()[..n], b"UDP from heap!");
                println!("UDP server received data in heap buffer correctly");
            });

            // Sender: Explicitly use heap buffer
            let mut buf = veloq_buf::FixedBuf::alloc_heap(veloq_buf::nz!(1024))
                .expect("Heap allocation failed");
            let data = b"UDP from heap!";
            buf.as_slice_mut()[..data.len()].copy_from_slice(data);
            buf.set_len(data.len());

            let (result, _) =
                crate::tests::timeout_op("sender", "send_to", 5, socket2.send_to(buf, addr1)).await;
            result.expect("send_to failed");
            println!("UDP client sent data from heap buffer correctly");

            crate::tests::timeout_op("main", "wait_receiver", 5, h_recv).await;
        });
}

/// Test IPv6 UDP
#[test]
fn test_udp_ipv6() {
    crate::tests::NetworkTestRunner::new("test_udp_ipv6")
        .worker_threads(1)
        .run(|_| async move {
            let socket_result = UdpSocket::bind("::1:0");

            if socket_result.is_err() {
                println!("IPv6 not available, skipping test");
                return;
            }

            let socket = socket_result.unwrap();
            let addr = socket.local_addr().expect("Failed to get local address");

            assert!(addr.is_ipv6());
            println!("IPv6 UDP socket bound to: {}", addr);

            drop(socket);
        });
}

// ============ Multi-Thread UDP Tests ============

/// Test UDP across multiple worker threads
#[test]
fn test_multithread_udp_no_echo() {
    crate::tests::NetworkTestRunner::new("test_multithread_udp_no_echo")
        .worker_threads(3)
        .run(|size| async move {
            let message_count = Arc::new(AtomicUsize::new(0));
            const NUM_WORKERS: usize = 3;

            let message_count_clone = message_count.clone();
            let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
            for worker_id in 0..NUM_WORKERS {
                let counter = message_count_clone.clone();

                let handle = crate::runtime::context::spawn_to(worker_id, async move || {
                    // Each worker creates its own UDP sockets and tests send/recv
                    let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
                    let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

                    let addr1 = socket1.local_addr().expect("Failed to get addr1");
                    tracing::info!("Worker {} socket 1 bound to: {}", worker_id, addr1);

                    let socket1_arc = Arc::new(socket1);
                    let socket2_arc = Arc::new(socket2);
                    let socket1_clone = socket1_arc.clone();

                    // Receiver task via crate::context::spawn
                    let h_recv = crate::runtime::context::spawn(async move {
                        let buf = crate::runtime::context::alloc(size);
                        let datagram = crate::tests::timeout_op(
                            "receiver",
                            "recv_stream",
                            5,
                            socket1_clone.recv_stream(buf),
                        )
                        .await
                        .unwrap_or_else(|e| {
                            tracing::error!("Worker {} recv_stream failed: {:?}", worker_id, e);
                            panic!("Worker {} recv_stream failed callback: {:?}", worker_id, e);
                        });
                        tracing::info!("Worker {} received message", worker_id);
                        drop(datagram.buf);
                    });

                    // Sender
                    let mut buf = crate::runtime::context::alloc(size);
                    let msg = format!("Hello from worker {}", worker_id);
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    buf.set_len(msg.len());

                    let payload = buf.as_slice().to_vec();
                    udp_send_to_with_retry(
                        &format!("worker_{}", worker_id),
                        &socket2_arc,
                        || {
                            let mut b = crate::runtime::context::alloc(size);
                            b.spare_capacity_mut()[..payload.len()].copy_from_slice(&payload);
                            b.set_len(payload.len());
                            b
                        },
                        addr1,
                        2,
                    )
                    .await;
                    tracing::info!("Worker {} sent message", worker_id);

                    tracing::info!("Worker {} waiting for h_recv", worker_id);
                    crate::tests::timeout_op(
                        &format!("worker_{}", worker_id),
                        "wait_recv",
                        5,
                        h_recv,
                    )
                    .await;
                    tracing::info!("Worker {} h_recv joined", worker_id);
                    counter.fetch_add(1, Ordering::SeqCst);
                    tracing::info!("Worker {} completed", worker_id);
                });
                worker_handles.push((worker_id, handle));
            }

            for (worker_id, handle) in worker_handles {
                crate::tests::timeout_op(
                    &format!("main_wait_worker_{}", worker_id),
                    "wait_worker_join",
                    5,
                    handle,
                )
                .await;
            }

            assert_eq!(message_count.load(Ordering::SeqCst), NUM_WORKERS);
            tracing::info!(
                "All {} workers completed UDP self-communication",
                NUM_WORKERS
            );
        });
}

/// Test UDP echo server on one worker, clients on another
#[test]
fn test_multithread_udp_echo() {
    crate::tests::NetworkTestRunner::new("test_multithread_udp_echo")
        .worker_threads(2) // 2 workers (0 and 1)
        .run(|size| async move {
            let (addr_tx, mut addr_rx) = crate::sync::mpsc::unbounded();

            // Worker 0: Echo server
            let addr_tx_clone = addr_tx.clone();
            let server_h = crate::runtime::context::spawn_to(0, async move || {
                let socket =
                    Arc::new(UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket"));
                let server_addr = socket.local_addr().expect("Failed to get server address");
                println!("UDP echo server listening on {}", server_addr);

                // Pre-post recv before publishing server address to avoid RIO timing window.
                let (ready_tx, mut ready_rx) = crate::sync::mpsc::unbounded::<()>();
                let socket_for_recv = socket.clone();
                let recv_h = crate::runtime::context::spawn(async move {
                    ready_tx.send(()).unwrap();
                    let buf = crate::runtime::context::alloc(size);
                    let datagram = crate::tests::timeout_op(
                        "server",
                        "recv_stream",
                        5,
                        socket_for_recv.recv_stream(buf),
                    )
                    .await
                    .expect("Server recv_stream failed");
                    let bytes = datagram.buf.len();
                    let from_addr = datagram.addr;
                    let buf = datagram.buf;
                    (bytes, from_addr, buf)
                });
                crate::tests::timeout_op("server", "recv_ready", 5, ready_rx.recv())
                    .await
                    .expect("server recv readiness channel closed");

                // Send address to client worker after recv is posted.
                addr_tx_clone.send(server_addr).unwrap();

                let (bytes, from_addr, buf) =
                    crate::tests::timeout_op("server", "wait_recv_join", 5, recv_h).await;
                println!("Server received {} bytes from {}", bytes, from_addr);

                // Echo back
                let mut echo_buf = crate::runtime::context::alloc(size);
                echo_buf.spare_capacity_mut()[..bytes].copy_from_slice(&buf.as_slice()[..bytes]);
                echo_buf.set_len(bytes);

                let (result, _) = crate::tests::timeout_op(
                    "server",
                    "send_to",
                    5,
                    socket.send_to(echo_buf, from_addr),
                )
                .await;
                result.expect("Server send_to failed");
                println!("Server echoed response");
            });

            // Worker 1: Client
            let client_h = crate::runtime::context::spawn_to(1, async move || {
                // Wait for server address
                let server_addr =
                    crate::tests::timeout_op("client", "wait_server_addr", 5, addr_rx.recv())
                        .await
                        .expect("Channel closed");

                println!("Client connecting to {}", server_addr);

                let client =
                    Arc::new(UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket"));

                // Pre-post client recv before sending request to avoid RIO response drop.
                let (client_ready_tx, mut client_ready_rx) = crate::sync::mpsc::unbounded::<()>();
                let client_for_recv = client.clone();
                let recv_h = crate::runtime::context::spawn(async move {
                    client_ready_tx.send(()).unwrap();
                    let recv_buf = crate::runtime::context::alloc(size);
                    let datagram = crate::tests::timeout_op(
                        "client",
                        "recv_stream",
                        5,
                        client_for_recv.recv_stream(recv_buf),
                    )
                    .await
                    .expect("Client recv_stream failed");
                    let from = datagram.addr;
                    let recv_buf = datagram.buf;
                    (from, recv_buf)
                });
                crate::tests::timeout_op("client", "recv_ready", 5, client_ready_rx.recv())
                    .await
                    .expect("client recv readiness channel closed");

                // Send data
                let mut send_buf = crate::runtime::context::alloc(size);
                let data = b"Hello from worker 2!";
                send_buf.as_slice_mut()[..data.len()].copy_from_slice(data);
                send_buf.set_len(data.len());

                let (result, _) = crate::tests::timeout_op(
                    "client",
                    "send_to",
                    5,
                    client.send_to(send_buf, server_addr),
                )
                .await;
                let sent = result.expect("Client send_to failed");
                println!("Client sent {} bytes", sent);

                let (from, recv_buf) =
                    crate::tests::timeout_op("client", "wait_recv_join", 5, recv_h).await;

                assert_eq!(from, server_addr);
                assert_eq!(&recv_buf.as_slice()[..data.len()], data);
                println!("Client received correct echo");
            });

            println!("UDP echo phase: wait_client_join");
            crate::tests::timeout_op("main", "wait_client_join", 5, client_h).await;
            println!("UDP echo phase: wait_server_join");
            crate::tests::timeout_op("main", "wait_server_join", 5, server_h).await;
            println!("UDP echo phase: joins_done");

            println!("UDP echo phase: block_on_done");
            println!("Multi-thread UDP echo test completed");
        });
}

/// Test concurrent UDP clients from multiple workers to shared server
#[test]
fn test_multithread_concurrent_udp_clients() {
    crate::tests::NetworkTestRunner::new("test_multithread_concurrent_udp_clients")
        .worker_threads(4) // 0=Server, 1,2,3=Clients
        .buffer_sizes(vec![veloq_buf::nz!(8192)])
        .run(|size| async move {
            const NUM_CLIENTS: usize = 3;
            let (addr_tx, mut addr_rx) = crate::sync::mpsc::unbounded::<SocketAddr>();
            let message_count = Arc::new(AtomicUsize::new(0));

            // Server worker (0)
            let addr_tx_clone = addr_tx.clone();
            let server_handle = crate::runtime::context::spawn_to(0, async move || {
                let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket");
                let server_addr = socket.local_addr().expect("Failed to get server address");
                println!("Server listening on {}", server_addr);

                // Pre-post recv collection before publishing address to avoid first-packet race.
                let (ready_tx, mut ready_rx) = crate::sync::mpsc::unbounded::<()>();
                let recv_h = crate::runtime::context::spawn(async move {
                    ready_tx.send(()).unwrap();
                    udp_recv_unique_peers(&socket, size, NUM_CLIENTS, NUM_CLIENTS * 3, 5).await
                });
                crate::tests::timeout_op("server", "recv_unique_ready", 5, ready_rx.recv())
                    .await
                    .expect("server unique-recv readiness channel closed");

                // Publish server address once recv path is ready; main task fans out to clients.
                addr_tx_clone.send(server_addr).unwrap();

                let seen =
                    crate::tests::timeout_op("server", "wait_unique_recv_join", 10, recv_h).await;
                assert_eq!(
                    seen.len(),
                    NUM_CLIENTS,
                    "server did not receive all client datagrams"
                );
                println!("Server received all {} messages", NUM_CLIENTS);
            });

            let server_addr =
                crate::tests::timeout_op("main", "wait_server_addr", 5, addr_rx.recv())
                    .await
                    .expect("Channel closed before server addr published");

            let counter_clone = message_count.clone();
            let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
            // Client workers (1..=3)
            for client_id in 1..=NUM_CLIENTS {
                let counter = counter_clone.clone();

                let handle = crate::runtime::context::spawn_to(client_id, async move || {
                    let client =
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket");

                    let msg = format!("Hello from client {}", client_id);
                    udp_send_to_with_retry(
                        &format!("client_{}", client_id),
                        &client,
                        || {
                            let mut buf = crate::runtime::context::alloc(size);
                            buf.as_slice_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                            buf.set_len(msg.len());
                            buf
                        },
                        server_addr,
                        2,
                    )
                    .await;
                    println!("Client {} sent message", client_id);

                    counter.fetch_add(1, Ordering::SeqCst);
                });
                client_handles.push((client_id, handle));
            }

            for (client_id, handle) in client_handles {
                crate::tests::timeout_op(
                    &format!("main_wait_client_{}", client_id),
                    "wait_client_join",
                    5,
                    handle,
                )
                .await;
            }

            crate::tests::timeout_op("main", "wait_server_join", 5, server_handle).await;

            assert_eq!(message_count.load(Ordering::SeqCst), NUM_CLIENTS);
            println!("All {} clients completed", NUM_CLIENTS);
        });
}

/// Test UDP recv cancellation
#[test]
fn test_udp_cancel_recv_stream() {
    use crate::select;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // Helper to yield once to allow the IO future to be polled and submitted
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    crate::tests::NetworkTestRunner::new("test_udp_cancel_recv_stream")
        .worker_threads(1)
        .run(|_| async move {
            let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
            let _addr = socket.local_addr().expect("Failed to get local address");

            let buf = crate::runtime::context::alloc(veloq_buf::nz!(1024));

            select! {
                _ = socket.recv_stream(buf) => {
                    panic!("RecvStream should have been cancelled, but it completed (unexpectedly)");
                },
                _ = YieldOnce(false) => {
                    println!("UDP recv cancelled successfully");
                }
            };
        });
}
