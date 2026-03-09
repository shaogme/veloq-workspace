//! UDP network tests - single-threaded and multi-threaded.

use veloq_buf::nz;

use crate::net::udp::UdpSocket;
use crate::runtime::Runtime;
use crate::time::timeout;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// ============ Helper Functions ============

// ============ Single-Thread UDP Tests (using Runtime/spawn) ============

/// Test basic UDP socket binding and local_addr
#[test]
fn test_udp_bind() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
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
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            println!("Testing with BufferSize: {:?}", size);

            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(1))
                .build()
                .unwrap();

            runtime.block_on(async move {
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
                    let datagram = socket1_clone
                        .recv_stream(buf)
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

                let (result, _) = socket2_arc.send_to(send_buf, addr1).await;
                let bytes_sent = result.expect("send_to failed");
                println!("Socket 2 sent {} bytes to {}", bytes_sent, addr1);

                handler.await;
            });
        })
        .join()
        .unwrap();
    }
}

/// Test UDP echo (send and receive response)
#[test]
fn test_udp_echo() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            println!("Testing with BufferSize: {:?}", size);
            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(1))
                .build()
                .unwrap();

            runtime.block_on(async move {
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
                    let datagram = server_clone
                        .recv_stream(buf)
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

                    let (result, _) = server_clone.send_to(echo_buf, from_addr).await;
                    result.expect("Server send_to failed");
                    println!("Server echoed data back to {}", from_addr);
                });

                // Client: send data to server
                let mut send_buf = crate::runtime::context::alloc(size);
                let test_data = b"Echo this message!";
                send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
                send_buf.set_len(test_data.len());

                let (result, _) = client_arc.send_to(send_buf, server_addr).await;
                let bytes_sent = result.expect("Client send_to failed");
                println!("Client sent {} bytes", bytes_sent);

                // Receive echo response
                let recv_buf = crate::runtime::context::alloc(size);
                let datagram = client_arc
                    .recv_stream(recv_buf)
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

                server_h.await;
            });
        })
        .join()
        .unwrap();
    }
}

/// Test multiple UDP messages
#[test]
fn test_udp_multiple_messages() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(1))
                .build()
                .unwrap();

            runtime.block_on(async move {
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
                        let datagram = timeout(Duration::from_secs(5), socket1_clone.recv_stream(buf))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "UDP multiple messages timeout: phase=recv_stream; expected={}; received_so_far={}; listen_addr={}; timeout_ms={}",
                                    NUM_MESSAGES,
                                    i,
                                    addr1,
                                    5000
                                )
                            });
                        let datagram = datagram.expect("recv_stream failed");
                        let bytes = datagram.buf.len();
                        let from = datagram.addr;
                        println!("Received message {} ({} bytes) from {}", i, bytes, from);
                    }
                    println!("Received all {} messages", NUM_MESSAGES);
                });

                // Sender
                for i in 0..NUM_MESSAGES {
                    let ready_idx = timeout(Duration::from_secs(5), ready_rx.recv())
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP multiple messages timeout: phase=wait_recv_ready; expected={}; sent_so_far={}; listen_addr={}; timeout_ms={}",
                                NUM_MESSAGES,
                                i,
                                addr1,
                                5000
                            )
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "UDP multiple messages receiver closed readiness channel early: expected={}; sent_so_far={}; listen_addr={}",
                                NUM_MESSAGES,
                                i,
                                addr1
                            )
                        });
                    assert_eq!(
                        ready_idx, i,
                        "UDP multiple messages receiver/sender iteration mismatch"
                    );

                    let mut buf = crate::runtime::context::alloc(size);
                    let msg = format!("Message {}", i);
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    buf.set_len(msg.len());

                    let (result, _) = timeout(Duration::from_secs(5), socket2_arc.send_to(buf, addr1))
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP multiple messages timeout: phase=send_to; sent_so_far={}; target_addr={}; timeout_ms={}",
                                i,
                                addr1,
                                5000
                            )
                        });
                    result.expect("send_to failed");
                    println!("Sent message {}", i);
                }
                println!("Sent all {} messages", NUM_MESSAGES);

                h_recv.await;
            });
        })
        .join()
        .unwrap();
    }
}

/// Test UDP with large data
#[test]
fn test_udp_large_data() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(1))
                .build()
                .unwrap();

            runtime.block_on(async move {
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
                    let datagram = socket1_clone
                        .recv_stream(buf)
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
                let (result, _) = socket2_arc.send_to(buf, addr1).await;
                let bytes = result.expect("send_to failed");
                println!("Sent {} bytes", bytes);

                h_recv.await;
            });
        })
        .join()
        .unwrap();
    }
}

/// Test UDP using heap-allocated FixedBuf
#[test]
fn test_udp_heap_buffer() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
        let socket1 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");

        // Receiver task: Explicitly use heap buffer
        let h_recv = crate::runtime::context::spawn(async move {
            let buf = veloq_buf::FixedBuf::alloc_heap(nz!(1024)).expect("Heap allocation failed");
            let datagram = socket1.recv_stream(buf).await.expect("recv_stream failed");
            let n = datagram.buf.len();
            let buf = datagram.buf;

            assert_eq!(&buf.as_slice()[..n], b"UDP from heap!");
            println!("UDP server received data in heap buffer correctly");
        });

        // Sender: Explicitly use heap buffer
        let mut buf = veloq_buf::FixedBuf::alloc_heap(nz!(1024)).expect("Heap allocation failed");
        let data = b"UDP from heap!";
        buf.as_slice_mut()[..data.len()].copy_from_slice(data);
        buf.set_len(data.len());

        let (result, _) = socket2.send_to(buf, addr1).await;
        result.expect("send_to failed");
        println!("UDP client sent data from heap buffer correctly");

        h_recv.await;
    });
}

/// Test IPv6 UDP
#[test]
fn test_udp_ipv6() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
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
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            let message_count = Arc::new(AtomicUsize::new(0));
            const NUM_WORKERS: usize = 3;

            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(NUM_WORKERS))
                .build()
                .unwrap();

            let message_count_clone = message_count.clone();
            runtime.block_on(async move {
                let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
                for worker_id in 0..NUM_WORKERS {
                    let counter = message_count_clone.clone();

                    let handle = crate::runtime::context::spawn_to(worker_id, async move || {
                        // Each worker creates its own UDP sockets and tests send/recv
                        let socket1 =
                            UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 1");
                        let socket2 =
                            UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

                        let addr1 = socket1.local_addr().expect("Failed to get addr1");
                        tracing::info!("Worker {} socket 1 bound to: {}", worker_id, addr1);

                        let socket1_arc = Arc::new(socket1);
                        let socket2_arc = Arc::new(socket2);
                        let socket1_clone = socket1_arc.clone();
                        // let pool = crate::runtime::context::current_pool().unwrap(); // Use global helper instead

                        // Receiver task via crate::context::spawn
                        let h_recv = crate::runtime::context::spawn(async move {
                            let buf = crate::runtime::context::alloc(size);
                            let datagram = socket1_clone.recv_stream(buf).await.unwrap_or_else(|e| {
                                tracing::error!(
                                    "Worker {} recv_stream failed: {:?}",
                                    worker_id,
                                    e
                                );
                                panic!(
                                    "Worker {} recv_stream failed callback: {:?}",
                                    worker_id, e
                                );
                            });
                            tracing::info!("Worker {} received message", worker_id);
                            drop(datagram.buf);
                        });

                        // Sender
                        let mut buf = crate::runtime::context::alloc(size);
                        let msg = format!("Hello from worker {}", worker_id);
                        buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                        buf.set_len(msg.len());

                        let (result, _) =
                            timeout(Duration::from_secs(5), socket2_arc.send_to(buf, addr1))
                                .await
                                .unwrap_or_else(|_| {
                                    panic!(
                                        "UDP no-echo timeout: phase=send_to; worker_id={}; target_addr={}; timeout_ms={}",
                                        worker_id,
                                        addr1,
                                        5000
                                    )
                                });
                        result.expect("send_to failed");
                        tracing::info!("Worker {} sent message", worker_id);

                        tracing::info!("Worker {} waiting for h_recv", worker_id);
                        timeout(Duration::from_secs(5), async move { h_recv.await })
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "UDP no-echo timeout: phase=wait_recv_task; worker_id={}; listen_addr={}; timeout_ms={}",
                                    worker_id,
                                    addr1,
                                    5000
                                )
                            });
                        tracing::info!("Worker {} h_recv joined", worker_id);
                        counter.fetch_add(1, Ordering::SeqCst);
                        tracing::info!("Worker {} completed", worker_id);
                    });
                    worker_handles.push((worker_id, handle));
                }

                for (worker_id, handle) in worker_handles {
                    timeout(Duration::from_secs(5), async move { handle.await })
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP no-echo timeout: phase=wait_worker_join; worker_id={}; done_count={}; timeout_ms={}",
                                worker_id,
                                message_count_clone.load(Ordering::SeqCst),
                                5000
                            )
                        });
                }
            });

            assert_eq!(message_count.load(Ordering::SeqCst), NUM_WORKERS);
            tracing::info!(
                "All {} workers completed UDP self-communication",
                NUM_WORKERS
            );
        })
        .join()
        .unwrap();
    }
}

/// Test UDP echo server on one worker, clients on another
#[test]
fn test_multithread_udp_echo() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            let (addr_tx, mut addr_rx) = crate::sync::mpsc::unbounded();
            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(2)) // 2 workers (0 and 1)
                .build()
                .unwrap();

            // Worker 0: Echo server
            runtime.block_on(async move {
                let addr_tx = addr_tx.clone();

                let server_h = crate::runtime::context::spawn_to(0, async move || {
                    let socket = Arc::new(
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket"),
                    );
                    let server_addr = socket.local_addr().expect("Failed to get server address");
                    println!("UDP echo server listening on {}", server_addr);

                    // Pre-post recv before publishing server address to avoid RIO timing window.
                    let (ready_tx, mut ready_rx) = crate::sync::mpsc::unbounded::<()>();
                    let socket_for_recv = socket.clone();
                    let recv_h = crate::runtime::context::spawn(async move {
                        ready_tx.send(()).unwrap();
                        let buf = crate::runtime::context::alloc(size);
                        let datagram = timeout(Duration::from_secs(5), socket_for_recv.recv_stream(buf))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "UDP echo timeout: phase=server_recv; server_addr={}; timeout_ms={}",
                                    server_addr,
                                    5000
                                )
                            });
                        let datagram = datagram.expect("Server recv_stream failed");
                        let bytes = datagram.buf.len();
                        let from_addr = datagram.addr;
                        let buf = datagram.buf;
                        (bytes, from_addr, buf)
                    });
                    timeout(Duration::from_secs(5), ready_rx.recv())
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=server_recv_ready; server_addr={}; timeout_ms={}",
                                server_addr,
                                5000
                            )
                        })
                        .expect("server recv readiness channel closed");

                    // Send address to client worker after recv is posted.
                    addr_tx.send(server_addr).unwrap();

                    let (bytes, from_addr, buf) = timeout(Duration::from_secs(5), async move { recv_h.await })
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=wait_server_recv_join; server_addr={}; timeout_ms={}",
                                server_addr,
                                5000
                            )
                        });
                    println!("Server received {} bytes from {}", bytes, from_addr);

                    // Echo back
                    let mut echo_buf = crate::runtime::context::alloc(size);
                    echo_buf.spare_capacity_mut()[..bytes]
                        .copy_from_slice(&buf.as_slice()[..bytes]);
                    echo_buf.set_len(bytes);

                    let (result, _) = timeout(Duration::from_secs(5), socket.send_to(echo_buf, from_addr))
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=server_send; server_addr={}; peer_addr={}; timeout_ms={}",
                                server_addr,
                                from_addr,
                                5000
                            )
                        });
                    result.expect("Server send_to failed");
                    println!("Server echoed response");
                });

                // Worker 1: Client
                let client_h = crate::runtime::context::spawn_to(1, async move || {
                    // Wait for server address
                    let server_addr = timeout(Duration::from_secs(5), addr_rx.recv())
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=wait_server_addr; timeout_ms={}",
                                5000
                            )
                        })
                        .expect("Channel closed");

                    println!("Client connecting to {}", server_addr);

                    let client = Arc::new(
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket"),
                    );

                    // Pre-post client recv before sending request to avoid RIO response drop.
                    let (client_ready_tx, mut client_ready_rx) = crate::sync::mpsc::unbounded::<()>();
                    let client_for_recv = client.clone();
                    let recv_h = crate::runtime::context::spawn(async move {
                        client_ready_tx.send(()).unwrap();
                        let recv_buf = crate::runtime::context::alloc(size);
                        let datagram = timeout(Duration::from_secs(5), client_for_recv.recv_stream(recv_buf))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "UDP echo timeout: phase=client_recv; server_addr={}; timeout_ms={}",
                                    server_addr,
                                    5000
                                )
                            });
                        let datagram = datagram.expect("Client recv_stream failed");
                        let from = datagram.addr;
                        let recv_buf = datagram.buf;
                        (from, recv_buf)
                    });
                    timeout(Duration::from_secs(5), client_ready_rx.recv())
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=client_recv_ready; server_addr={}; timeout_ms={}",
                                server_addr,
                                5000
                            )
                        })
                        .expect("client recv readiness channel closed");

                    // Send data
                    let mut send_buf = crate::runtime::context::alloc(size);
                    let data = b"Hello from worker 2!";
                    send_buf.as_slice_mut()[..data.len()].copy_from_slice(data);
                    send_buf.set_len(data.len());

                    let (result, _) = timeout(Duration::from_secs(5), client.send_to(send_buf, server_addr))
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=client_send; server_addr={}; timeout_ms={}",
                                server_addr,
                                5000
                            )
                        });
                    let sent = result.expect("Client send_to failed");
                    println!("Client sent {} bytes", sent);

                    let (from, recv_buf) = timeout(Duration::from_secs(5), async move { recv_h.await })
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP echo timeout: phase=wait_client_recv_join; server_addr={}; timeout_ms={}",
                                server_addr,
                                5000
                            )
                        });

                    assert_eq!(from, server_addr);
                    assert_eq!(&recv_buf.as_slice()[..data.len()], data);
                    println!("Client received correct echo");
                });

                println!("UDP echo phase: wait_client_join");
                timeout(Duration::from_secs(5), async move { client_h.await })
                    .await
                    .unwrap_or_else(|_| {
                        panic!("UDP echo timeout: phase=wait_client_join; timeout_ms={}", 5000)
                    });
                println!("UDP echo phase: wait_server_join");
                timeout(Duration::from_secs(5), async move { server_h.await })
                    .await
                    .unwrap_or_else(|_| {
                        panic!("UDP echo timeout: phase=wait_server_join; timeout_ms={}", 5000)
                    });
                println!("UDP echo phase: joins_done");
            });

            println!("UDP echo phase: block_on_done");
            println!("Multi-thread UDP echo test completed");
        })
        .join()
        .unwrap();
    }
}

/// Test concurrent UDP clients from multiple workers to shared server
#[test]
fn test_multithread_concurrent_udp_clients() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        const NUM_CLIENTS: usize = 3;
        const NUM_WORKERS: usize = 4; // 0=Server, 1,2,3=Clients

        // Phase code:
        // 0=init, 1=server_spawned, 2=wait_server_addr, 3=clients_spawned,
        // 4=wait_client_join, 5=wait_server_join, 6=done
        let phase = Arc::new(AtomicUsize::new(0));
        let server_recv_count = Arc::new(AtomicUsize::new(0));
        let client_send_count = Arc::new(AtomicUsize::new(0));
        let client_join_count = Arc::new(AtomicUsize::new(0));
        let last_client_id = Arc::new(AtomicUsize::new(0));

        let phase_t = phase.clone();
        let server_recv_count_t = server_recv_count.clone();
        let client_send_count_t = client_send_count.clone();
        let client_join_count_t = client_join_count.clone();
        let last_client_id_t = last_client_id.clone();

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        std::thread::spawn(move || {
            let (addr_tx, mut addr_rx) = crate::sync::mpsc::unbounded::<SocketAddr>();
            let message_count = Arc::new(AtomicUsize::new(0));

            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(NUM_WORKERS))
                .build()
                .unwrap();

            let message_count_clone = message_count.clone();
            runtime.block_on(async move {
                // Server worker (0)
                phase_t.store(1, Ordering::SeqCst);
                let addr_tx = addr_tx.clone();
                let server_recv_count_s = server_recv_count_t.clone();
                let server_handle = crate::runtime::context::spawn_to(0, async move || {
                    let socket =
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket");
                    let server_addr = socket.local_addr().expect("Failed to get server address");
                    println!("Server listening on {}", server_addr);

                    // Publish server address once; main task fans out to clients.
                    addr_tx.send(server_addr).unwrap();

                    // Receive messages from all clients
                    for i in 0..NUM_CLIENTS {
                        let buf = crate::runtime::context::alloc(size);
                        let datagram = timeout(Duration::from_secs(5), socket.recv_stream(buf))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "UDP concurrent clients timeout: phase=server_recv; expected_clients={}; received_so_far={}; server_addr={}; timeout_ms={}",
                                    NUM_CLIENTS,
                                    i,
                                    server_addr,
                                    5000
                                )
                            });
                        let datagram = datagram.expect("Server recv_stream failed");
                        let bytes = datagram.buf.len();
                        let from = datagram.addr;
                        let received = server_recv_count_s.fetch_add(1, Ordering::SeqCst) + 1;
                        println!(
                            "Server received message {} ({} bytes) from {}",
                            i, bytes, from
                        );
                        println!("Server received progress {}/{}", received, NUM_CLIENTS);
                    }
                    println!("Server received all {} messages", NUM_CLIENTS);
                });

                phase_t.store(2, Ordering::SeqCst);
                let server_addr = timeout(Duration::from_secs(5), addr_rx.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "UDP concurrent clients timeout: phase=wait_server_addr; timeout_ms={}",
                            5000
                        )
                    })
                    .expect("Channel closed before server addr published");

                let counter_clone = message_count_clone.clone();
                let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
                phase_t.store(3, Ordering::SeqCst);
                // Client workers (1..=3)
                for client_id in 1..=NUM_CLIENTS {
                    let counter = counter_clone.clone();
                    let server_addr = server_addr;
                    let client_send_count_c = client_send_count_t.clone();
                    let last_client_id_c = last_client_id_t.clone();

                    let handle = crate::runtime::context::spawn_to(client_id, async move || {
                        let client =
                            UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket");

                        let mut buf = crate::runtime::context::alloc(size);
                        let msg = format!("Hello from client {}", client_id);
                        buf.as_slice_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                        buf.set_len(msg.len());

                        let (result, _) =
                            timeout(Duration::from_secs(5), client.send_to(buf, server_addr))
                                .await
                                .unwrap_or_else(|_| {
                                    panic!(
                                        "UDP concurrent clients timeout: phase=client_send; client_id={}; server_addr={}; timeout_ms={}",
                                        client_id,
                                        server_addr,
                                        5000
                                    )
                                });
                        result.expect("Client send_to failed");
                        last_client_id_c.store(client_id, Ordering::SeqCst);
                        client_send_count_c.fetch_add(1, Ordering::SeqCst);
                        println!("Client {} sent message", client_id);

                        counter.fetch_add(1, Ordering::SeqCst);
                    });
                    client_handles.push((client_id, handle));
                }

                phase_t.store(4, Ordering::SeqCst);
                for (client_id, handle) in client_handles {
                    timeout(Duration::from_secs(5), async move { handle.await })
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "UDP concurrent clients timeout: phase=wait_client_join; client_id={}; current_done={}; timeout_ms={}",
                                client_id,
                                message_count_clone.load(Ordering::SeqCst),
                                5000
                            )
                        });
                    client_join_count_t.fetch_add(1, Ordering::SeqCst);
                }

                phase_t.store(5, Ordering::SeqCst);
                timeout(Duration::from_secs(5), async move { server_handle.await })
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "UDP concurrent clients timeout: phase=wait_server_join; expected_clients={}; current_done={}; timeout_ms={}",
                            NUM_CLIENTS,
                            message_count_clone.load(Ordering::SeqCst),
                            5000
                        )
                    });

                phase_t.store(6, Ordering::SeqCst);
            });

            assert_eq!(message_count.load(Ordering::SeqCst), NUM_CLIENTS);
            println!("All {} clients completed", NUM_CLIENTS);
            let _ = done_tx.send(());
        });

        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| {
                panic!(
                    "UDP concurrent clients hard-timeout: size={:?}; phase={}; server_recv_count={}; client_send_count={}; client_join_count={}; last_client_id={}; timeout_ms={}",
                    size,
                    phase.load(Ordering::SeqCst),
                    server_recv_count.load(Ordering::SeqCst),
                    client_send_count.load(Ordering::SeqCst),
                    client_join_count.load(Ordering::SeqCst),
                    last_client_id.load(Ordering::SeqCst),
                    10000
                )
            });
    }
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

    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
        let _addr = socket.local_addr().expect("Failed to get local address");

        let buf = crate::runtime::context::alloc(nz!(1024));

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
