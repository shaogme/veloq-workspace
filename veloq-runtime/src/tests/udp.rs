//! UDP network tests - single-threaded and multi-threaded.

use veloq_buf::nz;

use crate::net::udp::UdpSocket;
use crate::runtime::Runtime;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
                    // buf.set_len(buf.capacity());
                    let (result, _buf) = socket1_clone.recv_from(buf).await;
                    let (bytes_read, from_addr) = result.expect("recv_from failed");
                    println!("Socket 1 received {} bytes from {}", bytes_read, from_addr);
                    assert_eq!(from_addr, addr2);
                });

                // Sender: socket2 sends data to socket1
                let mut send_buf = crate::runtime::context::alloc(size);
                let test_data = b"Hello, UDP!";
                send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);

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
                    let (result, buf) = server_clone.recv_from(buf).await;
                    let (bytes_read, from_addr) = result.expect("Server recv_from failed");
                    println!("Server received {} bytes from {}", bytes_read, from_addr);

                    // Echo back
                    let mut echo_buf = crate::runtime::context::alloc(size);
                    echo_buf.spare_capacity_mut()[..bytes_read as usize]
                        .copy_from_slice(&buf.as_slice()[..bytes_read as usize]);

                    let (result, _) = server_clone.send_to(echo_buf, from_addr).await;
                    result.expect("Server send_to failed");
                    println!("Server echoed data back to {}", from_addr);
                });

                // Client: send data to server
                let mut send_buf = crate::runtime::context::alloc(size);
                let test_data = b"Echo this message!";
                send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);

                let (result, _) = client_arc.send_to(send_buf, server_addr).await;
                let bytes_sent = result.expect("Client send_to failed");
                println!("Client sent {} bytes", bytes_sent);

                // Receive echo response
                let recv_buf = crate::runtime::context::alloc(size);
                let (result, recv_buf) = client_arc.recv_from(recv_buf).await;
                let (bytes_received, from_addr) = result.expect("Client recv_from failed");

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

                // Receiver task
                let h_recv = crate::runtime::context::spawn(async move {
                    for i in 0..NUM_MESSAGES {
                        let buf = crate::runtime::context::alloc(size);
                        let (result, _buf) = socket1_clone.recv_from(buf).await;
                        let (bytes, from) = result.expect("recv_from failed");
                        println!("Received message {} ({} bytes) from {}", i, bytes, from);
                    }
                    println!("Received all {} messages", NUM_MESSAGES);
                });

                // Sender
                for i in 0..NUM_MESSAGES {
                    let mut buf = crate::runtime::context::alloc(size);
                    let msg = format!("Message {}", i);
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());

                    let (result, _) = socket2_arc.send_to(buf, addr1).await;
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
                    let (result, buf) = socket1_clone.recv_from(buf).await;
                    let (bytes, _from) = result.expect("recv_from failed");
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

                let (result, _) = socket2_arc.send_to(buf, addr1).await;
                let bytes = result.expect("send_to failed") as usize;
                println!("Sent {} bytes", bytes);

                h_recv.await;
            });
        })
        .join()
        .unwrap();
    }
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

            let (tx, mut rx) = crate::sync::mpsc::unbounded();

            let message_count_clone = message_count.clone();
            runtime.block_on(async move {
                for worker_id in 0..NUM_WORKERS {
                    let counter = message_count_clone.clone();
                    let tx_done = tx.clone();

                    crate::runtime::context::spawn_to(worker_id, async move || {
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
                            let (result, buf) = socket1_clone.recv_from(buf).await;
                            match result {
                                Ok((_bytes, _from)) => {
                                    tracing::info!("Worker {} received message", worker_id);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Worker {} recv_from failed: {:?}",
                                        worker_id,
                                        e
                                    );
                                    panic!(
                                        "Worker {} recv_from failed callback: {:?}",
                                        worker_id, e
                                    );
                                }
                            }
                            drop(buf);
                        });

                        // Sender
                        let mut buf = crate::runtime::context::alloc(size);
                        let msg = format!("Hello from worker {}", worker_id);
                        buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());

                        let (result, _) = socket2_arc.send_to(buf, addr1).await;
                        result.expect("send_to failed");
                        tracing::info!("Worker {} sent message", worker_id);

                        tracing::info!("Worker {} waiting for h_recv", worker_id);
                        h_recv.await;
                        tracing::info!("Worker {} h_recv joined", worker_id);
                        counter.fetch_add(1, Ordering::SeqCst);
                        tx_done.send(()).unwrap();
                        tracing::info!("Worker {} sent done signal", worker_id);
                    });
                }

                for i in 0..NUM_WORKERS {
                    rx.recv().await.unwrap();
                    tracing::info!("Main loop received done signal {}", i + 1);
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

            let (done_tx, mut done_rx) = crate::sync::mpsc::unbounded();

            // Worker 0: Echo server
            runtime.block_on(async move {
                let addr_tx = addr_tx.clone();

                crate::runtime::context::spawn_to(0, async move || {
                    let socket =
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket");
                    let server_addr = socket.local_addr().expect("Failed to get server address");
                    println!("UDP echo server listening on {}", server_addr);

                    // Send address to client worker
                    addr_tx.send(server_addr).unwrap();

                    // let pool = crate::runtime::context::current_pool().unwrap();

                    // Receive and echo
                    let buf = crate::runtime::context::alloc(size);

                    let (result, buf) = socket.recv_from(buf).await;
                    let (bytes, from_addr) = result.expect("Server recv_from failed");
                    println!("Server received {} bytes from {}", bytes, from_addr);

                    // Echo back
                    let mut echo_buf = crate::runtime::context::alloc(size);
                    echo_buf.spare_capacity_mut()[..bytes as usize]
                        .copy_from_slice(&buf.as_slice()[..bytes as usize]);

                    let (result, _) = socket.send_to(echo_buf, from_addr).await;
                    result.expect("Server send_to failed");
                    println!("Server echoed response");
                });

                // Worker 1: Client
                // let addr_rx = addr_rx.clone();
                let done_tx = done_tx.clone();

                crate::runtime::context::spawn_to(1, async move || {
                    // Wait for server address
                    let server_addr = addr_rx.recv().await.expect("Channel closed");

                    println!("Client connecting to {}", server_addr);

                    let client =
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket");
                    // let pool = crate::runtime::context::current_pool().unwrap();

                    // Send data
                    let mut send_buf = crate::runtime::context::alloc(size);
                    let data = b"Hello from worker 2!";
                    send_buf.as_slice_mut()[..data.len()].copy_from_slice(data);

                    let (result, _) = client.send_to(send_buf, server_addr).await;
                    let sent = result.expect("Client send_to failed");
                    println!("Client sent {} bytes", sent);

                    // Receive echo
                    let recv_buf = crate::runtime::context::alloc(size);
                    let (result, recv_buf) = client.recv_from(recv_buf).await;
                    let (_received, from) = result.expect("Client recv_from failed");

                    assert_eq!(from, server_addr);
                    assert_eq!(&recv_buf.as_slice()[..data.len()], data);
                    println!("Client received correct echo");

                    done_tx.send(()).unwrap();
                });

                // Wait for completion
                done_rx.recv().await.unwrap();
            });

            println!("Multi-thread UDP echo test completed");
        })
        .join()
        .unwrap();
    }
}

/// Test concurrent UDP clients from multiple workers to shared server
#[test]
fn test_multithread_concurrent_udp_clients() {
    use std::sync::mpsc;
    use std::time::Duration;

    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        std::thread::spawn(move || {
            let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();
            let addr_rx = Arc::new(Mutex::new(addr_rx));
            let message_count = Arc::new(AtomicUsize::new(0));

            const NUM_CLIENTS: usize = 3;
            const NUM_WORKERS: usize = 4; // 0=Server, 1,2,3=Clients

            let runtime = Runtime::builder()
                .config(crate::config::Config::default().worker_threads(NUM_WORKERS))
                .build()
                .unwrap();

            let (done_tx, mut done_rx) = crate::sync::mpsc::unbounded();

            let message_count_clone = message_count.clone();
            runtime.block_on(async move {
                // Server worker (0)
                let addr_tx = addr_tx.clone();
                crate::runtime::context::spawn_to(0, async move || {
                    let socket =
                        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind server socket");
                    let server_addr = socket.local_addr().expect("Failed to get server address");
                    println!("Server listening on {}", server_addr);

                    // Broadcast address to all clients
                    for _ in 0..NUM_CLIENTS {
                        addr_tx.send(server_addr).unwrap();
                    }

                    // let pool = crate::runtime::context::current_pool().unwrap();

                    // Receive messages from all clients
                    for i in 0..NUM_CLIENTS {
                        let buf = crate::runtime::context::alloc(size);
                        let (result, _buf) = socket.recv_from(buf).await;
                        let (bytes, from) = result.expect("Server recv_from failed");
                        println!(
                            "Server received message {} ({} bytes) from {}",
                            i, bytes, from
                        );
                    }
                    println!("Server received all {} messages", NUM_CLIENTS);
                });

                let counter_clone = message_count_clone.clone();
                // Client workers (1..=3)
                for client_id in 1..=NUM_CLIENTS {
                    let rx = addr_rx.clone();
                    let counter = counter_clone.clone();
                    let done_tx = done_tx.clone();

                    crate::runtime::context::spawn_to(client_id, async move || {
                        let server_addr = {
                            rx.lock()
                                .unwrap()
                                .recv_timeout(Duration::from_secs(5))
                                .expect("Timeout waiting for server address")
                        };

                        let client =
                            UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client socket");
                        // let pool = crate::runtime::context::current_pool().unwrap();

                        let mut buf = crate::runtime::context::alloc(size);
                        let msg = format!("Hello from client {}", client_id);
                        buf.as_slice_mut()[..msg.len()].copy_from_slice(msg.as_bytes());

                        let (result, _) = client.send_to(buf, server_addr).await;
                        result.expect("Client send_to failed");
                        println!("Client {} sent message", client_id);

                        counter.fetch_add(1, Ordering::SeqCst);
                        done_tx.send(()).unwrap();
                    });
                }

                // Wait for clients
                for _ in 0..NUM_CLIENTS {
                    done_rx.recv().await.unwrap();
                }
            });

            assert_eq!(message_count.load(Ordering::SeqCst), NUM_CLIENTS);
            println!("All {} clients completed", NUM_CLIENTS);
        })
        .join()
        .unwrap();
    }
}
