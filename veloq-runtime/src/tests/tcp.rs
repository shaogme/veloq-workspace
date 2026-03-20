//! TCP network tests - single-threaded and multi-threaded.

use crate::net::tcp::{TcpListener, TcpStream};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ============ Helper Functions ============

// ============ Single-Thread TCP Tests (using Runtime/spawn) ============

/// Test basic TCP connection using global spawn
#[test]
fn test_tcp_connect_with_global_api() {
    crate::tests::NetworkTestRunner::new("test_tcp_connect_with_global_api")
        .worker_threads(1)
        .run(|_| async move {
            // Create listener inside block_on to access context
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");
            println!("Listener bound to: {}", listen_addr);

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            // Server task using cx.spawn
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, peer_addr) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");
                println!("Accepted connection from: {}", peer_addr);
                drop(stream);
            });

            // Client uses cx implicitly
            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");
            println!("Connected successfully");
            drop(stream);

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test TCP data send and receive (echo)
#[test]
fn test_tcp_send_recv() {
    crate::tests::NetworkTestRunner::new("test_tcp_send_recv")
        .worker_threads(1)
        .run(|size| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            // Server task: Robust Echo Loop
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");

                loop {
                    let buf = crate::runtime::context::alloc(size);
                    let (result, mut buf) = stream.recv(buf).await;
                    let bytes_read = match result {
                        Ok(n) if n > 0 => n,
                        _ => break, // EOF or Error
                    };

                    // Echo exact bytes received
                    buf.set_len(bytes_read);
                    if let Err(e) = stream.send(buf).await.0 {
                        println!("Server echo failed: {}", e);
                        break;
                    }
                }
            });

            // Client
            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");

            // Prepare data
            let mut send_buf = crate::runtime::context::alloc(size);
            let test_data = b"Hello, TCP!";
            send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
            // Buffer is full length (clamped) by default, containing test_data + zeros

            // Send data
            let bytes_to_send = send_buf.len();
            let (result, _) =
                crate::tests::timeout_op("client", "send", 5, stream.send(send_buf)).await;
            let bytes_sent = result.expect("Client send failed");
            assert_eq!(bytes_sent, bytes_to_send);
            println!("Client sent {} bytes", bytes_sent);

            // Receive loop verify
            let mut total_received = 0;

            while total_received < bytes_sent {
                let recv_buf = crate::runtime::context::alloc(size);
                let (result, recv_buf) =
                    crate::tests::timeout_op("client", "recv", 5, stream.recv(recv_buf)).await;
                let n = result.expect("Client recv failed");
                if n == 0 {
                    break;
                } // Unexpected EOF?

                if total_received == 0 {
                    // Verify first chunk header
                    assert!(n >= test_data.len(), "First chunk too small");
                    assert_eq!(&recv_buf.as_slice()[..test_data.len()], test_data);
                }
                total_received += n;
            }

            println!("Client received {} bytes", total_received);

            // Verify
            assert_eq!(bytes_sent, total_received);
            println!("Data verification successful!");

            // Close client to let server exit loop
            drop(stream);
            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test multiple concurrent connections on single thread
#[test]
fn test_tcp_multiple_connections() {
    crate::tests::NetworkTestRunner::new("test_tcp_multiple_connections")
        .worker_threads(1)
        .run(|_| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            const NUM_CONNECTIONS: usize = 5;

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            // Server task: accept all connections
            let server_h = crate::runtime::context::spawn(async move {
                for i in 0..NUM_CONNECTIONS {
                    let (stream, peer) =
                        crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                            .await
                            .expect("Accept failed");
                    println!("Accepted connection {} from {}", i, peer);
                    drop(stream);
                }
                println!("All {} connections accepted", NUM_CONNECTIONS);
            });

            // Client: make connections sequentially
            for i in 0..NUM_CONNECTIONS {
                let stream = crate::tests::timeout_op(
                    "client",
                    "connect",
                    5,
                    TcpStream::connect(listen_addr),
                )
                .await
                .expect("Failed to connect");
                println!("Client {} connected", i);
                drop(stream);
            }
            println!("All {} connections completed", NUM_CONNECTIONS);

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test large data transfer
#[test]
fn test_tcp_large_data_transfer() {
    crate::tests::NetworkTestRunner::new("test_tcp_large_data_transfer")
        .worker_threads(1)
        .run(|size| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            const DATA_SIZE: usize = 8192; // 8KB
            const CHUNK_SIZE: usize = 4096;

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            // Server task
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");

                let mut total_received = 0;
                while total_received < DATA_SIZE {
                    let buf = crate::runtime::context::alloc(size);
                    // buf.set_len(buf.capacity());
                    let (result, _buf) =
                        crate::tests::timeout_op("server", "recv", 5, stream.recv(buf)).await;
                    let bytes = result.expect("Recv failed");
                    if bytes == 0 {
                        break;
                    }
                    total_received += bytes;
                    println!(
                        "Server received {} bytes (total: {})",
                        bytes, total_received
                    );
                }

                assert!(total_received >= DATA_SIZE);
                println!("Server received all {} bytes", DATA_SIZE);
            });

            // Client
            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");

            let mut total_sent = 0;
            while total_sent < DATA_SIZE {
                let chunk_size = std::cmp::min(CHUNK_SIZE, DATA_SIZE - total_sent);

                let mut buf = crate::runtime::context::alloc(size);

                for i in 0..chunk_size {
                    buf.spare_capacity_mut()[i] = (i % 256) as u8;
                }

                let (result, _buf) =
                    crate::tests::timeout_op("client", "send", 5, stream.send(buf)).await;
                let bytes = result.expect("Send failed");
                total_sent += bytes;
                println!("Client sent {} bytes (total: {})", bytes, total_sent);
            }

            assert!(total_sent >= DATA_SIZE);
            println!("Client sent all {} bytes", total_sent);

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test listener local_addr
#[test]
fn test_listener_local_addr() {
    crate::tests::NetworkTestRunner::new("test_listener_local_addr")
        .worker_threads(1)
        .run(|_| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");

            let addr = listener.local_addr().expect("Failed to get local address");

            assert_eq!(addr.ip().to_string(), "127.0.0.1");
            assert_ne!(addr.port(), 0);

            println!("Listener local address: {}", addr);
        });
}

/// Test connection refused
#[test]
fn test_tcp_connect_refused() {
    crate::tests::NetworkTestRunner::new("test_tcp_connect_refused")
        .worker_threads(1)
        .buffer_sizes(vec![veloq_buf::nz!(8192)])
        .run(|_| async move {
            // Reserve an ephemeral local port then close it immediately.
            // Connecting to this now-closed port should fail fast with connection refused.
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let addr = listener
                .local_addr()
                .expect("Failed to get listener address");
            drop(listener);

            let result = TcpStream::connect(addr).await;

            assert!(result.is_err());
            println!("Connection refused as expected: {:?}", result.err());
        });
}

/// Test receiving zero bytes (EOF)
#[test]
fn test_tcp_recv_zero_bytes() {
    crate::tests::NetworkTestRunner::new("test_tcp_recv_zero_bytes")
        .worker_threads(1)
        .run(|size| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();
            // Server: accept and immediately close
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");
                println!("Server accepted and closing connection");
                drop(stream);
            });

            // Client
            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");

            let buf = crate::runtime::context::alloc(size);
            let (result, _buf) =
                crate::tests::timeout_op("client", "recv", 5, stream.recv(buf)).await;

            if let Ok(bytes) = result {
                assert_eq!(bytes, 0, "Should receive 0 bytes on closed connection");
                println!("Correctly received 0 bytes (EOF)");
            } else {
                println!("Received error on closed connection: {:?}", result.err());
            }

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test TCP using heap-allocated FixedBuf (fallback mechanism)
#[test]
fn test_tcp_heap_buffer() {
    crate::tests::NetworkTestRunner::new("test_tcp_heap_buffer")
        .worker_threads(1)
        .run(|_| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            // Server task: Use heap-allocated buffer for receive
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, listener.accept())
                        .await
                        .expect("Accept failed");

                // Explicitly allocate from heap
                let buf = veloq_buf::FixedBuf::alloc_heap(veloq_buf::nz!(4096))
                    .expect("Heap allocation failed");
                let (result, buf) =
                    crate::tests::timeout_op("server", "recv", 5, stream.recv(buf)).await;
                let n = result.expect("Server recv failed");

                assert_eq!(&buf.as_slice()[..n], b"Hello from heap!");
                println!("Server received data in heap buffer correctly");
            });

            // Client: Use heap-allocated buffer for send
            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");

            let mut buf = veloq_buf::FixedBuf::alloc_heap(veloq_buf::nz!(4096))
                .expect("Heap allocation failed");
            let data = b"Hello from heap!";
            buf.as_slice_mut()[..data.len()].copy_from_slice(data);
            buf.set_len(data.len());

            let (result, _) = crate::tests::timeout_op("client", "send", 5, stream.send(buf)).await;
            result.expect("Client send failed");
            println!("Client sent data from heap buffer correctly");

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

/// Test IPv6 connection
#[test]
fn test_tcp_ipv6() {
    crate::tests::NetworkTestRunner::new("test_tcp_ipv6")
        .worker_threads(1)
        .run(|_| async move {
            let listener_result = TcpListener::bind("::1:0");

            if listener_result.is_err() {
                println!("IPv6 not available, skipping test");
                return;
            }

            let listener = listener_result.unwrap();
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            assert!(listen_addr.is_ipv6());
            println!("IPv6 listener bound to: {}", listen_addr);

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            let server_h = crate::runtime::context::spawn(async move {
                let (stream, peer) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");
                println!("Accepted IPv6 connection from: {}", peer);
                drop(stream);
            });

            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect via IPv6");

            println!("IPv6 connection successful");
            drop(stream);

            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

// ============ Multi-Thread TCP Tests ============

/// Test TCP connection across multiple worker threads (each thread is independent)
#[test]
fn test_multithread_tcp_connections() {
    crate::tests::NetworkTestRunner::new("test_multithread_tcp_connections")
        .worker_threads(3)
        .run(|_| async move {
            let connection_count = Arc::new(AtomicUsize::new(0));
            const NUM_WORKERS: usize = 3;

            let connection_count_for_block = connection_count.clone();

            let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
            // We will spawn a task for each worker pinned to that worker
            for worker_id in 0..NUM_WORKERS {
                let counter = connection_count_for_block.clone();

                let handle = crate::runtime::context::spawn_to(worker_id, async move || {
                    let listener =
                        TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
                    let listen_addr = listener.local_addr().expect("Failed to get local address");
                    println!("Worker {} listening on {}", worker_id, listen_addr);

                    let server_h = {
                        let listener_arc = Arc::new(listener);
                        let listener_clone = listener_arc.clone();

                        // Spawn server task
                        crate::runtime::context::spawn(async move {
                            let (stream, peer) = crate::tests::timeout_op(
                                "server",
                                "accept",
                                5,
                                listener_clone.accept(),
                            )
                            .await
                            .expect("Accept failed");
                            println!("Worker {} accepted from {}", worker_id, peer);
                            drop(stream);
                        })
                    }; // Arc is dropped here

                    // Client connects to self
                    let stream = crate::tests::timeout_op(
                        "client",
                        "connect",
                        5,
                        TcpStream::connect(listen_addr),
                    )
                    .await
                    .expect("Failed to connect");
                    println!("Worker {} connected to self", worker_id);
                    drop(stream);

                    crate::tests::timeout_op("worker", "wait_server_join", 5, server_h).await;
                    counter.fetch_add(1, Ordering::SeqCst);
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

            assert_eq!(connection_count.load(Ordering::SeqCst), NUM_WORKERS);
            println!("All {} workers completed TCP self-connections", NUM_WORKERS);
        });
}

/// Test TCP echo server on one worker, clients on another
#[test]
fn test_multithread_tcp_echo() {
    crate::tests::NetworkTestRunner::new("test_multithread_tcp_echo")
        .worker_threads(2)
        .run(|size| async move {
            let (addr_tx, mut addr_rx) = crate::sync::mpsc::unbounded();
            let addr_tx = addr_tx.clone(); // Move into task
            let (done_tx, mut done_rx) = crate::sync::mpsc::unbounded();

            // Worker 0: Echo server
            let server_h = crate::runtime::context::spawn_to(0, async move || {
                let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
                let listen_addr = listener.local_addr().expect("Failed to get local address");
                println!("Echo server listening on {}", listen_addr);

                // Send address to client (via channel)
                addr_tx.send(listen_addr).unwrap();

                // Accept and echo
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, Arc::new(listener).accept())
                        .await
                        .expect("Accept failed");
                let expect = b"Hello from worker 1!";
                let mut recv_buf = crate::runtime::context::alloc(size);
                let mut received = Vec::with_capacity(expect.len());
                while received.len() < expect.len() {
                    let (result, buf) =
                        crate::tests::timeout_op("server", "recv", 5, stream.recv(recv_buf)).await;
                    recv_buf = buf;
                    let n = result.expect("Recv failed");
                    assert!(n > 0, "Peer closed before sending full request");
                    let remain = expect.len() - received.len();
                    received.extend_from_slice(&recv_buf.as_slice()[..n.min(remain)]);
                }
                assert_eq!(received.as_slice(), expect);

                // Echo back (handle potential partial sends)
                let mut sent = 0usize;
                while sent < expect.len() {
                    let remain = &expect[sent..];
                    let mut echo_buf = crate::runtime::context::alloc(size);
                    let chunk = remain.len().min(echo_buf.capacity());
                    echo_buf.spare_capacity_mut()[..chunk].copy_from_slice(&remain[..chunk]);
                    echo_buf.set_len(chunk);

                    let (result, _) =
                        crate::tests::timeout_op("server", "send", 5, stream.send(echo_buf)).await;
                    let n = result.expect("Send failed");
                    assert!(n > 0, "Send returned 0 before echo completed");
                    sent += n;
                }
                println!("Echo server sent response");

                crate::tests::timeout_op("server", "wait_client_done", 5, done_rx.recv())
                    .await
                    .expect("Client done channel closed");
            });

            // Worker 1: Client
            let client_h = crate::runtime::context::spawn_to(1, async move || {
                // Wait for server address
                let listen_addr =
                    crate::tests::timeout_op("client", "wait_server_addr", 5, addr_rx.recv())
                        .await
                        .expect("Channel closed");

                let stream = crate::tests::timeout_op(
                    "client",
                    "connect",
                    5,
                    TcpStream::connect(listen_addr),
                )
                .await
                .expect("Failed to connect");
                let stream = Rc::new(stream);

                // Pre-post recv before send to avoid a race where response arrives
                // before a receive buffer is submitted in the client worker.
                let recv_stream = stream.clone();
                let recv_h = crate::runtime::context::spawn_local(async move {
                    let data = b"Hello from worker 1!";
                    let mut recv_buf = crate::runtime::context::alloc(size);
                    let mut echoed = Vec::with_capacity(data.len());
                    while echoed.len() < data.len() {
                        let (result, buf) = crate::tests::timeout_op(
                            "client",
                            "recv",
                            5,
                            recv_stream.recv(recv_buf),
                        )
                        .await;
                        recv_buf = buf;
                        let n = result.expect("Recv failed");
                        assert!(n > 0, "Peer closed before echo completed");
                        let remain = data.len() - echoed.len();
                        echoed.extend_from_slice(&recv_buf.as_slice()[..n.min(remain)]);
                    }
                    echoed
                });
                crate::runtime::context::yield_now().await;

                // Send data
                let data = b"Hello from worker 1!";
                let mut sent = 0usize;
                while sent < data.len() {
                    let remain = &data[sent..];
                    let mut send_buf = crate::runtime::context::alloc(size);
                    let chunk = remain.len().min(send_buf.capacity());
                    send_buf.spare_capacity_mut()[..chunk].copy_from_slice(&remain[..chunk]);
                    send_buf.set_len(chunk);

                    let (result, _) =
                        crate::tests::timeout_op("client", "send", 5, stream.send(send_buf)).await;
                    let n = result.expect("Send failed");
                    assert!(n > 0, "Send returned 0 before request completed");
                    sent += n;
                }

                let echoed = crate::tests::timeout_op("client", "wait_recv_join", 5, recv_h).await;

                assert_eq!(echoed.as_slice(), data);
                println!("Client received correct echo");

                done_tx.send(()).unwrap();
            });

            crate::tests::timeout_op("main", "wait_client_join", 5, client_h).await;
            crate::tests::timeout_op("main", "wait_server_join", 5, server_h).await;

            println!("Multi-thread echo test completed");
        });
}

/// Test concurrent connections from multiple workers to shared server
#[test]
fn test_multithread_concurrent_clients() {
    use std::sync::mpsc;

    crate::tests::NetworkTestRunner::new("test_multithread_concurrent_clients")
        .worker_threads(4) // 0=Server, 1,2,3=Clients
        .run(|_| async move {
            let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();
            let addr_rx = Arc::new(Mutex::new(addr_rx));
            let connection_count = Arc::new(AtomicUsize::new(0));

            const NUM_CLIENTS: usize = 3;

            let connection_count_clone = connection_count.clone();

            // Server worker (0)
            let addr_tx = addr_tx.clone();
            let server_h = crate::runtime::context::spawn_to(0, async move || {
                let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
                let listen_addr = listener.local_addr().expect("Failed to get local address");

                // Broadcast address to all clients
                for _ in 0..NUM_CLIENTS {
                    addr_tx.send(listen_addr).unwrap();
                }

                let listener_arc = Arc::new(listener);

                // Accept all connections
                for i in 0..NUM_CLIENTS {
                    let (stream, peer) =
                        crate::tests::timeout_op("server", "accept", 5, listener_arc.accept())
                            .await
                            .expect("Accept failed");
                    println!("Server accepted connection {} from {}", i, peer);
                    drop(stream);
                }
            });

            // Client workers (1..=3)
            let connection_count_clone = connection_count_clone.clone();
            let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
            for client_id in 1..=NUM_CLIENTS {
                let rx = addr_rx.clone();
                let counter = connection_count_clone.clone();

                let handle = crate::runtime::context::spawn_to(client_id, async move || {
                    let listen_addr = {
                        rx.lock()
                            .unwrap()
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .expect("Timeout waiting for server address")
                    };

                    let stream = crate::tests::timeout_op(
                        "client",
                        "connect",
                        5,
                        TcpStream::connect(listen_addr),
                    )
                    .await
                    .expect("Failed to connect");

                    println!("Client {} connected", client_id);
                    drop(stream);

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

            crate::tests::timeout_op("main", "wait_server_join", 5, server_h).await;

            assert_eq!(connection_count.load(Ordering::SeqCst), NUM_CLIENTS);
            println!("All {} clients completed", NUM_CLIENTS);
        });
}
/// Test TCP recv cancellation
#[test]
fn test_tcp_cancel_recv() {
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

    crate::tests::NetworkTestRunner::new("test_tcp_cancel_recv")
        .worker_threads(1)
        .run(|_| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            let listener_arc = Arc::new(listener);
            let listener_clone = listener_arc.clone();

            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) =
                    crate::tests::timeout_op("server", "accept", 5, listener_clone.accept())
                        .await
                        .expect("Accept failed");
                // Don't send anything, just hold connection
                // Keep stream alive until cancelled
                YieldOnce(false).await; // Yield a bit
                drop(stream);
            });

            let stream =
                crate::tests::timeout_op("client", "connect", 5, TcpStream::connect(listen_addr))
                    .await
                    .expect("Failed to connect");

            let buf = crate::runtime::context::alloc(veloq_buf::nz!(1024));

            // Use select to cancel recv
            select! {
                _ = stream.recv(buf) => {
                    panic!("Recv should have been cancelled, but it completed (unexpectedly)");
                },
                _ = YieldOnce(false) => {
                    println!("TCP recv cancelled successfully");
                }
            };

            drop(stream);
            crate::tests::timeout_op("main", "wait_server", 5, server_h).await;
        });
}

#[test]
fn test_tcp_read_exact_write_all() {
    crate::tests::NetworkTestRunner::new("test_tcp_read_exact_write_all")
        .worker_threads(1)
        .run(|_| async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
            let listen_addr = listener.local_addr().expect("Failed to get local address");

            const DATA: &[u8] = b"TCP Echo World!";
            use crate::io::{AsyncBufRead, AsyncBufWrite};
            let server_h = crate::runtime::context::spawn(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                let mut read_buf = crate::runtime::context::alloc(veloq_buf::nz!(DATA.len()));
                read_buf.set_len(DATA.len());
                
                let (res, buf) = stream.read_exact(read_buf).await;
                res.expect("Server read_exact failed");
                assert_eq!(buf.as_slice(), DATA);

                let (res, _) = stream.write_all(buf).await;
                res.expect("Server write_all failed");
            });

            let client = TcpStream::connect(listen_addr).await.expect("Failed to connect");
            let mut write_buf = crate::runtime::context::alloc(veloq_buf::nz!(DATA.len()));
            write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);
            write_buf.set_len(DATA.len());

            let (res, _) = client.write_all(write_buf).await;
            res.expect("Client write_all failed");

            let mut read_buf = crate::runtime::context::alloc(veloq_buf::nz!(DATA.len()));
            read_buf.set_len(DATA.len());
            let (res, buf) = client.read_exact(read_buf).await;
            res.expect("Client read_exact failed");
            assert_eq!(buf.as_slice(), DATA);

            server_h.await;
        });
}
