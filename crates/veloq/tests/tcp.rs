use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use veloq::{
    io::{AsyncBufRead, AsyncBufWrite},
    net::{TcpListener, TcpStream},
    nz,
    runtime::{Runtime, context::Ctx, scope},
    sync::mpsc,
};
use veloq_buf::{FixedBuf, UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_runtime::{select, task::yield_now};

fn run_test<F, R>(f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    run_test_with_workers(nz!(1), f)
}

fn run_test_with_workers<F, R>(worker_threads: NonZeroUsize, f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(worker_threads))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn tcp_connect_smoke() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (_stream, peer) = listener.accept().await.expect("Accept failed");
                assert!(peer.ip().is_ipv4());
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                drop(stream);
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn tcp_read_exact_write_all() {
    const DATA: &[u8] = b"TCP Echo World!";
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                let mut read_buf = ctx.alloc(nz!(DATA.len()));
                read_buf.set_len(DATA.len());

                let (_, buf) = stream
                    .read_exact(read_buf)
                    .await
                    .expect("Server read_exact failed");
                assert_eq!(buf.as_slice(), DATA);

                stream
                    .write_all(buf)
                    .await
                    .expect("Server write_all failed");
            });

            s.spawn_boxed(async move {
                let client = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let mut write_buf = ctx.alloc(nz!(DATA.len()));
                write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);
                write_buf.set_len(DATA.len());

                client
                    .write_all(write_buf)
                    .await
                    .expect("Client write_all failed");

                let mut read_buf = ctx.alloc(nz!(DATA.len()));
                read_buf.set_len(DATA.len());
                let (_, buf) = client
                    .read_exact(read_buf)
                    .await
                    .expect("Client read_exact failed");
                assert_eq!(buf.as_slice(), DATA);
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn tcp_listener_local_addr() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener.local_addr().expect("Failed to get local address");

        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    });
}

#[test]
fn tcp_connect_refused() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener
            .local_addr()
            .expect("Failed to get listener address");
        drop(listener);

        let result = TcpStream::connect(ctx, addr).await;
        assert!(result.is_err());
    });
}

#[test]
fn tcp_ipv6() {
    run_test(async |ctx| {
        let listener_result = TcpListener::bind(ctx, "::1:0");
        if listener_result.is_err() {
            return;
        }

        let listener = listener_result.expect("IPv6 listener bind unexpectedly failed");
        let listen_addr = listener.local_addr().expect("Failed to get local address");
        assert!(listen_addr.is_ipv6());

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (_stream, peer) = listener.accept().await.expect("Accept failed");
                assert!(peer.is_ipv6());
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect via IPv6");
                drop(stream);
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn tcp_recv_zero_bytes() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                drop(stream);
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let buf = ctx.alloc(nz!(1024));
                let result = stream.recv(buf).await;
                match result {
                    Ok((bytes, _buf)) => {
                        assert_eq!(bytes, 0, "Should receive 0 bytes on closed connection");
                    }
                    Err(_e) => {}
                }
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn tcp_heap_buffer() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                let buf = FixedBuf::alloc_heap(nz!(4096)).expect("Heap allocation failed");
                let (n, buf) = stream.recv(buf).await.expect("Server recv failed");
                assert_eq!(&buf.as_slice()[..n], b"Hello from heap!");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let mut buf = FixedBuf::alloc_heap(nz!(4096)).expect("Heap allocation failed");
                let data = b"Hello from heap!";
                buf.as_slice_mut()[..data.len()].copy_from_slice(data);
                buf.set_len(data.len());

                stream.send(buf).await.expect("Client send failed");
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn tcp_multiple_connections() {
    run_test(async |ctx| {
        const NUM_CONNECTIONS: usize = 5;
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                for i in 0..NUM_CONNECTIONS {
                    let (_stream, peer) = listener.accept().await.expect("Accept failed");
                    println!("Accepted connection {} from {}", i, peer);
                }
            });

            s.spawn_boxed(async move {
                for i in 0..NUM_CONNECTIONS {
                    let stream = TcpStream::connect(ctx, listen_addr)
                        .await
                        .expect("Failed to connect");
                    println!("Client {} connected", i);
                    drop(stream);
                }
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn multithread_tcp_connections() {
    run_test_with_workers(nz!(3), async |ctx| {
        const NUM_WORKERS: usize = 3;
        let connection_count = Arc::new(AtomicUsize::new(0));

        scope!(ctx, async |s| {
            for worker_id in 0..NUM_WORKERS {
                let counter = connection_count.clone();
                let (addr_tx, mut addr_rx) = mpsc::owned_unbounded::<SocketAddr>();

                s.spawn_boxed(async move {
                    let listener =
                        TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
                    let listen_addr = listener.local_addr().expect("Failed to get local address");
                    addr_tx.send(listen_addr).unwrap();

                    let (_stream, peer) = listener.accept().await.expect("Accept failed");
                    println!("Worker {} accepted from {}", worker_id, peer);
                    counter.fetch_add(1, Ordering::SeqCst);
                });

                s.spawn_boxed(async move {
                    let listen_addr = addr_rx.recv().await.expect("Channel closed");
                    let stream = TcpStream::connect(ctx, listen_addr)
                        .await
                        .expect("Failed to connect");
                    println!("Worker {} connected to self", worker_id);
                    drop(stream);
                });
            }
        })
        .await
        .unwrap();

        assert_eq!(connection_count.load(Ordering::SeqCst), NUM_WORKERS);
    });
}

#[test]
fn multithread_tcp_echo() {
    run_test_with_workers(nz!(2), async |ctx| {
        let state = mpsc::unbounded::<SocketAddr>();
        let (addr_tx, mut addr_rx) = state.split();
        let state = mpsc::unbounded::<()>();
        let (done_tx, mut done_rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let listener =
                    TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
                let listen_addr = listener.local_addr().expect("Failed to get local address");
                addr_tx.send(listen_addr).unwrap();

                let (stream, _) = listener.accept().await.expect("Accept failed");
                let expect = b"Hello from worker 1!";
                let mut recv_buf = ctx.alloc(nz!(1024));
                let mut received = Vec::with_capacity(expect.len());
                while received.len() < expect.len() {
                    let (n, buf) = stream.recv(recv_buf).await.expect("Recv failed");
                    recv_buf = buf;
                    assert!(n > 0, "Peer closed before sending full request");
                    let remain = expect.len() - received.len();
                    received.extend_from_slice(&recv_buf.as_slice()[..n.min(remain)]);
                }
                assert_eq!(received.as_slice(), expect);

                let mut sent = 0usize;
                while sent < expect.len() {
                    let remain = &expect[sent..];
                    let mut echo_buf = ctx.alloc(nz!(1024));
                    let chunk = remain.len().min(echo_buf.capacity());
                    echo_buf.spare_capacity_mut()[..chunk].copy_from_slice(&remain[..chunk]);
                    echo_buf.set_len(chunk);

                    let (n, _) = stream.send(echo_buf).await.expect("Send failed");
                    assert!(n > 0, "Send returned 0 before echo completed");
                    sent += n;
                }

                done_rx.recv().await.expect("Client done channel closed");
            });

            s.spawn_boxed(async move {
                let listen_addr = addr_rx.recv().await.expect("Channel closed");

                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let data = b"Hello from worker 1!";
                let mut send_buf = ctx.alloc(nz!(1024));
                send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                send_buf.set_len(data.len());

                let (sent, _) = stream.send(send_buf).await.expect("Send failed");
                assert_eq!(sent, data.len());

                let mut recv_buf = ctx.alloc(nz!(1024));
                let mut echoed = Vec::with_capacity(data.len());
                while echoed.len() < data.len() {
                    let (n, buf) = stream.recv(recv_buf).await.expect("Recv failed");
                    recv_buf = buf;
                    assert!(n > 0, "Peer closed before echo completed");
                    let remain = data.len() - echoed.len();
                    echoed.extend_from_slice(&recv_buf.as_slice()[..n.min(remain)]);
                }
                assert_eq!(echoed.as_slice(), data);

                done_tx.send(()).unwrap();
            });
        })
        .await
        .unwrap();
    });
}

#[test]
fn multithread_concurrent_tcp_clients() {
    run_test_with_workers(nz!(4), async |ctx| {
        const NUM_CLIENTS: usize = 3;
        let connection_count = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            let connection_count = connection_count.clone();
            let server_h = s.spawn_boxed(async move {
                for i in 0..NUM_CLIENTS {
                    let (_stream, peer) = listener.accept().await.expect("Accept failed");
                    println!("Server accepted connection {} from {}", i, peer);
                    connection_count.fetch_add(1, Ordering::SeqCst);
                }
            });

            let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
            for client_id in 0..NUM_CLIENTS {
                client_handles.push(s.spawn_boxed(async move {
                    let stream = TcpStream::connect(ctx, listen_addr)
                        .await
                        .expect("Failed to connect");
                    println!("Client {} connected", client_id);
                    drop(stream);
                }));
            }

            for handle in client_handles {
                handle.await.expect("client task failed");
            }
            server_h.await.expect("server task failed");
        })
        .await
        .unwrap();

        assert_eq!(connection_count.load(Ordering::SeqCst), NUM_CLIENTS);
    });
}

#[test]
fn tcp_cancel_recv() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (_stream, _) = listener.accept().await.expect("Accept failed");
                yield_now().await;
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let buf = ctx.alloc(nz!(1024));
                select! {
                    ctx;
                    biased;
                    _ = stream.recv(buf) => {
                        panic!("Recv should have been cancelled, but it completed (unexpectedly)");
                    },
                    _ = yield_now() => {
                        println!("TCP recv cancelled successfully");
                    }
                };
            });
        })
        .await
        .unwrap();
    });
}
