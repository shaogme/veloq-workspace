use veloq_std::{
    future::poll_fn,
    net::{SocketAddr, TcpStream as StdTcpStream},
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::spawn,
};

use futures_util::StreamExt;

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
                let read_buf = ctx.alloc_full(nz!(DATA.len()));

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
                let mut write_buf = ctx.alloc_full(nz!(DATA.len()));
                write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);

                client
                    .write_all(write_buf)
                    .await
                    .expect("Client write_all failed");

                let read_buf = ctx.alloc_full(nz!(DATA.len()));
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
                let buf = ctx.alloc_full(nz!(1024));
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
                let buf = FixedBuf::alloc_heap_full(nz!(4096)).expect("Heap allocation failed");
                let (n, buf) = stream.recv(buf).await.expect("Server recv failed");
                assert_eq!(&buf.as_slice()[..n], b"Hello from heap!");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let data = b"Hello from heap!";
                let mut buf =
                    FixedBuf::alloc_heap(nz!(4096), data.len()).expect("Heap allocation failed");
                buf.as_slice_mut()[..data.len()].copy_from_slice(data);

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
            let data = b"Hello from worker 1!";
            s.spawn_boxed(async move {
                let listener =
                    TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
                let listen_addr = listener.local_addr().expect("Failed to get local address");
                addr_tx.send(listen_addr).unwrap();

                let (stream, _) = listener.accept().await.expect("Accept failed");
                let mut recv_buf = ctx.alloc_full(nz!(1024));
                let mut received = Vec::with_capacity(data.len());
                while received.len() < data.len() {
                    let (n, buf) = stream.recv(recv_buf).await.expect("Recv failed");
                    recv_buf = buf;
                    assert!(n > 0, "Peer closed before sending full request");
                    let remain = data.len() - received.len();
                    received.extend_from_slice(&recv_buf.as_slice()[..n.min(remain)]);
                }
                assert_eq!(received.as_slice(), data);

                let mut sent = 0usize;
                while sent < data.len() {
                    let remain = &data[sent..];
                    let chunk = remain.len().min(1024);
                    let mut echo_buf = ctx.alloc(nz!(1024), chunk);
                    echo_buf.spare_capacity_mut()[..chunk].copy_from_slice(&remain[..chunk]);

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
                let mut send_buf = ctx.alloc(nz!(1024), data.len());
                send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                let (sent, _) = stream.send(send_buf).await.expect("Send failed");
                assert_eq!(sent, data.len());

                let mut recv_buf = ctx.alloc_full(nz!(1024));
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
                let buf = ctx.alloc_full(nz!(1024));
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

/// 一条 `AcceptStream` 连续产出多个连接。
///
/// 在支持 multishot accept 的内核上这只对应**一次** SQE 提交；不支持时（Linux < 5.19、
/// 或 IOCP）自动走每次重新提交单发 accept 的路径。两条路径的可观测行为必须一致，所以
/// 这个测试对两者都跑得通——那正是它的价值。
#[test]
fn accept_stream_yields_every_connection() {
    const CONNECTIONS: usize = 8;

    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let mut accepted = listener.accept_multi();
                for i in 0..CONNECTIONS {
                    let item = accepted
                        .next()
                        .await
                        .unwrap_or_else(|| panic!("accept stream ended early at {i}"));
                    let (_stream, peer) = item.unwrap_or_else(|e| panic!("accept {i} failed: {e}"));
                    assert!(
                        peer.ip().is_ipv4(),
                        "multishot accept must still report a usable peer address"
                    );
                    assert_ne!(peer.port(), 0, "peer address must be fully populated");
                }
            });

            s.spawn_boxed(async move {
                for _ in 0..CONNECTIONS {
                    let stream = TcpStream::connect(ctx, listen_addr)
                        .await
                        .expect("Failed to connect");
                    drop(stream);
                }
            });
        })
        .await
        .unwrap();
    });
}

/// 丢弃一条仍在途的 `AcceptStream` 之后，listener 上的常驻 multishot 不会被静默取消或丢弃连接。
///
/// 重新开启 `accept_multi` 能够无损连贯地接管之前在该 listener 上建立的连接（第一个连接归新流）。
#[test]
fn dropping_an_accept_stream_leaves_the_listener_usable() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        // 开一条流、不取任何东西就丢弃：multishot 已经提交给内核并托管于 Socket。
        drop(listener.accept_multi());
        yield_now().await;

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                // 客户端只连接一次
                let _stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
            });

            s.spawn_boxed(async move {
                // 验证新流可以 100% 拿到第一个连接，不会因为之前的 drop 被静默关闭
                let mut accepted = listener.accept_multi();
                let item = accepted.next().await.expect("accept stream ended early");
                let (_stream, peer) =
                    item.expect("accept failed after an earlier stream was dropped");
                assert!(peer.ip().is_ipv4());
            });
        })
        .await
        .unwrap();
    });
}

/// 同一条流跑在 thread-local 形态的 listener 上。
///
/// `LocalTcpListener` 上的每个操作都走 `LocalOp`（借当前 worker 的驱动，零 `Arc`），
/// 这条流也不例外——它证明 multishot 不再绑死在 `Arc` 化的 detached 句柄上。
///
/// 客户端用一条普通 OS 线程发起连接：`LocalTcpListener` 是 `!Send`，不能 spawn 到别的
/// 任务里去；而 `connect(2)` 在 backlog 未满时不需要对端 `accept()` 就会完成，所以这里
/// 不会互等。
#[test]
fn a_local_listener_streams_connections_without_a_detached_op() {
    use veloq::net::tcp::LocalTcpListener;

    const CONNECTIONS: usize = 4;

    run_test(async |ctx| {
        let listener = LocalTcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        let clients = spawn(move || {
            for _ in 0..CONNECTIONS {
                let stream = StdTcpStream::connect(listen_addr).expect("Failed to connect");
                drop(stream);
            }
        })
        .expect("Failed to spawn client thread");

        let mut accepted = listener.accept_multi();
        for i in 0..CONNECTIONS {
            let item = accepted
                .next()
                .await
                .unwrap_or_else(|| panic!("accept stream ended early at {i}"));
            let (_stream, peer) = item.unwrap_or_else(|e| panic!("accept {i} failed: {e}"));
            assert!(peer.ip().is_ipv4());
            assert_ne!(peer.port(), 0, "peer address must be fully populated");
        }

        clients.join().expect("client thread panicked");
    });
}

/// 用一个开了 provided buffer 的运行时跑 `f`。
///
/// 默认是关的：环按 worker 占住 `entries * buf_size` 的池内存，不该让没用到它的程序白付。
#[cfg(target_os = "linux")]
fn run_test_with_provided_buffers<F, R>(f: F) -> R
where
    F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
{
    use veloq::config::ProvidedBufConfig;

    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .with_config(|config| config.uring_provided_buffers(Some(ProvidedBufConfig::default())))
        .scope(f)
        .expect("failed to run scope")
}

/// `recv_provided` 收到的是内核挑的 buffer，调用方一个字节都不用先交出去。
///
/// 这正是 provided buffer 的收益所在：连接空闲时不占接收缓冲，数据到了才绑一个。所以这条
/// 用例特意在**连接建立之后、数据发出之前**就把 recv 挂上去。
#[cfg(target_os = "linux")]
#[test]
fn tcp_recv_provided_delivers_a_kernel_picked_buffer() {
    const ROUNDS: usize = 8;

    run_test_with_provided_buffers(async |ctx| {
        use veloq_driver_native::driver::Driver;

        // `IORING_REGISTER_PBUF_RING` 要 5.19，而仓库声明的最低内核是 5.6。
        if !ctx.driver(|driver| driver.capabilities().provided_buffers) {
            eprintln!("skipping: kernel has no provided buffer ring");
            return;
        }

        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _peer) = listener.accept().await.expect("Accept failed");
                for round in 0..ROUNDS {
                    let buf = stream
                        .recv_provided()
                        .await
                        .unwrap_or_else(|e| panic!("recv_provided round {round} failed: {e}"));
                    assert_eq!(
                        buf.as_slice(),
                        format!("round-{round}").as_bytes(),
                        "round {round} content"
                    );
                }
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                for round in 0..ROUNDS {
                    let payload = format!("round-{round}");
                    let mut buf = ctx.alloc(nz!(64), payload.len());
                    buf.as_slice_mut()[..payload.len()].copy_from_slice(payload.as_bytes());
                    let (written, _buf) = stream.send(buf).await.expect("send failed");
                    assert_eq!(written, payload.len());
                    // 一次一条，别让内核把两轮的数据合成一个 buffer 交上来。
                    yield_now().await;
                }
            });
        })
        .await
        .unwrap();
    });
}

/// 一条 `RecvStream` 连续产出每一段数据，对端关闭时正常结束。
///
/// 在 6.0+ 的内核上这只对应**一次** SQE 提交；5.19–5.x 上自动走每次重新提交单发
/// `RecvProvided` 的路径。两条路径的可观测行为必须一致，所以这个测试对两者都跑得通——那正
/// 是它的价值。
///
/// 「对端关闭 → 流结束」是这里最容易写错的一条：`recv` 读到 0 字节在流的语义里只有一种意
/// 思，所以它是 `None` 而不是一个空 buffer。
///
/// 客户端使用库自带的 [`TcpStream`]：驱动在反注册/关闭套接字时会确保发出 TCP FIN 报文，
/// 即使同一 ring 的固定文件表上有在途 multishot，对端也能立即收到 FIN 并正常优雅关闭。
#[cfg(target_os = "linux")]
#[test]
fn recv_multi_streams_every_chunk_until_the_peer_closes() {
    const ROUNDS: usize = 8;

    run_test_with_provided_buffers(async |ctx| {
        use veloq_driver_native::driver::Driver;

        if !ctx.driver(|driver| driver.capabilities().provided_buffers) {
            eprintln!("skipping: kernel has no provided buffer ring");
            return;
        }

        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let client = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                for round in 0..ROUNDS {
                    let text = format!("round-{round}");
                    let mut buf = ctx.alloc(nz!(256), text.len());
                    buf.as_slice_mut()[..text.len()].copy_from_slice(text.as_bytes());
                    client.write_all(buf).await.expect("write failed");
                    yield_now().await;
                }
                drop(client);
            });

            s.spawn_boxed(async move {
                let (stream, _peer) = listener.accept().await.expect("Accept failed");
                let mut chunks = stream.recv_multi().expect("recv_multi must be available");

                for round in 0..ROUNDS {
                    let buf = chunks
                        .next()
                        .await
                        .unwrap_or_else(|| panic!("recv stream ended early at {round}"))
                        .unwrap_or_else(|e| panic!("recv_multi round {round} failed: {e}"));
                    assert_eq!(
                        buf.as_slice(),
                        format!("round-{round}").as_bytes(),
                        "round {round} content"
                    );
                }

                // 对端关闭之后流结束，而不是无限产出空 buffer。
                assert!(
                    chunks.next().await.is_none(),
                    "the stream must end when the peer closes"
                );
                assert_eq!(chunks.rearms(), 0, "a 256-deep ring must not run dry here");
            });
        })
        .await
        .unwrap();
    });
}

/// 丢弃一条仍在途的 `RecvStream` 之后，连接还能继续用。
///
/// 覆盖的是取消路径：句柄的 `Drop` 把 slot 收进 `InFlightOrphaned`，内核随后的完成才找得到
/// slot 去跑 `orphan_cleanup`——而 multishot recv 的 orphan cleanup 要把内核挑走的 buffer
/// 还回环。漏掉的话环会一次比一次短，最后所有 recv 都 `-ENOBUFS`。
#[cfg(target_os = "linux")]
#[test]
fn dropping_a_recv_stream_leaves_the_connection_usable() {
    run_test_with_provided_buffers(async |ctx| {
        use veloq_driver_native::driver::Driver;

        if !ctx.driver(|driver| driver.capabilities().provided_buffers) {
            eprintln!("skipping: kernel has no provided buffer ring");
            return;
        }

        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _peer) = listener.accept().await.expect("Accept failed");

                // 开一条流、不取任何东西就丢弃：multishot 已经提交给内核了。
                drop(stream.recv_multi().expect("recv_multi must be available"));
                yield_now().await;

                // 验证 Socket 级常驻 multishot：即便前一条 RecvStream 被 drop，
                // 在窗口内收到的第一段数据也会被保存在在途完成表中，新流可以 100% 接收到第一段数据！
                let mut chunks = stream.recv_multi().expect("recv_multi must be available");
                let buf = chunks
                    .next()
                    .await
                    .expect("recv stream ended early")
                    .expect("recv failed after an earlier stream was dropped");
                assert_eq!(buf.as_slice(), b"first_chunk_data");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let mut buf = ctx.alloc(nz!(64), 16);
                buf.as_slice_mut()[..16].copy_from_slice(b"first_chunk_data");
                stream.send(buf).await.expect("Failed to send first chunk");
            });
        })
        .await
        .unwrap();
    });
}

/// 没开这项能力时 `recv_multi` 自动透明降级为 Single-shot Recv 模式。
#[test]
fn recv_multi_falls_back_when_provided_buffers_unavailable() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _peer) = listener.accept().await.expect("Accept failed");
                let mut chunks = stream
                    .recv_multi()
                    .expect("recv_multi must fallback and succeed");
                let polled = poll_fn(|cx| {
                    use futures_core::Stream;
                    unsafe { Pin::new_unchecked(&mut chunks) }.poll_next(cx)
                })
                .await;
                let buf = polled
                    .expect("stream item")
                    .expect("recv_multi fallback chunk failed");
                assert_eq!(buf.as_slice(), b"fallback_multi_data");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let payload = b"fallback_multi_data";
                let mut buf = ctx.alloc(nz!(64), payload.len());
                buf.as_slice_mut()[..payload.len()].copy_from_slice(payload);
                stream.send(buf).await.expect("Failed to send data");
                yield_now().await;
                drop(stream);
            });
        })
        .await
        .unwrap();
    });
}

/// 没开这项能力时 `recv_provided` 自动透明降级为 Single-shot Recv 模式。
#[test]
fn recv_provided_falls_back_when_provided_buffers_unavailable() {
    run_test(async |ctx| {
        let listener = TcpListener::bind(ctx, "127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let (stream, _peer) = listener.accept().await.expect("Accept failed");
                let buf = stream
                    .recv_provided()
                    .await
                    .expect("recv_provided must fallback and succeed");
                assert_eq!(buf.as_slice(), b"fallback_provided_data");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(ctx, listen_addr)
                    .await
                    .expect("Failed to connect");
                let payload = b"fallback_provided_data";
                let mut buf = ctx.alloc(nz!(64), payload.len());
                buf.as_slice_mut()[..payload.len()].copy_from_slice(payload);
                stream.send(buf).await.expect("Failed to send data");
                yield_now().await;
                drop(stream);
            });
        })
        .await
        .unwrap();
    });
}
