use std::num::NonZeroUsize;

use veloq::net::{TcpListener, TcpStream};
use veloq::runtime::{Runtime, context};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime_next::scope;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(NonZeroUsize::new(1).expect("1 is non-zero"))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn tcp_connect_smoke() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(s, {
            s.spawn_boxed(async move {
                let (_stream, peer) = listener.accept().await.expect("Accept failed");
                assert!(peer.ip().is_ipv4());
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(listen_addr)
                    .await
                    .expect("Failed to connect");
                drop(stream);
            });
        });
    });
}

#[test]
fn tcp_read_exact_write_all() {
    use veloq::io::{AsyncBufRead, AsyncBufWrite};

    const DATA: &[u8] = b"TCP Echo World!";
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(s, {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                let mut read_buf = context::alloc(nz!(DATA.len()));
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
                let client = TcpStream::connect(listen_addr)
                    .await
                    .expect("Failed to connect");
                let mut write_buf = context::alloc(nz!(DATA.len()));
                write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);
                write_buf.set_len(DATA.len());

                client
                    .write_all(write_buf)
                    .await
                    .expect("Client write_all failed");

                let mut read_buf = context::alloc(nz!(DATA.len()));
                read_buf.set_len(DATA.len());
                let (_, buf) = client
                    .read_exact(read_buf)
                    .await
                    .expect("Client read_exact failed");
                assert_eq!(buf.as_slice(), DATA);
            });
        });
    });
}

#[test]
fn tcp_listener_local_addr() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener.local_addr().expect("Failed to get local address");

        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    });
}

#[test]
fn tcp_connect_refused() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener
            .local_addr()
            .expect("Failed to get listener address");
        drop(listener);

        let result = TcpStream::connect(addr).await;
        assert!(result.is_err());
    });
}

#[test]
fn tcp_ipv6() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener_result = TcpListener::bind("::1:0");
        if listener_result.is_err() {
            return;
        }

        let listener = listener_result.expect("IPv6 listener bind unexpectedly failed");
        let listen_addr = listener.local_addr().expect("Failed to get local address");
        assert!(listen_addr.is_ipv6());

        scope!(s, {
            s.spawn_boxed(async move {
                let (_stream, peer) = listener.accept().await.expect("Accept failed");
                assert!(peer.is_ipv6());
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(listen_addr)
                    .await
                    .expect("Failed to connect via IPv6");
                drop(stream);
            });
        });
    });
}

#[test]
fn tcp_recv_zero_bytes() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(s, {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                drop(stream);
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(listen_addr)
                    .await
                    .expect("Failed to connect");
                let buf = context::alloc(nz!(1024));
                let result = stream.recv(buf).await;
                match result {
                    Ok((bytes, _buf)) => {
                        assert_eq!(bytes, 0, "Should receive 0 bytes on closed connection");
                    }
                    Err(_e) => {}
                }
            });
        });
    });
}

#[test]
fn tcp_heap_buffer() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let listen_addr = listener.local_addr().expect("Failed to get local address");

        scope!(s, {
            s.spawn_boxed(async move {
                let (stream, _) = listener.accept().await.expect("Accept failed");
                let buf =
                    veloq_buf::FixedBuf::alloc_heap(nz!(4096)).expect("Heap allocation failed");
                let (n, buf) = stream.recv(buf).await.expect("Server recv failed");
                assert_eq!(&buf.as_slice()[..n], b"Hello from heap!");
            });

            s.spawn_boxed(async move {
                let stream = TcpStream::connect(listen_addr)
                    .await
                    .expect("Failed to connect");
                let mut buf =
                    veloq_buf::FixedBuf::alloc_heap(nz!(4096)).expect("Heap allocation failed");
                let data = b"Hello from heap!";
                buf.as_slice_mut()[..data.len()].copy_from_slice(data);
                buf.set_len(data.len());

                stream.send(buf).await.expect("Client send failed");
            });
        });
    });
}
