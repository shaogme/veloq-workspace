use std::num::NonZeroUsize;

use veloq::net::UdpSocket;
use veloq::runtime::{Runtime, context};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime_next::scope;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(NonZeroUsize::new(1).expect("1 is non-zero"))
        .build()
        .expect("failed to build runtime")
}

async fn bind_udp_ready(bind_addr: &str, size: NonZeroUsize, credits: usize) -> UdpSocket {
    let socket = UdpSocket::bind(bind_addr).expect("Failed to bind UDP socket");
    socket
        .recv_ready(size, credits)
        .await
        .expect("UdpSocket recv_ready warmup failed");
    socket
}

#[test]
fn udp_bind() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
        let addr = socket.local_addr().expect("Failed to get local address");

        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    });
}

#[test]
fn udp_send_receive() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let socket1 = bind_udp_ready("127.0.0.1:0", nz!(1024), 8).await;
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");

        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        let addr2 = socket2.local_addr().expect("Failed to get addr2");

        scope!(s, {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_stream(context::alloc(nz!(1024)))
                    .await
                    .expect("recv_stream failed");
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

                let (sent, _) = socket2
                    .send_to(send_buf, addr1)
                    .await
                    .expect("send_to failed");
                assert_eq!(sent, data.len());
            });
        });
    });
}

#[test]
fn udp_echo() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let server = bind_udp_ready("127.0.0.1:0", nz!(1024), 8).await;
        let client = bind_udp_ready("127.0.0.1:0", nz!(1024), 8).await;

        let server_addr = server.local_addr().expect("Failed to get server address");

        scope!(s, {
            s.spawn_boxed(async move {
                let datagram = server
                    .recv_stream(context::alloc(nz!(1024)))
                    .await
                    .expect("Server recv_stream failed");
                let from_addr = datagram.addr;
                let bytes = datagram.buf.len();
                let mut echo_buf = context::alloc(nz!(1024));
                echo_buf.spare_capacity_mut()[..bytes]
                    .copy_from_slice(&datagram.buf.as_slice()[..bytes]);
                echo_buf.set_len(bytes);
                server
                    .send_to(echo_buf, from_addr)
                    .await
                    .expect("Server send_to failed");
            });

            s.spawn_boxed(async move {
                let mut send_buf = context::alloc(nz!(1024));
                let data = b"Echo this message!";
                send_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);
                send_buf.set_len(data.len());

                client
                    .send_to(send_buf, server_addr)
                    .await
                    .expect("Client send_to failed");

                let datagram = client
                    .recv_stream(context::alloc(nz!(1024)))
                    .await
                    .expect("Client recv_stream failed");
                assert_eq!(datagram.addr, server_addr);
                assert_eq!(&datagram.buf.as_slice()[..data.len()], data);
            });
        });
    });
}

#[test]
fn udp_multiple_messages() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let socket1 = bind_udp_ready("127.0.0.1:0", nz!(1024), 8).await;
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const NUM_MESSAGES: usize = 5;

        scope!(s, {
            s.spawn_boxed(async move {
                for i in 0..NUM_MESSAGES {
                    let datagram = socket1
                        .recv_stream(context::alloc(nz!(1024)))
                        .await
                        .expect("recv_stream failed");
                    let expected = format!("Message {i}");
                    assert_eq!(
                        &datagram.buf.as_slice()[..expected.len()],
                        expected.as_bytes()
                    );
                }
            });

            s.spawn_boxed(async move {
                for i in 0..NUM_MESSAGES {
                    let mut buf = context::alloc(nz!(1024));
                    let msg = format!("Message {i}");
                    buf.spare_capacity_mut()[..msg.len()].copy_from_slice(msg.as_bytes());
                    buf.set_len(msg.len());
                    socket2.send_to(buf, addr1).await.expect("send_to failed");
                }
            });
        });
    });
}

#[test]
fn udp_large_data() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let socket1 = bind_udp_ready("127.0.0.1:0", nz!(2048), 8).await;
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");
        const DATA_SIZE: usize = 1024;

        scope!(s, {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_stream(context::alloc(nz!(2048)))
                    .await
                    .expect("recv_stream failed");
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

                let (bytes, _) = socket2.send_to(buf, addr1).await.expect("send_to failed");
                assert_eq!(bytes, DATA_SIZE);
            });
        });
    });
}

#[test]
fn udp_heap_buffer() {
    let runtime = create_runtime();
    runtime.block_on(async {
        let socket1 = bind_udp_ready("127.0.0.1:0", nz!(1024), 8).await;
        let socket2 = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket 2");
        let addr1 = socket1.local_addr().expect("Failed to get addr1");

        scope!(s, {
            s.spawn_boxed(async move {
                let datagram = socket1
                    .recv_stream(
                        veloq_buf::FixedBuf::alloc_heap(nz!(1024)).expect("Heap allocation failed"),
                    )
                    .await
                    .expect("recv_stream failed");
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

                socket2.send_to(buf, addr1).await.expect("send_to failed");
            });
        });
    });
}

#[test]
fn udp_ipv6() {
    let runtime = create_runtime();
    runtime.block_on(async {
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
fn udp_cancel_recv_stream() {
    use veloq_runtime_next::task::yield_now;

    let runtime = create_runtime();
    runtime.block_on(async {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
        let buf = context::alloc(nz!(1024));

        veloq_runtime_next::select! {
            _ = socket.recv_stream(buf) => {
                panic!("RecvStream should have been cancelled, but it completed (unexpectedly)");
            },
            _ = yield_now() => {
                println!("UDP recv cancelled successfully");
            }
        };
    });
}

#[test]
fn udp_read_exact_write_all() {
    use veloq::io::{AsyncBufRead, AsyncBufWrite};

    let runtime = create_runtime();
    runtime.block_on(async {
        let socket_server = bind_udp_ready("127.0.0.1:0", nz!(16), 4).await;
        let server_addr = socket_server
            .local_addr()
            .expect("Failed to get server address");
        let socket_client = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind client");

        scope!(s, {
            s.spawn_boxed(async move {
                let mut read_buf = context::alloc(nz!(16));
                read_buf.set_len(16);

                let (_, buf) = socket_server
                    .read_exact(read_buf)
                    .await
                    .expect("Server read_exact failed");
                assert_eq!(buf.as_slice(), b"UDP Exact World!");
            });

            s.spawn_boxed(async move {
                socket_client
                    .connect(server_addr)
                    .await
                    .expect("Client connect failed");

                let mut write_buf = context::alloc(nz!(16));
                write_buf.as_slice_mut()[..16].copy_from_slice(b"UDP Exact World!");
                write_buf.set_len(16);

                socket_client
                    .write_all(write_buf)
                    .await
                    .expect("Client write_all failed");
            });
        });
    });
}
