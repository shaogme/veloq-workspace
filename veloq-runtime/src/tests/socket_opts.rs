use veloq_buf::nz;

use crate::net::socket::{TcpSocket, UdpSocketBuilder};
use crate::runtime::Runtime;

use std::sync::Arc;

#[test]
fn test_tcp_socket_options() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
        // Create listener using builder
        let socket = TcpSocket::new_v4().expect("Failed to create socket");
        socket.set_nodelay(true).expect("Failed to set nodelay");
        socket
            .set_reuse_address(true)
            .expect("Failed to set reuseaddr");
        socket
            .set_recv_buffer_size(8192)
            .expect("Failed to set rcvbuf");

        socket.bind("127.0.0.1:0").expect("Failed to bind");
        let listener = socket.listen(1024).expect("Failed to listen");

        let listen_addr = listener.local_addr().expect("Failed to get local addr");
        println!("Listener bound to: {}", listen_addr);

        let listener_arc = Arc::new(listener);
        let listener_clone = listener_arc.clone();

        // Server task
        let server_h = crate::runtime::context::spawn(async move {
            let (stream, peer_addr) = listener_clone.accept().await.expect("Accept failed");
            println!("Accepted connection from: {}", peer_addr);

            // Verify we can read/write
            drop(stream);
        });

        // Client using builder
        let client_socket = TcpSocket::new_v4().expect("Failed to create client socket");
        client_socket
            .set_nodelay(true)
            .expect("Failed to set nodelay");
        client_socket
            .set_send_buffer_size(8192)
            .expect("Failed to set sndbuf");

        let stream = client_socket
            .connect(listen_addr)
            .await
            .expect("Failed to connect");
        println!("Connected successfully");
        drop(stream);

        server_h.await;
    });
}

#[test]
fn test_udp_socket_options() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
        let builder = UdpSocketBuilder::new_v4().expect("Failed to create UDP builder");
        builder
            .set_broadcast(true)
            .expect("Failed to set broadcast");
        builder
            .set_recv_buffer_size(4096)
            .expect("Failed to set rcvbuf");
        builder
            .set_reuse_address(true)
            .expect("Failed to set reuseaddr");

        let socket = Arc::new(builder.bind("127.0.0.1:0").expect("Failed to bind UDP"));
        socket
            .recv_ready(nz!(1024), 8)
            .await
            .expect("Failed to warm up UDP recv pool");
        let addr = socket.local_addr().expect("Failed to get local addr");
        println!("UDP bound to: {}", addr);

        // Basic verify it works
        let builder2 = UdpSocketBuilder::new_v4().expect("Failed to create UDP builder 2");
        let client = builder2
            .bind("127.0.0.1:0")
            .expect("Failed to bind UDP client");
        let _client_addr = client.local_addr().expect("Failed to get local addr");

        let socket_clone = socket.clone();
        let (tx, rx) = veloq_sync::oneshot::channel::<()>();
        let recv_h = crate::runtime::context::spawn(async move {
            let buf = crate::runtime::context::alloc(nz!(1024));
            let res = socket_clone.recv_stream(buf).await;
            let _ = res.expect("Failed to recv");
            let _ = tx.send(());
        });

        // Send packets in a loop in case of UDP drop
        let send_h = crate::runtime::context::spawn(async move {
            let mut rx = rx;
            loop {
                let buf = crate::runtime::context::alloc(nz!(1024));
                client.send_to(buf, addr).await.expect("Failed to send");

                let sleep = crate::time::sleep(std::time::Duration::from_millis(50));

                crate::select! {
                    _ = &mut rx => break,
                    _ = sleep => continue,
                }
            }
        });

        recv_h.await;
        send_h.await;
    });
}
