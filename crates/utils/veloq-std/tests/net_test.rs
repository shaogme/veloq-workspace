use core::time::Duration;

use veloq_std::{
    io::{Read, Write},
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
};

#[cfg(unix)]
use veloq_std::os::unix::io::{AsFd, AsRawFd};

#[cfg(windows)]
use veloq_std::os::windows::io::{AsRawSocket, AsSocket};

#[test]
fn test_to_socket_addrs() {
    let addrs: Vec<SocketAddr> = "127.0.0.1:8080"
        .to_socket_addrs()
        .expect("should resolve IPv4 string")
        .collect();
    assert_eq!(addrs.len(), 1);
    assert_eq!(addrs[0].port(), 8080);
    assert_eq!(addrs[0].ip(), Ipv4Addr::new(127, 0, 0, 1));

    let addrs2: Vec<SocketAddr> = ("127.0.0.1", 9000)
        .to_socket_addrs()
        .expect("should resolve tuple")
        .collect();
    assert_eq!(addrs2.len(), 1);
    assert_eq!(addrs2[0].port(), 9000);

    let addrs_slice: &[SocketAddr] = &addrs;
    let from_slice: Vec<SocketAddr> = addrs_slice
        .to_socket_addrs()
        .expect("should resolve slice")
        .collect();
    assert_eq!(from_slice, addrs);

    let from_ref: Vec<SocketAddr> = (&"127.0.0.1:8080")
        .to_socket_addrs()
        .expect("should resolve &str ref")
        .collect();
    assert_eq!(from_ref, addrs);
}

#[test]
fn test_tcp_listener_and_stream_echo() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let local_addr = listener.local_addr().expect("listener local_addr");
    assert!(local_addr.port() > 0);

    let mut client = TcpStream::connect(local_addr).expect("connect tcp client");
    let (mut server, peer_addr) = listener.accept().expect("accept tcp connection");

    assert_eq!(peer_addr, client.local_addr().expect("client local_addr"));
    assert_eq!(client.peer_addr().expect("client peer_addr"), local_addr);
    assert_eq!(server.local_addr().expect("server local_addr"), local_addr);

    // Option testing
    client.set_nodelay(true).expect("set nodelay");
    assert!(client.nodelay().expect("get nodelay"));

    client.set_ttl(32).expect("set ttl");
    assert_eq!(client.ttl().expect("get ttl"), 32);

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set client read timeout");
    assert!(
        client
            .read_timeout()
            .expect("get client read timeout")
            .is_some()
    );

    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set client write timeout");
    assert!(
        client
            .write_timeout()
            .expect("get client write timeout")
            .is_some()
    );

    // Echo write and read
    let msg = b"ping from veloq tcp client";
    client.write_all(msg).expect("client write_all");

    let mut peek_buf = [0u8; 4];
    let peeked = server.peek(&mut peek_buf).expect("server peek");
    assert_eq!(peeked, 4);
    assert_eq!(&peek_buf, b"ping");

    let mut read_buf = [0u8; 26];
    server.read_exact(&mut read_buf).expect("server read_exact");
    assert_eq!(&read_buf, msg);

    server.write_all(b"pong").expect("server write_all");

    let mut reply_buf = [0u8; 4];
    client
        .read_exact(&mut reply_buf)
        .expect("client read_exact");
    assert_eq!(&reply_buf, b"pong");

    // Clone testing
    let cloned_client = client.try_clone().expect("try_clone tcp stream");
    assert_eq!(
        cloned_client.local_addr().expect("cloned local_addr"),
        client.local_addr().expect("original local_addr")
    );

    // Shutdown testing
    client.shutdown(Shutdown::Both).expect("client shutdown");
}

#[test]
fn test_tcp_nonblocking() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    listener.set_nonblocking(true).expect("set nonblocking");

    let accept_res = listener.accept();
    assert!(accept_res.is_err());
    let err = accept_res.unwrap_err();
    assert_eq!(err.kind(), veloq_std::io::ErrorKind::WouldBlock);

    listener.set_nonblocking(false).expect("unset nonblocking");
}

#[test]
fn test_udp_socket_communication() {
    let socket1 = UdpSocket::bind("127.0.0.1:0").expect("bind udp socket 1");
    let addr1 = socket1.local_addr().expect("local addr 1");

    let socket2 = UdpSocket::bind("127.0.0.1:0").expect("bind udp socket 2");
    let addr2 = socket2.local_addr().expect("local addr 2");

    socket1.set_broadcast(false).expect("set broadcast");
    assert!(!socket1.broadcast().expect("get broadcast"));

    socket1.set_ttl(48).expect("set ttl");
    assert_eq!(socket1.ttl().expect("get ttl"), 48);

    socket1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    assert!(socket1.read_timeout().expect("get read timeout").is_some());

    // send_to & recv_from
    let sent = socket1
        .send_to(b"udp request", addr2)
        .expect("send_to addr2");
    assert_eq!(sent, 11);

    let mut peek_buf = [0u8; 11];
    let (peek_len, peek_from) = socket2.peek_from(&mut peek_buf).expect("peek_from");
    assert_eq!(peek_len, 11);
    assert_eq!(&peek_buf, b"udp request");
    assert_eq!(peek_from, addr1);

    let mut recv_buf = [0u8; 11];
    let (recv_len, from_addr) = socket2.recv_from(&mut recv_buf).expect("recv_from");
    assert_eq!(recv_len, 11);
    assert_eq!(&recv_buf, b"udp request");
    assert_eq!(from_addr, addr1);

    // Connected UDP socket
    socket2.connect(addr1).expect("connect to addr1");
    assert_eq!(socket2.peer_addr().expect("peer_addr"), addr1);

    let sent2 = socket2
        .send(b"udp response")
        .expect("send on connected socket");
    assert_eq!(sent2, 12);

    let mut resp_buf = [0u8; 12];
    let (recv2_len, from2_addr) = socket1.recv_from(&mut resp_buf).expect("recv_from on s1");
    assert_eq!(recv2_len, 12);
    assert_eq!(&resp_buf, b"udp response");
    assert_eq!(from2_addr, addr2);

    // Clone test
    let cloned_s1 = socket1.try_clone().expect("try_clone udp socket");
    assert_eq!(
        cloned_s1.local_addr().expect("cloned local_addr"),
        socket1.local_addr().expect("original local_addr")
    );
}

#[cfg(unix)]
#[test]
fn test_unix_socket_fd_traits() {
    fn check_as_fd<T: AsFd>(t: &T) {
        let _ = t.as_fd();
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let fd = listener.as_raw_fd();
    assert!(fd >= 0);

    let borrowed = listener.as_fd();
    assert_eq!(borrowed.as_raw_fd(), fd);
    check_as_fd(&listener);

    let udp = UdpSocket::bind("127.0.0.1:0").expect("bind udp socket");
    assert!(udp.as_raw_fd() >= 0);
    assert_eq!(udp.as_fd().as_raw_fd(), udp.as_raw_fd());
    check_as_fd(&udp);
}

#[cfg(windows)]
#[test]
fn test_windows_socket_traits() {
    fn check_as_socket<T: AsSocket>(t: &T) {
        let _ = t.as_socket();
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let sock = listener.as_raw_socket();
    assert_ne!(sock, 0);

    let borrowed = listener.as_socket();
    assert_eq!(borrowed.as_raw_socket(), sock);
    check_as_socket(&listener);

    let udp = UdpSocket::bind("127.0.0.1:0").expect("bind udp socket");
    assert_ne!(udp.as_raw_socket(), 0);
    assert_eq!(udp.as_socket().as_raw_socket(), udp.as_raw_socket());
    check_as_socket(&udp);
}
