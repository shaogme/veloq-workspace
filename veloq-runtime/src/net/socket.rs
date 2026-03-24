use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use veloq_driver::Socket;
use veloq_driver::op::DetachedSubmitter;

use crate::net::common::InnerSocket;
use crate::net::tcp::{GenericTcpListener, TcpListener, TcpStream};
use crate::net::udp::{GenericUdpSocket, UdpSocket};

// ============================================================================
// TcpSocket
// ============================================================================

/// A builder for creating and configuring a TCP socket before binding or connecting.
///
/// This allows setting socket options like `TCP_NODELAY`, `SO_RCVBUF`, `SO_REUSEADDR`
/// before the socket is transitioned into a `TcpListener` or `TcpStream`.
pub struct TcpSocket {
    inner: Socket,
}

impl TcpSocket {
    /// Create a new IPv4 TCP socket.
    pub fn new_v4() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_tcp_v4()?,
        })
    }

    /// Create a new IPv6 TCP socket.
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_tcp_v6()?,
        })
    }

    /// Set `TCP_NODELAY` option.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_recv_buffer_size(size)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_send_buffer_size(size)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.inner.set_reuse_address(reuse)
    }

    /// Set `SO_KEEPALIVE` option.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        self.inner.set_keepalive(keepalive)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl)
    }

    /// Bind the socket to the given address.
    ///
    /// This only binds the socket. To start listening, call `listen`.
    pub fn bind<A: ToSocketAddrs>(&self, addr: A) -> io::Result<()> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;
        self.inner.bind(addr)
    }

    /// Listen for incoming connections.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpListener`.
    pub fn listen(self, backlog: u32) -> io::Result<TcpListener> {
        self.inner.listen(backlog as i32)?;
        Ok(GenericTcpListener {
            inner: InnerSocket::new(self.inner.into_owned_raw().into_raw()),
            submitter: DetachedSubmitter::new()?,
        })
    }

    /// Connect to the given address.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpStream` future.
    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        let inner = InnerSocket::new(self.inner.into_owned_raw().into_raw());
        TcpStream::connect_from_inner(inner, addr).await
    }
}

// ============================================================================
// UdpSocketBuilder
// ============================================================================

/// A builder for creating and configuring a UDP socket.
pub struct UdpSocketBuilder {
    inner: Socket,
}

impl UdpSocketBuilder {
    /// Create a new IPv4 UDP socket.
    pub fn new_v4() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_udp_v4()?,
        })
    }

    /// Create a new IPv6 UDP socket.
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_udp_v6()?,
        })
    }

    /// Set `SO_BROADCAST` option.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        self.inner.set_broadcast(broadcast)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_recv_buffer_size(size)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_send_buffer_size(size)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.inner.set_reuse_address(reuse)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl)
    }

    /// Bind the socket to the given address.
    ///
    /// Consumes the builder and returns a `UdpSocket`.
    pub fn bind<A: ToSocketAddrs>(self, addr: A) -> io::Result<UdpSocket> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;
        self.inner.bind(addr)?;

        Ok(GenericUdpSocket {
            inner: InnerSocket::new(self.inner.into_owned_raw().into_raw()),
            submitter: DetachedSubmitter::new()?,
        })
    }
}
