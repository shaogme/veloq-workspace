use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use error_stack::Report;
use veloq_driver::Socket;
use veloq_driver::op::DetachedSubmitter;

use crate::net::common::InnerSocket;
use crate::net::tcp::{GenericTcpListener, TcpListener, TcpStream};
use crate::net::udp::{GenericUdpSocket, UdpSocket};

#[inline]
fn driver_err<E>(err: Report<E>) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::other(err.to_string())
}

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
            inner: Socket::new_tcp_v4().map_err(driver_err)?,
        })
    }

    /// Create a new IPv6 TCP socket.
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_tcp_v6().map_err(driver_err)?,
        })
    }

    /// Set `TCP_NODELAY` option.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay).map_err(driver_err)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_recv_buffer_size(size).map_err(driver_err)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_send_buffer_size(size).map_err(driver_err)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.inner.set_reuse_address(reuse).map_err(driver_err)
    }

    /// Set `SO_KEEPALIVE` option.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        self.inner.set_keepalive(keepalive).map_err(driver_err)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl).map_err(driver_err)
    }

    /// Bind the socket to the given address.
    ///
    /// This only binds the socket. To start listening, call `listen`.
    pub fn bind<A: ToSocketAddrs>(&self, addr: A) -> io::Result<()> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;
        self.inner.bind(addr).map_err(driver_err)
    }

    /// Listen for incoming connections.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpListener`.
    pub fn listen(self, backlog: u32) -> io::Result<TcpListener> {
        let local_addr = self.inner.local_addr().map_err(driver_err)?;
        self.inner.listen(backlog as i32).map_err(driver_err)?;
        Ok(GenericTcpListener {
            inner: InnerSocket::new(self.inner.into_owned_raw().into_raw(), Some(local_addr))?,
            submitter: DetachedSubmitter::new(),
        })
    }

    /// Connect to the given address.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpStream` future.
    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        let inner = InnerSocket::new(self.inner.into_owned_raw().into_raw(), None)?;
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
            inner: Socket::new_udp_v4().map_err(driver_err)?,
        })
    }

    /// Create a new IPv6 UDP socket.
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            inner: Socket::new_udp_v6().map_err(driver_err)?,
        })
    }

    /// Set `SO_BROADCAST` option.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        self.inner.set_broadcast(broadcast).map_err(driver_err)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_recv_buffer_size(size).map_err(driver_err)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.inner.set_send_buffer_size(size).map_err(driver_err)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.inner.set_reuse_address(reuse).map_err(driver_err)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl).map_err(driver_err)
    }

    /// Bind the socket to the given address.
    ///
    /// Consumes the builder and returns a `UdpSocket`.
    pub fn bind<A: ToSocketAddrs>(self, addr: A) -> io::Result<UdpSocket> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;
        self.inner.bind(addr).map_err(driver_err)?;
        let local_addr = self.inner.local_addr().map_err(driver_err)?;

        Ok(GenericUdpSocket {
            inner: InnerSocket::new(self.inner.into_owned_raw().into_raw(), Some(local_addr))?,
            submitter: DetachedSubmitter::new(),
        })
    }
}
