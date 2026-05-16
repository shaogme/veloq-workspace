use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error};
use crate::runtime::context::RuntimeContext;
use veloq_driver_native::Socket;
use veloq_driver_native::op::DetachedSubmitter;

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
    pub fn new_v4() -> VeloqResult<Self> {
        Ok(Self {
            inner: Socket::new_tcp_v4().map_err(from_driver_report)?,
        })
    }

    /// Create a new IPv6 TCP socket.
    pub fn new_v6() -> VeloqResult<Self> {
        Ok(Self {
            inner: Socket::new_tcp_v6().map_err(from_driver_report)?,
        })
    }

    /// Set `TCP_NODELAY` option.
    pub fn set_nodelay(&self, nodelay: bool) -> VeloqResult<()> {
        self.inner.set_nodelay(nodelay).map_err(from_driver_report)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> VeloqResult<()> {
        self.inner
            .set_recv_buffer_size(size)
            .map_err(from_driver_report)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> VeloqResult<()> {
        self.inner
            .set_send_buffer_size(size)
            .map_err(from_driver_report)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> VeloqResult<()> {
        self.inner
            .set_reuse_address(reuse)
            .map_err(from_driver_report)
    }

    /// Set `SO_KEEPALIVE` option.
    pub fn set_keepalive(&self, keepalive: bool) -> VeloqResult<()> {
        self.inner
            .set_keepalive(keepalive)
            .map_err(from_driver_report)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> VeloqResult<()> {
        self.inner.set_ttl(ttl).map_err(from_driver_report)
    }

    /// Bind the socket to the given address.
    ///
    /// This only binds the socket. To start listening, call `listen`.
    pub fn bind<A: ToSocketAddrs>(&self, addr: A) -> VeloqResult<()> {
        let addr = addr
            .to_socket_addrs()
            .map_err(from_io_error)?
            .next()
            .ok_or_else(|| {
                from_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No address provided",
                ))
            })?;
        self.inner.bind(addr).map_err(from_driver_report)
    }

    /// Listen for incoming connections.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpListener`.
    pub fn listen<'a, 'ctx>(
        self,
        ctx: RuntimeContext<'a, 'ctx>,
        backlog: u32,
    ) -> VeloqResult<TcpListener<'a, 'ctx>> {
        let local_addr = self.inner.local_addr().map_err(from_driver_report)?;
        self.inner
            .listen(backlog as i32)
            .map_err(from_driver_report)?;
        Ok(GenericTcpListener {
            inner: InnerSocket::new(
                ctx,
                self.inner.into_owned_raw().into_raw(),
                Some(local_addr),
            )?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    /// Connect to the given address.
    ///
    /// Consumes the `TcpSocket` and returns a `TcpStream` future.
    pub async fn connect<'a, 'ctx>(
        self,
        ctx: RuntimeContext<'a, 'ctx>,
        addr: SocketAddr,
    ) -> VeloqResult<TcpStream<'a, 'ctx>> {
        let inner = InnerSocket::new(ctx, self.inner.into_owned_raw().into_raw(), None)?;
        TcpStream::connect_from_inner(ctx, inner, addr).await
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
    pub fn new_v4() -> VeloqResult<Self> {
        Ok(Self {
            inner: Socket::new_udp_v4().map_err(from_driver_report)?,
        })
    }

    /// Create a new IPv6 UDP socket.
    pub fn new_v6() -> VeloqResult<Self> {
        Ok(Self {
            inner: Socket::new_udp_v6().map_err(from_driver_report)?,
        })
    }

    /// Set `SO_BROADCAST` option.
    pub fn set_broadcast(&self, broadcast: bool) -> VeloqResult<()> {
        self.inner
            .set_broadcast(broadcast)
            .map_err(from_driver_report)
    }

    /// Set `SO_RCVBUF` option.
    pub fn set_recv_buffer_size(&self, size: usize) -> VeloqResult<()> {
        self.inner
            .set_recv_buffer_size(size)
            .map_err(from_driver_report)
    }

    /// Set `SO_SNDBUF` option.
    pub fn set_send_buffer_size(&self, size: usize) -> VeloqResult<()> {
        self.inner
            .set_send_buffer_size(size)
            .map_err(from_driver_report)
    }

    /// Set `SO_REUSEADDR` option.
    pub fn set_reuse_address(&self, reuse: bool) -> VeloqResult<()> {
        self.inner
            .set_reuse_address(reuse)
            .map_err(from_driver_report)
    }

    /// Set `IP_TTL` option.
    pub fn set_ttl(&self, ttl: u32) -> VeloqResult<()> {
        self.inner.set_ttl(ttl).map_err(from_driver_report)
    }

    /// Bind the socket to the given address.
    ///
    /// Consumes the builder and returns a `UdpSocket`.
    pub fn bind<'a, 'ctx, A: ToSocketAddrs>(
        self,
        ctx: RuntimeContext<'a, 'ctx>,
        addr: A,
    ) -> VeloqResult<UdpSocket<'a, 'ctx>> {
        let addr = addr
            .to_socket_addrs()
            .map_err(from_io_error)?
            .next()
            .ok_or_else(|| {
                from_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No address provided",
                ))
            })?;
        self.inner.bind(addr).map_err(from_driver_report)?;
        let local_addr = self.inner.local_addr().map_err(from_driver_report)?;

        Ok(GenericUdpSocket {
            inner: InnerSocket::new(
                ctx,
                self.inner.into_owned_raw().into_raw(),
                Some(local_addr),
            )?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }
}
