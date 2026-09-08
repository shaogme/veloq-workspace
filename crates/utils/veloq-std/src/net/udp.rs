use core::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use crate::{
    const_io_error,
    io::{Error, ErrorKind, Result},
    net::{
        addr::{ToSocketAddrs, each_addr},
        sys,
    },
};

#[cfg(unix)]
use crate::os::unix::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

#[cfg(windows)]
use crate::os::windows::io::{
    AsRawSocket, AsSocket, BorrowedSocket, FromRawSocket, IntoRawSocket, OwnedSocket, RawSocket,
};

/// A UDP socket.
pub struct UdpSocket {
    pub(crate) inner: sys::Socket,
}

impl UdpSocket {
    /// Creates a UDP socket from the given address.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        each_addr(addr, |addr| {
            let sock = sys::Socket::new_udp(addr)?;
            sock.bind(addr)?;
            Ok(Self { inner: sock })
        })
    }

    /// Receives a single datagram message on the socket. On success, returns the number
    /// of bytes read and the origin.
    #[inline]
    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf)
    }

    /// Receives a single datagram message on the socket, without removing it from the
    /// queue. On success, returns the number of bytes read and the origin.
    #[inline]
    pub fn peek_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        self.inner.peek_from(buf)
    }

    /// Sends data on the socket to the given address. On success, returns the
    /// number of bytes written.
    pub fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], addr: A) -> Result<usize> {
        match addr.to_socket_addrs()?.next() {
            Some(addr) => self.inner.send_to(buf, &addr),
            None => Err(const_io_error!(
                ErrorKind::InvalidInput,
                "no addresses to send data to"
            )),
        }
    }

    /// Returns the socket address of the remote peer this socket was connected to.
    #[inline]
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Returns the local socket address of this socket.
    #[inline]
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Creates a new independently owned handle to the underlying socket.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.inner.try_clone().map(|inner| Self { inner })
    }

    /// Sets the read timeout to the timeout specified.
    #[inline]
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.inner.set_read_timeout(dur)
    }

    /// Sets the write timeout to the timeout specified.
    #[inline]
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.inner.set_write_timeout(dur)
    }

    /// Returns the read timeout of this socket.
    #[inline]
    pub fn read_timeout(&self) -> Result<Option<Duration>> {
        self.inner.read_timeout()
    }

    /// Returns the write timeout of this socket.
    #[inline]
    pub fn write_timeout(&self) -> Result<Option<Duration>> {
        self.inner.write_timeout()
    }

    /// Sets the value of the `SO_BROADCAST` option for this socket.
    #[inline]
    pub fn set_broadcast(&self, broadcast: bool) -> Result<()> {
        self.inner.set_broadcast(broadcast)
    }

    /// Gets the value of the `SO_BROADCAST` option for this socket.
    #[inline]
    pub fn broadcast(&self) -> Result<bool> {
        self.inner.broadcast()
    }

    /// Sets the value of the `IP_MULTICAST_LOOP` option for this socket.
    #[inline]
    pub fn set_multicast_loop_v4(&self, multicast_loop_v4: bool) -> Result<()> {
        self.inner.set_multicast_loop_v4(multicast_loop_v4)
    }

    /// Gets the value of the `IP_MULTICAST_LOOP` option for this socket.
    #[inline]
    pub fn multicast_loop_v4(&self) -> Result<bool> {
        self.inner.multicast_loop_v4()
    }

    /// Sets the value of the `IP_MULTICAST_TTL` option for this socket.
    #[inline]
    pub fn set_multicast_ttl_v4(&self, multicast_ttl_v4: u32) -> Result<()> {
        self.inner.set_multicast_ttl_v4(multicast_ttl_v4)
    }

    /// Gets the value of the `IP_MULTICAST_TTL` option for this socket.
    #[inline]
    pub fn multicast_ttl_v4(&self) -> Result<u32> {
        self.inner.multicast_ttl_v4()
    }

    /// Sets the value of the `IPV6_MULTICAST_LOOP` option for this socket.
    #[inline]
    pub fn set_multicast_loop_v6(&self, multicast_loop_v6: bool) -> Result<()> {
        self.inner.set_multicast_loop_v6(multicast_loop_v6)
    }

    /// Gets the value of the `IPV6_MULTICAST_LOOP` option for this socket.
    #[inline]
    pub fn multicast_loop_v6(&self) -> Result<bool> {
        self.inner.multicast_loop_v6()
    }

    /// Executes an operation of the `IP_ADD_MEMBERSHIP` type.
    #[inline]
    pub fn join_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        self.inner.join_multicast_v4(multiaddr, interface)
    }

    /// Executes an operation of the `IPV6_ADD_MEMBERSHIP` type.
    #[inline]
    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        self.inner.join_multicast_v6(multiaddr, interface)
    }

    /// Executes an operation of the `IP_DROP_MEMBERSHIP` type.
    #[inline]
    pub fn leave_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        self.inner.leave_multicast_v4(multiaddr, interface)
    }

    /// Executes an operation of the `IPV6_DROP_MEMBERSHIP` type.
    #[inline]
    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        self.inner.leave_multicast_v6(multiaddr, interface)
    }

    /// Sets the value for the `IP_TTL` option on this socket.
    #[inline]
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        self.inner.set_ttl(ttl)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    #[inline]
    pub fn ttl(&self) -> Result<u32> {
        self.inner.ttl()
    }

    /// Gets the value of the `SO_ERROR` option on this socket.
    #[inline]
    pub fn take_error(&self) -> Result<Option<Error>> {
        self.inner.take_error()
    }

    /// Connects this UDP socket to a remote address, allowing the `send` and
    /// `recv` syscalls to be used to send and receive data.
    pub fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<()> {
        each_addr(addr, |addr| self.inner.connect(addr))
    }

    /// Sends data on the socket to the remote address to which it is connected.
    #[inline]
    pub fn send(&self, buf: &[u8]) -> Result<usize> {
        self.inner.send(buf)
    }

    /// Receives a single datagram message on the socket from the remote address to
    /// which it is connected.
    #[inline]
    pub fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        self.inner.recv(buf)
    }

    /// Receives a single datagram message on the socket, without removing it from
    /// the queue.
    #[inline]
    pub fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        self.inner.peek(buf)
    }

    /// Moves this socket into or out of nonblocking mode.
    #[inline]
    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.inner.set_nonblocking(nonblocking)
    }

    #[cfg(unix)]
    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    #[cfg(unix)]
    #[inline]
    pub fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }

    /// Constructs a new instance of `Self` from the given raw file descriptor.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `fd` must be open and suitable for assuming
    /// ownership. The caller must ensure that no other code owns `fd`.
    #[cfg(unix)]
    #[inline]
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            inner: unsafe { sys::Socket::from_raw_fd(fd) },
        }
    }

    #[cfg(unix)]
    #[inline]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }

    #[cfg(unix)]
    #[inline]
    pub fn from_inner(fd: OwnedFd) -> Self {
        Self {
            inner: sys::Socket::from_inner(fd),
        }
    }

    #[cfg(unix)]
    #[inline]
    pub fn into_inner(self) -> OwnedFd {
        self.inner.into_inner()
    }

    #[cfg(windows)]
    #[inline]
    pub fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }

    #[cfg(windows)]
    #[inline]
    pub fn into_raw_socket(self) -> RawSocket {
        self.inner.into_raw_socket()
    }

    /// Constructs a new instance of `Self` from the given raw socket.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `sock` must be open and suitable for assuming
    /// ownership. The caller must ensure that no other code owns `sock`.
    #[cfg(windows)]
    #[inline]
    pub unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        Self {
            inner: unsafe { sys::Socket::from_raw_socket(sock) },
        }
    }

    #[cfg(windows)]
    #[inline]
    pub fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }

    #[cfg(windows)]
    #[inline]
    pub fn from_inner(sock: OwnedSocket) -> Self {
        Self {
            inner: sys::Socket::from_inner(sock),
        }
    }

    #[cfg(windows)]
    #[inline]
    pub fn into_inner(self) -> OwnedSocket {
        self.inner.into_inner()
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("UdpSocket");
        if let Ok(addr) = self.local_addr() {
            builder.field("local", &addr);
        }
        builder.finish()
    }
}

#[cfg(unix)]
impl AsRawFd for UdpSocket {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl FromRawFd for UdpSocket {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

#[cfg(unix)]
impl IntoRawFd for UdpSocket {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }
}

#[cfg(unix)]
impl AsFd for UdpSocket {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for UdpSocket {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from_inner(fd)
    }
}

#[cfg(unix)]
impl From<UdpSocket> for OwnedFd {
    #[inline]
    fn from(s: UdpSocket) -> Self {
        s.into_inner()
    }
}

#[cfg(windows)]
impl AsRawSocket for UdpSocket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

#[cfg(windows)]
impl FromRawSocket for UdpSocket {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { Self::from_raw_socket(sock) }
    }
}

#[cfg(windows)]
impl IntoRawSocket for UdpSocket {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        self.inner.into_raw_socket()
    }
}

#[cfg(windows)]
impl AsSocket for UdpSocket {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

#[cfg(windows)]
impl From<OwnedSocket> for UdpSocket {
    #[inline]
    fn from(sock: OwnedSocket) -> Self {
        Self::from_inner(sock)
    }
}

#[cfg(windows)]
impl From<UdpSocket> for OwnedSocket {
    #[inline]
    fn from(s: UdpSocket) -> Self {
        s.into_inner()
    }
}
