use core::{fmt, net::SocketAddr, time::Duration};

use crate::{
    io::{Error, IoSlice, IoSliceMut, Read, Result, Write},
    net::{
        Shutdown,
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

/// A TCP stream between a local and a remote socket.
pub struct TcpStream {
    pub(crate) inner: sys::Socket,
}

/// A TCP socket server, listening for incoming connections.
pub struct TcpListener {
    pub(crate) inner: sys::Socket,
}

/// An iterator that infinitely [`accept`]s connections on a [`TcpListener`].
#[derive(Debug)]
pub struct Incoming<'a> {
    listener: &'a TcpListener,
}

impl TcpStream {
    /// Opens a TCP connection to a remote host.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        each_addr(addr, |addr| {
            let sock = sys::Socket::new_tcp(addr)?;
            sock.connect(addr)?;
            Ok(Self { inner: sock })
        })
    }

    /// Opens a TCP connection to a remote host with a timeout.
    pub fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> Result<Self> {
        let sock = sys::Socket::new_tcp(addr)?;
        sock.connect_timeout(addr, timeout)?;
        Ok(Self { inner: sock })
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

    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue. On success, returns
    /// the number of bytes peeked.
    #[inline]
    pub fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        self.inner.peek(buf)
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    #[inline]
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Returns the socket address of the local half of this TCP connection.
    #[inline]
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Shuts down the read, write, or both halves of this connection.
    #[inline]
    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        self.inner.shutdown(how)
    }

    /// Creates a new independently owned handle to the underlying socket.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.inner.try_clone().map(|inner| Self { inner })
    }

    /// Sets the value of the `TCP_NODELAY` option on this socket.
    #[inline]
    pub fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        self.inner.set_nodelay(nodelay)
    }

    /// Gets the value of the `TCP_NODELAY` option on this socket.
    #[inline]
    pub fn nodelay(&self) -> Result<bool> {
        self.inner.nodelay()
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

    /// Moves this socket into or out of nonblocking mode.
    #[inline]
    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.inner.set_nonblocking(nonblocking)
    }

    /// Sets the value of the `SO_LINGER` option on this socket.
    #[inline]
    pub fn set_linger(&self, dur: Option<Duration>) -> Result<()> {
        self.inner.set_linger(dur)
    }

    /// Gets the value of the `SO_LINGER` option on this socket.
    #[inline]
    pub fn linger(&self) -> Result<Option<Duration>> {
        self.inner.linger()
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

impl Read for TcpStream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (&*self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        (&*self).read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }
}

impl Read for &TcpStream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        self.inner.read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }
}

impl Write for TcpStream {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (&*self).write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (&*self).write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (&*self).flush()
    }
}

impl Write for &TcpStream {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.inner.write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TcpStream");
        if let Ok(addr) = self.local_addr() {
            builder.field("local", &addr);
        }
        if let Ok(addr) = self.peer_addr() {
            builder.field("peer", &addr);
        }
        builder.finish()
    }
}

#[cfg(unix)]
impl AsRawFd for TcpStream {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl FromRawFd for TcpStream {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

#[cfg(unix)]
impl IntoRawFd for TcpStream {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }
}

#[cfg(unix)]
impl AsFd for TcpStream {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for TcpStream {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from_inner(fd)
    }
}

#[cfg(unix)]
impl From<TcpStream> for OwnedFd {
    #[inline]
    fn from(s: TcpStream) -> Self {
        s.into_inner()
    }
}

#[cfg(windows)]
impl AsRawSocket for TcpStream {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

#[cfg(windows)]
impl FromRawSocket for TcpStream {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { Self::from_raw_socket(sock) }
    }
}

#[cfg(windows)]
impl IntoRawSocket for TcpStream {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        self.inner.into_raw_socket()
    }
}

#[cfg(windows)]
impl AsSocket for TcpStream {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

#[cfg(windows)]
impl From<OwnedSocket> for TcpStream {
    #[inline]
    fn from(sock: OwnedSocket) -> Self {
        Self::from_inner(sock)
    }
}

#[cfg(windows)]
impl From<TcpStream> for OwnedSocket {
    #[inline]
    fn from(s: TcpStream) -> Self {
        s.into_inner()
    }
}

impl TcpListener {
    /// Creates a new `TcpListener` which will be bound to the specified
    /// address.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        each_addr(addr, |addr| {
            let sock = sys::Socket::new_tcp(addr)?;
            sock.bind(addr)?;
            sock.listen(128)?;
            Ok(Self { inner: sock })
        })
    }

    /// Accept a new incoming connection from this listener.
    pub fn accept(&self) -> Result<(TcpStream, SocketAddr)> {
        self.inner
            .accept()
            .map(|(sock, addr)| (TcpStream { inner: sock }, addr))
    }

    /// Creates a new independently owned handle to the underlying socket.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.inner.try_clone().map(|inner| Self { inner })
    }

    /// Returns the local socket address of this listener.
    #[inline]
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns an iterator over the connections being received on this
    /// listener.
    #[inline]
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming { listener: self }
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

    /// Sets the value for the `IPV6_V6ONLY` option on this socket.
    #[inline]
    pub fn set_only_v6(&self, only_v6: bool) -> Result<()> {
        self.inner.set_only_v6(only_v6)
    }

    /// Gets the value of the `IPV6_V6ONLY` option for this socket.
    #[inline]
    pub fn only_v6(&self) -> Result<bool> {
        self.inner.only_v6()
    }

    /// Gets the value of the `SO_ERROR` option on this socket.
    #[inline]
    pub fn take_error(&self) -> Result<Option<Error>> {
        self.inner.take_error()
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

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TcpListener");
        if let Ok(addr) = self.local_addr() {
            builder.field("local", &addr);
        }
        builder.finish()
    }
}

#[cfg(unix)]
impl AsRawFd for TcpListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl FromRawFd for TcpListener {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

#[cfg(unix)]
impl IntoRawFd for TcpListener {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }
}

#[cfg(unix)]
impl AsFd for TcpListener {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for TcpListener {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from_inner(fd)
    }
}

#[cfg(unix)]
impl From<TcpListener> for OwnedFd {
    #[inline]
    fn from(l: TcpListener) -> Self {
        l.into_inner()
    }
}

#[cfg(windows)]
impl AsRawSocket for TcpListener {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

#[cfg(windows)]
impl FromRawSocket for TcpListener {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { Self::from_raw_socket(sock) }
    }
}

#[cfg(windows)]
impl IntoRawSocket for TcpListener {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        self.inner.into_raw_socket()
    }
}

#[cfg(windows)]
impl AsSocket for TcpListener {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

#[cfg(windows)]
impl From<OwnedSocket> for TcpListener {
    #[inline]
    fn from(sock: OwnedSocket) -> Self {
        Self::from_inner(sock)
    }
}

#[cfg(windows)]
impl From<TcpListener> for OwnedSocket {
    #[inline]
    fn from(l: TcpListener) -> Self {
        l.into_inner()
    }
}

impl<'a> Iterator for Incoming<'a> {
    type Item = Result<TcpStream>;

    #[inline]
    fn next(&mut self) -> Option<Result<TcpStream>> {
        Some(self.listener.accept().map(|(s, _)| s))
    }
}

impl<'a> IntoIterator for &'a TcpListener {
    type Item = Result<TcpStream>;
    type IntoIter = Incoming<'a>;

    #[inline]
    fn into_iter(self) -> Incoming<'a> {
        self.incoming()
    }
}
