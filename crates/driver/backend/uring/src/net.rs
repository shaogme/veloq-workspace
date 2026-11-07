use crate::{
    OwnedRawHandle, RawHandle,
    config::{SockAddrStorage, UringRawHandle},
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use libc::{
    AF_INET, AF_INET6, IP_TTL, IPPROTO_IP, IPPROTO_TCP, SO_BROADCAST, SO_KEEPALIVE, SO_RCVBUF,
    SO_REUSEADDR, SO_SNDBUF, SOCK_CLOEXEC, SOCK_DGRAM, SOCK_NONBLOCK, SOCK_STREAM, SOL_SOCKET,
    TCP_NODELAY, bind, c_int, c_void, connect, getpeername, getsockname, listen, setsockopt,
    sockaddr, sockaddr_storage, socket, socklen_t,
};
use veloq_driver_core::PlatformSocket;
use veloq_std::{io, mem::size_of, net::SocketAddr, os::fd::RawFd};

pub struct Socket {
    fd: OwnedRawHandle,
}

impl Socket {
    fn new_v4(ty: c_int) -> UringResult<Self> {
        let fd = unsafe { socket(AF_INET, ty, 0) };
        if fd < 0 {
            return Err(UringError::Socket.io_report("socket.new_v4", io::Error::last_os_error()));
        }
        Ok(Self {
            // SAFETY: newly created socket fd is uniquely owned.
            fd: unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(fd)))
            },
        })
    }

    fn new_v6(ty: c_int) -> UringResult<Self> {
        let fd = unsafe { socket(AF_INET6, ty, 0) };
        if fd < 0 {
            return Err(UringError::Socket.io_report("socket.new_v6", io::Error::last_os_error()));
        }
        Ok(Self {
            // SAFETY: newly created socket fd is uniquely owned.
            fd: unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(fd)))
            },
        })
    }

    fn setsockopt<T>(&self, level: c_int, optname: c_int, optval: T) -> UringResult<()> {
        let ret = unsafe {
            setsockopt(
                self.fd.raw().as_fd(),
                level,
                optname,
                &optval as *const _ as *const c_void,
                size_of::<T>() as socklen_t,
            )
        };
        if ret < 0 {
            return Err(
                UringError::Socket.io_report("socket.setsockopt", io::Error::last_os_error())
            );
        }
        Ok(())
    }

    pub fn new_tcp_v4() -> UringResult<Self> {
        Self::new_v4(SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK)
    }

    pub fn new_tcp_v6() -> UringResult<Self> {
        Self::new_v6(SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK)
    }

    pub fn new_udp_v4() -> UringResult<Self> {
        Self::new_v4(SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK)
    }

    pub fn new_udp_v6() -> UringResult<Self> {
        Self::new_v6(SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK)
    }

    pub fn bind(&self, addr: SocketAddr) -> UringResult<()> {
        let (raw_addr, raw_addr_len) = SockAddrStorage::from_socket_addr(addr);
        let ret = unsafe {
            bind(
                self.fd.raw().as_fd(),
                raw_addr.as_raw() as *const _ as *const sockaddr,
                raw_addr_len,
            )
        };
        if ret < 0 {
            return Err(UringError::Socket.io_report("socket.bind", io::Error::last_os_error()));
        }
        Ok(())
    }

    pub fn connect(&self, addr: SocketAddr) -> UringResult<()> {
        let (raw_addr, raw_addr_len) = SockAddrStorage::from_socket_addr(addr);
        let ret = unsafe {
            connect(
                self.fd.raw().as_fd(),
                raw_addr.as_raw() as *const _ as *const sockaddr,
                raw_addr_len,
            )
        };
        if ret < 0 {
            return Err(UringError::Socket.io_report("socket.connect", io::Error::last_os_error()));
        }
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> UringResult<()> {
        let ret = unsafe { listen(self.fd.raw().as_fd(), backlog as c_int) };
        if ret < 0 {
            return Err(UringError::Socket.io_report("socket.listen", io::Error::last_os_error()));
        }
        Ok(())
    }

    pub fn into_owned_raw(self) -> OwnedRawHandle {
        self.fd
    }

    /// # Safety
    ///
    /// `handle` 必须是有效 fd，且满足所有权语义。
    pub unsafe fn from_raw(handle: UringRawHandle) -> Self {
        Self {
            // SAFETY: forwarded from caller contract.
            fd: unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(handle)) },
        }
    }

    pub fn local_addr(&self) -> UringResult<SocketAddr> {
        let mut storage = SockAddrStorage::default();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        let ret = unsafe {
            getsockname(
                self.fd.raw().as_fd(),
                storage.as_raw_mut() as *mut _ as *mut sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(UringError::Socket
                .io_report("socket.local_addr.getsockname", io::Error::last_os_error()));
        }
        SockAddrStorage::decode(
            storage.bounded_bytes(len as usize, "socket.local_addr.getsockname")?,
        )
        .attach_note("socket.local_addr.decode")
    }

    pub fn peer_addr(&self) -> UringResult<SocketAddr> {
        Self::peer_addr_of(self.fd.raw().as_fd())
    }

    /// Reads the peer address after an accepted descriptor is returned without an address.
    pub fn peer_addr_of_handle(handle: UringRawHandle) -> UringResult<SocketAddr> {
        Self::peer_addr_of(handle.as_fd())
    }

    fn peer_addr_of(fd: RawFd) -> UringResult<SocketAddr> {
        let mut storage = SockAddrStorage::default();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        let ret = unsafe {
            getpeername(
                fd,
                storage.as_raw_mut() as *mut _ as *mut sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(UringError::Socket
                .io_report("socket.peer_addr.getpeername", io::Error::last_os_error()));
        }
        SockAddrStorage::decode(
            storage.bounded_bytes(len as usize, "socket.peer_addr.getpeername")?,
        )
        .attach_note("socket.peer_addr.decode")
    }

    pub fn set_nodelay(&self, nodelay: bool) -> UringResult<()> {
        self.setsockopt(IPPROTO_TCP, TCP_NODELAY, nodelay as c_int)
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> UringResult<()> {
        self.setsockopt(SOL_SOCKET, SO_RCVBUF, size as c_int)
    }

    pub fn set_send_buffer_size(&self, size: usize) -> UringResult<()> {
        self.setsockopt(SOL_SOCKET, SO_SNDBUF, size as c_int)
    }

    pub fn set_reuse_address(&self, reuse: bool) -> UringResult<()> {
        self.setsockopt(SOL_SOCKET, SO_REUSEADDR, reuse as c_int)
    }

    pub fn set_keepalive(&self, keepalive: bool) -> UringResult<()> {
        self.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keepalive as c_int)
    }

    pub fn set_ttl(&self, ttl: u32) -> UringResult<()> {
        self.setsockopt(IPPROTO_IP, IP_TTL, ttl as c_int)
    }

    pub fn set_broadcast(&self, broadcast: bool) -> UringResult<()> {
        self.setsockopt(SOL_SOCKET, SO_BROADCAST, broadcast as c_int)
    }
}

impl PlatformSocket for Socket {
    type Handle = UringRawHandle;
    type Error = UringError;

    fn new_tcp_v4() -> UringResult<Self> {
        Socket::new_tcp_v4()
    }

    fn new_tcp_v6() -> UringResult<Self> {
        Socket::new_tcp_v6()
    }

    fn new_udp_v4() -> UringResult<Self> {
        Socket::new_udp_v4()
    }

    fn new_udp_v6() -> UringResult<Self> {
        Socket::new_udp_v6()
    }

    fn bind(&self, addr: SocketAddr) -> UringResult<()> {
        Socket::bind(self, addr)
    }

    fn listen(&self, backlog: i32) -> UringResult<()> {
        Socket::listen(self, backlog)
    }

    fn connect(&self, addr: SocketAddr) -> UringResult<()> {
        Socket::connect(self, addr)
    }

    fn into_owned_raw(self) -> OwnedRawHandle {
        Socket::into_owned_raw(self)
    }

    unsafe fn from_raw(handle: UringRawHandle) -> Self {
        unsafe { Socket::from_raw(handle) }
    }

    fn local_addr(&self) -> UringResult<SocketAddr> {
        Socket::local_addr(self)
    }

    fn set_nodelay(&self, nodelay: bool) -> UringResult<()> {
        Socket::set_nodelay(self, nodelay)
    }

    fn set_recv_buffer_size(&self, size: usize) -> UringResult<()> {
        Socket::set_recv_buffer_size(self, size)
    }

    fn set_send_buffer_size(&self, size: usize) -> UringResult<()> {
        Socket::set_send_buffer_size(self, size)
    }

    fn set_reuse_address(&self, reuse: bool) -> UringResult<()> {
        Socket::set_reuse_address(self, reuse)
    }

    fn set_keepalive(&self, keepalive: bool) -> UringResult<()> {
        Socket::set_keepalive(self, keepalive)
    }

    fn set_ttl(&self, ttl: u32) -> UringResult<()> {
        Socket::set_ttl(self, ttl)
    }

    fn set_broadcast(&self, broadcast: bool) -> UringResult<()> {
        Socket::set_broadcast(self, broadcast)
    }
}

#[cfg(test)]
mod tests {
    use crate::{SockAddrStorage, UringError};
    use libc::{AF_INET, sockaddr_in, sockaddr_storage};
    use veloq_std::mem::size_of;

    fn storage() -> SockAddrStorage {
        let mut storage = SockAddrStorage::default();
        storage.as_raw_mut().ss_family = AF_INET as _;
        storage
    }

    #[test]
    fn bounded_sockaddr_bytes_accepts_lengths_within_storage() {
        let storage = storage();
        let bytes = storage
            .bounded_bytes(size_of::<sockaddr_in>(), "uring.net.tests.valid_sockaddr")
            .expect("a sockaddr_in fits in sockaddr_storage");
        assert_eq!(bytes.len(), size_of::<sockaddr_in>());
    }

    #[test]
    fn bounded_sockaddr_bytes_rejects_lengths_beyond_storage() {
        let storage = storage();
        let result = storage.bounded_bytes(
            size_of::<sockaddr_storage>() + 1,
            "uring.net.tests.invalid_sockaddr",
        );
        let Err(report) = result else {
            panic!("an oversized sockaddr length must be rejected");
        };
        assert_eq!(*report.inner(), UringError::InvalidState);
    }
}
