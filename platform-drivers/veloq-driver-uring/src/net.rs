use crate::config::UringRawHandle;
use crate::error::{UringError, UringResult, from_io_error};
use crate::{OwnedRawHandle, RawHandle, SockAddrStorage};
use libc::{c_int, sockaddr, sockaddr_in, sockaddr_in6, socklen_t};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::{PlatformSocket, SocketAddrCodec};

pub struct Socket {
    fd: OwnedRawHandle,
}

impl Socket {
    fn new_v4(ty: c_int) -> UringResult<Self> {
        let fd = unsafe { libc::socket(libc::AF_INET, ty, 0) };
        if fd < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.new_v4",
                io::Error::last_os_error(),
            ));
        }
        Ok(Self {
            // SAFETY: newly created socket fd is uniquely owned.
            fd: unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(fd)))
            },
        })
    }

    fn new_v6(ty: c_int) -> UringResult<Self> {
        let fd = unsafe { libc::socket(libc::AF_INET6, ty, 0) };
        if fd < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.new_v6",
                io::Error::last_os_error(),
            ));
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
            libc::setsockopt(
                self.fd.raw().as_fd(),
                level,
                optname,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<T>() as socklen_t,
            )
        };
        if ret < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.setsockopt",
                io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    pub fn new_tcp_v4() -> UringResult<Self> {
        Self::new_v4(libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_tcp_v6() -> UringResult<Self> {
        Self::new_v6(libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_udp_v4() -> UringResult<Self> {
        Self::new_v4(libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_udp_v6() -> UringResult<Self> {
        Self::new_v6(libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn bind(&self, addr: SocketAddr) -> UringResult<()> {
        let (raw_addr, raw_addr_len) = socket_addr_to_storage(addr);
        let ret = unsafe {
            libc::bind(
                self.fd.raw().as_fd(),
                &raw_addr.0 as *const _ as *const sockaddr,
                raw_addr_len,
            )
        };
        if ret < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.bind",
                io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    pub fn connect(&self, addr: SocketAddr) -> UringResult<()> {
        let (raw_addr, raw_addr_len) = socket_addr_to_storage(addr);
        let ret = unsafe {
            libc::connect(
                self.fd.raw().as_fd(),
                &raw_addr.0 as *const _ as *const sockaddr,
                raw_addr_len,
            )
        };
        if ret < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.connect",
                io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> UringResult<()> {
        let ret = unsafe { libc::listen(self.fd.raw().as_fd(), backlog as c_int) };
        if ret < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.listen",
                io::Error::last_os_error(),
            ));
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
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as socklen_t;
        let ret = unsafe {
            libc::getsockname(
                self.fd.raw().as_fd(),
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(from_io_error(
                UringError::Socket,
                "socket.local_addr.getsockname",
                io::Error::last_os_error(),
            ));
        }
        to_socket_addr(unsafe {
            std::slice::from_raw_parts(&storage as *const _ as *const u8, len as usize)
        })
        .map_err(|e| e.attach("socket.local_addr.decode"))
    }

    pub fn set_nodelay(&self, nodelay: bool) -> UringResult<()> {
        self.setsockopt(libc::IPPROTO_TCP, libc::TCP_NODELAY, nodelay as c_int)
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> UringResult<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_RCVBUF, size as c_int)
    }

    pub fn set_send_buffer_size(&self, size: usize) -> UringResult<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_SNDBUF, size as c_int)
    }

    pub fn set_reuse_address(&self, reuse: bool) -> UringResult<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_REUSEADDR, reuse as c_int)
    }

    pub fn set_keepalive(&self, keepalive: bool) -> UringResult<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, keepalive as c_int)
    }

    pub fn set_ttl(&self, ttl: u32) -> UringResult<()> {
        self.setsockopt(libc::IPPROTO_IP, libc::IP_TTL, ttl as c_int)
    }

    pub fn set_broadcast(&self, broadcast: bool) -> UringResult<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_BROADCAST, broadcast as c_int)
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

impl SocketAddrCodec for SockAddrStorage {
    type Len = socklen_t;
    type Error = UringError;

    fn to_socket_addr(buf: &[u8]) -> UringResult<SocketAddr> {
        to_socket_addr(buf).map_err(|e| e.attach("socket_addr.decode"))
    }

    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len) {
        socket_addr_to_storage(addr)
    }
}

pub fn to_socket_addr(buf: &[u8]) -> UringResult<SocketAddr> {
    if buf.len() < std::mem::size_of::<libc::sa_family_t>() {
        return Err(
            error_stack::Report::new(UringError::InvalidInput).attach("Invalid address length")
        );
    }
    let mut family_raw = std::mem::MaybeUninit::<libc::sa_family_t>::uninit();
    // Copy into properly aligned stack storage before reading, avoiding UB on unaligned input.
    unsafe {
        std::ptr::copy_nonoverlapping(
            buf.as_ptr(),
            family_raw.as_mut_ptr() as *mut u8,
            std::mem::size_of::<libc::sa_family_t>(),
        );
    }
    let family = unsafe { family_raw.assume_init() } as i32;
    match family {
        libc::AF_INET => {
            if buf.len() < std::mem::size_of::<sockaddr_in>() {
                return Err(error_stack::Report::new(UringError::InvalidInput)
                    .attach("Invalid address length"));
            }
            let mut sin_raw = std::mem::MaybeUninit::<sockaddr_in>::zeroed();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    sin_raw.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<sockaddr_in>(),
                );
            }
            let sin = unsafe { sin_raw.assume_init() };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if buf.len() < std::mem::size_of::<sockaddr_in6>() {
                return Err(error_stack::Report::new(UringError::InvalidInput)
                    .attach("Invalid address length"));
            }
            let mut sin6_raw = std::mem::MaybeUninit::<sockaddr_in6>::zeroed();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    sin6_raw.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<sockaddr_in6>(),
                );
            }
            let sin6 = unsafe { sin6_raw.assume_init() };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => {
            Err(error_stack::Report::new(UringError::InvalidInput)
                .attach("Unsupported address family"))
        }
    }
}

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, socklen_t) {
    let mut storage = SockAddrStorage::default();
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin_ptr = &mut storage.0 as *mut _ as *mut sockaddr_in;
            unsafe {
                (*sin_ptr).sin_family = libc::AF_INET as _;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<sockaddr_in>() as socklen_t
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage.0 as *mut _ as *mut sockaddr_in6;
            unsafe {
                (*sin6_ptr).sin6_family = libc::AF_INET6 as _;
                (*sin6_ptr).sin6_port = a.port().to_be();
                (*sin6_ptr).sin6_addr.s6_addr = a.ip().octets();
                (*sin6_ptr).sin6_flowinfo = a.flowinfo();
                (*sin6_ptr).sin6_scope_id = a.scope_id();
                std::mem::size_of::<sockaddr_in6>() as socklen_t
            }
        }
    };
    (storage, len)
}
