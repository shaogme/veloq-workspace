use libc::{c_int, sockaddr, sockaddr_in, sockaddr_in6, socklen_t};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::RawFd;
use veloq_driver_core::{RawHandle, SockAddrStorage};

pub struct Socket {
    fd: RawFd,
}

impl Socket {
    pub fn new_v4(ty: c_int) -> std::io::Result<Self> {
        let fd = unsafe { libc::socket(libc::AF_INET, ty, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    pub fn new_v6(ty: c_int) -> std::io::Result<Self> {
        let fd = unsafe { libc::socket(libc::AF_INET6, ty, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    pub fn new_tcp_v4() -> std::io::Result<Self> {
        Self::new_v4(libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_tcp_v6() -> std::io::Result<Self> {
        Self::new_v6(libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_udp_v4() -> std::io::Result<Self> {
        Self::new_v4(libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn new_udp_v6() -> std::io::Result<Self> {
        Self::new_v6(libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
    }

    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        let (raw_addr, raw_addr_len) = socket_addr_trans(addr);
        let ret =
            unsafe { libc::bind(self.fd, raw_addr.as_ptr() as *const sockaddr, raw_addr_len) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> std::io::Result<()> {
        let ret = unsafe { libc::listen(self.fd, backlog as c_int) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn into_raw(self) -> RawHandle {
        let fd = self.fd;
        std::mem::forget(self);
        fd.into()
    }

    /// # Safety
    ///
    /// The provided raw handle must be a valid file descriptor, and it must outlive the returned `Socket`.
    pub unsafe fn from_raw(handle: RawHandle) -> Self {
        Self { fd: handle.fd }
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as socklen_t;
        let ret = unsafe {
            libc::getsockname(
                self.fd,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
        to_socket_addr(unsafe {
            std::slice::from_raw_parts(&storage as *const _ as *const u8, len as usize)
        })
    }

    fn setsockopt<T>(&self, level: c_int, optname: c_int, optval: T) -> std::io::Result<()> {
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                optname,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<T>() as socklen_t,
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        self.setsockopt(libc::IPPROTO_TCP, libc::TCP_NODELAY, nodelay as c_int)
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_RCVBUF, size as c_int)
    }

    pub fn set_send_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_SNDBUF, size as c_int)
    }

    pub fn set_reuse_address(&self, reuse: bool) -> std::io::Result<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_REUSEADDR, reuse as c_int)
    }

    pub fn set_keepalive(&self, keepalive: bool) -> std::io::Result<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, keepalive as c_int)
    }

    pub fn set_ttl(&self, ttl: u32) -> std::io::Result<()> {
        self.setsockopt(libc::IPPROTO_IP, libc::IP_TTL, ttl as c_int)
    }

    pub fn set_broadcast(&self, broadcast: bool) -> std::io::Result<()> {
        self.setsockopt(libc::SOL_SOCKET, libc::SO_BROADCAST, broadcast as c_int)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

pub fn to_socket_addr(buf: &[u8]) -> std::io::Result<SocketAddr> {
    if buf.len() < std::mem::size_of::<libc::sa_family_t>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid address length",
        ));
    }
    let family = unsafe { *(buf.as_ptr() as *const libc::sa_family_t) } as i32;
    match family {
        libc::AF_INET => {
            if buf.len() < std::mem::size_of::<sockaddr_in>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin = unsafe { &*(buf.as_ptr() as *const sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if buf.len() < std::mem::size_of::<sockaddr_in6>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin6 = unsafe { &*(buf.as_ptr() as *const sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported address family",
        )),
    }
}

pub fn socket_addr_trans(addr: SocketAddr) -> (Vec<u8>, socklen_t) {
    match addr {
        SocketAddr::V4(a) => {
            let mut sin: sockaddr_in = unsafe { std::mem::zeroed() };
            sin.sin_family = libc::AF_INET as _;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());

            let ptr = &sin as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<sockaddr_in>()) }
                    .to_vec();
            (buf, std::mem::size_of::<sockaddr_in>() as socklen_t)
        }
        SocketAddr::V6(a) => {
            let mut sin6: sockaddr_in6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = libc::AF_INET6 as _;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr.s6_addr = a.ip().octets();
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_scope_id = a.scope_id();

            let ptr = &sin6 as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<sockaddr_in6>()) }
                    .to_vec();
            (buf, std::mem::size_of::<sockaddr_in6>() as socklen_t)
        }
    }
}

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, socklen_t) {
    let mut storage: SockAddrStorage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin_ptr = &mut storage as *mut _ as *mut sockaddr_in;
            unsafe {
                (*sin_ptr).sin_family = libc::AF_INET as _;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<sockaddr_in>() as socklen_t
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage as *mut _ as *mut sockaddr_in6;
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
