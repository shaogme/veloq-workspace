use super::addr::{socket_addr_to_storage, to_socket_addr};
use crate::config::{IocpHandle, OwnedRawHandle, RawHandle};
use crate::error::{IocpError, IocpResult, from_io_error};
use crate::win32::SafeSocket;
use std::net::SocketAddr;
use veloq_driver_core::net::PlatformSocket;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, INVALID_SOCKET, IP_TTL, IPPROTO_IP, IPPROTO_TCP, IPPROTO_UDP, SO_BROADCAST,
    SO_KEEPALIVE, SO_RCVBUF, SO_REUSEADDR, SO_SNDBUF, SOCK_DGRAM, SOCK_STREAM, SOCKADDR,
    SOL_SOCKET, TCP_NODELAY, WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO, WSASocketW,
};

/// A socket handle wrapper.
pub struct Socket {
    inner: SafeSocket,
}

impl Socket {
    fn new_with_flags_inner(af: u16, ty: i32, protocol: i32, flags: u32) -> IocpResult<Self> {
        // SAFETY: Calling WSASocketW with valid arguments.
        let s = unsafe { WSASocketW(af as i32, ty, protocol, std::ptr::null(), 0, flags) };
        if s == INVALID_SOCKET {
            return Err(from_io_error(
                IocpError::Socket,
                "WSASocketW",
                std::io::Error::last_os_error(),
            ));
        }
        Ok(Self {
            inner: SafeSocket(s),
        })
    }

    fn new_inner(af: u16, ty: i32, protocol: i32) -> IocpResult<Self> {
        Self::new_with_flags_inner(
            af,
            ty,
            protocol,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
        )
    }

    /// Creates a new TCP v4 socket.
    pub fn new_tcp_v4() -> IocpResult<Self> {
        Self::new_inner(AF_INET, SOCK_STREAM, IPPROTO_TCP)
            .map_err(|e| e.attach("socket new_tcp_v4 failed"))
    }

    /// Creates a new TCP v6 socket.
    pub fn new_tcp_v6() -> IocpResult<Self> {
        Self::new_inner(AF_INET6, SOCK_STREAM, IPPROTO_TCP)
            .map_err(|e| e.attach("socket new_tcp_v6 failed"))
    }

    /// Creates a new UDP v4 socket.
    pub fn new_udp_v4() -> IocpResult<Self> {
        Self::new_inner(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
            .map_err(|e| e.attach("socket new_udp_v4 failed"))
    }

    /// Creates a new UDP v6 socket.
    pub fn new_udp_v6() -> IocpResult<Self> {
        Self::new_inner(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)
            .map_err(|e| e.attach("socket new_udp_v6 failed"))
    }

    /// Binds the socket to the given address.
    pub fn bind(&self, addr: SocketAddr) -> IocpResult<()> {
        let (storage, len) = socket_addr_to_storage(addr);
        // SAFETY: storage is a valid SOCKADDR_STORAGE and we pass its pointer and size.
        unsafe {
            self.inner
                .bind(&storage.0 as *const _ as *const SOCKADDR, len)
                .map_err(|e| e.attach("socket bind failed"))
        }
    }

    /// Connects the socket to the given address.
    pub fn connect(&self, addr: SocketAddr) -> IocpResult<()> {
        let (storage, len) = socket_addr_to_storage(addr);
        // SAFETY: storage is a valid SOCKADDR_STORAGE and we pass its pointer and size.
        unsafe {
            self.inner
                .connect(&storage.0 as *const _ as *const SOCKADDR, len)
                .map_err(|e| e.attach("socket connect failed"))
        }
    }

    /// Listens for incoming connections.
    pub fn listen(&self, backlog: i32) -> IocpResult<()> {
        self.inner
            .listen(backlog)
            .map_err(|e| e.attach("socket listen failed"))
    }

    /// Consumes the Socket and returns an owned handle.
    pub fn into_owned_raw(self) -> OwnedRawHandle {
        let h = self.inner.0;
        std::mem::forget(self);
        let raw = RawHandle::new(IocpHandle::for_socket(h as _));
        // SAFETY: this socket originates from `self` and ownership is uniquely transferred.
        unsafe { OwnedRawHandle::from_raw_owned(raw) }
    }

    /// # Safety
    ///
    /// `handle` 必须是有效套接字句柄，且调用方转移所有权给返回值。
    pub unsafe fn from_raw(handle: IocpHandle) -> Self {
        Self {
            inner: SafeSocket(handle.as_socket()),
        }
    }

    /// Returns the local address of the socket.
    pub fn local_addr(&self) -> IocpResult<SocketAddr> {
        // SAFETY: SOCKADDR_STORAGE is a POD struct and safe to zero-initialize.
        let mut storage = unsafe {
            std::mem::zeroed::<windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE>()
        };
        let mut len = std::mem::size_of_val(&storage) as i32;
        // SAFETY: storage and len are valid pointers to local variables.
        unsafe {
            self.inner
                .getsockname(&mut storage as *mut _ as *mut SOCKADDR, &mut len)
                .map_err(|e| e.attach("socket getsockname failed"))?;
        }

        // SAFETY: storage is a valid SOCKADDR_STORAGE and len is its size.
        let buf =
            unsafe { std::slice::from_raw_parts(&storage as *const _ as *const u8, len as usize) };
        to_socket_addr(buf).map_err(|e| e.attach("decode local socket address failed"))
    }

    /// Sets TCP_NODELAY option.
    pub fn set_nodelay(&self, nodelay: bool) -> IocpResult<()> {
        let val = if nodelay { 1i32 } else { 0i32 };
        self.inner
            .setsockopt(IPPROTO_TCP, TCP_NODELAY, &val)
            .map_err(|e| e.attach("socket set_nodelay failed"))
    }

    /// Sets receive buffer size.
    pub fn set_recv_buffer_size(&self, size: usize) -> IocpResult<()> {
        let val = size as i32;
        self.inner
            .setsockopt(SOL_SOCKET, SO_RCVBUF, &val)
            .map_err(|e| e.attach("socket set_recv_buffer_size failed"))
    }

    /// Sets send buffer size.
    pub fn set_send_buffer_size(&self, size: usize) -> IocpResult<()> {
        let val = size as i32;
        self.inner
            .setsockopt(SOL_SOCKET, SO_SNDBUF, &val)
            .map_err(|e| e.attach("socket set_send_buffer_size failed"))
    }

    /// Sets SO_REUSEADDR option.
    pub fn set_reuse_address(&self, reuse: bool) -> IocpResult<()> {
        let val = if reuse { 1i32 } else { 0i32 };
        self.inner
            .setsockopt(SOL_SOCKET, SO_REUSEADDR, &val)
            .map_err(|e| e.attach("socket set_reuse_address failed"))
    }

    /// Sets SO_KEEPALIVE option.
    pub fn set_keepalive(&self, keepalive: bool) -> IocpResult<()> {
        let val = if keepalive { 1i32 } else { 0i32 };
        self.inner
            .setsockopt(SOL_SOCKET, SO_KEEPALIVE, &val)
            .map_err(|e| e.attach("socket set_keepalive failed"))
    }

    /// Sets IP_TTL option.
    pub fn set_ttl(&self, ttl: u32) -> IocpResult<()> {
        let val = ttl as i32;
        self.inner
            .setsockopt(IPPROTO_IP, IP_TTL, &val)
            .map_err(|e| e.attach("socket set_ttl failed"))
    }

    /// Sets SO_BROADCAST option.
    pub fn set_broadcast(&self, broadcast: bool) -> IocpResult<()> {
        let val = if broadcast { 1i32 } else { 0i32 };
        self.inner
            .setsockopt(SOL_SOCKET, SO_BROADCAST, &val)
            .map_err(|e| e.attach("socket set_broadcast failed"))
    }
}

impl PlatformSocket for Socket {
    type Handle = IocpHandle;
    type Error = IocpError;

    fn new_tcp_v4() -> IocpResult<Self> {
        Socket::new_tcp_v4()
    }

    fn new_tcp_v6() -> IocpResult<Self> {
        Socket::new_tcp_v6()
    }

    fn new_udp_v4() -> IocpResult<Self> {
        Socket::new_udp_v4()
    }

    fn new_udp_v6() -> IocpResult<Self> {
        Socket::new_udp_v6()
    }

    fn bind(&self, addr: SocketAddr) -> IocpResult<()> {
        Socket::bind(self, addr)
    }

    fn listen(&self, backlog: i32) -> IocpResult<()> {
        Socket::listen(self, backlog)
    }

    fn connect(&self, addr: SocketAddr) -> IocpResult<()> {
        Socket::connect(self, addr)
    }

    fn into_owned_raw(self) -> OwnedRawHandle {
        Socket::into_owned_raw(self)
    }

    /// # Safety
    ///
    /// `handle` 必须是有效套接字句柄，且调用方转移所有权给返回值。
    unsafe fn from_raw(handle: Self::Handle) -> Self {
        // SAFETY: 由 trait 调用方保证 `handle` 有效且所有权转移。
        unsafe { Socket::from_raw(handle) }
    }

    fn local_addr(&self) -> IocpResult<SocketAddr> {
        Socket::local_addr(self)
    }

    fn set_nodelay(&self, nodelay: bool) -> IocpResult<()> {
        Socket::set_nodelay(self, nodelay)
    }

    fn set_recv_buffer_size(&self, size: usize) -> IocpResult<()> {
        Socket::set_recv_buffer_size(self, size)
    }

    fn set_send_buffer_size(&self, size: usize) -> IocpResult<()> {
        Socket::set_send_buffer_size(self, size)
    }

    fn set_reuse_address(&self, reuse: bool) -> IocpResult<()> {
        Socket::set_reuse_address(self, reuse)
    }

    fn set_keepalive(&self, keepalive: bool) -> IocpResult<()> {
        Socket::set_keepalive(self, keepalive)
    }

    fn set_ttl(&self, ttl: u32) -> IocpResult<()> {
        Socket::set_ttl(self, ttl)
    }

    fn set_broadcast(&self, broadcast: bool) -> IocpResult<()> {
        Socket::set_broadcast(self, broadcast)
    }
}
