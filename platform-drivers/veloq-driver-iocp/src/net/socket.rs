use super::addr::{socket_addr_trans, to_socket_addr};
use crate::config::RawHandle;
use std::io;
use std::net::SocketAddr;
use veloq_driver_core::net::PlatformSocket;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP, IPPROTO_UDP, SOCK_DGRAM, SOCK_STREAM, SOCKADDR,
    WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO, WSASocketW, bind, closesocket, getsockname,
    listen,
};

/// A socket handle wrapper.
pub struct Socket {
    handle: RawHandle,
}

impl Socket {
    fn new(af: u16, ty: i32, protocol: i32) -> std::io::Result<Self> {
        // SAFETY: Calling WSASocketW with valid arguments.
        let s = unsafe {
            WSASocketW(
                af as i32,
                ty,
                protocol,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
            )
        };
        if s == INVALID_SOCKET {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { handle: s.into() })
    }

    /// Creates a new TCP v4 socket.
    pub fn new_tcp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_STREAM, IPPROTO_TCP)
    }

    /// Creates a new TCP v6 socket.
    pub fn new_tcp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_STREAM, IPPROTO_TCP)
    }

    /// Creates a new UDP v4 socket.
    pub fn new_udp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    }

    /// Creates a new UDP v6 socket.
    pub fn new_udp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)
    }

    /// Binds the socket to the given address.
    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        let (raw_addr, raw_addr_len) = socket_addr_trans(addr);
        // SAFETY: self.handle is a valid socket and raw_addr is correctly initialized.
        let ret = unsafe {
            bind(
                self.handle.into(),
                raw_addr.as_ptr() as *const SOCKADDR,
                raw_addr_len,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Listens for incoming connections.
    pub fn listen(&self, backlog: i32) -> std::io::Result<()> {
        // SAFETY: self.handle is a valid socket.
        let ret = unsafe { listen(self.handle.into(), backlog) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Consumes the Socket and returns the raw handle.
    pub fn into_raw(self) -> RawHandle {
        let h = self.handle;
        std::mem::forget(self);
        h
    }

    /// Creates a Socket from a raw handle.
    ///
    /// # Safety
    ///
    /// `handle` must be a valid socket handle.
    pub unsafe fn from_raw(handle: RawHandle) -> Self {
        Self { handle }
    }

    /// Returns the local address of the socket.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        let mut buf = [0u8; 128];
        let mut len = 128_i32;
        // SAFETY: self.handle is a valid socket and pointers are valid for mutation.
        let ret = unsafe {
            getsockname(
                self.handle.into(),
                buf.as_mut_ptr() as *mut SOCKADDR,
                &mut len,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        to_socket_addr(&buf[..len as usize])
    }

    fn setsockopt<T>(&self, level: i32, optname: i32, optval: T) -> std::io::Result<()> {
        // SAFETY: self.handle is a valid socket and optval pointer is valid.
        let ret = unsafe {
            windows_sys::Win32::Networking::WinSock::setsockopt(
                self.handle.into(),
                level,
                optname,
                &optval as *const _ as *const u8,
                std::mem::size_of::<T>() as i32,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Sets TCP_NODELAY option.
    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        let val = if nodelay { 1i32 } else { 0i32 };
        self.setsockopt(
            IPPROTO_TCP,
            windows_sys::Win32::Networking::WinSock::TCP_NODELAY,
            val,
        )
    }

    /// Sets receive buffer size.
    pub fn set_recv_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_RCVBUF,
            size as i32,
        )
    }

    /// Sets send buffer size.
    pub fn set_send_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_SNDBUF,
            size as i32,
        )
    }

    /// Sets SO_REUSEADDR option.
    pub fn set_reuse_address(&self, reuse: bool) -> std::io::Result<()> {
        let val = if reuse { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_REUSEADDR,
            val,
        )
    }

    /// Sets SO_KEEPALIVE option.
    pub fn set_keepalive(&self, keepalive: bool) -> std::io::Result<()> {
        let val = if keepalive { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_KEEPALIVE,
            val,
        )
    }

    /// Sets IP_TTL option.
    pub fn set_ttl(&self, ttl: u32) -> std::io::Result<()> {
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::IPPROTO_IP,
            windows_sys::Win32::Networking::WinSock::IP_TTL,
            ttl as i32,
        )
    }

    /// Sets SO_BROADCAST option.
    pub fn set_broadcast(&self, broadcast: bool) -> std::io::Result<()> {
        let val = if broadcast { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_BROADCAST,
            val,
        )
    }
}

impl PlatformSocket for Socket {
    type Handle = RawHandle;

    fn new_tcp_v4() -> io::Result<Self> {
        Socket::new_tcp_v4()
    }

    fn new_tcp_v6() -> io::Result<Self> {
        Socket::new_tcp_v6()
    }

    fn new_udp_v4() -> io::Result<Self> {
        Socket::new_udp_v4()
    }

    fn new_udp_v6() -> io::Result<Self> {
        Socket::new_udp_v6()
    }

    fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        Socket::bind(self, addr)
    }

    fn listen(&self, backlog: i32) -> io::Result<()> {
        Socket::listen(self, backlog)
    }

    fn into_raw(self) -> Self::Handle {
        Socket::into_raw(self)
    }

    /// # Safety
    ///
    /// Forwarding to Socket::from_raw which has the same safety requirements.
    unsafe fn from_raw(handle: Self::Handle) -> Self {
        // SAFETY: The caller must ensure the handle is valid and the ownership is transferred.
        unsafe { Socket::from_raw(handle) }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Socket::local_addr(self)
    }

    fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        Socket::set_nodelay(self, nodelay)
    }

    fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        Socket::set_recv_buffer_size(self, size)
    }

    fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        Socket::set_send_buffer_size(self, size)
    }

    fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        Socket::set_reuse_address(self, reuse)
    }

    fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        Socket::set_keepalive(self, keepalive)
    }

    fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        Socket::set_ttl(self, ttl)
    }

    fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        Socket::set_broadcast(self, broadcast)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // SAFETY: closing the socket handle.
        unsafe { closesocket(self.handle.into()) };
    }
}
