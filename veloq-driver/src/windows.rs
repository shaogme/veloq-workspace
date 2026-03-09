use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::raw::c_void;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP, IPPROTO_UDP, SOCK_DGRAM, SOCK_STREAM, SOCKADDR,
    SOCKADDR_IN, SOCKADDR_IN6, WSA_FLAG_OVERLAPPED, WSADATA, WSASocketW, WSAStartup, bind,
    closesocket, getsockname, listen,
};

pub struct Socket {
    handle: RawHandle,
}

impl Socket {
    fn new(af: u16, ty: i32, protocol: i32) -> std::io::Result<Self> {
        let s = unsafe {
            WSASocketW(
                af as i32,
                ty,
                protocol,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED,
            )
        };
        if s == INVALID_SOCKET {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { handle: s.into() })
    }

    pub fn new_tcp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_STREAM, IPPROTO_TCP)
    }

    pub fn new_tcp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_STREAM, IPPROTO_TCP)
    }

    pub fn new_udp_v4() -> std::io::Result<Self> {
        Self::new(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    }

    pub fn new_udp_v6() -> std::io::Result<Self> {
        Self::new(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)
    }

    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        let (raw_addr, raw_addr_len) = socket_addr_trans(addr);
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

    pub fn listen(&self, backlog: i32) -> std::io::Result<()> {
        let ret = unsafe { listen(self.handle.into(), backlog) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn into_raw(self) -> RawHandle {
        let h = self.handle;
        std::mem::forget(self);
        h
    }

    /// # Safety
    ///
    /// The provided raw handle must be a valid handle, and it must outlive the returned `Socket`.
    pub unsafe fn from_raw(handle: RawHandle) -> Self {
        Self { handle }
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        let mut buf = [0u8; 128];
        let mut len = 128_i32;
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

    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        let val = if nodelay { 1i32 } else { 0i32 };
        self.setsockopt(
            IPPROTO_TCP,
            windows_sys::Win32::Networking::WinSock::TCP_NODELAY,
            val,
        )
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_RCVBUF,
            size as i32,
        )
    }

    pub fn set_send_buffer_size(&self, size: usize) -> std::io::Result<()> {
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_SNDBUF,
            size as i32,
        )
    }

    pub fn set_reuse_address(&self, reuse: bool) -> std::io::Result<()> {
        let val = if reuse { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_REUSEADDR,
            val,
        )
    }

    pub fn set_keepalive(&self, keepalive: bool) -> std::io::Result<()> {
        let val = if keepalive { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_KEEPALIVE,
            val,
        )
    }

    pub fn set_ttl(&self, ttl: u32) -> std::io::Result<()> {
        // IP_TTL is not always standard across windows versions in header, but usually available.
        // windows-sys defines IP_TTL in WinSock.
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::IPPROTO_IP,
            windows_sys::Win32::Networking::WinSock::IP_TTL,
            ttl as i32,
        )
    }

    pub fn set_broadcast(&self, broadcast: bool) -> std::io::Result<()> {
        let val = if broadcast { 1i32 } else { 0i32 };
        self.setsockopt(
            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
            windows_sys::Win32::Networking::WinSock::SO_BROADCAST,
            val,
        )
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { closesocket(self.handle.into()) };
    }
}

/// Winsock Initialization Hook
///
/// This mimics C++ global constructors to run code before `main`.
/// `.CRT$XCU` is the linker section used by MSVC for C++ dynamic initializers.
/// placing a function pointer here ensures `WSAStartup` is called by the CRT start-up routines.
///
/// This resolves the "10093: WSAStartup not called" error globally without user intervention.
#[used]
#[unsafe(link_section = ".CRT$XCU")]
static INIT_WINSOCK: unsafe extern "C" fn() = {
    unsafe extern "C" fn init() {
        unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            let _ = WSAStartup(0x0202, &mut data);
        }
    }
    init
};

pub fn to_socket_addr(buf: &[u8]) -> std::io::Result<SocketAddr> {
    if buf.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid address length",
        ));
    }
    let family = unsafe { *(buf.as_ptr() as *const u16) };
    match family {
        AF_INET => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN) };
            let s_addr = unsafe { sin.sin_addr.S_un.S_addr };
            let ip = Ipv4Addr::from(u32::from_be(s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN6>() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin6 = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN6) };
            let addr_bytes = unsafe { sin6.sin6_addr.u.Byte };
            let ip = Ipv6Addr::from(addr_bytes);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported address family",
        )),
    }
}

pub fn socket_addr_trans(addr: SocketAddr) -> (Vec<u8>, i32) {
    match addr {
        SocketAddr::V4(a) => {
            let mut sin: SOCKADDR_IN = unsafe { std::mem::zeroed() };
            sin.sin_family = AF_INET;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());

            let ptr = &sin as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(a) => {
            let mut sin6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = AF_INET6;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr = unsafe {
                std::mem::transmute::<[u8; 16], windows_sys::Win32::Networking::WinSock::IN6_ADDR>(
                    a.ip().octets(),
                )
            };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.Anonymous.sin6_scope_id = a.scope_id();

            let ptr = &sin6 as *const _ as *const u8;
            let buf =
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN6>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN6>() as i32)
        }
    }
}

pub use windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE;

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin_ptr = &mut storage as *mut _ as *mut SOCKADDR_IN;
            unsafe {
                (*sin_ptr).sin_family = AF_INET;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<SOCKADDR_IN>() as i32
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage as *mut _ as *mut SOCKADDR_IN6;
            unsafe {
                (*sin6_ptr).sin6_family = AF_INET6;
                (*sin6_ptr).sin6_port = a.port().to_be();
                (*sin6_ptr).sin6_addr = std::mem::transmute::<
                    [u8; 16],
                    windows_sys::Win32::Networking::WinSock::IN6_ADDR,
                >(a.ip().octets());
                (*sin6_ptr).sin6_flowinfo = a.flowinfo();
                (*sin6_ptr).Anonymous.sin6_scope_id = a.scope_id();
                std::mem::size_of::<SOCKADDR_IN6>() as i32
            }
        }
    };
    (storage, len)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawHandle {
    // usually isize/ptr (8 bytes)
    pub handle: windows_sys::Win32::Foundation::HANDLE,
}

impl From<*mut c_void> for RawHandle {
    fn from(handle: *mut c_void) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(target_pointer_width = "64")]
impl From<u64> for RawHandle {
    fn from(handle: u64) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(target_pointer_width = "32")]
impl From<u32> for RawHandle {
    fn from(handle: u32) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

impl std::ops::Deref for RawHandle {
    type Target = windows_sys::Win32::Foundation::HANDLE;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl From<RawHandle> for windows_sys::Win32::Foundation::HANDLE {
    fn from(handle: RawHandle) -> Self {
        handle.handle
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.handle as usize
    }
}
