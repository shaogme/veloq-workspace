use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::SocketAddrCodec;
use veloq_pod::{Pod, Zeroable, from_bytes};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, IN6_ADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE,
};

/// A storage wrapper for socket addresses.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub SOCKADDR_STORAGE);

// SAFETY: SOCKADDR_STORAGE is a Win32 struct that can be zero-initialized.
unsafe impl Zeroable for SockAddrStorage {}
// SAFETY: SockAddrStorage is repr(transparent) and SOCKADDR_STORAGE is a POD-like struct in Win32.
unsafe impl Pod for SockAddrStorage {}

impl Default for SockAddrStorage {
    fn default() -> Self {
        // SAFETY: SOCKADDR_STORAGE is a POD struct and can be safely zeroed.
        Self(unsafe { std::mem::zeroed() })
    }
}

impl SockAddrStorage {
    /// Returns the address family of the stored address.
    pub fn family(&self) -> u16 {
        self.0.ss_family
    }
}

/// A wrapper for SOCKADDR_IN.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrIn(pub SOCKADDR_IN);
// SAFETY: SOCKADDR_IN is a POD struct and can be safely zeroed.
unsafe impl Zeroable for SockAddrIn {}
// SAFETY: SockAddrIn is repr(transparent) and SOCKADDR_IN is a POD struct.
unsafe impl Pod for SockAddrIn {}

impl SockAddrIn {
    /// Creates a new SockAddrIn from a SocketAddrV4.
    pub fn new(addr: &SocketAddrV4) -> Self {
        let mut sin: SOCKADDR_IN = unsafe { std::mem::zeroed() };
        sin.sin_family = AF_INET;
        sin.sin_port = addr.port().to_be();
        // SAFETY: sin.sin_addr.S_un is a union, accessing S_addr is safe for AF_INET.
        sin.sin_addr.S_un.S_addr = u32::from_ne_bytes(addr.ip().octets());
        Self(sin)
    }

    /// Converts to a standard library SocketAddrV4.
    pub fn to_std(&self) -> SocketAddrV4 {
        // SAFETY: sin_addr.S_un is a union, accessing S_addr is safe for AF_INET.
        let s_addr = unsafe { self.0.sin_addr.S_un.S_addr };
        let ip = Ipv4Addr::from(u32::from_be(s_addr));
        let port = u16::from_be(self.0.sin_port);
        SocketAddrV4::new(ip, port)
    }
}

/// A wrapper for SOCKADDR_IN6.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrIn6(pub SOCKADDR_IN6);
// SAFETY: SOCKADDR_IN6 is a POD struct and can be safely zeroed.
unsafe impl Zeroable for SockAddrIn6 {}
// SAFETY: SockAddrIn6 is repr(transparent) and SOCKADDR_IN6 is a POD struct.
unsafe impl Pod for SockAddrIn6 {}

impl SockAddrIn6 {
    /// Creates a new SockAddrIn6 from a SocketAddrV6.
    pub fn new(addr: &SocketAddrV6) -> Self {
        let mut sin6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
        sin6.sin6_family = AF_INET6;
        sin6.sin6_port = addr.port().to_be();
        // SAFETY: IN6_ADDR has the same layout as [u8; 16].
        sin6.sin6_addr = unsafe { std::mem::transmute::<[u8; 16], IN6_ADDR>(addr.ip().octets()) };
        sin6.sin6_flowinfo = addr.flowinfo();
        // SAFETY: sin6.Anonymous is a union, accessing sin6_scope_id is safe for AF_INET6.
        sin6.Anonymous.sin6_scope_id = addr.scope_id();
        Self(sin6)
    }

    /// Converts to a standard library SocketAddrV6.
    pub fn to_std(&self) -> SocketAddrV6 {
        // SAFETY: sin6_addr.u is a union, accessing Byte is safe for AF_INET6.
        let addr_bytes = unsafe { self.0.sin6_addr.u.Byte };
        let ip = Ipv6Addr::from(addr_bytes);
        let port = u16::from_be(self.0.sin6_port);
        let flowinfo = self.0.sin6_flowinfo;
        // SAFETY: sin6.Anonymous is a union, accessing sin6_scope_id is safe for AF_INET6.
        let scope_id = unsafe { self.0.Anonymous.sin6_scope_id };
        SocketAddrV6::new(ip, port, flowinfo, scope_id)
    }
}

/// Converts a byte buffer to a SocketAddr.
pub fn to_socket_addr(buf: &[u8]) -> io::Result<SocketAddr> {
    if buf.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid address length",
        ));
    }

    // Use veloq_pod to cast the family safely if possible, but family is at offset 0.
    let family = u16::from_ne_bytes([buf[0], buf[1]]);

    match family {
        AF_INET => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin_wrapped: &SockAddrIn = from_bytes(&buf[..std::mem::size_of::<SOCKADDR_IN>()]);
            Ok(SocketAddr::V4(sin_wrapped.to_std()))
        }
        AF_INET6 => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN6>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            let sin6_wrapped: &SockAddrIn6 =
                from_bytes(&buf[..std::mem::size_of::<SOCKADDR_IN6>()]);
            Ok(SocketAddr::V6(sin6_wrapped.to_std()))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unsupported address family",
        )),
    }
}

/// Converts a SocketAddr to storage and length.
pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, i32) {
    let mut storage = SockAddrStorage::default();
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = SockAddrIn::new(&a);
            let sin_ptr = &mut storage.0 as *mut _ as *mut SockAddrIn;
            // SAFETY: sin_ptr points to valid storage and is cast to the correct type.
            unsafe {
                *sin_ptr = sin;
                std::mem::size_of::<SOCKADDR_IN>() as i32
            }
        }
        SocketAddr::V6(a) => {
            let sin6 = SockAddrIn6::new(&a);
            let sin6_ptr = &mut storage.0 as *mut _ as *mut SockAddrIn6;
            // SAFETY: sin6_ptr points to valid storage and is cast to the correct type.
            unsafe {
                *sin6_ptr = sin6;
                std::mem::size_of::<SOCKADDR_IN6>() as i32
            }
        }
    };
    (storage, len)
}

impl SocketAddrCodec for SockAddrStorage {
    type Len = i32;

    fn to_socket_addr(buf: &[u8]) -> io::Result<SocketAddr> {
        to_socket_addr(buf)
    }

    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len) {
        socket_addr_to_storage(addr)
    }
}
