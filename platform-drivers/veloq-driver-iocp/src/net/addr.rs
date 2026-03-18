use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::SocketAddrCodec;
use veloq_pod::{Pod, Zeroable, bytes_of_mut, from_bytes, from_bytes_mut, zeroed};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE,
};

/// Helper trait for safe access to SOCKADDR_IN union fields.
trait SockAddrInExt {
    fn s_addr(&self) -> u32;
    fn set_s_addr(&mut self, addr: u32);
}

impl SockAddrInExt for SOCKADDR_IN {
    fn s_addr(&self) -> u32 {
        // SAFETY: sin_addr.S_un is a union, accessing S_addr is safe for AF_INET.
        unsafe { self.sin_addr.S_un.S_addr }
    }

    fn set_s_addr(&mut self, addr: u32) {
        // SAFETY: sin_addr.S_un is a union, setting S_addr is safe for AF_INET.
        // In modern Rust, writing to a Copy union field is safe.
        self.sin_addr.S_un.S_addr = addr;
    }
}

/// Helper trait for safe access to SOCKADDR_IN6 union fields.
trait SockAddrIn6Ext {
    fn scope_id(&self) -> u32;
    fn set_scope_id(&mut self, id: u32);
    fn addr_bytes(&self) -> [u8; 16];
    fn set_addr_bytes(&mut self, bytes: [u8; 16]);
}

impl SockAddrIn6Ext for SOCKADDR_IN6 {
    fn scope_id(&self) -> u32 {
        // SAFETY: sin6.Anonymous is a union, accessing sin6_scope_id is safe for AF_INET6.
        unsafe { self.Anonymous.sin6_scope_id }
    }

    fn set_scope_id(&mut self, id: u32) {
        // SAFETY: sin6.Anonymous is a union, setting sin6_scope_id is safe for AF_INET6.
        // In modern Rust, writing to a Copy union field is safe.
        self.Anonymous.sin6_scope_id = id;
    }

    fn addr_bytes(&self) -> [u8; 16] {
        // SAFETY: sin6_addr.u is a union, accessing Byte is safe.
        unsafe { self.sin6_addr.u.Byte }
    }

    fn set_addr_bytes(&mut self, bytes: [u8; 16]) {
        // SAFETY: sin6_addr.u is a union, setting Byte is safe.
        // In modern Rust, writing to a Copy union field is safe.
        self.sin6_addr.u.Byte = bytes;
    }
}

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
        zeroed()
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
        let mut sin_wrapped: SockAddrIn = zeroed();
        let sin = &mut sin_wrapped.0;
        sin.sin_family = AF_INET;
        sin.sin_port = addr.port().to_be();
        sin.set_s_addr(u32::from_ne_bytes(addr.ip().octets()));
        sin_wrapped
    }

    /// Converts to a standard library SocketAddrV4.
    pub fn to_std(&self) -> SocketAddrV4 {
        let s_addr = self.0.s_addr();
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
        let mut sin6_wrapped: SockAddrIn6 = zeroed();
        let sin6 = &mut sin6_wrapped.0;
        sin6.sin6_family = AF_INET6;
        sin6.sin6_port = addr.port().to_be();
        sin6.set_addr_bytes(addr.ip().octets());
        sin6.sin6_flowinfo = addr.flowinfo();
        sin6.set_scope_id(addr.scope_id());
        sin6_wrapped
    }

    /// Converts to a standard library SocketAddrV6.
    pub fn to_std(&self) -> SocketAddrV6 {
        let addr_bytes = self.0.addr_bytes();
        let ip = Ipv6Addr::from(addr_bytes);
        let port = u16::from_be(self.0.sin6_port);
        let flowinfo = self.0.sin6_flowinfo;
        let scope_id = self.0.scope_id();
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
            let sin_ref = from_bytes_mut::<SockAddrIn>(
                &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN>()],
            );
            *sin_ref = sin;
            std::mem::size_of::<SOCKADDR_IN>() as i32
        }
        SocketAddr::V6(a) => {
            let sin6 = SockAddrIn6::new(&a);
            let sin6_ref = from_bytes_mut::<SockAddrIn6>(
                &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN6>()],
            );
            *sin6_ref = sin6;
            std::mem::size_of::<SOCKADDR_IN6>() as i32
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
