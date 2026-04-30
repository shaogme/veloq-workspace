use crate::error::{IocpError, IocpResult};
use std::mem::offset_of;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::SocketAddrCodec;
use veloq_pod::{Pod, Zeroable, bytes_of, bytes_of_mut, from_bytes, from_bytes_mut, zeroed};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE,
};

const SOCKADDR_IN_S_ADDR_OFFSET: usize = offset_of!(SOCKADDR_IN, sin_addr);
const SOCKADDR_IN6_ADDR_BYTES_OFFSET: usize = offset_of!(SOCKADDR_IN6, sin6_addr);
const SOCKADDR_IN6_SCOPE_ID_OFFSET: usize = offset_of!(SOCKADDR_IN6, Anonymous);

fn read_u32_ne(bytes: &[u8], offset: usize) -> u32 {
    const SIZE: usize = std::mem::size_of::<u32>();
    let mut raw = [0u8; SIZE];
    raw.copy_from_slice(&bytes[offset..offset + SIZE]);
    u32::from_ne_bytes(raw)
}

fn write_u32_ne(bytes: &mut [u8], offset: usize, value: u32) {
    const SIZE: usize = std::mem::size_of::<u32>();
    let raw = value.to_ne_bytes();
    bytes[offset..offset + SIZE].copy_from_slice(&raw);
}

fn read_ipv6_bytes(bytes: &[u8], offset: usize) -> [u8; 16] {
    const SIZE: usize = 16;
    let mut raw = [0u8; SIZE];
    raw.copy_from_slice(&bytes[offset..offset + SIZE]);
    raw
}

fn write_ipv6_bytes(bytes: &mut [u8], offset: usize, value: [u8; 16]) {
    bytes[offset..offset + value.len()].copy_from_slice(&value);
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
        sin_wrapped.0.sin_family = AF_INET;
        sin_wrapped.0.sin_port = addr.port().to_be();
        write_u32_ne(
            bytes_of_mut(&mut sin_wrapped),
            SOCKADDR_IN_S_ADDR_OFFSET,
            u32::from_ne_bytes(addr.ip().octets()),
        );
        sin_wrapped
    }

    /// Converts to a standard library SocketAddrV4.
    pub fn to_std(&self) -> SocketAddrV4 {
        let s_addr = read_u32_ne(bytes_of(self), SOCKADDR_IN_S_ADDR_OFFSET);
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
        sin6_wrapped.0.sin6_family = AF_INET6;
        sin6_wrapped.0.sin6_port = addr.port().to_be();
        write_ipv6_bytes(
            bytes_of_mut(&mut sin6_wrapped),
            SOCKADDR_IN6_ADDR_BYTES_OFFSET,
            addr.ip().octets(),
        );
        sin6_wrapped.0.sin6_flowinfo = addr.flowinfo();
        write_u32_ne(
            bytes_of_mut(&mut sin6_wrapped),
            SOCKADDR_IN6_SCOPE_ID_OFFSET,
            addr.scope_id(),
        );
        sin6_wrapped
    }

    /// Converts to a standard library SocketAddrV6.
    pub fn to_std(&self) -> SocketAddrV6 {
        let addr_bytes = read_ipv6_bytes(bytes_of(self), SOCKADDR_IN6_ADDR_BYTES_OFFSET);
        let ip = Ipv6Addr::from(addr_bytes);
        let port = u16::from_be(self.0.sin6_port);
        let flowinfo = self.0.sin6_flowinfo;
        let scope_id = read_u32_ne(bytes_of(self), SOCKADDR_IN6_SCOPE_ID_OFFSET);
        SocketAddrV6::new(ip, port, flowinfo, scope_id)
    }
}

/// Converts a byte buffer to a SocketAddr.
pub fn to_socket_addr(buf: &[u8]) -> IocpResult<SocketAddr> {
    if buf.len() < 2 {
        return Err(
            error_stack::Report::new(IocpError::InvalidInput).attach("invalid address length")
        );
    }

    // Use veloq_pod to cast the family safely if possible, but family is at offset 0.
    let family = u16::from_ne_bytes([buf[0], buf[1]]);

    match family {
        AF_INET => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(error_stack::Report::new(IocpError::InvalidInput)
                    .attach("invalid IPv4 sockaddr length"));
            }
            let sin_wrapped: &SockAddrIn = from_bytes(&buf[..std::mem::size_of::<SOCKADDR_IN>()]);
            Ok(SocketAddr::V4(sin_wrapped.to_std()))
        }
        AF_INET6 => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN6>() {
                return Err(error_stack::Report::new(IocpError::InvalidInput)
                    .attach("invalid IPv6 sockaddr length"));
            }
            let sin6_wrapped: &SockAddrIn6 =
                from_bytes(&buf[..std::mem::size_of::<SOCKADDR_IN6>()]);
            Ok(SocketAddr::V6(sin6_wrapped.to_std()))
        }
        _ => {
            Err(error_stack::Report::new(IocpError::InvalidInput)
                .attach("unsupported address family"))
        }
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
    type Error = IocpError;

    fn to_socket_addr(buf: &[u8]) -> IocpResult<SocketAddr> {
        to_socket_addr(buf).map_err(|e| e.attach("decode socket address failed"))
    }

    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len) {
        socket_addr_to_storage(addr)
    }
}
