use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::SocketAddrCodec;
use veloq_pod::{Pod, Zeroable, bytes_of, from_bytes};
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
        Self(unsafe { std::mem::zeroed() })
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrIn(pub SOCKADDR_IN);
unsafe impl Zeroable for SockAddrIn {}
unsafe impl Pod for SockAddrIn {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrIn6(pub SOCKADDR_IN6);
unsafe impl Zeroable for SockAddrIn6 {}
unsafe impl Pod for SockAddrIn6 {}

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
            let sin = &sin_wrapped.0;
            let s_addr = unsafe { sin.sin_addr.S_un.S_addr };
            let ip = Ipv4Addr::from(u32::from_be(s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
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
            let sin6 = &sin6_wrapped.0;
            let addr_bytes = unsafe { sin6.sin6_addr.u.Byte };
            let ip = Ipv6Addr::from(addr_bytes);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
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
            let sin_ptr = &mut storage.0 as *mut _ as *mut SOCKADDR_IN;
            unsafe {
                (*sin_ptr).sin_family = AF_INET;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<SOCKADDR_IN>() as i32
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage.0 as *mut _ as *mut SOCKADDR_IN6;
            unsafe {
                (*sin6_ptr).sin6_family = AF_INET6;
                (*sin6_ptr).sin6_port = a.port().to_be();
                (*sin6_ptr).sin6_addr = std::mem::transmute::<[u8; 16], IN6_ADDR>(a.ip().octets());
                (*sin6_ptr).sin6_flowinfo = a.flowinfo();
                (*sin6_ptr).Anonymous.sin6_scope_id = a.scope_id();
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

/// Helper function to convert SocketAddr to a byte buffer.
pub(crate) fn socket_addr_trans(addr: SocketAddr) -> (Vec<u8>, i32) {
    match addr {
        SocketAddr::V4(a) => {
            let mut sin: SOCKADDR_IN = unsafe { std::mem::zeroed() };
            sin.sin_family = AF_INET;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());

            let buf = bytes_of(&SockAddrIn(sin)).to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(a) => {
            let mut sin6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = AF_INET6;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr = unsafe { std::mem::transmute::<[u8; 16], IN6_ADDR>(a.ip().octets()) };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.Anonymous.sin6_scope_id = a.scope_id();

            let buf = bytes_of(&SockAddrIn6(sin6)).to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN6>() as i32)
        }
    }
}
