use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_driver_core::net::SocketAddrCodec;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, IN6_ADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE,
};

/// A storage wrapper for socket addresses.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub SOCKADDR_STORAGE);

impl Default for SockAddrStorage {
    fn default() -> Self {
        // SAFETY: SOCKADDR_STORAGE can be safely zero-initialized.
        Self(unsafe { std::mem::zeroed() })
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
    // SAFETY: buffer length is checked above.
    let family = unsafe { *(buf.as_ptr() as *const u16) };
    match family {
        AF_INET => {
            if buf.len() < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid address length",
                ));
            }
            // SAFETY: buffer length is checked.
            let sin = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN) };
            // SAFETY: Accessing union field is safe because the address family is AF_INET.
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
            // SAFETY: buffer length is checked.
            let sin6 = unsafe { &*(buf.as_ptr() as *const SOCKADDR_IN6) };
            // SAFETY: Accessing union field is safe because the address family is AF_INET6.
            let addr_bytes = unsafe { sin6.sin6_addr.u.Byte };
            let ip = Ipv6Addr::from(addr_bytes);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = sin6.sin6_flowinfo;
            // SAFETY: Accessing union field is safe because the address family is AF_INET6.
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
            // SAFETY: storage is guaranteed to be large enough for SOCKADDR_IN.
            unsafe {
                (*sin_ptr).sin_family = AF_INET;
                (*sin_ptr).sin_port = a.port().to_be();
                (*sin_ptr).sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());
                std::mem::size_of::<SOCKADDR_IN>() as i32
            }
        }
        SocketAddr::V6(a) => {
            let sin6_ptr = &mut storage.0 as *mut _ as *mut SOCKADDR_IN6;
            // SAFETY: storage is large enough for SOCKADDR_IN6.
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
            // SAFETY: SOCKADDR_IN can be safely zero-initialized.
            let mut sin: SOCKADDR_IN = unsafe { std::mem::zeroed() };
            sin.sin_family = AF_INET;
            sin.sin_port = a.port().to_be();
            sin.sin_addr.S_un.S_addr = u32::from_ne_bytes(a.ip().octets());

            let ptr = &sin as *const _ as *const u8;
            let buf =
                // SAFETY: sin is a valid struct.
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(a) => {
            // SAFETY: SOCKADDR_IN6 can be safely zero-initialized.
            let mut sin6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = AF_INET6;
            sin6.sin6_port = a.port().to_be();
            // SAFETY: transmute for byte array to Win32 IN6_ADDR.
            sin6.sin6_addr = unsafe { std::mem::transmute::<[u8; 16], IN6_ADDR>(a.ip().octets()) };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.Anonymous.sin6_scope_id = a.scope_id();

            let ptr = &sin6 as *const _ as *const u8;
            let buf =
                // SAFETY: sin6 is a valid struct.
                unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<SOCKADDR_IN6>()) }
                    .to_vec();
            (buf, std::mem::size_of::<SOCKADDR_IN6>() as i32)
        }
    }
}
