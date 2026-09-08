use core::{
    cmp,
    mem::{self, MaybeUninit},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    ptr,
    time::Duration,
};

use windows_sys::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, ADDRINFOW, AF_INET, AF_INET6, FD_SET, FIONBIO, FreeAddrInfoW, GetAddrInfoW,
    IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0, INVALID_SOCKET, IP_ADD_MEMBERSHIP,
    IP_DROP_MEMBERSHIP, IP_MREQ, IP_MULTICAST_LOOP, IP_MULTICAST_TTL, IP_TTL, IPPROTO_IP,
    IPPROTO_IPV6, IPPROTO_TCP, IPV6_ADD_MEMBERSHIP, IPV6_DROP_MEMBERSHIP, IPV6_MREQ,
    IPV6_MULTICAST_LOOP, IPV6_V6ONLY, LINGER, MSG_PEEK, SD_BOTH, SD_RECEIVE, SD_SEND, SO_BROADCAST,
    SO_ERROR, SO_LINGER, SO_RCVTIMEO, SO_SNDTIMEO, SOCK_DGRAM, SOCK_STREAM, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_IN6_0, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, TCP_NODELAY,
    TIMEVAL, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED, WSABUF, WSAEINVAL, WSAEPROTOTYPE,
    WSAESHUTDOWN, WSAGetLastError, WSARecv, WSASend, WSASocketW, accept, bind, connect,
    getpeername, getsockname, getsockopt as win_getsockopt, ioctlsocket, listen, recv, recvfrom,
    select, send, sendto, setsockopt as win_setsockopt, shutdown,
};

use crate::{
    alloc_crate::vec::Vec,
    const_io_error,
    io::{Error, ErrorKind, IoSlice, IoSliceMut, Result},
    net::Shutdown,
    os::{
        cvt::{cvt_gai, cvt_socket},
        windows::{
            io::{
                AsRawSocket, AsSocket, BorrowedSocket, FromRawSocket, IntoRawSocket, OwnedSocket,
                RawSocket,
            },
            net,
        },
    },
};

#[repr(C)]
pub union SocketAddrCRepr {
    v4: SOCKADDR_IN,
    v6: SOCKADDR_IN6,
}

impl SocketAddrCRepr {
    #[inline]
    pub fn as_ptr(&self) -> *const SOCKADDR {
        self as *const _ as *const SOCKADDR
    }
}

pub fn socket_addr_to_c(addr: &SocketAddr) -> (SocketAddrCRepr, i32) {
    match addr {
        SocketAddr::V4(a) => {
            let sin = SOCKADDR_IN {
                sin_family: AF_INET as ADDRESS_FAMILY,
                sin_port: a.port().to_be(),
                sin_addr: IN_ADDR {
                    S_un: IN_ADDR_0 {
                        S_addr: u32::from_ne_bytes(a.ip().octets()),
                    },
                },
                sin_zero: [0; 8],
            };
            (SocketAddrCRepr { v4: sin }, size_of::<SOCKADDR_IN>() as i32)
        }
        SocketAddr::V6(a) => {
            let sin6 = SOCKADDR_IN6 {
                sin6_family: AF_INET6 as ADDRESS_FAMILY,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: IN6_ADDR {
                    u: IN6_ADDR_0 {
                        Byte: a.ip().octets(),
                    },
                },
                Anonymous: SOCKADDR_IN6_0 {
                    sin6_scope_id: a.scope_id(),
                },
            };
            (
                SocketAddrCRepr { v6: sin6 },
                size_of::<SOCKADDR_IN6>() as i32,
            )
        }
    }
}

/// Constructs a `SocketAddr` from a C `SOCKADDR` pointer.
///
/// # Safety
///
/// `storage` must point to a valid and readable `SOCKADDR` structure with at least `len` bytes.
pub unsafe fn socket_addr_from_c(storage: *const SOCKADDR, len: usize) -> Result<SocketAddr> {
    match unsafe { (*storage).sa_family } {
        AF_INET => {
            if len < size_of::<SOCKADDR_IN>() {
                return Err(const_io_error!(
                    ErrorKind::InvalidInput,
                    "invalid sockaddr length"
                ));
            }
            let sin = unsafe { *(storage as *const SOCKADDR_IN) };
            let ip = Ipv4Addr::from(unsafe { sin.sin_addr.S_un.S_addr }.to_ne_bytes());
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            if len < size_of::<SOCKADDR_IN6>() {
                return Err(const_io_error!(
                    ErrorKind::InvalidInput,
                    "invalid sockaddr length"
                ));
            }
            let sin6 = unsafe { *(storage as *const SOCKADDR_IN6) };
            let ip = Ipv6Addr::from(unsafe { sin6.sin6_addr.u.Byte });
            let port = u16::from_be(sin6.sin6_port);
            let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                scope_id,
            )))
        }
        _ => Err(const_io_error!(
            ErrorKind::InvalidInput,
            "invalid address family"
        )),
    }
}

/// Sets a socket option on this socket.
///
/// # Safety
///
/// The option value and type must be valid for the given socket option.
pub unsafe fn setsockopt<T>(
    sock: &Socket,
    level: i32,
    option_name: i32,
    option_value: T,
) -> Result<()> {
    let option_len = size_of::<T>() as i32;
    let ret = unsafe {
        win_setsockopt(
            sock.0.as_raw_socket() as usize as SOCKET,
            level,
            option_name,
            (&raw const option_value) as *const _,
            option_len,
        )
    };
    cvt_socket(ret)?;
    Ok(())
}

/// Gets a socket option from this socket.
///
/// # Safety
///
/// The socket option must be compatible with type `T`.
pub unsafe fn getsockopt<T: Copy>(sock: &Socket, level: i32, option_name: i32) -> Result<T> {
    let mut option_value = MaybeUninit::<T>::zeroed();
    let mut option_len = size_of::<T>() as i32;
    let ret = unsafe {
        win_getsockopt(
            sock.0.as_raw_socket() as usize as SOCKET,
            level,
            option_name,
            option_value.as_mut_ptr() as *mut _,
            &mut option_len,
        )
    };
    cvt_socket(ret)?;
    Ok(unsafe { option_value.assume_init() })
}

#[derive(Debug)]
pub struct Socket(pub(crate) OwnedSocket);

impl Socket {
    pub fn new(family: i32, ty: i32) -> Result<Self> {
        net::init();
        let socket = unsafe {
            WSASocketW(
                family,
                ty,
                0,
                ptr::null_mut(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
            )
        };
        if socket != INVALID_SOCKET {
            Ok(Self(unsafe {
                OwnedSocket::from_raw_socket(socket as RawSocket)
            }))
        } else {
            let error = unsafe { WSAGetLastError() };
            if error != WSAEPROTOTYPE && error != WSAEINVAL {
                return Err(Error::from_raw_os_error(error));
            }
            let socket =
                unsafe { WSASocketW(family, ty, 0, ptr::null_mut(), 0, WSA_FLAG_OVERLAPPED) };
            if socket == INVALID_SOCKET {
                return Err(net::last_error());
            }
            let owned = unsafe { OwnedSocket::from_raw_socket(socket as RawSocket) };
            owned.set_no_inherit()?;
            Ok(Self(owned))
        }
    }

    pub fn new_tcp(addr: &SocketAddr) -> Result<Self> {
        let family = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        Self::new(family as i32, SOCK_STREAM)
    }

    pub fn new_udp(addr: &SocketAddr) -> Result<Self> {
        let family = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        Self::new(family as i32, SOCK_DGRAM)
    }

    pub fn connect(&self, addr: &SocketAddr) -> Result<()> {
        let (c_addr, len) = socket_addr_to_c(addr);
        let ret = unsafe {
            connect(
                self.0.as_raw_socket() as usize as SOCKET,
                c_addr.as_ptr(),
                len,
            )
        };
        cvt_socket(ret).map(drop)
    }

    pub fn connect_timeout(&self, addr: &SocketAddr, timeout: Duration) -> Result<()> {
        self.set_nonblocking(true)?;
        let result = self.connect(addr);
        self.set_nonblocking(false)?;

        match result {
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                if timeout.as_secs() == 0 && timeout.subsec_nanos() == 0 {
                    return Err(const_io_error!(
                        ErrorKind::InvalidInput,
                        "cannot set a 0 duration timeout"
                    ));
                }

                let mut writefds = FD_SET {
                    fd_count: 1,
                    fd_array: [0; 64],
                };
                writefds.fd_array[0] = self.0.as_raw_socket() as usize as SOCKET;
                let mut errorfds = writefds;

                let timeout_tv = TIMEVAL {
                    tv_sec: cmp::min(timeout.as_secs(), i32::MAX as u64) as i32,
                    tv_usec: timeout.subsec_micros() as i32,
                };

                let count = unsafe {
                    select(
                        0,
                        ptr::null_mut(),
                        &mut writefds,
                        &mut errorfds,
                        &timeout_tv,
                    )
                };
                cvt_socket(count)?;

                if count == 0 {
                    return Err(const_io_error!(ErrorKind::TimedOut, "connection timed out"));
                }

                if (writefds.fd_count == 0 || errorfds.fd_count > 0)
                    && let Some(err) = self.take_error()?
                {
                    return Err(err);
                }

                Ok(())
            }
            _ => result,
        }
    }

    pub fn bind(&self, addr: &SocketAddr) -> Result<()> {
        let (c_addr, len) = socket_addr_to_c(addr);
        let ret = unsafe {
            bind(
                self.0.as_raw_socket() as usize as SOCKET,
                c_addr.as_ptr(),
                len,
            )
        };
        cvt_socket(ret).map(drop)
    }

    pub fn listen(&self, backlog: i32) -> Result<()> {
        let ret = unsafe { listen(self.0.as_raw_socket() as usize as SOCKET, backlog) };
        cvt_socket(ret).map(drop)
    }

    pub fn accept(&self) -> Result<(Self, SocketAddr)> {
        let mut storage = unsafe { mem::zeroed::<SOCKADDR_STORAGE>() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        let sock = unsafe {
            accept(
                self.0.as_raw_socket() as usize as SOCKET,
                (&raw mut storage) as *mut SOCKADDR,
                &mut len,
            )
        };
        if sock == INVALID_SOCKET {
            return Err(net::last_error());
        }
        let owned = unsafe { OwnedSocket::from_raw_socket(sock as RawSocket) };
        owned.set_no_inherit()?;
        let addr =
            unsafe { socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize)? };
        Ok((Self(owned), addr))
    }

    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            recv(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_mut_ptr() as *mut _,
                length,
                0,
            )
        };
        if ret == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            if err == WSAESHUTDOWN {
                Ok(0)
            } else {
                Err(Error::from_raw_os_error(err))
            }
        } else {
            Ok(ret as usize)
        }
    }

    pub fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut wsabufs = [WSABUF {
            len: 0,
            buf: ptr::null_mut(),
        }; 16];
        for i in 0..len {
            wsabufs[i] = WSABUF {
                len: cmp::min(bufs[i].len(), u32::MAX as usize) as u32,
                buf: bufs[i].as_mut_ptr(),
            };
        }
        let mut nread = 0;
        let mut flags = 0;
        let ret = unsafe {
            WSARecv(
                self.0.as_raw_socket() as usize as SOCKET,
                wsabufs.as_mut_ptr(),
                len as u32,
                &mut nread,
                &mut flags,
                ptr::null_mut(),
                None,
            )
        };
        if ret != 0 {
            let err = unsafe { WSAGetLastError() };
            if err == WSAESHUTDOWN {
                Ok(0)
            } else {
                Err(Error::from_raw_os_error(err))
            }
        } else {
            Ok(nread as usize)
        }
    }

    #[inline]
    pub fn is_read_vectored(&self) -> bool {
        true
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            send(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_ptr() as *const _,
                length,
                0,
            )
        };
        cvt_socket(ret).map(|n| n as usize)
    }

    pub fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut wsabufs = [WSABUF {
            len: 0,
            buf: ptr::null_mut(),
        }; 16];
        for i in 0..len {
            wsabufs[i] = WSABUF {
                len: cmp::min(bufs[i].len(), u32::MAX as usize) as u32,
                buf: bufs[i].as_ptr() as *mut u8,
            };
        }
        let mut nwritten = 0;
        let ret = unsafe {
            WSASend(
                self.0.as_raw_socket() as usize as SOCKET,
                wsabufs.as_ptr() as *mut _,
                len as u32,
                &mut nwritten,
                0,
                ptr::null_mut(),
                None,
            )
        };
        cvt_socket(ret)?;
        Ok(nwritten as usize)
    }

    #[inline]
    pub fn is_write_vectored(&self) -> bool {
        true
    }

    pub fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        self.read(buf)
    }

    pub fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            recv(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_mut_ptr() as *mut _,
                length,
                MSG_PEEK,
            )
        };
        if ret == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            if err == WSAESHUTDOWN {
                Ok(0)
            } else {
                Err(Error::from_raw_os_error(err))
            }
        } else {
            Ok(ret as usize)
        }
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let mut storage = unsafe { mem::zeroed::<SOCKADDR_STORAGE>() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            recvfrom(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_mut_ptr() as *mut _,
                length,
                0,
                (&raw mut storage) as *mut SOCKADDR,
                &mut len,
            )
        };
        if ret == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            if err == WSAESHUTDOWN {
                Ok((0, unsafe {
                    socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize)?
                }))
            } else {
                Err(Error::from_raw_os_error(err))
            }
        } else {
            let addr = unsafe {
                socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize)?
            };
            Ok((ret as usize, addr))
        }
    }

    pub fn peek_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let mut storage = unsafe { mem::zeroed::<SOCKADDR_STORAGE>() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            recvfrom(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_mut_ptr() as *mut _,
                length,
                MSG_PEEK,
                (&raw mut storage) as *mut SOCKADDR,
                &mut len,
            )
        };
        if ret == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            if err == WSAESHUTDOWN {
                Ok((0, unsafe {
                    socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize)?
                }))
            } else {
                Err(Error::from_raw_os_error(err))
            }
        } else {
            let addr = unsafe {
                socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize)?
            };
            Ok((ret as usize, addr))
        }
    }

    pub fn send(&self, buf: &[u8]) -> Result<usize> {
        self.write(buf)
    }

    pub fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> Result<usize> {
        let (c_addr, len) = socket_addr_to_c(dst);
        let length = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let ret = unsafe {
            sendto(
                self.0.as_raw_socket() as usize as SOCKET,
                buf.as_ptr() as *const _,
                length,
                0,
                c_addr.as_ptr(),
                len,
            )
        };
        cvt_socket(ret).map(|n| n as usize)
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let mut storage = unsafe { mem::zeroed::<SOCKADDR_STORAGE>() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        let ret = unsafe {
            getsockname(
                self.0.as_raw_socket() as usize as SOCKET,
                (&raw mut storage) as *mut SOCKADDR,
                &mut len,
            )
        };
        cvt_socket(ret)?;
        unsafe { socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize) }
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let mut storage = unsafe { mem::zeroed::<SOCKADDR_STORAGE>() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        let ret = unsafe {
            getpeername(
                self.0.as_raw_socket() as usize as SOCKET,
                (&raw mut storage) as *mut SOCKADDR,
                &mut len,
            )
        };
        cvt_socket(ret)?;
        unsafe { socket_addr_from_c((&raw const storage) as *const SOCKADDR, len as usize) }
    }

    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        let flag = match how {
            Shutdown::Read => SD_RECEIVE,
            Shutdown::Write => SD_SEND,
            Shutdown::Both => SD_BOTH,
        };
        let ret = unsafe { shutdown(self.0.as_raw_socket() as usize as SOCKET, flag) };
        cvt_socket(ret).map(drop)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        let mut mode = nonblocking as u32;
        let ret = unsafe {
            ioctlsocket(
                self.0.as_raw_socket() as usize as SOCKET,
                FIONBIO,
                &mut mode,
            )
        };
        cvt_socket(ret).map(drop)
    }

    pub fn take_error(&self) -> Result<Option<Error>> {
        let raw: i32 = unsafe { getsockopt(self, SOL_SOCKET, SO_ERROR)? };
        if raw == 0 {
            Ok(None)
        } else {
            Ok(Some(Error::from_raw_os_error(raw)))
        }
    }

    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.0.try_clone().map(Self)
    }

    pub fn set_timeout(&self, dur: Option<Duration>, kind: i32) -> Result<()> {
        let timeout = match dur {
            Some(dur) => {
                if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
                    return Err(const_io_error!(
                        ErrorKind::InvalidInput,
                        "cannot set a 0 duration timeout"
                    ));
                }
                let ms = dur.as_millis();
                let ms = cmp::min(ms, u32::MAX as u128) as u32;
                cmp::max(ms, 1)
            }
            None => 0,
        };
        unsafe { setsockopt(self, SOL_SOCKET, kind, timeout) }
    }

    pub fn timeout(&self, kind: i32) -> Result<Option<Duration>> {
        let raw: u32 = unsafe { getsockopt(self, SOL_SOCKET, kind)? };
        if raw == 0 {
            Ok(None)
        } else {
            Ok(Some(Duration::from_millis(raw as u64)))
        }
    }

    #[inline]
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.set_timeout(dur, SO_RCVTIMEO)
    }

    #[inline]
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.set_timeout(dur, SO_SNDTIMEO)
    }

    #[inline]
    pub fn read_timeout(&self) -> Result<Option<Duration>> {
        self.timeout(SO_RCVTIMEO)
    }

    #[inline]
    pub fn write_timeout(&self) -> Result<Option<Duration>> {
        self.timeout(SO_SNDTIMEO)
    }

    pub fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_TCP, TCP_NODELAY, nodelay as i32) }
    }

    pub fn nodelay(&self) -> Result<bool> {
        let raw: i32 = unsafe { getsockopt(self, IPPROTO_TCP, TCP_NODELAY)? };
        Ok(raw != 0)
    }

    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_TTL, ttl as i32) }
    }

    pub fn ttl(&self) -> Result<u32> {
        let raw: i32 = unsafe { getsockopt(self, IPPROTO_IP, IP_TTL)? };
        Ok(raw as u32)
    }

    pub fn set_only_v6(&self, only_v6: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_V6ONLY, only_v6 as i32) }
    }

    pub fn only_v6(&self) -> Result<bool> {
        let raw: i32 = unsafe { getsockopt(self, IPPROTO_IPV6, IPV6_V6ONLY)? };
        Ok(raw != 0)
    }

    pub fn set_broadcast(&self, broadcast: bool) -> Result<()> {
        unsafe { setsockopt(self, SOL_SOCKET, SO_BROADCAST, broadcast as i32) }
    }

    pub fn broadcast(&self) -> Result<bool> {
        let raw: i32 = unsafe { getsockopt(self, SOL_SOCKET, SO_BROADCAST)? };
        Ok(raw != 0)
    }

    pub fn set_linger(&self, linger: Option<Duration>) -> Result<()> {
        let linger = LINGER {
            l_onoff: linger.is_some() as u16,
            l_linger: linger.map_or(0, |dur| cmp::min(dur.as_secs(), u16::MAX as u64) as u16),
        };
        unsafe { setsockopt(self, SOL_SOCKET, SO_LINGER, linger) }
    }

    pub fn linger(&self) -> Result<Option<Duration>> {
        let val: LINGER = unsafe { getsockopt(self, SOL_SOCKET, SO_LINGER)? };
        Ok((val.l_onoff != 0).then(|| Duration::from_secs(val.l_linger as u64)))
    }

    pub fn set_multicast_loop_v4(&self, loop_v4: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_MULTICAST_LOOP, loop_v4 as u32) }
    }

    pub fn multicast_loop_v4(&self) -> Result<bool> {
        let raw: u32 = unsafe { getsockopt(self, IPPROTO_IP, IP_MULTICAST_LOOP)? };
        Ok(raw != 0)
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_MULTICAST_TTL, ttl) }
    }

    pub fn multicast_ttl_v4(&self) -> Result<u32> {
        let raw: u32 = unsafe { getsockopt(self, IPPROTO_IP, IP_MULTICAST_TTL)? };
        Ok(raw)
    }

    pub fn set_multicast_loop_v6(&self, loop_v6: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, loop_v6 as u32) }
    }

    pub fn multicast_loop_v6(&self) -> Result<bool> {
        let raw: u32 = unsafe { getsockopt(self, IPPROTO_IPV6, IPV6_MULTICAST_LOOP)? };
        Ok(raw != 0)
    }

    pub fn join_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        let mreq = IP_MREQ {
            imr_multiaddr: IN_ADDR {
                S_un: IN_ADDR_0 {
                    S_addr: u32::from_ne_bytes(multiaddr.octets()),
                },
            },
            imr_interface: IN_ADDR {
                S_un: IN_ADDR_0 {
                    S_addr: u32::from_ne_bytes(interface.octets()),
                },
            },
        };
        unsafe { setsockopt(self, IPPROTO_IP, IP_ADD_MEMBERSHIP, mreq) }
    }

    pub fn leave_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        let mreq = IP_MREQ {
            imr_multiaddr: IN_ADDR {
                S_un: IN_ADDR_0 {
                    S_addr: u32::from_ne_bytes(multiaddr.octets()),
                },
            },
            imr_interface: IN_ADDR {
                S_un: IN_ADDR_0 {
                    S_addr: u32::from_ne_bytes(interface.octets()),
                },
            },
        };
        unsafe { setsockopt(self, IPPROTO_IP, IP_DROP_MEMBERSHIP, mreq) }
    }

    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        let mreq = IPV6_MREQ {
            ipv6mr_multiaddr: IN6_ADDR {
                u: IN6_ADDR_0 {
                    Byte: multiaddr.octets(),
                },
            },
            ipv6mr_interface: interface,
        };
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_ADD_MEMBERSHIP, mreq) }
    }

    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        let mreq = IPV6_MREQ {
            ipv6mr_multiaddr: IN6_ADDR {
                u: IN6_ADDR_0 {
                    Byte: multiaddr.octets(),
                },
            },
            ipv6mr_interface: interface,
        };
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_DROP_MEMBERSHIP, mreq) }
    }

    #[inline]
    pub fn as_raw_socket(&self) -> RawSocket {
        self.0.as_raw_socket()
    }

    #[inline]
    pub fn into_raw_socket(self) -> RawSocket {
        self.0.into_raw_socket()
    }

    /// Constructs a `Socket` from a raw socket.
    ///
    /// # Safety
    ///
    /// `sock` must be a valid open socket handle and owned by the caller.
    #[inline]
    pub unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        Self(unsafe { OwnedSocket::from_raw_socket(sock) })
    }

    #[inline]
    pub fn as_socket(&self) -> BorrowedSocket<'_> {
        self.0.as_socket()
    }

    #[inline]
    pub fn from_inner(sock: OwnedSocket) -> Self {
        Self(sock)
    }

    #[inline]
    pub fn into_inner(self) -> OwnedSocket {
        self.0
    }
}

pub fn lookup_host(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
    net::init();
    let mut wide_host: Vec<u16> = host.encode_utf16().collect();
    wide_host.push(0);

    let mut hints = unsafe { mem::zeroed::<ADDRINFOW>() };
    hints.ai_socktype = SOCK_STREAM;

    let mut res = ptr::null_mut();
    cvt_gai(unsafe { GetAddrInfoW(wide_host.as_ptr(), ptr::null(), &hints, &mut res) })?;

    struct AddrInfoGuard(*mut ADDRINFOW);
    impl Drop for AddrInfoGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { FreeAddrInfoW(self.0) };
            }
        }
    }

    let _guard = AddrInfoGuard(res);
    let mut addrs = Vec::new();
    let mut cur = res;
    while !cur.is_null() {
        let info = unsafe { &*cur };
        if let Ok(mut addr) = unsafe { socket_addr_from_c(info.ai_addr, info.ai_addrlen) } {
            addr.set_port(port);
            addrs.push(addr);
        }
        cur = info.ai_next;
    }
    Ok(addrs)
}
