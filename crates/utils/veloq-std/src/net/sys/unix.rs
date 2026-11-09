use core::{
    cmp,
    mem::{self, MaybeUninit},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    ptr,
    time::Duration,
};

use crate::{
    alloc_crate::{ffi::CString, vec::Vec},
    const_io_error,
    io::{Error, ErrorKind, IoSlice, IoSliceMut, Result},
    net::Shutdown,
    os::{
        Cvt,
        unix::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
    },
    time::Instant,
};
use libc::{
    AF_INET, AF_INET6, EINPROGRESS, EISCONN, F_GETFL, F_SETFL, IP_ADD_MEMBERSHIP,
    IP_DROP_MEMBERSHIP, IP_MULTICAST_LOOP, IP_MULTICAST_TTL, IP_TTL, IPPROTO_IP, IPPROTO_IPV6,
    IPPROTO_TCP, IPV6_ADD_MEMBERSHIP, IPV6_DROP_MEMBERSHIP, IPV6_MULTICAST_LOOP, IPV6_V6ONLY,
    MSG_NOSIGNAL, MSG_PEEK, O_NONBLOCK, POLLERR, POLLHUP, POLLOUT, SHUT_RD, SHUT_RDWR, SHUT_WR,
    SO_BROADCAST, SO_ERROR, SO_LINGER, SO_RCVTIMEO, SO_REUSEADDR, SO_SNDTIMEO, SOCK_CLOEXEC,
    SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET, TCP_NODELAY, accept4, addrinfo, bind, c_int, c_void,
    connect, fcntl, freeaddrinfo, getaddrinfo, getpeername, getsockname,
    getsockopt as libc_getsockopt, in_addr, in6_addr, iovec, ip_mreq, ipv6_mreq, linger, listen,
    msghdr, poll, pollfd, read, readv, recv, recvfrom, sa_family_t, send, sendmsg, sendto,
    setsockopt as libc_setsockopt, shutdown, size_t, sockaddr, sockaddr_in, sockaddr_in6,
    sockaddr_storage, socket, socklen_t, ssize_t, timeval,
};

#[derive(Debug)]
pub struct Socket(OwnedFd);

#[repr(C)]
pub union SocketAddrCRepr {
    v4: sockaddr_in,
    v6: sockaddr_in6,
}

impl SocketAddrCRepr {
    #[inline]
    pub fn as_ptr(&self) -> *const sockaddr {
        self as *const _ as *const sockaddr
    }
}

fn socket_addr_to_c(addr: &SocketAddr) -> (SocketAddrCRepr, socklen_t) {
    match addr {
        SocketAddr::V4(a) => {
            let sin = sockaddr_in {
                sin_family: AF_INET as sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: in_addr {
                    s_addr: u32::from_ne_bytes(a.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            (
                SocketAddrCRepr { v4: sin },
                size_of::<sockaddr_in>() as socklen_t,
            )
        }
        SocketAddr::V6(a) => {
            let sin6 = sockaddr_in6 {
                sin6_family: AF_INET6 as sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: in6_addr {
                    s6_addr: a.ip().octets(),
                },
                sin6_scope_id: a.scope_id(),
            };
            (
                SocketAddrCRepr { v6: sin6 },
                size_of::<sockaddr_in6>() as socklen_t,
            )
        }
    }
}

/// Constructs a `SocketAddr` from a C `sockaddr` pointer.
///
/// # Safety
///
/// `storage` must point to a valid and readable `sockaddr` structure with at least `len` bytes.
unsafe fn socket_addr_from_c(storage: *const sockaddr, len: usize) -> Result<SocketAddr> {
    let family = unsafe { (*storage).sa_family as c_int };
    match family {
        AF_INET => {
            if len < size_of::<sockaddr_in>() {
                return Err(const_io_error!(
                    ErrorKind::InvalidInput,
                    "invalid sockaddr length"
                ));
            }
            let sin = unsafe { *(storage as *const sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        AF_INET6 => {
            if len < size_of::<sockaddr_in6>() {
                return Err(const_io_error!(
                    ErrorKind::InvalidInput,
                    "invalid sockaddr length"
                ));
            }
            let sin6 = unsafe { *(storage as *const sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
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
unsafe fn setsockopt<T>(
    sock: &Socket,
    level: c_int,
    option_name: c_int,
    option_value: T,
) -> Result<()> {
    let option_len = size_of::<T>() as socklen_t;
    Cvt::cvt(unsafe {
        libc_setsockopt(
            sock.0.as_raw_fd(),
            level,
            option_name,
            (&raw const option_value) as *const c_void,
            option_len,
        )
    })?;
    Ok(())
}

/// Gets a socket option from this socket.
///
/// # Safety
///
/// The socket option must be compatible with type `T`.
unsafe fn getsockopt<T: Copy>(sock: &Socket, level: c_int, option_name: c_int) -> Result<T> {
    let mut option_value = MaybeUninit::<T>::zeroed();
    let mut option_len = size_of::<T>() as socklen_t;
    Cvt::cvt(unsafe {
        libc_getsockopt(
            sock.0.as_raw_fd(),
            level,
            option_name,
            option_value.as_mut_ptr() as *mut c_void,
            &mut option_len,
        )
    })?;
    Ok(unsafe { option_value.assume_init() })
}

impl Socket {
    pub fn new(family: c_int, ty: c_int) -> Result<Self> {
        let fd = Cvt::cvt(unsafe { socket(family, ty | SOCK_CLOEXEC, 0) })?;
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    pub fn new_tcp(addr: &SocketAddr) -> Result<Self> {
        let family = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        Self::new(family, SOCK_STREAM)
    }

    pub fn new_udp(addr: &SocketAddr) -> Result<Self> {
        let family = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        Self::new(family, SOCK_DGRAM)
    }

    pub fn connect(&self, addr: &SocketAddr) -> Result<()> {
        let (c_addr, len) = socket_addr_to_c(addr);
        loop {
            let ret = unsafe { connect(self.0.as_raw_fd(), c_addr.as_ptr(), len) };
            if ret < 0 {
                let err = Error::last_os_error();
                if err.is_interrupted() {
                    continue;
                }
                if err.raw_os_error() == Some(EISCONN) {
                    return Ok(());
                }
                return Err(err);
            }
            return Ok(());
        }
    }

    pub fn connect_timeout(&self, addr: &SocketAddr, timeout: Duration) -> Result<()> {
        self.set_nonblocking(true)?;
        let (c_addr, len) = socket_addr_to_c(addr);
        let ret = unsafe { connect(self.0.as_raw_fd(), c_addr.as_ptr(), len) };
        let result = if ret < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        };
        self.set_nonblocking(false)?;

        match result {
            Ok(()) => Ok(()),
            Err(ref e) if e.raw_os_error() == Some(EINPROGRESS) => {
                if timeout.as_secs() == 0 && timeout.subsec_nanos() == 0 {
                    return Err(const_io_error!(
                        ErrorKind::InvalidInput,
                        "cannot set a 0 duration timeout"
                    ));
                }

                let mut pollfd = pollfd {
                    fd: self.0.as_raw_fd(),
                    events: POLLOUT,
                    revents: 0,
                };

                let start = Instant::now();
                loop {
                    let elapsed = start.elapsed();
                    if elapsed >= timeout {
                        return Err(const_io_error!(ErrorKind::TimedOut, "connection timed out"));
                    }

                    let remaining = timeout - elapsed;
                    let timeout_ms = cmp::min(
                        remaining
                            .as_secs()
                            .saturating_mul(1000)
                            .saturating_add(remaining.subsec_millis() as u64),
                        c_int::MAX as u64,
                    ) as c_int;
                    let timeout_ms = cmp::max(timeout_ms, 1);

                    match unsafe { poll(&mut pollfd, 1, timeout_ms) } {
                        -1 => {
                            let err = Error::last_os_error();
                            if !err.is_interrupted() {
                                return Err(err);
                            }
                        }
                        0 => {
                            return Err(const_io_error!(
                                ErrorKind::TimedOut,
                                "connection timed out"
                            ));
                        }
                        _ => {
                            if pollfd.revents & (POLLHUP | POLLERR) != 0 {
                                let err = self.take_error()?.unwrap_or_else(|| {
                                    const_io_error!(
                                        ErrorKind::Uncategorized,
                                        "no error set after POLLHUP"
                                    )
                                });
                                return Err(err);
                            }
                            return Ok(());
                        }
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn bind(&self, addr: &SocketAddr) -> Result<()> {
        unsafe { setsockopt(self, SOL_SOCKET, SO_REUSEADDR, 1 as c_int)? };
        let (c_addr, len) = socket_addr_to_c(addr);
        Cvt::cvt(unsafe { bind(self.0.as_raw_fd(), c_addr.as_ptr(), len) })?;
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> Result<()> {
        Cvt::cvt(unsafe { listen(self.0.as_raw_fd(), backlog as c_int) })?;
        Ok(())
    }

    pub fn accept(&self) -> Result<(Self, SocketAddr)> {
        let mut storage = MaybeUninit::<sockaddr_storage>::uninit();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        let fd = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                accept4(
                    self.0.as_raw_fd(),
                    storage.as_mut_ptr() as *mut sockaddr,
                    &mut len,
                    SOCK_CLOEXEC,
                )
            })
        })?;
        let addr =
            unsafe { socket_addr_from_c(storage.as_ptr() as *const sockaddr, len as usize)? };
        Ok((Self(unsafe { OwnedFd::from_raw_fd(fd) }), addr))
    }

    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                read(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr() as *mut c_void,
                    cmp::min(buf.len(), ssize_t::MAX as usize),
                )
            })
        })?;
        Ok(ret as usize)
    }

    pub fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = iovec {
                iov_base: bufs[i].as_mut_ptr() as *mut c_void,
                iov_len: bufs[i].len(),
            };
        }
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe { readv(self.0.as_raw_fd(), iovecs.as_ptr(), len as c_int) })
        })?;
        Ok(ret as usize)
    }

    #[inline]
    pub fn is_read_vectored(&self) -> bool {
        true
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let len = cmp::min(buf.len(), size_t::MAX);
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                send(
                    self.0.as_raw_fd(),
                    buf.as_ptr() as *const c_void,
                    len,
                    MSG_NOSIGNAL,
                )
            })
        })?;
        Ok(ret as usize)
    }

    pub fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = iovec {
                iov_base: bufs[i].as_ptr() as *mut c_void,
                iov_len: bufs[i].len(),
            };
        }
        let mut msghdr = unsafe { mem::zeroed::<msghdr>() };
        msghdr.msg_iov = iovecs.as_mut_ptr();
        msghdr.msg_iovlen = len as _;
        let ret =
            Cvt::cvt_r(|| Cvt::cvt(unsafe { sendmsg(self.0.as_raw_fd(), &msghdr, MSG_NOSIGNAL) }))?;
        Ok(ret as usize)
    }

    #[inline]
    pub fn is_write_vectored(&self) -> bool {
        true
    }

    pub fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                recv(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr() as *mut c_void,
                    cmp::min(buf.len(), size_t::MAX),
                    0,
                )
            })
        })?;
        Ok(ret as usize)
    }

    pub fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                recv(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr() as *mut c_void,
                    cmp::min(buf.len(), size_t::MAX),
                    MSG_PEEK,
                )
            })
        })?;
        Ok(ret as usize)
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let mut storage = MaybeUninit::<sockaddr_storage>::uninit();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                recvfrom(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr() as *mut c_void,
                    cmp::min(buf.len(), size_t::MAX),
                    0,
                    storage.as_mut_ptr() as *mut sockaddr,
                    &mut len,
                )
            })
        })?;
        let addr =
            unsafe { socket_addr_from_c(storage.as_ptr() as *const sockaddr, len as usize)? };
        Ok((ret as usize, addr))
    }

    pub fn peek_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let mut storage = MaybeUninit::<sockaddr_storage>::uninit();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                recvfrom(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr() as *mut c_void,
                    cmp::min(buf.len(), size_t::MAX),
                    MSG_PEEK,
                    storage.as_mut_ptr() as *mut sockaddr,
                    &mut len,
                )
            })
        })?;
        let addr =
            unsafe { socket_addr_from_c(storage.as_ptr() as *const sockaddr, len as usize)? };
        Ok((ret as usize, addr))
    }

    pub fn send(&self, buf: &[u8]) -> Result<usize> {
        self.write(buf)
    }

    pub fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> Result<usize> {
        let (c_addr, len) = socket_addr_to_c(dst);
        let ret = Cvt::cvt_r(|| {
            Cvt::cvt(unsafe {
                sendto(
                    self.0.as_raw_fd(),
                    buf.as_ptr() as *const c_void,
                    cmp::min(buf.len(), size_t::MAX),
                    MSG_NOSIGNAL,
                    c_addr.as_ptr(),
                    len,
                )
            })
        })?;
        Ok(ret as usize)
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let mut storage = MaybeUninit::<sockaddr_storage>::zeroed();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        Cvt::cvt(unsafe {
            getsockname(
                self.0.as_raw_fd(),
                storage.as_mut_ptr() as *mut sockaddr,
                &mut len,
            )
        })?;
        unsafe { socket_addr_from_c(storage.as_ptr() as *const sockaddr, len as usize) }
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let mut storage = MaybeUninit::<sockaddr_storage>::zeroed();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;
        Cvt::cvt(unsafe {
            getpeername(
                self.0.as_raw_fd(),
                storage.as_mut_ptr() as *mut sockaddr,
                &mut len,
            )
        })?;
        unsafe { socket_addr_from_c(storage.as_ptr() as *const sockaddr, len as usize) }
    }

    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        let flag = match how {
            Shutdown::Read => SHUT_RD,
            Shutdown::Write => SHUT_WR,
            Shutdown::Both => SHUT_RDWR,
        };
        Cvt::cvt(unsafe { shutdown(self.0.as_raw_fd(), flag) })?;
        Ok(())
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        let mut flags = Cvt::cvt(unsafe { fcntl(self.0.as_raw_fd(), F_GETFL) })?;
        if nonblocking {
            flags |= O_NONBLOCK;
        } else {
            flags &= !O_NONBLOCK;
        }
        Cvt::cvt(unsafe { fcntl(self.0.as_raw_fd(), F_SETFL, flags) })?;
        Ok(())
    }

    pub fn take_error(&self) -> Result<Option<Error>> {
        let raw: c_int = unsafe { getsockopt(self, SOL_SOCKET, SO_ERROR)? };
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

    pub fn set_timeout(&self, dur: Option<Duration>, kind: c_int) -> Result<()> {
        let tv = match dur {
            Some(dur) => {
                if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
                    return Err(const_io_error!(
                        ErrorKind::InvalidInput,
                        "cannot set a 0 duration timeout"
                    ));
                }
                timeval {
                    tv_sec: dur.as_secs().min(i64::MAX as u64) as _,
                    tv_usec: dur.subsec_micros() as _,
                }
            }
            None => timeval {
                tv_sec: 0,
                tv_usec: 0,
            },
        };
        unsafe { setsockopt(self, SOL_SOCKET, kind, tv) }
    }

    pub fn timeout(&self, kind: c_int) -> Result<Option<Duration>> {
        let tv: timeval = unsafe { getsockopt(self, SOL_SOCKET, kind)? };
        if tv.tv_sec == 0 && tv.tv_usec == 0 {
            Ok(None)
        } else {
            Ok(Some(Duration::new(
                tv.tv_sec as u64,
                (tv.tv_usec as u32) * 1000,
            )))
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
        unsafe { setsockopt(self, IPPROTO_TCP, TCP_NODELAY, nodelay as c_int) }
    }

    pub fn nodelay(&self) -> Result<bool> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_TCP, TCP_NODELAY)? };
        Ok(raw != 0)
    }

    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_TTL, ttl as c_int) }
    }

    pub fn ttl(&self) -> Result<u32> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_IP, IP_TTL)? };
        Ok(raw as u32)
    }

    pub fn set_only_v6(&self, only_v6: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_V6ONLY, only_v6 as c_int) }
    }

    pub fn only_v6(&self) -> Result<bool> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_IPV6, IPV6_V6ONLY)? };
        Ok(raw != 0)
    }

    pub fn set_broadcast(&self, broadcast: bool) -> Result<()> {
        unsafe { setsockopt(self, SOL_SOCKET, SO_BROADCAST, broadcast as c_int) }
    }

    pub fn broadcast(&self) -> Result<bool> {
        let raw: c_int = unsafe { getsockopt(self, SOL_SOCKET, SO_BROADCAST)? };
        Ok(raw != 0)
    }

    pub fn set_linger(&self, linger: Option<Duration>) -> Result<()> {
        let linger = linger {
            l_onoff: linger.is_some() as c_int,
            l_linger: linger.map_or(0, |dur| dur.as_secs() as c_int),
        };
        unsafe { setsockopt(self, SOL_SOCKET, SO_LINGER, linger) }
    }

    pub fn linger(&self) -> Result<Option<Duration>> {
        let val: linger = unsafe { getsockopt(self, SOL_SOCKET, SO_LINGER)? };
        Ok((val.l_onoff != 0).then(|| Duration::from_secs(val.l_linger as u64)))
    }

    pub fn set_multicast_loop_v4(&self, loop_v4: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_MULTICAST_LOOP, loop_v4 as c_int) }
    }

    pub fn multicast_loop_v4(&self) -> Result<bool> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_IP, IP_MULTICAST_LOOP)? };
        Ok(raw != 0)
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IP, IP_MULTICAST_TTL, ttl as c_int) }
    }

    pub fn multicast_ttl_v4(&self) -> Result<u32> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_IP, IP_MULTICAST_TTL)? };
        Ok(raw as u32)
    }

    pub fn set_multicast_loop_v6(&self, loop_v6: bool) -> Result<()> {
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, loop_v6 as c_int) }
    }

    pub fn multicast_loop_v6(&self) -> Result<bool> {
        let raw: c_int = unsafe { getsockopt(self, IPPROTO_IPV6, IPV6_MULTICAST_LOOP)? };
        Ok(raw != 0)
    }

    pub fn join_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        let mreq = ip_mreq {
            imr_multiaddr: in_addr {
                s_addr: u32::from_ne_bytes(multiaddr.octets()),
            },
            imr_interface: in_addr {
                s_addr: u32::from_ne_bytes(interface.octets()),
            },
        };
        unsafe { setsockopt(self, IPPROTO_IP, IP_ADD_MEMBERSHIP, mreq) }
    }

    pub fn leave_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        let mreq = ip_mreq {
            imr_multiaddr: in_addr {
                s_addr: u32::from_ne_bytes(multiaddr.octets()),
            },
            imr_interface: in_addr {
                s_addr: u32::from_ne_bytes(interface.octets()),
            },
        };
        unsafe { setsockopt(self, IPPROTO_IP, IP_DROP_MEMBERSHIP, mreq) }
    }

    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        let mreq = ipv6_mreq {
            ipv6mr_multiaddr: in6_addr {
                s6_addr: multiaddr.octets(),
            },
            ipv6mr_interface: interface as _,
        };
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_ADD_MEMBERSHIP, mreq) }
    }

    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        let mreq = ipv6_mreq {
            ipv6mr_multiaddr: in6_addr {
                s6_addr: multiaddr.octets(),
            },
            ipv6mr_interface: interface as _,
        };
        unsafe { setsockopt(self, IPPROTO_IPV6, IPV6_DROP_MEMBERSHIP, mreq) }
    }

    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

    #[inline]
    pub fn into_raw_fd(self) -> RawFd {
        self.0.into_raw_fd()
    }

    /// Constructs a `Socket` from a raw file descriptor.
    ///
    /// # Safety
    ///
    /// `fd` must be an open, valid file descriptor suitable for assuming ownership.
    #[inline]
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    #[inline]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }

    #[inline]
    pub fn from_inner(fd: OwnedFd) -> Self {
        Self(fd)
    }

    #[inline]
    pub fn into_inner(self) -> OwnedFd {
        self.0
    }
}

fn lookup_host(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
    let c_host = CString::new(host)
        .map_err(|_| const_io_error!(ErrorKind::InvalidInput, "host contains null byte"))?;

    let mut hints = unsafe { mem::zeroed::<addrinfo>() };
    hints.ai_socktype = SOCK_STREAM;

    let mut res = ptr::null_mut();
    Cvt::cvt_gai(unsafe { getaddrinfo(c_host.as_ptr(), ptr::null(), &hints, &mut res) })?;

    struct AddrInfoGuard(*mut addrinfo);
    impl Drop for AddrInfoGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { freeaddrinfo(self.0) };
            }
        }
    }

    let _guard = AddrInfoGuard(res);
    let mut addrs = Vec::new();
    let mut cur = res;
    while !cur.is_null() {
        let info = unsafe { &*cur };
        if let Ok(mut addr) =
            unsafe { socket_addr_from_c(info.ai_addr as *const _, info.ai_addrlen as usize) }
        {
            addr.set_port(port);
            addrs.push(addr);
        }
        cur = info.ai_next;
    }
    Ok(addrs)
}

impl Socket {
    pub(crate) fn lookup_host(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
        lookup_host(host, port)
    }
}
