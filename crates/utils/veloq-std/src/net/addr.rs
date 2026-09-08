use core::{
    iter::Cloned,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    option, slice,
};

use crate::{
    alloc_crate::{string::String, vec},
    const_io_error,
    io::{ErrorKind, Result},
    net::sys,
};

/// A trait for objects which can be converted or resolved to one or more [`SocketAddr`] values.
pub trait ToSocketAddrs {
    /// Returned iterator over socket addresses which this type may correspond to.
    type Iter: Iterator<Item = SocketAddr>;

    /// Converts this object to an iterator of resolved [`SocketAddr`]s.
    fn to_socket_addrs(&self) -> Result<Self::Iter>;
}

impl ToSocketAddrs for SocketAddr {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        Ok(Some(*self).into_iter())
    }
}

impl ToSocketAddrs for SocketAddrV4 {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        SocketAddr::V4(*self).to_socket_addrs()
    }
}

impl ToSocketAddrs for SocketAddrV6 {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        SocketAddr::V6(*self).to_socket_addrs()
    }
}

impl ToSocketAddrs for (IpAddr, u16) {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        let (ip, port) = *self;
        match ip {
            IpAddr::V4(a) => (a, port).to_socket_addrs(),
            IpAddr::V6(a) => (a, port).to_socket_addrs(),
        }
    }
}

impl ToSocketAddrs for (Ipv4Addr, u16) {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        let (ip, port) = *self;
        SocketAddrV4::new(ip, port).to_socket_addrs()
    }
}

impl ToSocketAddrs for (Ipv6Addr, u16) {
    type Iter = option::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        let (ip, port) = *self;
        SocketAddrV6::new(ip, port, 0, 0).to_socket_addrs()
    }
}

impl ToSocketAddrs for (&str, u16) {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        let (host, port) = *self;
        if let Ok(addr) = host.parse::<Ipv4Addr>() {
            return Ok(vec![SocketAddr::V4(SocketAddrV4::new(addr, port))].into_iter());
        }
        if let Ok(addr) = host.parse::<Ipv6Addr>() {
            return Ok(vec![SocketAddr::V6(SocketAddrV6::new(addr, port, 0, 0))].into_iter());
        }
        sys::lookup_host(host, port).map(|v| v.into_iter())
    }
}

impl ToSocketAddrs for (String, u16) {
    type Iter = vec::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        (&*self.0, self.1).to_socket_addrs()
    }
}

impl ToSocketAddrs for str {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        if let Ok(addr) = self.parse::<SocketAddr>() {
            return Ok(vec![addr].into_iter());
        }

        let (host, port_str) = self
            .rsplit_once(':')
            .ok_or_else(|| const_io_error!(ErrorKind::InvalidInput, "invalid socket address"))?;
        let port = port_str
            .parse::<u16>()
            .map_err(|_| const_io_error!(ErrorKind::InvalidInput, "invalid port value"))?;
        (host, port).to_socket_addrs()
    }
}

impl<'a> ToSocketAddrs for &'a [SocketAddr] {
    type Iter = Cloned<slice::Iter<'a, SocketAddr>>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        Ok(self.iter().cloned())
    }
}

impl<T: ToSocketAddrs + ?Sized> ToSocketAddrs for &T {
    type Iter = T::Iter;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        (**self).to_socket_addrs()
    }
}

impl ToSocketAddrs for String {
    type Iter = vec::IntoIter<SocketAddr>;

    #[inline]
    fn to_socket_addrs(&self) -> Result<Self::Iter> {
        (**self).to_socket_addrs()
    }
}

pub(crate) fn each_addr<A: ToSocketAddrs, F, T>(addr: A, mut f: F) -> Result<T>
where
    F: FnMut(&SocketAddr) -> Result<T>,
{
    let mut last_err = None;
    for addr in addr.to_socket_addrs()? {
        match f(&addr) {
            Ok(l) => return Ok(l),
            Err(e) => last_err = Some(e),
        }
    }

    match last_err {
        Some(err) => Err(err),
        None => Err(const_io_error!(
            ErrorKind::InvalidInput,
            "could not resolve to any addresses"
        )),
    }
}
