use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::net::common::InnerSocket;
use crate::runtime::context::submit;
use veloq_buf::buffer::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    Connect, DetachedSubmitter, IoFd, LocalSubmitter, Op, OpSubmitter, ReadFixed, RecvFrom, SendTo,
    WriteFixed,
};

// ============================================================================
// Generic UDP Socket
// ============================================================================

pub struct GenericUdpSocket<S: OpSubmitter> {
    inner: InnerSocket,
    submitter: S,
}

pub type LocalUdpSocket = GenericUdpSocket<LocalSubmitter>;
pub type UdpSocket = GenericUdpSocket<DetachedSubmitter>;

// ============================================================================
// Constructors
// ============================================================================

fn bind_inner<A: ToSocketAddrs>(addr: A) -> io::Result<InnerSocket> {
    let addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;

    let socket = if addr.is_ipv4() {
        Socket::new_udp_v4()?
    } else {
        Socket::new_udp_v6()?
    };

    socket.bind(addr)?;

    Ok(InnerSocket::new(socket.into_raw().into()))
}

impl LocalUdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: LocalSubmitter,
        })
    }
}

impl UdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: DetachedSubmitter::new()?,
        })
    }
}

// ============================================================================
// Shared Implementation
// ============================================================================

impl<S: OpSubmitter> GenericUdpSocket<S> {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> (io::Result<usize>, FixedBuf) {
        let op = SendTo {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            addr: target,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await;
        (res, op_back.buf)
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> (io::Result<(usize, SocketAddr)>, FixedBuf) {
        let op = RecvFrom {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            addr: None,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await;

        match res {
            Ok(n) => {
                let addr = op_back.addr.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
                (Ok((n, addr)), op_back.buf)
            }
            Err(e) => (Err(e), op_back.buf),
        }
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: IoFd::Raw(self.inner.raw()),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = submit(&self.submitter, Op::new(op)).await;
        res.map(|_| ())
    }

    pub async fn send(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = WriteFixed {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            offset: 0,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await;
        (res, op_back.buf)
    }

    pub async fn recv(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = ReadFixed {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            offset: 0,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await;
        (res, op_back.buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufRead for GenericUdpSocket<S> {
    fn read(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = (io::Result<usize>, FixedBuf)> {
        self.recv(buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufWrite for GenericUdpSocket<S> {
    fn write(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = (io::Result<usize>, FixedBuf)> {
        self.send(buf)
    }

    fn flush(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }
}
