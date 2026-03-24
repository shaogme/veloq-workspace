use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;

use crate::net::common::InnerSocket;
use crate::runtime::context::submit;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, SendTo, UdpRecv as OpUdpRecv,
    UdpRecvPacket, UdpRecvStream, UdpSend as OpUdpSend,
};

// ============================================================================
// Generic UDP Socket
// ============================================================================

pub struct GenericUdpSocket<S: OpSubmitter> {
    pub(crate) inner: InnerSocket,
    pub(crate) submitter: S,
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

    Ok(InnerSocket::new(socket.into_owned_raw().into_raw()))
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
    pub async fn recv_ready(&self, _buf_capacity: NonZeroUsize, _credits: usize) -> io::Result<()> {
        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = SendTo {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: target,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res?, buf))
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> io::Result<UdpRecvPacket> {
        let op = UdpRecvStream {
            fd: self.inner.fd(),
            buf: Some(buf),
            addr: None,
            result: None,
        };
        let (res, op_back_opt) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let mut op_back = op_back_opt.ok_or_else(|| io::Error::other("UdpRecvStream op lost"))?;
        let n = res?;

        if let Some(datagram) = op_back.result.take() {
            return Ok(datagram);
        }

        let mut recv_buf = op_back
            .buf
            .take()
            .ok_or_else(|| io::Error::other("udp recv_stream buffer missing"))?;
        recv_buf.set_len(n);
        let addr = op_back.addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "driver must populate UdpRecvStream::addr before completion",
            )
        })?;
        Ok(UdpRecvPacket {
            buf: recv_buf,
            addr,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.connect(addr)
    }

    pub async fn send(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res?, buf))
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res?, buf))
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufRead for GenericUdpSocket<S> {
    fn read(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = io::Result<(usize, FixedBuf)>> {
        self.recv(buf)
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufWrite for GenericUdpSocket<S> {
    fn write(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = io::Result<(usize, FixedBuf)>> {
        self.send(buf)
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            total += n;
        }
        Ok((total, buf))
    }

    fn flush(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }
}
