use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use crate::net::common::{InnerSocket, SocketToken, SocketTokenPtr};
use crate::runtime::context::submit;
use error_stack::Report;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::driver::Driver;
use veloq_driver::op::{
    DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, SendTo, UdpConnect, UdpRecv as OpUdpRecv,
    UdpRecvPacket, UdpRecvStream, UdpSend as OpUdpSend,
};

#[derive(Clone)]
pub struct GenericUdpSocket<S: OpSubmitter, P: SocketTokenPtr> {
    pub(crate) inner: InnerSocket<P>,
    pub(crate) submitter: S,
}

pub type LocalUdpSocket = GenericUdpSocket<LocalSubmitter, Rc<SocketToken>>;
pub type UdpSocket = GenericUdpSocket<DetachedSubmitter, Arc<SocketToken>>;

#[inline]
fn driver_err<E>(err: Report<E>) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::other(err.to_string())
}

fn bind_inner<A: ToSocketAddrs, P: SocketTokenPtr>(addr: A) -> io::Result<InnerSocket<P>> {
    let addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;

    let socket = if addr.is_ipv4() {
        Socket::new_udp_v4().map_err(driver_err)?
    } else {
        Socket::new_udp_v6().map_err(driver_err)?
    };

    socket.bind(addr).map_err(driver_err)?;
    let local_addr = socket.local_addr().map_err(driver_err)?;

    InnerSocket::new(socket.into_owned_raw().into_raw(), Some(local_addr))
}

impl<S: OpSubmitter + Copy, P: SocketTokenPtr> GenericUdpSocket<S, P> {
    pub async fn recv_ready(&self, buf_capacity: NonZeroUsize, credits: usize) -> io::Result<()> {
        self.inner.ensure_affinity().await?;
        if credits == 0 {
            return Ok(());
        }

        if let Some(ctx) = crate::runtime::context::try_current() {
            ctx.registrar().sync_to_driver();
            let driver = ctx.driver();
            return driver
                .borrow_mut()
                .warmup_udp_socket(self.inner.fd(), buf_capacity, credits)
                .map_err(driver_err);
        }

        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    async fn send_to_direct(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> io::Result<(usize, FixedBuf)> {
        self.inner.ensure_affinity().await?;
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
        Ok((res.map_err(driver_err)?, buf))
    }

    async fn recv_stream_direct(&self, buf: FixedBuf) -> io::Result<UdpRecvPacket> {
        self.inner.ensure_affinity().await?;
        let op = UdpRecvStream {
            fd: self.inner.fd(),
            buf: Some(buf),
            addr: None,
            result: None,
        };
        let (res, op_back_opt) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let mut op_back = op_back_opt.ok_or_else(|| io::Error::other("UdpRecvStream op lost"))?;
        let n = res.map_err(driver_err)?;

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

    async fn connect_direct(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.ensure_affinity().await?;
        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(driver_err)
    }

    async fn send_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.inner.ensure_affinity().await?;
        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }

    async fn recv_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.inner.ensure_affinity().await?;
        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }
}

impl LocalUdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: LocalSubmitter,
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> io::Result<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> io::Result<UdpRecvPacket> {
        self.recv_stream_direct(buf).await
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.connect_direct(addr).await
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
        self.send_subset_direct(buf, buf_offset).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }
}

impl UdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: DetachedSubmitter::new(),
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> io::Result<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> io::Result<UdpRecvPacket> {
        self.recv_stream_direct(buf).await
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.connect_direct(addr).await
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
        self.send_subset_direct(buf, buf_offset).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }
}

impl crate::io::AsyncBufRead for LocalUdpSocket {
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

impl crate::io::AsyncBufRead for UdpSocket {
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

impl crate::io::AsyncBufWrite for LocalUdpSocket {
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

impl crate::io::AsyncBufWrite for UdpSocket {
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
