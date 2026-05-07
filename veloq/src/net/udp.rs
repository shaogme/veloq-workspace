use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error, to_io_error};
use crate::net::common::{InnerSocket, SocketToken, SocketTokenPtr};
use crate::net::route;
use crate::runtime::context::submit;
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

fn bind_inner<A: ToSocketAddrs, P: SocketTokenPtr>(addr: A) -> VeloqResult<InnerSocket<P>> {
    let addr = addr
        .to_socket_addrs()
        .map_err(from_io_error)?
        .next()
        .ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No address provided",
            ))
        })?;

    let socket = if addr.is_ipv4() {
        Socket::new_udp_v4().map_err(from_driver_report)?
    } else {
        Socket::new_udp_v6().map_err(from_driver_report)?
    };

    socket.bind(addr).map_err(from_driver_report)?;
    let local_addr = socket.local_addr().map_err(from_driver_report)?;

    InnerSocket::new(
        socket.into_owned_raw().into_raw(),
        Some(local_addr),
        veloq_runtime::runtime::current_worker_id(),
    )
}

impl<S: OpSubmitter + Copy, P: SocketTokenPtr> GenericUdpSocket<S, P> {
    pub async fn recv_ready(&self, buf_capacity: NonZeroUsize, credits: usize) -> VeloqResult<()> {
        if credits == 0 {
            return Ok(());
        }

        if let Some(ctx) = crate::runtime::context::try_current() {
            ctx.registrar().sync_to_driver();
            let driver = ctx.driver();
            return driver
                .borrow_mut()
                .warmup_udp_socket(self.inner.fd(), buf_capacity, credits)
                .map_err(from_driver_report);
        }

        Ok(())
    }

    pub fn local_addr(&self) -> VeloqResult<SocketAddr> {
        self.inner.local_addr()
    }

    async fn send_to_direct(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op = SendTo {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: target,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    async fn recv_stream_direct(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        let op = UdpRecvStream {
            fd: self.inner.fd(),
            buf: Some(buf),
            addr: None,
            result: None,
        };
        let (res, op_back_opt) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let mut op_back =
            op_back_opt.ok_or_else(|| from_io_error(io::Error::other("UdpRecvStream op lost")))?;
        let n = res.map_err(from_driver_report)?;

        if let Some(datagram) = op_back.result.take() {
            return Ok(datagram);
        }

        let mut recv_buf = op_back
            .buf
            .take()
            .ok_or_else(|| from_io_error(io::Error::other("udp recv_stream buffer missing")))?;
        recv_buf.set_len(n);
        let addr = op_back.addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "driver must populate UdpRecvStream::addr before completion",
            ))
        })?;
        Ok(UdpRecvPacket {
            buf: recv_buf,
            addr,
        })
    }

    async fn connect_direct(&self, addr: SocketAddr) -> VeloqResult<()> {
        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    async fn send_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    async fn recv_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }
}

impl LocalUdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: LocalSubmitter,
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        self.recv_stream_direct(buf).await
    }

    pub async fn connect(&self, addr: SocketAddr) -> VeloqResult<()> {
        self.connect_direct(addr).await
    }

    pub async fn send(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }
}

impl UdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: DetachedSubmitter::new(),
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        if veloq_runtime::runtime::current_worker_id() == owner {
            return self.send_to_direct(buf, target).await;
        }

        let op = SendTo {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: target,
        };
        let routed = route::route_udp_send_to(owner, op).map_err(from_io_error)?;
        let (res, op_back) = routed.await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        let owner = self.inner.owner_worker_id();
        if veloq_runtime::runtime::current_worker_id() == owner {
            return self.recv_stream_direct(buf).await;
        }

        let op = UdpRecvStream {
            fd: self.inner.fd(),
            buf: Some(buf),
            addr: None,
            result: None,
        };
        let routed = route::route_udp_recv_stream(owner, op).map_err(from_io_error)?;
        let (res, op_back_opt) = routed.await.into_inner();
        let mut op_back =
            op_back_opt.ok_or_else(|| from_io_error(io::Error::other("UdpRecvStream op lost")))?;
        let n = res.map_err(from_driver_report)?;

        if let Some(datagram) = op_back.result.take() {
            return Ok(datagram);
        }

        let mut recv_buf = op_back
            .buf
            .take()
            .ok_or_else(|| from_io_error(io::Error::other("udp recv_stream buffer missing")))?;
        recv_buf.set_len(n);
        let addr = op_back.addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "driver must populate UdpRecvStream::addr before completion",
            ))
        })?;
        Ok(UdpRecvPacket {
            buf: recv_buf,
            addr,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> VeloqResult<()> {
        let owner = self.inner.owner_worker_id();
        if veloq_runtime::runtime::current_worker_id() == owner {
            return self.connect_direct(addr).await;
        }

        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let routed = route::route_udp_connect(owner, op).map_err(from_io_error)?;
        let (res, _) = routed.await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub async fn send(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        if veloq_runtime::runtime::current_worker_id() == owner {
            return self.send_subset_direct(buf, buf_offset).await;
        }

        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let routed = route::route_udp_send(owner, op).map_err(from_io_error)?;
        let (res, op_back) = routed.await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        if veloq_runtime::runtime::current_worker_id() == owner {
            return self.recv_subset_direct(buf, buf_offset).await;
        }

        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let routed = route::route_udp_recv(owner, op).map_err(from_io_error)?;
        let (res, op_back) = routed.await.into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }
}

impl crate::io::AsyncBufRead for LocalUdpSocket {
    async fn read(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.recv(buf).await.map_err(to_io_error)
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await.map_err(to_io_error)?;
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
    async fn read(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.recv(buf).await.map_err(to_io_error)
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await.map_err(to_io_error)?;
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
    async fn write(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.send(buf).await.map_err(to_io_error)
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await.map_err(to_io_error)?;
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

    async fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> io::Result<()> {
        Ok(())
    }
}

impl crate::io::AsyncBufWrite for UdpSocket {
    async fn write(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.send(buf).await.map_err(to_io_error)
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await.map_err(to_io_error)?;
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

    async fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> io::Result<()> {
        Ok(())
    }
}
