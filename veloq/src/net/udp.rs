use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error, to_io_error};
use crate::net::common::{InnerSocket, SocketToken, SocketTokenPtr};
use crate::runtime::context::RuntimeContext;
use veloq_buf::FixedBuf;
use veloq_driver_native::Socket;
use veloq_driver_native::op::{
    DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, SendTo, UdpConnect, UdpRecv as OpUdpRecv,
    UdpRecvFrom, UdpRecvPacket, UdpRecvPacketBuf, UdpSend as OpUdpSend,
};

#[derive(Clone)]
pub struct GenericUdpSocket<'a, 'ctx, S, P: SocketTokenPtr<'a, 'ctx>> {
    pub(crate) inner: InnerSocket<'a, 'ctx, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: RuntimeContext<'a, 'ctx>,
}

pub type LocalUdpSocket<'a, 'ctx> =
    GenericUdpSocket<'a, 'ctx, LocalSubmitter<RuntimeContext<'a, 'ctx>>, Rc<SocketToken<'a, 'ctx>>>;
pub type UdpSocket<'a, 'ctx> =
    GenericUdpSocket<'a, 'ctx, DetachedSubmitter, Arc<SocketToken<'a, 'ctx>>>;

fn bind_inner<'a, 'ctx, A: ToSocketAddrs, P: SocketTokenPtr<'a, 'ctx>>(
    ctx: RuntimeContext<'a, 'ctx>,
    addr: A,
) -> VeloqResult<InnerSocket<'a, 'ctx, P>> {
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

    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), Some(local_addr))
}

impl<'a, 'ctx, S: OpSubmitter<'a, RuntimeContext<'a, 'ctx>> + Copy, P: SocketTokenPtr<'a, 'ctx>>
    GenericUdpSocket<'a, 'ctx, S, P>
{
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
        let (res, op_back) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    async fn recv_from_direct(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        let op = UdpRecvFrom {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: None,
        };
        let (res, op_back_opt) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let op_back =
            op_back_opt.ok_or_else(|| from_io_error(io::Error::other("UdpRecvFrom op lost")))?;
        let n = res.map_err(from_driver_report)?;
        let mut recv_buf = op_back.buf;
        recv_buf.set_len(n);
        let addr = op_back.addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "driver must populate UdpRecvFrom::addr before completion",
            ))
        })?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf(recv_buf),
            addr,
        })
    }

    async fn connect_direct(&self, addr: SocketAddr) -> VeloqResult<()> {
        let (raw_addr, raw_addr_len) = veloq_driver_native::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
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
        let (res, op_back) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
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
        let (res, op_back) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }
}

impl<'a, 'ctx> LocalUdpSocket<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        self.recv_from_direct(buf).await
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

impl<'a, 'ctx> UdpSocket<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = SendTo {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: target,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.map_err(from_driver_report)?, op.buf))
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> VeloqResult<UdpRecvPacket> {
        let owner = self.inner.owner_worker_id();
        let op = UdpRecvFrom {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: None,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let n = res.map_err(from_driver_report)?;
        let mut recv_buf = op.buf;
        recv_buf.set_len(n);
        let addr = op.addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "driver must populate UdpRecvFrom::addr before completion",
            ))
        })?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf(recv_buf),
            addr,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> VeloqResult<()> {
        let owner = self.inner.owner_worker_id();
        let (raw_addr, raw_addr_len) = veloq_driver_native::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = self.ctx.submit_to(owner, Op::new(op)).await?;
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
        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.map_err(from_driver_report)?, op.buf))
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.map_err(from_driver_report)?, op.buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for LocalUdpSocket<'a, 'ctx> {
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

impl<'a, 'ctx> crate::io::AsyncBufRead for UdpSocket<'a, 'ctx> {
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

impl<'a, 'ctx> crate::io::AsyncBufWrite for LocalUdpSocket<'a, 'ctx> {
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

impl<'a, 'ctx> crate::io::AsyncBufWrite for UdpSocket<'a, 'ctx> {
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
