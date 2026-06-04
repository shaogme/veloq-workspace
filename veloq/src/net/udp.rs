use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::net::common::{InnerSocket, SocketToken, SocketTokenPtr};
use crate::net::error::NetError;
use crate::runtime::context::RuntimeContext;
use diagweave::prelude::*;
use diagweave::report::Report;
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
) -> Result<InnerSocket<'a, 'ctx, P>> {
    let addr = addr
        .to_socket_addrs()
        .map_err(NetError::ToSocketAddrs)?
        .next()
        .ok_or(NetError::NoAddressProvided)?;

    let socket = if addr.is_ipv4() {
        Socket::new_udp_v4().trans()?
    } else {
        Socket::new_udp_v6().trans()?
    };

    socket.bind(addr).trans()?;
    let local_addr = socket.local_addr().trans()?;

    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), Some(local_addr))
}

impl<'a, 'ctx, S: OpSubmitter<'ctx, RuntimeContext<'a, 'ctx>> + Copy, P: SocketTokenPtr<'a, 'ctx>>
    GenericUdpSocket<'a, 'ctx, S, P>
{
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }

    async fn send_to_direct(&self, buf: FixedBuf, target: SocketAddr) -> Result<(usize, FixedBuf)> {
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
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| NetError::OpBufferLost.to_report())
            .trans()?;
        Ok((res.trans()?, buf))
    }

    async fn recv_from_direct(&self, buf: FixedBuf) -> Result<UdpRecvPacket> {
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
        let op_back = op_back_opt
            .ok_or_else(|| NetError::UdpRecvFromOpLost.to_report())
            .trans()?;
        let n = res.trans()?;
        let mut recv_buf = op_back.buf;
        recv_buf.set_len(n);
        let addr = op_back
            .addr
            .ok_or_else(|| NetError::UdpRecvFromMissingAddr.to_report())
            .trans()?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf(recv_buf),
            addr,
        })
    }

    async fn connect_direct(&self, addr: SocketAddr) -> Result<()> {
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
        res.map(|_| ()).trans()
    }

    async fn send_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
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
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| NetError::OpBufferLost.to_report())
            .trans()?;
        Ok((res.trans()?, buf))
    }

    async fn recv_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
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
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| NetError::OpBufferLost.to_report())
            .trans()?;
        Ok((res.trans()?, buf))
    }
}

impl<'a, 'ctx> LocalUdpSocket<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn send_to(&self, buf: FixedBuf, target: SocketAddr) -> Result<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> Result<UdpRecvPacket> {
        self.recv_from_direct(buf).await
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<()> {
        self.connect_direct(addr).await
    }

    pub async fn send(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }

    pub async fn recv_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }
}

impl<'a, 'ctx> UdpSocket<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn send_to(&self, buf: FixedBuf, target: SocketAddr) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = SendTo {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: target,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans()?, op.buf))
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> Result<UdpRecvPacket> {
        let owner = self.inner.owner_worker_id();
        let op = UdpRecvFrom {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: None,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let n = res.trans()?;
        let mut recv_buf = op.buf;
        recv_buf.set_len(n);
        let addr = op
            .addr
            .ok_or_else(|| NetError::UdpRecvFromMissingAddr.to_report())
            .trans()?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf(recv_buf),
            addr,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<()> {
        let owner = self.inner.owner_worker_id();
        let (raw_addr, raw_addr_len) = veloq_driver_native::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = UdpConnect {
            fd: self.inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = self.ctx.submit_to(owner, Op::new(op)).await?;
        res.map(|_| ()).trans()
    }

    pub async fn send(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpUdpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans()?, op.buf))
    }

    pub async fn recv_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpUdpRecv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans()?, op.buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for LocalUdpSocket<'a, 'ctx> {
    type Error = Report<Error>;

    async fn read(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv(buf).await
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(NetError::UnexpectedEof.to_report_trans());
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for UdpSocket<'a, 'ctx> {
    type Error = Report<Error>;

    async fn read(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv(buf).await
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(NetError::UnexpectedEof.to_report_trans());
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufWrite for LocalUdpSocket<'a, 'ctx> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send(buf).await
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(NetError::WriteZero.to_report_trans());
            }
            total += n;
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

impl<'a, 'ctx> crate::io::AsyncBufWrite for UdpSocket<'a, 'ctx> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send(buf).await
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(NetError::WriteZero.to_report_trans());
            }
            total += n;
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
