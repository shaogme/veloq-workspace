use std::io;
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
    Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, Send as OpSend,
};

#[derive(Clone)]
pub struct GenericTcpListener<'a, 'ctx, S, P: SocketTokenPtr<'a, 'ctx>> {
    pub(crate) inner: InnerSocket<'a, 'ctx, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: RuntimeContext<'a, 'ctx>,
}

#[derive(Clone)]
pub struct GenericTcpStream<'a, 'ctx, S, P: SocketTokenPtr<'a, 'ctx>> {
    pub(crate) inner: InnerSocket<'a, 'ctx, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: RuntimeContext<'a, 'ctx>,
}

pub type LocalTcpListener<'a, 'ctx> = GenericTcpListener<
    'a,
    'ctx,
    LocalSubmitter<RuntimeContext<'a, 'ctx>>,
    Rc<SocketToken<'a, 'ctx>>,
>;
pub type LocalTcpStream<'a, 'ctx> =
    GenericTcpStream<'a, 'ctx, LocalSubmitter<RuntimeContext<'a, 'ctx>>, Rc<SocketToken<'a, 'ctx>>>;

pub type TcpListener<'a, 'ctx> =
    GenericTcpListener<'a, 'ctx, DetachedSubmitter, Arc<SocketToken<'a, 'ctx>>>;
pub type TcpStream<'a, 'ctx> =
    GenericTcpStream<'a, 'ctx, DetachedSubmitter, Arc<SocketToken<'a, 'ctx>>>;

fn bind_listener_inner<'a, 'ctx, A: ToSocketAddrs, P: SocketTokenPtr<'a, 'ctx>>(
    ctx: RuntimeContext<'a, 'ctx>,
    addr: A,
) -> Result<InnerSocket<'a, 'ctx, P>> {
    let addr = addr
        .to_socket_addrs()
        .to_report()
        .trans_inner_err()?
        .next()
        .ok_or_else(|| Report::new(Error::from(NetError::NoAddressProvided)))?;

    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().trans_inner_err()?
    } else {
        Socket::new_tcp_v6().trans_inner_err()?
    };

    socket.bind(addr).trans_inner_err()?;
    socket.listen(1024).trans_inner_err()?;
    let local_addr = socket.local_addr().trans_inner_err()?;

    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), Some(local_addr))
}

fn new_stream_inner<'a, 'ctx, P: SocketTokenPtr<'a, 'ctx>>(
    ctx: RuntimeContext<'a, 'ctx>,
    addr: &SocketAddr,
) -> Result<InnerSocket<'a, 'ctx, P>> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().trans_inner_err()?
    } else {
        Socket::new_tcp_v6().trans_inner_err()?
    };
    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), None)
}

impl<'a, 'ctx, S: OpSubmitter<'ctx, RuntimeContext<'a, 'ctx>> + Copy, P: SocketTokenPtr<'a, 'ctx>>
    GenericTcpListener<'a, 'ctx, S, P>
{
    async fn accept_direct(&self) -> Result<(GenericTcpStream<'a, 'ctx, S, P>, SocketAddr)> {
        let op = Accept {
            fd: self.inner.fd(),
            addr: veloq_driver_native::SockAddrStorage::default(),
            addr_len: std::mem::size_of::<veloq_driver_native::SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op_back) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let op = op_back
            .ok_or_else(|| NetError::AcceptOpLost.to_report())
            .trans_inner_err()?;

        let accepted = res.trans_inner_err()?;
        let addr = op
            .remote_addr
            .ok_or_else(|| NetError::AcceptMissingRemoteAddr.to_report())
            .trans_inner_err()?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
            submitter: self.submitter,
            ctx: self.ctx,
        };

        Ok((stream, addr))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl<'a, 'ctx, S: OpSubmitter<'ctx, RuntimeContext<'a, 'ctx>> + Copy, P: SocketTokenPtr<'a, 'ctx>>
    GenericTcpStream<'a, 'ctx, S, P>
{
    async fn connect_from_inner_direct(
        inner: InnerSocket<'a, 'ctx, P>,
        submitter: S,
        ctx: RuntimeContext<'a, 'ctx>,
        addr: SocketAddr,
    ) -> Result<Self> {
        let (raw_addr, raw_addr_len) = veloq_driver_native::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };

        let (res, _) = ctx.submit(&submitter, Op::new(op)).await.into_inner();
        res.trans_inner_err()?;

        Ok(Self {
            inner,
            submitter,
            ctx,
        })
    }

    async fn recv_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = Recv {
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
            .trans_inner_err()?;
        Ok((res.trans_inner_err()?, buf))
    }

    async fn send_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = OpSend {
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
            .trans_inner_err()?;
        Ok((res.trans_inner_err()?, buf))
    }
}

impl<'a, 'ctx> LocalTcpListener<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(LocalTcpStream<'a, 'ctx>, SocketAddr)> {
        self.accept_direct().await
    }
}

impl<'a, 'ctx> TcpListener<'a, 'ctx> {
    pub fn bind<A: ToSocketAddrs>(ctx: RuntimeContext<'a, 'ctx>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(TcpStream<'a, 'ctx>, SocketAddr)> {
        let owner = self.inner.owner_worker_id();
        let op = Accept {
            fd: self.inner.fd(),
            addr: veloq_driver_native::SockAddrStorage::default(),
            addr_len: std::mem::size_of::<veloq_driver_native::SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let accepted = res.trans_inner_err()?;
        let addr = op
            .remote_addr
            .ok_or_else(|| NetError::AcceptMissingRemoteAddr.to_report())
            .trans_inner_err()?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
            submitter: self.submitter,
            ctx: self.ctx,
        };

        Ok((stream, addr))
    }
}

impl<'a, 'ctx> LocalTcpStream<'a, 'ctx> {
    pub async fn connect(ctx: RuntimeContext<'a, 'ctx>, addr: SocketAddr) -> Result<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, LocalSubmitter::new(), ctx, addr).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }
}

impl<'a, 'ctx> TcpStream<'a, 'ctx> {
    pub async fn connect(ctx: RuntimeContext<'a, 'ctx>, addr: SocketAddr) -> Result<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub(crate) async fn connect_from_inner(
        ctx: RuntimeContext<'a, 'ctx>,
        inner: InnerSocket<'a, 'ctx, Arc<SocketToken<'a, 'ctx>>>,
        addr: SocketAddr,
    ) -> Result<Self> {
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = Recv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans_inner_err()?, op.buf))
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans_inner_err()?, op.buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for LocalTcpStream<'a, 'ctx> {
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
                return Err(Report::new(Error::from(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))));
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for TcpStream<'a, 'ctx> {
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
                return Err(Report::new(Error::from(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))));
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufWrite for LocalTcpStream<'a, 'ctx> {
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
                return Err(Report::new(Error::from(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ))));
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

impl<'a, 'ctx> crate::io::AsyncBufWrite for TcpStream<'a, 'ctx> {
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
                return Err(Report::new(Error::from(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ))));
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
