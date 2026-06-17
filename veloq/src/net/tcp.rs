use std::{
    mem::size_of,
    net::{SocketAddr, ToSocketAddrs},
    rc::Rc,
    sync::Arc,
};

use crate::{
    error::{Error, Result},
    io::{AsyncBufRead, AsyncBufWrite},
    net::{
        common::{InnerSocket, SocketToken, SocketTokenPtr},
        error::NetError,
    },
    runtime::context::Ctx,
};
use diagweave::{prelude::*, report::Report};
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    SockAddrStorage, Socket,
    op::{
        Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, Send as OpSend,
    },
    socket_addr_to_storage,
};

#[derive(Clone)]
pub struct GenericTcpListener<'rt, 'reg, S, P: SocketTokenPtr<'rt, 'reg>> {
    pub(crate) inner: InnerSocket<'rt, 'reg, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt, 'reg>,
}

#[derive(Clone)]
pub struct GenericTcpStream<'rt, 'reg, S, P: SocketTokenPtr<'rt, 'reg>> {
    pub(crate) inner: InnerSocket<'rt, 'reg, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt, 'reg>,
}

pub type LocalTcpListener<'rt, 'reg> =
    GenericTcpListener<'rt, 'reg, LocalSubmitter<Ctx<'rt, 'reg>>, Rc<SocketToken<'rt, 'reg>>>;
pub type LocalTcpStream<'rt, 'reg> =
    GenericTcpStream<'rt, 'reg, LocalSubmitter<Ctx<'rt, 'reg>>, Rc<SocketToken<'rt, 'reg>>>;

pub type TcpListener<'rt, 'reg> =
    GenericTcpListener<'rt, 'reg, DetachedSubmitter, Arc<SocketToken<'rt, 'reg>>>;
pub type TcpStream<'rt, 'reg> =
    GenericTcpStream<'rt, 'reg, DetachedSubmitter, Arc<SocketToken<'rt, 'reg>>>;

fn bind_listener_inner<'rt, 'reg, A: ToSocketAddrs, P: SocketTokenPtr<'rt, 'reg>>(
    ctx: Ctx<'rt, 'reg>,
    addr: A,
) -> Result<InnerSocket<'rt, 'reg, P>> {
    let addr = addr
        .to_socket_addrs()
        .map_err(NetError::ToSocketAddrs)?
        .next()
        .ok_or(NetError::NoAddressProvided)?;

    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().trans()?
    } else {
        Socket::new_tcp_v6().trans()?
    };

    socket.bind(addr).trans()?;
    socket.listen(1024).trans()?;
    let local_addr = socket.local_addr().trans()?;

    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), Some(local_addr))
}

fn new_stream_inner<'rt, 'reg, P: SocketTokenPtr<'rt, 'reg>>(
    ctx: Ctx<'rt, 'reg>,
    addr: &SocketAddr,
) -> Result<InnerSocket<'rt, 'reg, P>> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().trans()?
    } else {
        Socket::new_tcp_v6().trans()?
    };
    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), None)
}

impl<'rt, 'reg, S: OpSubmitter<'reg, Ctx<'rt, 'reg>> + Copy, P: SocketTokenPtr<'rt, 'reg>>
    GenericTcpListener<'rt, 'reg, S, P>
{
    async fn accept_direct(&self) -> Result<(GenericTcpStream<'rt, 'reg, S, P>, SocketAddr)> {
        let op = Accept {
            fd: self.inner.fd(),
            addr: SockAddrStorage::default(),
            addr_len: size_of::<SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op_back) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let op = op_back.ok_or(NetError::AcceptOpLost)?;

        let accepted = res.trans()?;
        let addr = op.remote_addr.ok_or(NetError::AcceptMissingRemoteAddr)?;

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

impl<'rt, 'reg, S: OpSubmitter<'reg, Ctx<'rt, 'reg>> + Copy, P: SocketTokenPtr<'rt, 'reg>>
    GenericTcpStream<'rt, 'reg, S, P>
{
    async fn connect_from_inner_direct(
        inner: InnerSocket<'rt, 'reg, P>,
        submitter: S,
        ctx: Ctx<'rt, 'reg>,
        addr: SocketAddr,
    ) -> Result<Self> {
        let (raw_addr, raw_addr_len) = socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };

        let (res, _) = ctx.submit(&submitter, Op::new(op)).await.into_inner();
        res.trans()?;

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
            .ok_or(NetError::OpBufferLost)
            .trans()?;
        Ok((res.trans()?, buf))
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
        let buf = op_back.map(|o| o.buf).ok_or(NetError::OpBufferLost)?;
        Ok((res.trans()?, buf))
    }
}

impl<'rt, 'reg> LocalTcpListener<'rt, 'reg> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt, 'reg>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(LocalTcpStream<'rt, 'reg>, SocketAddr)> {
        self.accept_direct().await
    }
}

impl<'rt, 'reg> TcpListener<'rt, 'reg> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt, 'reg>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(TcpStream<'rt, 'reg>, SocketAddr)> {
        let owner = self.inner.owner_worker_id();
        let op = Accept {
            fd: self.inner.fd(),
            addr: SockAddrStorage::default(),
            addr_len: size_of::<SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let accepted = res.trans()?;
        let addr = op.remote_addr.ok_or(NetError::AcceptMissingRemoteAddr)?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
            submitter: self.submitter,
            ctx: self.ctx,
        };

        Ok((stream, addr))
    }
}

impl<'rt, 'reg> LocalTcpStream<'rt, 'reg> {
    pub async fn connect(ctx: Ctx<'rt, 'reg>, addr: SocketAddr) -> Result<Self> {
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

impl<'rt, 'reg> TcpStream<'rt, 'reg> {
    pub async fn connect(ctx: Ctx<'rt, 'reg>, addr: SocketAddr) -> Result<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub(crate) async fn connect_from_inner(
        ctx: Ctx<'rt, 'reg>,
        inner: InnerSocket<'rt, 'reg, Arc<SocketToken<'rt, 'reg>>>,
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
        Ok((res.trans()?, op.buf))
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.trans()?, op.buf))
    }
}

impl<'rt, 'reg> AsyncBufRead for LocalTcpStream<'rt, 'reg> {
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
                return Err(NetError::UnexpectedEof)?;
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'rt, 'reg> AsyncBufRead for TcpStream<'rt, 'reg> {
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
                return Err(NetError::UnexpectedEof)?;
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'rt, 'reg> AsyncBufWrite for LocalTcpStream<'rt, 'reg> {
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
                return Err(NetError::WriteZero)?;
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

impl<'rt, 'reg> AsyncBufWrite for TcpStream<'rt, 'reg> {
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
                return Err(NetError::WriteZero)?;
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
