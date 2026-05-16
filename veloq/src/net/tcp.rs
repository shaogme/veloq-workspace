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
    Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, Send as OpSend,
};

#[derive(Clone)]
pub struct GenericTcpListener<'a, S: OpSubmitter<'a, RuntimeContext<'a>>, P: SocketTokenPtr<'a>> {
    pub(crate) inner: InnerSocket<'a, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: &'a RuntimeContext<'a>,
}

#[derive(Clone)]
pub struct GenericTcpStream<'a, S: OpSubmitter<'a, RuntimeContext<'a>>, P: SocketTokenPtr<'a>> {
    pub(crate) inner: InnerSocket<'a, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: &'a RuntimeContext<'a>,
}

pub type LocalTcpListener<'a> =
    GenericTcpListener<'a, LocalSubmitter<RuntimeContext<'a>>, Rc<SocketToken<'a>>>;
pub type LocalTcpStream<'a> =
    GenericTcpStream<'a, LocalSubmitter<RuntimeContext<'a>>, Rc<SocketToken<'a>>>;

pub type TcpListener<'a> = GenericTcpListener<'a, DetachedSubmitter, Arc<SocketToken<'a>>>;
pub type TcpStream<'a> = GenericTcpStream<'a, DetachedSubmitter, Arc<SocketToken<'a>>>;

fn bind_listener_inner<'a, A: ToSocketAddrs, P: SocketTokenPtr<'a>>(
    ctx: &'a RuntimeContext<'a>,
    addr: A,
) -> VeloqResult<InnerSocket<'a, P>> {
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
        Socket::new_tcp_v4().map_err(from_driver_report)?
    } else {
        Socket::new_tcp_v6().map_err(from_driver_report)?
    };

    socket.bind(addr).map_err(from_driver_report)?;
    socket.listen(1024).map_err(from_driver_report)?;
    let local_addr = socket.local_addr().map_err(from_driver_report)?;

    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), Some(local_addr))
}

fn new_stream_inner<'a, P: SocketTokenPtr<'a>>(
    ctx: &'a RuntimeContext<'a>,
    addr: &SocketAddr,
) -> VeloqResult<InnerSocket<'a, P>> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().map_err(from_driver_report)?
    } else {
        Socket::new_tcp_v6().map_err(from_driver_report)?
    };
    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), None)
}

impl<'a, S: OpSubmitter<'a, RuntimeContext<'a>> + Copy, P: SocketTokenPtr<'a>>
    GenericTcpListener<'a, S, P>
{
    async fn accept_direct(&self) -> VeloqResult<(GenericTcpStream<'a, S, P>, SocketAddr)> {
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
        let op = op_back.ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Accept op lost"))
        })?;

        let accepted = res.map_err(from_driver_report)?;
        let addr = op.remote_addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "Accept completed without remote address",
            ))
        })?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
            submitter: self.submitter,
            ctx: self.ctx,
        };

        Ok((stream, addr))
    }

    pub fn local_addr(&self) -> VeloqResult<SocketAddr> {
        self.inner.local_addr()
    }
}

impl<'a, S: OpSubmitter<'a, RuntimeContext<'a>> + Copy, P: SocketTokenPtr<'a>>
    GenericTcpStream<'a, S, P>
{
    async fn connect_from_inner_direct(
        inner: InnerSocket<'a, P>,
        submitter: S,
        ctx: &'a RuntimeContext<'a>,
        addr: SocketAddr,
    ) -> VeloqResult<Self> {
        let (raw_addr, raw_addr_len) = veloq_driver_native::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };

        let (res, _) = ctx.submit(&submitter, Op::new(op)).await.into_inner();
        res.map_err(from_driver_report)?;

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
    ) -> VeloqResult<(usize, FixedBuf)> {
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
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    async fn send_subset_direct(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
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
        let buf = op_back.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }
}

impl<'a> LocalTcpListener<'a> {
    pub fn bind<A: ToSocketAddrs>(ctx: &'a RuntimeContext<'a>, addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> VeloqResult<(LocalTcpStream<'a>, SocketAddr)> {
        self.accept_direct().await
    }
}

impl<'a> TcpListener<'a> {
    pub fn bind<A: ToSocketAddrs>(ctx: &'a RuntimeContext<'a>, addr: A) -> VeloqResult<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> VeloqResult<(TcpStream<'a>, SocketAddr)> {
        let owner = self.inner.owner_worker_id();
        let op = Accept {
            fd: self.inner.fd(),
            addr: veloq_driver_native::SockAddrStorage::default(),
            addr_len: std::mem::size_of::<veloq_driver_native::SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let accepted = res.map_err(from_driver_report)?;
        let addr = op.remote_addr.ok_or_else(|| {
            from_io_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "Accept completed without remote address",
            ))
        })?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
            submitter: self.submitter,
            ctx: self.ctx,
        };

        Ok((stream, addr))
    }
}

impl<'a> LocalTcpStream<'a> {
    pub async fn connect(ctx: &'a RuntimeContext<'a>, addr: SocketAddr) -> VeloqResult<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, LocalSubmitter::new(), ctx, addr).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset_direct(buf, buf_offset).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }
}

impl<'a> TcpStream<'a> {
    pub async fn connect(ctx: &'a RuntimeContext<'a>, addr: SocketAddr) -> VeloqResult<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub(crate) async fn connect_from_inner(
        ctx: &'a RuntimeContext<'a>,
        inner: InnerSocket<'a, Arc<SocketToken<'a>>>,
        addr: SocketAddr,
    ) -> VeloqResult<Self> {
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub async fn recv(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send(&self, buf: FixedBuf) -> VeloqResult<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = Recv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.map_err(from_driver_report)?, op.buf))
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let owner = self.inner.owner_worker_id();
        let op = OpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        Ok((res.map_err(from_driver_report)?, op.buf))
    }
}

impl<'a> crate::io::AsyncBufRead for LocalTcpStream<'a> {
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

impl<'a> crate::io::AsyncBufRead for TcpStream<'a> {
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

impl<'a> crate::io::AsyncBufWrite for LocalTcpStream<'a> {
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

impl<'a> crate::io::AsyncBufWrite for TcpStream<'a> {
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
