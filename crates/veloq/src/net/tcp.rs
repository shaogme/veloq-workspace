use veloq_std::{
    future::poll_fn,
    mem::size_of,
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    rc::Rc,
    sync::Arc,
};

use crate::{
    error::{Error, Result},
    io::{AsyncBufRead, AsyncBufWrite},
    net::{
        accept_stream::AcceptStream,
        common::{InnerSocket, SocketToken, SocketTokenPtr},
        error::NetError,
        recv_stream::RecvStream,
    },
    runtime::context::Ctx,
};
use diagweave::{prelude::*, report::Report};
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    SockAddrStorage, Socket,
    driver::Driver,
    op::{
        Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, RecvProvided,
        Send as OpSend,
    },
    socket_addr_to_storage,
};

#[derive(Clone)]
pub struct GenericTcpListener<'rt, S, P: SocketTokenPtr<'rt>> {
    pub(crate) inner: InnerSocket<'rt, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt>,
}

#[derive(Clone)]
pub struct GenericTcpStream<'rt, S, P: SocketTokenPtr<'rt>> {
    pub(crate) inner: InnerSocket<'rt, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt>,
}

pub type LocalTcpListener<'rt> =
    GenericTcpListener<'rt, LocalSubmitter<Ctx<'rt>>, Rc<SocketToken<'rt>>>;
pub type LocalTcpStream<'rt> =
    GenericTcpStream<'rt, LocalSubmitter<Ctx<'rt>>, Rc<SocketToken<'rt>>>;

pub type TcpListener<'rt> = GenericTcpListener<'rt, DetachedSubmitter, Arc<SocketToken<'rt>>>;
pub type TcpStream<'rt> = GenericTcpStream<'rt, DetachedSubmitter, Arc<SocketToken<'rt>>>;

fn bind_listener_inner<'rt, A: ToSocketAddrs, P: SocketTokenPtr<'rt>>(
    ctx: Ctx<'rt>,
    addr: A,
) -> Result<InnerSocket<'rt, P>> {
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

fn new_stream_inner<'rt, P: SocketTokenPtr<'rt>>(
    ctx: Ctx<'rt>,
    addr: &SocketAddr,
) -> Result<InnerSocket<'rt, P>> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().trans()?
    } else {
        Socket::new_tcp_v6().trans()?
    };
    InnerSocket::new(ctx, socket.into_owned_raw().into_raw(), None)
}

impl<'rt, S: OpSubmitter<'rt, Ctx<'rt>> + Copy, P: SocketTokenPtr<'rt>>
    GenericTcpListener<'rt, S, P>
{
    async fn accept_direct(&self) -> Result<(GenericTcpStream<'rt, S, P>, SocketAddr)> {
        if self.inner.token().has_stashed_accept() {
            let mut stream = self.accept_multi();
            let polled = poll_fn(|cx| {
                use futures_core::Stream;
                unsafe { Pin::new_unchecked(&mut stream) }.poll_next(cx)
            })
            .await;
            if let Some(res) = polled {
                return res;
            }
        }

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

    /// 显式优雅关闭 Listener 并解绑底层资源。
    pub async fn close(self) -> Result<()> {
        self.inner.close_async().await
    }

    /// 把这个 listener 变成一条连接流。
    ///
    /// 与反复 `accept()` 语义相同，但在支持 multishot accept 的内核上只提交一次 SQE、
    /// 只占一个 slot。不支持时自动退回「每次重新提交一次单发 accept」——两个平台共用
    /// 那条路径，见 [`AcceptStream`]。
    ///
    /// 注意 `accept()` 与本方法**不要同时用在同一个 listener 上**：两者都会从同一个内核
    /// accept 队列取连接，谁拿到哪一个不确定。
    pub fn accept_multi(&self) -> AcceptStream<'rt, S, P> {
        AcceptStream::new(self.ctx, self.inner.clone(), self.submitter)
    }
}

impl<'rt, S: OpSubmitter<'rt, Ctx<'rt>> + Copy, P: SocketTokenPtr<'rt>>
    GenericTcpStream<'rt, S, P>
{
    async fn connect_from_inner_direct(
        inner: InnerSocket<'rt, P>,
        submitter: S,
        ctx: Ctx<'rt>,
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

    /// 接收一段数据，buffer 由内核在数据到达时才从 provided buffer 环里挑。
    ///
    /// 若当前 Runtime 或 OS 不支持 provided buffer，该方法将自动降级为由 Runtime 动态分配 Buffer
    /// 并执行单发 recv，保持透明可控。
    async fn recv_provided_direct(&self) -> Result<FixedBuf> {
        if self
            .ctx
            .driver(|driver| driver.capabilities().provided_buffers)
        {
            let op = RecvProvided {
                fd: self.inner.fd(),
            };
            let (res, op_back) = self
                .ctx
                .submit(&self.submitter, Op::new(op))
                .await
                .into_inner();
            if let Ok(received) = res
                && let Some(buf) = op_back.and_then(|provided| provided.buf)
            {
                debug_assert_eq!(buf.len(), received);
                return Ok(buf);
            }
        }

        let buf = self.ctx.try_alloc_full(veloq_std::nz!(8192)).trans()?;
        let (n, mut buf) = self.recv_subset_direct(buf, 0).await?;
        buf.set_len(n);
        Ok(buf)
    }

    /// 把这条连接变成一条数据流，每一项的 buffer 由内核在数据到达时才从 provided buffer
    /// 环里挑。
    ///
    /// 与反复 `recv_provided()` 语义相同，但在支持 multishot recv 的内核（6.0+）上只提交
    /// 一次 SQE、只占一个 slot；不支持时退回「每次重新提交一次单发 recv」。两条路径都要求
    /// 运行时开了 provided buffers（[`crate::config::Config::uring_provided_buffers`]），
    /// 否则返回 [`NetError::ProvidedBuffersUnavailable`]——**这是内核语义不是设计选择**，
    /// 见 [`RecvStream`]。
    ///
    /// 对端关闭时流正常结束。流在当前 worker 上提交，与这个 socket 上的其它操作一样。
    pub fn recv_multi(&self) -> Result<RecvStream<'rt, S, P>> {
        RecvStream::new(self.ctx, self.inner.clone(), self.submitter)
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

impl<'rt> LocalTcpListener<'rt> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(LocalTcpStream<'rt>, SocketAddr)> {
        self.accept_direct().await
    }
}

impl<'rt> TcpListener<'rt> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub async fn accept(&self) -> Result<(TcpStream<'rt>, SocketAddr)> {
        if self.inner.token().has_stashed_accept() {
            let mut stream = self.accept_multi();
            let polled = poll_fn(|cx| {
                use futures_core::Stream;
                unsafe { Pin::new_unchecked(&mut stream) }.poll_next(cx)
            })
            .await;
            if let Some(res) = polled {
                return res;
            }
        }

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

impl<'rt> LocalTcpStream<'rt> {
    pub async fn connect(ctx: Ctx<'rt>, addr: SocketAddr) -> Result<Self> {
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

    /// 接收一段数据，buffer 由内核从 provided buffer 环里挑（见
    /// [`crate::config::Config::uring_provided_buffers`]）。
    pub async fn recv_provided(&self) -> Result<FixedBuf> {
        self.recv_provided_direct().await
    }
}

impl<'rt> TcpStream<'rt> {
    pub async fn connect(ctx: Ctx<'rt>, addr: SocketAddr) -> Result<Self> {
        let inner = new_stream_inner(ctx, &addr)?;
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), ctx, addr).await
    }

    pub(crate) async fn connect_from_inner(
        ctx: Ctx<'rt>,
        inner: InnerSocket<'rt, Arc<SocketToken<'rt>>>,
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

    /// 接收一段数据，buffer 由内核从 provided buffer 环里挑（见
    /// [`crate::config::Config::uring_provided_buffers`]）。
    ///
    /// 环是 per-worker 的，所以操作照例路由到持有这个 socket 的 worker 上——用户拿到的
    /// `FixedBuf` 属于**那个** worker 的池，跨 worker drop 走池自己的归还路径，与其它任何
    /// `FixedBuf` 没有区别。
    pub async fn recv_provided(&self) -> Result<FixedBuf> {
        if self
            .ctx
            .driver(|driver| driver.capabilities().provided_buffers)
        {
            let owner = self.inner.owner_worker_id();
            let op = RecvProvided {
                fd: self.inner.fd(),
            };
            let (res, provided) = self.ctx.submit_to(owner, Op::new(op)).await?;
            if let Ok(received) = res
                && let Some(buf) = provided.buf
            {
                debug_assert_eq!(buf.len(), received);
                return Ok(buf);
            }
        }

        let buf = self.ctx.try_alloc_full(veloq_std::nz!(8192)).trans()?;
        let (n, mut buf) = self.recv_subset(buf, 0).await?;
        buf.set_len(n);
        Ok(buf)
    }

    /// 显式优雅关闭 TcpStream 并解绑底层资源。
    pub async fn close(self) -> Result<()> {
        self.inner.close_async().await
    }
}

impl<'rt> AsyncBufRead for LocalTcpStream<'rt> {
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
                Err(NetError::UnexpectedEof)?;
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'rt> AsyncBufRead for TcpStream<'rt> {
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
                Err(NetError::UnexpectedEof)?;
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'rt> AsyncBufWrite for LocalTcpStream<'rt> {
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
                Err(NetError::WriteZero)?;
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

impl<'rt> AsyncBufWrite for TcpStream<'rt> {
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
                Err(NetError::WriteZero)?;
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
