use veloq_std::{
    boxed::Box,
    future::{poll_fn, ready},
    marker::PhantomData,
    mem::replace,
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::Poll,
};

use crate::{
    error::{Error, Result},
    io::AsyncBufWrite,
    net::{
        common::{InnerSocket, ReceiveClaim, SocketToken, SocketTokenPtr},
        error::NetError,
    },
    runtime::context::Ctx,
};
use diagweave::{prelude::*, report::Report};
use futures_core::Stream;
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    Socket,
    driver::{DriverRaw, PlatformDriver},
    op::{
        DetachedOp, DetachedSubmitter, IoFd, LocalOp, LocalSubmitter, Op, OpItem, OpSubmitter,
        SendTo, UdpConnect, UdpReceiveOperationBuilder, UdpRecvMulti, UdpSend as OpUdpSend,
    },
    socket_addr_to_storage,
};

pub use veloq_driver_native::op::{UdpReceiveConfig, UdpRecvPacket, UdpRecvPacketBuf};

#[derive(Clone)]
pub struct GenericUdpSocket<'rt, S, P: SocketTokenPtr<'rt>> {
    pub(crate) inner: InnerSocket<'rt, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt>,
}

pub type LocalUdpSocket<'rt> =
    GenericUdpSocket<'rt, LocalSubmitter<Ctx<'rt>>, Rc<SocketToken<'rt>>>;
pub type UdpSocket<'rt> = GenericUdpSocket<'rt, DetachedSubmitter, Arc<SocketToken<'rt>>>;

type UdpRecvLocalStream<'rt> = LocalOp<'rt, UdpRecvMulti, Ctx<'rt>>;
type UdpRecvDetachedStream<'rt> =
    DetachedOp<UdpRecvMulti, <PlatformDriver<'rt> as DriverRaw>::SlotSpec>;

enum UdpReceiverState<'rt> {
    Created,
    Local(Box<UdpRecvLocalStream<'rt>>),
    Detached(Box<UdpRecvDetachedStream<'rt>>),
    Closed,
}

/// 独占一个 UDP socket 接收方向的长期 receiver。
///
/// receiver 不可 clone；socket 的 clone 仍然可以继续发送，但同一个 socket 同时只能
/// 存在一个接收 claim。
pub struct GenericUdpReceiver<'rt, S, P: SocketTokenPtr<'rt>>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
{
    inner: InnerSocket<'rt, P>,
    ctx: Ctx<'rt>,
    config: UdpReceiveConfig,
    claim: ReceiveClaim,
    state: UdpReceiverState<'rt>,
    marker: PhantomData<S>,
}

pub type LocalUdpReceiver<'rt> =
    GenericUdpReceiver<'rt, LocalSubmitter<Ctx<'rt>>, Rc<SocketToken<'rt>>>;
pub type UdpReceiver<'rt> = GenericUdpReceiver<'rt, DetachedSubmitter, Arc<SocketToken<'rt>>>;

impl<'rt, S, P> GenericUdpSocket<'rt, S, P>
where
    P: SocketTokenPtr<'rt>,
{
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn bind_inner<'rt, A: ToSocketAddrs, P: SocketTokenPtr<'rt>>(
    ctx: Ctx<'rt>,
    addr: A,
) -> Result<InnerSocket<'rt, P>> {
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

fn build_receive_operation(
    ctx: Ctx<'_>,
    fd: IoFd,
    config: UdpReceiveConfig,
) -> Result<UdpRecvMulti> {
    config
        .validate()
        .map_err(|_| NetError::ReceiveConfigInvalid)
        .trans()?;
    let buffer_pool = ctx.buf_pool();
    ctx.driver(|mut driver| driver.build_udp_recv_multi(fd, config, buffer_pool).trans())
}

fn parse_receive_item<'rt>(
    item: OpItem<UdpRecvMulti, <PlatformDriver<'rt> as DriverRaw>::SlotSpec>,
) -> Result<UdpRecvPacket> {
    let (result, packet) = item.into_inner();
    result.trans()?;
    packet.ok_or(NetError::ReceiveContextCorrupt).trans()
}

impl<'rt, S, P> GenericUdpReceiver<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    fn ensure_created(&self) -> Result<()> {
        if matches!(self.state, UdpReceiverState::Created) {
            Ok(())
        } else {
            NetError::ReceiverNotReady.trans()
        }
    }

    fn receive_operation(&self) -> Result<UdpRecvMulti> {
        build_receive_operation(self.ctx, self.inner.fd(), self.config)
    }

    fn ready_local(&mut self) -> Result<()> {
        self.ensure_created()?;
        let operation = self.receive_operation()?;
        let mut stream = self
            .ctx
            .submit_stream(&LocalSubmitter::new(), Op::new(operation));
        if !stream.arm() {
            return NetError::InitialReceiveSubmitFailed.trans();
        }
        self.state = UdpReceiverState::Local(Box::new(stream));
        Ok(())
    }

    async fn ready_detached(&mut self) -> Result<()> {
        self.ensure_created()?;

        let owner = self.inner.owner_worker_id();
        if self.ctx.runtime_ctx.worker_id() == owner {
            let operation = self.receive_operation()?;
            let stream = self
                .ctx
                .driver(|mut driver| Op::new(operation).submit_detached(&mut driver));
            self.state = UdpReceiverState::Detached(Box::new(stream));
            return Ok(());
        }

        let runtime_ctx = self.ctx.runtime_ctx;
        let fd = self.inner.fd();
        let config = self.config;
        let routed = self
            .ctx
            .runtime_ctx
            .route_to(owner, move || {
                let ctx = Ctx { runtime_ctx };
                ready(build_receive_operation(ctx, fd, config).map(|operation| {
                    ctx.driver(|mut driver| Op::new(operation).submit_detached(&mut driver))
                }))
            })
            .trans()?;
        let stream = routed.await.trans()??;
        self.state = UdpReceiverState::Detached(Box::new(stream));
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<UdpRecvPacket> {
        if !matches!(
            self.state,
            UdpReceiverState::Local(_) | UdpReceiverState::Detached(_)
        ) {
            return NetError::ReceiverNotReady.trans();
        }

        let item = poll_fn(|cx| match &mut self.state {
            UdpReceiverState::Local(stream) => {
                // SAFETY: the receiver is not self-referential; the stream remains pinned only
                // for the duration of this poll and is never moved while it is armed.
                unsafe { Pin::new_unchecked(&mut **stream) }.poll_next(cx)
            }
            UdpReceiverState::Detached(stream) => {
                // SAFETY: see the local stream branch above.
                unsafe { Pin::new_unchecked(&mut **stream) }.poll_next(cx)
            }
            UdpReceiverState::Created | UdpReceiverState::Closed => Poll::Ready(None),
        })
        .await;

        match item {
            Some(item) => parse_receive_item(item),
            None => {
                self.state = UdpReceiverState::Closed;
                NetError::ReceiveCancelled.trans()
            }
        }
    }

    async fn close_inner(mut self) -> Result<()> {
        let state = replace(&mut self.state, UdpReceiverState::Closed);
        drop(state);
        self.inner.token().release_receive(self.claim);
        Ok(())
    }
}

impl<'rt, S, P> Drop for GenericUdpReceiver<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    fn drop(&mut self) {
        let state = replace(&mut self.state, UdpReceiverState::Closed);
        drop(state);
        self.inner.token().release_receive(self.claim);
    }
}

impl<'rt> LocalUdpSocket<'rt> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: LocalSubmitter::new(),
            ctx,
        })
    }

    pub fn receiver(&self, config: UdpReceiveConfig) -> Result<LocalUdpReceiver<'rt>> {
        let claim = self.inner.token().claim_receive()?;
        Ok(GenericUdpReceiver {
            inner: self.inner.clone(),
            ctx: self.ctx,
            config,
            claim,
            state: UdpReceiverState::Created,
            marker: PhantomData,
        })
    }

    pub async fn send_to(&self, buf: FixedBuf, target: SocketAddr) -> Result<(usize, FixedBuf)> {
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
            .map(|op| op.buf)
            .ok_or(NetError::OpBufferLost)
            .trans()?;
        Ok((res.trans()?, buf))
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<()> {
        let (raw_addr, raw_addr_len) = socket_addr_to_storage(addr);
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

    pub async fn send(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn send_subset(&self, buf: FixedBuf, buf_offset: usize) -> Result<(usize, FixedBuf)> {
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
            .map(|op| op.buf)
            .ok_or(NetError::OpBufferLost)
            .trans()?;
        Ok((res.trans()?, buf))
    }
}

impl<'rt> LocalUdpReceiver<'rt> {
    pub async fn ready(&mut self) -> Result<()> {
        self.ready_local()
    }

    pub async fn close(self) -> Result<()> {
        self.close_inner().await
    }
}

impl<'rt> UdpSocket<'rt> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt>, addr: A) -> Result<Self> {
        Ok(Self {
            inner: bind_inner(ctx, addr)?,
            submitter: DetachedSubmitter::new(),
            ctx,
        })
    }

    pub fn receiver(&self, config: UdpReceiveConfig) -> Result<UdpReceiver<'rt>> {
        let claim = self.inner.token().claim_receive()?;
        Ok(GenericUdpReceiver {
            inner: self.inner.clone(),
            ctx: self.ctx,
            config,
            claim,
            state: UdpReceiverState::Created,
            marker: PhantomData,
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

    pub async fn connect(&self, addr: SocketAddr) -> Result<()> {
        let owner = self.inner.owner_worker_id();
        let (raw_addr, raw_addr_len) = socket_addr_to_storage(addr);
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

    pub async fn close(self) -> Result<()> {
        self.inner.close_async().await
    }
}

impl<'rt> UdpReceiver<'rt> {
    pub async fn ready(&mut self) -> Result<()> {
        self.ready_detached().await
    }

    pub async fn close(self) -> Result<()> {
        self.close_inner().await
    }
}

impl<'rt> AsyncBufWrite for LocalUdpSocket<'rt> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send(buf).await
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, next) = self.send_subset(buf, total).await?;
            buf = next;
            if n == 0 {
                return NetError::WriteZero.trans();
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

impl<'rt> AsyncBufWrite for UdpSocket<'rt> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        self.send(buf).await
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, next) = self.send_subset(buf, total).await?;
            buf = next;
            if n == 0 {
                return NetError::WriteZero.trans();
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
