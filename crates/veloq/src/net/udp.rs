use std::{
    future::Future,
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
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
    Socket,
    driver::{DriverRaw, PlatformDriver},
    op::{
        DetachedOp, DetachedSubmitter, LocalOp, LocalSubmitter, Op, OpItem, OpSubmitter, SendTo,
        UdpConnect, UdpRecv as OpUdpRecv, UdpRecvFrom, UdpRecvPacket, UdpRecvPacketBuf,
        UdpSend as OpUdpSend,
    },
    socket_addr_to_storage,
};
use veloq_runtime::runtime::context::RoutedFuture;

#[derive(Clone)]
pub struct GenericUdpSocket<'rt, S, P: SocketTokenPtr<'rt>> {
    pub(crate) inner: InnerSocket<'rt, P>,
    pub(crate) submitter: S,
    pub(crate) ctx: Ctx<'rt>,
}

pub type LocalUdpSocket<'rt> =
    GenericUdpSocket<'rt, LocalSubmitter<Ctx<'rt>>, Rc<SocketToken<'rt>>>;
pub type UdpSocket<'rt> = GenericUdpSocket<'rt, DetachedSubmitter, Arc<SocketToken<'rt>>>;

type UdpRecvLocalOp<'rt> = LocalOp<'rt, UdpRecvFrom, Ctx<'rt>>;
type UdpRecvDetachedOp<'rt> = DetachedOp<UdpRecvFrom, <PlatformDriver<'rt> as DriverRaw>::SlotSpec>;

pub struct PreparedLocalUdpRecv<'rt> {
    pub(crate) op_fut: UdpRecvLocalOp<'rt>,
}

impl<'rt> PreparedLocalUdpRecv<'rt> {
    /// 确保底层接收操作已被真正 Arm。
    pub async fn arm(&mut self) -> Result<()> {
        self.op_fut.arm();
        Ok(())
    }

    pub fn is_armed(&self) -> bool {
        self.op_fut.is_armed()
    }
}

impl<'rt> Future for PreparedLocalUdpRecv<'rt> {
    type Output = Result<UdpRecvPacket>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let item = match Pin::new(&mut self.op_fut).poll(cx) {
            Poll::Ready(item) => item,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(parse_udp_recv_item(item))
    }
}

fn parse_udp_recv_item<'rt>(
    item: OpItem<UdpRecvFrom, <PlatformDriver<'rt> as DriverRaw>::SlotSpec>,
) -> Result<UdpRecvPacket> {
    let (res, op_back_opt) = item.into_inner();
    let op_back = op_back_opt.ok_or(NetError::UdpRecvFromOpLost).trans()?;
    let n = res.trans()?;
    let mut recv_buf = op_back.buf;
    recv_buf.set_len(n);
    let addr = op_back
        .addr
        .ok_or(NetError::UdpRecvFromMissingAddr)
        .trans()?;
    Ok(UdpRecvPacket {
        buf: UdpRecvPacketBuf::from_fixed_buf(recv_buf),
        addr,
    })
}

pub enum PreparedUdpRecvState<'rt> {
    Local(UdpRecvDetachedOp<'rt>),
    Remote(RoutedFuture<UdpRecvDetachedOp<'rt>>),
    Done,
}

pub struct PreparedUdpRecv<'rt> {
    pub(crate) state: PreparedUdpRecvState<'rt>,
}

impl<'rt> PreparedUdpRecv<'rt> {
    /// 异步确保底层接收操作已被真正 Arm（提交到底层内核驱动）。
    ///
    /// - 若该套接字归属于当前 Worker，该方法同步提交并立即返回 `Ok(())`；
    /// - 若该套接字归属于远程 Worker，该方法将异步等待远程 Worker 调度并成功执行
    ///   `submit_detached`，确保底层网络栈（如 Windows RIO Request Queue）
    ///   已成功挂载接收缓冲区后再返回。
    pub async fn arm(&mut self) -> Result<()> {
        match &mut self.state {
            PreparedUdpRecvState::Local(op) => {
                op.arm();
                Ok(())
            }
            PreparedUdpRecvState::Remote(routed) => {
                routed.wait_ready().await.trans()?;
                Ok(())
            }
            PreparedUdpRecvState::Done => NetError::UdpRecvFromOpLost.trans(),
        }
    }

    /// 查询该操作当前是否确实已被 Arm。
    pub fn is_armed(&self) -> bool {
        match &self.state {
            PreparedUdpRecvState::Local(op) => op.is_armed(),
            PreparedUdpRecvState::Remote(routed) => routed.is_ready(),
            PreparedUdpRecvState::Done => false,
        }
    }
}

impl<'rt> Future for PreparedUdpRecv<'rt> {
    type Output = Result<UdpRecvPacket>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match &mut this.state {
            PreparedUdpRecvState::Local(op_fut) => {
                let item = match Pin::new(op_fut).poll(cx) {
                    Poll::Ready(item) => item,
                    Poll::Pending => return Poll::Pending,
                };
                this.state = PreparedUdpRecvState::Done;
                Poll::Ready(parse_udp_recv_item(item))
            }
            PreparedUdpRecvState::Remote(routed) => {
                let item_res = match Pin::new(routed).poll(cx) {
                    Poll::Ready(res) => res,
                    Poll::Pending => return Poll::Pending,
                };
                this.state = PreparedUdpRecvState::Done;
                let item = match item_res.trans() {
                    Ok(item) => item,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                Poll::Ready(parse_udp_recv_item(item))
            }
            PreparedUdpRecvState::Done => panic!("PreparedUdpRecv polled after completion"),
        }
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

impl<'rt, S: OpSubmitter<'rt, Ctx<'rt>> + Copy, P: SocketTokenPtr<'rt>>
    GenericUdpSocket<'rt, S, P>
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
            .ok_or(NetError::OpBufferLost)
            .trans()?;
        Ok((res.trans()?, buf))
    }

    async fn connect_direct(&self, addr: SocketAddr) -> Result<()> {
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
            .ok_or(NetError::OpBufferLost)
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
            .ok_or(NetError::OpBufferLost)
            .trans()?;
        Ok((res.trans()?, buf))
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

    pub async fn send_to(&self, buf: FixedBuf, target: SocketAddr) -> Result<(usize, FixedBuf)> {
        self.send_to_direct(buf, target).await
    }

    pub fn prepare_recv_from(&self, buf: FixedBuf) -> PreparedLocalUdpRecv<'rt> {
        let op = UdpRecvFrom {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: None,
        };
        let op_fut = self.ctx.submit(&self.submitter, Op::new(op));
        PreparedLocalUdpRecv { op_fut }
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> Result<UdpRecvPacket> {
        self.prepare_recv_from(buf).await
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

impl<'rt> UdpSocket<'rt> {
    pub fn bind<A: ToSocketAddrs>(ctx: Ctx<'rt>, addr: A) -> Result<Self> {
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

    pub fn prepare_recv_from(&self, buf: FixedBuf) -> PreparedUdpRecv<'rt> {
        let owner = self.inner.owner_worker_id();
        let op = UdpRecvFrom {
            fd: self.inner.fd(),
            buf,
            buf_offset: 0,
            addr: None,
        };
        if self.ctx.runtime_ctx.worker_id() == owner {
            let op_fut = self.ctx.submit(&self.submitter, Op::new(op));
            PreparedUdpRecv {
                state: PreparedUdpRecvState::Local(op_fut),
            }
        } else {
            let runtime_ctx_clone = self.ctx.runtime_ctx;
            let routed = self
                .ctx
                .runtime_ctx
                .route_to(owner, move || {
                    let ctx = Ctx {
                        runtime_ctx: runtime_ctx_clone,
                    };
                    ctx.driver(|mut driver| Op::new(op).submit_detached(&mut driver))
                })
                .expect("Failed to route submit_detached");
            PreparedUdpRecv {
                state: PreparedUdpRecvState::Remote(routed),
            }
        }
    }

    pub async fn recv_from(&self, buf: FixedBuf) -> Result<UdpRecvPacket> {
        self.prepare_recv_from(buf).await
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

    /// 显式优雅关闭 Socket 并解绑底层资源。
    pub async fn close(self) -> Result<()> {
        self.inner.close_async().await
    }
}

impl<'rt> AsyncBufRead for LocalUdpSocket<'rt> {
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
                return NetError::UnexpectedEof.trans();
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<'rt> AsyncBufRead for UdpSocket<'rt> {
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
                return NetError::UnexpectedEof.trans();
            }
            total += n;
        }
        Ok((total, buf))
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
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
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
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
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
