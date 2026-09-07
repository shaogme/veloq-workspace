//! multishot accept 的用户侧流。
//!
//! 两条实现路径，选哪条由后端能力决定：
//!
//! - `Native`：一次提交，内核持续投递完成（io_uring `AcceptMulti`）；
//! - `Emulated`：每次 `poll_next` 提交一次单发 `Accept`。
//!
//! **`Emulated` 不是 Windows 兼容层**，它同时也是 Linux 5.6–5.18 的路径——multishot accept
//! 要 5.19，而仓库声明的最低内核是 5.6。两个平台共用同一段代码，语义等价、性能不等价。
//!
//! 两条路径的对端地址来源**不同**，这一点不要「统一」掉：`AcceptMulti` 的 SQE 没有 addr
//! 字段（多条完成共享一个地址缓冲会互相覆盖），所以 `Native` 必须在拿到 fd 之后调
//! `getpeername`；`Emulated` 走单发 `Accept`，地址由内核直接填好。
//!
//! 两条路径都通过 `S` 提交，所以 [`crate::net::LocalTcpListener`] 上的流走 `LocalOp`、
//! [`crate::net::TcpListener`] 上的流走 `DetachedOp`——和这两种 listener 上其它每个操作
//! 的选择一致。

use std::{
    mem::size_of,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use diagweave::prelude::*;
use futures_core::Stream;
use veloq_driver_native::{
    OwnedRawHandle, SockAddrStorage,
    driver::{Driver, DriverCapability, PlatformSlotSpec},
    multishot::is_capability_rejected,
    op::{Accept, AcceptMulti, Op, OpItem, OpResult, OpSubmitter},
    peer_addr_of_handle,
};

use crate::{
    error::Result,
    net::{
        common::{InnerSocket, SocketTokenPtr},
        error::NetError,
        tcp::GenericTcpStream,
    },
    runtime::context::Ctx,
};

type NativeItem = OpItem<AcceptMulti, PlatformSlotSpec>;
type EmulatedItem = OpItem<Accept, PlatformSlotSpec>;

/// 选哪条路是**运行期**的事，不是编译期的事。
///
/// 两个变体在两个平台上都在。IOCP 的 `capabilities().accept_multi` 恒为 `false`，所以
/// `Native` 在那里永远构造不出来——但它构造得**出类型**，代价只是一段编译出来却走不到的
/// 分支，换来的是这个文件里一处 `#[cfg]` 都不需要。IOCP 后端为此给 `AcceptMulti` 提供了
/// 一个提交即失败的实现（见 `veloq-driver-iocp` 的 `impl_iocp_unsupported_op!`）。
enum AcceptMode<'rt, S: OpSubmitter<'rt, Ctx<'rt>>> {
    /// 一次提交，多条完成：句柄本身就是流。
    Native(S::Stream<AcceptMulti>),
    /// 每取一条都重新提交一次单发 accept。`None` 表示当前没有在途的那一次。
    ///
    /// 单发操作也是流（只有一项），所以这里用的是同一个 `S::Stream` 而不是 future——
    /// 两条分支的差别只剩「重新 arm 是内核做还是自己做」。
    Emulated { pending: Option<S::Stream<Accept>> },
}

/// [`crate::net::TcpListener::accept_multi`] 产出的连接流。
///
/// 语义与反复调用 `accept()` 相同：每一项是一个新连接及其对端地址。差别只在提交次数——
/// `Native` 路径下 N 个连接只对应一次 SQE 提交与一个 slot。
///
/// 流被丢弃时会取消在途的 multishot 操作（句柄的 `Drop` → `mark_orphaned` +
/// `CancelRequest::abandon`），内核随后投递的完成走 orphan cleanup，其中已经建立好的连接
/// 会被关闭而不是泄漏。
pub struct AcceptStream<'rt, S: OpSubmitter<'rt, Ctx<'rt>>, P: SocketTokenPtr<'rt>> {
    mode: Option<AcceptMode<'rt, S>>,
    inner: InnerSocket<'rt, P>,
    submitter: S,
    ctx: Ctx<'rt>,
    /// `Native` 路径有没有产出过至少一项。
    ///
    /// 「内核不认识 multishot accept」只可能在**第一条**完成上表现出来，所以降级判据仅在
    /// 这个标志还是 `false` 时生效；此后同样的 `-EINVAL` 是真错误，照原样交给用户。
    native_delivered: bool,
}

impl<'rt, S, P> AcceptStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    pub(crate) fn new(ctx: Ctx<'rt>, inner: InnerSocket<'rt, P>, submitter: S) -> Self {
        if let Some((mode, native_delivered)) =
            inner.token().take_accept::<(AcceptMode<'rt, S>, bool)>()
        {
            return Self {
                mode: Some(mode),
                inner,
                submitter,
                ctx,
                native_delivered,
            };
        }

        // 能力由 driver 缓存：第一次被内核以 EINVAL 拒绝之后就不会再试（见
        // `Driver::note_capability_rejected`），后续的 listener 不重复付探测代价。
        let native = ctx.driver(|driver| driver.capabilities().accept_multi);
        let mode = Self::arm(ctx, &inner, submitter, native);

        Self {
            mode: Some(mode),
            inner,
            submitter,
            ctx,
            native_delivered: false,
        }
    }

    fn arm(
        ctx: Ctx<'rt>,
        inner: &InnerSocket<'rt, P>,
        submitter: S,
        native: bool,
    ) -> AcceptMode<'rt, S> {
        if !native {
            return AcceptMode::Emulated { pending: None };
        }
        let fd = inner.fd();
        AcceptMode::Native(ctx.submit_stream(&submitter, Op::new(AcceptMulti { fd })))
    }

    /// 把一个刚被 accept 出来的描述符包装成流对外产出的那一项。
    fn make_item(
        &self,
        accepted: OwnedRawHandle,
        addr: SocketAddr,
    ) -> Result<(GenericTcpStream<'rt, S, P>, SocketAddr)> {
        Ok((
            GenericTcpStream {
                inner: InnerSocket::new(self.ctx, accepted.into_raw(), None)?,
                submitter: self.submitter,
                ctx: self.ctx,
            },
            addr,
        ))
    }

    /// `Native` 的一项：新连接的 fd 来自 CQE，对端地址要另外问内核。
    fn native_item(&self, item: NativeItem) -> Result<(GenericTcpStream<'rt, S, P>, SocketAddr)> {
        let (res, _) = item.into_inner();
        let accepted = res.trans()?;
        // multishot accept 不带对端地址——见模块文档。`getpeername` 失败只让**这一条**
        // 降级为错误项，流本身继续。
        let addr = peer_addr_of_handle(accepted.raw()).trans()?;
        self.make_item(accepted, addr)
    }

    /// `Emulated` 的一项：走单发 `Accept`，地址由内核填在 payload 里。
    fn emulated_item(
        &self,
        result: EmulatedItem,
    ) -> Result<(GenericTcpStream<'rt, S, P>, SocketAddr)> {
        let (res, op) = result.into_inner();
        let op = op.ok_or(NetError::AcceptOpLost)?;
        let accepted = res.trans()?;
        let addr = op.remote_addr.ok_or(NetError::AcceptMissingRemoteAddr)?;
        self.make_item(accepted, addr)
    }

    /// 这一项是不是「内核不认识 multishot accept」，是的话就地降级。
    ///
    /// 返回 `true` 表示这一项**不该**交给用户：能力已经在 driver 上关掉、模式换成了
    /// `Emulated`，调用方重来一轮就会走单发 accept 拿到同一个连接。
    fn downgraded(&mut self, item: &NativeItem) -> bool {
        if self.native_delivered {
            return false;
        }
        let OpResult::Completed(Err(report), _) = item else {
            return false;
        };
        if !is_capability_rejected(report) {
            return false;
        }
        // 关掉的是 driver 上的能力，不是这条流的：同一个 worker 后续开的每条流都直接从
        // `Emulated` 起步，不再各自付一次失败提交。
        self.ctx
            .driver(|mut driver| driver.note_capability_rejected(DriverCapability::AcceptMulti));
        self.mode = Some(AcceptMode::Emulated { pending: None });
        true
    }

    /// 取一轮原始结果。
    ///
    /// `Native` 在这里可能就地降级：那一条完成不交给用户，直接**落到** `Emulated` 分支
    /// 重新提交一次单发 accept，本轮照样交得出一项。降级至多发生一次（此后 `mode` 已经是
    /// `Emulated`），所以这里没有循环也不会漏。
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Step> {
        let Some(mode) = self.mode.as_mut() else {
            return Poll::Ready(Step::Ended);
        };
        let polled = match mode {
            // SAFETY: 句柄不是自引用的，投影出 `&mut` 不违反 pin 契约。
            AcceptMode::Native(stream) => unsafe { Pin::new_unchecked(stream) }.poll_next(cx),
            AcceptMode::Emulated { .. } => Poll::Ready(None),
        };
        match polled {
            Poll::Ready(Some(item)) => {
                if !self.downgraded(&item) {
                    self.native_delivered = true;
                    return Poll::Ready(Step::Native(item));
                }
            }
            // `Emulated` 用 `Ready(None)` 表示「这一轮不归 Native」，所以只有真的处在
            // `Native` 时它才意味着流结束。
            Poll::Ready(None) => {
                if matches!(self.mode.as_ref(), Some(AcceptMode::Native(_))) {
                    return Poll::Ready(Step::Ended);
                }
            }
            Poll::Pending => return Poll::Pending,
        }

        self.poll_emulated(cx)
    }

    /// 提交（必要时）并取走这一次单发 accept 的结果。
    fn poll_emulated(&mut self, cx: &mut Context<'_>) -> Poll<Step> {
        let Some(mode) = self.mode.as_mut() else {
            return Poll::Ready(Step::Ended);
        };
        let pending = match mode {
            AcceptMode::Emulated { pending } => pending,
            AcceptMode::Native(_) => {
                unreachable!("poll_emulated runs only once the mode settled on Emulated")
            }
        };

        if pending.is_none() {
            let fd = self.inner.fd();
            *pending = Some(self.ctx.submit_stream(
                &self.submitter,
                Op::new(Accept {
                    fd,
                    addr: SockAddrStorage::default(),
                    addr_len: size_of::<SockAddrStorage>() as u32,
                    remote_addr: None,
                }),
            ));
        }

        let Some(op) = pending.as_mut() else {
            return Poll::Ready(Step::Ended);
        };
        // SAFETY: 句柄不是自引用的，投影出 `&mut` 不违反 pin 契约。
        let op = unsafe { Pin::new_unchecked(op) };
        let result = match op.poll_next(cx) {
            Poll::Ready(Some(result)) => result,
            // 单发操作的流只有一项，不可能在这里就空——空了说明上一轮忘了清。
            Poll::Ready(None) => unreachable!("a single-shot accept was polled twice"),
            Poll::Pending => return Poll::Pending,
        };
        // 这一次已经结束，下一轮 `poll_next` 会重新提交。
        *pending = None;
        Poll::Ready(Step::Emulated(result))
    }
}

impl<'rt, S, P> Drop for AcceptStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>>,
    P: SocketTokenPtr<'rt>,
{
    fn drop(&mut self) {
        if let Some(mode) = self.mode.take() {
            self.inner
                .token()
                .stash_accept((mode, self.native_delivered));
        }
    }
}

impl<'rt, S, P> Stream for AcceptStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    type Item = Result<(GenericTcpStream<'rt, S, P>, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: `AcceptStream` 的字段都不是自引用的，投影出 `&mut` 不违反 pin 契约。
        let this = unsafe { self.get_unchecked_mut() };

        let step = match this.poll_step(cx) {
            Poll::Ready(step) => step,
            Poll::Pending => return Poll::Pending,
        };

        let item = match step {
            Step::Ended => return Poll::Ready(None),
            Step::Native(item) => this.native_item(item),
            Step::Emulated(result) => this.emulated_item(result),
        };
        Poll::Ready(Some(item))
    }
}

/// 一轮 `poll_step` 从后端拿到的原始结果。
enum Step {
    Native(NativeItem),
    Emulated(EmulatedItem),
    /// 后端句柄的流空了：这条 accept 流也就结束了。`Emulated` 永远走不到这里——单发流总有
    /// 那一项。
    Ended,
}
