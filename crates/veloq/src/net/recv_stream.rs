//! multishot recv 的用户侧流。
//!
//! 与 [`AcceptStream`](crate::net::AcceptStream) 同形，两条实现路径：
//!
//! - `Native`：一次提交，内核持续投递完成（io_uring `RecvMulti`，Linux 6.0+）；
//! - `Emulated`：每次 `poll_next` 提交一次单发 `RecvProvided`（Linux 5.19–5.x）。
//!
//! 但与 accept 有一处根本不同：**两条路径都要求 provided buffer 环**。这不是设计选择而是
//! 内核语义——`IORING_OP_RECV` 的 multishot 变体强制 `IOSQE_BUFFER_SELECT`（一个调用方交
//! 出来的 buffer 装不下多条完成的数据），而 `Emulated` 那一侧必须与它语义等价，也就不能反
//! 过来向调用方要 buffer。于是 IOCP 上这条流**建不起来**：[`TcpStream::recv_multi`] 在那
//! 里恒返回 [`NetError::ProvidedBuffersUnavailable`]，两条路径一条都走不了。
//!
//! 类型本身在两个平台上是同一个，判据是运行期的 `capabilities().provided_buffers` 而不是
//! `#[cfg]`——见 `RecvMode` 上的注释。
//!
//! [`TcpStream::recv_multi`]: crate::net::TcpStream::recv_multi
//!
//! 收益就是 provided buffer 的收益：**buffer 只在数据到达时才与连接绑定**。一万个挂着
//! `recv_multi()` 的空闲连接不占任何接收缓冲，而一万个挂着 `recv()` 的连接各压一个。

use veloq_std::{
    pin::Pin,
    task::{Context, Poll},
};

use diagweave::{prelude::*, report::Report};
use futures_core::Stream;
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    driver::{Driver, DriverCapability, PlatformSlotSpec},
    error::Error as DriverError,
    multishot::{is_buffer_ring_exhausted, is_capability_rejected},
    op::{Op, OpItem, OpResult, OpSubmitter, Recv, RecvMulti, RecvProvided},
};

use crate::{
    error::{Error, Result},
    net::{
        common::{InnerSocket, SocketTokenPtr},
        error::NetError,
    },
    runtime::context::Ctx,
};

/// 一条完成产出的东西，两条路径共用。
///
/// `RecvMulti` 与 `RecvProvided` 的 `Output` / `Completion` 逐个相同（都是 `ProvidedBuf`
/// 加 `usize`），所以这个别名对两者都成立——「产物不是提交物」这件事两者一样，不一样的只是
/// 「一次提交产出几条」。
type ProvidedItem = OpItem<RecvProvided, PlatformSlotSpec>;

enum RecvItem {
    Provided(ProvidedItem),
    Single(OpItem<Recv, PlatformSlotSpec>),
    AllocError(Report<Error>),
}

/// 环被掏空后连着重 arm 多少次仍然立刻 `-ENOBUFS`，就把错误交给用户。
///
/// 重 arm 本身是必需的：`-ENOBUFS` 那条 CQE 不带 `IORING_CQE_F_MORE`，内核顺带把整个
/// multishot 也终止了。但只重 arm 不设上限，遇上「消费方长期跟不上」就会变成一个安静的忙
/// 循环——每次唤醒提交一次、立刻再失败一次。到了上限就说明这不是抖动，用户该知道。
const MAX_EXHAUSTED_REARMS: u32 = 8;

/// 选哪条路是**运行期**的事，不是编译期的事。
///
/// 与 [`AcceptStream`](crate::net::AcceptStream) 里的 `AcceptMode` 同理：三个变体在两个平
/// 台上都在，当 `capabilities().provided_buffers` 为 `false` 时（例如 IOCP 或未开启配置），
/// `RecvStream` 会透明使用 `FallbackSingle` 降级路径，从 Runtime 缓冲池分配 Buffer 执行单发 recv。
enum RecvMode<'rt, S: OpSubmitter<'rt, Ctx<'rt>>> {
    /// 一次提交，多条完成：句柄本身就是流。
    Native(S::Stream<RecvMulti>),
    /// 每取一条都重新提交一次单发 recv。`None` 表示当前没有在途的那一次。
    Emulated {
        pending: Option<S::Stream<RecvProvided>>,
    },
    /// 当 Provided Buffers 不可用或降级时：从 Runtime 动态分配 Buffer 提交单发 Recv。
    FallbackSingle { pending: Option<S::Stream<Recv>> },
    /// 不再提交任何东西：终态错误已经交给用户了。
    Done,
}

/// [`crate::net::TcpStream::recv_multi`] 产出的数据流。
///
/// 每一项是内核挑给这条连接的一个 [`FixedBuf`]，长度已经是实际收到的字节数。**对端关闭时
/// 流正常结束**（`poll_next` 返回 `None`），而不是产出一个空 buffer——「读到 0 字节」在这里
/// 只有一种意思，用流的终点表达它比让每个调用方都去判长度可靠。
///
/// 流被丢弃时会取消在途的操作（句柄的 `Drop` → `mark_orphaned` +
/// `CancelRequest::abandon`），内核随后投递的完成走 orphan cleanup，其中被挑走的 buffer 会
/// 还回环而不是泄漏。
///
/// 与所有其它操作一样，这条流在**创建它的那个 worker** 上提交——socket 的注册描述符是
/// per-worker 的，provided buffer 环也是。
pub struct RecvStream<'rt, S: OpSubmitter<'rt, Ctx<'rt>>, P: SocketTokenPtr<'rt>> {
    mode: Option<RecvMode<'rt, S>>,
    inner: InnerSocket<'rt, P>,
    submitter: S,
    ctx: Ctx<'rt>,
    /// `Native` 路径有没有产出过至少一项，见 [`RecvStream::downgraded`]。
    native_delivered: bool,
    /// 因为环空了而重新 arm 的累计次数（诊断用）。
    rearms: u32,
    /// 连续几次重 arm 之后仍然立刻 `-ENOBUFS`。收到任何一条数据就清零。
    exhausted_streak: u32,
}

impl<'rt, S, P> RecvStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    pub(crate) fn new(ctx: Ctx<'rt>, inner: InnerSocket<'rt, P>, submitter: S) -> Result<Self> {
        let capabilities = ctx.driver(|driver| driver.capabilities());

        if let Some((mode, native_delivered, rearms, exhausted_streak)) = inner
            .token()
            .take_recv::<(RecvMode<'rt, S>, bool, u32, u32)>()
        {
            return Ok(Self {
                mode: Some(mode),
                inner,
                submitter,
                ctx,
                native_delivered,
                rearms,
                exhausted_streak,
            });
        }

        let mode = if capabilities.provided_buffers {
            if capabilities.recv_multi {
                RecvMode::Native(Self::arm_native(ctx, &inner, submitter))
            } else {
                RecvMode::Emulated { pending: None }
            }
        } else {
            RecvMode::FallbackSingle { pending: None }
        };

        Ok(Self {
            mode: Some(mode),
            inner,
            submitter,
            ctx,
            native_delivered: false,
            rearms: 0,
            exhausted_streak: 0,
        })
    }

    /// 因为环空了而重新 arm 过多少次。
    ///
    /// 驱动那一侧的对应量是 `ProvidedBufStats` 的 `exhausted`（内核报了多少条 `-ENOBUFS`）
    /// 与 `available_low_water`。两者一起才说得清「偶发」还是「持续」。
    #[inline]
    pub fn rearms(&self) -> u32 {
        self.rearms
    }

    fn arm_native(
        ctx: Ctx<'rt>,
        inner: &InnerSocket<'rt, P>,
        submitter: S,
    ) -> S::Stream<RecvMulti> {
        let fd = inner.fd();
        ctx.submit_stream(&submitter, Op::new(RecvMulti { fd }))
    }

    /// 这一项是不是「内核不认识 multishot recv」或不支持 provided buffers，是的话就地降级。
    ///
    /// 与 [`AcceptStream`](crate::net::AcceptStream) 上同名方法逐条同理。
    fn downgraded(&mut self, item: &RecvItem) -> bool {
        let Some(mode) = self.mode.as_ref() else {
            return false;
        };
        let RecvItem::Provided(provided_item) = item else {
            return false;
        };
        if self.native_delivered {
            return false;
        }
        let OpResult::Completed(Err(report), _) = provided_item else {
            return false;
        };
        if !is_capability_rejected(report) {
            return false;
        }

        match mode {
            RecvMode::Native(_) => {
                self.ctx.driver(|mut driver| {
                    driver.note_capability_rejected(DriverCapability::RecvMulti)
                });
                let provided = self
                    .ctx
                    .driver(|driver| driver.capabilities().provided_buffers);
                if provided {
                    self.mode = Some(RecvMode::Emulated { pending: None });
                } else {
                    self.mode = Some(RecvMode::FallbackSingle { pending: None });
                }
                true
            }
            RecvMode::Emulated { .. } => {
                self.mode = Some(RecvMode::FallbackSingle { pending: None });
                true
            }
            _ => false,
        }
    }

    /// 环空了：重新 arm 并要求再来一轮，除非已经连着试了太多次。
    ///
    /// 返回 `false` 表示放弃，调用方把那条 `-ENOBUFS` 交给用户。
    fn rearm_exhausted(&mut self) -> bool {
        self.exhausted_streak += 1;
        if self.exhausted_streak > MAX_EXHAUSTED_REARMS {
            self.mode = Some(RecvMode::Done);
            return false;
        }
        self.rearms = self.rearms.saturating_add(1);
        if matches!(self.mode.as_ref(), Some(RecvMode::Native(_))) {
            // multishot 已经被内核连同这条 `-ENOBUFS` 一起终止了，句柄里那条流是空的。
            self.mode = Some(RecvMode::Native(Self::arm_native(
                self.ctx,
                &self.inner,
                self.submitter,
            )));
        }
        // `Emulated` 不必做什么：`poll_step` 取走结果时已经把 `pending` 清了，下一轮自然会
        // 重新提交。
        true
    }

    /// 从当前模式取一轮原始结果。`Ready(None)` 表示后端句柄的流空了。
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Option<RecvItem>> {
        let Some(mode) = self.mode.as_mut() else {
            return Poll::Ready(None);
        };
        match mode {
            RecvMode::Done => Poll::Ready(None),
            RecvMode::Native(stream) => {
                // SAFETY: 句柄不是自引用的，投影出 `&mut` 不违反 pin 契约。
                let stream = unsafe { Pin::new_unchecked(stream) };
                stream.poll_next(cx).map(|opt| opt.map(RecvItem::Provided))
            }
            RecvMode::Emulated { pending } => {
                if pending.is_none() {
                    let fd = self.inner.fd();
                    *pending = Some(
                        self.ctx
                            .submit_stream(&self.submitter, Op::new(RecvProvided { fd })),
                    );
                }

                let Some(op) = pending.as_mut() else {
                    return Poll::Ready(None);
                };
                // SAFETY: 与上面同理。
                let op = unsafe { Pin::new_unchecked(op) };
                let result = match op.poll_next(cx) {
                    Poll::Ready(Some(result)) => result,
                    // 单发操作的流只有一项，不可能在这里就空——空了说明上一轮忘了清。
                    Poll::Ready(None) => unreachable!("a single-shot recv was polled twice"),
                    Poll::Pending => return Poll::Pending,
                };
                // 这一次已经结束，下一轮会重新提交。
                *pending = None;
                Poll::Ready(Some(RecvItem::Provided(result)))
            }
            RecvMode::FallbackSingle { pending } => {
                if pending.is_none() {
                    let buf = match self.ctx.try_alloc_full(veloq_std::nz!(8192)) {
                        Ok(buf) => buf,
                        Err(err) => return Poll::Ready(Some(RecvItem::AllocError(err.trans()))),
                    };
                    let fd = self.inner.fd();
                    *pending = Some(self.ctx.submit_stream(
                        &self.submitter,
                        Op::new(Recv {
                            fd,
                            buf,
                            buf_offset: 0,
                        }),
                    ));
                }

                let Some(op) = pending.as_mut() else {
                    return Poll::Ready(None);
                };
                // SAFETY: 与上面同理。
                let op = unsafe { Pin::new_unchecked(op) };
                let result = match op.poll_next(cx) {
                    Poll::Ready(Some(result)) => result,
                    Poll::Ready(None) => unreachable!("a single-shot recv was polled twice"),
                    Poll::Pending => return Poll::Pending,
                };
                *pending = None;
                Poll::Ready(Some(RecvItem::Single(result)))
            }
        }
    }

    /// 把一条完成变成这条流的下一步动作。
    fn classify(&mut self, item: RecvItem) -> Flow {
        if self.downgraded(&item) {
            return Flow::Retry;
        }
        self.native_delivered = true;

        match item {
            RecvItem::Provided(provided_item) => {
                let (res, provided) = provided_item.into_inner();
                let received = match res {
                    // 对端关闭。那个（空的）buffer 随 `provided` 一起回池。
                    Ok(0) => return Flow::End,
                    Ok(received) => received,
                    Err(report) => return self.classify_error(report),
                };

                self.exhausted_streak = 0;
                Flow::Yield(match provided.and_then(|provided| provided.buf) {
                    Some(buf) => {
                        debug_assert_eq!(
                            buf.len(),
                            received,
                            "the driver sizes the buffer it hands out"
                        );
                        Ok(buf)
                    }
                    None => Err(NetError::ProvidedBufferMissing).trans(),
                })
            }
            RecvItem::Single(single_item) => {
                let (res, recv_op) = single_item.into_inner();
                let received = match res {
                    Ok(0) => return Flow::End,
                    Ok(received) => received,
                    Err(report) => return Flow::Yield(Err(report).trans()),
                };

                self.exhausted_streak = 0;
                let Some(recv_op) = recv_op else {
                    return Flow::Yield(Err(NetError::OpBufferLost).trans());
                };
                let mut buf = recv_op.buf;
                buf.set_len(received);
                Flow::Yield(Ok(buf))
            }
            RecvItem::AllocError(err) => Flow::Yield(Err(err)),
        }
    }

    fn classify_error(&mut self, report: Report<DriverError>) -> Flow {
        if is_buffer_ring_exhausted(&report) && self.rearm_exhausted() {
            return Flow::Retry;
        }
        Flow::Yield(Err(report).trans())
    }
}

/// 一条完成之后这条流该做什么。
enum Flow {
    /// 交给用户。
    Yield(Result<FixedBuf>),
    /// 这一条不该交给用户（降级 / 重 arm），再取一轮。
    Retry,
    /// 流正常结束。
    End,
}

impl<'rt, S, P> Drop for RecvStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>>,
    P: SocketTokenPtr<'rt>,
{
    fn drop(&mut self) {
        if let Some(mode) = self.mode.take() {
            self.inner.token().stash_recv((
                mode,
                self.native_delivered,
                self.rearms,
                self.exhausted_streak,
            ));
        }
    }
}

impl<'rt, S, P> Stream for RecvStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    type Item = Result<FixedBuf>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: `RecvStream` 的字段都不是自引用的，投影出 `&mut` 不违反 pin 契约。
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let item = match this.poll_step(cx) {
                Poll::Ready(Some(item)) => item,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };

            match this.classify(item) {
                Flow::Yield(item) => return Poll::Ready(Some(item)),
                Flow::Retry => continue,
                Flow::End => return Poll::Ready(None),
            }
        }
    }
}
