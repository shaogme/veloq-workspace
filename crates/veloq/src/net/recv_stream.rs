//! multishot recv 的用户侧流。
//!
//! 每一条流都直接提交 `RecvMulti`。provided buffer 的分配、重发、背压和回收由
//! driver/backend 负责，网络 facade 只消费完成记录。
//!
//! [`TcpStream::recv_multi`]: crate::net::TcpStream::recv_multi
//!
//! 收益就是 provided buffer 的收益：**buffer 只在数据到达时才与连接绑定**。一万个挂着
//! `recv_multi()` 的空闲连接不占任何接收缓冲，而一万个挂着 `recv()` 的连接各压一个。

use veloq_std::{
    pin::Pin,
    task::{Context, Poll},
};

use diagweave::prelude::*;
use futures_core::Stream;
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    driver::PlatformSlotSpec,
    op::{Op, OpItem, OpSubmitter, RecvMulti},
};

use crate::{
    error::Result,
    net::{
        common::{InnerSocket, SocketTokenPtr},
        error::NetError,
    },
    runtime::context::Ctx,
};

type ProvidedItem = OpItem<RecvMulti, PlatformSlotSpec>;

/// [`crate::net::TcpStream::recv_multi`] 产出的数据流。
///
/// 每一项是 driver 交付的一个 [`FixedBuf`]，长度已经是实际收到的字节数。**对端关闭时
/// 流正常结束**（`poll_next` 返回 `None`），而不是产出一个空 buffer——「读到 0 字节」在
/// 这里仅用流的终点表达。
///
/// 流被丢弃时会取消在途的操作（句柄的 `Drop` → `mark_orphaned` +
/// `CancelRequest::abandon`），内核随后投递的完成走 orphan cleanup，其中被挑走的 buffer 会
/// 还回 driver。
///
/// 与所有其它操作一样，这条流在**创建它的那个 worker** 上提交——socket 的注册描述符是
/// per-worker 的，provided buffer 也由对应 driver 管理。
pub struct RecvStream<'rt, S: OpSubmitter<'rt, Ctx<'rt>>, P: SocketTokenPtr<'rt>> {
    stream: Option<S::Stream<RecvMulti>>,
    inner: InnerSocket<'rt, P>,
}

impl<'rt, S, P> RecvStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    pub(crate) fn new(ctx: Ctx<'rt>, inner: InnerSocket<'rt, P>, submitter: S) -> Result<Self> {
        let stream = inner
            .token()
            .take_recv::<S::Stream<RecvMulti>>()
            .unwrap_or_else(|| {
                let fd = inner.fd();
                ctx.submit_stream(&submitter, Op::new(RecvMulti { fd }))
            });
        Ok(Self {
            stream: Some(stream),
            inner,
        })
    }

    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Option<ProvidedItem>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        // SAFETY: the stream is not self-referential and stays in place while polled.
        unsafe { Pin::new_unchecked(stream) }.poll_next(cx)
    }

    fn classify(item: ProvidedItem) -> Flow {
        let (res, provided) = item.into_inner();
        let received = match res {
            Ok(0) => return Flow::End,
            Ok(received) => received,
            Err(report) => return Flow::Yield(Err(report).trans()),
        };

        let Some(buf) = provided.and_then(|provided| provided.buf) else {
            return Flow::Yield(Err(NetError::ProvidedBufferMissing).trans());
        };
        debug_assert_eq!(
            buf.len(),
            received,
            "the driver sizes the buffer it hands out"
        );
        Flow::Yield(Ok(buf))
    }
}

enum Flow {
    Yield(Result<FixedBuf>),
    End,
}

impl<'rt, S, P> Drop for RecvStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>>,
    P: SocketTokenPtr<'rt>,
{
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.inner.token().stash_recv(stream);
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
        let this = unsafe { self.get_unchecked_mut() };
        let item = match this.poll_step(cx) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        match Self::classify(item) {
            Flow::Yield(item) => Poll::Ready(Some(item)),
            Flow::End => Poll::Ready(None),
        }
    }
}
