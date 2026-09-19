//! multishot accept 的用户侧流。

use veloq_std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use diagweave::prelude::*;
use futures_core::Stream;
use veloq_driver_native::{
    OwnedRawHandle,
    driver::PlatformSlotSpec,
    op::{AcceptMulti, Op, OpItem, OpSubmitter},
    peer_addr_of_handle,
};

use crate::{
    error::Result,
    net::{
        common::{InnerSocket, SocketTokenPtr},
        tcp::GenericTcpStream,
    },
    runtime::context::Ctx,
};

type AcceptItem = OpItem<AcceptMulti, PlatformSlotSpec>;

/// [`crate::net::TcpListener::accept_multi`] 产出的连接流。
pub struct AcceptStream<'rt, S: OpSubmitter<'rt, Ctx<'rt>>, P: SocketTokenPtr<'rt>> {
    stream: Option<S::Stream<AcceptMulti>>,
    inner: InnerSocket<'rt, P>,
    submitter: S,
    ctx: Ctx<'rt>,
}

impl<'rt, S, P> AcceptStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>> + Copy,
    P: SocketTokenPtr<'rt>,
{
    pub(crate) fn new(ctx: Ctx<'rt>, inner: InnerSocket<'rt, P>, submitter: S) -> Self {
        let stream = inner
            .token()
            .take_accept::<S::Stream<AcceptMulti>>()
            .unwrap_or_else(|| {
                let fd = inner.fd();
                ctx.submit_stream(&submitter, Op::new(AcceptMulti { fd }))
            });
        Self {
            stream: Some(stream),
            inner,
            submitter,
            ctx,
        }
    }

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

    fn item(&self, item: AcceptItem) -> Result<(GenericTcpStream<'rt, S, P>, SocketAddr)> {
        let (res, _) = item.into_inner();
        let accepted = res.trans()?;
        let addr = peer_addr_of_handle(accepted.raw()).trans()?;
        self.make_item(accepted, addr)
    }

    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Option<AcceptItem>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        // SAFETY: the stream is not self-referential and stays in place while polled.
        unsafe { Pin::new_unchecked(stream) }.poll_next(cx)
    }
}

impl<'rt, S, P> Drop for AcceptStream<'rt, S, P>
where
    S: OpSubmitter<'rt, Ctx<'rt>>,
    P: SocketTokenPtr<'rt>,
{
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.inner.token().stash_accept(stream);
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
        let this = unsafe { self.get_unchecked_mut() };
        match this.poll_step(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(this.item(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
