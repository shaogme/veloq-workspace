use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite};
use veloq_buf::FixedBuf;

use crate::io::{AsyncBufRead, AsyncBufWrite};

type BufIoResult = io::Result<(usize, FixedBuf)>;
type BufIoFuture<'a> = Pin<Box<dyn Future<Output = BufIoResult> + 'a>>;
type BoxBufIoFuture = Pin<Box<dyn Future<Output = BufIoResult>>>;
type BoxIoFuture = Pin<Box<dyn Future<Output = io::Result<()>>>>;

/// A compatibility layer to adapt `AsyncBufRead` / `AsyncBufWrite` to `futures::io::AsyncRead` / `futures::io::AsyncWrite`.
///
/// This adapter uses an internal `FixedBuf` to bridge the gap between the owned-buffer model of `veloq`
/// and the borrowed-buffer model of standard async traits.
///
/// Note: This adapter boxes the inner I/O object to ensure memory stability, allowing `Compat` to be `Unpin`.
pub struct Compat<T> {
    // We box the inner object to ensure its address is stable even if Compat moves.
    // This allows Compat to be `Unpin`.
    inner: Option<Box<T>>,
    buf: Option<FixedBuf>,

    // Futures for async operations.
    // We erase the lifetime to store them, as they borrow `inner` (which is stable on heap).
    // We must ensure they are dropped before `inner` (the Box) is dropped.
    read_future: Option<BoxBufIoFuture>,
    write_future: Option<BoxBufIoFuture>,
    flush_future: Option<BoxIoFuture>,
    shutdown_future: Option<BoxIoFuture>,

    read_pos: usize,
    read_cap: usize,
    write_len: usize,
}

// Safety: If T is Send+Sync, then Compat is Send.
unsafe impl<T: Send + Sync> Send for Compat<T> {}

impl<T> Compat<T> {
    pub fn new(inner: T, buf: FixedBuf) -> Self {
        Self {
            inner: Some(Box::new(inner)),
            buf: Some(buf),
            read_future: None,
            write_future: None,
            flush_future: None,
            shutdown_future: None,
            read_pos: 0,
            read_cap: 0,
            write_len: 0,
        }
    }

    pub fn into_inner(mut self) -> (T, Option<FixedBuf>) {
        // Drop futures first to release borrows on inner
        self.read_future = None;
        self.write_future = None;
        self.flush_future = None;
        self.shutdown_future = None;

        (*self.inner.take().unwrap(), self.buf.take())
    }
}

impl<T> Drop for Compat<T> {
    fn drop(&mut self) {
        // Manually drop futures to ensure they are destroyed before `inner` (Box) is dropped.
        self.read_future = None;
        self.write_future = None;
        self.flush_future = None;
        self.shutdown_future = None;
    }
}

// Helper to erase lifetime of future.
unsafe fn erase_lifetime_read(fut: BufIoFuture<'_>) -> BoxBufIoFuture {
    unsafe { mem::transmute(fut) }
}

unsafe fn erase_lifetime_void(
    fut: Pin<Box<dyn Future<Output = io::Result<()>> + '_>>,
) -> BoxIoFuture {
    unsafe { mem::transmute(fut) }
}

impl<T: AsyncBufRead> AsyncRead for Compat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out_buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Since Compat is Unpin (all fields are Unpin or handled), we can deref.
        let this = &mut *self;

        loop {
            if let Some(mut fut) = this.read_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => match res {
                        Ok((n, buf)) => {
                            this.buf = Some(buf);
                            this.read_pos = 0;
                            this.read_cap = n;
                            if n == 0 {
                                return Poll::Ready(Ok(0));
                            }
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    },
                    Poll::Pending => {
                        this.read_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            if this.read_pos < this.read_cap {
                let available = this.read_cap - this.read_pos;
                let to_copy = std::cmp::min(available, out_buf.len());

                if let Some(buf) = &this.buf {
                    let src = &buf.as_slice()[this.read_pos..this.read_pos + to_copy];
                    out_buf[..to_copy].copy_from_slice(src);

                    this.read_pos += to_copy;
                    return Poll::Ready(Ok(to_copy));
                }
            }

            if out_buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            let mut buf = match this.buf.take() {
                Some(b) => b,
                None => {
                    return Poll::Ready(Err(io::Error::other("Buffer missing for read")));
                }
            };

            // Reset length to capacity to ensure we read as much as possible.
            let cap = buf.capacity();
            buf.set_len(cap);

            let inner = this.inner.as_ref().expect("Compat polled after into_inner");
            let fut = inner.read(buf);
            this.read_future = Some(unsafe { erase_lifetime_read(Box::pin(fut)) });
        }
    }
}

impl<T: AsyncBufWrite> AsyncWrite for Compat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;

        loop {
            if let Some(mut fut) = this.write_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => {
                        let (_, buf) = res?;
                        this.buf = Some(buf);
                        this.write_len = 0;
                    }
                    Poll::Pending => {
                        this.write_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            let buf = match this.buf.as_mut() {
                Some(b) => b,
                None => {
                    return Poll::Ready(Err(io::Error::other("Buffer missing for write")));
                }
            };

            let capacity = buf.capacity();
            let current_len = this.write_len;
            let available = capacity - current_len;

            if available > 0 {
                let to_copy = std::cmp::min(available, data.len());
                {
                    let dest = &mut buf.spare_capacity_mut()[current_len..current_len + to_copy];
                    dest.copy_from_slice(&data[..to_copy]);
                }
                this.write_len += to_copy;
                return Poll::Ready(Ok(to_copy));
            }

            if this.write_len == 0 {
                return Poll::Ready(Err(io::Error::other("Buffer has zero capacity")));
            }

            let mut buf = this.buf.take().unwrap();
            buf.set_len(this.write_len);

            let inner = this.inner.as_ref().expect("Compat polled after into_inner");
            let fut = inner.write_all(buf);
            this.write_future = Some(unsafe { erase_lifetime_read(Box::pin(fut)) });
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;

        loop {
            if let Some(mut fut) = this.write_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => {
                        let (_, buf) = res?;
                        this.buf = Some(buf);
                        this.write_len = 0;
                    }
                    Poll::Pending => {
                        this.write_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            if this.write_len > 0
                && let Some(mut buf) = this.buf.take()
            {
                buf.set_len(this.write_len);

                let inner = this.inner.as_ref().expect("Compat polled after into_inner");
                let fut = inner.write_all(buf);
                this.write_future = Some(unsafe { erase_lifetime_read(Box::pin(fut)) });
                continue;
            }

            if let Some(mut fut) = this.flush_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => return Poll::Ready(res),
                    Poll::Pending => {
                        this.flush_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            let inner = this.inner.as_ref().expect("Compat polled after into_inner");
            let fut = inner.flush();
            this.flush_future = Some(unsafe { erase_lifetime_void(Box::pin(fut)) });
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;

        loop {
            if let Some(mut fut) = this.write_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => {
                        let (_, buf) = res?;
                        this.buf = Some(buf);
                        this.write_len = 0;
                    }
                    Poll::Pending => {
                        this.write_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            if this.write_len > 0
                && let Some(mut buf) = this.buf.take()
            {
                buf.set_len(this.write_len);

                let inner = this.inner.as_ref().expect("Compat polled after into_inner");
                let fut = inner.write_all(buf);
                this.write_future = Some(unsafe { erase_lifetime_read(Box::pin(fut)) });
                continue;
            }

            if let Some(mut fut) = this.shutdown_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => return Poll::Ready(res),
                    Poll::Pending => {
                        this.shutdown_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            let inner = this.inner.as_ref().expect("Compat polled after into_inner");
            let fut = inner.shutdown();
            this.shutdown_future = Some(unsafe { erase_lifetime_void(Box::pin(fut)) });
        }
    }
}
