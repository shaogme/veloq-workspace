use crate::runtime::context::submit;

use super::open_options::OpenOptions;

use veloq_buf::FixedBuf;
use veloq_driver::driver::Driver;
use veloq_driver::op::{
    DetachedSubmitter, Fallocate, Fsync, IoFd, LocalSubmitter, Op, OpSubmitter, ReadFixed,
    SyncFileRange, WriteFixed,
};
use veloq_driver::{RawHandle, RawHandleKind};

use std::cell::Cell;
use std::future::{Future, IntoFuture};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

fn driver_err(err: error_stack::Report<veloq_driver::error::DriverErrorKind>) -> io::Error {
    io::Error::other(format!("{err:#}"))
}

#[cfg(not(unix))]
macro_rules! ignore {
    ($($x:expr),* $(,)?) => {
        $(
            let _ = $x;
        )*
    };
}

// ============================================================================
// Internal Helper: InnerFile (RAII Wrapper)
// ============================================================================

pub(crate) struct InnerFile {
    raw: RawHandle,
    fd: IoFd,
}

impl InnerFile {
    #[inline]
    pub(crate) const fn new(raw: RawHandle, fd: IoFd) -> Self {
        Self { raw, fd }
    }

    #[inline]
    pub(crate) const fn fd(&self) -> IoFd {
        self.fd
    }
}

impl Drop for InnerFile {
    fn drop(&mut self) {
        if let Some(ctx) = crate::runtime::context::try_current()
            && let Some(driver) = ctx.driver().upgrade()
        {
            let _ = driver.borrow_mut().unregister_files(vec![self.fd]);
        }
        debug_assert!(
            matches!(self.raw.borrow().kind(), RawHandleKind::File),
            "InnerFile expects file-kind handle"
        );
        #[cfg(unix)]
        unsafe {
            libc::close(self.raw.raw().as_fd());
        }
        #[cfg(windows)]
        match self.raw.borrow().kind() {
            RawHandleKind::File => unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.raw.raw().as_handle());
            },
            RawHandleKind::Socket => {
                let _ = unsafe { veloq_driver::Socket::from_raw(self.raw.raw()) };
            }
        }
    }
}

// ============================================================================
// File Position Trait
// ============================================================================

pub trait FilePos: Send + 'static {
    fn new(v: u64) -> Self;
    fn get(&self) -> u64;
    fn set(&self, v: u64);
    fn add(&self, v: u64);
}

impl FilePos for Cell<u64> {
    fn new(v: u64) -> Self {
        Cell::new(v)
    }
    fn get(&self) -> u64 {
        self.get()
    }
    fn set(&self, v: u64) {
        self.set(v)
    }
    fn add(&self, v: u64) {
        self.set(self.get() + v)
    }
}

impl FilePos for AtomicU64 {
    fn new(v: u64) -> Self {
        AtomicU64::new(v)
    }
    fn get(&self) -> u64 {
        self.load(Ordering::Relaxed)
    }
    fn set(&self, v: u64) {
        self.store(v, Ordering::Relaxed)
    }
    fn add(&self, v: u64) {
        self.fetch_add(v, Ordering::Relaxed);
    }
}

// ============================================================================
// Generic File Components
// ============================================================================

pub struct GenericFile<S: OpSubmitter, P: FilePos> {
    pub(crate) inner: InnerFile,
    pub(crate) submitter: S,
    pub(crate) pos: P,
}

pub type LocalFile = GenericFile<LocalSubmitter, Cell<u64>>;
pub type File = GenericFile<DetachedSubmitter, AtomicU64>;

// ============================================================================
// SyncRange Types (Zero-allocation)
// ============================================================================

enum SyncRangeState<S: OpSubmitter> {
    Idle(Option<(S, SyncFileRange)>),
    Submitted(S::Future<SyncFileRange>),
}

pub struct SyncRangeFuture<S: OpSubmitter> {
    state: SyncRangeState<S>,
}

impl<S: OpSubmitter> Future for SyncRangeFuture<S> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: We use manual pin projection to access `state`.
        // `SyncRangeState` fields:
        // - `Idle` variant holds `S` (Clone) and `SyncFileRange` (Copy-like). Safe to move out?
        //   We use `Option` to take ownership.
        // - `Submitted` variant holds `S::Future`. This MUST be pinned.
        //   We ensure it is pinned when polling.
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                SyncRangeState::Idle(data) => {
                    let (submitter, op) = data
                        .take()
                        .expect("Polled after completion or invalid state");
                    let fut = submit(&submitter, Op::new(op));
                    this.state = SyncRangeState::Submitted(fut);
                }
                SyncRangeState::Submitted(fut) => {
                    // Safety: `fut` is structurally pinned because `self` is pinned.
                    let fut = unsafe { Pin::new_unchecked(fut) };
                    match fut.poll(cx) {
                        Poll::Ready(op_res) => {
                            let (res, _) = op_res.into_inner();
                            return Poll::Ready(res.map(|_| ()).map_err(driver_err));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

pub struct SyncRangeBuilder<'a, S: OpSubmitter, P: FilePos> {
    file: &'a GenericFile<S, P>,
    offset: u64,
    nbytes: u64,
    flags: u32,
}

impl<'a, S: OpSubmitter, P: FilePos> SyncRangeBuilder<'a, S, P> {
    fn new(file: &'a GenericFile<S, P>, offset: u64, nbytes: u64) -> Self {
        #[cfg(unix)]
        let flags = libc::SYNC_FILE_RANGE_WAIT_BEFORE
            | libc::SYNC_FILE_RANGE_WRITE
            | libc::SYNC_FILE_RANGE_WAIT_AFTER;
        #[cfg(not(unix))]
        let flags = 0;

        Self {
            file,
            offset,
            nbytes,
            flags,
        }
    }

    pub fn wait_before(mut self, wait: bool) -> Self {
        #[cfg(unix)]
        if wait {
            self.flags |= libc::SYNC_FILE_RANGE_WAIT_BEFORE;
        } else {
            self.flags &= !libc::SYNC_FILE_RANGE_WAIT_BEFORE;
        }
        #[cfg(not(unix))]
        ignore!(wait, &mut self);
        self
    }

    pub fn write(mut self, write: bool) -> Self {
        #[cfg(unix)]
        if write {
            self.flags |= libc::SYNC_FILE_RANGE_WRITE;
        } else {
            self.flags &= !libc::SYNC_FILE_RANGE_WRITE;
        }
        #[cfg(not(unix))]
        ignore!(write, &mut self);
        self
    }

    pub fn wait_after(mut self, wait: bool) -> Self {
        #[cfg(unix)]
        if wait {
            self.flags |= libc::SYNC_FILE_RANGE_WAIT_AFTER;
        } else {
            self.flags &= !libc::SYNC_FILE_RANGE_WAIT_AFTER;
        }
        #[cfg(not(unix))]
        ignore!(wait, &mut self);
        self
    }
}

impl<'a, S: OpSubmitter, P: FilePos> IntoFuture for SyncRangeBuilder<'a, S, P> {
    type Output = io::Result<()>;
    type IntoFuture = SyncRangeFuture<S>;

    fn into_future(self) -> Self::IntoFuture {
        let op = SyncFileRange {
            fd: self.file.inner.fd(),
            offset: self.offset,
            nbytes: self.nbytes,
            flags: self.flags,
        };

        let submitter = self.file.submitter.clone();

        SyncRangeFuture {
            state: SyncRangeState::Idle(Some((submitter, op))),
        }
    }
}

impl<S: OpSubmitter, P: FilePos> GenericFile<S, P> {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn seek(&self, pos: u64) {
        self.pos.set(pos);
    }

    pub fn stream_position(&self) -> u64 {
        self.pos.get()
    }

    pub async fn read_at(&self, buf: FixedBuf, offset: u64) -> io::Result<(usize, FixedBuf)> {
        self.read_at_subset(buf, offset, 0).await
    }

    pub async fn write_at(&self, buf: FixedBuf, offset: u64) -> io::Result<(usize, FixedBuf)> {
        self.write_at_subset(buf, offset, 0).await
    }

    pub async fn read_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = ReadFixed {
            fd: self.inner.fd(),
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();

        let buf = op
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = WriteFixed {
            fd: self.inner.fd(),
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();

        let buf = op
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }

    pub async fn sync_all(&self) -> io::Result<()> {
        let op = Fsync {
            fd: self.inner.fd(),
            datasync: false,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(driver_err)
    }

    pub async fn sync_data(&self) -> io::Result<()> {
        let op = Fsync {
            fd: self.inner.fd(),
            datasync: true,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(driver_err)
    }

    /// Sync a file range.
    ///
    /// On Windows, this falls back to `FlushFileBuffers` which syncs the entire file, ignoring the range.
    ///
    /// Returns a specific Future (Builder) that allows configuring flags.
    /// Usage: `file.sync_range(0, 100).wait_before(false).write(true).await`
    pub fn sync_range(&self, offset: u64, nbytes: u64) -> SyncRangeBuilder<'_, S, P> {
        SyncRangeBuilder::new(self, offset, nbytes)
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> io::Result<()> {
        let op = Fallocate {
            fd: self.inner.fd(),
            mode: 0, // Default mode
            offset,
            len,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(driver_err)
    }
}

impl<S: OpSubmitter, P: FilePos> crate::io::AsyncBufRead for GenericFile<S, P> {
    async fn read(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.read_at(buf, offset).await?;
        self.pos.add(n as u64);
        Ok((n, buf))
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self.read_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            total += n;
            self.pos.add(n as u64);
        }
        Ok((total, buf))
    }
}

impl<S: OpSubmitter, P: FilePos> crate::io::AsyncBufWrite for GenericFile<S, P> {
    async fn write(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.write_at(buf, offset).await?;
        self.pos.add(n as u64);
        Ok((n, buf))
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self.write_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            total += n;
            self.pos.add(n as u64);
        }
        Ok((total, buf))
    }

    fn flush(&self) -> impl std::future::Future<Output = io::Result<()>> {
        self.sync_data()
    }

    fn shutdown(&self) -> impl std::future::Future<Output = io::Result<()>> {
        self.sync_all()
    }
}

// --- Specific Implementations ---

impl LocalFile {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<LocalFile> {
        OpenOptions::new().read(true).open_local(path).await
    }

    pub async fn create(path: impl AsRef<Path>) -> io::Result<LocalFile> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_local(path)
            .await
    }
}

impl File {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new().read(true).open(path).await
    }

    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }
}
