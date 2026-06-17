use super::open_options::OpenOptions;
use crate::{
    error::{Error, Result},
    fs::error::FsError,
    io::{AsyncBufRead, AsyncBufWrite},
    runtime::context::{Ctx, submit_control_task},
};
use diagweave::prelude::*;
use std::{
    cell::Cell,
    future::{Future, IntoFuture},
    path::Path,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};
use veloq_buf::FixedBuf;
use veloq_driver_native::{
    RawHandle, RawHandleKind,
    driver::Driver,
    op::{
        DetachedSubmitter, Fallocate, FileSyncFileRangeRaw, Fsync, IoFd, LocalSubmitter, Op,
        OpSubmitter, ReadFixed, WriteFixed,
    },
};
#[cfg(windows)]
use windows_sys::Win32::Foundation::CloseHandle;

#[cfg(not(unix))]
macro_rules! ignore {
    ($($x:expr),* $(,)?) => {
        $(let _ = $x;)*
    };
}

fn close_raw_handle(raw: RawHandle) {
    debug_assert!(
        matches!(raw.borrow().kind(), RawHandleKind::File),
        "file handle expected"
    );
    #[cfg(unix)]
    unsafe {
        libc::close(raw.raw().as_fd());
    }
    #[cfg(windows)]
    match raw.borrow().kind() {
        RawHandleKind::File => unsafe {
            CloseHandle(raw.raw().as_handle());
        },
        RawHandleKind::Socket => {
            use veloq_driver_native::Socket;
            let _ = unsafe { Socket::from_raw(raw.raw()) };
        }
    }
}

// fn unregister_fixed_fd is no longer needed globally

pub struct LocalFile<'rt, 'reg> {
    pub(crate) raw: RawHandle,
    pub(crate) fd: IoFd,
    pub(crate) submitter: LocalSubmitter<Ctx<'rt, 'reg>>,
    pub(crate) pos: Cell<u64>,
    pub(crate) ctx: Ctx<'rt, 'reg>,
}

pub struct File<'rt, 'reg> {
    pub(crate) raw: RawHandle,
    pub(crate) fd: IoFd,
    pub(crate) owner_worker_id: usize,
    pub(crate) submitter: DetachedSubmitter,
    pub(crate) pos: AtomicU64,
    pub(crate) ctx: Ctx<'rt, 'reg>,
}

impl<'rt, 'reg> Drop for LocalFile<'rt, 'reg> {
    fn drop(&mut self) {
        self.ctx.runtime_ctx.shared().extra_tls.with(|extra| {
            let mut driver = extra.driver.borrow_mut();
            let _ = driver.unregister_files(vec![self.fd]);
        });
        close_raw_handle(self.raw);
    }
}

impl<'rt, 'reg> Drop for File<'rt, 'reg> {
    fn drop(&mut self) {
        let current_worker_id = self.ctx.runtime_ctx.worker_id();
        if current_worker_id == self.owner_worker_id {
            self.ctx.runtime_ctx.shared().extra_tls.with(|extra| {
                let mut driver = extra.driver.borrow_mut();
                let _ = driver.unregister_files(vec![self.fd]);
            });
        } else {
            submit_control_task(self.ctx.runtime_ctx.shared(), self.owner_worker_id, self.fd);
        }
    }
}

impl<'rt, 'reg> LocalFile<'rt, 'reg> {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn seek(&self, pos: u64) {
        self.pos.set(pos);
    }

    pub fn stream_position(&self) -> u64 {
        self.pos.get()
    }

    pub async fn read_at(&self, buf: FixedBuf, offset: u64) -> Result<(usize, FixedBuf)> {
        self.read_at_subset(buf, offset, 0).await
    }

    pub async fn write_at(&self, buf: FixedBuf, offset: u64) -> Result<(usize, FixedBuf)> {
        self.write_at_subset(buf, offset, 0).await
    }

    pub async fn read_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = ReadFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let buf = op.map(|o| o.buf).ok_or(FsError::OpBufferLost)?;
        let res = res
            .with_ctx("op", "read_at_subset")
            .with_ctx("offset", offset)
            .with_ctx("buf_offset", buf_offset)
            .with_ctx("buf_len", buf.len())
            .trans()?;
        Ok((res, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = WriteFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        let buf = op.map(|o| o.buf).ok_or(FsError::OpBufferLost)?;
        let res = res
            .with_ctx("op", "write_at_subset")
            .with_ctx("offset", offset)
            .with_ctx("buf_offset", buf_offset)
            .with_ctx("buf_len", buf.len())
            .trans()?;
        Ok((res, buf))
    }

    pub async fn sync_all(&self) -> Result<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: false,
        };

        let (res, _) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        res.map(|_| ()).trans()
    }

    pub async fn sync_data(&self) -> Result<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: true,
        };

        let (res, _) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        res.map(|_| ()).trans()
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
        let op = Fallocate {
            fd: self.fd,
            mode: 0,
            offset,
            len,
        };

        let (res, _) = self
            .ctx
            .submit(&self.submitter, Op::new(op))
            .await
            .into_inner();
        res.map(|_| ()).trans()
    }
}

impl<'rt, 'reg> AsyncBufRead for LocalFile<'rt, 'reg> {
    type Error = Report<Error>;

    async fn read(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.read_at(buf, offset).await?;
        self.pos.set(self.pos.get() + n as u64);
        Ok((n, buf))
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self.read_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return FsError::UnexpectedEof.trans();
            }
            total += n;
            self.pos.set(self.pos.get() + n as u64);
        }
        Ok((total, buf))
    }
}

impl<'rt, 'reg> AsyncBufWrite for LocalFile<'rt, 'reg> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.write_at(buf, offset).await?;
        self.pos.set(self.pos.get() + n as u64);
        Ok((n, buf))
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self.write_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return FsError::WriteZero.trans();
            }
            total += n;
            self.pos.set(self.pos.get() + n as u64);
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> Result<()> {
        self.sync_data().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.sync_all().await
    }
}

impl<'rt, 'reg> File<'rt, 'reg> {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn seek(&self, pos: u64) {
        self.pos.store(pos, Ordering::Relaxed);
    }

    pub fn stream_position(&self) -> u64 {
        self.pos.load(Ordering::Relaxed)
    }

    pub async fn read_at(&self, buf: FixedBuf, offset: u64) -> Result<(usize, FixedBuf)> {
        self.read_at_subset(buf, offset, 0).await
    }

    pub async fn write_at(&self, buf: FixedBuf, offset: u64) -> Result<(usize, FixedBuf)> {
        self.write_at_subset(buf, offset, 0).await
    }

    pub async fn read_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = ReadFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = self
            .ctx
            .submit_to(self.owner_worker_id, Op::new(op))
            .await?;
        let buf = op.buf;
        let res = res
            .with_ctx("op", "read_at_subset")
            .with_ctx("offset", offset)
            .with_ctx("buf_offset", buf_offset)
            .with_ctx("buf_len", buf.len())
            .trans()?;
        Ok((res, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op = WriteFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = self
            .ctx
            .submit_to(self.owner_worker_id, Op::new(op))
            .await?;
        let buf = op.buf;
        let res = res
            .with_ctx("op", "write_at_subset")
            .with_ctx("offset", offset)
            .with_ctx("buf_offset", buf_offset)
            .with_ctx("buf_len", buf.len())
            .trans()?;
        Ok((res, buf))
    }

    pub async fn sync_all(&self) -> Result<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: false,
        };

        let (res, _) = self
            .ctx
            .submit_to(self.owner_worker_id, Op::new(op))
            .await?;
        res.map(|_| ()).trans()
    }

    pub async fn sync_data(&self) -> Result<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: true,
        };

        let (res, _) = self
            .ctx
            .submit_to(self.owner_worker_id, Op::new(op))
            .await?;
        res.map(|_| ()).trans()
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
        let op = Fallocate {
            fd: self.fd,
            mode: 0,
            offset,
            len,
        };

        let (res, _) = self
            .ctx
            .submit_to(self.owner_worker_id, Op::new(op))
            .await?;
        res.map(|_| ()).trans()
    }

    pub fn sync_range(&self, offset: u64, nbytes: u64) -> SyncRangeBuilder<'_, 'rt, 'reg> {
        SyncRangeBuilder::new(self, offset, nbytes)
    }
}

pub struct SyncRangeBuilder<'f, 'rt, 'reg> {
    file: &'f File<'rt, 'reg>,
    offset: u64,
    nbytes: u64,
    flags: u32,
}

impl<'f, 'rt, 'reg> SyncRangeBuilder<'f, 'rt, 'reg> {
    fn new(file: &'f File<'rt, 'reg>, offset: u64, nbytes: u64) -> Self {
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

impl<'f, 'rt, 'reg> IntoFuture for SyncRangeBuilder<'f, 'rt, 'reg> {
    type Output = Result<usize>;
    type IntoFuture = SyncRangeFuture<'rt, 'reg>;

    fn into_future(self) -> Self::IntoFuture {
        let op = FileSyncFileRangeRaw {
            fd: self.file.raw.raw(),
            offset: self.offset,
            nbytes: self.nbytes,
            flags: self.flags,
        };

        SyncRangeFuture {
            inner: self.file.ctx.submit(&self.file.submitter, Op::new(op)),
        }
    }
}

pub struct SyncRangeFuture<'rt, 'reg> {
    inner: <DetachedSubmitter as OpSubmitter<'reg, Ctx<'rt, 'reg>>>::Future<FileSyncFileRangeRaw>,
}

impl<'rt, 'reg> Future for SyncRangeFuture<'rt, 'reg> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match Pin::new(&mut this.inner).poll(cx) {
            Poll::Ready(res) => {
                let (res, _) = res.into_inner();
                Poll::Ready(res.trans())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'rt, 'reg> AsyncBufRead for File<'rt, 'reg> {
    type Error = Report<Error>;

    async fn read(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let offset = self.pos.load(Ordering::Relaxed);
        let (n, buf) = self.read_at(buf, offset).await?;
        self.pos.fetch_add(n as u64, Ordering::Relaxed);
        Ok((n, buf))
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.load(Ordering::Relaxed);
            let (n, b) = self.read_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return FsError::UnexpectedEof.trans();
            }
            total += n;
            self.pos.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok((total, buf))
    }
}

impl<'rt, 'reg> AsyncBufWrite for File<'rt, 'reg> {
    type Error = Report<Error>;

    async fn write(&self, buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let offset = self.pos.load(Ordering::Relaxed);
        let (n, buf) = self.write_at(buf, offset).await?;
        self.pos.fetch_add(n as u64, Ordering::Relaxed);
        Ok((n, buf))
    }

    async fn write_all(&self, mut buf: FixedBuf) -> Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.load(Ordering::Relaxed);
            let (n, b) = self.write_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return FsError::WriteZero.trans();
            }
            total += n;
            self.pos.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> Result<()> {
        self.sync_data().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.sync_all().await
    }
}

impl<'rt, 'reg> LocalFile<'rt, 'reg> {
    pub async fn open(ctx: Ctx<'rt, 'reg>, path: impl AsRef<Path>) -> Result<LocalFile<'rt, 'reg>> {
        OpenOptions::new().read(true).open_local(ctx, path).await
    }

    pub async fn create(
        ctx: Ctx<'rt, 'reg>,
        path: impl AsRef<Path>,
    ) -> Result<LocalFile<'rt, 'reg>> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_local(ctx, path)
            .await
    }
}

impl<'rt, 'reg> File<'rt, 'reg> {
    pub async fn open(ctx: Ctx<'rt, 'reg>, path: impl AsRef<Path>) -> Result<File<'rt, 'reg>> {
        OpenOptions::new().read(true).open(ctx, path).await
    }

    pub async fn create(ctx: Ctx<'rt, 'reg>, path: impl AsRef<Path>) -> Result<File<'rt, 'reg>> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(ctx, path)
            .await
    }
}
