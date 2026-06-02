use super::open_options::OpenOptions;
use crate::fs::error::FsError;
use crate::runtime::context::RuntimeContext;

use diagweave::report::{Diagnostic, Report, ResultReportExt};
use veloq_buf::FixedBuf;
use veloq_driver_native::driver::Driver;
use veloq_driver_native::op::{
    DetachedSubmitter, Fallocate, FileFallocateRaw, FileFsyncRaw, FileReadRaw,
    FileSyncFileRangeRaw, FileWriteRaw, Fsync, IoFd, LocalSubmitter, Op, ReadFixed, WriteFixed,
};
use veloq_driver_native::{RawHandle, RawHandleKind};

use std::cell::Cell;
use std::future::{Future, IntoFuture};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use crate::error::{Error, Result};

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
            windows_sys::Win32::Foundation::CloseHandle(raw.raw().as_handle());
        },
        RawHandleKind::Socket => {
            let _ = unsafe { veloq_driver_native::Socket::from_raw(raw.raw()) };
        }
    }
}

// fn unregister_fixed_fd is no longer needed globally

pub struct LocalFile<'a, 'ctx> {
    pub(crate) raw: RawHandle,
    pub(crate) fd: IoFd,
    pub(crate) submitter: LocalSubmitter<RuntimeContext<'a, 'ctx>>,
    pub(crate) pos: Cell<u64>,
    pub(crate) ctx: RuntimeContext<'a, 'ctx>,
}

pub struct File<'a, 'ctx> {
    pub(crate) raw: RawHandle,
    pub(crate) submitter: DetachedSubmitter,
    pub(crate) pos: AtomicU64,
    pub(crate) ctx: RuntimeContext<'a, 'ctx>,
}

impl<'a, 'ctx> Drop for LocalFile<'a, 'ctx> {
    fn drop(&mut self) {
        self.ctx.scope.shared().extra_tls.with(|extra| {
            let mut driver = extra.driver.borrow_mut();
            let _ = driver.unregister_files(vec![self.fd]);
        });
        close_raw_handle(self.raw);
    }
}

impl<'a, 'ctx> Drop for File<'a, 'ctx> {
    fn drop(&mut self) {
        close_raw_handle(self.raw);
    }
}

impl<'a, 'ctx> LocalFile<'a, 'ctx> {
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
        let buf = op
            .map(|o| o.buf)
            .ok_or(FsError::op_buffer_lost())
            .diag(|r| r.map_err(Into::into))?;
        Ok((res.trans_inner_err()?, buf))
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
        let buf = op
            .map(|o| o.buf)
            .ok_or(FsError::op_buffer_lost())
            .diag(|r| r.map_err(Into::into))?;
        Ok((res.trans_inner_err()?, buf))
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
        res.map(|_| ()).trans_inner_err()
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
        res.map(|_| ()).trans_inner_err()
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
        res.map(|_| ()).trans_inner_err()
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for LocalFile<'a, 'ctx> {
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
                return Err(FsError::UnexpectedEof.to_report()).trans_inner_err();
            }
            total += n;
            self.pos.set(self.pos.get() + n as u64);
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufWrite for LocalFile<'a, 'ctx> {
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
            let (n, b) = self.read_at_subset(buf, offset, total).await?;
            buf = b;
            if n == 0 {
                return Err(FsError::WriteZero.to_report()).trans_inner_err();
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

impl<'a, 'ctx> File<'a, 'ctx> {
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
        let op: FileReadRaw = FileReadRaw {
            fd: self.raw.raw(),
            buf,
            offset,
            buf_offset,
        };

        let owner = self.ctx.scope.worker_id();
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let buf = op.buf;
        Ok((res.trans_inner_err()?, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> Result<(usize, FixedBuf)> {
        let op: FileWriteRaw = FileWriteRaw {
            fd: self.raw.raw(),
            buf,
            offset,
            buf_offset,
        };

        let owner = self.ctx.scope.worker_id();
        let (res, op) = self.ctx.submit_to(owner, Op::new(op)).await?;
        let buf = op.buf;
        Ok((res.trans_inner_err()?, buf))
    }

    pub async fn sync_all(&self) -> Result<()> {
        let op: FileFsyncRaw = FileFsyncRaw {
            fd: self.raw.raw(),
            datasync: false,
        };

        let owner = self.ctx.scope.worker_id();
        let (res, _) = self.ctx.submit_to(owner, Op::new(op)).await?;
        res.map(|_| ()).trans_inner_err()
    }

    pub async fn sync_data(&self) -> Result<()> {
        let op: FileFsyncRaw = FileFsyncRaw {
            fd: self.raw.raw(),
            datasync: true,
        };

        let owner = self.ctx.scope.worker_id();
        let (res, _) = self.ctx.submit_to(owner, Op::new(op)).await?;
        res.map(|_| ()).trans_inner_err()
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
        let op: FileFallocateRaw = FileFallocateRaw {
            fd: self.raw.raw(),
            mode: 0,
            offset,
            len,
        };

        let owner = self.ctx.scope.worker_id();
        let (res, _) = self.ctx.submit_to(owner, Op::new(op)).await?;
        res.map(|_| ()).trans_inner_err()
    }

    pub fn sync_range(&self, offset: u64, nbytes: u64) -> SyncRangeBuilder<'_, 'a, 'ctx> {
        SyncRangeBuilder::new(self, offset, nbytes)
    }
}

pub struct SyncRangeBuilder<'f, 'a, 'ctx> {
    file: &'f File<'a, 'ctx>,
    offset: u64,
    nbytes: u64,
    flags: u32,
}

impl<'f, 'a, 'ctx> SyncRangeBuilder<'f, 'a, 'ctx> {
    fn new(file: &'f File<'a, 'ctx>, offset: u64, nbytes: u64) -> Self {
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

impl<'f, 'a, 'ctx> IntoFuture for SyncRangeBuilder<'f, 'a, 'ctx> {
    type Output = Result<usize>;
    type IntoFuture = SyncRangeFuture<'a, 'ctx>;

    fn into_future(self) -> Self::IntoFuture {
        let op: FileSyncFileRangeRaw = FileSyncFileRangeRaw {
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

pub struct SyncRangeFuture<'a, 'ctx> {
    inner: <DetachedSubmitter as veloq_driver_native::op::OpSubmitter<
        'ctx,
        RuntimeContext<'a, 'ctx>,
    >>::Future<FileSyncFileRangeRaw>,
}

impl<'a, 'ctx> Future for SyncRangeFuture<'a, 'ctx> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match Pin::new(&mut this.inner).poll(cx) {
            Poll::Ready(res) => {
                let (res, _) = res.into_inner();
                Poll::Ready(res.trans_inner_err())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a, 'ctx> crate::io::AsyncBufRead for File<'a, 'ctx> {
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
                return Err(FsError::UnexpectedEof.to_report()).trans_inner_err();
            }
            total += n;
            self.pos.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok((total, buf))
    }
}

impl<'a, 'ctx> crate::io::AsyncBufWrite for File<'a, 'ctx> {
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
                return Err(FsError::WriteZero.to_report()).trans_inner_err();
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

impl<'a, 'ctx> LocalFile<'a, 'ctx> {
    pub async fn open(
        ctx: RuntimeContext<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<LocalFile<'a, 'ctx>> {
        OpenOptions::new().read(true).open_local(ctx, path).await
    }

    pub async fn create(
        ctx: RuntimeContext<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<LocalFile<'a, 'ctx>> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_local(ctx, path)
            .await
    }
}

impl<'a, 'ctx> File<'a, 'ctx> {
    pub async fn open(
        ctx: RuntimeContext<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<File<'a, 'ctx>> {
        OpenOptions::new().read(true).open(ctx, path).await
    }

    pub async fn create(
        ctx: RuntimeContext<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<File<'a, 'ctx>> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(ctx, path)
            .await
    }
}
