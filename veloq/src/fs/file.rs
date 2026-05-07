use super::open_options::OpenOptions;
use crate::runtime::context::submit;

use veloq_buf::FixedBuf;
use veloq_driver::driver::Driver;
use veloq_driver::op::{
    DetachedSubmitter, Fallocate, FileFallocateRaw, FileFsyncRaw, FileReadRaw,
    FileSyncFileRangeRaw, FileWriteRaw, Fsync, IoFd, LocalSubmitter, Op, ReadFixed, WriteFixed,
};
use veloq_driver::{RawHandle, RawHandleKind};

use std::cell::Cell;
use std::future::{Future, IntoFuture};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error, to_io_error};

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
            let _ = unsafe { veloq_driver::Socket::from_raw(raw.raw()) };
        }
    }
}

fn unregister_fixed_fd(fd: IoFd) {
    if let Some(ctx) = crate::runtime::context::try_current() {
        let _ = ctx.driver().borrow_mut().unregister_files(vec![fd]);
    }
}

pub struct LocalFile {
    pub(crate) raw: RawHandle,
    pub(crate) fd: IoFd,
    pub(crate) submitter: LocalSubmitter,
    pub(crate) pos: Cell<u64>,
}

pub struct File {
    pub(crate) raw: RawHandle,
    pub(crate) submitter: DetachedSubmitter,
    pub(crate) pos: AtomicU64,
}

impl Drop for LocalFile {
    fn drop(&mut self) {
        unregister_fixed_fd(self.fd);
        close_raw_handle(self.raw);
    }
}

impl Drop for File {
    fn drop(&mut self) {
        close_raw_handle(self.raw);
    }
}

impl LocalFile {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn seek(&self, pos: u64) {
        self.pos.set(pos);
    }

    pub fn stream_position(&self) -> u64 {
        self.pos.get()
    }

    pub async fn read_at(&self, buf: FixedBuf, offset: u64) -> VeloqResult<(usize, FixedBuf)> {
        self.read_at_subset(buf, offset, 0).await
    }

    pub async fn write_at(&self, buf: FixedBuf, offset: u64) -> VeloqResult<(usize, FixedBuf)> {
        self.write_at_subset(buf, offset, 0).await
    }

    pub async fn read_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op = ReadFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op = WriteFixed {
            fd: self.fd,
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn sync_all(&self) -> VeloqResult<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: false,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub async fn sync_data(&self) -> VeloqResult<()> {
        let op = Fsync {
            fd: self.fd,
            datasync: true,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> VeloqResult<()> {
        let op = Fallocate {
            fd: self.fd,
            mode: 0,
            offset,
            len,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }
}

impl crate::io::AsyncBufRead for LocalFile {
    async fn read(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.read_at(buf, offset).await.map_err(to_io_error)?;
        self.pos.set(self.pos.get() + n as u64);
        Ok((n, buf))
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self
                .read_at_subset(buf, offset, total)
                .await
                .map_err(to_io_error)?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            total += n;
            self.pos.set(self.pos.get() + n as u64);
        }
        Ok((total, buf))
    }
}

impl crate::io::AsyncBufWrite for LocalFile {
    async fn write(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.get();
        let (n, buf) = self.write_at(buf, offset).await.map_err(to_io_error)?;
        self.pos.set(self.pos.get() + n as u64);
        Ok((n, buf))
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.get();
            let (n, b) = self
                .write_at_subset(buf, offset, total)
                .await
                .map_err(to_io_error)?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            total += n;
            self.pos.set(self.pos.get() + n as u64);
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> io::Result<()> {
        self.sync_data().await.map_err(to_io_error)
    }

    async fn shutdown(&self) -> io::Result<()> {
        self.sync_all().await.map_err(to_io_error)
    }
}

impl File {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn seek(&self, pos: u64) {
        self.pos.store(pos, Ordering::Relaxed);
    }

    pub fn stream_position(&self) -> u64 {
        self.pos.load(Ordering::Relaxed)
    }

    pub async fn read_at(&self, buf: FixedBuf, offset: u64) -> VeloqResult<(usize, FixedBuf)> {
        self.read_at_subset(buf, offset, 0).await
    }

    pub async fn write_at(&self, buf: FixedBuf, offset: u64) -> VeloqResult<(usize, FixedBuf)> {
        self.write_at_subset(buf, offset, 0).await
    }

    pub async fn read_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op: FileReadRaw = FileReadRaw {
            fd: self.raw.raw(),
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn write_at_subset(
        &self,
        buf: FixedBuf,
        offset: u64,
        buf_offset: usize,
    ) -> VeloqResult<(usize, FixedBuf)> {
        let op: FileWriteRaw = FileWriteRaw {
            fd: self.raw.raw(),
            buf,
            offset,
            buf_offset,
        };

        let (res, op) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op.map(|o| o.buf).ok_or_else(|| {
            from_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))
        })?;
        Ok((res.map_err(from_driver_report)?, buf))
    }

    pub async fn sync_all(&self) -> VeloqResult<()> {
        let op: FileFsyncRaw = FileFsyncRaw {
            fd: self.raw.raw(),
            datasync: false,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub async fn sync_data(&self) -> VeloqResult<()> {
        let op: FileFsyncRaw = FileFsyncRaw {
            fd: self.raw.raw(),
            datasync: true,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub async fn fallocate(&self, offset: u64, len: u64) -> VeloqResult<()> {
        let op: FileFallocateRaw = FileFallocateRaw {
            fd: self.raw.raw(),
            mode: 0,
            offset,
            len,
        };

        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ()).map_err(from_driver_report)
    }

    pub fn sync_range(&self, offset: u64, nbytes: u64) -> SyncRangeBuilder<'_> {
        SyncRangeBuilder::new(self, offset, nbytes)
    }
}

pub struct SyncRangeBuilder<'a> {
    file: &'a File,
    offset: u64,
    nbytes: u64,
    flags: u32,
}

impl<'a> SyncRangeBuilder<'a> {
    fn new(file: &'a File, offset: u64, nbytes: u64) -> Self {
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

impl<'a> IntoFuture for SyncRangeBuilder<'a> {
    type Output = io::Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let submitter = self.file.submitter;
        let file = self.file;
        let offset = self.offset;
        let nbytes = self.nbytes;
        let flags = self.flags;
        Box::pin(async move {
            let op: FileSyncFileRangeRaw = FileSyncFileRangeRaw {
                fd: file.raw.raw(),
                offset,
                nbytes,
                flags,
            };

            let (res, _) = submit(&submitter, Op::new(op)).await.into_inner();
            res.map(|_| ())
                .map_err(from_driver_report)
                .map_err(to_io_error)
        })
    }
}

impl crate::io::AsyncBufRead for File {
    async fn read(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.load(Ordering::Relaxed);
        let (n, buf) = self.read_at(buf, offset).await.map_err(to_io_error)?;
        self.pos.fetch_add(n as u64, Ordering::Relaxed);
        Ok((n, buf))
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.load(Ordering::Relaxed);
            let (n, b) = self
                .read_at_subset(buf, offset, total)
                .await
                .map_err(to_io_error)?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            total += n;
            self.pos.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok((total, buf))
    }
}

impl crate::io::AsyncBufWrite for File {
    async fn write(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let offset = self.pos.load(Ordering::Relaxed);
        let (n, buf) = self.write_at(buf, offset).await.map_err(to_io_error)?;
        self.pos.fetch_add(n as u64, Ordering::Relaxed);
        Ok((n, buf))
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let offset = self.pos.load(Ordering::Relaxed);
            let (n, b) = self
                .write_at_subset(buf, offset, total)
                .await
                .map_err(to_io_error)?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            total += n;
            self.pos.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok((total, buf))
    }

    async fn flush(&self) -> io::Result<()> {
        self.sync_data().await.map_err(to_io_error)
    }

    async fn shutdown(&self) -> io::Result<()> {
        self.sync_all().await.map_err(to_io_error)
    }
}

impl LocalFile {
    pub async fn open(path: impl AsRef<Path>) -> VeloqResult<LocalFile> {
        OpenOptions::new().read(true).open_local(path).await
    }

    pub async fn create(path: impl AsRef<Path>) -> VeloqResult<LocalFile> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_local(path)
            .await
    }
}

impl File {
    pub async fn open(path: impl AsRef<Path>) -> VeloqResult<File> {
        OpenOptions::new().read(true).open(path).await
    }

    pub async fn create(path: impl AsRef<Path>) -> VeloqResult<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }
}
