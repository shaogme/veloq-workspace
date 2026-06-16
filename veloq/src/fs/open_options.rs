use super::file::{File, LocalFile};
use crate::{error::Result, fs::error::FsError, runtime::context::Ctx};
use diagweave::prelude::*;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::{cell::Cell, num::NonZeroUsize, path::Path, sync::atomic::AtomicU64};

use veloq_driver_native::{
    OwnedRawHandle,
    driver::{Driver, RegisterFd},
    op::{DetachedSubmitter, LocalSubmitter, Op, Open},
};
#[cfg(windows)]
use windows_sys::Win32::{Foundation::*, Storage::FileSystem::FILE_APPEND_DATA};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferingMode {
    /// Use system default buffering (Page Cache).
    Buffered,
    /// Bypass system cache (e.g., O_DIRECT on Unix, FILE_FLAG_NO_BUFFERING on Windows).
    Direct,
    /// Bypass system cache and force write-through to physical storage.
    DirectSync,
}

#[derive(Clone, Debug)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    mode: u32,
    custom_flags: i32,
    buffering_mode: BufferingMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            mode: 0o666,
            custom_flags: 0,
            buffering_mode: BufferingMode::Buffered,
        }
    }

    pub fn buffering(&mut self, mode: BufferingMode) -> &mut Self {
        self.buffering_mode = mode;
        self
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;
        self
    }

    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;
        self
    }

    pub async fn open_local<'a, 'ctx>(
        &self,
        ctx: Ctx<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<LocalFile<'a, 'ctx>> {
        let op = self.build_op(&ctx, path.as_ref())?;

        let submitter = LocalSubmitter::new();
        let (res, _) = ctx.submit(&submitter, Op::new(op)).await.into_inner();
        let owned = res.trans()?;
        let fd = owned.into_raw();
        let fixed = ctx.driver(|mut driver| {
            let res = driver
                .register_files(vec![RegisterFd::Borrowed(fd.borrow())])
                .trans()?
                .into_iter()
                .next();
            match res {
                Some(fixed) => Ok(fixed),
                None => FsError::RegisterFailed.trans(),
            }
        })?;

        Ok(LocalFile {
            raw: fd,
            fd: fixed,
            submitter,
            pos: Cell::new(0),
            ctx,
        })
    }

    pub async fn open<'a, 'ctx>(
        &self,
        ctx: Ctx<'a, 'ctx>,
        path: impl AsRef<Path>,
    ) -> Result<File<'a, 'ctx>> {
        let op = self.build_op(&ctx, path.as_ref())?;

        let submitter = DetachedSubmitter::new();
        let owner = ctx.scope.worker_id();
        let (res, _) = ctx.submit_to(owner, Op::new(op)).await?;
        let owned = res.trans()?;
        let raw = owned.into_raw();
        // SAFETY: ownership is transferred into the owner driver's registered file table.
        let owned = unsafe { OwnedRawHandle::from_raw_owned(raw) };
        let fd = ctx.driver(|mut driver| {
            driver
                .register_files(vec![RegisterFd::Owned(owned)])
                .trans()?
                .into_iter()
                .next()
                .ok_or(FsError::RegisterFailed)
                .trans()
        })?;

        Ok(File {
            raw,
            fd,
            owner_worker_id: owner,
            submitter,
            pos: AtomicU64::new(0),
            ctx,
        })
    }

    #[cfg(unix)]
    fn build_op(&self, ctx: &Ctx<'_, '_>, path: &Path) -> Result<Open> {
        let path_bytes = path.as_os_str().as_bytes();
        let len = path_bytes.len() + 1;
        let len_nz = NonZeroUsize::new(len).unwrap();

        let mut buf = ctx.try_alloc(len_nz).trans()?;
        let slice = buf.as_slice_mut();
        if slice.len() < len {
            return FsError::PathTooLong.trans();
        }
        slice[..len - 1].copy_from_slice(path_bytes);
        slice[len - 1] = 0;
        buf.set_len(len);

        let mut flags = if self.read && !self.write && !self.append {
            libc::O_RDONLY
        } else if !self.read && self.write && !self.append {
            libc::O_WRONLY
        } else if self.append {
            libc::O_WRONLY | libc::O_APPEND
        } else {
            libc::O_RDWR
        };

        if self.create {
            flags |= libc::O_CREAT;
        }
        if self.create_new {
            flags |= libc::O_EXCL | libc::O_CREAT;
        }
        if self.truncate {
            flags |= libc::O_TRUNC;
        }

        match self.buffering_mode {
            BufferingMode::Buffered => {}
            BufferingMode::Direct => {
                flags |= libc::O_DIRECT;
            }
            BufferingMode::DirectSync => {
                flags |= libc::O_DIRECT | libc::O_DSYNC;
            }
        }

        flags |= self.custom_flags;

        Ok(Open {
            path: buf,
            flags,
            mode: self.mode,
        })
    }

    #[cfg(windows)]
    fn build_op(&self, ctx: &Ctx<'_, '_>, path: &Path) -> Result<Open> {
        const FAKE_NO_BUFFERING: u32 = 1 << 8;
        const FAKE_WRITE_THROUGH: u32 = 1 << 9;

        let os_str = path.as_os_str();
        let mut path_w = Vec::with_capacity(os_str.len() + 1);
        path_w.extend(os_str.encode_wide());
        path_w.push(0);
        let len_bytes = NonZeroUsize::new(path_w.len() * 2).unwrap();

        let mut buf = ctx.try_alloc(len_bytes).trans()?;
        let slice = buf.as_slice_mut();
        if slice.len() < len_bytes.get() {
            return FsError::PathTooLong.trans();
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                path_w.as_ptr() as *const u8,
                slice.as_mut_ptr(),
                len_bytes.get(),
            );
            buf.set_len(len_bytes.get());
        }

        let mut access = 0;
        if self.read {
            access |= GENERIC_READ;
        }
        if self.write {
            access |= GENERIC_WRITE;
        }
        if self.append {
            access |= FILE_APPEND_DATA;
        }

        const OPEN_EXISTING: u32 = 3;
        const CREATE_NEW: u32 = 1;
        const CREATE_ALWAYS: u32 = 2;
        const OPEN_ALWAYS: u32 = 4;
        const TRUNCATE_EXISTING: u32 = 5;

        let mut disposition = match (self.create, self.create_new, self.truncate) {
            (_, true, _) => CREATE_NEW,
            (true, _, true) => CREATE_ALWAYS,
            (true, _, false) => OPEN_ALWAYS,
            (false, _, true) => TRUNCATE_EXISTING,
            (false, _, false) => OPEN_EXISTING,
        };

        match self.buffering_mode {
            BufferingMode::Buffered => {}
            BufferingMode::Direct => {
                disposition |= FAKE_NO_BUFFERING;
            }
            BufferingMode::DirectSync => {
                disposition |= FAKE_NO_BUFFERING | FAKE_WRITE_THROUGH;
            }
        }

        Ok(Open {
            path: buf,
            flags: access as i32,
            mode: disposition,
        })
    }
}
