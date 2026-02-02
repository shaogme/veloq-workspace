use std::num::NonZeroUsize;
use std::path::Path;
use veloq_driver::op::Open;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferingMode {
    /// Use system default buffering (Page Cache).
    Buffered,
    /// Bypass system cache (e.g., O_DIRECT on Unix, FILE_FLAG_NO_BUFFERING on Windows).
    /// Requires buffer alignment (handled by BufPool).
    Direct,
    /// Bypass system cache and force write-through to physical storage
    /// (e.g., O_DIRECT | O_DSYNC on Unix, FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH on Windows).
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

    /// Open the file with local thread binding (optimized, non-Send/Sync).
    pub async fn open_local(
        &self,
        path: impl AsRef<Path>,
    ) -> std::io::Result<super::file::LocalFile> {
        // 1. 根据不同平台生成对应的 Op 参数
        let op = self.build_op(path.as_ref())?;

        // 2. 提交给 runtime (local)
        use crate::runtime::context::submit;
        use veloq_driver::op::{LocalSubmitter, Op};

        let submitter = LocalSubmitter;
        let (res, _) = submit(&submitter, Op::new(op)).await;

        // 3. 转换结果
        let fd = veloq_driver::RawHandle::from(res?);
        use super::file::InnerFile;
        use std::cell::Cell;

        Ok(super::file::LocalFile {
            inner: InnerFile(fd),
            submitter,
            pos: Cell::new(0),
        })
    }

    /// Open the file with shared submission support (Send, capable of being offloaded).
    pub async fn open(&self, path: impl AsRef<Path>) -> std::io::Result<super::file::File> {
        // 构造 Op
        let op = self.build_op(path.as_ref())?;

        // 使用 DetachedSubmitter 提交
        use crate::runtime::context::submit;
        use veloq_driver::op::{DetachedSubmitter, Op};

        // 捕获 SubmitContext (Injector)
        let submitter = DetachedSubmitter::new()?;

        // 提交执行 (Result, Op) — Op 的所有权被返还
        let (res, _) = submit(&submitter, Op::new(op)).await;

        let fd = veloq_driver::RawHandle::from(res?);

        use super::file::InnerFile;
        use std::sync::atomic::AtomicU64;

        Ok(super::file::File {
            inner: InnerFile(fd),
            submitter,
            pos: AtomicU64::new(0),
        })
    }

    // ==========================================
    // Unix 平台实现
    // ==========================================
    #[cfg(unix)]
    fn build_op(&self, path: &Path) -> std::io::Result<Open> {
        use std::os::unix::ffi::OsStrExt;

        let path_bytes = path.as_os_str().as_bytes();
        // ensure null termination
        let len = path_bytes.len() + 1;

        let mut buf = crate::runtime::context::try_alloc(len).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, "buf pool exhausted")
        })?;

        // Write path + null
        let slice = buf.as_slice_mut();
        if slice.len() < len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "path too long for buffer",
            ));
        }
        slice[..len - 1].copy_from_slice(path_bytes);
        slice[len - 1] = 0;

        buf.set_len(NonZeroUsize::new(len).unwrap());

        // 标志位计算
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
                // Linux: O_DIRECT for bypass cache, O_DSYNC for data integrity
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

    // ==========================================
    // Windows 平台实现
    // ==========================================
    #[cfg(windows)]
    fn build_op(&self, path: &Path) -> std::io::Result<Open> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Foundation::*;
        use windows_sys::Win32::Storage::FileSystem::FILE_APPEND_DATA;

        // Custom bit-packing constants for passing flags via 'mode' field
        // These must match the decoding logic in blocking.rs
        const FAKE_NO_BUFFERING: u32 = 1 << 8;
        const FAKE_WRITE_THROUGH: u32 = 1 << 9;

        // 1. Process Path (UTF-16 + Null)
        let path_w: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let len_bytes = path_w.len() * 2;

        let mut buf = crate::runtime::context::try_alloc(len_bytes).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, "buf pool exhausted")
        })?;

        let slice = buf.as_slice_mut();
        if slice.len() < len_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "path too long for buffer",
            ));
        }

        // Copy u16s to u8 buffer
        unsafe {
            std::ptr::copy_nonoverlapping(
                path_w.as_ptr() as *const u8,
                slice.as_mut_ptr(),
                len_bytes,
            );
            buf.set_len(NonZeroUsize::new(len_bytes).unwrap());
        }

        // 2. Process Access
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

        // 3. Disposition
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
