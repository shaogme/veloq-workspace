use core::{cmp, mem::zeroed, ptr::null_mut, result::Result as CoreResult};

use crate::{
    alloc_crate::ffi::CString,
    fs::{FileTimes, FileType, OpenOptions, TryLockError},
    io::{Error, ErrorKind, IoSlice, IoSliceMut, Result, SeekFrom},
    os::{
        Cvt,
        unix::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
    },
    path::Path,
};
use libc::{
    LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN, O_ACCMODE, O_APPEND, O_CLOEXEC, O_CREAT, O_EXCL, O_RDONLY,
    O_RDWR, O_TRUNC, O_WRONLY, S_IFDIR, S_IFLNK, S_IFMT, S_IFREG, SEEK_CUR, SEEK_END, SEEK_SET,
    UTIME_OMIT, c_int, c_long, c_uint, c_void, fchmod, fdatasync, flock, fstat, fsync, ftruncate,
    futimens, iovec, lseek, mode_t, off_t, open, pread, pwrite, read, readv, ssize_t, stat,
    stat as stat_path, timespec, unlink, write, writev,
};

#[derive(Clone, Debug)]
pub struct FileAttr {
    stat: stat,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FilePermissions {
    readonly: bool,
    mode: u32,
}

#[derive(Debug)]
pub struct File(OwnedFd);

impl File {
    pub fn open_options(path: &Path, opts: &OpenOptions) -> Result<Self> {
        let c_path = CString::new(path.as_bytes())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains null byte"))?;

        let mut flags = O_CLOEXEC;
        if opts.read {
            flags |= if opts.write || opts.append {
                O_RDWR
            } else {
                O_RDONLY
            };
        } else if opts.write || opts.append {
            flags |= O_WRONLY;
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "must specify at least one of read, write, or append access",
            ));
        }
        if opts.append {
            flags |= O_APPEND;
        }

        match (opts.write, opts.append) {
            (true, false) => {}
            (false, false) => {
                if opts.truncate || opts.create || opts.create_new {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "creating or truncating a file requires write or append access",
                    ));
                }
            }
            (_, true) => {
                if opts.truncate && !opts.create_new {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "creating or truncating a file requires write or append access",
                    ));
                }
            }
        }

        if opts.create_new {
            flags |= O_CREAT | O_EXCL;
        } else {
            if opts.create {
                flags |= O_CREAT;
            }
            if opts.truncate {
                flags |= O_TRUNC;
            }
        }

        flags |= opts.custom_flags & !O_ACCMODE;

        let fd =
            Cvt::cvt_r(|| Cvt::cvt(unsafe { open(c_path.as_ptr(), flags, opts.mode as c_uint) }))?;

        Ok(File(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let ret = Cvt::cvt(unsafe {
            read(
                self.0.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                cmp::min(buf.len(), ssize_t::MAX as usize),
            )
        })?;
        Ok(ret as usize)
    }

    pub fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [iovec {
            iov_base: null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = iovec {
                iov_base: bufs[i].as_mut_ptr() as *mut c_void,
                iov_len: bufs[i].len(),
            };
        }
        let ret = Cvt::cvt(unsafe { readv(self.0.as_raw_fd(), iovecs.as_ptr(), len as c_int) })?;
        Ok(ret as usize)
    }

    #[inline]
    pub fn is_read_vectored(&self) -> bool {
        true
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let ret = Cvt::cvt(unsafe {
            write(
                self.0.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                cmp::min(buf.len(), ssize_t::MAX as usize),
            )
        })?;
        Ok(ret as usize)
    }

    pub fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [iovec {
            iov_base: null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = iovec {
                iov_base: bufs[i].as_ptr() as *mut c_void,
                iov_len: bufs[i].len(),
            };
        }
        let ret = Cvt::cvt(unsafe { writev(self.0.as_raw_fd(), iovecs.as_ptr(), len as c_int) })?;
        Ok(ret as usize)
    }

    #[inline]
    pub fn is_write_vectored(&self) -> bool {
        true
    }

    #[inline]
    pub fn flush(&self) -> Result<()> {
        Ok(())
    }

    pub fn seek(&self, pos: SeekFrom) -> Result<u64> {
        let (whence, offset) = match pos {
            SeekFrom::Start(off) => (SEEK_SET, off as i64),
            SeekFrom::End(off) => (SEEK_END, off),
            SeekFrom::Current(off) => (SEEK_CUR, off),
        };
        let ret = Cvt::cvt(unsafe { lseek(self.0.as_raw_fd(), offset as off_t, whence) })?;
        Ok(ret as u64)
    }

    pub fn sync_all(&self) -> Result<()> {
        Cvt::cvt_r(|| Cvt::cvt(unsafe { fsync(self.0.as_raw_fd()) })).map(drop)
    }

    pub fn sync_data(&self) -> Result<()> {
        Cvt::cvt_r(|| Cvt::cvt(unsafe { fdatasync(self.0.as_raw_fd()) })).map(drop)
    }

    pub fn set_len(&self, size: u64) -> Result<()> {
        let size: off_t = size
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        Cvt::cvt_r(|| Cvt::cvt(unsafe { ftruncate(self.0.as_raw_fd(), size) })).map(drop)
    }

    pub fn try_clone(&self) -> Result<File> {
        self.0.try_clone().map(File)
    }

    pub fn lock(&self) -> Result<()> {
        Cvt::cvt(unsafe { flock(self.0.as_raw_fd(), LOCK_EX) }).map(drop)
    }

    pub fn lock_shared(&self) -> Result<()> {
        Cvt::cvt(unsafe { flock(self.0.as_raw_fd(), LOCK_SH) }).map(drop)
    }

    pub fn try_lock(&self) -> CoreResult<(), TryLockError> {
        let res = Cvt::cvt(unsafe { flock(self.0.as_raw_fd(), LOCK_EX | LOCK_NB) });
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Err(TryLockError::WouldBlock),
            Err(e) => Err(TryLockError::Error(e)),
        }
    }

    pub fn try_lock_shared(&self) -> CoreResult<(), TryLockError> {
        let res = Cvt::cvt(unsafe { flock(self.0.as_raw_fd(), LOCK_SH | LOCK_NB) });
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Err(TryLockError::WouldBlock),
            Err(e) => Err(TryLockError::Error(e)),
        }
    }

    pub fn unlock(&self) -> Result<()> {
        Cvt::cvt(unsafe { flock(self.0.as_raw_fd(), LOCK_UN) }).map(drop)
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset: off_t = offset
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let ret = Cvt::cvt(unsafe {
            pread(
                self.0.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                cmp::min(buf.len(), ssize_t::MAX as usize),
                offset,
            )
        })?;
        Ok(ret as usize)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let offset: off_t = offset
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let ret = Cvt::cvt(unsafe {
            pwrite(
                self.0.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                cmp::min(buf.len(), ssize_t::MAX as usize),
                offset,
            )
        })?;
        Ok(ret as usize)
    }

    pub fn file_attr(&self) -> Result<FileAttr> {
        let mut stat: stat = unsafe { zeroed() };
        Cvt::cvt(unsafe { fstat(self.0.as_raw_fd(), &mut stat) })?;
        Ok(FileAttr { stat })
    }

    pub fn set_permissions(&self, perm: FilePermissions) -> Result<()> {
        Cvt::cvt_r(|| Cvt::cvt(unsafe { fchmod(self.0.as_raw_fd(), perm.mode as mode_t) }))
            .map(drop)
    }

    pub fn set_times(&self, times: FileTimes) -> Result<()> {
        let mut ts = [timespec {
            tv_sec: 0,
            tv_nsec: UTIME_OMIT,
        }; 2];

        if let Some(accessed) = times.accessed {
            ts[0] = timespec {
                tv_sec: accessed.as_secs() as _,
                tv_nsec: accessed.subsec_nanos() as c_long,
            };
        }
        if let Some(modified) = times.modified {
            ts[1] = timespec {
                tv_sec: modified.as_secs() as _,
                tv_nsec: modified.subsec_nanos() as c_long,
            };
        }

        Cvt::cvt(unsafe { futimens(self.0.as_raw_fd(), ts.as_ptr()) }).map(drop)
    }

    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

    #[inline]
    pub fn into_raw_fd(self) -> RawFd {
        self.0.into_raw_fd()
    }

    #[inline]
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    #[inline]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }

    #[inline]
    pub fn from_inner(fd: OwnedFd) -> Self {
        Self(fd)
    }

    #[inline]
    pub fn into_inner(self) -> OwnedFd {
        self.0
    }
}

pub(crate) struct Platform;

impl Platform {
    pub(crate) fn path_exists(path: &Path) -> bool {
        let c_path = match CString::new(path.as_bytes()) {
            Ok(path) => path,
            Err(_) => return false,
        };
        let mut stat: stat = unsafe { zeroed() };
        unsafe { stat_path(c_path.as_ptr(), &mut stat) == 0 }
    }
}

impl FileAttr {
    #[inline]
    pub fn dev(&self) -> u64 {
        self.stat.st_dev
    }

    #[inline]
    pub fn ino(&self) -> u64 {
        self.stat.st_ino as _
    }

    #[inline]
    pub fn mode(&self) -> u32 {
        self.stat.st_mode
    }

    #[inline]
    pub fn nlink(&self) -> u64 {
        self.stat.st_nlink as _
    }

    #[inline]
    pub fn uid(&self) -> u32 {
        self.stat.st_uid
    }

    #[inline]
    pub fn gid(&self) -> u32 {
        self.stat.st_gid
    }

    #[inline]
    pub fn rdev(&self) -> u64 {
        self.stat.st_rdev
    }

    #[inline]
    pub fn atime(&self) -> i64 {
        self.stat.st_atime as _
    }

    #[inline]
    pub fn atime_nsec(&self) -> i64 {
        self.stat.st_atime_nsec as _
    }

    #[inline]
    pub fn mtime(&self) -> i64 {
        self.stat.st_mtime as _
    }

    #[inline]
    pub fn mtime_nsec(&self) -> i64 {
        self.stat.st_mtime_nsec as _
    }

    #[inline]
    pub fn ctime(&self) -> i64 {
        self.stat.st_ctime as _
    }

    #[inline]
    pub fn ctime_nsec(&self) -> i64 {
        self.stat.st_ctime_nsec as _
    }

    #[inline]
    pub fn blksize(&self) -> u64 {
        self.stat.st_blksize as u64
    }

    #[inline]
    pub fn blocks(&self) -> u64 {
        self.stat.st_blocks as u64
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.stat.st_size as u64
    }

    #[inline]
    pub fn perm(&self) -> FilePermissions {
        FilePermissions {
            readonly: (self.stat.st_mode & 0o222) == 0,
            mode: self.stat.st_mode,
        }
    }

    #[inline]
    pub fn file_type(&self) -> FileType {
        let mode = self.stat.st_mode;
        FileType {
            is_dir: (mode & S_IFMT) == S_IFDIR,
            is_file: (mode & S_IFMT) == S_IFREG,
            is_symlink: (mode & S_IFMT) == S_IFLNK,
        }
    }
}

impl FilePermissions {
    #[inline]
    pub fn mode(&self) -> u32 {
        self.mode
    }

    #[inline]
    pub fn set_mode(&mut self, mode: u32) {
        self.mode = mode;
        self.readonly = (mode & 0o222) == 0;
    }

    #[inline]
    pub fn from_mode(mode: u32) -> Self {
        Self {
            readonly: (mode & 0o222) == 0,
            mode,
        }
    }

    #[inline]
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    #[inline]
    pub fn set_readonly(&mut self, readonly: bool) {
        self.readonly = readonly;
        if readonly {
            self.mode &= !0o222;
        } else {
            self.mode |= 0o222;
        }
    }
}

impl Platform {
    pub(crate) fn remove_file(path: &Path) -> Result<()> {
        let c_path = CString::new(path.as_bytes())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains null byte"))?;
        Cvt::cvt(unsafe { unlink(c_path.as_ptr()) })?;
        Ok(())
    }
}
