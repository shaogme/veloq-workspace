use core::cmp;

use crate::{
    alloc_crate::ffi::CString,
    fs::{FileTimes, FileType, OpenOptions, TryLockError},
    io::{Error, ErrorKind, IoSlice, IoSliceMut, Result, SeekFrom},
    os::unix::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
    path::Path,
};

#[inline]
fn cvt(res: libc::c_int) -> Result<libc::c_int> {
    if res < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(res)
    }
}

#[inline]
fn cvt_ssize(res: libc::ssize_t) -> Result<libc::ssize_t> {
    if res < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(res)
    }
}

#[inline]
fn cvt_off(res: libc::off_t) -> Result<libc::off_t> {
    if res < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(res)
    }
}

fn cvt_r<T, F: FnMut() -> Result<T>>(mut f: F) -> Result<T> {
    loop {
        match f() {
            Err(ref e) if e.is_interrupted() => {}
            other => return other,
        }
    }
}

#[derive(Debug)]
pub struct File(pub(crate) OwnedFd);

impl File {
    pub fn open_options(path: &Path, opts: &OpenOptions) -> Result<Self> {
        let c_path = CString::new(path.as_bytes())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains null byte"))?;

        let mut flags = libc::O_CLOEXEC;
        flags |= match (opts.read, opts.write, opts.append) {
            (true, false, false) => libc::O_RDONLY,
            (false, true, false) => libc::O_WRONLY,
            (true, true, false) => libc::O_RDWR,
            (false, _, true) => libc::O_WRONLY | libc::O_APPEND,
            (true, _, true) => libc::O_RDWR | libc::O_APPEND,
            (false, false, false) => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "must specify at least one of read, write, or append access",
                ));
            }
        };

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

        flags |= match (opts.create, opts.truncate, opts.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        };

        flags |= opts.custom_flags & !libc::O_ACCMODE;

        let fd = cvt_r(|| {
            cvt(unsafe { libc::open(c_path.as_ptr(), flags, opts.mode as libc::c_uint) })
        })?;

        Ok(File(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let ret = cvt_ssize(unsafe {
            libc::read(
                self.0.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                cmp::min(buf.len(), libc::ssize_t::MAX as usize),
            )
        })?;
        Ok(ret as usize)
    }

    pub fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [libc::iovec {
            iov_base: core::ptr::null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = libc::iovec {
                iov_base: bufs[i].as_mut_ptr() as *mut libc::c_void,
                iov_len: bufs[i].len(),
            };
        }
        let ret = cvt_ssize(unsafe {
            libc::readv(self.0.as_raw_fd(), iovecs.as_ptr(), len as libc::c_int)
        })?;
        Ok(ret as usize)
    }

    #[inline]
    pub fn is_read_vectored(&self) -> bool {
        true
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let ret = cvt_ssize(unsafe {
            libc::write(
                self.0.as_raw_fd(),
                buf.as_ptr() as *const libc::c_void,
                cmp::min(buf.len(), libc::ssize_t::MAX as usize),
            )
        })?;
        Ok(ret as usize)
    }

    pub fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let len = cmp::min(bufs.len(), 16);
        let mut iovecs = [libc::iovec {
            iov_base: core::ptr::null_mut(),
            iov_len: 0,
        }; 16];
        for i in 0..len {
            iovecs[i] = libc::iovec {
                iov_base: bufs[i].as_ptr() as *mut libc::c_void,
                iov_len: bufs[i].len(),
            };
        }
        let ret = cvt_ssize(unsafe {
            libc::writev(self.0.as_raw_fd(), iovecs.as_ptr(), len as libc::c_int)
        })?;
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
            SeekFrom::Start(off) => (libc::SEEK_SET, off as i64),
            SeekFrom::End(off) => (libc::SEEK_END, off),
            SeekFrom::Current(off) => (libc::SEEK_CUR, off),
        };
        let ret =
            cvt_off(unsafe { libc::lseek(self.0.as_raw_fd(), offset as libc::off_t, whence) })?;
        Ok(ret as u64)
    }

    pub fn sync_all(&self) -> Result<()> {
        cvt_r(|| cvt(unsafe { libc::fsync(self.0.as_raw_fd()) })).map(drop)
    }

    pub fn sync_data(&self) -> Result<()> {
        cvt_r(|| cvt(unsafe { libc::fdatasync(self.0.as_raw_fd()) })).map(drop)
    }

    pub fn set_len(&self, size: u64) -> Result<()> {
        let size: libc::off_t = size
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        cvt_r(|| cvt(unsafe { libc::ftruncate(self.0.as_raw_fd(), size) })).map(drop)
    }

    pub fn try_clone(&self) -> Result<File> {
        self.0.try_clone().map(File)
    }

    pub fn lock(&self) -> Result<()> {
        cvt(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_EX) }).map(drop)
    }

    pub fn lock_shared(&self) -> Result<()> {
        cvt(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_SH) }).map(drop)
    }

    pub fn try_lock(&self) -> core::result::Result<(), TryLockError> {
        let res = cvt(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) });
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Err(TryLockError::WouldBlock),
            Err(e) => Err(TryLockError::Error(e)),
        }
    }

    pub fn try_lock_shared(&self) -> core::result::Result<(), TryLockError> {
        let res = cvt(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) });
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Err(TryLockError::WouldBlock),
            Err(e) => Err(TryLockError::Error(e)),
        }
    }

    pub fn unlock(&self) -> Result<()> {
        cvt(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) }).map(drop)
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset: libc::off_t = offset
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let ret = cvt_ssize(unsafe {
            libc::pread(
                self.0.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                cmp::min(buf.len(), libc::ssize_t::MAX as usize),
                offset,
            )
        })?;
        Ok(ret as usize)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let offset: libc::off_t = offset
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let ret = cvt_ssize(unsafe {
            libc::pwrite(
                self.0.as_raw_fd(),
                buf.as_ptr() as *const libc::c_void,
                cmp::min(buf.len(), libc::ssize_t::MAX as usize),
                offset,
            )
        })?;
        Ok(ret as usize)
    }

    pub fn file_attr(&self) -> Result<FileAttr> {
        let mut stat: libc::stat = unsafe { core::mem::zeroed() };
        cvt(unsafe { libc::fstat(self.0.as_raw_fd(), &mut stat) })?;
        Ok(FileAttr { stat })
    }

    pub fn set_permissions(&self, perm: FilePermissions) -> Result<()> {
        cvt_r(|| cvt(unsafe { libc::fchmod(self.0.as_raw_fd(), perm.mode as libc::mode_t) }))
            .map(drop)
    }

    pub fn set_times(&self, times: FileTimes) -> Result<()> {
        let mut ts = [libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        }; 2];

        if let Some(accessed) = times.accessed {
            ts[0] = libc::timespec {
                tv_sec: accessed.as_secs() as libc::time_t,
                tv_nsec: accessed.subsec_nanos() as libc::c_long,
            };
        }
        if let Some(modified) = times.modified {
            ts[1] = libc::timespec {
                tv_sec: modified.as_secs() as libc::time_t,
                tv_nsec: modified.subsec_nanos() as libc::c_long,
            };
        }

        cvt(unsafe { libc::futimens(self.0.as_raw_fd(), ts.as_ptr()) }).map(drop)
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

#[derive(Clone, Debug)]
pub struct FileAttr {
    pub(crate) stat: libc::stat,
}

impl FileAttr {
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
            is_dir: (mode & libc::S_IFMT) == libc::S_IFDIR,
            is_file: (mode & libc::S_IFMT) == libc::S_IFREG,
            is_symlink: (mode & libc::S_IFMT) == libc::S_IFLNK,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FilePermissions {
    pub(crate) readonly: bool,
    pub(crate) mode: u32,
}

impl FilePermissions {
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
