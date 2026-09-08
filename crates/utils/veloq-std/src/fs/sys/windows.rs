use core::{cmp, mem, ptr};

use windows_sys::Win32::{
    Foundation::{
        ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED, FILETIME,
        GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
    },
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CREATE_ALWAYS, CREATE_NEW, CreateFileW, DeleteFileW,
        FILE_APPEND_DATA, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_BEGIN, FILE_CURRENT, FILE_END, FlushFileBuffers,
        GetFileInformationByHandle, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
        OPEN_ALWAYS, OPEN_EXISTING, ReadFile, SetEndOfFile, SetFilePointerEx, SetFileTime,
        TRUNCATE_EXISTING, UnlockFile, WriteFile,
    },
    System::IO::OVERLAPPED,
};

use crate::{
    alloc_crate::vec::Vec,
    fs::{FileTimes, FileType, OpenOptions, TryLockError},
    io::{Error, ErrorKind, IoSlice, IoSliceMut, Result, SeekFrom},
    os::{
        cvt::cvt,
        windows::io::{
            AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, IntoRawHandle, OwnedHandle,
            RawHandle,
        },
    },
    path::Path,
};

#[derive(Debug)]
pub struct File(pub(crate) OwnedHandle);

impl File {
    pub fn open_options(path: &Path, opts: &OpenOptions) -> Result<Self> {
        let mut wide: Vec<u16> = path.as_str().encode_utf16().collect();
        wide.push(0);

        let access_mode = if let Some(mode) = opts.access_mode {
            mode
        } else {
            match (opts.read, opts.write, opts.append) {
                (true, false, false) => GENERIC_READ,
                (false, true, false) => GENERIC_WRITE,
                (true, true, false) => GENERIC_READ | GENERIC_WRITE,
                (false, _, true) => FILE_APPEND_DATA,
                (true, _, true) => GENERIC_READ | FILE_APPEND_DATA,
                (false, false, false) => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "must specify at least one of read, write, or append access",
                    ));
                }
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

        let disposition = match (opts.create, opts.truncate, opts.create_new) {
            (false, false, false) => OPEN_EXISTING,
            (true, false, false) => OPEN_ALWAYS,
            (false, true, false) => TRUNCATE_EXISTING,
            (true, true, false) => CREATE_ALWAYS,
            (_, _, true) => CREATE_NEW,
        };

        let flags_and_attrs =
            if opts.custom_flags != 0 || opts.attributes != 0 || opts.security_qos_flags != 0 {
                opts.custom_flags | opts.attributes | opts.security_qos_flags
            } else {
                FILE_ATTRIBUTE_NORMAL
            };

        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                access_mode,
                opts.share_mode,
                ptr::null(),
                disposition,
                flags_and_attrs,
                0 as HANDLE,
            )
        };

        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            return Err(Error::last_os_error());
        }

        Ok(File(unsafe {
            OwnedHandle::from_raw_handle(handle as RawHandle)
        }))
    }

    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let mut bytes_read = 0u32;
        let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
        let ok = unsafe {
            ReadFile(
                self.0.as_raw_handle() as HANDLE,
                buf.as_mut_ptr(),
                len,
                &mut bytes_read,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32)
                || err.raw_os_error() == Some(ERROR_HANDLE_EOF as i32)
            {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(bytes_read as usize)
        }
    }

    pub fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        for buf in bufs {
            if !buf.is_empty() {
                return self.read(buf);
            }
        }
        Ok(0)
    }

    #[inline]
    pub fn is_read_vectored(&self) -> bool {
        false
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut bytes_written = 0u32;
        let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
        let ok = unsafe {
            WriteFile(
                self.0.as_raw_handle() as HANDLE,
                buf.as_ptr(),
                len,
                &mut bytes_written,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(bytes_written as usize)
        }
    }

    pub fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        for buf in bufs {
            if !buf.is_empty() {
                return self.write(buf);
            }
        }
        Ok(0)
    }

    #[inline]
    pub fn is_write_vectored(&self) -> bool {
        false
    }

    #[inline]
    pub fn flush(&self) -> Result<()> {
        Ok(())
    }

    pub fn seek(&self, pos: SeekFrom) -> Result<u64> {
        let (whence, offset) = match pos {
            SeekFrom::Start(off) => (FILE_BEGIN, off as i64),
            SeekFrom::End(off) => (FILE_END, off),
            SeekFrom::Current(off) => (FILE_CURRENT, off),
        };
        let mut new_pos = 0i64;
        let ok = unsafe {
            SetFilePointerEx(
                self.0.as_raw_handle() as HANDLE,
                offset,
                &mut new_pos,
                whence,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(new_pos as u64)
        }
    }

    pub fn sync_all(&self) -> Result<()> {
        let ok = unsafe { FlushFileBuffers(self.0.as_raw_handle() as HANDLE) };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn sync_data(&self) -> Result<()> {
        self.sync_all()
    }

    pub fn set_len(&self, size: u64) -> Result<()> {
        let mut current_pos = 0i64;
        let ok = unsafe {
            SetFilePointerEx(
                self.0.as_raw_handle() as HANDLE,
                0,
                &mut current_pos,
                FILE_CURRENT,
            )
        };
        if ok == 0 {
            return Err(Error::last_os_error());
        }

        let ok = unsafe {
            SetFilePointerEx(
                self.0.as_raw_handle() as HANDLE,
                size as i64,
                ptr::null_mut(),
                FILE_BEGIN,
            )
        };
        if ok == 0 {
            return Err(Error::last_os_error());
        }

        let ok = unsafe { SetEndOfFile(self.0.as_raw_handle() as HANDLE) };
        let end_err = if ok == 0 {
            Some(Error::last_os_error())
        } else {
            None
        };

        unsafe {
            SetFilePointerEx(
                self.0.as_raw_handle() as HANDLE,
                current_pos,
                ptr::null_mut(),
                FILE_BEGIN,
            );
        }

        if let Some(err) = end_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    pub fn try_clone(&self) -> Result<File> {
        self.0.try_clone().map(File)
    }

    pub fn lock(&self) -> Result<()> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        let ok = unsafe {
            LockFileEx(
                self.0.as_raw_handle() as HANDLE,
                LOCKFILE_EXCLUSIVE_LOCK,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn lock_shared(&self) -> Result<()> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        let ok = unsafe {
            LockFileEx(
                self.0.as_raw_handle() as HANDLE,
                0,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn try_lock(&self) -> core::result::Result<(), TryLockError> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        let ok = unsafe {
            LockFileEx(
                self.0.as_raw_handle() as HANDLE,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if ok == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
                Err(TryLockError::WouldBlock)
            } else {
                Err(TryLockError::Error(err))
            }
        } else {
            Ok(())
        }
    }

    pub fn try_lock_shared(&self) -> core::result::Result<(), TryLockError> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        let ok = unsafe {
            LockFileEx(
                self.0.as_raw_handle() as HANDLE,
                LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if ok == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
                Err(TryLockError::WouldBlock)
            } else {
                Err(TryLockError::Error(err))
            }
        } else {
            Ok(())
        }
    }

    pub fn unlock(&self) -> Result<()> {
        let ok1 = unsafe { UnlockFile(self.0.as_raw_handle() as HANDLE, 0, 0, u32::MAX, u32::MAX) };
        let ok2 = unsafe { UnlockFile(self.0.as_raw_handle() as HANDLE, 0, 0, u32::MAX, u32::MAX) };
        if ok1 == 0 && ok2 == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_NOT_LOCKED as i32) {
                Ok(())
            } else {
                Err(err)
            }
        } else {
            Ok(())
        }
    }

    pub fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let mut bytes_read = 0u32;
        let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
        let ok = unsafe {
            ReadFile(
                self.0.as_raw_handle() as HANDLE,
                buf.as_mut_ptr(),
                len,
                &mut bytes_read,
                &mut overlapped,
            )
        };
        if ok == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(bytes_read as usize)
        }
    }

    pub fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let mut bytes_written = 0u32;
        let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
        let ok = unsafe {
            WriteFile(
                self.0.as_raw_handle() as HANDLE,
                buf.as_ptr(),
                len,
                &mut bytes_written,
                &mut overlapped,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(bytes_written as usize)
        }
    }

    pub fn file_attr(&self) -> Result<FileAttr> {
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(self.0.as_raw_handle() as HANDLE, &mut info) };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(FileAttr {
                attributes: info.dwFileAttributes,
                creation_time: ((info.ftCreationTime.dwHighDateTime as u64) << 32)
                    | (info.ftCreationTime.dwLowDateTime as u64),
                last_access_time: ((info.ftLastAccessTime.dwHighDateTime as u64) << 32)
                    | (info.ftLastAccessTime.dwLowDateTime as u64),
                last_write_time: ((info.ftLastWriteTime.dwHighDateTime as u64) << 32)
                    | (info.ftLastWriteTime.dwLowDateTime as u64),
                file_size: ((info.nFileSizeHigh as u64) << 32) | (info.nFileSizeLow as u64),
            })
        }
    }

    pub fn set_permissions(&self, perm: FilePermissions) -> Result<()> {
        let _ = perm;
        Ok(())
    }

    pub fn set_times(&self, times: FileTimes) -> Result<()> {
        let to_filetime = |d: core::time::Duration| -> FILETIME {
            let intervals =
                (d.as_secs() + 11644473600) * 10_000_000 + (d.subsec_nanos() as u64 / 100);
            FILETIME {
                dwLowDateTime: intervals as u32,
                dwHighDateTime: (intervals >> 32) as u32,
            }
        };

        let atime = times.accessed.map(to_filetime);
        let mtime = times.modified.map(to_filetime);

        let p_atime = atime
            .as_ref()
            .map(|t| t as *const FILETIME)
            .unwrap_or(ptr::null());
        let p_mtime = mtime
            .as_ref()
            .map(|t| t as *const FILETIME)
            .unwrap_or(ptr::null());

        let ok = unsafe {
            SetFileTime(
                self.0.as_raw_handle() as HANDLE,
                ptr::null(),
                p_atime,
                p_mtime,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn as_raw_handle(&self) -> RawHandle {
        self.0.as_raw_handle()
    }

    #[inline]
    pub fn into_raw_handle(self) -> RawHandle {
        self.0.into_raw_handle()
    }

    #[inline]
    pub unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self(unsafe { OwnedHandle::from_raw_handle(handle) })
    }

    #[inline]
    pub fn as_handle(&self) -> BorrowedHandle<'_> {
        self.0.as_handle()
    }

    #[inline]
    pub fn from_inner(handle: OwnedHandle) -> Self {
        Self(handle)
    }

    #[inline]
    pub fn into_inner(self) -> OwnedHandle {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct FileAttr {
    pub(crate) attributes: u32,
    pub(crate) creation_time: u64,
    pub(crate) last_access_time: u64,
    pub(crate) last_write_time: u64,
    pub(crate) file_size: u64,
}

impl FileAttr {
    #[inline]
    pub fn size(&self) -> u64 {
        self.file_size
    }

    #[inline]
    pub fn perm(&self) -> FilePermissions {
        FilePermissions {
            readonly: (self.attributes & FILE_ATTRIBUTE_READONLY) != 0,
        }
    }

    #[inline]
    pub fn file_type(&self) -> FileType {
        FileType {
            is_dir: (self.attributes & FILE_ATTRIBUTE_DIRECTORY) != 0,
            is_file: (self.attributes & FILE_ATTRIBUTE_DIRECTORY) == 0
                && (self.attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0,
            is_symlink: (self.attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FilePermissions {
    pub(crate) readonly: bool,
}

impl FilePermissions {
    #[inline]
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    #[inline]
    pub fn set_readonly(&mut self, readonly: bool) {
        self.readonly = readonly;
    }
}

pub fn remove_file(path: &Path) -> Result<()> {
    if path.as_str().contains('\0') {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "path contains null byte",
        ));
    }
    let mut wide: Vec<u16> = path.as_str().encode_utf16().collect();
    wide.push(0);
    cvt(unsafe { DeleteFileW(wide.as_ptr()) })?;
    Ok(())
}
