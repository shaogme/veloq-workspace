//! File system manipulation without depending on the standard library.

mod sys;

use core::{fmt, time::Duration};

use crate::{
    alloc_crate::{string::String, vec::Vec},
    io::{Error, IoSlice, IoSliceMut, Read, Result, Seek, SeekFrom, Write, copy as io_copy},
    path::Path,
};

#[cfg(unix)]
use crate::os::unix::{
    fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
    fs::{
        FileExt as UnixFileExt, MetadataExt as UnixMetadataExt,
        OpenOptionsExt as UnixOpenOptionsExt, PermissionsExt as UnixPermissionsExt,
    },
};

#[cfg(windows)]
use crate::os::windows::{
    fs::{
        FileExt as WinFileExt, MetadataExt as WinMetadataExt, OpenOptionsExt as WinOpenOptionsExt,
    },
    io::{
        AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle,
    },
};

pub use sys::{FileAttr, FilePermissions};

/// An open file on the filesystem.
pub struct File {
    inner: sys::File,
}

impl File {
    /// Attempts to open a file in read-only mode.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new().read(true).open(path)
    }

    /// Opens a file in write-only mode, creating it if it doesn't exist and truncating it if it does.
    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    /// Creates a new file in read-write mode; returns an error if the file exists.
    #[inline]
    pub fn create_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
    }

    /// Returns a new [`OpenOptions`] builder.
    #[inline]
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub(crate) fn open_options(path: &Path, opts: &OpenOptions) -> Result<Self> {
        sys::File::open_options(path, opts).map(|inner| Self { inner })
    }

    /// Attempts to sync all OS-internal file content and metadata to disk.
    #[inline]
    pub fn sync_all(&self) -> Result<()> {
        self.inner.sync_all()
    }

    /// Synchronizes file content to disk, avoiding metadata updates where possible.
    #[inline]
    pub fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }

    /// Truncates or extends the underlying file to `size` bytes.
    #[inline]
    pub fn set_len(&self, size: u64) -> Result<()> {
        self.inner.set_len(size)
    }

    /// Queries metadata about the underlying file.
    #[inline]
    pub fn metadata(&self) -> Result<Metadata> {
        self.inner.file_attr().map(Metadata)
    }

    /// Creates a new `File` instance that shares the same underlying file handle.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.inner.try_clone().map(|inner| Self { inner })
    }

    /// Acquires an exclusive lock on the file, blocking until available.
    #[inline]
    pub fn lock(&self) -> Result<()> {
        self.inner.lock()
    }

    /// Acquires a shared lock on the file, blocking until available.
    #[inline]
    pub fn lock_shared(&self) -> Result<()> {
        self.inner.lock_shared()
    }

    /// Attempts to acquire an exclusive lock on the file without blocking.
    #[inline]
    pub fn try_lock(&self) -> core::result::Result<(), TryLockError> {
        self.inner.try_lock()
    }

    /// Attempts to acquire a shared lock on the file without blocking.
    #[inline]
    pub fn try_lock_shared(&self) -> core::result::Result<(), TryLockError> {
        self.inner.try_lock_shared()
    }

    /// Releases all locks on the file.
    #[inline]
    pub fn unlock(&self) -> Result<()> {
        self.inner.unlock()
    }

    /// Changes the permissions on the underlying file.
    #[inline]
    pub fn set_permissions(&self, perm: Permissions) -> Result<()> {
        self.inner.set_permissions(perm.0)
    }

    /// Changes the timestamps of the underlying file.
    #[inline]
    pub fn set_times(&self, times: FileTimes) -> Result<()> {
        self.inner.set_times(times)
    }

    #[cfg(unix)]
    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    #[cfg(unix)]
    #[inline]
    pub fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }

    /// Constructs a new instance of `Self` from the given raw file descriptor.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `fd` must be open and suitable for assuming
    /// ownership. The caller must ensure that `fd` is valid and not simultaneously
    /// dropped or used elsewhere.
    #[cfg(unix)]
    #[inline]
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            inner: unsafe { sys::File::from_raw_fd(fd) },
        }
    }

    #[cfg(unix)]
    #[inline]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }

    #[cfg(unix)]
    #[inline]
    pub fn from_inner(fd: OwnedFd) -> Self {
        Self {
            inner: sys::File::from_inner(fd),
        }
    }

    #[cfg(unix)]
    #[inline]
    pub fn into_inner(self) -> OwnedFd {
        self.inner.into_inner()
    }

    #[cfg(windows)]
    #[inline]
    pub fn as_raw_handle(&self) -> RawHandle {
        self.inner.as_raw_handle()
    }

    #[cfg(windows)]
    #[inline]
    pub fn into_raw_handle(self) -> RawHandle {
        self.inner.into_raw_handle()
    }

    /// Constructs a new instance of `Self` from the given raw handle.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `handle` must be open and suitable for assuming
    /// ownership. The caller must ensure that `handle` is valid and not simultaneously
    /// closed or used elsewhere.
    #[cfg(windows)]
    #[inline]
    pub unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self {
            inner: unsafe { sys::File::from_raw_handle(handle) },
        }
    }

    #[cfg(windows)]
    #[inline]
    pub fn as_handle(&self) -> BorrowedHandle<'_> {
        self.inner.as_handle()
    }

    #[cfg(windows)]
    #[inline]
    pub fn from_inner(handle: OwnedHandle) -> Self {
        Self {
            inner: sys::File::from_inner(handle),
        }
    }

    #[cfg(windows)]
    #[inline]
    pub fn into_inner(self) -> OwnedHandle {
        self.inner.into_inner()
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File").finish()
    }
}

impl Read for File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (&*self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        (&*self).read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }
}

impl Read for &File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        self.inner.read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }
}

impl Write for File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (&*self).write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (&*self).write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (&*self).flush()
    }
}

impl Write for &File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.inner.write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

impl Seek for File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (&*self).seek(pos)
    }
}

impl Seek for &File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl FromRawFd for File {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

#[cfg(unix)]
impl IntoRawFd for File {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.inner.into_raw_fd()
    }
}

#[cfg(unix)]
impl AsFd for File {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for File {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from_inner(fd)
    }
}

#[cfg(unix)]
impl From<File> for OwnedFd {
    #[inline]
    fn from(file: File) -> Self {
        file.into_inner()
    }
}

#[cfg(unix)]
impl UnixFileExt for File {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.inner.read_at(buf, offset)
    }

    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.inner.write_at(buf, offset)
    }
}

#[cfg(windows)]
impl AsRawHandle for File {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.inner.as_raw_handle()
    }
}

#[cfg(windows)]
impl FromRawHandle for File {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        unsafe { Self::from_raw_handle(handle) }
    }
}

#[cfg(windows)]
impl IntoRawHandle for File {
    #[inline]
    fn into_raw_handle(self) -> RawHandle {
        self.inner.into_raw_handle()
    }
}

#[cfg(windows)]
impl AsHandle for File {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.inner.as_handle()
    }
}

#[cfg(windows)]
impl From<OwnedHandle> for File {
    #[inline]
    fn from(handle: OwnedHandle) -> Self {
        Self::from_inner(handle)
    }
}

#[cfg(windows)]
impl From<File> for OwnedHandle {
    #[inline]
    fn from(file: File) -> Self {
        file.into_inner()
    }
}

#[cfg(windows)]
impl WinFileExt for File {
    #[inline]
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.inner.seek_read(buf, offset)
    }

    #[inline]
    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.inner.seek_write(buf, offset)
    }
}

/// Options and flags which can be used to configure how a file is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) append: bool,
    pub(crate) truncate: bool,
    pub(crate) create: bool,
    pub(crate) create_new: bool,
    #[cfg(unix)]
    pub(crate) mode: u32,
    #[cfg(unix)]
    pub(crate) custom_flags: i32,
    #[cfg(windows)]
    pub(crate) access_mode: Option<u32>,
    #[cfg(windows)]
    pub(crate) share_mode: u32,
    #[cfg(windows)]
    pub(crate) custom_flags: u32,
    #[cfg(windows)]
    pub(crate) attributes: u32,
    #[cfg(windows)]
    pub(crate) security_qos_flags: u32,
}

impl Default for OpenOptions {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Creates a blank new set of options.
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            #[cfg(unix)]
            mode: 0o666,
            #[cfg(unix)]
            custom_flags: 0,
            #[cfg(windows)]
            access_mode: None,
            #[cfg(windows)]
            share_mode: 1 | 2 | 4, // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
            #[cfg(windows)]
            custom_flags: 0,
            #[cfg(windows)]
            attributes: 0,
            #[cfg(windows)]
            security_qos_flags: 0,
        }
    }

    /// Sets the option for read access.
    #[inline]
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    #[inline]
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Sets the option for append mode.
    #[inline]
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Sets the option for truncating an existing file.
    #[inline]
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Sets the option to create a new file, or open it if it already exists.
    #[inline]
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new file, failing if it already exists.
    #[inline]
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Opens a file at `path` with the options specified by `self`.
    #[inline]
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        File::open_options(path.as_ref(), self)
    }
}

#[cfg(unix)]
impl UnixOpenOptionsExt for OpenOptions {
    #[inline]
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;
        self
    }

    #[inline]
    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;
        self
    }
}

#[cfg(windows)]
impl WinOpenOptionsExt for OpenOptions {
    #[inline]
    fn access_mode(&mut self, access: u32) -> &mut Self {
        self.access_mode = Some(access);
        self
    }

    #[inline]
    fn share_mode(&mut self, val: u32) -> &mut Self {
        self.share_mode = val;
        self
    }

    #[inline]
    fn custom_flags(&mut self, flags: u32) -> &mut Self {
        self.custom_flags = flags;
        self
    }

    #[inline]
    fn attributes(&mut self, val: u32) -> &mut Self {
        self.attributes = val;
        self
    }

    #[inline]
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        self.security_qos_flags = flags;
        self
    }
}

/// Metadata information about a file.
#[derive(Clone, Debug)]
pub struct Metadata(pub(crate) sys::FileAttr);

impl Metadata {
    /// Returns the file type for this metadata.
    #[inline]
    pub fn file_type(&self) -> FileType {
        self.0.file_type()
    }

    /// Returns `true` if this metadata is for a directory.
    #[inline]
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    /// Returns `true` if this metadata is for a regular file.
    #[inline]
    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    /// Returns `true` if this metadata is for a symbolic link.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    /// Returns the size of the file, in bytes, this metadata represents.
    #[inline]
    pub fn len(&self) -> u64 {
        self.0.size()
    }

    /// Returns `true` if this metadata is for a file of length 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the permissions of the file this metadata represents.
    #[inline]
    pub fn permissions(&self) -> Permissions {
        Permissions(self.0.perm())
    }
}

#[cfg(unix)]
impl UnixMetadataExt for Metadata {
    #[inline]
    fn dev(&self) -> u64 {
        self.0.stat.st_dev
    }

    #[inline]
    fn ino(&self) -> u64 {
        self.0.stat.st_ino as _
    }

    #[inline]
    fn mode(&self) -> u32 {
        self.0.stat.st_mode
    }

    #[inline]
    fn nlink(&self) -> u64 {
        self.0.stat.st_nlink as _
    }

    #[inline]
    fn uid(&self) -> u32 {
        self.0.stat.st_uid
    }

    #[inline]
    fn gid(&self) -> u32 {
        self.0.stat.st_gid
    }

    #[inline]
    fn rdev(&self) -> u64 {
        self.0.stat.st_rdev
    }

    #[inline]
    fn size(&self) -> u64 {
        self.0.stat.st_size as u64
    }

    #[inline]
    fn atime(&self) -> i64 {
        self.0.stat.st_atime as _
    }

    #[inline]
    fn atime_nsec(&self) -> i64 {
        self.0.stat.st_atime_nsec as _
    }

    #[inline]
    fn mtime(&self) -> i64 {
        self.0.stat.st_mtime as _
    }

    #[inline]
    fn mtime_nsec(&self) -> i64 {
        self.0.stat.st_mtime_nsec as _
    }

    #[inline]
    fn ctime(&self) -> i64 {
        self.0.stat.st_ctime as _
    }

    #[inline]
    fn ctime_nsec(&self) -> i64 {
        self.0.stat.st_ctime_nsec as _
    }

    #[inline]
    fn blksize(&self) -> u64 {
        self.0.stat.st_blksize as u64
    }

    #[inline]
    fn blocks(&self) -> u64 {
        self.0.stat.st_blocks as u64
    }
}

#[cfg(windows)]
impl WinMetadataExt for Metadata {
    #[inline]
    fn file_attributes(&self) -> u32 {
        self.0.attributes
    }

    #[inline]
    fn creation_time(&self) -> u64 {
        self.0.creation_time
    }

    #[inline]
    fn last_access_time(&self) -> u64 {
        self.0.last_access_time
    }

    #[inline]
    fn last_write_time(&self) -> u64 {
        self.0.last_write_time
    }

    #[inline]
    fn file_size(&self) -> u64 {
        self.0.file_size
    }
}

/// Representation of the permissions of a file or directory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Permissions(pub(crate) sys::FilePermissions);

impl Permissions {
    /// Returns `true` if these permissions describe a readonly (unwritable) file.
    #[inline]
    pub fn readonly(&self) -> bool {
        self.0.readonly()
    }

    /// Modifies the readonly flag for this set of permissions.
    #[inline]
    pub fn set_readonly(&mut self, readonly: bool) {
        self.0.set_readonly(readonly);
    }
}

#[cfg(unix)]
impl UnixPermissionsExt for Permissions {
    #[inline]
    fn mode(&self) -> u32 {
        self.0.mode
    }

    #[inline]
    fn set_mode(&mut self, mode: u32) {
        self.0.mode = mode;
        self.0.readonly = (mode & 0o222) == 0;
    }

    #[inline]
    fn from_mode(mode: u32) -> Self {
        Self(FilePermissions {
            readonly: (mode & 0o222) == 0,
            mode,
        })
    }
}

/// A structure representing a type of file with accessors for each file type.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct FileType {
    pub(crate) is_dir: bool,
    pub(crate) is_file: bool,
    pub(crate) is_symlink: bool,
}

impl FileType {
    /// Returns `true` if this file type represents a directory.
    #[inline]
    pub fn is_dir(&self) -> bool {
        self.is_dir
    }

    /// Returns `true` if this file type represents a regular file.
    #[inline]
    pub fn is_file(&self) -> bool {
        self.is_file
    }

    /// Returns `true` if this file type represents a symbolic link.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.is_symlink
    }
}

/// Representation of timestamps for a file.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct FileTimes {
    pub(crate) accessed: Option<Duration>,
    pub(crate) modified: Option<Duration>,
}

impl FileTimes {
    /// Creates a new `FileTimes` with no times set.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the last access time of a file.
    #[inline]
    pub fn set_accessed(mut self, t: Duration) -> Self {
        self.accessed = Some(t);
        self
    }

    /// Sets the last modified time of a file.
    #[inline]
    pub fn set_modified(mut self, t: Duration) -> Self {
        self.modified = Some(t);
        self
    }
}

/// An error that can occur while trying to acquire a file lock.
#[derive(Debug)]
pub enum TryLockError {
    /// An I/O error occurred while attempting to acquire the lock.
    Error(Error),
    /// The lock is currently held by another process or descriptor.
    WouldBlock,
}

impl fmt::Display for TryLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryLockError::Error(e) => fmt::Display::fmt(e, f),
            TryLockError::WouldBlock => f.write_str("resource temporarily unavailable"),
        }
    }
}

impl core::error::Error for TryLockError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            TryLockError::Error(e) => Some(e),
            TryLockError::WouldBlock => None,
        }
    }
}

impl From<Error> for TryLockError {
    #[inline]
    fn from(err: Error) -> Self {
        TryLockError::Error(err)
    }
}

/// Reads the entire contents of a file into a bytes vector.
pub fn read<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let size = file.metadata().map(|m| m.len() as usize).ok();
    let mut bytes = Vec::new();
    if let Some(size) = size {
        let _ = bytes.try_reserve(size);
    }
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Reads the entire contents of a file into a string.
pub fn read_to_string<P: AsRef<Path>>(path: P) -> Result<String> {
    let mut file = File::open(path)?;
    let size = file.metadata().map(|m| m.len() as usize).ok();
    let mut string = String::new();
    if let Some(size) = size {
        let _ = string.try_reserve(size);
    }
    file.read_to_string(&mut string)?;
    Ok(string)
}

/// Writes a slice as the entire contents of a file.
pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> Result<()> {
    File::create(path)?.write_all(contents.as_ref())
}

/// Copies the contents of one file to another.
pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<u64> {
    let mut reader = File::open(from)?;
    let mut writer = File::create(to)?;
    io_copy(&mut reader, &mut writer)
}

/// Removes a file from the filesystem.
pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    sys::remove_file(path.as_ref())
}
