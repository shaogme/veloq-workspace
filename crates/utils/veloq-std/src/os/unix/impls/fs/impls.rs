//! Unix-specific file system primitives and extension traits.

use crate::io::{Error, IoSlice, IoSliceMut, Result};

#[cfg(feature = "std")]
use std::{
    fs::{
        File as StdFile, Metadata as StdMetadata, OpenOptions as StdOpenOptions,
        Permissions as StdPermissions,
    },
    os::unix::fs::{
        FileExt as StdFileExt, MetadataExt as StdMetadataExt, OpenOptionsExt as StdOpenOptionsExt,
        PermissionsExt as StdPermissionsExt,
    },
};

/// Unix-specific extensions to file open options.
pub trait OpenOptionsExt {
    /// Sets the mode bits for a new file.
    fn mode(&mut self, mode: u32) -> &mut Self;

    /// Sets custom flags for the `open` call.
    fn custom_flags(&mut self, flags: i32) -> &mut Self;
}

/// Unix-specific extensions to file operations.
pub trait FileExt {
    /// Reads a number of bytes starting from a given offset.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Writes a number of bytes starting from a given offset.
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Reads the exact number of bytes required to fill `buf` from the given offset.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.is_interrupted() => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(Error::READ_EXACT_EOF)
        } else {
            Ok(())
        }
    }

    /// Attempts to write an entire buffer starting from a given offset.
    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => return Err(Error::WRITE_ZERO),
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.is_interrupted() => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Reads into a slice of buffers from a given offset.
    fn read_vectored_at(&self, bufs: &mut [IoSliceMut<'_>], mut offset: u64) -> Result<usize> {
        let mut total = 0;
        for buf in bufs {
            if buf.is_empty() {
                continue;
            }
            match self.read_at(buf, offset)? {
                0 => break,
                n => {
                    total += n;
                    offset += n as u64;
                    if n < buf.len() {
                        break;
                    }
                }
            }
        }
        Ok(total)
    }

    /// Writes from a slice of buffers to a given offset.
    fn write_vectored_at(&self, bufs: &[IoSlice<'_>], mut offset: u64) -> Result<usize> {
        let mut total = 0;
        for buf in bufs {
            if buf.is_empty() {
                continue;
            }
            match self.write_at(buf, offset)? {
                0 => break,
                n => {
                    total += n;
                    offset += n as u64;
                    if n < buf.len() {
                        break;
                    }
                }
            }
        }
        Ok(total)
    }
}

/// Unix-specific extensions to file metadata.
pub trait MetadataExt {
    /// Returns the device ID of the filesystem containing the file.
    fn dev(&self) -> u64;

    /// Returns the inode number.
    fn ino(&self) -> u64;

    /// Returns the file mode (permissions).
    fn mode(&self) -> u32;

    /// Returns the number of hard links.
    fn nlink(&self) -> u64;

    /// Returns the user ID of the owner.
    fn uid(&self) -> u32;

    /// Returns the group ID of the owner.
    fn gid(&self) -> u32;

    /// Returns the device ID representing the device if file is special.
    fn rdev(&self) -> u64;

    /// Returns the total size of this file in bytes.
    fn size(&self) -> u64;

    /// Returns the time of last access (seconds).
    fn atime(&self) -> i64;

    /// Returns the time of last access (nanoseconds).
    fn atime_nsec(&self) -> i64;

    /// Returns the time of last modification (seconds).
    fn mtime(&self) -> i64;

    /// Returns the time of last modification (nanoseconds).
    fn mtime_nsec(&self) -> i64;

    /// Returns the time of last status change (seconds).
    fn ctime(&self) -> i64;

    /// Returns the time of last status change (nanoseconds).
    fn ctime_nsec(&self) -> i64;

    /// Returns the block size for filesystem I/O.
    fn blksize(&self) -> u64;

    /// Returns the number of 512B blocks allocated.
    fn blocks(&self) -> u64;
}

/// Unix-specific extensions to permissions.
pub trait PermissionsExt {
    /// Returns the underlying raw file mode.
    fn mode(&self) -> u32;

    /// Sets the underlying raw file mode.
    fn set_mode(&mut self, mode: u32);

    /// Creates a new `Permissions` instance from raw mode bits.
    fn from_mode(mode: u32) -> Self;
}

#[cfg(feature = "std")]
impl OpenOptionsExt for StdOpenOptions {
    #[inline]
    fn mode(&mut self, mode: u32) -> &mut Self {
        StdOpenOptionsExt::mode(self, mode);
        self
    }

    #[inline]
    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        StdOpenOptionsExt::custom_flags(self, flags);
        self
    }
}

#[cfg(feature = "std")]
impl FileExt for StdFile {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        StdFileExt::read_at(self, buf, offset).map_err(Error::from)
    }

    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        StdFileExt::write_at(self, buf, offset).map_err(Error::from)
    }
}

#[cfg(feature = "std")]
impl MetadataExt for StdMetadata {
    #[inline]
    fn dev(&self) -> u64 {
        StdMetadataExt::dev(self)
    }

    #[inline]
    fn ino(&self) -> u64 {
        StdMetadataExt::ino(self)
    }

    #[inline]
    fn mode(&self) -> u32 {
        StdMetadataExt::mode(self)
    }

    #[inline]
    fn nlink(&self) -> u64 {
        StdMetadataExt::nlink(self)
    }

    #[inline]
    fn uid(&self) -> u32 {
        StdMetadataExt::uid(self)
    }

    #[inline]
    fn gid(&self) -> u32 {
        StdMetadataExt::gid(self)
    }

    #[inline]
    fn rdev(&self) -> u64 {
        StdMetadataExt::rdev(self)
    }

    #[inline]
    fn size(&self) -> u64 {
        StdMetadataExt::size(self)
    }

    #[inline]
    fn atime(&self) -> i64 {
        StdMetadataExt::atime(self)
    }

    #[inline]
    fn atime_nsec(&self) -> i64 {
        StdMetadataExt::atime_nsec(self)
    }

    #[inline]
    fn mtime(&self) -> i64 {
        StdMetadataExt::mtime(self)
    }

    #[inline]
    fn mtime_nsec(&self) -> i64 {
        StdMetadataExt::mtime_nsec(self)
    }

    #[inline]
    fn ctime(&self) -> i64 {
        StdMetadataExt::ctime(self)
    }

    #[inline]
    fn ctime_nsec(&self) -> i64 {
        StdMetadataExt::ctime_nsec(self)
    }

    #[inline]
    fn blksize(&self) -> u64 {
        StdMetadataExt::blksize(self)
    }

    #[inline]
    fn blocks(&self) -> u64 {
        StdMetadataExt::blocks(self)
    }
}

#[cfg(feature = "std")]
impl PermissionsExt for StdPermissions {
    #[inline]
    fn mode(&self) -> u32 {
        StdPermissionsExt::mode(self)
    }

    #[inline]
    fn set_mode(&mut self, mode: u32) {
        StdPermissionsExt::set_mode(self, mode);
    }

    #[inline]
    fn from_mode(mode: u32) -> Self {
        StdPermissionsExt::from_mode(mode)
    }
}
