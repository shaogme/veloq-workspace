//! Windows-specific file system primitives and extension traits.

use crate::io::{Error, Result};

#[cfg(feature = "std")]
use std::{
    fs::{File as StdFile, Metadata as StdMetadata, OpenOptions as StdOpenOptions},
    os::windows::fs::{
        FileExt as StdFileExt, MetadataExt as StdMetadataExt, OpenOptionsExt as StdOpenOptionsExt,
    },
};

/// Windows-specific extensions to file open options.
pub trait OpenOptionsExt {
    /// Overrides the `dwDesiredAccess` argument to the call to `CreateFile`.
    fn access_mode(&mut self, access: u32) -> &mut Self;

    /// Overrides the `dwShareMode` argument to the call to `CreateFile`.
    fn share_mode(&mut self, val: u32) -> &mut Self;

    /// Sets extra flags for the `dwFileFlags` argument to `CreateFile`.
    fn custom_flags(&mut self, flags: u32) -> &mut Self;

    /// Sets the `dwFileAttributes` argument to `CreateFile`.
    fn attributes(&mut self, val: u32) -> &mut Self;

    /// Sets the `dwSecurityQosFlags` argument to `CreateFile`.
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self;
}

/// Windows file open options representation for standalone `no_std` environments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    pub access_mode: Option<u32>,
    pub share_mode: u32,
    pub custom_flags: u32,
    pub attributes: u32,
    pub security_qos_flags: u32,
}

impl OpenOptions {
    /// Creates a new, blank set of options.
    pub const fn new() -> Self {
        Self {
            access_mode: None,
            share_mode: 0,
            custom_flags: 0,
            attributes: 0,
            security_qos_flags: 0,
        }
    }
}

impl OpenOptionsExt for OpenOptions {
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

#[cfg(feature = "std")]
impl OpenOptionsExt for StdOpenOptions {
    #[inline]
    fn access_mode(&mut self, access: u32) -> &mut Self {
        StdOpenOptionsExt::access_mode(self, access);
        self
    }

    #[inline]
    fn share_mode(&mut self, val: u32) -> &mut Self {
        StdOpenOptionsExt::share_mode(self, val);
        self
    }

    #[inline]
    fn custom_flags(&mut self, flags: u32) -> &mut Self {
        StdOpenOptionsExt::custom_flags(self, flags);
        self
    }

    #[inline]
    fn attributes(&mut self, val: u32) -> &mut Self {
        StdOpenOptionsExt::attributes(self, val);
        self
    }

    #[inline]
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        StdOpenOptionsExt::security_qos_flags(self, flags);
        self
    }
}

/// Windows-specific extensions to file operations.
pub trait FileExt {
    /// Seeks to a given position and reads a number of bytes.
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Seeks to a given position and writes a number of bytes.
    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize>;
}

#[cfg(feature = "std")]
impl FileExt for StdFile {
    #[inline]
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        StdFileExt::seek_read(self, buf, offset)
            .map_err(|e| Error::from_raw_os_error(e.raw_os_error().unwrap_or(0)))
    }

    #[inline]
    fn seek_write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        StdFileExt::seek_write(self, buf, offset)
            .map_err(|e| Error::from_raw_os_error(e.raw_os_error().unwrap_or(0)))
    }
}

/// Windows-specific extensions to file metadata.
pub trait MetadataExt {
    /// Returns the value of the `dwFileAttributes` field of this metadata.
    fn file_attributes(&self) -> u32;

    /// Returns the value of the `ftCreationTime` field of this metadata.
    fn creation_time(&self) -> u64;

    /// Returns the value of the `ftLastAccessTime` field of this metadata.
    fn last_access_time(&self) -> u64;

    /// Returns the value of the `ftLastWriteTime` field of this metadata.
    fn last_write_time(&self) -> u64;

    /// Returns the value of the `nFileSize` fields of this metadata.
    fn file_size(&self) -> u64;
}

#[cfg(feature = "std")]
impl MetadataExt for StdMetadata {
    #[inline]
    fn file_attributes(&self) -> u32 {
        StdMetadataExt::file_attributes(self)
    }

    #[inline]
    fn creation_time(&self) -> u64 {
        StdMetadataExt::creation_time(self)
    }

    #[inline]
    fn last_access_time(&self) -> u64 {
        StdMetadataExt::last_access_time(self)
    }

    #[inline]
    fn last_write_time(&self) -> u64 {
        StdMetadataExt::last_write_time(self)
    }

    #[inline]
    fn file_size(&self) -> u64 {
        StdMetadataExt::file_size(self)
    }
}
