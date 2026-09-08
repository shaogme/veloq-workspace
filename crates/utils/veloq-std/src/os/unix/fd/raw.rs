//! Raw Unix file descriptors and conversion traits.

use core::ffi::c_int;

use crate::{
    alloc_crate as alloc,
    io::{Stderr, Stdin, Stdout},
};

use alloc::{boxed::Box, rc::Rc, sync::Arc};

#[cfg(feature = "std")]
use std::{
    fs::File as StdFile,
    io::{Stderr as StdStderr, Stdin as StdStdin, Stdout as StdStdout},
    net::{TcpListener as StdTcpListener, TcpStream as StdTcpStream, UdpSocket as StdUdpSocket},
    os::fd::{AsRawFd as StdAsRawFd, FromRawFd as StdFromRawFd, IntoRawFd as StdIntoRawFd},
};

/// Raw file descriptors.
pub type RawFd = c_int;

/// A trait to extract the raw file descriptor from an underlying object.
pub trait AsRawFd {
    /// Extracts the raw file descriptor.
    fn as_raw_fd(&self) -> RawFd;
}

/// A trait to express the ability to construct an object from a raw file descriptor.
pub trait FromRawFd {
    /// Constructs a new instance of `Self` from the given raw file descriptor.
    ///
    /// # Safety
    ///
    /// The `fd` passed in must be an owned file descriptor; in particular, it must be open.
    unsafe fn from_raw_fd(fd: RawFd) -> Self;
}

/// A trait to express the ability to consume an object and acquire ownership of its raw file descriptor.
pub trait IntoRawFd {
    /// Consumes this object, returning the raw underlying file descriptor.
    #[must_use = "losing the raw file descriptor may leak resources"]
    fn into_raw_fd(self) -> RawFd;
}

impl AsRawFd for RawFd {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        *self
    }
}

impl IntoRawFd for RawFd {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self
    }
}

impl FromRawFd for RawFd {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> RawFd {
        fd
    }
}

impl<T: AsRawFd + ?Sized> AsRawFd for &T {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        T::as_raw_fd(self)
    }
}

impl<T: AsRawFd + ?Sized> AsRawFd for &mut T {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        T::as_raw_fd(self)
    }
}

impl<T: AsRawFd + ?Sized> AsRawFd for Box<T> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        (**self).as_raw_fd()
    }
}

impl<T: AsRawFd + ?Sized> AsRawFd for Arc<T> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        (**self).as_raw_fd()
    }
}

impl<T: AsRawFd + ?Sized> AsRawFd for Rc<T> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        (**self).as_raw_fd()
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        StdAsRawFd::as_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl FromRawFd for StdFile {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { StdFromRawFd::from_raw_fd(fd) }
    }
}

#[cfg(feature = "std")]
impl IntoRawFd for StdFile {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        StdIntoRawFd::into_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdTcpStream {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        StdAsRawFd::as_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl FromRawFd for StdTcpStream {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { StdFromRawFd::from_raw_fd(fd) }
    }
}

#[cfg(feature = "std")]
impl IntoRawFd for StdTcpStream {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        StdIntoRawFd::into_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdTcpListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        StdAsRawFd::as_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl FromRawFd for StdTcpListener {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { StdFromRawFd::from_raw_fd(fd) }
    }
}

#[cfg(feature = "std")]
impl IntoRawFd for StdTcpListener {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        StdIntoRawFd::into_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdUdpSocket {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        StdAsRawFd::as_raw_fd(self)
    }
}

#[cfg(feature = "std")]
impl FromRawFd for StdUdpSocket {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { StdFromRawFd::from_raw_fd(fd) }
    }
}

#[cfg(feature = "std")]
impl IntoRawFd for StdUdpSocket {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        StdIntoRawFd::into_raw_fd(self)
    }
}

impl AsRawFd for Stdin {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDIN_FILENO
    }
}

impl AsRawFd for Stdout {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDOUT_FILENO
    }
}

impl AsRawFd for Stderr {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDERR_FILENO
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdStdin {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDIN_FILENO
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdStdout {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDOUT_FILENO
    }
}

#[cfg(feature = "std")]
impl AsRawFd for StdStderr {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        libc::STDERR_FILENO
    }
}
