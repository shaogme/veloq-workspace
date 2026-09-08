//! Owned and borrowed Unix file descriptors.

use core::{fmt, marker::PhantomData, mem::forget};

use super::raw::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use crate::alloc_crate as alloc;
use crate::io::{Error, Result};

use alloc::{boxed::Box, rc::Rc, sync::Arc};

#[cfg(feature = "std")]
use std::{
    fs::File as StdFile,
    net::{TcpListener, TcpStream, UdpSocket},
    os::fd::AsRawFd as StdAsRawFd,
};

/// A borrowed file descriptor.
#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct BorrowedFd<'fd> {
    fd: RawFd,
    _phantom: PhantomData<&'fd OwnedFd>,
}

/// An owned file descriptor.
#[repr(transparent)]
pub struct OwnedFd {
    fd: RawFd,
}

/// A trait to borrow the file descriptor from an underlying object.
pub trait AsFd {
    /// Borrows the file descriptor.
    fn as_fd(&self) -> BorrowedFd<'_>;
}

impl BorrowedFd<'_> {
    /// Returns a `BorrowedFd` holding the given raw file descriptor.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `fd` must remain open for the duration of
    /// the returned `BorrowedFd`.
    ///
    /// # Panics
    ///
    /// Panics if the raw file descriptor has the value `-1`.
    #[inline]
    #[track_caller]
    pub const unsafe fn borrow_raw(fd: RawFd) -> Self {
        assert!(fd != -1, "invalid raw file descriptor: -1");
        Self {
            fd,
            _phantom: PhantomData,
        }
    }

    /// Creates a new `OwnedFd` instance that shares the same underlying file
    /// description as the existing `BorrowedFd` instance.
    pub fn try_clone_to_owned(&self) -> Result<OwnedFd> {
        let fd = unsafe { libc::fcntl(self.fd, libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    }
}

impl OwnedFd {
    /// Creates a new `OwnedFd` instance that shares the same underlying file
    /// description as the existing `OwnedFd` instance.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.as_fd().try_clone_to_owned()
    }
}

impl Drop for OwnedFd {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let _ = libc::close(self.fd);
        }
    }
}

unsafe impl Send for OwnedFd {}
unsafe impl Sync for OwnedFd {}
unsafe impl Send for BorrowedFd<'_> {}
unsafe impl Sync for BorrowedFd<'_> {}

impl fmt::Debug for BorrowedFd<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedFd").field("fd", &self.fd).finish()
    }
}

impl fmt::Debug for OwnedFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedFd").field("fd", &self.fd).finish()
    }
}

impl AsRawFd for BorrowedFd<'_> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl AsFd for BorrowedFd<'_> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        *self
    }
}

impl AsRawFd for OwnedFd {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for OwnedFd {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        forget(self);
        fd
    }
}

impl FromRawFd for OwnedFd {
    #[inline]
    #[track_caller]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        assert!(fd != -1, "invalid raw file descriptor: -1");
        Self { fd }
    }
}

impl AsFd for OwnedFd {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

impl<'fd> From<&'fd OwnedFd> for BorrowedFd<'fd> {
    #[inline]
    fn from(fd: &'fd OwnedFd) -> Self {
        fd.as_fd()
    }
}

impl From<OwnedFd> for RawFd {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        fd.into_raw_fd()
    }
}

impl<T: AsFd + ?Sized> AsFd for &T {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        T::as_fd(self)
    }
}

impl<T: AsFd + ?Sized> AsFd for &mut T {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        T::as_fd(self)
    }
}

impl<T: AsFd + ?Sized> AsFd for Box<T> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        (**self).as_fd()
    }
}

impl<T: AsFd + ?Sized> AsFd for Arc<T> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        (**self).as_fd()
    }
}

impl<T: AsFd + ?Sized> AsFd for Rc<T> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        (**self).as_fd()
    }
}

#[cfg(feature = "std")]
impl AsFd for StdFile {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(StdAsRawFd::as_raw_fd(self)) }
    }
}

#[cfg(feature = "std")]
impl AsFd for TcpStream {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(StdAsRawFd::as_raw_fd(self)) }
    }
}

#[cfg(feature = "std")]
impl AsFd for TcpListener {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(StdAsRawFd::as_raw_fd(self)) }
    }
}

#[cfg(feature = "std")]
impl AsFd for UdpSocket {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(StdAsRawFd::as_raw_fd(self)) }
    }
}
