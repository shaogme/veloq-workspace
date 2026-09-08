//! Owned and borrowed Windows handles.

use core::{
    fmt,
    marker::PhantomData,
    mem::forget,
    ptr::{eq, null_mut},
};

use super::raw::{AsRawHandle, FromRawHandle, IntoRawHandle, RawHandle};
use crate::{
    alloc_crate as alloc,
    io::{Error, Result},
};

use alloc::{boxed::Box, rc::Rc, sync::Arc};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE, INVALID_HANDLE_VALUE,
    },
    System::Threading::GetCurrentProcess,
};

#[cfg(feature = "std")]
use std::{fs::File as StdFile, os::windows::io::AsRawHandle as StdAsRawHandle};

/// A borrowed Windows handle.
#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct BorrowedHandle<'handle> {
    handle: RawHandle,
    _phantom: PhantomData<&'handle OwnedHandle>,
}

/// An owned Windows handle.
#[repr(transparent)]
pub struct OwnedHandle {
    handle: RawHandle,
}

/// A trait to borrow the handle from an underlying object.
pub trait AsHandle {
    /// Borrows the handle.
    fn as_handle(&self) -> BorrowedHandle<'_>;
}

/// Error indicating that a Windows handle was null.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NullHandleError;

impl fmt::Display for NullHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("null handle")
    }
}

impl core::error::Error for NullHandleError {}

/// FFI type for handles in return values or out parameters, where `NULL` is used as sentry.
#[repr(transparent)]
#[derive(Debug)]
pub struct HandleOrNull(RawHandle);

/// Error indicating that a Windows handle was `INVALID_HANDLE_VALUE`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct InvalidHandleError;

impl fmt::Display for InvalidHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid handle value")
    }
}

impl core::error::Error for InvalidHandleError {}

/// FFI type for handles in return values or out parameters, where `INVALID_HANDLE_VALUE` is used as sentry.
#[repr(transparent)]
#[derive(Debug)]
pub struct HandleOrInvalid(RawHandle);

unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}
unsafe impl Send for BorrowedHandle<'_> {}
unsafe impl Sync for BorrowedHandle<'_> {}
unsafe impl Send for HandleOrNull {}
unsafe impl Sync for HandleOrNull {}
unsafe impl Send for HandleOrInvalid {}
unsafe impl Sync for HandleOrInvalid {}

impl BorrowedHandle<'_> {
    /// Returns a `BorrowedHandle` holding the given raw handle.
    ///
    /// # Safety
    ///
    /// The resource pointed to by `handle` must be an open handle and remain open for the duration.
    #[inline]
    pub const unsafe fn borrow_raw(handle: RawHandle) -> Self {
        Self {
            handle,
            _phantom: PhantomData,
        }
    }

    /// Duplicates the handle using `DuplicateHandle`.
    pub fn try_clone_to_owned(&self) -> Result<OwnedHandle> {
        let mut dup: HANDLE = null_mut();
        let current_process = unsafe { GetCurrentProcess() };
        let ok = unsafe {
            DuplicateHandle(
                current_process,
                self.handle as HANDLE,
                current_process,
                &mut dup,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(unsafe { OwnedHandle::from_raw_handle(dup as RawHandle) })
        }
    }
}

impl OwnedHandle {
    /// Creates a new `OwnedHandle` instance that duplicates the handle.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.as_handle().try_clone_to_owned()
    }
}

impl Drop for OwnedHandle {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.handle as HANDLE);
        }
    }
}

impl fmt::Debug for BorrowedHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedHandle")
            .field("handle", &self.handle)
            .finish()
    }
}

impl fmt::Debug for OwnedHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedHandle")
            .field("handle", &self.handle)
            .finish()
    }
}

impl HandleOrNull {
    /// Constructs a `HandleOrNull` from a raw handle.
    ///
    /// # Safety
    ///
    /// The handle must be either null or an open, valid handle.
    #[inline]
    pub const unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self(handle)
    }

    /// Returns `true` if this handle is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// Converts into an `OwnedHandle` if not null.
    #[inline]
    pub fn try_into_handle(self) -> core::result::Result<OwnedHandle, NullHandleError> {
        self.try_into()
    }
}

impl TryFrom<HandleOrNull> for OwnedHandle {
    type Error = NullHandleError;

    #[inline]
    fn try_from(handle: HandleOrNull) -> core::result::Result<Self, Self::Error> {
        if handle.is_null() {
            Err(NullHandleError)
        } else {
            let raw = handle.0;
            forget(handle);
            Ok(unsafe { Self::from_raw_handle(raw) })
        }
    }
}

impl Drop for HandleOrNull {
    #[inline]
    fn drop(&mut self) {
        if !self.is_null() {
            unsafe {
                let _ = CloseHandle(self.0 as HANDLE);
            }
        }
    }
}

impl HandleOrInvalid {
    /// Constructs a `HandleOrInvalid` from a raw handle.
    ///
    /// # Safety
    ///
    /// The handle must be either `INVALID_HANDLE_VALUE` or an open, valid handle.
    #[inline]
    pub const unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self(handle)
    }

    /// Returns `true` if this handle is `INVALID_HANDLE_VALUE`.
    #[inline]
    pub fn is_invalid(&self) -> bool {
        self.0 as isize == -1 || eq(self.0, INVALID_HANDLE_VALUE as _)
    }

    /// Converts into an `OwnedHandle` if not invalid.
    #[inline]
    pub fn try_into_handle(self) -> core::result::Result<OwnedHandle, InvalidHandleError> {
        self.try_into()
    }
}

impl TryFrom<HandleOrInvalid> for OwnedHandle {
    type Error = InvalidHandleError;

    #[inline]
    fn try_from(handle: HandleOrInvalid) -> core::result::Result<Self, Self::Error> {
        if handle.is_invalid() {
            Err(InvalidHandleError)
        } else {
            let raw = handle.0;
            forget(handle);
            Ok(unsafe { Self::from_raw_handle(raw) })
        }
    }
}

impl Drop for HandleOrInvalid {
    #[inline]
    fn drop(&mut self) {
        if !self.is_invalid() {
            unsafe {
                let _ = CloseHandle(self.0 as HANDLE);
            }
        }
    }
}

impl AsRawHandle for BorrowedHandle<'_> {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.handle
    }
}

impl AsHandle for BorrowedHandle<'_> {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        *self
    }
}

impl AsRawHandle for OwnedHandle {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.handle
    }
}

impl IntoRawHandle for OwnedHandle {
    #[inline]
    fn into_raw_handle(self) -> RawHandle {
        let handle = self.handle;
        forget(self);
        handle
    }
}

impl FromRawHandle for OwnedHandle {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self { handle }
    }
}

impl AsHandle for OwnedHandle {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        unsafe { BorrowedHandle::borrow_raw(self.handle) }
    }
}

impl<'handle> From<&'handle OwnedHandle> for BorrowedHandle<'handle> {
    #[inline]
    fn from(handle: &'handle OwnedHandle) -> Self {
        handle.as_handle()
    }
}

impl From<OwnedHandle> for RawHandle {
    #[inline]
    fn from(handle: OwnedHandle) -> Self {
        handle.into_raw_handle()
    }
}

impl<T: AsHandle + ?Sized> AsHandle for &T {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        T::as_handle(self)
    }
}

impl<T: AsHandle + ?Sized> AsHandle for &mut T {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        T::as_handle(self)
    }
}

impl<T: AsHandle + ?Sized> AsHandle for Box<T> {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        (**self).as_handle()
    }
}

impl<T: AsHandle + ?Sized> AsHandle for Arc<T> {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        (**self).as_handle()
    }
}

impl<T: AsHandle + ?Sized> AsHandle for Rc<T> {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        (**self).as_handle()
    }
}

#[cfg(feature = "std")]
impl AsHandle for StdFile {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        unsafe { BorrowedHandle::borrow_raw(StdAsRawHandle::as_raw_handle(self)) }
    }
}
