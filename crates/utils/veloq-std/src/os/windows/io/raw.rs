//! Raw Windows handles, sockets, and conversion traits.

use core::ffi::c_void;

use crate::alloc_crate as alloc;
use crate::io::{Stderr, Stdin, Stdout};

use alloc::{boxed::Box, rc::Rc, sync::Arc};
use windows_sys::Win32::System::Console::{
    GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};

#[cfg(feature = "std")]
use std::{
    fs::File as StdFile,
    io::{Stderr as StdStderr, Stdin as StdStdin, Stdout as StdStdout},
    net::{TcpListener, TcpStream, UdpSocket},
    os::windows::io::{
        AsRawHandle as StdAsRawHandle, AsRawSocket as StdAsRawSocket,
        FromRawHandle as StdFromRawHandle, FromRawSocket as StdFromRawSocket,
        IntoRawHandle as StdIntoRawHandle, IntoRawSocket as StdIntoRawSocket,
    },
};

/// Raw HANDLEs.
pub type RawHandle = *mut c_void;

/// Raw SOCKETs.
#[cfg(target_pointer_width = "32")]
pub type RawSocket = u32;

/// Raw SOCKETs.
#[cfg(target_pointer_width = "64")]
pub type RawSocket = u64;

/// Extracts raw handles.
pub trait AsRawHandle {
    /// Extracts the raw handle.
    fn as_raw_handle(&self) -> RawHandle;
}

/// Constructs I/O objects from raw handles.
pub trait FromRawHandle {
    /// Constructs a new I/O object from the specified raw handle.
    ///
    /// # Safety
    ///
    /// The `handle` passed in must be an owned handle; in particular, it must be open.
    unsafe fn from_raw_handle(handle: RawHandle) -> Self;
}

/// Consumes an object and acquires ownership of its raw handle.
pub trait IntoRawHandle {
    /// Consumes this object, returning the raw underlying handle.
    #[must_use = "losing the raw handle may leak resources"]
    fn into_raw_handle(self) -> RawHandle;
}

/// Extracts raw sockets.
pub trait AsRawSocket {
    /// Extracts the raw socket.
    fn as_raw_socket(&self) -> RawSocket;
}

/// Creates I/O objects from raw sockets.
pub trait FromRawSocket {
    /// Constructs a new I/O object from the specified raw socket.
    ///
    /// # Safety
    ///
    /// The `socket` passed in must be an owned socket; in particular, it must be open.
    unsafe fn from_raw_socket(sock: RawSocket) -> Self;
}

/// Consumes an object and acquires ownership of its raw socket.
pub trait IntoRawSocket {
    /// Consumes this object, returning the raw underlying socket.
    #[must_use = "losing the raw socket may leak resources"]
    fn into_raw_socket(self) -> RawSocket;
}

impl AsRawHandle for RawHandle {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        *self
    }
}

impl IntoRawHandle for RawHandle {
    #[inline]
    fn into_raw_handle(self) -> RawHandle {
        self
    }
}

impl FromRawHandle for RawHandle {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> RawHandle {
        handle
    }
}

impl AsRawSocket for RawSocket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        *self
    }
}

impl IntoRawSocket for RawSocket {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        self
    }
}

impl FromRawSocket for RawSocket {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> RawSocket {
        sock
    }
}

impl<T: AsRawHandle + ?Sized> AsRawHandle for &T {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        T::as_raw_handle(self)
    }
}

impl<T: AsRawHandle + ?Sized> AsRawHandle for &mut T {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        T::as_raw_handle(self)
    }
}

impl<T: AsRawHandle + ?Sized> AsRawHandle for Box<T> {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        (**self).as_raw_handle()
    }
}

impl<T: AsRawHandle + ?Sized> AsRawHandle for Arc<T> {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        (**self).as_raw_handle()
    }
}

impl<T: AsRawHandle + ?Sized> AsRawHandle for Rc<T> {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        (**self).as_raw_handle()
    }
}

impl<T: AsRawSocket + ?Sized> AsRawSocket for &T {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        T::as_raw_socket(self)
    }
}

impl<T: AsRawSocket + ?Sized> AsRawSocket for &mut T {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        T::as_raw_socket(self)
    }
}

impl<T: AsRawSocket + ?Sized> AsRawSocket for Box<T> {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        (**self).as_raw_socket()
    }
}

impl<T: AsRawSocket + ?Sized> AsRawSocket for Arc<T> {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        (**self).as_raw_socket()
    }
}

impl<T: AsRawSocket + ?Sized> AsRawSocket for Rc<T> {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        (**self).as_raw_socket()
    }
}

#[cfg(feature = "std")]
impl AsRawHandle for StdFile {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        StdAsRawHandle::as_raw_handle(self)
    }
}

#[cfg(feature = "std")]
impl FromRawHandle for StdFile {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        unsafe { StdFromRawHandle::from_raw_handle(handle) }
    }
}

#[cfg(feature = "std")]
impl IntoRawHandle for StdFile {
    #[inline]
    fn into_raw_handle(self) -> RawHandle {
        StdIntoRawHandle::into_raw_handle(self)
    }
}

#[cfg(feature = "std")]
impl AsRawSocket for TcpStream {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        StdAsRawSocket::as_raw_socket(self)
    }
}

#[cfg(feature = "std")]
impl FromRawSocket for TcpStream {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { StdFromRawSocket::from_raw_socket(sock) }
    }
}

#[cfg(feature = "std")]
impl IntoRawSocket for TcpStream {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        StdIntoRawSocket::into_raw_socket(self)
    }
}

#[cfg(feature = "std")]
impl AsRawSocket for TcpListener {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        StdAsRawSocket::as_raw_socket(self)
    }
}

#[cfg(feature = "std")]
impl FromRawSocket for TcpListener {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { StdFromRawSocket::from_raw_socket(sock) }
    }
}

#[cfg(feature = "std")]
impl IntoRawSocket for TcpListener {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        StdIntoRawSocket::into_raw_socket(self)
    }
}

#[cfg(feature = "std")]
impl AsRawSocket for UdpSocket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        StdAsRawSocket::as_raw_socket(self)
    }
}

#[cfg(feature = "std")]
impl FromRawSocket for UdpSocket {
    #[inline]
    unsafe fn from_raw_socket(sock: RawSocket) -> Self {
        unsafe { StdFromRawSocket::from_raw_socket(sock) }
    }
}

#[cfg(feature = "std")]
impl IntoRawSocket for UdpSocket {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        StdIntoRawSocket::into_raw_socket(self)
    }
}

impl AsRawHandle for Stdin {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        unsafe { GetStdHandle(STD_INPUT_HANDLE) as RawHandle }
    }
}

impl AsRawHandle for Stdout {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        unsafe { GetStdHandle(STD_OUTPUT_HANDLE) as RawHandle }
    }
}

impl AsRawHandle for Stderr {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        unsafe { GetStdHandle(STD_ERROR_HANDLE) as RawHandle }
    }
}

#[cfg(feature = "std")]
impl AsRawHandle for StdStdin {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        StdAsRawHandle::as_raw_handle(self)
    }
}

#[cfg(feature = "std")]
impl AsRawHandle for StdStdout {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        StdAsRawHandle::as_raw_handle(self)
    }
}

#[cfg(feature = "std")]
impl AsRawHandle for StdStderr {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        StdAsRawHandle::as_raw_handle(self)
    }
}
