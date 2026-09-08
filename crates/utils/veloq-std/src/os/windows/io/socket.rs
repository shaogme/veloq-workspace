//! Owned and borrowed Windows sockets.

use core::{fmt, marker::PhantomData, mem::forget};

use super::raw::{AsRawSocket, FromRawSocket, IntoRawSocket, RawSocket};
use crate::alloc_crate as alloc;

use alloc::{boxed::Box, rc::Rc, sync::Arc};
use windows_sys::Win32::Networking::WinSock::closesocket;

#[cfg(feature = "std")]
use std::{
    net::{TcpListener, TcpStream, UdpSocket},
    os::windows::io::AsRawSocket as StdAsRawSocket,
};

/// A borrowed Windows socket.
#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct BorrowedSocket<'socket> {
    socket: RawSocket,
    _phantom: PhantomData<&'socket OwnedSocket>,
}

/// An owned Windows socket.
#[repr(transparent)]
pub struct OwnedSocket {
    socket: RawSocket,
}

/// A trait to borrow the socket from an underlying object.
pub trait AsSocket {
    /// Borrows the socket.
    fn as_socket(&self) -> BorrowedSocket<'_>;
}

unsafe impl Send for OwnedSocket {}
unsafe impl Sync for OwnedSocket {}
unsafe impl Send for BorrowedSocket<'_> {}
unsafe impl Sync for BorrowedSocket<'_> {}

impl BorrowedSocket<'_> {
    /// Returns a `BorrowedSocket` holding the given raw socket.
    ///
    /// # Safety
    ///
    /// The socket must remain open for the duration of the returned `BorrowedSocket`.
    #[inline]
    pub const unsafe fn borrow_raw(socket: RawSocket) -> Self {
        Self {
            socket,
            _phantom: PhantomData,
        }
    }
}

impl Drop for OwnedSocket {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let _ = closesocket(self.socket as _);
        }
    }
}

impl fmt::Debug for BorrowedSocket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedSocket")
            .field("socket", &self.socket)
            .finish()
    }
}

impl fmt::Debug for OwnedSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSocket")
            .field("socket", &self.socket)
            .finish()
    }
}

impl AsRawSocket for BorrowedSocket<'_> {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.socket
    }
}

impl AsSocket for BorrowedSocket<'_> {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        *self
    }
}

impl AsRawSocket for OwnedSocket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.socket
    }
}

impl IntoRawSocket for OwnedSocket {
    #[inline]
    fn into_raw_socket(self) -> RawSocket {
        let socket = self.socket;
        forget(self);
        socket
    }
}

impl FromRawSocket for OwnedSocket {
    #[inline]
    unsafe fn from_raw_socket(socket: RawSocket) -> Self {
        Self { socket }
    }
}

impl AsSocket for OwnedSocket {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(self.socket) }
    }
}

impl<'socket> From<&'socket OwnedSocket> for BorrowedSocket<'socket> {
    #[inline]
    fn from(sock: &'socket OwnedSocket) -> Self {
        sock.as_socket()
    }
}

impl From<OwnedSocket> for RawSocket {
    #[inline]
    fn from(sock: OwnedSocket) -> Self {
        sock.into_raw_socket()
    }
}

impl<T: AsSocket + ?Sized> AsSocket for &T {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        T::as_socket(self)
    }
}

impl<T: AsSocket + ?Sized> AsSocket for &mut T {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        T::as_socket(self)
    }
}

impl<T: AsSocket + ?Sized> AsSocket for Box<T> {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        (**self).as_socket()
    }
}

impl<T: AsSocket + ?Sized> AsSocket for Arc<T> {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        (**self).as_socket()
    }
}

impl<T: AsSocket + ?Sized> AsSocket for Rc<T> {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        (**self).as_socket()
    }
}

#[cfg(feature = "std")]
impl AsSocket for TcpStream {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}

#[cfg(feature = "std")]
impl AsSocket for TcpListener {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}

#[cfg(feature = "std")]
impl AsSocket for UdpSocket {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}
