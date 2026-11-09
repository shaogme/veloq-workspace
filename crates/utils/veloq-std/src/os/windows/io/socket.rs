//! Owned and borrowed Windows sockets.

use core::{
    fmt,
    marker::PhantomData,
    mem::{self, forget},
    sync::atomic::{
        AtomicBool,
        Ordering::{AcqRel, Relaxed},
    },
};

use super::raw::{AsRawSocket, FromRawSocket, IntoRawSocket, RawSocket};
use crate::{
    alloc_crate as alloc,
    io::{Error, Result},
    os::Cvt,
};

use alloc::{boxed::Box, rc::Rc, sync::Arc};
use windows_sys::Win32::{
    Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation},
    Networking::WinSock::{
        INVALID_SOCKET, SOCKET, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED, WSACleanup,
        WSADATA, WSADuplicateSocketW, WSAEINVAL, WSAEPROTOTYPE, WSAGetLastError, WSAPROTOCOL_INFOW,
        WSASocketW, WSAStartup, closesocket,
    },
    System::Threading::GetCurrentProcessId,
};

#[cfg(feature = "std")]
use std::{
    net::{TcpListener as StdTcpListener, TcpStream as StdTcpStream, UdpSocket as StdUdpSocket},
    os::windows::io::AsRawSocket as StdAsRawSocket,
};

static WSA_STARTED: AtomicBool = AtomicBool::new(false);

pub(crate) struct Winsock;

impl Winsock {
    pub(crate) fn init() {
        if !WSA_STARTED.load(Relaxed) {
            unsafe {
                let mut data: WSADATA = mem::zeroed();
                let ret = WSAStartup(0x0202, &mut data);
                assert_eq!(ret, 0, "failed to initialize winsock");
                if WSA_STARTED.swap(true, AcqRel) {
                    let _ = WSACleanup();
                }
            }
        }
    }

    pub(crate) fn last_error() -> Error {
        Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }
}

/// An owned Windows socket.
#[repr(transparent)]
pub struct OwnedSocket {
    socket: RawSocket,
}

/// A borrowed Windows socket.
#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct BorrowedSocket<'socket> {
    socket: RawSocket,
    _phantom: PhantomData<&'socket OwnedSocket>,
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

    /// Creates a new `OwnedSocket` instance that shares the same underlying
    /// object as the existing `BorrowedSocket` instance.
    pub fn try_clone_to_owned(&self) -> Result<OwnedSocket> {
        Winsock::init();
        let mut info = unsafe { mem::zeroed::<WSAPROTOCOL_INFOW>() };
        let result = unsafe {
            WSADuplicateSocketW(
                self.socket as usize as SOCKET,
                GetCurrentProcessId(),
                &mut info,
            )
        };
        Cvt::cvt_socket(result)?;
        let socket = unsafe {
            WSASocketW(
                info.iAddressFamily,
                info.iSocketType,
                info.iProtocol,
                &info,
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
            )
        };

        if socket != INVALID_SOCKET {
            Ok(unsafe { OwnedSocket::from_raw_socket(socket as RawSocket) })
        } else {
            let error = unsafe { WSAGetLastError() };

            if error != WSAEPROTOTYPE && error != WSAEINVAL {
                return Err(Error::from_raw_os_error(error));
            }

            let socket = unsafe {
                WSASocketW(
                    info.iAddressFamily,
                    info.iSocketType,
                    info.iProtocol,
                    &info,
                    0,
                    WSA_FLAG_OVERLAPPED,
                )
            };

            if socket == INVALID_SOCKET {
                return Err(Error::from_raw_os_error(unsafe { WSAGetLastError() }));
            }

            let owned = unsafe { OwnedSocket::from_raw_socket(socket as RawSocket) };
            owned.set_no_inherit()?;
            Ok(owned)
        }
    }
}

impl OwnedSocket {
    /// Creates a new `OwnedSocket` instance that shares the same underlying
    /// object as the existing `OwnedSocket` instance.
    #[inline]
    pub fn try_clone(&self) -> Result<Self> {
        self.as_socket().try_clone_to_owned()
    }

    pub(crate) fn set_no_inherit(&self) -> Result<()> {
        Cvt::cvt(unsafe {
            SetHandleInformation(self.socket as usize as HANDLE, HANDLE_FLAG_INHERIT, 0)
        })?;
        Ok(())
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
impl AsSocket for StdTcpStream {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}

#[cfg(feature = "std")]
impl AsSocket for StdTcpListener {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}

#[cfg(feature = "std")]
impl AsSocket for StdUdpSocket {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        unsafe { BorrowedSocket::borrow_raw(StdAsRawSocket::as_raw_socket(self)) }
    }
}
