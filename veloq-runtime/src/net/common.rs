use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;

use veloq_driver::Socket;
use veloq_driver::SocketLifecycleHandle;
use veloq_driver::driver::Driver;
use veloq_driver::op::IoFd;
use veloq_driver::{RawHandle, RawHandleKind};

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub(crate) struct SocketToken {
    raw: RawHandle,
    fd: IoFd,
    lifecycle_handle: Option<SocketLifecycleHandle>,
    registered_fd: Option<IoFd>,
}

impl SocketToken {
    pub(crate) fn new(handle: RawHandle) -> Self {
        let lifecycle_handle = crate::runtime::context::try_current()
            .and_then(|ctx| ctx.driver().upgrade())
            .map(|driver| driver.borrow().socket_lifecycle_handle());

        let registered_fd = Self::try_register_with_driver(handle, &lifecycle_handle);

        // Keep runtime submission path on Raw handle; registration is used only
        // for lifecycle/cleanup coordination on Windows driver side.
        let fd = IoFd::Raw(handle);

        Self {
            raw: handle,
            fd,
            lifecycle_handle,
            registered_fd,
        }
    }

    fn try_register_with_driver(
        handle: RawHandle,
        lifecycle_handle: &Option<SocketLifecycleHandle>,
    ) -> Option<IoFd> {
        if !handle.borrow().is_socket() {
            return None;
        }
        if !lifecycle_handle
            .as_ref()
            .is_some_and(SocketLifecycleHandle::supports_registration)
        {
            return None;
        }
        let ctx = crate::runtime::context::try_current()?;
        let driver = ctx.driver().upgrade()?;
        let fd = driver
            .borrow_mut()
            .register_files(&[handle])
            .ok()?
            .into_iter()
            .next()?;
        matches!(fd, IoFd::Fixed(_)).then_some(fd)
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }

    #[inline]
    pub(crate) fn raw(&self) -> RawHandle {
        self.raw
    }
}

impl Drop for SocketToken {
    fn drop(&mut self) {
        let _ = (&self.lifecycle_handle, self.registered_fd);
        if self.raw.borrow().is_socket() {
            // Socket lifecycle is intentionally deferred to driver/runtime teardown.
            // Avoid per-drop close/unregister races under high churn.
            return;
        }
        match self.raw.borrow().kind() {
            RawHandleKind::Socket => {
                let _ = unsafe { Socket::from_raw(self.raw) };
            }
            #[cfg(unix)]
            RawHandleKind::File => unsafe {
                libc::close(self.raw.as_fd());
            },
            #[cfg(windows)]
            RawHandleKind::File => unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.raw.as_handle());
            },
        }
    }
}

pub struct InnerSocket {
    token: SocketToken,
}

impl InnerSocket {
    pub fn new(handle: RawHandle) -> Self {
        Self {
            token: SocketToken::new(handle),
        }
    }

    pub fn fd(&self) -> IoFd {
        self.token.fd()
    }

    pub fn raw(&self) -> RawHandle {
        self.token.raw()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        debug_assert!(
            self.token.raw().borrow().is_socket(),
            "InnerSocket expects socket-kind handle"
        );
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.token.raw())) };
        socket.local_addr()
    }

    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        debug_assert!(
            self.token.raw().borrow().is_socket(),
            "InnerSocket expects socket-kind handle"
        );
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.token.raw())) };
        socket.connect(addr)
    }
}
