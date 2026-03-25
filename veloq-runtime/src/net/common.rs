use std::io;
use std::net::SocketAddr;

use veloq_driver::driver::{Driver, RegisterFd};
use veloq_driver::op::IoFd;
use veloq_driver::{OwnedRawHandle, RawHandle};

fn driver_err(err: error_stack::Report<veloq_driver::error::DriverErrorKind>) -> io::Error {
    io::Error::other(format!("{err:#}"))
}

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub(crate) struct SocketToken {
    fd: IoFd,
}

impl SocketToken {
    pub(crate) fn new(handle: RawHandle) -> io::Result<Self> {
        if !handle.borrow().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket registration requires socket handle",
            ));
        }
        // SAFETY: caller transfers ownership via RawHandle created from OwnedRawHandle::into_raw.
        let owned = unsafe { OwnedRawHandle::from_raw_owned(handle) };
        let ctx = crate::runtime::context::try_current()
            .ok_or_else(|| io::Error::other("runtime context not set"))?;
        let driver = ctx
            .driver()
            .upgrade()
            .ok_or_else(|| io::Error::other("runtime driver missing"))?;
        let fd = driver
            .borrow_mut()
            .register_files(vec![RegisterFd::Owned(owned)])
            .map_err(driver_err)
            .and_then(|mut fds| {
                fds.pop()
                    .ok_or_else(|| io::Error::other("register_files returned empty"))
            })?;
        Ok(Self { fd })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }
}

impl Drop for SocketToken {
    fn drop(&mut self) {
        if let Some(ctx) = crate::runtime::context::try_current()
            && let Some(driver) = ctx.driver().upgrade()
        {
            let _ = driver.borrow_mut().unregister_files(vec![self.fd]);
        }
    }
}

pub struct InnerSocket {
    token: SocketToken,
    local_addr: Option<SocketAddr>,
}

impl InnerSocket {
    pub fn new(handle: RawHandle, local_addr: Option<SocketAddr>) -> io::Result<Self> {
        Ok(Self {
            token: SocketToken::new(handle)?,
            local_addr,
        })
    }

    pub fn fd(&self) -> IoFd {
        self.token.fd()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr
            .ok_or_else(|| io::Error::other("local addr is unavailable for this socket"))
    }
}
