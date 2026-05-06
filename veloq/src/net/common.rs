use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use veloq_driver::driver::{Driver, RegisterFd};
use veloq_driver::op::IoFd;
use veloq_driver::{OwnedRawHandle, RawHandle};

fn driver_err(err: error_stack::Report<veloq_driver::error::DriverErrorKind>) -> io::Error {
    io::Error::other(format!("{err:#}"))
}

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub struct SocketToken {
    fd: IoFd,
    owner_worker_id: usize,
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
        let driver = ctx.driver();
        let fd = driver
            .borrow_mut()
            .register_files(vec![RegisterFd::Owned(owned)])
            .map_err(driver_err)
            .and_then(|mut fds| {
                fds.pop()
                    .ok_or_else(|| io::Error::other("register_files returned empty"))
            })?;
        Ok(Self {
            fd,
            owner_worker_id: veloq_runtime::runtime::current_worker_id(),
        })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }
}

impl Drop for SocketToken {
    fn drop(&mut self) {
        let Some(ctx) = crate::runtime::context::try_current() else {
            return;
        };

        debug_assert_eq!(
            veloq_runtime::runtime::current_worker_id(),
            self.owner_worker_id
        );
        let _ = ctx.driver().borrow_mut().unregister_files(vec![self.fd]);
    }
}

// ============================================================================
// SocketTokenPtr Trait
// ============================================================================

pub trait SocketTokenPtr: Deref<Target = SocketToken> + Clone {
    fn new_ptr(token: SocketToken) -> Self;
}

impl SocketTokenPtr for Rc<SocketToken> {
    fn new_ptr(token: SocketToken) -> Self {
        Rc::new(token)
    }
}

impl SocketTokenPtr for Arc<SocketToken> {
    fn new_ptr(token: SocketToken) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<P> {
    token: P,
    local_addr: Option<SocketAddr>,
}

impl<P: SocketTokenPtr> InnerSocket<P> {
    pub fn new(handle: RawHandle, local_addr: Option<SocketAddr>) -> io::Result<Self> {
        Ok(Self {
            token: P::new_ptr(SocketToken::new(handle)?),
            local_addr,
        })
    }

    #[inline]
    pub fn fd(&self) -> IoFd {
        self.token.fd()
    }

    pub fn owner_worker_id(&self) -> usize {
        self.token.owner_worker_id
    }

    pub async fn ensure_affinity(&self) -> io::Result<()> {
        veloq_runtime::task::ensure_current_task_affinity(self.token.owner_worker_id).await;
        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr
            .ok_or_else(|| io::Error::other("local addr is unavailable for this socket"))
    }
}
