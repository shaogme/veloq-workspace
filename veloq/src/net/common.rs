use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error};
use crate::runtime::context::{RuntimeContext, submit_control_task};
use veloq_driver_native::driver::{Driver, RegisterFd};
use veloq_driver_native::op::IoFd;
use veloq_driver_native::{OwnedRawHandle, RawHandle};

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub struct SocketToken<'ctx> {
    fd: IoFd,
    owner_worker_id: usize,
    ctx: RuntimeContext<'ctx>,
}

impl<'ctx> SocketToken<'ctx> {
    pub(crate) fn new(ctx: RuntimeContext<'ctx>, handle: RawHandle) -> VeloqResult<Self> {
        if !handle.borrow().is_socket() {
            return Err(from_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket registration requires socket handle",
            )));
        }

        // SAFETY: caller transfers ownership via RawHandle created from OwnedRawHandle::into_raw.
        let owned = unsafe { OwnedRawHandle::from_raw_owned(handle) };
        let fd = ctx.driver(|driver| {
            driver
                .register_files(vec![RegisterFd::Owned(owned)])
                .map_err(from_driver_report)
                .and_then(|mut fds| {
                    fds.pop().ok_or_else(|| {
                        from_io_error(io::Error::other("register_files returned empty"))
                    })
                })
        })?;
        Ok(Self {
            fd,
            owner_worker_id: ctx.scope.worker_id(),
            ctx,
        })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }
}

impl<'ctx> Drop for SocketToken<'ctx> {
    fn drop(&mut self) {
        let current_worker_id = self.ctx.scope.worker_id();
        if current_worker_id == self.owner_worker_id {
            let ctx = self.ctx.scope.shared().context_tls.get();
            let mut driver = ctx.extra.driver.borrow_mut();
            let _ = driver.unregister_files(vec![self.fd]);
        } else {
            submit_control_task(self.ctx.scope.shared(), self.owner_worker_id, self.fd);
        }
    }
}

// ============================================================================
// SocketTokenPtr Trait
// ============================================================================

pub trait SocketTokenPtr<'ctx>: Deref<Target = SocketToken<'ctx>> + Clone {
    fn new_ptr(token: SocketToken<'ctx>) -> Self;
}

impl<'ctx> SocketTokenPtr<'ctx> for Rc<SocketToken<'ctx>> {
    fn new_ptr(token: SocketToken<'ctx>) -> Self {
        Rc::new(token)
    }
}

impl<'ctx> SocketTokenPtr<'ctx> for Arc<SocketToken<'ctx>> {
    fn new_ptr(token: SocketToken<'ctx>) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<'ctx, P: SocketTokenPtr<'ctx>> {
    token: P,
    local_addr: Option<SocketAddr>,
    _marker: std::marker::PhantomData<&'ctx ()>,
}

impl<'ctx, P: SocketTokenPtr<'ctx>> InnerSocket<'ctx, P> {
    pub fn new(
        ctx: RuntimeContext<'ctx>,
        handle: RawHandle,
        local_addr: Option<SocketAddr>,
    ) -> VeloqResult<Self> {
        Ok(Self {
            token: P::new_ptr(SocketToken::new(ctx, handle)?),
            local_addr,
            _marker: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn fd(&self) -> IoFd {
        self.token.fd()
    }

    pub fn owner_worker_id(&self) -> usize {
        self.token.owner_worker_id
    }

    pub fn local_addr(&self) -> VeloqResult<SocketAddr> {
        self.local_addr.ok_or_else(|| {
            from_io_error(io::Error::other(
                "local addr is unavailable for this socket",
            ))
        })
    }
}
