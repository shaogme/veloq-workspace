use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{Result as VeloqResult, from_driver_report, from_io_error};
use crate::runtime::context::submit_control_task;
use veloq_driver_native::driver::{Driver, RegisterFd};
use veloq_driver_native::op::IoFd;
use veloq_driver_native::{OwnedRawHandle, RawHandle};
use veloq_runtime::runtime::shared::RuntimeShared;

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub struct SocketToken<'a> {
    fd: IoFd,
    owner_worker_id: usize,
    shared: &'a RuntimeShared<crate::runtime::context::WorkerState>,
}

impl<'a> SocketToken<'a> {
    pub(crate) fn new(
        ctx: &'a crate::runtime::context::RuntimeContext,
        handle: RawHandle,
    ) -> VeloqResult<Self> {
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
            shared: ctx.scope.shared(),
        })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }
}

impl<'a> Drop for SocketToken<'a> {
    fn drop(&mut self) {
        let current_worker_id = self.shared.worker_id();
        if current_worker_id == self.owner_worker_id {
            if let Some(tls) = self.shared.context_tls.get() {
                let extra = unsafe { &tls.as_ref().extra };
                if let Some(driver) = extra.driver.borrow_mut().as_mut() {
                    let _ = driver.unregister_files(vec![self.fd]);
                }
            }
        } else {
            submit_control_task(self.shared, self.owner_worker_id, self.fd);
        }
    }
}

// ============================================================================
// SocketTokenPtr Trait
// ============================================================================

pub trait SocketTokenPtr<'a>: Deref<Target = SocketToken<'a>> + Clone {
    fn new_ptr(token: SocketToken<'a>) -> Self;
}

impl<'a> SocketTokenPtr<'a> for Rc<SocketToken<'a>> {
    fn new_ptr(token: SocketToken<'a>) -> Self {
        Rc::new(token)
    }
}

impl<'a> SocketTokenPtr<'a> for Arc<SocketToken<'a>> {
    fn new_ptr(token: SocketToken<'a>) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<'a, P: SocketTokenPtr<'a>> {
    token: P,
    local_addr: Option<SocketAddr>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, P: SocketTokenPtr<'a>> InnerSocket<'a, P> {
    pub fn new(
        ctx: &'a crate::runtime::context::RuntimeContext,
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
