use std::{marker::PhantomData, net::SocketAddr, ops::Deref, rc::Rc, sync::Arc};

use crate::{
    error::Result,
    net::error::NetError,
    runtime::context::{Ctx, submit_control_task},
};
use veloq_driver_native::{
    OwnedRawHandle, RawHandle,
    driver::{Driver, RegisterFd},
    op::IoFd,
};

use diagweave::prelude::*;

// ============================================================================
// SocketToken + InnerSocket (RAII Wrapper)
// ============================================================================

pub struct SocketToken<'a, 'ctx> {
    fd: IoFd,
    owner_worker_id: usize,
    ctx: Ctx<'a, 'ctx>,
}

impl<'a, 'ctx> SocketToken<'a, 'ctx> {
    pub(crate) fn new(ctx: Ctx<'a, 'ctx>, handle: RawHandle) -> Result<Self> {
        if !handle.borrow().is_socket() {
            return NetError::InvalidSocketHandle.trans();
        }

        // SAFETY: caller transfers ownership via RawHandle created from OwnedRawHandle::into_raw.
        let owned = unsafe { OwnedRawHandle::from_raw_owned(handle) };
        let fd = ctx.driver(|mut driver| {
            driver
                .register_files(vec![RegisterFd::Owned(owned)])
                .trans()
                .and_then(|mut fds| fds.pop().ok_or(NetError::RegistrationEmpty).trans())
        })?;
        Ok(Self {
            fd,
            owner_worker_id: ctx.runtime_ctx.worker_id(),
            ctx,
        })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }
}

impl<'a, 'ctx> Drop for SocketToken<'a, 'ctx> {
    fn drop(&mut self) {
        let current_worker_id = self.ctx.runtime_ctx.worker_id();
        if current_worker_id == self.owner_worker_id {
            self.ctx.runtime_ctx.shared().extra_tls.with(|extra| {
                let mut driver = extra.driver.borrow_mut();
                let _ = driver.unregister_files(vec![self.fd]);
            });
        } else {
            submit_control_task(self.ctx.runtime_ctx.shared(), self.owner_worker_id, self.fd);
        }
    }
}

// ============================================================================
// SocketTokenPtr Trait
// ============================================================================

pub trait SocketTokenPtr<'a, 'ctx>: Deref<Target = SocketToken<'a, 'ctx>> + Clone
where
    'ctx: 'a,
{
    fn new_ptr(token: SocketToken<'a, 'ctx>) -> Self;
}

impl<'a, 'ctx> SocketTokenPtr<'a, 'ctx> for Rc<SocketToken<'a, 'ctx>> {
    fn new_ptr(token: SocketToken<'a, 'ctx>) -> Self {
        Rc::new(token)
    }
}

impl<'a, 'ctx> SocketTokenPtr<'a, 'ctx> for Arc<SocketToken<'a, 'ctx>> {
    fn new_ptr(token: SocketToken<'a, 'ctx>) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<'a, 'ctx, P: SocketTokenPtr<'a, 'ctx>>
where
    'ctx: 'a,
{
    token: P,
    local_addr: Option<SocketAddr>,
    marker: PhantomData<(&'a (), &'ctx ())>,
}

impl<'a, 'ctx, P: SocketTokenPtr<'a, 'ctx>> InnerSocket<'a, 'ctx, P>
where
    'ctx: 'a,
{
    pub fn new(
        ctx: Ctx<'a, 'ctx>,
        handle: RawHandle,
        local_addr: Option<SocketAddr>,
    ) -> Result<Self> {
        Ok(Self {
            token: P::new_ptr(SocketToken::new(ctx, handle)?),
            local_addr,
            marker: PhantomData,
        })
    }

    #[inline]
    pub fn fd(&self) -> IoFd {
        self.token.fd()
    }

    pub fn owner_worker_id(&self) -> usize {
        self.token.owner_worker_id
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.local_addr
            .ok_or(NetError::LocalAddrUnavailable)
            .trans()
    }
}
