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

pub struct SocketToken<'rt, 'reg> {
    fd: IoFd,
    owner_worker_id: usize,
    ctx: Ctx<'rt, 'reg>,
}

impl<'rt, 'reg> SocketToken<'rt, 'reg> {
    pub(crate) fn new(ctx: Ctx<'rt, 'reg>, handle: RawHandle) -> Result<Self> {
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

impl<'rt, 'reg> Drop for SocketToken<'rt, 'reg> {
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

pub trait SocketTokenPtr<'rt, 'reg>: Deref<Target = SocketToken<'rt, 'reg>> + Clone
where
    'reg: 'rt,
{
    fn new_ptr(token: SocketToken<'rt, 'reg>) -> Self;
}

impl<'rt, 'reg> SocketTokenPtr<'rt, 'reg> for Rc<SocketToken<'rt, 'reg>> {
    fn new_ptr(token: SocketToken<'rt, 'reg>) -> Self {
        Rc::new(token)
    }
}

impl<'rt, 'reg> SocketTokenPtr<'rt, 'reg> for Arc<SocketToken<'rt, 'reg>> {
    fn new_ptr(token: SocketToken<'rt, 'reg>) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<'rt, 'reg, P: SocketTokenPtr<'rt, 'reg>>
where
    'reg: 'rt,
{
    token: P,
    local_addr: Option<SocketAddr>,
    marker: PhantomData<(&'rt (), &'reg ())>,
}

impl<'rt, 'reg, P: SocketTokenPtr<'rt, 'reg>> InnerSocket<'rt, 'reg, P>
where
    'reg: 'rt,
{
    pub fn new(
        ctx: Ctx<'rt, 'reg>,
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
