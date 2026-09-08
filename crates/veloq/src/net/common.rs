use veloq_std::{
    marker::PhantomData,
    net::SocketAddr,
    ops::Deref,
    ptr::null_mut,
    rc::Rc,
    sync::{Arc, Mutex},
};

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

struct StashedBox {
    ptr: *mut (),
    drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for StashedBox {}
unsafe impl Sync for StashedBox {}

impl Drop for StashedBox {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { (self.drop_fn)(self.ptr) };
            self.ptr = null_mut();
        }
    }
}

pub struct SocketToken<'rt> {
    fd: IoFd,
    owner_worker_id: usize,
    ctx: Ctx<'rt>,
    accept_stash: Mutex<Option<StashedBox>>,
    recv_stash: Mutex<Option<StashedBox>>,
}

impl<'rt> SocketToken<'rt> {
    pub(crate) fn new(ctx: Ctx<'rt>, handle: RawHandle) -> Result<Self> {
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
            accept_stash: Mutex::new(None),
            recv_stash: Mutex::new(None),
        })
    }

    #[inline]
    pub(crate) fn fd(&self) -> IoFd {
        self.fd
    }

    pub(crate) fn stash_accept<T>(&self, val: T) {
        unsafe fn drop_ptr<T>(ptr: *mut ()) {
            let _ = unsafe { Box::from_raw(ptr as *mut T) };
        }

        let ptr = Box::into_raw(Box::new(val)) as *mut ();
        let stashed = StashedBox {
            ptr,
            drop_fn: drop_ptr::<T>,
        };
        let mut guard = self.accept_stash.lock();
        *guard = Some(stashed);
    }

    pub(crate) fn take_accept<T>(&self) -> Option<T> {
        let mut guard = self.accept_stash.lock();
        if let Some(mut stashed) = guard.take() {
            let ptr = stashed.ptr;
            stashed.ptr = null_mut();
            let boxed = unsafe { Box::from_raw(ptr as *mut T) };
            Some(*boxed)
        } else {
            None
        }
    }

    pub(crate) fn has_stashed_accept(&self) -> bool {
        self.accept_stash.lock().is_some()
    }

    pub(crate) fn stash_recv<T>(&self, val: T) {
        unsafe fn drop_ptr<T>(ptr: *mut ()) {
            let _ = unsafe { Box::from_raw(ptr as *mut T) };
        }

        let ptr = Box::into_raw(Box::new(val)) as *mut ();
        let stashed = StashedBox {
            ptr,
            drop_fn: drop_ptr::<T>,
        };
        let mut guard = self.recv_stash.lock();
        *guard = Some(stashed);
    }

    pub(crate) fn take_recv<T>(&self) -> Option<T> {
        let mut guard = self.recv_stash.lock();
        if let Some(mut stashed) = guard.take() {
            let ptr = stashed.ptr;
            stashed.ptr = null_mut();
            let boxed = unsafe { Box::from_raw(ptr as *mut T) };
            Some(*boxed)
        } else {
            None
        }
    }
}

impl<'rt> Drop for SocketToken<'rt> {
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

pub trait SocketTokenPtr<'rt>: Deref<Target = SocketToken<'rt>> + Clone {
    fn new_ptr(token: SocketToken<'rt>) -> Self;
}

impl<'rt> SocketTokenPtr<'rt> for Rc<SocketToken<'rt>> {
    fn new_ptr(token: SocketToken<'rt>) -> Self {
        Rc::new(token)
    }
}

impl<'rt> SocketTokenPtr<'rt> for Arc<SocketToken<'rt>> {
    fn new_ptr(token: SocketToken<'rt>) -> Self {
        Arc::new(token)
    }
}

#[derive(Clone)]
pub struct InnerSocket<'rt, P: SocketTokenPtr<'rt>> {
    token: P,
    local_addr: Option<SocketAddr>,
    marker: PhantomData<&'rt ()>,
}

impl<'rt, P: SocketTokenPtr<'rt>> InnerSocket<'rt, P> {
    pub fn new(ctx: Ctx<'rt>, handle: RawHandle, local_addr: Option<SocketAddr>) -> Result<Self> {
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

    #[inline]
    pub fn token(&self) -> &SocketToken<'rt> {
        &self.token
    }

    pub fn owner_worker_id(&self) -> usize {
        self.token.owner_worker_id
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.local_addr
            .ok_or(NetError::LocalAddrUnavailable)
            .trans()
    }

    pub async fn close_async(self) -> Result<()> {
        drop(self.token);
        Ok(())
    }
}
