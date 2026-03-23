use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;

use veloq_driver::RawHandle;
use veloq_driver::Socket;
#[cfg(windows)]
use veloq_driver::driver::Driver;
use veloq_driver::op::IoFd;

// ============================================================================
// InnerSocket (RAII Wrapper)
// ============================================================================

#[cfg(windows)]
struct RegisteredFd {
    fd: IoFd,
    driver_id: usize,
}

#[cfg(windows)]
impl RegisteredFd {
    fn try_register(handle: RawHandle) -> Option<Self> {
        let ctx = crate::runtime::context::try_current()?;
        let driver = ctx.driver().upgrade()?;
        let driver_id = driver.borrow().driver_id();
        let fd = driver
            .borrow_mut()
            .register_files(&[handle])
            .ok()?
            .into_iter()
            .next()?;
        if !matches!(fd, IoFd::Fixed(_)) {
            return None;
        }
        Some(Self { fd, driver_id })
    }
}

#[cfg(windows)]
impl Drop for RegisteredFd {
    fn drop(&mut self) {
        if let Some(ctx) = crate::runtime::context::try_current()
            && let Some(driver) = ctx.driver().upgrade()
        {
            let mut driver = driver.borrow_mut();
            if driver.driver_id() == self.driver_id {
                let _ = driver.unregister_files(vec![self.fd]);
            }
        }
    }
}

pub struct InnerSocket {
    raw: RawHandle,
    #[cfg(windows)]
    registered: Option<RegisteredFd>,
}

impl Drop for InnerSocket {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            if let Some(ctx) = crate::runtime::context::try_current()
                && let Some(driver) = ctx.driver().upgrade()
            {
                // Socket teardown must clear the RIO actor for both TCP and UDP
                // to avoid stale actor state when handle values are reused.
                driver.borrow_mut().shutdown_actor(self.raw);
            }
            let _ = self.registered.take();
        }
        let _ = unsafe { Socket::from_raw(self.raw) };
    }
}

impl InnerSocket {
    pub fn new(handle: RawHandle) -> Self {
        Self {
            raw: handle,
            #[cfg(windows)]
            registered: RegisteredFd::try_register(handle),
        }
    }

    pub fn fd(&self) -> IoFd {
        #[cfg(windows)]
        {
            if let Some(registered) = &self.registered {
                return registered.fd;
            }
        }
        IoFd::Raw(self.raw)
    }

    pub fn raw(&self) -> RawHandle {
        self.raw
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.raw)) };
        socket.local_addr()
    }

    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.raw)) };
        socket.connect(addr)
    }
}
