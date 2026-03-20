use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;

use veloq_driver::RawHandle;
use veloq_driver::Socket;

// ============================================================================
// InnerSocket (RAII Wrapper)
// ============================================================================

pub struct InnerSocket(pub(crate) RawHandle);

impl Drop for InnerSocket {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            if let Some(ctx) = crate::runtime::context::try_current()
                && let Some(driver) = ctx.driver().upgrade()
            {
                driver.borrow_mut().shutdown_udp_pool(self.0);
            }
        }
        let _ = unsafe { Socket::from_raw(self.0) };
    }
}

impl InnerSocket {
    pub fn new(handle: RawHandle) -> Self {
        Self(handle)
    }

    pub fn raw(&self) -> RawHandle {
        self.0
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.0)) };
        socket.local_addr()
    }

    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let socket = unsafe { ManuallyDrop::new(Socket::from_raw(self.0)) };
        socket.connect(addr)
    }
}
