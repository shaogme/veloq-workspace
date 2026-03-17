use super::inner::WAKEUP_USER_DATA;
use super::port::CompletionPort;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use veloq_driver_core::driver::RemoteWaker;
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

/// A waker that posts a completion status to the port to wake up the event loop.
pub(crate) struct IocpWaker {
    pub(crate) port: Arc<CompletionPort>,
    pub(crate) is_notified: Arc<AtomicBool>,
}

impl RemoteWaker for IocpWaker {
    fn wake(&self) -> io::Result<()> {
        if self.is_notified.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_notified.swap(true, Ordering::AcqRel) {
            // SAFETY: `self.port.handle` is guaranteed to be a valid, open I/O completion port handle throughout the lifetime of the `IocpWaker`.
            let res = unsafe {
                PostQueuedCompletionStatus(
                    self.port.handle,
                    0,
                    WAKEUP_USER_DATA,
                    std::ptr::null_mut(),
                )
            };
            if res == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}
