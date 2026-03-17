use super::inner::{IocpDriver, RIO_EVENT_KEY};
use super::op::OverlappedEntry;
use super::state::CloseMode;
use std::io;
use std::time::{Duration, Instant};
use tracing::debug;
use windows_sys::Win32::Foundation::{GetLastError, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::GetQueuedCompletionStatus;

impl IocpDriver {
    pub(crate) fn shutdown_ops(&mut self) -> usize {
        if self.shutting_down {
            return 0;
        }
        self.shutting_down = true;
        self.rio_state.begin_shutdown();

        let mut in_flight = Vec::new();
        for user_data in 0..self.ops.local.len() {
            if let Some(op) = self.ops.local.get(user_data)
                && matches!(
                    op.platform_data.lifecycle,
                    crate::state::OpLifecycle::InFlight
                )
            {
                in_flight.push(user_data);
            }
        }
        let count = in_flight.len();
        for user_data in in_flight {
            self.cancel_op_internal(user_data);
        }
        count
    }

    pub(crate) fn drain_pending_iocp(
        &mut self,
        pending_count: usize,
        timeout: Duration,
    ) -> io::Result<()> {
        if pending_count == 0 {
            return Ok(());
        }
        let mut drained = 0usize;
        let deadline = Instant::now() + timeout;

        while drained < pending_count {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "drain timed out"));
            }
            drained += self.poll_completion()?;
        }
        Ok(())
    }

    pub(crate) fn poll_completion(&mut self) -> io::Result<usize> {
        let mut bytes = 0;
        let mut key = 0;
        let mut overlapped = std::ptr::null_mut();

        // SAFETY: Waiting for a single completion during shutdown.
        let res = unsafe {
            GetQueuedCompletionStatus(self.port.handle, &mut bytes, &mut key, &mut overlapped, 10)
        };

        if key == RIO_EVENT_KEY {
            return self.rio_state.process_completions(
                &mut self.ops,
                &*self.registrar,
                &self.completion_events,
                &self.completion_table,
            );
        }

        if !overlapped.is_null() {
            // SAFETY: Accessing `user_data` from overlapped entry.
            let user_data = unsafe { (*(overlapped as *const OverlappedEntry)).user_data };
            self.process_completion(user_data, res, bytes);
            return Ok(1);
        }

        if res == 0 {
            // SAFETY: Checking for error if no completion received.
            let err = unsafe { GetLastError() };
            if err != WAIT_TIMEOUT {
                return Err(io::Error::from_raw_os_error(err as i32));
            }
        }
        Ok(0)
    }

    pub(crate) fn close_impl(&mut self, mode: CloseMode) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        let pending = self.shutdown_ops();
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout)?;
            self.rio_state.drain_outstanding_for(timeout)?;
        }
        self.closed = true;
        Ok(())
    }
}

impl Drop for IocpDriver {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        let _ = self.close_impl(CloseMode::Fast);
    }
}
