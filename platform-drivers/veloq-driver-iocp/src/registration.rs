use super::inner::IocpDriver;
use crate::IoFd;
use crate::RawHandle;
use std::io;

impl IocpDriver {
    /// Registers a chunk of memory for RIO operations.
    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        self.rio_state.register_chunk(id, ptr, len)?;
        Ok(())
    }

    /// Shuts down the UDP buffer pool associated with the specified handle.
    pub fn shutdown_udp_pool(&mut self, handle: RawHandle) {
        self.rio_state
            .begin_udp_pool_shutdown_for_handle(handle.handle);
    }

    /// Registers a set of file/socket handles for use with the driver.
    pub(crate) fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
        let mut registered = Vec::with_capacity(files.len());
        for &handle in files {
            let idx = if let Some(idx) = self.free_slots.pop() {
                self.registered_files[idx] = Some(handle.handle);
                self.rio_state.clear_registered_rq(idx);
                idx
            } else {
                self.registered_files.push(Some(handle.handle));
                self.rio_state
                    .resize_registered_rqs(self.registered_files.len());
                self.registered_files.len() - 1
            };
            registered.push(IoFd::Fixed(idx as u32));
        }
        Ok(registered)
    }

    /// Unregisters a set of previously registered files.
    pub(crate) fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()> {
        for fd in files {
            if let IoFd::Fixed(idx) = fd {
                let idx = idx as usize;
                if idx < self.registered_files.len() && self.registered_files[idx].is_some() {
                    self.registered_files[idx] = None;
                    self.rio_state.clear_registered_rq(idx);
                    self.free_slots.push(idx);
                }
            }
        }
        Ok(())
    }
}
