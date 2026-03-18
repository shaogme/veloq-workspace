use std::io;
use std::ptr;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED, PostQueuedCompletionStatus,
};

/// A safe wrapper around a Win32 HANDLE that ensures it is closed when dropped.
#[derive(Debug)]
pub struct OwnedHandle(HANDLE);

impl OwnedHandle {
    /// Creates a new `OwnedHandle` from a raw HANDLE.
    ///
    /// # Safety
    ///
    /// The handle must be valid and owned by the caller.
    pub unsafe fn from_raw(handle: HANDLE) -> Self {
        Self(handle)
    }

    /// Returns the raw HANDLE.
    pub fn as_raw(&self) -> HANDLE {
        self.0
    }

    /// Checks if the handle is valid.
    pub fn is_valid(&self) -> bool {
        !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if self.is_valid() {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}

/// A safe wrapper for an I/O Completion Port.
pub struct IoCompletionPort(OwnedHandle);

impl IoCompletionPort {
    /// Creates a new, unconnected I/O Completion Port.
    pub fn new(threads: u32) -> io::Result<Self> {
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, threads) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(OwnedHandle(handle)))
    }

    /// Associates a handle with this I/O Completion Port.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `handle` is a valid file/socket handle.
    pub unsafe fn associate(&self, handle: HANDLE, completion_key: usize) -> io::Result<()> {
        let res = unsafe { CreateIoCompletionPort(handle, self.0.as_raw(), completion_key, 0) };
        if res.is_null() {
            let err = unsafe { GetLastError() };
            // Windows returns ERROR_INVALID_PARAMETER when trying to re-associate
            // a handle that is already bound to an IOCP.
            if err == windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        Ok(())
    }

    /// Posts a completion status to the port.
    ///
    /// # Safety
    ///
    /// If `overlapped` is not null, it must point to a valid `OVERLAPPED` structure
    /// that remains valid until the completion is retrieved.
    pub unsafe fn post(
        &self,
        bytes: u32,
        key: usize,
        overlapped: *mut OVERLAPPED,
    ) -> io::Result<()> {
        let res = unsafe { PostQueuedCompletionStatus(self.0.as_raw(), bytes, key, overlapped) };
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Retrieves a completion status from the port.
    pub fn get_status(&self, timeout_ms: u32) -> io::Result<CompletionStatus> {
        let mut bytes = 0;
        let mut key = 0;
        let mut overlapped = ptr::null_mut();

        let res = unsafe {
            GetQueuedCompletionStatus(
                self.0.as_raw(),
                &mut bytes,
                &mut key,
                &mut overlapped,
                timeout_ms,
            )
        };

        if res == 0 {
            let err = unsafe { GetLastError() };
            if overlapped.is_null() {
                if err == WAIT_TIMEOUT {
                    return Ok(CompletionStatus::Timeout);
                }
                return Err(io::Error::from_raw_os_error(err as i32));
            } else {
                // Operation failed but we got an overlapped pointer
                return Ok(CompletionStatus::Completed {
                    bytes,
                    key,
                    overlapped,
                    success: false,
                    error_code: Some(err),
                });
            }
        }

        Ok(CompletionStatus::Completed {
            bytes,
            key,
            overlapped,
            success: true,
            error_code: None,
        })
    }

    pub fn as_raw(&self) -> HANDLE {
        self.0.as_raw()
    }
}

pub enum CompletionStatus {
    Completed {
        bytes: u32,
        key: usize,
        overlapped: *mut OVERLAPPED,
        success: bool,
        error_code: Option<u32>,
    },
    Timeout,
}
