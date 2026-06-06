use crate::IocpHandle;
use crate::error::{IocpError, IocpResult};
use crate::win32::Overlapped;
use std::io;

/// A wrapper for the Windows OVERLAPPED structure with additional metadata.
#[repr(C)]
pub struct OverlappedEntry {
    /// The underlying Windows Overlapped structure.
    pub(crate) inner: Overlapped,
    /// User-defined data associated with the operation.
    pub(crate) user_data: usize,
    /// Generation count for slot validation.
    pub(crate) generation: u32,
    /// Whether the operation is currently in-flight in the kernel.
    pub(crate) in_flight: bool,
    /// Result of an offloaded blocking operation.
    pub(crate) blocking_result: Option<IocpResult<usize>>,
    /// Resolved handle captured during submission to avoid re-resolving Fixed fd on hot paths.
    pub(crate) resolved_handle: Option<IocpHandle>,
}

impl OverlappedEntry {
    /// Creates a new `OverlappedEntry` with the given user data.
    pub(crate) fn new(user_data: usize) -> Self {
        Self {
            inner: Overlapped::zeroed(),
            user_data,
            generation: 0,
            in_flight: false,
            blocking_result: None,
            resolved_handle: None,
        }
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(0)
    }
}

pub(crate) unsafe fn store_blocking_result(overlapped: usize, result: io::Result<usize>) {
    let overlapped = overlapped as *mut Overlapped;
    // SAFETY: `OverlappedEntry` is `repr(C)` and `inner` is its first field,
    // so the overlapped pointer is the same as the struct pointer.
    let entry = unsafe { &mut *(overlapped as *mut OverlappedEntry) };
    entry.blocking_result =
        Some(result.map_err(|e| {
            IocpError::Win32.io_report("iocp.driver.inner.blocking_completion.store", e)
        }));
}

pub(crate) unsafe fn clear_blocking_result(overlapped: usize) {
    let overlapped = overlapped as *mut Overlapped;
    // SAFETY: `OverlappedEntry` is `repr(C)` and `inner` is its first field,
    // so the overlapped pointer is the same as the struct pointer.
    let entry = unsafe { &mut *(overlapped as *mut OverlappedEntry) };
    entry.blocking_result = None;
}

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}
