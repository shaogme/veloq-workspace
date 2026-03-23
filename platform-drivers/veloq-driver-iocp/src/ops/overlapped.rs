use crate::RawHandle;
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
    pub(crate) blocking_result: Option<io::Result<usize>>,
    /// Resolved raw handle captured during submission to avoid re-resolving Fixed fd on hot paths.
    pub(crate) resolved_handle: Option<RawHandle>,
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

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}
