use std::io;
use windows_sys::Win32::System::IO::OVERLAPPED;

/// A wrapper for the Windows OVERLAPPED structure with additional metadata.
#[repr(C)]
pub struct OverlappedEntry {
    /// The underlying Windows OVERLAPPED structure.
    pub(crate) inner: OVERLAPPED,
    /// User-defined data associated with the operation.
    pub(crate) user_data: usize,
    /// Generation count for slot validation.
    pub(crate) generation: u32,
    /// Result of an offloaded blocking operation.
    pub(crate) blocking_result: Option<io::Result<usize>>,
}

impl OverlappedEntry {
    /// Creates a new `OverlappedEntry` with the given user data.
    pub(crate) fn new(user_data: usize) -> Self {
        Self {
            // SAFETY: OVERLAPPED can be safely zero-initialized.
            inner: unsafe { std::mem::zeroed() },
            user_data,
            generation: 0,
            blocking_result: None,
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
