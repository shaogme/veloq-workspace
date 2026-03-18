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
    /// Whether the operation is currently in-flight in the kernel.
    pub(crate) in_flight: bool,
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
            in_flight: false,
            blocking_result: None,
        }
    }

    /// Recovers the `user_data` from a raw pointer to the `OVERLAPPED` structure.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the pointer was originally obtained via
    /// `Slot<Initialized>::overlapped_ptr` or that it points to the `inner` field of
    /// a valid `OverlappedEntry`.
    pub(crate) unsafe fn user_data_from_ptr(ptr: *const OVERLAPPED) -> usize {
        // SAFETY: The `inner` field is at the start of `OverlappedEntry` due to `#[repr(C)]`.
        unsafe { (*(ptr as *const OverlappedEntry)).user_data }
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(0)
    }
}

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}
