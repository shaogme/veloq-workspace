use windows_sys::Win32::Foundation::HANDLE;

/// A wrapper around a Windows I/O Completion Port handle.
pub struct CompletionPort {
    pub(crate) handle: HANDLE,
}

impl Drop for CompletionPort {
    fn drop(&mut self) {
        // SAFETY: The handle is owned by this `CompletionPort` and is guaranteed to be open and valid at this point of drop.
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

// SAFETY: The `CompletionPort` wraps a raw `HANDLE`, which is a thread-safe handle for completion port operations and can be safely sent between threads.
unsafe impl Send for CompletionPort {}
// SAFETY: The `CompletionPort` wraps a raw `HANDLE`, which is thread-safe to access concurrently for completion port operations.
unsafe impl Sync for CompletionPort {}
