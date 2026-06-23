#[cfg(unix)]
use libc::pthread_setspecific;
#[cfg(windows)]
use windows_sys::Win32::System::Threading::FlsSetValue;

#[cfg(windows)]
pub(crate) type RawKey = u32;
#[cfg(unix)]
pub(crate) type RawKey = libc::pthread_key_t;

/// Helper to check if a pointer is the reentrancy sentinel.
///
/// Since pointers allocated via `Box` or `Node` are aligned, their addresses are never 1
/// (or any odd address in standard environments). Using `1 as *mut T` is a highly safe,
/// unaligned, non-null value that avoids any undefined behavior from pointer casting or
/// alignment checks on modern architectures.
#[inline(always)]
pub(crate) fn is_sentinel<T>(ptr: *const T) -> bool {
    ptr as usize == 1
}

/// Helper to construct the reentrancy sentinel pointer.
#[inline(always)]
pub(crate) fn sentinel_ptr<T>() -> *mut T {
    1 as *mut T
}

/// RAII helper to clean up the TLS sentinel value and reset it to null if initialization or modification fails/panics.
pub(crate) struct ResetGuard {
    pub(crate) key: RawKey,
    pub(crate) active: bool,
}

impl ResetGuard {
    #[inline(always)]
    pub(crate) fn new(key: RawKey) -> Self {
        Self { key, active: true }
    }

    #[inline(always)]
    pub(crate) fn cancel(mut self) {
        self.active = false;
    }
}

impl Drop for ResetGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if self.active {
            #[cfg(windows)]
            unsafe {
                FlsSetValue(self.key, core::ptr::null_mut());
            }
            #[cfg(unix)]
            unsafe {
                pthread_setspecific(self.key, core::ptr::null_mut());
            }
        }
    }
}
