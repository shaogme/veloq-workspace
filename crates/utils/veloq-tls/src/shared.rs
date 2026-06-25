use crate::{Key, PlatformKey};

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
    pub(crate) key: Key,
    pub(crate) active: bool,
}

impl ResetGuard {
    #[inline(always)]
    pub(crate) fn new(key: Key) -> Self {
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
            unsafe {
                let _ = self.key.set_value::<()>(core::ptr::null_mut());
            }
        }
    }
}
