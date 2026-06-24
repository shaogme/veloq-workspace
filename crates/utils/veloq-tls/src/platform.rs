pub(crate) trait PlatformKey: Copy + Send + Sync {
    /// # Safety
    ///
    /// The caller must ensure that the key is valid.
    unsafe fn free(self);

    /// # Safety
    ///
    /// The caller must ensure that the key is valid.
    unsafe fn get_value<T>(self) -> *mut T;

    /// # Safety
    ///
    /// The caller must ensure that the key is valid and the pointer points to valid memory or is a sentinel/null pointer.
    unsafe fn set_value<T>(self, ptr: *mut T) -> Result<(), i32>;
}

#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "windows")]
pub(crate) use windows::{AtomicKey, Key};

#[cfg(not(target_os = "windows"))]
mod linux;

#[cfg(not(target_os = "windows"))]
pub(crate) use linux::{AtomicKey, Key};
