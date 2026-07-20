pub(crate) trait SystermKey: Copy + Send + Sync {
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

#[cfg(feature = "loom")]
mod loom;

#[cfg(feature = "loom")]
pub(crate) use loom::{AtomicKey, Key};

#[cfg(all(not(feature = "loom"), target_os = "windows"))]
mod windows;

#[cfg(all(not(feature = "loom"), target_os = "windows"))]
pub(crate) use windows::{AtomicKey, Key};

#[cfg(all(not(feature = "loom"), not(target_os = "windows")))]
mod linux;

#[cfg(all(not(feature = "loom"), not(target_os = "windows")))]
pub(crate) use linux::{AtomicKey, Key};
