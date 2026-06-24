use crate::TlsErrorKind;

pub(crate) trait Platform {
    type Key: Copy + Send + Sync;

    fn alloc_key<T>() -> Result<Self::Key, TlsErrorKind>;

    /// # Safety
    ///
    /// The caller must ensure that the key is valid.
    unsafe fn free_key(key: Self::Key);

    /// # Safety
    ///
    /// The caller must ensure that the key is valid.
    unsafe fn get_value<T>(key: Self::Key) -> *mut T;

    /// # Safety
    ///
    /// The caller must ensure that the key is valid and the pointer points to valid memory or is a sentinel/null pointer.
    unsafe fn set_value<T>(key: Self::Key, ptr: *mut T) -> Result<(), i32>;
}

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub(crate) use windows::PlatformImpl;

#[cfg(not(target_os = "windows"))]
mod linux;
#[cfg(not(target_os = "windows"))]
pub(crate) use linux::PlatformImpl;
