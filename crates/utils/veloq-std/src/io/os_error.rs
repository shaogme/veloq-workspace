pub type RawOsError = i32;

#[cfg(any(target_os = "linux", target_os = "android"))]
mod unix;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
mod generic;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use unix::{decode_error_kind, error_string, last_os_error};

#[cfg(target_os = "windows")]
pub use windows::{decode_error_kind, error_string, last_os_error};

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
pub use generic::{decode_error_kind, error_string, last_os_error};
