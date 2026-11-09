#[cfg(any(target_os = "linux", target_os = "android"))]
mod unix;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
mod generic;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use unix::Platform;

#[cfg(target_os = "windows")]
pub(crate) use windows::Platform;

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
pub(crate) use generic::Platform;

pub type RawOsError = i32;
