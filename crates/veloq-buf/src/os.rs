#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use linux::Platform;

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub(crate) use windows::Platform;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
compile_error!("veloq-buf only supports Linux and Windows");
