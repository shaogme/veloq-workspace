#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "windows")]
pub use windows::get_system_time_seed;

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use linux::get_system_time_seed;
