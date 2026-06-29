#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub use windows::{wait_on_address, wait_on_address_timeout, wake_all_by_address, wake_by_address};

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use linux::{wait_on_address, wait_on_address_timeout, wake_all_by_address, wake_by_address};
