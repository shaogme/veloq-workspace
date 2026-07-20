#[cfg(all(target_os = "windows", not(feature = "loom")))]
mod windows;
#[cfg(all(target_os = "windows", not(feature = "loom")))]
pub use windows::{wait_on_address, wait_on_address_timeout, wake_all_by_address, wake_by_address};

#[cfg(all(any(target_os = "linux", target_os = "android"), not(feature = "loom")))]
mod linux;
#[cfg(all(any(target_os = "linux", target_os = "android"), not(feature = "loom")))]
pub use linux::{wait_on_address, wait_on_address_timeout, wake_all_by_address, wake_by_address};

#[cfg(feature = "loom")]
mod loom;
#[cfg(feature = "loom")]
pub use loom::{wait_on_address, wake_all_by_address, wake_by_address};
