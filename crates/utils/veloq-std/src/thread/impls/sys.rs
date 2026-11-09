#[cfg(all(any(target_os = "linux", target_os = "android"), not(feature = "loom")))]
mod linux;
#[cfg(feature = "loom")]
mod loom;
#[cfg(all(target_os = "windows", not(feature = "loom")))]
mod windows;

#[cfg(all(any(target_os = "linux", target_os = "android"), not(feature = "loom")))]
pub use linux::{RawJoinHandle, RawThreadError, System};
#[cfg(feature = "loom")]
pub use loom::{RawJoinHandle, RawThreadError, System};
#[cfg(all(target_os = "windows", not(feature = "loom")))]
pub use windows::{RawJoinHandle, RawThreadError, System};
