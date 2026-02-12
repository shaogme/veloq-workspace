#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{alloc_huge_pages, alloc_pages, free_pages};

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub use windows::{alloc_huge_pages, alloc_pages, free_pages};

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
compile_error!("veloq-buf only supports Linux and Windows");
