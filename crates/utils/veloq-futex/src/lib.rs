#![no_std]
#![deny(warnings)]

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use linux::{FutexError, WaitOutcome, wait, wake};
