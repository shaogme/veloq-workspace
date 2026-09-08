//! Common OS error-checking and retry helpers (`cvt`, `cvt_r`, etc.).

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub use unix::*;

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub use windows::*;
