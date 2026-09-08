#[cfg(not(windows))]
mod unix;

#[cfg(not(windows))]
pub use unix::*;

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub use windows::*;
