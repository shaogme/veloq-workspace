//! Common OS error-checking and retry helpers (`cvt`, `cvt_r`, etc.).

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub(crate) use windows::Cvt;
