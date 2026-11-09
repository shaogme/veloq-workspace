#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use unix::Platform;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::Platform;
