#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub(crate) use unix::Platform;
#[cfg(unix)]
pub use unix::{File, FileAttr, FilePermissions};

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub(crate) use windows::Platform;
#[cfg(windows)]
pub use windows::{File, FileAttr, FilePermissions};
