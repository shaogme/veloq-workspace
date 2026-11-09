#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Platform};
#[cfg(windows)]
pub use windows::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Platform};
