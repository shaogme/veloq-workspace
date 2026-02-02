#[cfg(windows)]
pub mod windows;

#[cfg(target_os = "windows")]
pub type SysBlockingOps = windows::BlockingOps;
#[cfg(target_os = "linux")]
pub type SysBlockingOps = ();
