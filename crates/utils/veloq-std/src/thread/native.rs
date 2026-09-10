use super::ThreadId;

#[cfg(any(target_os = "linux", target_os = "android"))]
use libc::pthread_self;

#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Threading::GetCurrentThreadId;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn current_id() -> ThreadId {
    ThreadId(unsafe { pthread_self() as u64 })
}

#[cfg(target_os = "windows")]
pub(crate) fn current_id() -> ThreadId {
    ThreadId(unsafe { GetCurrentThreadId() } as u64)
}
