use std::io;
use windows_sys::Win32::Foundation::HANDLE;

use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::config::IoFd;

// ============================================================================
// Submission Result
// ============================================================================

pub(crate) enum SubmissionResult {
    Pending,
    PostToQueue,
    Offload(veloq_blocking::BlockingTask),
    Timer(std::time::Duration),
}

// ============================================================================
// Helper Functions
// ============================================================================

pub(crate) fn resolve_fd(fd: IoFd, registered_files: &[Option<HANDLE>]) -> io::Result<HANDLE> {
    match fd {
        IoFd::Raw(h) => Ok(h.handle as HANDLE),
        IoFd::Fixed(idx) => {
            if let Some(Some(h)) = registered_files.get(idx as usize) {
                Ok(*h)
            } else {
                Err(io_msg(
                    IocpErrorContext::ResolveFd,
                    format!("invalid registered file descriptor: fd={fd:?}, idx={idx}"),
                ))
            }
        }
    }
}

/// Associates a handle with an IOCP.
///
/// # Safety
///
/// The caller must ensure that `handle` is a valid file/socket handle.
pub(crate) unsafe fn ensure_iocp_association(
    handle: HANDLE,
    port: &crate::common::IoCompletionPort,
    detail: impl Into<String>,
) -> io::Result<()> {
    // SAFETY: the handle is checked for validity by the caller or by resolve_fd.
    unsafe { port.associate(handle, 0) }
        .map_err(|e| io_error(IocpErrorContext::Submission, e, detail))
}
