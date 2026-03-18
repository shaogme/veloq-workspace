use std::io;
use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, GetLastError, HANDLE};
use windows_sys::Win32::System::IO::CreateIoCompletionPort;

use crate::config::IoFd;
use crate::common::{IocpErrorContext, io_error, io_msg};

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

pub(crate) fn ensure_iocp_association(
    handle: HANDLE,
    port: HANDLE,
    detail: impl Into<String>,
) -> io::Result<()> {
    // SAFETY: CreateIoCompletionPort is a safe Win32 API to associate a handle with an IOCP.
    let assoc = unsafe { CreateIoCompletionPort(handle, port, 0, 0) };
    if assoc.is_null() {
        // SAFETY: Calling GetLastError to get the reason for failure.
        let err = unsafe { GetLastError() } as i32;
        // Windows returns ERROR_INVALID_PARAMETER when trying to re-associate
        // a handle that is already bound to an IOCP.
        if err == ERROR_INVALID_PARAMETER as i32 {
            return Ok(());
        }
        return Err(io_error(
            IocpErrorContext::Submission,
            io::Error::from_raw_os_error(err),
            detail,
        ));
    }
    Ok(())
}
