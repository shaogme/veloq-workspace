use std::io;
use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError, HANDLE};
use windows_sys::Win32::Networking::WinSock::SOCKET;
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::config::IoFd;
use crate::ext::{LpfnAcceptEx, LpfnConnectEx};

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
// FFI Wrappers
// ============================================================================

/// Safe wrapper for ReadFile.
pub(crate) unsafe fn iocp_submit_read(
    handle: HANDLE,
    buf: *mut u8,
    len: u32,
    overlapped: *mut OVERLAPPED,
) -> io::Result<SubmissionResult> {
    let mut bytes = 0;
    let ret = unsafe { ReadFile(handle, buf as _, len, &mut bytes, overlapped) };
    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for WriteFile.
pub(crate) unsafe fn iocp_submit_write(
    handle: HANDLE,
    buf: *const u8,
    len: u32,
    overlapped: *mut OVERLAPPED,
) -> io::Result<SubmissionResult> {
    let mut bytes = 0;
    let ret = unsafe { WriteFile(handle, buf as _, len, &mut bytes, overlapped) };
    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for ConnectEx.
pub(crate) unsafe fn iocp_submit_connect_ex(
    connect_ex: LpfnConnectEx,
    s: SOCKET,
    name: *const windows_sys::Win32::Networking::WinSock::SOCKADDR,
    namelen: i32,
    lp_send_buffer: *const std::ffi::c_void,
    dw_send_data_length: u32,
    lp_dw_bytes_sent: *mut u32,
    lp_overlapped: *mut OVERLAPPED,
) -> io::Result<SubmissionResult> {
    let ret = unsafe {
        connect_ex(
            s,
            name,
            namelen,
            lp_send_buffer,
            dw_send_data_length,
            lp_dw_bytes_sent,
            lp_overlapped,
        )
    };
    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for AcceptEx.
pub(crate) unsafe fn iocp_submit_accept_ex(
    accept_ex: LpfnAcceptEx,
    s_listen_socket: SOCKET,
    s_accept_socket: SOCKET,
    lp_output_buffer: *mut std::ffi::c_void,
    dw_receive_data_length: u32,
    dw_local_address_length: u32,
    dw_remote_address_length: u32,
    lp_dw_bytes_received: *mut u32,
    lp_overlapped: *mut OVERLAPPED,
) -> io::Result<SubmissionResult> {
    let ret = unsafe {
        accept_ex(
            s_listen_socket,
            s_accept_socket,
            lp_output_buffer,
            dw_receive_data_length,
            dw_local_address_length,
            dw_remote_address_length,
            lp_dw_bytes_received,
            lp_overlapped,
        )
    };
    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
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
