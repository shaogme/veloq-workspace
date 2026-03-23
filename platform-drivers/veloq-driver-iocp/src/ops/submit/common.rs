use std::io;
use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError, HANDLE};
use windows_sys::Win32::Networking::WinSock::{
    SOCKET, SOCKET_ERROR, WSABUF, WSAGetLastError, WSARecv, WSASend,
};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::config::{IoFd, RawHandle};
use crate::ext::{LpfnAcceptEx, LpfnConnectEx};
use crate::ops::KernelRef;
use crate::win32::Overlapped;

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

pub(crate) struct ConnectExArgs {
    pub(crate) connect_ex: LpfnConnectEx,
    pub(crate) s: SOCKET,
    pub(crate) name: *const windows_sys::Win32::Networking::WinSock::SOCKADDR,
    pub(crate) namelen: i32,
    pub(crate) lp_send_buffer: *const std::ffi::c_void,
    pub(crate) dw_send_data_length: u32,
    pub(crate) lp_dw_bytes_sent: *mut u32,
    pub(crate) lp_overlapped: *mut Overlapped,
}

pub(crate) struct AcceptExArgs {
    pub(crate) accept_ex: LpfnAcceptEx,
    pub(crate) s_listen_socket: SOCKET,
    pub(crate) s_accept_socket: SOCKET,
    pub(crate) lp_output_buffer: *mut std::ffi::c_void,
    pub(crate) dw_receive_data_length: u32,
    pub(crate) dw_local_address_length: u32,
    pub(crate) dw_remote_address_length: u32,
    pub(crate) lp_dw_bytes_received: *mut u32,
    pub(crate) lp_overlapped: *mut Overlapped,
}

/// Safe wrapper for ReadFile.
///
/// # Safety
///
/// The caller must ensure that the handle, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_read(
    handle: HANDLE,
    buf: *mut u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> io::Result<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: ReadFile is called with valid parameters.
    let ret = unsafe {
        ReadFile(
            handle,
            buf as _,
            len,
            &mut bytes,
            overlapped as *mut OVERLAPPED,
        )
    };
    if ret == 0 {
        // SAFETY: GetLastError is safe to call.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for WriteFile.
///
/// # Safety
///
/// The caller must ensure that the handle, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_write(
    handle: HANDLE,
    buf: *const u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> io::Result<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: WriteFile is called with valid parameters.
    let ret = unsafe {
        WriteFile(
            handle,
            buf as _,
            len,
            &mut bytes,
            overlapped as *mut OVERLAPPED,
        )
    };
    if ret == 0 {
        // SAFETY: GetLastError is safe to call.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for WSARecv (socket overlapped receive).
///
/// # Safety
///
/// The caller must ensure that the socket, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_socket_recv(
    socket: SOCKET,
    buf: *mut u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> io::Result<SubmissionResult> {
    let wsabuf = WSABUF { len, buf };
    let mut bytes = 0u32;
    let mut flags = 0u32;
    // SAFETY: WSARecv is called with valid pointers and overlapped state.
    let ret = unsafe {
        WSARecv(
            socket,
            &wsabuf,
            1,
            &mut bytes,
            &mut flags,
            overlapped as *mut OVERLAPPED,
            None,
        )
    };
    if ret == SOCKET_ERROR {
        // SAFETY: WSAGetLastError is safe to call.
        let err = unsafe { WSAGetLastError() };
        if err != ERROR_IO_PENDING as i32 {
            return Err(io::Error::from_raw_os_error(err));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for WSASend (socket overlapped send).
///
/// # Safety
///
/// The caller must ensure that the socket, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_socket_send(
    socket: SOCKET,
    buf: *const u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> io::Result<SubmissionResult> {
    let wsabuf = WSABUF {
        len,
        buf: buf as *mut u8,
    };
    let mut bytes = 0u32;
    // SAFETY: WSASend is called with valid pointers and overlapped state.
    let ret = unsafe {
        WSASend(
            socket,
            &wsabuf,
            1,
            &mut bytes,
            0,
            overlapped as *mut OVERLAPPED,
            None,
        )
    };
    if ret == SOCKET_ERROR {
        // SAFETY: WSAGetLastError is safe to call.
        let err = unsafe { WSAGetLastError() };
        if err != ERROR_IO_PENDING as i32 {
            return Err(io::Error::from_raw_os_error(err));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for ConnectEx.
///
/// # Safety
///
/// The caller must ensure that all pointers and the socket handle are valid.
pub(crate) unsafe fn iocp_submit_connect_ex(args: ConnectExArgs) -> io::Result<SubmissionResult> {
    // SAFETY: connect_ex is called with valid parameters.
    let ret = unsafe {
        (args.connect_ex)(
            args.s,
            args.name,
            args.namelen,
            args.lp_send_buffer,
            args.dw_send_data_length,
            args.lp_dw_bytes_sent,
            args.lp_overlapped as *mut OVERLAPPED,
        )
    };
    if ret == 0 {
        // SAFETY: GetLastError is safe to call.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for AcceptEx.
///
/// # Safety
///
/// The caller must ensure that all pointers and the socket handles are valid.
pub(crate) unsafe fn iocp_submit_accept_ex(args: AcceptExArgs) -> io::Result<SubmissionResult> {
    // SAFETY: accept_ex is called with valid parameters.
    let ret = unsafe {
        (args.accept_ex)(
            args.s_listen_socket,
            args.s_accept_socket,
            args.lp_output_buffer,
            args.dw_receive_data_length,
            args.dw_local_address_length,
            args.dw_remote_address_length,
            args.lp_dw_bytes_received,
            args.lp_overlapped as *mut OVERLAPPED,
        )
    };
    if ret == 0 {
        // SAFETY: GetLastError is safe to call.
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

pub(crate) fn resolve_fd(
    fd: IoFd,
    registered_files: &[Option<RawHandle>],
) -> io::Result<RawHandle> {
    match fd {
        IoFd::Raw(h) => Ok(h),
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

/// Unpacks a [`KernelRef<T>`] and slot overlapped pointer from submit context.
///
/// # Safety
///
/// The caller must ensure `payload.user` and `ctx.overlapped` are both valid for
/// mutable access during the call.
pub(crate) unsafe fn unpack_kernel_ref<T>(
    payload: &mut KernelRef<T>,
    overlapped: *mut Overlapped,
) -> (&mut T, &mut Overlapped) {
    // SAFETY: guaranteed by the caller.
    let val = unsafe { payload.user.as_mut() };
    // SAFETY: guaranteed by the caller.
    let overlapped = unsafe { &mut *overlapped };
    (val, overlapped)
}

/// Associates a handle with an IOCP.
///
pub(crate) fn ensure_iocp_association(
    handle: HANDLE,
    port: &crate::win32::IoCompletionPort,
    detail: impl Into<String>,
) -> io::Result<()> {
    // SAFETY: the handle is checked for validity by the caller or by resolve_fd.
    unsafe { port.associate(handle, 0) }
        .map_err(|e| io_error(IocpErrorContext::Submission, e, detail))
}
