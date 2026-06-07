use diagweave::prelude::*;
use std::io;
use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError};
use windows_sys::Win32::Networking::WinSock::SOCKET;
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::config::{BorrowedRawHandle, IoFd, IocpAssociation, IocpHandle, RegisteredHandle};
use crate::error::{IocpError, IocpResult};
use crate::ext::{LpfnAcceptEx, LpfnConnectEx};
use crate::op::{KernelRef, OverlappedEntry};
use crate::win32::{IoCompletionPort, Overlapped};

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
    handle: BorrowedRawHandle<'_>,
    buf: *mut u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> IocpResult<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: ReadFile is called with valid parameters.
    let ret = unsafe {
        ReadFile(
            handle.raw().as_handle(),
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
            return Err(IocpError::Submission
                .io_report("ReadFile", io::Error::from_raw_os_error(err as i32)));
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
    handle: BorrowedRawHandle<'_>,
    buf: *const u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> IocpResult<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: WriteFile is called with valid parameters.
    let ret = unsafe {
        WriteFile(
            handle.raw().as_handle(),
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
            return Err(IocpError::Submission
                .io_report("WriteFile", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for ConnectEx.
///
/// # Safety
///
/// The caller must ensure that all pointers and the socket handle are valid.
pub(crate) unsafe fn iocp_submit_connect_ex(args: ConnectExArgs) -> IocpResult<SubmissionResult> {
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
            return Err(IocpError::Submission
                .io_report("ConnectEx", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// Safe wrapper for AcceptEx.
///
/// # Safety
///
/// The caller must ensure that all pointers and the socket handles are valid.
pub(crate) unsafe fn iocp_submit_accept_ex(args: AcceptExArgs) -> IocpResult<SubmissionResult> {
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
            return Err(IocpError::Submission
                .io_report("AcceptEx", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

// ============================================================================
// Helper Functions
// ============================================================================

pub(crate) fn resolve_fd_borrowed<'a>(
    fd: &'a IoFd,
    registered_files: &'a [Option<RegisteredHandle>],
    file_generations: &[u64],
) -> IocpResult<BorrowedRawHandle<'a>> {
    let idx = fd.fixed_index();
    let Some(&generation) = file_generations.get(idx as usize) else {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds");
    };

    if generation != fd.generation() {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("current_generation", generation)
            .attach_note("stale registered file descriptor generation");
    }

    if let Some(Some(h)) = registered_files.get(idx as usize) {
        Ok(h.as_borrowed())
    } else {
        IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("invalid registered file descriptor")
    }
}

pub(crate) fn resolve_fd_handle(
    fd: &IoFd,
    registered_files: &[Option<RegisteredHandle>],
    file_generations: &[u64],
) -> IocpResult<IocpHandle> {
    resolve_fd_borrowed(fd, registered_files, file_generations).map(|h| h.raw())
}

pub(crate) fn resolve_registered_raw_file<'a>(
    raw: IocpHandle,
    registered_files: &'a [Option<RegisteredHandle>],
    file_generations: &[u64],
) -> IocpResult<(IoFd, BorrowedRawHandle<'a>)> {
    if !raw.is_file() {
        return IocpError::InvalidInput
            .with_ctx("handle_raw", raw.as_handle() as usize)
            .attach_note("raw file I/O only accepts file handles");
    }

    for (idx, entry) in registered_files.iter().enumerate() {
        let Some(entry) = entry else {
            continue;
        };
        if entry.as_raw().raw() != raw {
            continue;
        }

        let Some(&generation) = file_generations.get(idx) else {
            return IocpError::ResolveFd
                .with_ctx("registered_index", idx)
                .with_ctx("handle_raw", raw.as_handle() as usize)
                .attach_note("registered file descriptor generation missing");
        };
        let fixed_index = u32::try_from(idx).map_err(|_| {
            IocpError::Internal
                .to_report()
                .with_ctx("registered_index", idx)
                .attach_note("registered file index exceeds IoFd range")
        })?;
        let fd = IoFd::fixed_with_generation(fixed_index, generation);
        return Ok((fd, entry.as_borrowed()));
    }

    IocpError::InvalidInput
        .with_ctx("handle_raw", raw.as_handle() as usize)
        .attach_note("raw file I/O requires the handle to be registered first")
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
    fd: &IoFd,
    handle: BorrowedRawHandle<'_>,
    port: &IoCompletionPort,
    iocp_associations: &mut [Option<IocpAssociation>],
) -> IocpResult<()> {
    let idx = fd.fixed_index() as usize;
    let Some(association) = iocp_associations.get_mut(idx) else {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds");
    };

    ensure_handle_iocp_association(handle, port, association)
}

fn ensure_handle_iocp_association(
    handle: BorrowedRawHandle<'_>,
    port: &IoCompletionPort,
    association: &mut Option<IocpAssociation>,
) -> IocpResult<()> {
    let port_raw = port.as_raw() as usize;
    let completion_key = 0;
    let requested = IocpAssociation::new(port_raw, completion_key);

    match *association {
        Some(existing) if existing == requested => return Ok(()),
        Some(existing) => {
            return IocpError::InvalidState
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("port_raw", port_raw)
                .with_ctx("completion_key", completion_key)
                .with_ctx("existing_port_raw", existing.port_raw)
                .with_ctx("existing_completion_key", existing.completion_key)
                .attach_note("handle already associated with a different IOCP context");
        }
        None => {}
    }

    // SAFETY: the handle is checked for validity by the caller or by resolve_fd.
    unsafe { port.associate(handle.raw().as_handle(), 0) }
        .attach_note("CreateIoCompletionPort association failed")?;
    *association = Some(requested);
    Ok(())
}

#[inline]
pub(crate) fn mark_header_in_flight(
    header: &mut OverlappedEntry,
    res: IocpResult<SubmissionResult>,
) -> IocpResult<SubmissionResult> {
    if matches!(res, Ok(SubmissionResult::Pending)) {
        header.in_flight = true;
    }
    res
}
