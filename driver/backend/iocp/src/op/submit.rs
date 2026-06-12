mod file;
mod net;

use diagweave::prelude::*;
use std::io;
use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError};
use windows_sys::Win32::Networking::WinSock::SOCKET;
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::config::{BorrowedRawHandle, IoFd, IocpAssociation, IocpHandle, RegisteredSlot};
use crate::error::{IocpError, IocpResult};
use crate::ext::{LpfnAcceptEx, LpfnConnectEx};
use crate::op::{
    AcceptPayload, Close, Connect, Fallocate, Fsync, KernelRef, OpSend, OverlappedEntry, Recv,
    SendToPayload, SubmitContext, SyncFileRange, Timeout, UdpConnect, UdpRecv, UdpRecvFromPayload,
    UdpSend, Wakeup,
};
use crate::win32::{IoCompletionPort, Overlapped};

pub(crate) use file::{
    completion_cleanup_close_file, submit_close, submit_fallocate, submit_fallocate_raw,
    submit_fsync, submit_fsync_raw, submit_open, submit_read_fixed, submit_read_raw,
    submit_sync_range, submit_sync_range_raw, submit_write_fixed, submit_write_raw,
};
pub(crate) use net::{
    completion_cleanup_close_socket, on_complete_accept, on_complete_connect,
    on_complete_udp_connect, on_complete_udp_recv_from, submit_accept, submit_connect, submit_recv,
    submit_send, submit_send_to, submit_udp_connect, submit_udp_recv, submit_udp_recv_from,
    submit_udp_send,
};

pub(crate) enum SubmissionResult {
    Pending,
    PostToQueue,
    Offload(veloq_blocking::BlockingTask),
    Timer(std::time::Duration),
}

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

pub(crate) fn resolve_fd_handle(
    fd: &IoFd,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<IocpHandle> {
    let idx = fd.fixed_index();
    let Some(slot) = registered_slots.get(idx as usize) else {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds");
    };

    if slot.generation != fd.generation() {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("current_generation", slot.generation)
            .attach_note("stale registered file descriptor generation");
    }

    if let Some(handle) = slot.handle.as_ref() {
        Ok(handle.as_raw().raw())
    } else {
        IocpError::ResolveFd
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("invalid registered file descriptor")
    }
}

pub(crate) fn resolve_registered_raw_file(
    raw: IocpHandle,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<(IoFd, IocpHandle)> {
    if !raw.is_file() {
        return IocpError::InvalidInput
            .with_ctx("handle_raw", raw.as_handle() as usize)
            .attach_note("raw file I/O only accepts file handles");
    }

    for (idx, slot) in registered_slots.iter().enumerate() {
        let Some(entry) = slot.handle.as_ref() else {
            continue;
        };
        if entry.as_raw().raw() != raw {
            continue;
        }

        let fixed_index = u32::try_from(idx).map_err(|_| {
            IocpError::Internal
                .to_report()
                .with_ctx("registered_index", idx)
                .attach_note("registered file index exceeds IoFd range")
        })?;
        let fd = IoFd::fixed_with_generation(fixed_index, slot.generation);
        return Ok((fd, entry.as_raw().raw()));
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
) -> IocpResult<(&mut T, &mut Overlapped)> {
    // SAFETY: guaranteed by the caller.
    let val = unsafe { payload.user.as_mut()? };
    // SAFETY: guaranteed by the caller.
    let overlapped = unsafe { &mut *overlapped };
    Ok((val, overlapped))
}

/// Associates a handle with an IOCP.
pub(crate) fn ensure_iocp_association(
    fd: &IoFd,
    handle: IocpHandle,
    port: &IoCompletionPort,
    registered_slots: &mut [RegisteredSlot],
) -> IocpResult<()> {
    let idx = fd.fixed_index() as usize;
    let Some(slot) = registered_slots.get_mut(idx) else {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds");
    };

    if slot.generation != fd.generation() {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("current_generation", slot.generation)
            .attach_note("stale registered file descriptor generation");
    }

    if slot.handle.is_none() {
        return IocpError::ResolveFd
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .attach_note("invalid registered file descriptor");
    }

    ensure_handle_iocp_association(handle, port, &mut slot.association)
}

fn ensure_handle_iocp_association(
    handle: IocpHandle,
    port: &IoCompletionPort,
    association: &mut Option<IocpAssociation>,
) -> IocpResult<()> {
    let port_raw_value = port.as_raw() as usize;
    let Some(port_raw) = std::num::NonZeroUsize::new(port_raw_value) else {
        return IocpError::InvalidState
            .with_ctx("handle_raw", handle.as_handle() as usize)
            .with_ctx("port_raw", port_raw_value)
            .attach_note("IOCP port handle is null");
    };
    let completion_key = 0;
    let requested = IocpAssociation::new(port_raw, completion_key);

    match *association {
        Some(existing) if existing == requested => return Ok(()),
        Some(existing) => {
            return IocpError::InvalidState
                .with_ctx("handle_raw", handle.as_handle() as usize)
                .with_ctx("port_raw", port_raw_value)
                .with_ctx("completion_key", completion_key)
                .with_ctx("existing_port_raw", existing.port_raw())
                .with_ctx("existing_completion_key", existing.completion_key)
                .attach_note("handle already associated with a different IOCP context");
        }
        None => {}
    }

    // SAFETY: the handle is checked for validity by the caller or by resolve_fd.
    unsafe { port.associate(handle.as_handle(), 0) }
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

macro_rules! impl_get_fd {
    ($fn_name:ident, $payload:ty, direct_fd) => {
        pub(crate) unsafe fn $fn_name(payload: &$payload) -> Option<crate::config::IoFd> {
            // SAFETY: the caller guarantees the payload pointer is valid.
            unsafe { payload.user.as_ref().ok().map(|user| user.fd) }
        }
    };
    ($fn_name:ident, $payload:ty, no_fd) => {
        pub(crate) unsafe fn $fn_name(_payload: &$payload) -> Option<crate::config::IoFd> {
            None
        }
    };
}

impl_get_fd!(
    get_fd_read_fixed,
    KernelRef<crate::op::ReadFixed>,
    direct_fd
);
impl_get_fd!(
    get_fd_write_fixed,
    KernelRef<crate::op::WriteFixed>,
    direct_fd
);
impl_get_fd!(get_fd_recv, KernelRef<Recv>, direct_fd);
impl_get_fd!(get_fd_send, KernelRef<OpSend>, direct_fd);
impl_get_fd!(get_fd_udp_recv, KernelRef<UdpRecv>, direct_fd);
impl_get_fd!(get_fd_udp_send, KernelRef<UdpSend>, direct_fd);
impl_get_fd!(get_fd_connect, KernelRef<Connect>, direct_fd);
impl_get_fd!(get_fd_udp_connect, KernelRef<UdpConnect>, direct_fd);
impl_get_fd!(get_fd_accept, AcceptPayload, direct_fd);
impl_get_fd!(get_fd_send_to, SendToPayload, direct_fd);
impl_get_fd!(get_fd_udp_recv_from, UdpRecvFromPayload, direct_fd);

impl_get_fd!(get_fd_close, KernelRef<Close>, direct_fd);
impl_get_fd!(get_fd_fsync, KernelRef<Fsync>, direct_fd);
impl_get_fd!(get_fd_sync_range, KernelRef<SyncFileRange>, direct_fd);
impl_get_fd!(get_fd_fallocate, KernelRef<Fallocate>, direct_fd);

// ============================================================================
// Other Operations
// ============================================================================

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_wakeup(
    _header: &mut crate::op::OverlappedEntry,
    _payload: &mut KernelRef<Wakeup>,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_timeout(
    _header: &mut crate::op::OverlappedEntry,
    payload: &mut KernelRef<Timeout>,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: Dereferencing the user pointer in KernelRef.
    let u = unsafe { payload.user.as_ref()? };
    Ok(SubmissionResult::Timer(u.duration))
}
