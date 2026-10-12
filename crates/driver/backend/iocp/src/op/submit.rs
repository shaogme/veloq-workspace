mod file;
mod net;

use diagweave::prelude::*;
use veloq_blocking::BlockingTask;
use veloq_std::{ffi::c_void, io, num::NonZeroUsize, string::ToString, time::Duration};
use windows_sys::Win32::{
    Foundation::{ERROR_IO_PENDING, GetLastError},
    Networking::WinSock::{SOCKADDR, SOCKET},
    Storage::FileSystem::{ReadFile, WriteFile},
    System::IO::OVERLAPPED,
};

use crate::{
    config::{BorrowedRawHandle, IoFd, IocpAssociation, IocpHandle, RawHandle, RegisteredSlot},
    error::{IocpError, IocpResult},
    ext::{LpfnAcceptEx, LpfnConnectEx},
    op::{
        AcceptMultiPayload, AcceptPayload, Close, Connect, Fallocate, Fsync, KernelRef, OpSend,
        OverlappedEntry, ReadFixed, Recv, RecvMultiPayload, RecvProvidedPayload, SendToPayload,
        SubmitContext, SyncFileRange, Timeout, UdpConnect, UdpRecvMultiPayload, UdpSend, Wakeup,
        WriteFixed,
    },
    win32::{IoCompletionPort, Overlapped},
};

pub(crate) use file::{
    completion_cleanup_close_file, submit_close, submit_fallocate, submit_fallocate_raw,
    submit_fsync, submit_fsync_raw, submit_open, submit_read_fixed, submit_read_raw,
    submit_sync_range, submit_sync_range_raw, submit_write_fixed, submit_write_raw,
};
pub(crate) use net::{
    completion_cleanup_close_socket, on_complete_accept, on_complete_accept_multi,
    on_complete_connect, on_complete_recv_provided, on_complete_udp_connect,
    orphan_cleanup_accept_multi, submit_accept, submit_accept_multi, submit_connect, submit_recv,
    submit_recv_multi, submit_recv_provided, submit_send, submit_send_to, submit_udp_connect,
    submit_udp_recv_multi, submit_udp_send,
};

pub(crate) enum SubmissionResult {
    Pending,
    PostToQueue,
    Offload(BlockingTask),
    Timer(Duration),
}

pub(crate) struct ConnectExArgs {
    pub(crate) connect_ex: LpfnConnectEx,
    pub(crate) s: SOCKET,
    pub(crate) name: *const SOCKADDR,
    pub(crate) namelen: i32,
    pub(crate) lp_send_buffer: *const c_void,
    pub(crate) dw_send_data_length: u32,
    pub(crate) lp_dw_bytes_sent: *mut u32,
    pub(crate) lp_overlapped: *mut Overlapped,
}

pub(crate) struct AcceptExArgs {
    pub(crate) accept_ex: LpfnAcceptEx,
    pub(crate) s_listen_socket: SOCKET,
    pub(crate) s_accept_socket: SOCKET,
    pub(crate) lp_output_buffer: *mut c_void,
    pub(crate) dw_receive_data_length: u32,
    pub(crate) dw_local_address_length: u32,
    pub(crate) dw_remote_address_length: u32,
    pub(crate) lp_dw_bytes_received: *mut u32,
    pub(crate) lp_overlapped: *mut Overlapped,
}

/// `ReadFile` 的安全封装。
/// Safe wrapper for `ReadFile`.
///
/// # Safety
///
/// 调用方必须保证 handle、buf 和 overlapped 指针均有效。
/// The caller must ensure that the handle, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_read(
    handle: BorrowedRawHandle<'_>,
    buf: *mut u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> IocpResult<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: 调用 ReadFile 时所有参数均有效。
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
        // SAFETY: 可安全调用 GetLastError 获取系统错误。
        // SAFETY: GetLastError is safe to call to retrieve the system error.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(IocpError::Submission
                .io_report("ReadFile", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// `WriteFile` 的安全封装。
/// Safe wrapper for `WriteFile`.
///
/// # Safety
///
/// 调用方必须保证 handle、buf 和 overlapped 指针均有效。
/// The caller must ensure that the handle, buf, and overlapped pointers are valid.
pub(crate) unsafe fn iocp_submit_write(
    handle: BorrowedRawHandle<'_>,
    buf: *const u8,
    len: u32,
    overlapped: *mut Overlapped,
) -> IocpResult<SubmissionResult> {
    let mut bytes = 0;
    // SAFETY: 调用 WriteFile 时所有参数均有效。
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
        // SAFETY: 可安全调用 GetLastError 获取系统错误。
        // SAFETY: GetLastError is safe to call to retrieve the system error.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(IocpError::Submission
                .io_report("WriteFile", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// `ConnectEx` 的安全封装。
/// Safe wrapper for `ConnectEx`.
///
/// # Safety
///
/// 调用方必须保证所有指针和 socket 句柄均有效。
/// The caller must ensure that all pointers and the socket handle are valid.
pub(crate) unsafe fn iocp_submit_connect_ex(args: ConnectExArgs) -> IocpResult<SubmissionResult> {
    // SAFETY: 调用 connect_ex 时所有参数均有效。
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
        // SAFETY: 可安全调用 GetLastError 获取系统错误。
        // SAFETY: GetLastError is safe to call to retrieve the system error.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(IocpError::Submission
                .io_report("ConnectEx", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// `AcceptEx` 的安全封装。
/// Safe wrapper for `AcceptEx`.
///
/// # Safety
///
/// 调用方必须保证所有指针和 socket 句柄均有效。
/// The caller must ensure that all pointers and socket handles are valid.
pub(crate) unsafe fn iocp_submit_accept_ex(args: AcceptExArgs) -> IocpResult<SubmissionResult> {
    // SAFETY: 调用 accept_ex 时所有参数均有效。
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
        // SAFETY: 可安全调用 GetLastError 获取系统错误。
        // SAFETY: GetLastError is safe to call to retrieve the system error.
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(IocpError::Submission
                .io_report("AcceptEx", io::Error::from_raw_os_error(err as i32)));
        }
    }
    Ok(SubmissionResult::Pending)
}

/// 持有 `raw` 的已占用注册槽索引。
/// Index of the occupied registry slot holding `raw`.
fn find_slot_by_handle(raw: IocpHandle, registered_slots: &[RegisteredSlot]) -> Option<usize> {
    registered_slots.iter().position(|slot| {
        slot.handle
            .as_ref()
            .is_some_and(|entry| entry.as_raw().raw() == raw)
    })
}

/// 描述符所指注册槽的索引。
/// Index of the registry slot named by a descriptor.
///
/// [`IoFd::Registered`] 按索引解析，并校验 generation，使过期描述符报错而非误指向复用该槽的新句柄。
/// [`IoFd::Registered`] resolves by index and checks its generation so stale descriptors fail instead of silently naming a replacement handle.
///
/// [`IoFd::Direct`] 没有索引，因此按句柄身份匹配，复杂度为 O(n)。注册表按需增长，不会耗尽；扫描用于找到保存句柄状态及 completion-port 关联的槽，确保每个句柄只关联一次。
/// [`IoFd::Direct`] has no index and is matched by handle identity in O(n). The registry grows on demand, so the scan maps caller-created direct descriptors to slots holding per-handle state and the one-time completion-port association.
pub(crate) fn locate_registered_slot(
    fd: IoFd,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<usize> {
    match fd {
        IoFd::Registered { index, generation } => {
            let idx = index as usize;
            let Some(slot) = registered_slots.get(idx) else {
                return IocpError::ResolveFd
                    .with_ctx("fd", fd.to_string())
                    .attach_note("registered file descriptor index out of bounds");
            };

            if slot.generation != generation {
                return IocpError::ResolveFd
                    .with_ctx("fd", fd.to_string())
                    .with_ctx("current_generation", slot.generation)
                    .attach_note("stale registered file descriptor generation");
            }

            if slot.handle.is_none() {
                return IocpError::ResolveFd
                    .with_ctx("fd", fd.to_string())
                    .attach_note("invalid registered file descriptor");
            }

            Ok(idx)
        }
        IoFd::Direct(raw) => find_slot_by_handle(raw, registered_slots).ok_or_else(|| {
            IocpError::ResolveFd
                .to_report()
                .with_ctx("fd", fd.to_string())
                .attach_note("direct descriptors require the handle to be registered first")
        }),
        IoFd::OwnedDirect { .. } => Err(IocpError::ResolveFd
            .to_report()
            .with_ctx("fd", fd.to_string())
            .attach_note("owned direct descriptors are unsupported by the IOCP backend")),
    }
}

pub(crate) fn resolve_fd_handle(
    fd: &IoFd,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<RawHandle> {
    let idx = locate_registered_slot(*fd, registered_slots)?;
    Ok(registered_slots[idx]
        .handle
        .as_ref()
        .expect("located slot is occupied")
        .as_raw())
}

pub(crate) fn resolve_registered_raw_file(
    raw: IocpHandle,
    registered_slots: &[RegisteredSlot],
) -> IocpResult<(IoFd, RawHandle)> {
    if !raw.is_file() {
        return IocpError::InvalidInput
            .with_ctx("handle_raw", raw.as_handle() as usize)
            .attach_note("raw file I/O only accepts file handles");
    }

    let Some(idx) = find_slot_by_handle(raw, registered_slots) else {
        return IocpError::InvalidInput
            .with_ctx("handle_raw", raw.as_handle() as usize)
            .attach_note("raw file I/O requires the handle to be registered first");
    };

    let slot = &registered_slots[idx];
    let fixed_index = u32::try_from(idx).map_err(|_| {
        IocpError::Internal
            .to_report()
            .with_ctx("registered_index", idx)
            .attach_note("registered file index exceeds IoFd range")
    })?;
    let fd = IoFd::fixed_with_generation(fixed_index, slot.generation);
    Ok((
        fd,
        slot.handle
            .as_ref()
            .expect("located slot is occupied")
            .as_raw(),
    ))
}

/// 从提交上下文中解包 [`KernelRef<T>`] 和 slot 的 overlapped 指针。
/// Unpacks a [`KernelRef<T>`] and slot overlapped pointer from the submit context.
///
/// # Safety
///
/// 调用期间，调用方必须保证 `payload.user` 和 `ctx.overlapped` 均可有效地进行可变访问。
/// The caller must ensure `payload.user` and `ctx.overlapped` both permit valid mutable access during the call.
pub(crate) unsafe fn unpack_kernel_ref<T>(
    payload: &mut KernelRef<T>,
    overlapped: *mut Overlapped,
) -> IocpResult<(&mut T, &mut Overlapped)> {
    // SAFETY: 此有效性由调用方保证。
    // SAFETY: validity is guaranteed by the caller.
    let val = unsafe { payload.user.as_mut()? };
    // SAFETY: 此有效性由调用方保证。
    // SAFETY: validity is guaranteed by the caller.
    let overlapped = unsafe { &mut *overlapped };
    Ok((val, overlapped))
}

/// 将句柄关联到 IOCP。
/// Associates a handle with an IOCP.
pub(crate) fn ensure_iocp_association(
    fd: &IoFd,
    handle: RawHandle,
    port: &IoCompletionPort,
    registered_slots: &mut [RegisteredSlot],
) -> IocpResult<()> {
    let idx = locate_registered_slot(*fd, registered_slots)?;
    let slot = &mut registered_slots[idx];
    ensure_handle_iocp_association(handle.raw(), port, &mut slot.association)
}

fn ensure_handle_iocp_association(
    handle: IocpHandle,
    port: &IoCompletionPort,
    association: &mut Option<IocpAssociation>,
) -> IocpResult<()> {
    let port_raw_value = port.as_raw() as usize;
    let Some(port_raw) = NonZeroUsize::new(port_raw_value) else {
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
                .with_ctx("existing_completion_key", existing.completion_key())
                .attach_note("handle already associated with a different IOCP context");
        }
        None => {}
    }

    // SAFETY: 调用方或 resolve_fd 已校验句柄有效性。
    // SAFETY: the caller or resolve_fd has validated the handle.
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
        pub(crate) unsafe fn $fn_name(payload: &$payload) -> Option<IoFd> {
            // SAFETY: 调用方保证 payload 指针有效。
            // SAFETY: the caller guarantees that the payload pointer is valid.
            unsafe { payload.user.as_ref().ok().map(|user| user.fd) }
        }
    };
    ($fn_name:ident, $payload:ty, no_fd) => {
        pub(crate) unsafe fn $fn_name(_payload: &$payload) -> Option<IoFd> {
            None
        }
    };
}

impl_get_fd!(get_fd_read_fixed, KernelRef<ReadFixed>, direct_fd);
impl_get_fd!(get_fd_write_fixed, KernelRef<WriteFixed>, direct_fd);
impl_get_fd!(get_fd_recv, KernelRef<Recv>, direct_fd);
impl_get_fd!(get_fd_send, KernelRef<OpSend>, direct_fd);
impl_get_fd!(get_fd_udp_send, KernelRef<UdpSend>, direct_fd);
impl_get_fd!(get_fd_connect, KernelRef<Connect>, direct_fd);
impl_get_fd!(get_fd_udp_connect, KernelRef<UdpConnect>, direct_fd);
impl_get_fd!(get_fd_accept, AcceptPayload, direct_fd);
impl_get_fd!(get_fd_accept_multi, AcceptMultiPayload, direct_fd);
impl_get_fd!(get_fd_send_to, SendToPayload, direct_fd);
impl_get_fd!(get_fd_recv_provided, RecvProvidedPayload, direct_fd);
impl_get_fd!(get_fd_recv_multi, RecvMultiPayload, direct_fd);
impl_get_fd!(get_fd_udp_recv_multi, UdpRecvMultiPayload, direct_fd);

impl_get_fd!(get_fd_close, KernelRef<Close>, direct_fd);
impl_get_fd!(get_fd_fsync, KernelRef<Fsync>, direct_fd);
impl_get_fd!(get_fd_sync_range, KernelRef<SyncFileRange>, direct_fd);
impl_get_fd!(get_fd_fallocate, KernelRef<Fallocate>, direct_fd);

// ============================================================================
// 其他操作 / Other operations
// ============================================================================

/// # Safety
///
/// 调用方必须保证 header、payload 和 ctx 均有效。
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_wakeup(
    _header: &mut OverlappedEntry,
    _payload: &mut KernelRef<Wakeup>,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

/// # Safety
///
/// 调用方必须保证 header、payload 和 ctx 均有效。
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_timeout(
    _header: &mut OverlappedEntry,
    payload: &mut KernelRef<Timeout>,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: 解引用 KernelRef 中的用户指针。
    // SAFETY: Dereferences the user pointer in KernelRef.
    let u = unsafe { payload.user.as_ref()? };
    Ok(SubmissionResult::Timer(u.duration))
}
