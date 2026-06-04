use diagweave::DiagnosticResult;
use std::mem::ManuallyDrop;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use veloq_pod::{bytes_of_mut, from_bytes_mut};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET,
};

use crate::config::BorrowedRawHandle;
use crate::error::{IocpError, IocpResult, from_io_error};
use crate::ext::Extensions;
use crate::net::addr::{self, SockAddrStorage};
use crate::op::submit::common::{
    AcceptExArgs, ConnectExArgs, SubmissionResult, ensure_iocp_association, iocp_submit_accept_ex,
    iocp_submit_connect_ex, mark_header_in_flight, resolve_fd_borrowed, resolve_fd_handle,
    unpack_kernel_ref,
};
use crate::op::{
    ACCEPT_EX_ADDR_SECTION_LEN, AcceptPayload, Connect, KernelRef, OpSend, OverlappedEntry, Recv,
    SendToPayload, SubmitContext, UdpConnect, UdpRecv, UdpRecvFromPayload, UdpSend,
};
use crate::rio::{RioTarget, RioUdpRecvFromArgs};
use crate::win32::SafeSocket;

// ============================================================================
// Network Operations
// ============================================================================

fn with_borrowed_socket<T>(
    raw: SOCKET,
    f: impl FnOnce(&SafeSocket) -> IocpResult<T>,
) -> IocpResult<T> {
    let socket = ManuallyDrop::new(SafeSocket(raw));
    f(&socket)
}

pub(crate) fn submit_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Recv>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let fd = val.fd;
    let handle = resolve_fd_borrowed(&fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&fd, ctx.registered_files)?);
    let user_data = header.user_data;
    let generation = header.generation;
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv(
                RioTarget {
                    fd,
                    handle,
                    user_data,
                    generation,
                    buf_offset: val.buf_offset,
                },
                &mut val.buf,
                ctx.registrar,
            )
            .map_err(|e| {
                from_io_error(IocpError::Submission, "submit_recv", e).attach_note(format!(
                    "RIO recv submit failed: fd={:?}, user_data={}, generation={}",
                    val.fd, user_data, generation
                ))
            }),
    )
}

pub(crate) fn submit_udp_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecv>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let fd = val.fd;
    let handle = resolve_fd_borrowed(&fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&fd, ctx.registered_files)?);
    let user_data = header.user_data;
    let generation = header.generation;
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv(
                RioTarget {
                    fd,
                    handle,
                    user_data,
                    generation,
                    buf_offset: val.buf_offset,
                },
                &mut val.buf,
                ctx.registrar,
            )
            .map_err(|e| {
                from_io_error(IocpError::Submission, "submit_udp_recv", e).attach_note(format!(
                    "RIO udp_recv submit failed: fd={:?}, user_data={}, generation={}",
                    val.fd, header.user_data, header.generation
                ))
            }),
    )
}

pub(crate) fn submit_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<OpSend>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let handle = resolve_fd_borrowed(&val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&val.fd, ctx.registered_files)?);
    let user_data = header.user_data;
    let generation = header.generation;
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send(
                RioTarget {
                    fd: val.fd,
                    handle,
                    user_data,
                    generation,
                    buf_offset: val.buf_offset,
                },
                &val.buf,
                ctx.registrar,
            )
            .map_err(|e| {
                from_io_error(IocpError::Submission, "submit_send", e).attach_note(format!(
                    "RIO send submit failed: fd={:?}, user_data={}, generation={}",
                    val.fd, user_data, generation
                ))
            }),
    )
}

pub(crate) fn submit_udp_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpSend>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let handle = resolve_fd_borrowed(&val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&val.fd, ctx.registered_files)?);
    let user_data = header.user_data;
    let generation = header.generation;
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send(
                RioTarget {
                    fd: val.fd,
                    handle,
                    user_data,
                    generation,
                    buf_offset: val.buf_offset,
                },
                &val.buf,
                ctx.registrar,
            )
            .map_err(|e| {
                from_io_error(IocpError::Submission, "submit_udp_send", e).attach_note(format!(
                    "RIO udp_send submit failed: fd={:?}, user_data={}, generation={}",
                    val.fd, user_data, generation
                ))
            }),
    )
}

pub(crate) fn submit_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    let handle = resolve_fd_borrowed(&connect_op.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&connect_op.fd, ctx.registered_files)?);
    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_connect: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}",
            connect_op.fd,
            handle.raw().as_handle(),
            header.user_data,
            header.generation
        ),
    )?;
    ensure_socket_bound(handle, connect_op)?;

    let mut bytes_sent = 0;
    // SAFETY: iocp_submit_connect_ex is a safe wrapper for the WinSock extension.
    mark_header_in_flight(header, unsafe {
        iocp_submit_connect_ex(ConnectExArgs {
            connect_ex: ctx.ext.connect_ex,
            s: handle.raw().as_socket(),
            name: &connect_op.addr as *const _ as *const SOCKADDR,
            namelen: connect_op.addr_len as i32,
            lp_send_buffer: std::ptr::null(),
            dw_send_data_length: 0,
            lp_dw_bytes_sent: &mut bytes_sent,
            lp_overlapped: ctx.overlapped,
        })
    })
}

fn ensure_socket_bound(handle: BorrowedRawHandle<'_>, connect_op: &Connect) -> IocpResult<()> {
    let mut storage = SockAddrStorage::default();
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

    if with_borrowed_socket(handle.raw().as_socket(), |socket| {
        // SAFETY: storage and namelen are valid for this call.
        unsafe { socket.getsockname(&mut storage.0 as *mut _ as *mut SOCKADDR, &mut namelen) }
    })
    .is_ok()
    {
        let family = storage.family();
        let is_bound = if family == AF_INET {
            // SAFETY: storage and namelen are valid and initialized by getsockname.
            let buf = unsafe {
                std::slice::from_raw_parts(&storage.0 as *const _ as *const u8, namelen as usize)
            };
            addr::to_socket_addr(buf).is_ok_and(|a| a.port() != 0)
        } else if family == AF_INET6 {
            // SAFETY: storage and namelen are valid and initialized by getsockname.
            let buf = unsafe {
                std::slice::from_raw_parts(&storage.0 as *const _ as *const u8, namelen as usize)
            };
            addr::to_socket_addr(buf).is_ok_and(|a| a.port() != 0)
        } else {
            false
        };

        if is_bound {
            return Ok(());
        }
    }

    let family = connect_op.addr.family();
    let (storage, len) = if family == AF_INET {
        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);
        let s = addr::SockAddrIn::new(&addr);
        let mut storage = SockAddrStorage::default();
        let sin_ref = from_bytes_mut::<addr::SockAddrIn>(
            &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN>()],
        );
        *sin_ref = s;
        (storage, std::mem::size_of::<SOCKADDR_IN>() as i32)
    } else {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let s = addr::SockAddrIn6::new(&addr);
        let mut storage = SockAddrStorage::default();
        let sin6_ref = from_bytes_mut::<addr::SockAddrIn6>(
            &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN6>()],
        );
        *sin6_ref = s;
        (storage, std::mem::size_of::<SOCKADDR_IN6>() as i32)
    };
    with_borrowed_socket(handle.raw().as_socket(), |socket| {
        // SAFETY: storage is a valid SOCKADDR_STORAGE for this call.
        unsafe { socket.bind(&storage.0 as *const _ as *const SOCKADDR, len) }
    })
}

fn socket_family_from_handle(handle: BorrowedRawHandle<'_>) -> IocpResult<u16> {
    let mut storage = SockAddrStorage::default();
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
    with_borrowed_socket(handle.raw().as_socket(), |socket| {
        // SAFETY: storage and namelen are valid output pointers.
        unsafe { socket.getsockname(&mut storage.0 as *mut _ as *mut SOCKADDR, &mut namelen) }
    })?;
    match storage.family() {
        AF_INET | AF_INET6 => Ok(storage.family()),
        family => IocpError::InvalidInput.attach_note(format!(
            "unsupported listen socket family for accept: {family}",
        )),
    }
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_connect(
    header: &mut OverlappedEntry,
    _payload: &mut KernelRef<Connect>,
    result: usize,
    _ext: &Extensions,
) -> IocpResult<usize> {
    let raw_handle = header
        .resolved_handle
        .ok_or(IocpError::InvalidState)
        .attach_note("resolved handle missing for connect completion")?;
    with_borrowed_socket(raw_handle.as_socket(), |socket| {
        socket.setsockopt_empty(SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT)
    })?;
    Ok(result)
}

pub(crate) fn submit_udp_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpConnect>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    let handle = resolve_fd_borrowed(&connect_op.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&connect_op.fd, ctx.registered_files)?);
    with_borrowed_socket(handle.raw().as_socket(), |socket| {
        // SAFETY: address pointer/length are validated by caller and come from op payload.
        unsafe {
            socket.connect(
                &connect_op.addr as *const _ as *const SOCKADDR,
                connect_op.addr_len as i32,
            )
        }
    })?;
    Ok(SubmissionResult::PostToQueue)
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_udp_connect(
    _header: &mut OverlappedEntry,
    _payload: &mut KernelRef<UdpConnect>,
    result: usize,
    _ext: &Extensions,
) -> IocpResult<usize> {
    Ok(result)
}

pub(crate) fn submit_accept(
    header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);
    if payload.accept_socket.is_none() {
        let family = socket_family_from_handle(handle)?;
        let accept_socket = if family == AF_INET {
            crate::Socket::new_tcp_v4()
        } else {
            crate::Socket::new_tcp_v6()
        }
        .map(|s| s.into_owned_raw())
        .map_err(|e| {
            e.attach_note(format!(
                "submit_accept: create accept socket failed: listen=0x{:x}, family={}",
                handle.raw().as_handle() as usize,
                family
            ))
        })?;
        payload.accept_socket = Some(accept_socket);
    }
    let accept_socket = payload
        .accept_socket
        .as_ref()
        .ok_or(IocpError::InvalidState)
        .attach_note("accept socket not initialized")?;
    let accept_socket_raw = accept_socket.raw().as_socket();

    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_accept: associate listen socket failed: listen=0x{:x}, user_data={}, generation={}",
            handle.raw().as_handle() as usize,
            header.user_data,
            header.generation
        ),
    )?;

    let split = ACCEPT_EX_ADDR_SECTION_LEN;
    let mut bytes_received = 0;
    // SAFETY: iocp_submit_accept_ex is a safe wrapper for the WinSock extension.
    let submit_res = unsafe {
        iocp_submit_accept_ex(AcceptExArgs {
            accept_ex: ctx.ext.accept_ex,
            s_listen_socket: handle.raw().as_socket(),
            s_accept_socket: accept_socket_raw,
            lp_output_buffer: payload.accept_buffer.as_mut_ptr() as *mut _,
            dw_receive_data_length: 0,
            dw_local_address_length: split as u32,
            dw_remote_address_length: split as u32,
            lp_dw_bytes_received: &mut bytes_received,
            lp_overlapped: ctx.overlapped,
        })
    }
    .map_err(|e| {
        e.attach_note(format!(
            "submit_accept: AcceptEx failure: listen=0x{:x}, accept=0x{:x}, in_len={}, out_len={}, user_data={}, generation={}",
            handle.raw().as_handle() as usize,
            accept_socket_raw,
            split,
            split,
            header.user_data,
            header.generation
        ))
    });
    mark_header_in_flight(header, submit_res)
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_accept(
    header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    _result: usize,
    ext: &Extensions,
) -> IocpResult<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut() };
    let accept_socket = payload
        .accept_socket
        .take()
        .ok_or(IocpError::InvalidState)
        .attach_note("accept socket not initialized")?;
    let listen_handle = header
        .resolved_handle
        .ok_or(IocpError::InvalidState)
        .attach_note("resolved listen handle missing for accept completion")?;
    let listen_socket = listen_handle.as_socket();
    let accept_socket_raw = accept_socket.raw().as_socket();

    if let Err(e) = with_borrowed_socket(accept_socket_raw, |socket| {
        socket.setsockopt(SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, &listen_socket)
    }) {
        return Err(e.attach_note(format!(
            "on_complete_accept: setsockopt(SO_UPDATE_ACCEPT_CONTEXT) failed: accept_socket=0x{:x}, listen_socket=0x{:x}, optlen={}",
            accept_socket_raw,
            listen_socket,
            std::mem::size_of::<SOCKET>()
        )));
    }

    let split = ACCEPT_EX_ADDR_SECTION_LEN;

    let mut local_sockaddr: *mut SOCKADDR = std::ptr::null_mut();
    let mut remote_sockaddr: *mut SOCKADDR = std::ptr::null_mut();
    let mut local_len: i32 = 0;
    let mut remote_len: i32 = 0;

    // SAFETY: get_accept_ex_sockaddrs is safe to call after setsockopt.
    unsafe {
        (ext.get_accept_ex_sockaddrs)(
            payload.accept_buffer.as_ptr() as *const _,
            0,
            split as u32,
            split as u32,
            &mut local_sockaddr,
            &mut local_len,
            &mut remote_sockaddr,
            &mut remote_len,
        );
    }

    if !remote_sockaddr.is_null() && remote_len > 0 {
        // SAFETY: remote_sockaddr and remote_len are provided by AcceptEx.
        let buf = unsafe {
            std::slice::from_raw_parts(remote_sockaddr as *const u8, remote_len as usize)
        };
        if let Ok(addr) = addr::to_socket_addr(buf) {
            user.remote_addr = Some(addr);
        }
    }
    // Transfer ownership to upper layer completion; payload must not close this socket again.
    let accepted_raw = accept_socket.into_raw();
    Ok(accepted_raw.raw().as_handle() as usize)
}

pub(crate) fn submit_send_to(
    header: &mut OverlappedEntry,
    payload: &mut SendToPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);

    // RIO path is mandatory for socket send_to.
    let page_idx = header.user_data / ctx.slots_per_page;
    let args = crate::rio::RioSendToArgs {
        fd: user.fd,
        handle,
        buf: &user.buf,
        addr_ptr: &payload.addr as *const _ as *const std::ffi::c_void,
        addr_len: payload.addr_len,
        user_data: header.user_data,
        generation: header.generation,
        page_idx,
        buf_offset: user.buf_offset,
    };
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send_to(args, ctx.registrar, ctx.slab_resolver)
            .map_err(|e| {
                from_io_error(IocpError::Submission, "submit_send_to", e).attach_note(format!(
                    "RIO send_to submit failed: fd={:?}, user_data={}, generation={}, page_idx={}",
                    user.fd, header.user_data, header.generation, page_idx
                ))
            }),
    )
}

// ============================================================================
// UDP RIO RecvFrom
// ============================================================================

pub(crate) fn submit_udp_recv_from(
    header: &mut OverlappedEntry,
    payload: &mut UdpRecvFromPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: payload.user and overlapped come from the in-flight slot.
    let (val, overlapped) = unsafe {
        let user = payload.user.as_mut();
        (user, &mut *ctx.overlapped)
    };
    overlapped.set_offset(0);
    let fd = val.fd;
    let handle = resolve_fd_borrowed(&fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&fd, ctx.registered_files)?);
    let page_idx = header.user_data / ctx.slots_per_page;

    let args = RioUdpRecvFromArgs {
        fd,
        handle,
        recv_from_op: val,
        addr_ptr: (&mut payload.addr as *mut SockAddrStorage).cast::<std::ffi::c_void>(),
        user_data: header.user_data,
        generation: header.generation,
        page_idx,
    };
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv_from(args, ctx.registrar, ctx.slab_resolver)
            .map_err(|e| {
            from_io_error(IocpError::Submission, "submit_udp_recv_from", e).attach_note(format!(
                "RIO udp_recv_from submit failed: fd={:?}, user_data={}, generation={}, page_idx={}",
                fd, header.user_data, header.generation, page_idx
            ))
        }),
    )
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_udp_recv_from(
    _header: &mut OverlappedEntry,
    payload: &mut UdpRecvFromPayload,
    result: usize,
    _ext: &Extensions,
) -> IocpResult<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };
    let addr_bytes = unsafe {
        std::slice::from_raw_parts(
            (&payload.addr as *const SockAddrStorage).cast::<u8>(),
            std::mem::size_of::<SockAddrStorage>(),
        )
    };
    val.addr = Some(
        addr::to_socket_addr(addr_bytes)
            .map_err(|_| IocpError::InvalidState)
            .attach_note("failed to parse RIO udp_recv_from source address")?,
    );
    Ok(result)
}
