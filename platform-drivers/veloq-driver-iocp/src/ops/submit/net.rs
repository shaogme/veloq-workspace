use std::io;
use std::mem::ManuallyDrop;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use veloq_pod::{bytes_of_mut, from_bytes_mut};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET,
};

use crate::common::{IocpErrorContext, io_error};
use crate::config::BorrowedRawHandle;
use crate::ext::Extensions;
use crate::net::addr::{self, SockAddrStorage};
use crate::ops::submit::common::{
    AcceptExArgs, ConnectExArgs, SubmissionResult, ensure_iocp_association, iocp_submit_accept_ex,
    iocp_submit_connect_ex, iocp_submit_socket_recv, iocp_submit_socket_send,
    mark_header_in_flight, resolve_fd, unpack_kernel_ref,
};
use crate::ops::{
    AcceptPayload, Connect, KernelRef, OpSend, OverlappedEntry, Recv, SendToPayload, SubmitContext,
    UdpRecv, UdpRecvStream, UdpSend,
};
use crate::rio::{RioTarget, SocketActorKey};
use crate::win32::SafeSocket;

// ============================================================================
// Network Operations
// ============================================================================

fn with_borrowed_socket<T>(
    raw: SOCKET,
    f: impl FnOnce(&SafeSocket) -> io::Result<T>,
) -> io::Result<T> {
    let socket = ManuallyDrop::new(SafeSocket(raw));
    f(&socket)
}

#[inline]
fn ensure_sticky_fallback_association(
    ctx: &mut SubmitContext,
    socket_key: SocketActorKey,
    handle: BorrowedRawHandle<'_>,
    detail: impl FnOnce() -> String,
) -> io::Result<()> {
    if ctx.rio.needs_iocp_fallback_association(socket_key) {
        ensure_iocp_association(handle, ctx.port, detail())?;
        ctx.rio.mark_iocp_fallback_associated(socket_key);
    }
    Ok(())
}

fn submit_iocp_recv_fallback(
    header: &mut OverlappedEntry,
    ctx: &mut SubmitContext,
    socket_key: SocketActorKey,
    handle: BorrowedRawHandle<'_>,
    fd: impl std::fmt::Debug,
    buf: *mut u8,
    len: u32,
    op_name: &str,
) -> io::Result<SubmissionResult> {
    ensure_sticky_fallback_association(ctx, socket_key, handle, || {
        format!("{op_name} fallback association failed: fd={fd:?}")
    })?;
    let socket = handle.as_socket();
    // SAFETY: socket/buffer/overlapped are guaranteed valid by submit contract.
    mark_header_in_flight(header, unsafe { iocp_submit_socket_recv(socket, buf, len, ctx.overlapped) })
        .map_err(|err| {
            io_error(
                IocpErrorContext::Submission,
                err,
                format!(
                    "{op_name} fallback syscall failed: fd={fd:?}, handle={:?}, user_data={}, generation={}",
                    handle.as_handle(),
                    header.user_data,
                    header.generation
                ),
            )
        })
}

fn submit_iocp_send_fallback(
    header: &mut OverlappedEntry,
    ctx: &mut SubmitContext,
    socket_key: SocketActorKey,
    handle: BorrowedRawHandle<'_>,
    fd: impl std::fmt::Debug,
    buf: *const u8,
    len: u32,
    op_name: &str,
) -> io::Result<SubmissionResult> {
    ensure_sticky_fallback_association(ctx, socket_key, handle, || {
        format!("{op_name} fallback association failed: fd={fd:?}")
    })?;
    let socket = handle.as_socket();
    // SAFETY: socket/buffer/overlapped are guaranteed valid by submit contract.
    mark_header_in_flight(header, unsafe { iocp_submit_socket_send(socket, buf, len, ctx.overlapped) })
        .map_err(|err| {
            io_error(
                IocpErrorContext::Submission,
                err,
                format!(
                    "{op_name} fallback syscall failed: fd={fd:?}, handle={:?}, user_data={}, generation={}",
                    handle.as_handle(),
                    header.user_data,
                    header.generation
                ),
            )
        })
}

fn submit_rio_then_fallback_wrapped(
    header: &mut OverlappedEntry,
    ctx: &mut SubmitContext,
    socket_key: SocketActorKey,
    rio_submit: impl FnOnce(&mut SubmitContext) -> io::Result<SubmissionResult>,
    fallback_submit: impl FnOnce(
        &mut OverlappedEntry,
        &mut SubmitContext,
    ) -> io::Result<SubmissionResult>,
    rio_err_detail: impl FnOnce() -> String,
) -> io::Result<SubmissionResult> {
    let mut fallback_submit = Some(fallback_submit);
    if ctx.rio.is_iocp_fallback(socket_key) {
        return fallback_submit.take().expect("fallback closure missing")(header, ctx);
    }

    match rio_submit(ctx) {
        Ok(res) => Ok(res),
        Err(e) => {
            ctx.rio.maybe_mark_iocp_fallback(socket_key, &e);
            if ctx.rio.is_iocp_fallback(socket_key) {
                fallback_submit.take().expect("fallback closure missing")(header, ctx)
            } else {
                Err(io_error(IocpErrorContext::Submission, e, rio_err_detail()))
            }
        }
    }
}

pub(crate) fn submit_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Recv>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let socket_key = SocketActorKey::new(handle.as_handle(), handle.generation());
    let user_data = header.user_data;
    let generation = header.generation;
    // SAFETY: pointer/len are derived from valid buffer and used only for fallback submit.
    let buf_ptr = unsafe { val.buf.as_mut_ptr().add(val.buf_offset) };
    let buf_len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;

    submit_rio_then_fallback_wrapped(
        header,
        ctx,
        socket_key,
        |ctx| {
            ctx.rio.try_submit_recv(
                RioTarget {
                    fd: val.fd,
                    handle,
                    user_data,
                    generation,
                    buf_offset: val.buf_offset,
                },
                &mut val.buf,
                ctx.registrar,
            )
        },
        |header, ctx| {
            submit_iocp_recv_fallback(
                header, ctx, socket_key, handle, val.fd, buf_ptr, buf_len, "TCP recv",
            )
        },
        || {
            format!(
                "RIO recv submit failed: fd={:?}, user_data={}, generation={}",
                val.fd, user_data, generation
            )
        },
    )
}

pub(crate) fn submit_udp_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecv>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let socket_key = SocketActorKey::new(handle.as_handle(), handle.generation());

    if ctx.rio.is_iocp_fallback(socket_key) {
        return submit_iocp_recv_fallback(
            header,
            ctx,
            socket_key,
            handle,
            val.fd,
            // SAFETY: buf pointer and length are valid for this operation.
            unsafe { val.buf.as_mut_ptr().add(val.buf_offset) },
            (val.buf.len().saturating_sub(val.buf_offset)) as u32,
            "UDP recv",
        );
    }

    match ctx.rio.try_submit_pool_recv_for_recv(
        crate::rio::RioUdpRecvArgs {
            fd: val.fd,
            handle,
            recv_op: val,
            sidecar: header,
        },
        ctx.registrar,
    ) {
        Ok(res) => Ok(res),
        Err(e) => {
            ctx.rio.maybe_mark_iocp_fallback(socket_key, &e);
            if ctx.rio.is_iocp_fallback(socket_key) {
                ensure_sticky_fallback_association(ctx, socket_key, handle, || {
                    format!("UDP recv fallback association failed: fd={:?}", val.fd)
                })?;
                submit_iocp_recv_fallback(
                    header,
                    ctx,
                    socket_key,
                    handle,
                    val.fd,
                    // SAFETY: buf pointer and length are valid for this operation.
                    unsafe { val.buf.as_mut_ptr().add(val.buf_offset) },
                    (val.buf.len().saturating_sub(val.buf_offset)) as u32,
                    "UDP recv",
                )
            } else {
                Err(e)
            }
        }
    }
}

pub(crate) fn submit_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<OpSend>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let socket_key = SocketActorKey::new(handle.as_handle(), handle.generation());
    let user_data = header.user_data;
    let generation = header.generation;

    submit_rio_then_fallback_wrapped(
        header,
        ctx,
        socket_key,
        |ctx| {
            ctx.rio.try_submit_send(
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
        },
        |header, ctx| {
            submit_iocp_send_fallback(
                header,
                ctx,
                socket_key,
                handle,
                val.fd,
                // SAFETY: buf pointer and length are valid for this operation.
                unsafe { val.buf.as_ptr().add(val.buf_offset) },
                (val.buf.len().saturating_sub(val.buf_offset)) as u32,
                "TCP send",
            )
        },
        || {
            format!(
                "RIO send submit failed: fd={:?}, user_data={}, generation={}",
                val.fd, user_data, generation
            )
        },
    )
}

pub(crate) fn submit_udp_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpSend>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

    let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let socket_key = SocketActorKey::new(handle.as_handle(), handle.generation());
    let user_data = header.user_data;
    let generation = header.generation;

    submit_rio_then_fallback_wrapped(
        header,
        ctx,
        socket_key,
        |ctx| {
            ctx.rio.try_submit_send(
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
        },
        |header, ctx| {
            submit_iocp_send_fallback(
                header,
                ctx,
                socket_key,
                handle,
                val.fd,
                // SAFETY: buf pointer and length are valid for this operation.
                unsafe { val.buf.as_ptr().add(val.buf_offset) },
                (val.buf.len().saturating_sub(val.buf_offset)) as u32,
                "UDP send",
            )
        },
        || {
            format!(
                "RIO udp_send submit failed: fd={:?}, user_data={}, generation={}",
                val.fd, user_data, generation
            )
        },
    )
}

pub(crate) fn submit_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    let raw_handle = resolve_fd(connect_op.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_connect: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}",
            connect_op.fd,
            handle.as_handle(),
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
            s: handle.as_socket(),
            name: &connect_op.addr as *const _ as *const SOCKADDR,
            namelen: connect_op.addr_len as i32,
            lp_send_buffer: std::ptr::null(),
            dw_send_data_length: 0,
            lp_dw_bytes_sent: &mut bytes_sent,
            lp_overlapped: ctx.overlapped,
        })
    })
}

fn ensure_socket_bound(handle: BorrowedRawHandle<'_>, connect_op: &Connect) -> io::Result<()> {
    let mut storage = SockAddrStorage::default();
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

    if with_borrowed_socket(handle.as_socket(), |socket| {
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
    with_borrowed_socket(handle.as_socket(), |socket| {
        // SAFETY: storage is a valid SOCKADDR_STORAGE for this call.
        unsafe { socket.bind(&storage.0 as *const _ as *const SOCKADDR, len) }
    })
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    result: usize,
    _ext: &Extensions,
) -> io::Result<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let connect_op = unsafe { payload.user.as_ref() };
    if let Some(raw_handle) = header.resolved_handle.or_else(|| connect_op.fd.raw()) {
        with_borrowed_socket(raw_handle.as_socket(), |socket| {
            socket.setsockopt_empty(SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT)
        })?;
    }
    Ok(result)
}

pub(crate) fn submit_accept(
    header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let accept_socket = payload
        .accept_socket
        .as_ref()
        .ok_or_else(|| io::Error::other("accept socket not initialized"))?;
    let accept_socket_raw = accept_socket.as_socket();

    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_accept: associate listen socket failed: listen=0x{:x}, user_data={}, generation={}",
            handle.as_handle() as usize,
            header.user_data,
            header.generation
        ),
    )?;

    // Ensure the pre-allocated accept socket is also associated with the same IOCP.
    let accept_socket_handle = crate::config::RawHandle::for_socket(accept_socket_raw as _);
    ensure_iocp_association(
        accept_socket_handle.borrow(),
        ctx.port,
        format!(
            "submit_accept: associate accept socket failed: accept=0x{:x}, listen=0x{:x}, user_data={}, generation={}",
            accept_socket_raw,
            handle.as_handle() as usize,
            header.user_data,
            header.generation
        ),
    )?;

    const MIN_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
    let split = MIN_ADDR_LEN;
    let mut bytes_received = 0;
    // SAFETY: iocp_submit_accept_ex is a safe wrapper for the WinSock extension.
    let submit_res = unsafe {
        iocp_submit_accept_ex(AcceptExArgs {
            accept_ex: ctx.ext.accept_ex,
            s_listen_socket: handle.as_socket(),
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
        io_error(
            IocpErrorContext::Submission,
            e,
            format!(
                "submit_accept: AcceptEx failure: listen=0x{:x}, accept=0x{:x}, in_len={}, out_len={}, user_data={}, generation={}",
                handle.as_handle() as usize,
                accept_socket_raw,
                split,
                split,
                header.user_data,
                header.generation
            ),
        )
    });
    mark_header_in_flight(header, submit_res)
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_accept(
    _header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    _result: usize,
    ext: &Extensions,
) -> io::Result<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut() };
    let accept_socket = payload
        .accept_socket
        .take()
        .ok_or_else(|| io::Error::other("accept socket not initialized"))?;
    let listen_handle = user.fd.raw().ok_or(io::Error::from_raw_os_error(0))?;
    let listen_socket = listen_handle.as_socket();
    let accept_socket_raw = accept_socket.as_socket();

    if let Err(e) = with_borrowed_socket(accept_socket_raw, |socket| {
        socket.setsockopt(SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, &listen_socket)
    }) {
        return Err(io_error(
            IocpErrorContext::Submission,
            e,
            format!(
                "on_complete_accept: setsockopt(SO_UPDATE_ACCEPT_CONTEXT) failed: accept_socket=0x{:x}, listen_socket=0x{:x}, optlen={}",
                accept_socket_raw,
                listen_socket,
                std::mem::size_of::<SOCKET>()
            ),
        ));
    }

    const MIN_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
    let split = MIN_ADDR_LEN;

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
    Ok(accepted_raw.as_handle() as usize)
}

pub(crate) fn submit_send_to(
    header: &mut OverlappedEntry,
    payload: &mut SendToPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();

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
    ctx.rio
        .try_submit_send_to(args, ctx.registrar, ctx.slab_resolver)
        .map_err(|e| {
            io_error(
                IocpErrorContext::Submission,
                e,
                format!(
                    "RIO send_to submit failed: fd={:?}, user_data={}, generation={}, page_idx={}",
                    user.fd, header.user_data, header.generation, page_idx
                ),
            )
        })
}

// ============================================================================
// UDP RIO Pool (Stream)
// ============================================================================

pub(crate) fn submit_udp_recv_stream(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecvStream>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);
    let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
    header.resolved_handle = Some(raw_handle);
    let handle = raw_handle.borrow();
    let socket_key = SocketActorKey::new(handle.as_handle(), handle.generation());

    let args = crate::rio::RioUdpStreamArgs {
        fd: val.fd,
        handle,
        stream_op: val,
        user_data: header.user_data,
        generation: header.generation,
    };
    match ctx.rio.try_submit_pool_recv(args, ctx.registrar) {
        Ok(res) => Ok(res),
        Err(e) => {
            ctx.rio.maybe_mark_iocp_fallback(socket_key, &e);
            if ctx.rio.is_iocp_fallback(socket_key) {
                let buf = val
                    .buf
                    .as_mut()
                    .ok_or_else(|| io::Error::other("udp recv_stream buffer missing"))?;
                submit_iocp_recv_fallback(
                    header,
                    ctx,
                    socket_key,
                    handle,
                    val.fd,
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                    "UDP recv_stream",
                )
            } else {
                Err(io_error(
                    IocpErrorContext::Submission,
                    e,
                    format!(
                        "RIO udp_recv_stream submit failed: fd={:?}, user_data={}, generation={}",
                        val.fd, header.user_data, header.generation
                    ),
                ))
            }
        }
    }
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_udp_stream_complete(
    _header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecvStream>,
    result: usize,
    _ext: &Extensions,
) -> io::Result<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };
    if val.result.is_none()
        && val.addr.is_none()
        && let Some(raw) = val.fd.raw()
    {
        let mut storage = SockAddrStorage::default();
        let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
        let _ = with_borrowed_socket(raw.as_socket(), |socket| {
            // SAFETY: storage and namelen are valid output pointers.
            unsafe { socket.getpeername(&mut storage.0 as *mut _ as *mut SOCKADDR, &mut namelen) }
        });
        if namelen > 0 {
            // SAFETY: storage was initialized by getpeername when namelen > 0.
            let buf = unsafe {
                std::slice::from_raw_parts(&storage.0 as *const _ as *const u8, namelen as usize)
            };
            if let Ok(addr) = addr::to_socket_addr(buf) {
                val.addr = Some(addr);
            }
        }
    }
    if result == 0
        && let Some(datagram) = val.result.as_ref()
    {
        return Ok(datagram.buf.len());
    }
    Ok(result)
}
