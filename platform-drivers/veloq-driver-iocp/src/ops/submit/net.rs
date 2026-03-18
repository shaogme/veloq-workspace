use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, WSAGetLastError, bind,
    getsockname, setsockopt,
};

use crate::common::{IocpErrorContext, io_error};
use crate::ext::Extensions;
use crate::ops::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_accept_ex, iocp_submit_connect_ex,
    iocp_submit_read, iocp_submit_write, resolve_fd,
};
use crate::ops::{
    AcceptPayload, Connect, KernelRef, OpSend, OverlappedEntry, Recv, SendToPayload, SubmitContext,
    UdpRecvStream, UdpRefill,
};
use crate::rio::RioTarget;

// ============================================================================
// Network Operations
// ============================================================================

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) unsafe fn submit_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Recv>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };

    // SAFETY: The caller guarantees that ctx.overlapped is valid.
    let overlapped = unsafe { &mut *ctx.overlapped };
    overlapped.Anonymous.Anonymous.Offset = 0;
    overlapped.Anonymous.Anonymous.OffsetHigh = 0;

    let handle = resolve_fd(val.fd, ctx.registered_files)?;

    // Try RIO path first.
    let rio_res = ctx.rio.try_submit_recv(
        RioTarget {
            fd: val.fd,
            handle,
            user_data: header.user_data,
            generation: header.generation,
        },
        &mut val.buf,
        ctx.registrar,
    );

    match rio_res {
        Ok(res) => Ok(res),
        Err(_) if ctx.rio.registration_mode == crate::BufferRegistrationMode::Compatible => {
            // Fallback to standard IOCP for socket recv.
            // Safety: ensure socket is associated with the completion port.
            unsafe {
                ensure_iocp_association(
                    handle,
                    ctx.port,
                    format!("RIO fallback recv association failed: fd={:?}", val.fd),
                )?;
                iocp_submit_read(
                    handle,
                    val.buf.as_mut_ptr(),
                    val.buf.len() as u32,
                    ctx.overlapped,
                )
            }
        }
        Err(e) => Err(io_error(
            IocpErrorContext::Submission,
            e,
            format!(
                "RIO recv submit failed: fd={:?}, user_data={}, generation={}",
                val.fd, header.user_data, header.generation
            ),
        )),
    }
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) unsafe fn submit_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<OpSend>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };

    // SAFETY: The caller guarantees that ctx.overlapped is valid.
    let overlapped = unsafe { &mut *ctx.overlapped };
    overlapped.Anonymous.Anonymous.Offset = 0;
    overlapped.Anonymous.Anonymous.OffsetHigh = 0;

    let handle = resolve_fd(val.fd, ctx.registered_files)?;

    // Try RIO path first.
    let rio_res = ctx.rio.try_submit_send(
        RioTarget {
            fd: val.fd,
            handle,
            user_data: header.user_data,
            generation: header.generation,
        },
        &val.buf,
        ctx.registrar,
    );

    match rio_res {
        Ok(res) => Ok(res),
        Err(_) if ctx.rio.registration_mode == crate::BufferRegistrationMode::Compatible => {
            // Fallback to standard IOCP for socket send.
            // Safety: ensure socket is associated with the completion port.
            unsafe {
                ensure_iocp_association(
                    handle,
                    ctx.port,
                    format!("RIO fallback send association failed: fd={:?}", val.fd),
                )?;
                iocp_submit_write(
                    handle,
                    val.buf.as_ptr(),
                    val.buf.len() as u32,
                    ctx.overlapped,
                )
            }
        }
        Err(e) => Err(io_error(
            IocpErrorContext::Submission,
            e,
            format!(
                "RIO send submit failed: fd={:?}, user_data={}, generation={}",
                val.fd, header.user_data, header.generation
            ),
        )),
    }
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) unsafe fn submit_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let connect_op = unsafe { payload.user.as_mut() };
    let handle = resolve_fd(connect_op.fd, ctx.registered_files)?;
    // SAFETY: the handle is checked for validity by resolve_fd.
    unsafe {
        ensure_iocp_association(
            handle,
            ctx.port,
            format!(
                "submit_connect: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}",
                connect_op.fd, handle, header.user_data, header.generation
            ),
        )
    }?;

    let mut need_bind = true;
    // SAFETY: SOCKADDR_STORAGE is a POD type and zeroing it is safe.
    let mut name: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

    // SAFETY: getsockname is safe to call with a valid socket and buffer.
    let ret = unsafe {
        getsockname(
            handle as SOCKET,
            &mut name as *mut _ as *mut SOCKADDR,
            &mut namelen,
        )
    };

    if ret == 0 {
        let family = name.ss_family;
        if family == AF_INET {
            // SAFETY: name is verified to be AF_INET.
            let addr_in = unsafe { &*(&name as *const _ as *const SOCKADDR_IN) };
            if addr_in.sin_port != 0 {
                need_bind = false;
            }
        } else if family == AF_INET6 {
            // SAFETY: name is verified to be AF_INET6.
            let addr_in6 = unsafe { &*(&name as *const _ as *const SOCKADDR_IN6) };
            if addr_in6.sin6_port != 0 {
                need_bind = false;
            }
        }
    }

    if need_bind {
        let family = connect_op.addr.0.ss_family;
        // SAFETY: SOCKADDR_IN is a POD type.
        let mut bind_addr: SOCKADDR_IN = unsafe { std::mem::zeroed() };
        bind_addr.sin_family = AF_INET;
        // SAFETY: SOCKADDR_IN6 is a POD type.
        let mut bind_addr6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
        bind_addr6.sin6_family = AF_INET6;

        let (ptr, len) = if family == AF_INET {
            (
                &bind_addr as *const _ as *const SOCKADDR,
                std::mem::size_of::<SOCKADDR_IN>() as i32,
            )
        } else {
            (
                &bind_addr6 as *const _ as *const SOCKADDR,
                std::mem::size_of::<SOCKADDR_IN6>() as i32,
            )
        };
        // SAFETY: bind is safe to call with valid parameters for a new socket.
        let bind_ret = unsafe { bind(handle as SOCKET, ptr, len) };
        if bind_ret == SOCKET_ERROR {
            return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
        }
    }

    let mut bytes_sent = 0;
    // SAFETY: iocp_submit_connect_ex is a safe wrapper for the WinSock extension.
    unsafe {
        iocp_submit_connect_ex(
            ctx.ext.connect_ex,
            handle as SOCKET,
            &connect_op.addr as *const _ as *const SOCKADDR,
            connect_op.addr_len as i32,
            std::ptr::null(),
            0,
            &mut bytes_sent,
            ctx.overlapped,
        )
    }
}

/// # Safety
///
/// The caller must ensure that header and payload are valid.
pub(crate) unsafe fn on_complete_connect(
    _header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    result: usize,
    _ext: &Extensions,
) -> io::Result<usize> {
    // SAFETY: The caller guarantees that payload is valid.
    let connect_op = unsafe { payload.user.as_ref() };
    if let Some(fd) = connect_op.fd.raw() {
        // SAFETY: setsockopt is safe to call after ConnectEx completion to update socket context.
        let ret = unsafe {
            setsockopt(
                fd.handle as SOCKET,
                SOL_SOCKET,
                SO_UPDATE_CONNECT_CONTEXT,
                std::ptr::null(),
                0,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(result)
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) unsafe fn submit_accept(
    header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;
    let accept_socket = payload.accept_socket;
    let accept_socket_raw = accept_socket.handle as SOCKET;

    // SAFETY: the handle is checked for validity by resolve_fd.
    unsafe {
        ensure_iocp_association(
            handle,
            ctx.port,
            format!(
                "submit_accept: associate listen socket failed: listen=0x{:x}, user_data={}, generation={}",
                handle as usize, header.user_data, header.generation
            ),
        )
    }?;

    // Ensure the pre-allocated accept socket is also associated with the same IOCP.
    // SAFETY: the accept socket handle is managed by the driver.
    unsafe {
        ensure_iocp_association(
            accept_socket_raw as HANDLE,
            ctx.port,
            format!(
                "submit_accept: associate accept socket failed: accept=0x{:x}, listen=0x{:x}, user_data={}, generation={}",
                accept_socket_raw, handle as usize, header.user_data, header.generation
            ),
        )
    }?;

    const MIN_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
    let split = MIN_ADDR_LEN;
    let mut bytes_received = 0;

    // SAFETY: iocp_submit_accept_ex is a safe wrapper for the WinSock extension.
    unsafe {
        iocp_submit_accept_ex(
            ctx.ext.accept_ex,
            handle as SOCKET,
            accept_socket_raw,
            payload.accept_buffer.as_mut_ptr() as *mut _,
            0,
            split as u32,
            split as u32,
            &mut bytes_received,
            ctx.overlapped,
        )
    }.map_err(|e| {
        io_error(
            IocpErrorContext::Submission,
            e,
            format!(
                "submit_accept: AcceptEx failure: listen=0x{:x}, accept=0x{:x}, in_len={}, out_len={}, user_data={}, generation={}",
                handle as usize,
                accept_socket_raw,
                split,
                split,
                header.user_data,
                header.generation
            ),
        )
    })
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
    let accept_socket = payload.accept_socket;
    let listen_handle = user.fd.raw().ok_or(io::Error::from_raw_os_error(0))?;
    let listen_socket = listen_handle.handle as SOCKET;
    let accept_socket_raw = accept_socket.handle as SOCKET;

    // SAFETY: setsockopt is safe to call after AcceptEx Completion.
    let ret = unsafe {
        setsockopt(
            accept_socket_raw,
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            &listen_socket as *const _ as *const _,
            std::mem::size_of::<SOCKET>() as i32,
        )
    };
    if ret != 0 {
        return Err(io_error(
            IocpErrorContext::Submission,
            io::Error::last_os_error(),
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
        // SAFETY: remote_sockaddr is verified to be non-null.
        let family = unsafe { (*remote_sockaddr).sa_family };
        if family == AF_INET {
            // SAFETY: remote_sockaddr is verified to be AF_INET.
            let addr_in = unsafe { &*(remote_sockaddr as *const SOCKADDR_IN) };
            // SAFETY: S_un.S_addr is a POD field.
            let ip = Ipv4Addr::from(unsafe { addr_in.sin_addr.S_un.S_addr.to_ne_bytes() });
            let port = u16::from_be(addr_in.sin_port);
            user.remote_addr = Some(SocketAddr::V4(SocketAddrV4::new(ip, port)));
        } else if family == AF_INET6 {
            // SAFETY: remote_sockaddr is verified to be AF_INET6.
            let addr_in6 = unsafe { &*(remote_sockaddr as *const SOCKADDR_IN6) };
            // SAFETY: sin6_addr.u.Byte is a POD field.
            let ip = Ipv6Addr::from(unsafe { addr_in6.sin6_addr.u.Byte });
            let port = u16::from_be(addr_in6.sin6_port);
            let flowinfo = addr_in6.sin6_flowinfo;
            // SAFETY: Anonymous.sin6_scope_id is a POD field.
            let scope_id = unsafe { addr_in6.Anonymous.sin6_scope_id };
            user.remote_addr = Some(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )));
        }
    }
    Ok(accept_socket_raw as usize)
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) unsafe fn submit_send_to(
    header: &mut OverlappedEntry,
    payload: &mut SendToPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;

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

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) unsafe fn submit_udp_recv_stream(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecvStream>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };
    let handle = resolve_fd(val.fd, ctx.registered_files)?;
    let args = crate::rio::RioUdpStreamArgs {
        fd: val.fd,
        handle,
        stream_op: val,
        user_data: header.user_data,
        generation: header.generation,
    };
    ctx.rio
        .try_submit_udp_recv_stream_pooled(args, ctx.registrar)
        .map_err(|e| {
            io_error(
                IocpErrorContext::Submission,
                e,
                format!(
                    "RIO udp_recv_stream submit failed: fd={:?}, user_data={}, generation={}",
                    val.fd, header.user_data, header.generation
                ),
            )
        })
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
    if result == 0
        && let Some(datagram) = val.result.as_ref()
    {
        return Ok(datagram.buf.len());
    }
    Ok(result)
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) unsafe fn submit_udp_refill(
    _header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRefill>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let val = unsafe { payload.user.as_mut() };
    let handle = resolve_fd(val.fd, ctx.registered_files)?;
    if let Some(buf) = val.buf.take() {
        ctx.rio
            .try_refill_udp_pool((val.fd, handle), buf, ctx.registrar)?;
    }

    // Refill is not an async IO op that completes via IOCP,
    // it just updates the internal pool. We post a completion to notify success.
    Ok(SubmissionResult::PostToQueue)
}
