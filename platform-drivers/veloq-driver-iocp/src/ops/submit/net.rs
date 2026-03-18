use std::io;
use std::mem::ManuallyDrop;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use veloq_pod::{bytes_of_mut, from_bytes_mut};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET,
};

use crate::common::{IocpErrorContext, io_error};
use crate::ext::Extensions;
use crate::net::addr::SockAddrStorage;
use crate::ops::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_accept_ex, iocp_submit_connect_ex,
    iocp_submit_read, iocp_submit_write, resolve_fd, unpack_kernel_ref,
};
use crate::ops::{
    AcceptPayload, Connect, KernelRef, OpSend, OverlappedEntry, Recv, SendToPayload, SubmitContext,
    UdpRecvStream, UdpRefill,
};
use crate::rio::RioTarget;
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

pub(crate) fn submit_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Recv>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

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
            ensure_iocp_association(
                handle,
                ctx.port,
                format!("RIO fallback recv association failed: fd={:?}", val.fd),
            )?;
            // SAFETY: handle/buffer/overlapped are guaranteed valid by submit contract.
            unsafe {
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

pub(crate) fn submit_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<OpSend>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    overlapped.set_offset(0);

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
            ensure_iocp_association(
                handle,
                ctx.port,
                format!("RIO fallback send association failed: fd={:?}", val.fd),
            )?;
            // SAFETY: handle/buffer/overlapped are guaranteed valid by submit contract.
            unsafe {
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

pub(crate) fn submit_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    let handle = resolve_fd(connect_op.fd, ctx.registered_files)?;
    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_connect: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}",
            connect_op.fd, handle, header.user_data, header.generation
        ),
    )?;

    let mut need_bind = true;
    let mut storage = SockAddrStorage::default();
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

    if with_borrowed_socket(handle as SOCKET, |socket| {
        socket.getsockname(&mut storage.0 as *mut _ as *mut SOCKADDR, &mut namelen)
    })
    .is_ok()
    {
        let family = storage.family();
        if family == AF_INET {
            let buf = unsafe {
                std::slice::from_raw_parts(&storage.0 as *const _ as *const u8, namelen as usize)
            };
            if let Ok(SocketAddr::V4(a)) = crate::net::addr::to_socket_addr(buf) {
                if a.port() != 0 {
                    need_bind = false;
                }
            }
        } else if family == AF_INET6 {
            let buf = unsafe {
                std::slice::from_raw_parts(&storage.0 as *const _ as *const u8, namelen as usize)
            };
            if let Ok(SocketAddr::V6(a)) = crate::net::addr::to_socket_addr(buf) {
                if a.port() != 0 {
                    need_bind = false;
                }
            }
        }
    }

    if need_bind {
        let family = connect_op.addr.family();
        let (storage, len) = if family == AF_INET {
            let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);
            let s = crate::net::addr::SockAddrIn::new(&addr);
            let mut storage = SockAddrStorage::default();
            let sin_ref = from_bytes_mut::<crate::net::addr::SockAddrIn>(
                &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN>()],
            );
            *sin_ref = s;
            (storage, std::mem::size_of::<SOCKADDR_IN>() as i32)
        } else {
            let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
            let s = crate::net::addr::SockAddrIn6::new(&addr);
            let mut storage = SockAddrStorage::default();
            let sin6_ref = from_bytes_mut::<crate::net::addr::SockAddrIn6>(
                &mut bytes_of_mut(&mut storage)[..std::mem::size_of::<SOCKADDR_IN6>()],
            );
            *sin6_ref = s;
            (storage, std::mem::size_of::<SOCKADDR_IN6>() as i32)
        };
        with_borrowed_socket(handle as SOCKET, |socket| {
            socket.bind(&storage.0 as *const _ as *const SOCKADDR, len)
        })?;
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
        with_borrowed_socket(fd.handle as SOCKET, |socket| {
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
    let handle = resolve_fd(user.fd, ctx.registered_files)?;
    let accept_socket = payload.accept_socket;
    let accept_socket_raw = accept_socket.handle as SOCKET;

    ensure_iocp_association(
        handle,
        ctx.port,
        format!(
            "submit_accept: associate listen socket failed: listen=0x{:x}, user_data={}, generation={}",
            handle as usize, header.user_data, header.generation
        ),
    )?;

    // Ensure the pre-allocated accept socket is also associated with the same IOCP.
    ensure_iocp_association(
        accept_socket_raw as HANDLE,
        ctx.port,
        format!(
            "submit_accept: associate accept socket failed: accept=0x{:x}, listen=0x{:x}, user_data={}, generation={}",
            accept_socket_raw, handle as usize, header.user_data, header.generation
        ),
    )?;

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
        let buf = unsafe {
            std::slice::from_raw_parts(remote_sockaddr as *const u8, remote_len as usize)
        };
        if let Ok(addr) = crate::net::addr::to_socket_addr(buf) {
            user.remote_addr = Some(addr);
        }
    }
    Ok(accept_socket_raw as usize)
}

pub(crate) fn submit_send_to(
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

pub(crate) fn submit_udp_recv_stream(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecvStream>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
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

pub(crate) fn submit_udp_refill(
    _header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRefill>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };
    let handle = resolve_fd(val.fd, ctx.registered_files)?;
    if let Some(buf) = val.buf.take() {
        ctx.rio
            .try_refill_udp_pool((val.fd, handle), buf, ctx.registrar)?;
    }

    // Refill is not an async IO op that completes via IOCP,
    // it just updates the internal pool. We post a completion to notify success.
    Ok(SubmissionResult::PostToQueue)
}
