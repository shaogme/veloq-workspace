use diagweave::prelude::*;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use veloq_pod::{bytes_of_mut, from_bytes_mut};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6,
    SOCKADDR_STORAGE, SOL_SOCKET,
};

use crate::config::BorrowedRawHandle;
use crate::error::{IocpError, IocpResult};
use crate::ext::Extensions;
use crate::net::addr::{self, SockAddrStorage};
use crate::op::submit::{
    ConnectExArgs, SubmissionResult, ensure_iocp_association, iocp_submit_connect_ex,
    resolve_fd_handle, unpack_kernel_ref,
};
use crate::op::{Connect, KernelRef, OverlappedEntry, SubmitContext, UdpConnect};

use super::{mark_socket_header_in_flight, with_borrowed_socket};

pub(crate) fn submit_connect(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Connect>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    let raw = resolve_fd_handle(&connect_op.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    ensure_iocp_association(
        &connect_op.fd,
        raw,
        ctx.port.as_ref(),
        &mut *ctx.registered_slots,
    )
    .push_ctx("scope", "submit_connect")
    .with_ctx("fd_fixed_index", connect_op.fd.fixed_index())
    .with_ctx("fd_generation", connect_op.fd.generation())
    .with_ctx("handle_raw", raw.as_handle() as usize)
    .with_ctx("user_data", header.token.index())
    .with_ctx("generation", header.token.generation())?;
    let raw_handle = crate::config::RawHandle::new(raw);
    let handle = raw_handle.borrow();
    ensure_socket_bound(handle, connect_op)?;
    let connect_ex = ctx.ext.connect_ex;
    let overlapped = ctx.overlapped;
    let inflight = ctx
        .rio
        .try_acquire_socket_inflight_guard(raw.actor_key())
        .push_ctx("scope", "submit_connect.acquire_socket_inflight")
        .with_ctx("fd_fixed_index", connect_op.fd.fixed_index())
        .with_ctx("fd_generation", connect_op.fd.generation())
        .with_ctx("handle_raw", raw.as_handle() as usize)
        .with_ctx("user_data", header.token.index())
        .with_ctx("generation", header.token.generation())
        .attach_note("failed to acquire socket inflight slot before ConnectEx")
        .trans()?;

    let mut bytes_sent = 0;
    // SAFETY: iocp_submit_connect_ex is a safe wrapper for the WinSock extension.
    mark_socket_header_in_flight(header, inflight, unsafe {
        iocp_submit_connect_ex(ConnectExArgs {
            connect_ex,
            s: handle.raw().as_socket(),
            name: &connect_op.addr as *const _ as *const SOCKADDR,
            namelen: connect_op.addr_len as i32,
            lp_send_buffer: std::ptr::null(),
            dw_send_data_length: 0,
            lp_dw_bytes_sent: &mut bytes_sent,
            lp_overlapped: overlapped,
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
    let (connect_op, _overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    let raw = resolve_fd_handle(&connect_op.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    with_borrowed_socket(raw.as_socket(), |socket| {
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
