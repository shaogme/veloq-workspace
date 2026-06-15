use diagweave::prelude::*;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SOCKADDR, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET,
};

use crate::{
    Socket,
    config::{BorrowedRawHandle, IocpHandle},
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::addr::{self, SockAddrStorage},
    op::{
        ACCEPT_EX_ADDR_SECTION_LEN, AcceptPayload, OverlappedEntry, SubmitContext,
        submit::{
            AcceptExArgs, SubmissionResult, ensure_iocp_association, iocp_submit_accept_ex,
            resolve_fd_handle,
        },
    },
};
use veloq_driver_core::{
    RawHandleMeta,
    driver::{CompletionCleanup, CompletionCleanupGuard},
};

use super::{mark_socket_header_in_flight, with_borrowed_socket};

fn socket_family_from_handle(handle: BorrowedRawHandle<'_>) -> IocpResult<u16> {
    let mut storage = SockAddrStorage::default();
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
    with_borrowed_socket(handle.raw().as_socket(), |socket| {
        // SAFETY: storage and namelen are valid output pointers.
        unsafe { socket.getsockname(&mut storage.0 as *mut _ as *mut SOCKADDR, &mut namelen) }
    })?;
    match storage.family() {
        AF_INET | AF_INET6 => Ok(storage.family()),
        family => IocpError::InvalidInput
            .with_ctx("address_family", family)
            .attach_note("unsupported listen socket family for accept"),
    }
}

pub(crate) fn submit_accept(
    header: &mut OverlappedEntry,
    payload: &mut AcceptPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_mut()? };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    if payload.accept_socket.is_none() {
        let family = socket_family_from_handle(handle)?;
        let accept_socket = if family == AF_INET {
            Socket::new_tcp_v4()
        } else {
            Socket::new_tcp_v6()
        }
        .map(|s| s.into_owned_raw())
        .push_ctx("scope", "submit_accept.create_accept_socket")
        .with_ctx("listen_handle_raw", handle.raw().as_handle() as usize)
        .with_ctx("address_family", family)
        .attach_note("create accept socket failed")?;
        payload.accept_socket = Some(accept_socket);
    }
    let accept_socket = payload
        .accept_socket
        .as_ref()
        .ok_or(IocpError::InvalidState)
        .attach_note("accept socket not initialized")?;
    let accept_socket_raw = accept_socket.raw().as_socket();

    ensure_iocp_association(&user.fd, raw, ctx.port.as_ref(), &mut *ctx.registered_slots)
        .push_ctx("scope", "submit_accept")
        .with_ctx("listen_handle_raw", raw.raw().as_handle() as usize)
        .with_ctx("user_data", header.token.index())
        .with_ctx("generation", header.token.generation())?;

    let split = ACCEPT_EX_ADDR_SECTION_LEN;
    let mut bytes_received = 0;
    let accept_ex = ctx.ext.accept_ex;
    let overlapped = ctx.overlapped;
    let inflight = ctx
        .rio
        .try_acquire_socket_inflight_guard(raw.raw().actor_key())
        .push_ctx("scope", "submit_accept.acquire_socket_inflight")
        .with_ctx("fd_fixed_index", user.fd.fixed_index())
        .with_ctx("fd_generation", user.fd.generation())
        .with_ctx("listen_handle_raw", raw.raw().as_handle() as usize)
        .with_ctx("accept_socket_raw", accept_socket_raw)
        .with_ctx("user_data", header.token.index())
        .with_ctx("generation", header.token.generation())
        .attach_note("failed to acquire socket inflight slot before AcceptEx")
        .trans()?;
    // SAFETY: iocp_submit_accept_ex is a safe wrapper for the WinSock extension.
    let submit_res = unsafe {
        iocp_submit_accept_ex(AcceptExArgs {
            accept_ex,
            s_listen_socket: handle.raw().as_socket(),
            s_accept_socket: accept_socket_raw,
            lp_output_buffer: payload.accept_buffer.as_mut_ptr() as *mut _,
            dw_receive_data_length: 0,
            dw_local_address_length: split as u32,
            dw_remote_address_length: split as u32,
            lp_dw_bytes_received: &mut bytes_received,
            lp_overlapped: overlapped,
        })
    }
    .push_ctx("scope", "submit_accept")
    .with_ctx("listen_handle_raw", handle.raw().as_handle() as usize)
    .with_ctx("accept_socket_raw", accept_socket_raw)
    .with_ctx("accept_input_length", split)
    .with_ctx("accept_output_length", split)
    .with_ctx("user_data", header.token.index())
    .with_ctx("generation", header.token.generation())
    .attach_note("AcceptEx submit failed");
    mark_socket_header_in_flight(header, inflight, submit_res)
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
    let user = unsafe { payload.user.as_mut()? };
    let accept_socket = payload
        .accept_socket
        .take()
        .ok_or(IocpError::InvalidState)
        .attach_note("accept socket not initialized")?;
    let listen_handle = header
        .resolved_handle
        .ok_or(IocpError::InvalidState)
        .attach_note("resolved listen handle missing for accept completion")?;
    let listen_socket = listen_handle.raw().as_socket();
    let accept_socket_raw = accept_socket.raw().as_socket();

    if let Err(e) = with_borrowed_socket(accept_socket_raw, |socket| {
        socket.setsockopt(SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, &listen_socket)
    }) {
        return Err(e
            .push_ctx("scope", "on_complete_accept")
            .with_ctx("accept_socket_raw", accept_socket_raw)
            .with_ctx("listen_socket_raw", listen_socket)
            .with_ctx("socket_opt_len", std::mem::size_of::<SOCKET>())
            .attach_note("setsockopt(SO_UPDATE_ACCEPT_CONTEXT) failed"));
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

pub(crate) fn completion_cleanup_close_socket(
    result: &IocpResult<usize>,
) -> CompletionCleanupGuard {
    let Ok(raw) = result.as_ref().copied() else {
        return CompletionCleanupGuard::default();
    };
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        IocpHandle::for_socket(raw as _).close();
        Ok(())
    }))
}
