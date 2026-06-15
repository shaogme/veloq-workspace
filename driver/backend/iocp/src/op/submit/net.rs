mod accept;
mod connect;

pub(crate) use accept::{completion_cleanup_close_socket, on_complete_accept, submit_accept};
pub(crate) use connect::{
    on_complete_connect, on_complete_udp_connect, submit_connect, submit_udp_connect,
};

use diagweave::prelude::*;
use std::mem::ManuallyDrop;
use windows_sys::Win32::Networking::WinSock::SOCKET;

use crate::{
    error::IocpResult,
    ext::Extensions,
    net::addr::{self, SockAddrStorage},
    op::{
        KernelRef, OpSend, OverlappedEntry, Recv, SendToPayload, SubmitContext, UdpRecv,
        UdpRecvFromPayload, UdpSend,
        submit::{SubmissionResult, mark_header_in_flight, resolve_fd_handle, unpack_kernel_ref},
    },
    rio::{RioSendToArgs, RioTarget, RioUdpRecvFromArgs, SocketInflightGuard},
    win32::SafeSocket,
};

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

fn mark_socket_header_in_flight(
    header: &mut OverlappedEntry,
    inflight: SocketInflightGuard<'_>,
    res: IocpResult<SubmissionResult>,
) -> IocpResult<SubmissionResult> {
    let res = mark_header_in_flight(header, res);
    if matches!(res, Ok(SubmissionResult::Pending)) {
        debug_assert!(
            header.socket_inflight.is_none(),
            "socket inflight token already attached to op header"
        );
        header.socket_inflight = Some(inflight.commit());
    }
    res
}

pub(crate) fn submit_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Recv>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    overlapped.set_offset(0);

    let fd = val.fd;
    let raw = resolve_fd_handle(&fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    let token = ctx.op_token;
    let (user_data, generation) = token.parts();
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv(
                RioTarget {
                    fd,
                    handle,
                    token,
                    buf_offset: val.buf_offset,
                    operation: "recv",
                },
                &mut val.buf,
                ctx.registrar,
            )
            .with_ctx("outer_scope", "submit_recv")
            .with_ctx("fd_fixed_index", val.fd.fixed_index())
            .with_ctx("fd_generation", val.fd.generation())
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .attach_note("RIO recv submit failed")
            .trans(),
    )
}

pub(crate) fn submit_udp_recv(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpRecv>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    overlapped.set_offset(0);

    let fd = val.fd;
    let raw = resolve_fd_handle(&fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    let token = ctx.op_token;
    let (user_data, generation) = token.parts();
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv(
                RioTarget {
                    fd,
                    handle,
                    token,
                    buf_offset: val.buf_offset,
                    operation: "udp_recv",
                },
                &mut val.buf,
                ctx.registrar,
            )
            .with_ctx("outer_scope", "submit_udp_recv")
            .with_ctx("fd_fixed_index", val.fd.fixed_index())
            .with_ctx("fd_generation", val.fd.generation())
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .attach_note("RIO udp_recv submit failed")
            .trans(),
    )
}

pub(crate) fn submit_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<OpSend>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    overlapped.set_offset(0);

    let raw = resolve_fd_handle(&val.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    let token = ctx.op_token;
    let (user_data, generation) = token.parts();
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send(
                RioTarget {
                    fd: val.fd,
                    handle,
                    token,
                    buf_offset: val.buf_offset,
                    operation: "send",
                },
                &val.buf,
                ctx.registrar,
            )
            .with_ctx("outer_scope", "submit_send")
            .with_ctx("fd_fixed_index", val.fd.fixed_index())
            .with_ctx("fd_generation", val.fd.generation())
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .attach_note("RIO send submit failed")
            .trans(),
    )
}

pub(crate) fn submit_udp_send(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<UdpSend>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
    let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) }?;
    overlapped.set_offset(0);

    let raw = resolve_fd_handle(&val.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    let token = ctx.op_token;
    let (user_data, generation) = token.parts();
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send(
                RioTarget {
                    fd: val.fd,
                    handle,
                    token,
                    buf_offset: val.buf_offset,
                    operation: "udp_send",
                },
                &val.buf,
                ctx.registrar,
            )
            .with_ctx("outer_scope", "submit_udp_send")
            .with_ctx("fd_fixed_index", val.fd.fixed_index())
            .with_ctx("fd_generation", val.fd.generation())
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .attach_note("RIO udp_send submit failed")
            .trans(),
    )
}

pub(crate) fn submit_send_to(
    header: &mut OverlappedEntry,
    payload: &mut SendToPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref()? };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();

    let args = RioSendToArgs {
        fd: user.fd,
        handle,
        buf: &user.buf,
        addr_ptr: &payload.addr as *const _ as *const std::ffi::c_void,
        addr_len: payload.addr_len,
        token: ctx.op_token,
        buf_offset: user.buf_offset,
    };
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send_to(args, ctx.registrar)
            .with_ctx("outer_scope", "submit_send_to")
            .with_ctx("fd_fixed_index", user.fd.fixed_index())
            .with_ctx("fd_generation", user.fd.generation())
            .with_ctx("user_data", header.token.index())
            .with_ctx("generation", header.token.generation())
            .attach_note("RIO send_to submit failed")
            .trans(),
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
        let user = payload.user.as_mut()?;
        (user, &mut *ctx.overlapped)
    };
    overlapped.set_offset(0);
    let fd = val.fd;
    let raw = resolve_fd_handle(&fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let handle = raw.borrow();
    let args = RioUdpRecvFromArgs {
        fd,
        handle,
        recv_from_op: val,
        addr_ptr: (&mut payload.addr as *mut SockAddrStorage).cast::<std::ffi::c_void>(),
        token: ctx.op_token,
    };
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_recv_from(args, ctx.registrar)
            .with_ctx("outer_scope", "submit_udp_recv_from")
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("user_data", header.token.index())
            .with_ctx("generation", header.token.generation())
            .attach_note("RIO udp_recv_from submit failed")
            .trans(),
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
    let val = unsafe { payload.user.as_mut()? };
    let addr_bytes = unsafe {
        std::slice::from_raw_parts(
            (&payload.addr as *const SockAddrStorage).cast::<u8>(),
            std::mem::size_of::<SockAddrStorage>(),
        )
    };
    val.addr = Some(
        addr::to_socket_addr(addr_bytes)
            .attach_note("failed to parse RIO udp_recv_from source address")?,
    );
    Ok(result)
}
