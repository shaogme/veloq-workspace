mod accept;
mod connect;
mod socket;
mod udp_receive;

pub(crate) use accept::{
    completion_cleanup_close_socket, on_complete_accept, on_complete_accept_multi,
    orphan_cleanup_accept_multi, submit_accept, submit_accept_multi,
};
pub(crate) use connect::{on_complete_connect, submit_connect, submit_udp_connect};
pub(crate) use udp_receive::UdpReceiveSubmit;

use diagweave::prelude::*;
use veloq_buf::{BufPool, FixedBuf};
use veloq_driver_core::{ReceivePumpConfig, ReceivePumpState, ReceiveSlot};
use veloq_std::{format, num::NonZeroUsize, string::ToString, time::Duration, vec::Vec};
use windows_sys::Win32::Networking::WinSock::{SOCKADDR, WSABUF, WSASendTo};

use crate::{
    error::{IocpError, IocpResult},
    op::{
        KernelRef, OpSend, OverlappedEntry, Recv, RecvMultiPayload, RecvProvidedPayload,
        SendToPayload, SubmitContext, UdpSend,
        submit::{SubmissionResult, mark_header_in_flight, resolve_fd_handle, unpack_kernel_ref},
    },
    rio::{RioOpKind, RioSendToArgs, RioTarget},
};

use self::socket::{RawSocketRequest, SocketSubmission, invalid_buffer_range};

pub(crate) const RECV_PROVIDED_BUFFER_CAPACITY: usize = 8192;
const RECV_MULTI_BUFFER_CAPACITY: usize = 8192;
const RECV_MULTI_INFLIGHT: usize = 1;
const RECV_MULTI_QUEUE_CAPACITY: usize = 4;

// ============================================================================
// Network Operations
// ============================================================================

pub(crate) fn submit_recv_provided(
    header: &mut OverlappedEntry,
    payload: &mut RecvProvidedPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: the payload is bound to the live slot before submission.
    let user = unsafe { payload.user.as_mut()? };
    if payload.buffer.is_some() {
        return IocpError::InvalidState
            .with_ctx("operation", "recv_provided")
            .attach_note("RecvProvided payload still owns a previous receive buffer");
    }

    let mut buffer = FixedBuf::alloc_heap(
        NonZeroUsize::new(RECV_PROVIDED_BUFFER_CAPACITY)
            .expect("RecvProvided buffer capacity must be non-zero"),
        0,
    )
    .map_err(|error| {
        IocpError::Submission
            .to_report()
            .with_ctx("operation", "recv_provided")
            .with_ctx("buffer_capacity", RECV_PROVIDED_BUFFER_CAPACITY)
            .attach_note(format!(
                "failed to allocate backend receive buffer: {error}"
            ))
    })?;
    let fd = user.fd;
    let raw = resolve_fd_handle(&fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    let result = SocketSubmission::submit_wsa_recv(
        RawSocketRequest::new(header, fd, raw, ctx),
        &mut buffer,
        0,
        "recv_provided",
    );
    let result = result.with_ctx("operation", "recv_provided").trans();
    if matches!(result, Ok(SubmissionResult::Pending)) {
        let request_generation = payload.request_generation.next();
        payload.request_generation = request_generation;
        header.request_generation = request_generation;
        payload.buffer = Some(buffer);
    }
    result
}

pub(crate) fn submit_recv_multi(
    header: &mut OverlappedEntry,
    payload: &mut RecvMultiPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    if payload.pump.is_some() {
        return Err(IocpError::InvalidState
            .to_report()
            .attach_note("RIO recv_multi payload already owns a receive pump"));
    }
    // SAFETY: the vtable submit shim binds the user payload before invoking this function.
    let user = unsafe { payload.user.as_mut()? };
    let fd = user.fd;
    let raw = resolve_fd_handle(&fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);
    if let Some(buffer) = payload.raw_buffer.as_mut() {
        let result = SocketSubmission::submit_wsa_recv(
            RawSocketRequest::new(&mut *header, fd, raw, &mut *ctx),
            buffer,
            0,
            "recv_multi",
        );
        if matches!(result, Ok(SubmissionResult::Pending)) {
            header.request_generation = header.request_generation.next();
        }
        return result;
    }

    let handle = raw.borrow();
    let config = ReceivePumpConfig::new(
        NonZeroUsize::new(RECV_MULTI_INFLIGHT).expect("recv_multi inflight capacity is non-zero"),
        NonZeroUsize::new(RECV_MULTI_QUEUE_CAPACITY)
            .expect("recv_multi queue capacity is non-zero"),
        NonZeroUsize::new(RECV_MULTI_BUFFER_CAPACITY)
            .expect("recv_multi buffer capacity is non-zero"),
        Duration::from_secs(5),
    );
    let capacity = NonZeroUsize::new(RECV_MULTI_BUFFER_CAPACITY)
        .expect("recv_multi buffer capacity is non-zero");
    let mut buffer = ctx
        .buffer_pool
        .and_then(|pool| pool.alloc(capacity, 0))
        .or_else(|| FixedBuf::alloc_heap(capacity, 0).ok())
        .ok_or_else(|| {
            IocpError::Submission
                .to_report()
                .with_ctx("operation", "recv_multi")
                .with_ctx("buffer_capacity", capacity.get())
                .attach_note("failed to allocate backend receive buffer")
        })?;
    if !SocketSubmission::uses_rio(raw, &buffer) {
        let result = SocketSubmission::submit_wsa_recv(
            RawSocketRequest::new(&mut *header, fd, raw, &mut *ctx),
            &mut buffer,
            0,
            "recv_multi",
        );
        if matches!(result, Ok(SubmissionResult::Pending)) {
            header.request_generation = header.request_generation.next();
            payload.raw_buffer = Some(buffer);
        }
        return result;
    }

    let mut slots = Vec::with_capacity(RECV_MULTI_INFLIGHT);
    slots.push(ReceiveSlot::new(0, buffer));

    let mut pump =
        ReceivePumpState::try_new_tcp(config, slots.into_boxed_slice()).map_err(|_| {
            IocpError::InvalidState
                .to_report()
                .attach_note("RIO recv_multi receive pump configuration is invalid")
        })?;
    let submit_result = ctx
        .rio
        .try_submit_recv_multi_initial(
            RioTarget {
                fd,
                handle,
                token: ctx.op_token,
                buf_offset: 0,
                operation: "recv_multi",
            },
            RioOpKind::TcpRecvMulti,
            &mut pump,
        )
        .with_ctx("outer_scope", "submit_recv_multi")
        .with_ctx("fd", fd.to_string())
        .with_ctx("user_data", ctx.op_token.index())
        .with_ctx("generation", ctx.op_token.generation())
        .attach_note("RIO recv_multi initial arm failed")
        .trans()
        .map(|()| SubmissionResult::Pending);
    let result = mark_header_in_flight(header, submit_result);
    if matches!(result, Ok(SubmissionResult::Pending)) {
        payload.pump = Some(pump);
    }
    result
}

/// # Safety
///
/// The caller must provide the live slot-owned payload and completion header for the request.
pub(crate) unsafe fn on_complete_recv_provided(
    header: &mut OverlappedEntry,
    payload: &mut RecvProvidedPayload,
    result: usize,
) -> IocpResult<usize> {
    if payload.request_generation != header.request_generation {
        return IocpError::InvalidState
            .with_ctx("payload_request_generation", payload.request_generation)
            .with_ctx("header_request_generation", header.request_generation)
            .attach_note("stale overlapped RecvProvided completion generation");
    }

    let buffer = payload
        .buffer
        .as_mut()
        .ok_or(IocpError::InvalidState)
        .attach_note("RecvProvided completion has no backend-owned buffer")?;
    if result > buffer.capacity() {
        return IocpError::CompletionWait
            .with_ctx("received", result)
            .with_ctx("buffer_capacity", buffer.capacity())
            .attach_note("overlapped RecvProvided completion exceeds buffer capacity");
    }
    buffer.set_len(result);
    Ok(result)
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
    if !SocketSubmission::uses_rio(raw, &val.buf) {
        return SocketSubmission::submit_wsa_recv(
            RawSocketRequest::new(header, fd, raw, ctx),
            &mut val.buf,
            val.buf_offset,
            "recv",
        );
    }
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
            )
            .with_ctx("outer_scope", "submit_recv")
            .with_ctx("fd", val.fd.to_string())
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .attach_note("RIO recv submit failed")
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
    if !SocketSubmission::uses_rio(raw, &val.buf) {
        return SocketSubmission::submit_wsa_send(
            RawSocketRequest::new(header, val.fd, raw, ctx),
            &val.buf,
            val.buf_offset,
            "send",
        );
    }
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
            )
            .with_ctx("outer_scope", "submit_send")
            .with_ctx("fd", val.fd.to_string())
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
    if !SocketSubmission::uses_rio(raw, &val.buf) {
        return SocketSubmission::submit_wsa_send(
            RawSocketRequest::new(header, val.fd, raw, ctx),
            &val.buf,
            val.buf_offset,
            "udp_send",
        );
    }
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
            )
            .with_ctx("outer_scope", "submit_udp_send")
            .with_ctx("fd", val.fd.to_string())
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
    if !SocketSubmission::uses_rio(raw, &user.buf) {
        let (ptr, len) = user
            .buf
            .checked_write_range(user.buf_offset)
            .map_err(|error| invalid_buffer_range("send_to", error))?;
        let wsabuf = WSABUF {
            len,
            buf: ptr.cast_mut(),
        };
        return RawSocketRequest::new(header, user.fd, raw, ctx).submit(|socket, overlapped| {
            let mut bytes_sent = 0;
            let result = unsafe {
                WSASendTo(
                    socket,
                    &wsabuf,
                    1,
                    &mut bytes_sent,
                    0,
                    &payload.addr.0 as *const _ as *const SOCKADDR,
                    payload.addr_len,
                    overlapped.cast(),
                    None,
                )
            };
            SocketSubmission::wsa_submit_result(result, "send_to")
        });
    }
    let handle = raw.borrow();

    let args = RioSendToArgs {
        fd: user.fd,
        handle,
        buf: &user.buf,
        addr: &payload.addr,
        addr_len: payload.addr_len,
        token: ctx.op_token,
        buf_offset: user.buf_offset,
    };
    mark_header_in_flight(
        header,
        ctx.rio
            .try_submit_send_to(args)
            .with_ctx("outer_scope", "submit_send_to")
            .with_ctx("fd", user.fd.to_string())
            .with_ctx("user_data", header.token.index())
            .with_ctx("generation", header.token.generation())
            .attach_note("RIO send_to submit failed")
            .trans(),
    )
}
