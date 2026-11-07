use diagweave::prelude::*;
use tracing::error;
use veloq_driver_core::{
    CompletionToken, ReceivePermitNotifier, ReceiveReplacementReservation, ReceiveRequest,
    UdpRecvMultiBackend,
};
use veloq_std::{
    format, ptr,
    string::ToString,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    config::RawHandle,
    error::{IocpError, IocpResult},
    op::{
        OverlappedEntry, RAW_UDP_RECV_MULTI_RESUME_BYTES, RawUdpReceiveRequest, SubmitContext,
        UdpRecvMultiPayload,
        submit::{SubmissionOps, SubmissionResult},
    },
    rio::{RioError, RioOpKind, RioTarget},
    win32::{CancelRequestResult, IoCompletionPort, Overlapped},
};

use super::socket::{RawSocketRequest, SocketSubmission};

struct UdpReceivePermitNotifier {
    port: Arc<IoCompletionPort>,
    token: AtomicU64,
}

impl ReceivePermitNotifier for UdpReceivePermitNotifier {
    fn notify(&self) {
        let token = self.token.load(Ordering::Acquire);
        if token != 0 {
            // SAFETY: A null OVERLAPPED is valid for a posted token notification. / 空 OVERLAPPED 可用于投递 token 通知。
            let _ = unsafe {
                self.port.post(
                    RAW_UDP_RECV_MULTI_RESUME_BYTES,
                    token as usize,
                    ptr::null_mut(),
                )
            };
        }
    }

    fn bind_resume_token(&self, token: u64) {
        self.token.store(token, Ordering::Release);
    }
}

fn udp_receive_permit_notifier(port: Arc<IoCompletionPort>) -> Arc<dyn ReceivePermitNotifier> {
    let notifier = Arc::new(UdpReceivePermitNotifier {
        port,
        token: AtomicU64::new(0),
    });
    // SAFETY: The allocation contains UdpReceivePermitNotifier, which implements ReceivePermitNotifier. / 分配对象实现了 ReceivePermitNotifier。
    unsafe { Arc::cast_unsized(notifier, |ptr| ptr as *const dyn ReceivePermitNotifier) }
}

pub(crate) struct UdpReceiveSubmit;

impl UdpReceiveSubmit {
    pub(crate) fn submit_initial(
        header: &mut OverlappedEntry,
        payload: &mut UdpRecvMultiPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        // SAFETY: payload.user is bound to the live slot before submission. / 提交前 payload.user 已绑定到活动槽位。
        let (fd, raw, target_depth, all_registered) = {
            let user = unsafe { payload.user().as_ref()? };
            let fd = user.fd();
            let target_depth = user.receive_pump().snapshot().target_depth;
            let raw = SubmissionOps::resolve_fd_handle(&fd, &*ctx.registered_slots)?;
            let slot_count = u32::try_from(target_depth).map_err(|_| {
                IocpError::InvalidInput
                    .to_report()
                    .with_ctx("target_depth", target_depth)
                    .attach_note("UDP receive depth exceeds the supported slot id range")
            })?;
            *header.resolved_handle_mut() = Some(raw);
            let mut all_registered = true;
            for slot_id in 0..slot_count {
                let Some(slot) = user.receive_pump().slot(slot_id) else {
                    return IocpError::InvalidState
                        .with_ctx("slot_id", slot_id)
                        .attach_note("UDP receive pump is missing a configured buffer slot");
                };
                if !SocketSubmission::uses_rio(raw, slot.data_buf()) {
                    all_registered = false;
                }
            }
            (fd, raw, target_depth, all_registered)
        };
        if raw.raw().uses_rio() && !all_registered {
            return IocpError::Rio(RioError::BufferRegistrationRequired)
                .with_ctx("socket_mode", "rio")
                .attach_note("RIO UDP receive requires every pump buffer to be registered");
        }
        if !raw.raw().uses_rio() {
            payload.try_allocate_raw_requests(ctx.op_token, target_depth)?;
            ctx.overlapped_registry
                .as_deref_mut()
                .ok_or_else(|| {
                    IocpError::InvalidState
                        .to_report()
                        .attach_note("IOCP raw UDP submit has no OVERLAPPED owner registry")
                })?
                .reserve_children(target_depth)?;
            let port = ctx.port.clone();
            let token = ctx.op_token;
            let result = payload.with_bound_parts_mut(|user, requests, request_id, terminal_error| {
                let pump = user.receive_pump_mut();
                pump.set_permit_notifier(udp_receive_permit_notifier(port))
                    .map_err(|error| {
                        IocpError::InvalidState
                            .to_report()
                            .with_ctx("receive_pump_error", format!("{error:?}"))
                            .attach_note("failed to install overlapped UDP receive wakeup")
                    })?;
                pump.bind_resume_token(CompletionToken::user(token).raw());
                pump.start().map_err(|error| {
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("receive_pump_error", format!("{error:?}"))
                        .attach_note("failed to start overlapped UDP receive pump")
                })?;
                let mut startup_error = None;
                while let Some(submission) = pump.next_submission() {
                    let Some(next_request_id) = request_id.checked_add(1) else {
                        let _ = pump.fail_initial_submit(submission);
                        startup_error = Some(
                            IocpError::InvalidState
                                .to_report()
                                .attach_note("UDP receive request id overflow"),
                        );
                        break;
                    };
                    let Some(buffer) = pump
                        .slot_mut(submission.slot_id)
                        .map(|slot| slot.data_buf_mut())
                    else {
                        let _ = pump.fail_initial_submit(submission);
                        startup_error = Some(
                            IocpError::InvalidState
                                .to_report()
                                .with_ctx("slot_id", submission.slot_id)
                                .attach_note("UDP receive slot disappeared before submission"),
                        );
                        break;
                    };
                    let Some(child) = requests.get_mut(submission.slot_id as usize) else {
                        let _ = pump.fail_initial_submit(submission);
                        startup_error = Some(
                            IocpError::InvalidState
                                .to_report()
                                .with_ctx("slot_id", submission.slot_id)
                                .attach_note("UDP child request owner is missing"),
                        );
                        break;
                    };
                    if child.slot_id() != submission.slot_id {
                        let _ = pump.fail_initial_submit(submission);
                        startup_error = Some(
                            IocpError::InvalidState
                                .to_report()
                                .with_ctx("slot_id", submission.slot_id)
                                .attach_note("UDP child request owner does not match its receive slot"),
                        );
                        break;
                    }
                    child.header_mut().reset_for_token(token);
                    let result = child.with_submit_fields(|child_header, address, address_len| {
                        ctx.overlapped = child_header.inner_mut();
                        SocketSubmission::submit_wsa_recv_from(
                            RawSocketRequest::new(child_header, fd, raw, ctx),
                            buffer,
                            address,
                            address_len,
                        )
                    });
                    match result {
                        Ok(SubmissionResult::Pending) => {
                            let request = match pump.submit_initial(submission, next_request_id) {
                                Ok(request) => request,
                                Err(error) => {
                                    startup_error = Some(
                                        IocpError::InvalidState
                                            .to_report()
                                            .with_ctx("receive_pump_error", format!("{error:?}"))
                                            .attach_note("failed to commit initial UDP receive submission"),
                                    );
                                    break;
                                }
                            };
                            *child.header_mut().receive_request_mut() = Some(request);
                            let child_header = child.header_mut();
                            *child_header.request_generation_mut() =
                                child_header.request_generation().next();
                            *request_id = next_request_id;
                        }
                        Ok(_) => {
                            let _ = pump.fail_initial_submit(submission);
                            startup_error = Some(IocpError::InvalidState.to_report().attach_note(
                                "initial overlapped UDP receive returned a non-pending state",
                            ));
                            break;
                        }
                        Err(error) => {
                            let _ = pump.fail_initial_submit(submission);
                            startup_error = Some(error);
                            break;
                        }
                    }
                }
                ctx.overlapped = header.inner_mut();
                if let Some(error) = startup_error {
                    if pump.actual_in_flight() == 0 {
                        return Err(error);
                    }
                    if terminal_error.is_none() {
                        *terminal_error = Some(error);
                    }
                    Self::cancel_raw_request_slice(requests, raw);
                    *header.in_flight_mut() = true;
                    return Ok(SubmissionResult::Pending);
                }
                if !pump.is_ready() {
                    return IocpError::InvalidState
                        .with_ctx("actual_in_flight", pump.actual_in_flight())
                        .with_ctx("target_depth", target_depth)
                        .attach_note("raw UDP receive startup did not reach the configured depth");
                }
                *header.in_flight_mut() = true;
                Ok(SubmissionResult::Pending)
            })?;
            let registry = ctx.overlapped_registry.as_deref_mut().ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("IOCP raw UDP submit has no OVERLAPPED owner registry")
            })?;
            for child in payload.raw_requests().into_iter().flatten() {
                let child_header = child.header();
                if child_header.in_flight()
                    && let Some(request) = child_header.receive_request()
                {
                    debug_assert!(registry.register_child(child_header.inner(), token, request));
                }
            }
            return Ok(result);
        }
        let user = unsafe { payload.user_mut().as_mut()? };
        let handle = raw.borrow();
        SubmissionOps::mark_header_in_flight(
            header,
            ctx.rio
                .try_submit_recv_multi_initial(
                    RioTarget {
                        fd,
                        handle,
                        token: ctx.op_token,
                        buf_offset: 0,
                        operation: "udp_recv_multi",
                    },
                    RioOpKind::UdpRecvMulti,
                    user.receive_pump_mut(),
                )
                .with_ctx("outer_scope", "submit_udp_recv_multi")
                .with_ctx("fd", fd.to_string())
                .with_ctx("user_data", ctx.op_token.index())
                .with_ctx("generation", ctx.op_token.generation())
                .attach_note("RIO udp_recv_multi initial arm failed")
                .trans()
                .map(|()| SubmissionResult::Pending),
        )
    }

    pub(crate) fn submit_replacement(
        header: &mut OverlappedEntry,
        payload: &mut UdpRecvMultiPayload,
        ctx: &mut SubmitContext,
        reservation: ReceiveReplacementReservation,
    ) -> IocpResult<u64> {
        // SAFETY: 提交入口会将用户操作绑定到槽位，直到完成事件完成路由。
        // SAFETY: The submit shim binds the user operation until its completion is routed.
        let fd = unsafe { payload.user().as_ref()?.fd() };
        let raw = SubmissionOps::resolve_fd_handle(&fd, &*ctx.registered_slots)?;
        payload.with_bound_parts_mut(|user, requests, request_id, _| {
            let next_request_id = request_id.checked_add(1).ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("UDP receive request id overflow")
            })?;
            let buffer = user
                .receive_pump_mut()
                .slot_mut(reservation.slot_id())
                .map(|slot| slot.data_buf_mut())
                .ok_or_else(|| {
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("slot_id", reservation.slot_id())
                        .attach_note("UDP receive replacement slot is missing")
                })?;
            let child = requests
                .get_mut(reservation.slot_id() as usize)
                .ok_or_else(|| {
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("slot_id", reservation.slot_id())
                        .attach_note("UDP replacement child request owner is missing")
                })?;
            if child.slot_id() != reservation.slot_id() {
                return IocpError::InvalidState
                    .with_ctx("slot_id", reservation.slot_id())
                    .attach_note("UDP replacement child request owner does not match its slot");
            }
            let request_generation = child.header().request_generation().next();
            child.header_mut().reset_for_token(ctx.op_token);
            let result = child.with_submit_fields(|child_header, address, address_len| {
                ctx.overlapped = child_header.inner_mut();
                SocketSubmission::submit_wsa_recv_from(
                    RawSocketRequest::new(child_header, fd, raw, ctx),
                    buffer,
                    address,
                    address_len,
                )
            })?;
            if !matches!(result, SubmissionResult::Pending) {
                return IocpError::InvalidState
                    .attach_note("overlapped UDP receive replacement was not submitted");
            }
            *child.header_mut().receive_request_mut() = Some(ReceiveRequest {
                request_id: next_request_id,
                slot_id: reservation.slot_id(),
                slot_generation: reservation.slot_generation(),
            });
            *request_id = next_request_id;
            *child.header_mut().request_generation_mut() = request_generation;
            *header.in_flight_mut() = true;
            Ok(next_request_id)
        })
    }

    pub(crate) fn cancel_raw_requests(payload: &mut UdpRecvMultiPayload, raw: RawHandle) {
        let Some(requests) = payload.raw_requests_mut() else {
            return;
        };
        Self::cancel_raw_request_slice(requests, raw);
    }

    pub(crate) fn cancel_raw_request_slice(requests: &mut [RawUdpReceiveRequest], raw: RawHandle) {
        for request in requests {
            let slot_id = request.slot_id();
            let header = request.header_mut();
            if !header.in_flight() {
                continue;
            }
            let overlapped = header.inner_mut() as *mut Overlapped;
            match unsafe { IoCompletionPort::cancel_request(raw.raw().as_handle(), overlapped) } {
                Ok(CancelRequestResult::Submitted | CancelRequestResult::NotFound) => {}
                Err(error) => error!(
                    error = ?error,
                    slot_id,
                    "failed to cancel raw UDP receive child"
                ),
            }
        }
    }
}
