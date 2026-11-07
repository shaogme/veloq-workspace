use veloq_buf::FixedBuf;
use veloq_std::{
    format,
    io::Error as IoError,
    mem::size_of,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    slice::from_raw_parts,
    sync::Arc,
};
use windows_sys::Win32::{
    Foundation::ERROR_OPERATION_ABORTED, Networking::WinSock::SOCKADDR_STORAGE,
};

use diagweave::prelude::*;
use veloq_driver_core::{
    AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, CompletionContinuation,
    CompletionSettlement, CompletionToken, InFlightWaiting, OpToken, ReceiveCompletion,
    ReceiveDelivery, ReceivePumpError, ReceivePumpEvent, ReceiveReplacementReservation,
    ReceiveRequest, ReplacementSubmit, UdpRecvMultiBackend, UserCompletionEvent,
};

use crate::{
    config::RegisteredSlot,
    driver::OverlappedRegistry,
    driver::completion::{
        protocol::{COMP_BACKEND_IOCP, IocpBackendEffect},
        settlement::SettlementService,
    },
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::addr::{SockAddrStorage, to_socket_addr},
    op::{
        IocpOpPayload, IocpSlotSpec, IocpUserPayload, RAW_UDP_RECV_MULTI_RESUME_BYTES, Slot,
        SubmitContext, UdpReceiveSubmit, UdpRecvPacket,
    },
    rio::{RioState, SocketInflightToken},
    win32::{IoCompletionPort, Overlapped},
};

pub(super) struct RawUdpCompletionServices<'a> {
    pub(super) ext: &'a Extensions,
    pub(super) rio: &'a mut RioState,
    pub(super) port: Arc<IoCompletionPort>,
    pub(super) registered_slots: &'a mut [RegisteredSlot],
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) shutting_down: bool,
}

struct RawUdpPreparedDelivery {
    delivery: ReceiveDelivery,
    output: FixedBuf,
    reservation: Option<ReceiveReplacementReservation>,
}

struct RawUdpChildCompletion {
    address: SockAddrStorage,
    address_len: i32,
    socket_inflight: SocketInflightToken,
}

enum RawUdpWork {
    Delivery(RawUdpPreparedDelivery),
    Drain,
    Final(ReceivePumpError),
}

pub(super) struct RawUdpCompletion<'a> {
    services: RawUdpCompletionServices<'a>,
}

impl<'a> RawUdpCompletion<'a> {
    pub(super) fn complete(
        event: UserCompletionEvent,
        request: Option<ReceiveRequest>,
        slot: Slot<'_, InFlightWaiting>,
        services: RawUdpCompletionServices<'a>,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        Self { services }.complete_inner(event, request, slot)
    }

    fn complete_inner(
        &mut self,
        event: UserCompletionEvent,
        request: Option<ReceiveRequest>,
        mut slot: Slot<'_, InFlightWaiting>,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let is_resume = event.res() == RAW_UDP_RECV_MULTI_RESUME_BYTES as i32;
        let cancel_requested = slot.platform().iocp_user_cancel_requested;
        let child_completion = if is_resume {
            None
        } else {
            match request.and_then(|request| Self::take_child_completion(&mut slot, request).ok()) {
                Some(completion) => Some(completion),
                None => {
                    return CompletionSettlement::Anomaly {
                        kind: CompletionAnomalyKind::backend_context(
                            COMP_BACKEND_IOCP,
                            event.token().index() as u64,
                        ),
                        attach: AnomalyAttach::from_raw_completion(event.raw()),
                        cleanup: CompletionCleanupGuard::default(),
                        effect: IocpBackendEffect::None,
                    };
                }
            }
        };
        let (completion_address, completion_address_len, effect) = match child_completion {
            Some(completion) => (
                Some(completion.address),
                completion.address_len,
                IocpBackendEffect::SocketInflight(completion.socket_inflight),
            ),
            None => (None, 0, IocpBackendEffect::None),
        };

        let work = slot.with_access_mut(|access| -> IocpResult<RawUdpWork> {
            let op = access.operation_mut().get_mut();
            let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .attach_note("raw UDP completion has the wrong payload kind"));
            };
            let raw = op.header.resolved_handle;
            payload.with_bound_parts_mut(|user, requests, _, terminal_error| {
                let pump = user.receive_pump_mut();
                if self.services.shutting_down || cancel_requested {
                    let _ = pump.cancel();
                }

                let completion_event = if let Some(address) = completion_address {
                    let mut result = if event.res() >= 0 {
                        Ok(event.res() as usize)
                    } else {
                        let code = event.res().unsigned_abs().min(i32::MAX as u32) as i32;
                        if !self.services.shutting_down
                            && !cancel_requested
                            && terminal_error.is_none()
                        {
                            *terminal_error = Some(IocpError::CompletionWait.io_report(
                                "iocp.udp_recv_multi.raw",
                                IoError::from_raw_os_error(code),
                            ));
                        }
                        Err(ReceivePumpError::ReceiveOsError { code })
                    };
                    let remote_addr = if event.res() >= 0 {
                        match Self::remote_addr(&address, completion_address_len) {
                            Ok(remote_addr) => remote_addr,
                            Err(error) => {
                                if terminal_error.is_none() {
                                    *terminal_error = Some(error);
                                }
                                result = Err(ReceivePumpError::ReceiveContextCorrupt);
                                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                            }
                        }
                    } else {
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                    };
                    pump.record_completion_observation(
                        event.res(),
                        event.res().max(0) as u32,
                        cancel_requested,
                    );
                    let request = request.ok_or_else(|| {
                        IocpError::InvalidState
                            .to_report()
                            .attach_note("raw UDP child completion has no receive identity")
                    })?;
                    match pump.complete(ReceiveCompletion {
                        request,
                        result,
                        remote_addr,
                    }) {
                        Ok(event) => Some(event),
                        Err(error) => {
                            if terminal_error.is_none() {
                                *terminal_error = Some(Self::receive_pump_report(error));
                            }
                            let _ = pump.cancel();
                            if let Some(raw) = raw {
                                UdpReceiveSubmit::cancel_raw_request_slice(requests, raw);
                            }
                            return Self::poll_final_work(pump);
                        }
                    }
                } else {
                    None
                };

                if (self.services.shutting_down || cancel_requested || terminal_error.is_some())
                    && let Some(raw) = raw
                {
                    UdpReceiveSubmit::cancel_raw_request_slice(requests, raw);
                }

                if self.services.shutting_down || cancel_requested {
                    return match completion_event {
                        Some(ReceivePumpEvent::Final { error }) => Ok(RawUdpWork::Final(error)),
                        _ => Self::poll_final_work(pump),
                    };
                }
                match completion_event {
                    Some(ReceivePumpEvent::Final { error }) => {
                        return Ok(RawUdpWork::Final(error));
                    }
                    Some(ReceivePumpEvent::Drain | ReceivePumpEvent::Ignored) => {
                        return Ok(RawUdpWork::Drain);
                    }
                    Some(ReceivePumpEvent::Retained { .. }) => {
                        return IocpError::InvalidState
                            .attach_note("raw UDP receive returned an unexpected retained event");
                    }
                    Some(ReceivePumpEvent::Pending { .. } | ReceivePumpEvent::Held { .. })
                    | None => {}
                }

                let Some(delivery) = pump.next_delivery() else {
                    if pump.pending_completions() > 0 && pump.permits_available() == 0 {
                        pump.bind_resume_token(CompletionToken::user(event.token()).raw());
                    }
                    return Self::poll_final_work(pump);
                };
                Self::prepare_delivery(user, terminal_error, delivery)
            })
        });

        let work = match work {
            Ok(Ok(work)) => work,
            Ok(Err(_)) | Err(_) => {
                return CompletionSettlement::Anomaly {
                    kind: CompletionAnomalyKind::backend_context(
                        COMP_BACKEND_IOCP,
                        event.token().index() as u64,
                    ),
                    attach: AnomalyAttach::from_raw_completion(event.raw()),
                    cleanup: CompletionCleanupGuard::default(),
                    effect,
                };
            }
        };
        Self::cancel_if_terminal(&mut slot);

        match work {
            RawUdpWork::Drain => {
                if self.services.shutting_down {
                    let active = Self::has_active_children(&mut slot);
                    if !active {
                        return SettlementService::complete_iocp_abandoned_waiting_slot(
                            slot,
                            Ok(0),
                            effect,
                        );
                    }
                }
                CompletionSettlement::Cleanup {
                    cleanup: CompletionCleanupGuard::default(),
                    continuation: CompletionContinuation::More,
                    effect,
                }
            }
            RawUdpWork::Final(error) => {
                if self
                    .services
                    .overlapped_registry
                    .has_children_for(event.token())
                {
                    return CompletionSettlement::Anomaly {
                        kind: CompletionAnomalyKind::backend_context(
                            COMP_BACKEND_IOCP,
                            event.token().index() as u64,
                        ),
                        attach: AnomalyAttach::from_raw_completion(event.raw()),
                        cleanup: CompletionCleanupGuard::default(),
                        effect,
                    };
                }
                if self.services.shutting_down {
                    return SettlementService::complete_iocp_abandoned_waiting_slot(
                        slot,
                        Ok(0),
                        effect,
                    );
                }
                let terminal_error = Self::take_terminal_error(&mut slot)
                    .unwrap_or_else(|| Self::terminal_error(error));
                SettlementService::complete_iocp_failure_slot(slot, terminal_error, effect)
            }
            RawUdpWork::Delivery(prepared) => self.finish_delivery(event, slot, prepared, effect),
        }
    }

    fn take_child_completion(
        slot: &mut Slot<'_, InFlightWaiting>,
        request: ReceiveRequest,
    ) -> IocpResult<RawUdpChildCompletion> {
        slot.with_access_mut(|access| -> IocpResult<RawUdpChildCompletion> {
            let op = access.operation_mut().get_mut();
            let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .attach_note("raw UDP completion has the wrong payload kind"));
            };
            let child = payload.raw_request_mut(request.slot_id).ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .with_ctx("slot_id", request.slot_id)
                    .attach_note("completed raw UDP child request owner is missing")
            })?;
            if child.slot_id() != request.slot_id
                || child.header().receive_request != Some(request)
                || !child.header().in_flight
            {
                return Err(IocpError::InvalidState
                    .to_report()
                    .with_ctx("request_id", request.request_id)
                    .with_ctx("slot_id", request.slot_id)
                    .with_ctx("slot_generation", request.slot_generation)
                    .attach_note("raw UDP child completion identity is stale or duplicated"));
            }
            let header = child.header_mut();
            let Some(socket_inflight) = header.socket_inflight.take() else {
                return Err(IocpError::InvalidState
                    .to_report()
                    .with_ctx("request_id", request.request_id)
                    .attach_note("completed raw UDP child has no socket inflight token"));
            };
            header.in_flight = false;
            header.receive_request = None;
            Ok(RawUdpChildCompletion {
                address: *child.address(),
                address_len: child.address_len(),
                socket_inflight,
            })
        })
        .map_err(|_| {
            IocpError::InvalidState
                .to_report()
                .attach_note("failed to access raw UDP child completion state")
        })?
    }

    fn prepare_delivery(
        user: &mut crate::op::UdpRecvMulti,
        terminal_error: &mut Option<Report<IocpError>>,
        delivery: ReceiveDelivery,
    ) -> IocpResult<RawUdpWork> {
        let capacity =
            NonZeroUsize::new(delivery.len().max(1)).expect("UDP output capacity must be non-zero");
        let mut output = match FixedBuf::alloc_heap(capacity, 0) {
            Ok(output) => output,
            Err(error) => {
                let report = IocpError::Submission
                    .to_report()
                    .with_ctx("buffer_capacity", capacity.get())
                    .attach_note(format!("failed to allocate raw UDP output buffer: {error}"));
                return Self::abort_delivery(
                    user,
                    terminal_error,
                    delivery,
                    ReceivePumpError::ReceiveBufferExhausted,
                    report,
                );
            }
        };
        let pump = user.receive_pump_mut();
        let source = pump
            .slot(delivery.request().slot_id)
            .and_then(|receive_slot| receive_slot.completed_data())
            .ok_or_else(|| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("raw UDP receive slot has no completed payload")
            });
        let source = match source {
            Ok(source) if source.len() >= delivery.len() => source,
            Ok(source) => {
                let available = source.len();
                let expected = delivery.len();
                return Self::abort_delivery(
                    user,
                    terminal_error,
                    delivery,
                    ReceivePumpError::ReceiveContextCorrupt,
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("received", available)
                        .with_ctx("expected", expected)
                        .attach_note("raw UDP completion exceeds written receive data"),
                );
            }
            Err(error) => {
                return Self::abort_delivery(
                    user,
                    terminal_error,
                    delivery,
                    ReceivePumpError::ReceiveContextCorrupt,
                    error,
                );
            }
        };
        output.spare_capacity_mut()[..delivery.len()].copy_from_slice(&source[..delivery.len()]);
        let reservation = if delivery.replacement_required() {
            match pump.reserve_replacement(&delivery) {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    return Self::abort_delivery(
                        user,
                        terminal_error,
                        delivery,
                        error,
                        Self::receive_pump_report(error),
                    );
                }
            }
        } else {
            None
        };
        Ok(RawUdpWork::Delivery(RawUdpPreparedDelivery {
            delivery,
            output,
            reservation,
        }))
    }

    fn abort_delivery(
        user: &mut crate::op::UdpRecvMulti,
        terminal_error: &mut Option<Report<IocpError>>,
        delivery: ReceiveDelivery,
        pump_error: ReceivePumpError,
        report: Report<IocpError>,
    ) -> IocpResult<RawUdpWork> {
        if terminal_error.is_none() {
            *terminal_error = Some(report);
        }
        let pump = user.receive_pump_mut();
        pump.abort_delivery(delivery, pump_error)
            .map_err(Self::receive_pump_report)?;
        let _ = pump.cancel();
        Self::poll_final_work(pump)
    }

    fn poll_final_work(pump: &mut veloq_driver_core::ReceivePumpState) -> IocpResult<RawUdpWork> {
        let event = pump.poll_final().map_err(Self::receive_pump_report)?;
        match event {
            ReceivePumpEvent::Final { error } => Ok(RawUdpWork::Final(error)),
            _ if pump.snapshot().final_emitted => Ok(RawUdpWork::Final(
                pump.terminal_error()
                    .unwrap_or(ReceivePumpError::DriverShutdown),
            )),
            _ => Ok(RawUdpWork::Drain),
        }
    }

    fn finish_delivery(
        &mut self,
        event: UserCompletionEvent,
        mut slot: Slot<'_, InFlightWaiting>,
        prepared: RawUdpPreparedDelivery,
        effect: IocpBackendEffect,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let RawUdpPreparedDelivery {
            delivery,
            output,
            reservation,
        } = prepared;
        let mut replacement_error = None;
        let replacement = if let Some(reservation) = reservation {
            match self.submit_replacement(&mut slot, event.token(), reservation) {
                Ok(request_id) => ReplacementSubmit::Submitted {
                    request_id,
                    reservation,
                },
                Err(error) => {
                    replacement_error = Some(error);
                    ReplacementSubmit::Failed { reservation }
                }
            }
        } else {
            ReplacementSubmit::NotSubmitted
        };

        let settled = slot.with_access_mut(
            |access| -> IocpResult<(UdpRecvPacket, CompletionContinuation)> {
                let op = access.operation_mut().get_mut();
                let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .attach_note("raw UDP delivery has the wrong payload kind"));
                };
                let raw = op.header.resolved_handle;
                payload.with_bound_parts_mut(|user, requests, _, terminal_error| {
                    if let Some(error) = replacement_error.take()
                        && terminal_error.is_none()
                    {
                        *terminal_error = Some(error);
                    }
                    let record = user
                        .receive_pump_mut()
                        .finish_delivery(delivery, output, replacement)
                        .map_err(Self::receive_pump_report)?;
                    if matches!(replacement, ReplacementSubmit::Failed { .. })
                        && let Some(raw) = raw
                    {
                        UdpReceiveSubmit::cancel_raw_request_slice(requests, raw);
                    }
                    Ok((record.packet, record.continuation))
                })
            },
        );
        let (packet, continuation) = match settled {
            Ok(Ok(packet)) => packet,
            Ok(Err(_)) | Err(_) => {
                return CompletionSettlement::Anomaly {
                    kind: CompletionAnomalyKind::backend_context(
                        COMP_BACKEND_IOCP,
                        event.token().index() as u64,
                    ),
                    attach: AnomalyAttach::from_raw_completion(event.raw()),
                    cleanup: CompletionCleanupGuard::default(),
                    effect,
                };
            }
        };
        let packet_len = packet.buf.len();
        let payload = IocpUserPayload::UdpRecvPacket(packet);
        let mut terminal_error = Self::take_terminal_error(&mut slot);
        if continuation.is_final() && terminal_error.is_some() {
            let terminal_error = terminal_error.take().expect("terminal error exists");
            let terminal_res = crate::error::iocp_report_to_event_res(&terminal_error);
            return CompletionSettlement::UserThenTerminal {
                event: UserCompletionEvent::from_parts(
                    COMP_BACKEND_IOCP,
                    event.token(),
                    packet_len.min(i32::MAX as usize) as i32,
                    0,
                ),
                payload,
                detail: Some(Ok(packet_len)),
                cleanup: CompletionCleanupGuard::default(),
                effect,
                terminal_event: UserCompletionEvent::from_parts(
                    COMP_BACKEND_IOCP,
                    event.token(),
                    terminal_res,
                    0,
                ),
                terminal_error,
                terminal_cleanup: CompletionCleanupGuard::default(),
                terminal_effect: IocpBackendEffect::None,
            };
        }
        CompletionSettlement::User {
            event: UserCompletionEvent::from_parts(
                COMP_BACKEND_IOCP,
                event.token(),
                packet_len.min(i32::MAX as usize) as i32,
                0,
            ),
            payload,
            detail: Some(Ok(packet_len)),
            cleanup: CompletionCleanupGuard::default(),
            continuation,
            effect,
        }
    }

    fn submit_replacement(
        &mut self,
        slot: &mut Slot<'_, InFlightWaiting>,
        token: OpToken,
        reservation: ReceiveReplacementReservation,
    ) -> IocpResult<u64> {
        self.services.overlapped_registry.reserve_children(1)?;
        let overlapped = slot.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);
        let request_id = slot
            .with_access_mut(|access| {
                let op = access.operation_mut().get_mut();
                let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                    return Err(IocpError::InvalidState
                        .to_report()
                        .attach_note("raw UDP receive replacement has the wrong payload kind"));
                };
                let mut context = SubmitContext {
                    port: self.services.port.clone(),
                    overlapped,
                    op_token: token,
                    completion_token: CompletionToken::user(token),
                    ext: self.services.ext,
                    registered_slots: self.services.registered_slots,
                    overlapped_registry: Some(self.services.overlapped_registry),
                    buffer_pool: None,
                    rio: self.services.rio,
                };
                UdpReceiveSubmit::submit_replacement(
                    &mut op.header,
                    payload,
                    &mut context,
                    reservation,
                )
            })
            .map_err(|_| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("failed to access raw UDP receive replacement state")
            })??;
        let registered = slot
            .with_access_mut(|access| {
                let op = access.operation().get_ref();
                let IocpOpPayload::UdpRecvMulti(payload) = &op.payload else {
                    return false;
                };
                let Some(child) = payload
                    .raw_requests()
                    .and_then(|requests| requests.get(reservation.slot_id() as usize))
                else {
                    return false;
                };
                child.header().receive_request.is_some_and(|request| {
                    request.request_id == request_id
                        && self.services.overlapped_registry.register_child(
                            &child.header().inner,
                            token,
                            request,
                        )
                })
            })
            .map_err(|_| {
                IocpError::InvalidState
                    .to_report()
                    .attach_note("failed to access raw UDP replacement owner")
            })?;
        if !registered {
            return IocpError::InvalidState
                .with_ctx("request_id", request_id)
                .with_ctx("slot_id", reservation.slot_id())
                .attach_note("raw UDP replacement OVERLAPPED owner could not be registered");
        }
        Ok(request_id)
    }

    fn has_active_children(slot: &mut Slot<'_, InFlightWaiting>) -> bool {
        slot.with_access_mut(|access| {
            let op = access.operation().get_ref();
            let IocpOpPayload::UdpRecvMulti(payload) = &op.payload else {
                return false;
            };
            payload
                .raw_requests()
                .is_some_and(|requests| requests.iter().any(|request| request.header().in_flight))
        })
        .unwrap_or(true)
    }

    fn cancel_if_terminal(slot: &mut Slot<'_, InFlightWaiting>) {
        let _ = slot.with_access_mut(|access| {
            let op = access.operation_mut().get_mut();
            let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                return;
            };
            // SAFETY: The payload remains bound to this live slot while its active children drain. / 子请求排空期间，该 payload 仍绑定到此活动槽位。
            let terminal = unsafe { payload.user.as_ref() }
                .is_ok_and(|user| user.receive_pump().terminal_error().is_some());
            if terminal && let Some(raw) = op.header.resolved_handle {
                UdpReceiveSubmit::cancel_raw_requests(payload, raw);
            }
        });
    }

    fn take_terminal_error(slot: &mut Slot<'_, InFlightWaiting>) -> Option<Report<IocpError>> {
        slot.with_access_mut(|access| {
            let op = access.operation_mut().get_mut();
            let IocpOpPayload::UdpRecvMulti(payload) = &mut op.payload else {
                return None;
            };
            payload.take_terminal_error()
        })
        .ok()
        .flatten()
    }

    fn remote_addr(address: &SockAddrStorage, raw_address_len: i32) -> IocpResult<SocketAddr> {
        let address_len = usize::try_from(raw_address_len).map_err(|_| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("address_length", raw_address_len)
                .attach_note("raw UDP completion returned a negative address length")
        })?;
        if address_len > size_of::<SOCKADDR_STORAGE>() {
            return IocpError::InvalidState
                .with_ctx("address_length", address_len)
                .attach_note("raw UDP completion address exceeds SOCKADDR_STORAGE");
        }
        let address_bytes = unsafe {
            from_raw_parts(
                (&address.0 as *const SOCKADDR_STORAGE).cast::<u8>(),
                address_len,
            )
        };
        to_socket_addr(address_bytes).attach_note("failed to decode raw UDP peer address")
    }

    fn terminal_error(error: ReceivePumpError) -> Report<IocpError> {
        if error == ReceivePumpError::ReceiveCancelled {
            return IocpError::CompletionWait.io_report(
                "iocp.udp_recv_multi.cancel",
                IoError::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
            );
        }
        Self::receive_pump_report(error)
    }

    fn receive_pump_report(error: ReceivePumpError) -> Report<IocpError> {
        IocpError::CompletionWait
            .to_report()
            .with_ctx("receive_pump_error", format!("{error:?}"))
            .attach_note("IOCP UDP receive pump could not complete its transition")
    }
}
