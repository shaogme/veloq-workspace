//! Actor coordination and completion routing for the RIO runtime.

use crate::{
    IoFd,
    config::{BorrowedRawHandle, RawHandle, SocketKey},
    driver::{IocpDriverCompletionDiagnostics, RIO_EVENT_TOKEN, completion::COMP_BACKEND_RIO},
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::{IocpOpRegistry, IocpSlotSpec, IocpUserPayload, Slot},
    rio::{
        RioEnv, RioState, SocketInflightIdentity, SocketLifecycleState,
        core::{
            RioAddrReservation, RioBufferLeaseToken, RioCompletionKind, RioOpKind,
            RioOpRequestInit, RioRequestContextDecode, RioRq, rio_result_to_event_res,
        },
        error::{RioError, RioResult},
        runtime::RioTarget,
    },
};
use diagweave::prelude::*;
use veloq_buf::{BufferRegistrar, FixedBuf};
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionBackendHooks,
        CompletionBackendIngressAction, CompletionCleanupGuard, CompletionContinuation,
        CompletionControl, CompletionFailure, CompletionFlowExt, CompletionIngress,
        CompletionSettlement, CompletionSource, PlatformOp, RawCompletion, SharedCompletionTable,
        UserCompletionEvent,
    },
    op::types::{OpKind, ProvidedBuf, UdpRecvMultiBackend},
    platform::receive_pump::{
        ReceiveCompletion, ReceiveCompletionDiagnostics, ReceiveCompletionResult, ReceiveDelivery,
        ReceivePumpError, ReceivePumpEvent, ReceiveRequest, ReplacementSubmit,
    },
    slot::{Generation, InFlightOrphaned, InFlightWaiting, SlotState, SlotStatus},
};
use veloq_pod::bytes_of;
use veloq_std::{
    format,
    mem::{take, zeroed},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    string::ToString,
};
use windows_sys::Win32::{
    Foundation::ERROR_OPERATION_ABORTED,
    Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT, WSA_OPERATION_ABORTED, WSAENOBUFS},
};

pub(crate) struct RioSocketActor {
    pub(crate) rq: RioRq,
}

impl RioSocketActor {
    pub(crate) fn new(rq: RioRq) -> Self {
        Self { rq }
    }
}

#[derive(Clone, Copy)]
struct RioResultData {
    request_context: u64,
    status: i32,
    bytes: u32,
}

/// Immutable observation of one real RIO completion.
///
/// The observation is deliberately kept separate from the logical user result.  A user cancel
/// may turn a successful RIO completion into a cancelled operation, but it must not erase the
/// status, byte count, request identity, or receive-slot identity needed for diagnostics.
#[derive(Clone, Copy)]
struct RioCompletionObservation {
    request_id: u64,
    request_context: u64,
    op_kind: RioOpKind,
    status: i32,
    bytes: u32,
    receive_slot_id: Option<u32>,
    receive_slot_generation: Option<u32>,
}

impl RioCompletionObservation {
    #[inline]
    fn new(init: &RioOpRequestInit, result: RioResultData) -> Self {
        Self {
            request_id: init.request_id,
            request_context: result.request_context,
            op_kind: init.op_kind,
            status: result.status,
            bytes: result.bytes,
            receive_slot_id: init.receive_slot_id,
            receive_slot_generation: init.receive_slot_generation,
        }
    }

    #[inline]
    fn attach_to_report(
        self,
        report: Report<IocpError>,
        user_cancelled: bool,
    ) -> Report<IocpError> {
        report
            .with_ctx("rio_user_cancelled", user_cancelled)
            .with_ctx("rio_completion_status", self.status)
            .with_ctx("rio_completion_bytes", self.bytes)
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx("rio_request_context", self.request_context)
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("receive_slot_id", self.receive_slot_id.unwrap_or(u32::MAX))
            .with_ctx(
                "receive_slot_generation",
                self.receive_slot_generation.unwrap_or(u32::MAX),
            )
    }
}

impl RioResultData {
    #[inline]
    fn from_result(res: &RIORESULT) -> Self {
        Self {
            request_context: res.RequestContext,
            status: res.Status,
            bytes: res.BytesTransferred,
        }
    }

    #[inline]
    fn raw_res(self) -> i32 {
        if self.status == 0 {
            self.bytes.min(i32::MAX as u32) as i32
        } else if self.status > 0 {
            -self.status
        } else {
            self.status
        }
    }
}

struct RioIngress {
    init: RioOpRequestInit,
    result: RioResultData,
}

#[derive(Default)]
struct RioBackendEffect {
    release: Option<RioReleaseEffect>,
}

#[derive(Clone, Copy)]
struct RioReleaseEffect {
    addr: Option<RioAddrReservation>,
    buffer_lease: Option<RioBufferLeaseToken>,
    socket_inflight: SocketInflightIdentity,
    op_kind: RioOpKind,
}

impl RioBackendEffect {
    #[inline]
    fn from_init(init: &RioOpRequestInit) -> Self {
        Self {
            release: Some(RioReleaseEffect {
                addr: init.addr,
                buffer_lease: init.buffer_lease,
                socket_inflight: init.socket_inflight.identity(),
                op_kind: init.op_kind,
            }),
        }
    }
}

struct RioCompletionHooks<'a> {
    state: &'a mut RioState,
    registrar: &'a dyn BufferRegistrar,
    ext: &'a Extensions,
    completed_count: usize,
}

impl<'a> RioCompletionHooks<'a> {
    fn new(
        state: &'a mut RioState,
        registrar: &'a dyn BufferRegistrar,
        ext: &'a Extensions,
    ) -> Self {
        Self {
            state,
            registrar,
            ext,
            completed_count: 0,
        }
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for RioCompletionHooks<'_> {
    type BackendIngress = RioIngress;
    type BackendEffect = RioBackendEffect;

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        CompletionSettlement::Ignore {
            effect: RioBackendEffect::default(),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let CompletionSource::Backend(ingress) = source else {
            let source_name = match source {
                CompletionSource::Kernel => "Kernel",
                CompletionSource::User => "User",
                CompletionSource::Synthetic(_) => "Synthetic",
                CompletionSource::Backend(_) => "Backend",
            };
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    IocpError::InvalidState
                        .to_report()
                        .push_ctx("scope", "rio.runtime.control_flow.complete_waiting")
                        .with_ctx("token_index", event.token().index())
                        .with_ctx("token_generation", event.token().generation())
                        .with_ctx(
                            "slot_status",
                            format!("{:?}", SlotStatus::of(SlotState::InFlightWaiting)),
                        )
                        .with_ctx("completion_source", source_name)
                        .attach_note(
                            "Backend invariant broken: RIO complete_waiting received non-Backend source",
                        ),
                    CompletionCleanupGuard::default(),
                    RioBackendEffect::default(),
                ),
            };
        };
        complete_rio_waiting_slot(self, slot, ingress)
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let CompletionSource::Backend(ingress) = source else {
            return CompletionSettlement::Ignore {
                effect: RioBackendEffect::default(),
            };
        };
        complete_rio_orphaned_slot(self, slot, ingress)
    }

    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let effect = match source {
            CompletionSource::Backend(ingress) => RioBackendEffect::from_init(&ingress.init),
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Synthetic(_) => {
                RioBackendEffect::default()
            }
        };
        CompletionSettlement::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            cleanup: CompletionCleanupGuard::default(),
            effect,
        }
    }

    fn complete_backend_ingress(
        &mut self,
        ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<IocpSlotSpec, Self::BackendEffect> {
        CompletionBackendIngressAction::RouteUser(UserCompletionEvent::from_parts(
            COMP_BACKEND_RIO,
            ingress.init.token,
            ingress.result.raw_res(),
            0,
        ))
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        self.release_backend_effect(effect)
    }
}

impl RioCompletionHooks<'_> {
    fn release_backend_effect(&mut self, effect: RioBackendEffect) -> IocpResult<()> {
        let Some(release) = effect.release else {
            return Ok(());
        };
        let env = self
            .state
            .kernel
            .env(self.registrar, self.state.registration_mode)
            .ok_or_else(|| {
                IocpError::Unsupported
                    .to_report()
                    .attach_note("RIO completion release requires an active dispatch table")
            })?;
        self.state
            .registry
            .free_addr_reservation(release.addr)
            .trans()?;
        self.state
            .registry
            .release_buffer_lease(release.buffer_lease, env)
            .trans()?;
        self.state
            .release_request_inflight_identity(release.op_kind, release.socket_inflight)
            .trans()?;
        if self.state.rio_outstanding_count > 0 {
            self.state.rio_outstanding_count -= 1;
        }
        self.completed_count += 1;
        Ok(())
    }
}

fn complete_rio_waiting_slot(
    hooks: &mut RioCompletionHooks<'_>,
    mut slot: Slot<'_, InFlightWaiting>,
    ingress: &RioIngress,
) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
    let init = &ingress.init;
    let result = ingress.result;
    let token = init.token;
    let (user_data, generation) = token.parts();
    let effect = RioBackendEffect::from_init(init);
    let is_recv_provided = slot
        .with_access_mut(|access| access.operation().get_ref().kind() == OpKind::RecvProvided)
        .unwrap_or(false);
    if slot.platform().generation != generation {
        let report = IocpError::Internal
            .to_report()
            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .with_ctx("platform_generation", slot.platform().generation)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .attach_note("RIO slot platform generation mismatch");
        return complete_rio_failure_slot(slot, report, effect);
    }

    let cancelled = slot.platform().rio_user_cancel_requested;
    let observation = RioCompletionObservation::new(init, result);
    let mut completion = if cancelled {
        Err(completion_error_report(
            init,
            observation,
            true,
            "RIO operation was cancelled before kernel completion",
        ))
    } else if result.status == 0 {
        Ok(result.bytes as usize)
    } else {
        Err(completion_error_report(
            init,
            observation,
            false,
            "rio completion returned os error",
        ))
    };

    if matches!(
        init.op_kind,
        RioOpKind::TcpRecvMulti | RioOpKind::UdpRecvMulti
    ) {
        return complete_rio_multishot_slot(
            hooks, slot, ingress, effect, result, completion, cancelled,
        );
    }

    let _ = slot.with_access_mut(|access| {
        let iocp_op = access.operation_mut().get_mut();
        if iocp_op.header.in_flight {
            iocp_op.header.in_flight = false;
        }
        if !cancelled && let Ok(bytes) = completion.as_ref().copied() {
            completion = iocp_op
                .on_complete(bytes, hooks.ext)
                .with_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                .attach_note("rio op completion hook failed");
        }
    });

    let res_code = rio_result_to_event_res(&completion);
    let mut guard = slot.complete();
    let cleanup = guard
        .with_access_mut(|access| {
            PlatformOp::completion_cleanup(access.operation_mut(), &completion)
        })
        .unwrap_or_default();
    let record_result = if is_recv_provided {
        Some(
            guard
                .with_access_mut(|access| {
                    access
                        .operation_mut()
                        .get_mut()
                        .take_recv_provided_record(completion.is_ok())
                })
                .map_err(|_| {
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("rio_request_id", init.request_id)
                        .attach_note("failed to access RecvProvided completion record")
                })
                .and_then(|result| result),
        )
    } else {
        None
    };
    let _ = guard.take_op();
    let (payload, detail) = guard.take_completion_data();
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_RIO, token, res_code, 0);
    if let Some(record_result) = record_result {
        let record_payload = match record_result {
            Ok(record_payload) => record_payload,
            Err(error) => {
                drop(payload);
                drop(detail);
                return CompletionSettlement::TerminalFailure {
                    failure: CompletionFailure::terminal(error, cleanup, effect),
                };
            }
        };
        drop(payload);
        return CompletionSettlement::User {
            event,
            payload: record_payload,
            detail: detail.or(Some(completion)),
            cleanup,
            continuation: CompletionContinuation::Final,
            effect,
        };
    }
    if let Some(payload) = payload {
        CompletionSettlement::User {
            event,
            payload,
            detail: detail.or(Some(completion)),
            cleanup,
            // RIO 也没有 multishot：一次请求恰好对应一条完成。
            continuation: CompletionContinuation::Final,
            effect,
        }
    } else {
        drop(detail);
        CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(
                IocpError::InvalidState
                    .to_report()
                    .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                    .with_ctx("token_index", token.index())
                    .with_ctx("token_generation", token.generation())
                    .with_ctx("rio_op_kind", init.op_kind.as_str())
                    .with_ctx("rio_request_id", init.request_id)
                    .attach_note(
                        "Backend invariant broken: RIO slot completion payload is missing",
                    ),
                cleanup,
                effect,
            ),
        }
    }
}

fn completion_error_report(
    init: &RioOpRequestInit,
    observation: RioCompletionObservation,
    user_cancelled: bool,
    note: &'static str,
) -> Report<IocpError> {
    let error_code = if user_cancelled {
        ERROR_OPERATION_ABORTED as i32
    } else {
        observation.status
    };
    observation
        .attach_to_report(
            IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                .with_ctx(
                    "socket_raw",
                    init.socket_inflight.socket_key().as_handle() as usize,
                )
                .with_ctx("addr_generation", init.addr_generation.unwrap_or_default())
                .with_ctx("rq_raw", init.diagnostics.rq_raw)
                .with_ctx("data_buffer_id", init.diagnostics.data_buffer_id)
                .with_ctx("data_buffer_offset", init.diagnostics.data_buffer_offset)
                .with_ctx("data_buffer_length", init.diagnostics.data_buffer_length)
                .with_ctx("addr_slot", init.addr_slot.unwrap_or(usize::MAX))
                .set_error_code(error_code),
            user_cancelled,
        )
        .attach_note(note)
}

fn receive_completion_result(
    result: RioResultData,
    cancel_requested: bool,
) -> ReceiveCompletionResult {
    if result.status == 0 {
        return if cancel_requested {
            Err(ReceivePumpError::ReceiveCancelled)
        } else {
            Ok(result.bytes as usize)
        };
    }

    match result.status {
        WSA_OPERATION_ABORTED => Err(ReceivePumpError::ReceiveCancelled),
        WSAENOBUFS => Err(ReceivePumpError::ReceiveBufferExhausted),
        code => Err(ReceivePumpError::ReceiveOsError { code }),
    }
}

fn receive_remote_addr(
    hooks: &RioCompletionHooks<'_>,
    init: &RioOpRequestInit,
) -> IocpResult<SocketAddr> {
    let Some(addr) = init.addr else {
        return IocpError::InvalidState
            .attach_note("RIO recv_multi completion is missing its address reservation");
    };
    let mut storage = crate::net::addr::SockAddrStorage::default();
    hooks
        .state
        .registry
        .copy_addr_slot_to(addr, &mut storage)
        .trans()?;
    crate::net::addr::to_socket_addr(bytes_of(&storage))
}

fn complete_rio_tcp_multishot_slot(
    hooks: &mut RioCompletionHooks<'_>,
    mut slot: Slot<'_, InFlightWaiting>,
    ingress: &RioIngress,
    effect: RioBackendEffect,
    result: RioResultData,
    cancelled: bool,
) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
    let init = &ingress.init;
    let socket_key = init.socket_inflight.socket_key();
    let request = match (init.receive_slot_id, init.receive_slot_generation) {
        (Some(slot_id), Some(slot_generation)) => ReceiveRequest {
            request_id: init.request_id,
            slot_id,
            slot_generation,
        },
        _ => {
            return complete_rio_failure_slot(
                slot,
                IocpError::InvalidState
                    .to_report()
                    .with_ctx("rio_request_id", init.request_id)
                    .attach_note("RIO recv_multi completion lacks slot identity"),
                effect,
            );
        }
    };
    let receive_result = receive_completion_result(result, cancelled);
    let observation = RioCompletionObservation::new(init, result);
    let (pump_event, pump_diagnostics) = match slot.with_access_mut(|access| {
        let (operation, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO TCP recv_multi payload is missing"))?;
        let IocpUserPayload::RecvMulti(_) = &mut *payload else {
            return Err(access_error_report(
                "RIO TCP recv_multi payload has the wrong kind",
            ));
        };
        let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
            return Err(access_error_report(
                "RIO TCP recv_multi payload has no receive pump",
            ));
        };
        pump.record_completion_observation(observation.status, observation.bytes, cancelled);
        let event = pump
            .complete(ReceiveCompletion {
                request,
                result: receive_result,
                remote_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            })
            .map_err(|error| {
                receive_pump_report(init, error, Some(observation), cancelled, None)
            })?;
        Ok((event, pump.completion_diagnostics()))
    }) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return complete_rio_failure_slot(slot, error, effect),
        Err(_) => {
            return complete_rio_failure_slot(
                slot,
                access_error_report("RIO TCP recv_multi slot access failed"),
                effect,
            );
        }
    };

    if let Err(error) = hooks.release_backend_effect(effect) {
        return complete_rio_failure_slot(slot, error, RioBackendEffect::default());
    }

    match pump_event {
        ReceivePumpEvent::Drain => CompletionSettlement::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: RioBackendEffect::default(),
        },
        ReceivePumpEvent::Ignored => CompletionSettlement::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: RioBackendEffect::default(),
        },
        ReceivePumpEvent::Final { error } => {
            hooks.state.finish_receive_pump(socket_key, init.token);
            complete_rio_failure_slot(
                slot,
                receive_pump_report(
                    init,
                    error,
                    Some(observation),
                    cancelled,
                    Some(pump_diagnostics),
                ),
                RioBackendEffect::default(),
            )
        }
        ReceivePumpEvent::Pending { .. } => {
            let delivery = match slot.with_access_mut(|access| {
                let (operation, payload) = access.operation_and_payload_mut().map_err(|_| {
                    access_error_report("RIO TCP recv_multi delivery payload is missing")
                })?;
                let IocpUserPayload::RecvMulti(_) = &mut *payload else {
                    return Err(access_error_report(
                        "RIO TCP recv_multi payload changed during delivery",
                    ));
                };
                let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                    return Err(access_error_report(
                        "RIO TCP recv_multi delivery has no receive pump",
                    ));
                };
                Ok(pump.next_delivery())
            }) {
                Ok(Ok(Some(delivery))) => delivery,
                Ok(Ok(None)) => {
                    return CompletionSettlement::Cleanup {
                        cleanup: CompletionCleanupGuard::default(),
                        continuation: CompletionContinuation::More,
                        effect: RioBackendEffect::default(),
                    };
                }
                Ok(Err(error)) => {
                    return complete_rio_failure_slot(slot, error, RioBackendEffect::default());
                }
                Err(_) => {
                    return complete_rio_failure_slot(
                        slot,
                        access_error_report("RIO TCP recv_multi delivery access failed"),
                        RioBackendEffect::default(),
                    );
                }
            };
            let output = match FixedBuf::alloc_heap(
                NonZeroUsize::new(init.diagnostics.data_buffer_length.max(1) as usize)
                    .expect("RIO receive capacity is non-zero"),
                0,
            ) {
                Ok(output) => output,
                Err(_) => {
                    let _ = abort_tcp_receive_delivery(
                        &mut slot,
                        delivery,
                        ReceivePumpError::ReceiveBufferExhausted,
                    );
                    return CompletionSettlement::Cleanup {
                        cleanup: CompletionCleanupGuard::default(),
                        continuation: CompletionContinuation::More,
                        effect: RioBackendEffect::default(),
                    };
                }
            };
            match prepare_tcp_receive_replacement(hooks, &mut slot, init, delivery, output) {
                Ok(replacement) => replacement,
                Err(error) => complete_rio_failure_slot(slot, error, RioBackendEffect::default()),
            }
        }
        ReceivePumpEvent::Held { .. } | ReceivePumpEvent::Retained { .. } => {
            CompletionSettlement::Retained {
                cleanup: CompletionCleanupGuard::default(),
                effect: RioBackendEffect::default(),
            }
        }
    }
}

fn complete_rio_multishot_slot(
    hooks: &mut RioCompletionHooks<'_>,
    mut slot: Slot<'_, InFlightWaiting>,
    ingress: &RioIngress,
    effect: RioBackendEffect,
    result: RioResultData,
    completion: IocpResult<usize>,
    cancelled: bool,
) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
    if ingress.init.op_kind == RioOpKind::TcpRecvMulti {
        return complete_rio_tcp_multishot_slot(hooks, slot, ingress, effect, result, cancelled);
    }
    let init = &ingress.init;
    let socket_key = init.socket_inflight.socket_key();
    let request = match (init.receive_slot_id, init.receive_slot_generation) {
        (Some(slot_id), Some(slot_generation)) => ReceiveRequest {
            request_id: init.request_id,
            slot_id,
            slot_generation,
        },
        _ => {
            return complete_rio_failure_slot(
                slot,
                IocpError::InvalidState
                    .to_report()
                    .with_ctx("rio_request_id", init.request_id)
                    .attach_note("RIO recv_multi completion lacks slot identity"),
                effect,
            );
        }
    };
    let remote_addr = if completion.is_ok() && !cancelled {
        match receive_remote_addr(hooks, init) {
            Ok(addr) => addr,
            Err(error) => {
                return complete_rio_failure_slot(slot, error, effect);
            }
        }
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    };
    let receive_result = receive_completion_result(result, cancelled);
    let observation = RioCompletionObservation::new(init, result);
    let (pump_event, pump_diagnostics) = match slot.with_access_mut(|access| {
        let (_, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO recv_multi slot payload is missing"))?;
        let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
            return Err(access_error_report(
                "RIO recv_multi slot payload has the wrong kind",
            ));
        };
        user.receive_pump_mut().record_completion_observation(
            observation.status,
            observation.bytes,
            cancelled,
        );
        let event = user
            .receive_pump_mut()
            .complete(ReceiveCompletion {
                request,
                result: receive_result,
                remote_addr,
            })
            .map_err(|error| {
                receive_pump_report(init, error, Some(observation), cancelled, None)
            })?;
        Ok((event, user.receive_pump().completion_diagnostics()))
    }) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            return complete_rio_failure_slot(slot, error, effect);
        }
        Err(_) => {
            return complete_rio_failure_slot(
                slot,
                access_error_report("RIO recv_multi slot access failed"),
                effect,
            );
        }
    };

    if let Err(error) = hooks.release_backend_effect(effect) {
        return complete_rio_failure_slot(slot, error, RioBackendEffect::default());
    }

    match pump_event {
        ReceivePumpEvent::Drain => CompletionSettlement::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: RioBackendEffect::default(),
        },
        ReceivePumpEvent::Ignored => CompletionSettlement::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::More,
            effect: RioBackendEffect::default(),
        },
        ReceivePumpEvent::Final { error } => {
            hooks.state.finish_receive_pump(socket_key, init.token);
            complete_rio_failure_slot(
                slot,
                receive_pump_report(
                    init,
                    error,
                    Some(observation),
                    cancelled,
                    Some(pump_diagnostics),
                ),
                RioBackendEffect::default(),
            )
        }
        ReceivePumpEvent::Pending { .. } => {
            let delivery = match slot.with_access_mut(|access| {
                let (_, payload) = access.operation_and_payload_mut().map_err(|_| {
                    access_error_report("RIO recv_multi delivery payload is missing")
                })?;
                let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                    return Err(access_error_report(
                        "RIO recv_multi payload changed during delivery",
                    ));
                };
                Ok(user.receive_pump_mut().next_delivery())
            }) {
                Ok(Ok(Some(delivery))) => delivery,
                Ok(Ok(None)) => {
                    return CompletionSettlement::Cleanup {
                        cleanup: CompletionCleanupGuard::default(),
                        continuation: CompletionContinuation::More,
                        effect: RioBackendEffect::default(),
                    };
                }
                Ok(Err(error)) => {
                    return complete_rio_failure_slot(slot, error, RioBackendEffect::default());
                }
                Err(_) => {
                    return complete_rio_failure_slot(
                        slot,
                        access_error_report("RIO recv_multi delivery access failed"),
                        RioBackendEffect::default(),
                    );
                }
            };
            let output = match FixedBuf::alloc_heap(
                NonZeroUsize::new(init.diagnostics.data_buffer_length.max(1) as usize)
                    .expect("RIO datagram capacity is non-zero"),
                0,
            ) {
                Ok(output) => output,
                Err(_) => {
                    let _ = abort_receive_delivery(
                        &mut slot,
                        delivery,
                        ReceivePumpError::ReceiveBufferExhausted,
                    );
                    return CompletionSettlement::Cleanup {
                        cleanup: CompletionCleanupGuard::default(),
                        continuation: CompletionContinuation::More,
                        effect: RioBackendEffect::default(),
                    };
                }
            };
            match prepare_receive_replacement(hooks, &mut slot, init, delivery, output) {
                Ok(replacement) => replacement,
                Err(error) => complete_rio_failure_slot(slot, error, RioBackendEffect::default()),
            }
        }
        ReceivePumpEvent::Held { .. } | ReceivePumpEvent::Retained { .. } => {
            CompletionSettlement::Retained {
                cleanup: CompletionCleanupGuard::default(),
                effect: RioBackendEffect::default(),
            }
        }
    }
}

fn access_error_report(note: &'static str) -> Report<IocpError> {
    IocpError::InvalidState
        .to_report()
        .push_ctx("scope", "rio.runtime.control_flow.multishot")
        .attach_note(note)
}

fn receive_pump_report(
    init: &RioOpRequestInit,
    error: ReceivePumpError,
    observation: Option<RioCompletionObservation>,
    user_cancelled: bool,
    pump_diagnostics: Option<ReceiveCompletionDiagnostics>,
) -> Report<IocpError> {
    let mut report = IocpError::CompletionWait
        .to_report()
        .with_ctx("rio_request_id", init.request_id)
        .with_ctx("receive_slot_id", init.receive_slot_id.unwrap_or(u32::MAX))
        .with_ctx(
            "receive_slot_generation",
            init.receive_slot_generation.unwrap_or(u32::MAX),
        )
        .with_ctx("receive_pump_error", error.to_string())
        .attach_note("RIO receive pump state transition failed");
    if let Some(observation) = observation {
        report = observation.attach_to_report(report, user_cancelled);
    }
    if let Some(diagnostics) = pump_diagnostics {
        report = report
            .with_ctx("rio_pump_completion_count", diagnostics.completion_count)
            .with_ctx(
                "rio_pump_cancelled_completion_count",
                diagnostics.cancelled_completion_count,
            )
            .with_ctx("rio_pump_total_bytes", diagnostics.total_bytes)
            .with_ctx("rio_pump_error_count", diagnostics.error_count)
            .with_ctx(
                "rio_pump_first_error_status",
                diagnostics.first_error_status.unwrap_or_default(),
            )
            .with_ctx(
                "rio_pump_last_status",
                diagnostics.last_status.unwrap_or_default(),
            );
    }
    let error_code = match error {
        ReceivePumpError::ReceiveOsError { code } => Some(code),
        ReceivePumpError::ReceiveBufferExhausted => Some(WSAENOBUFS),
        ReceivePumpError::ReceiveCancelled => Some(WSA_OPERATION_ABORTED),
        _ => None,
    };
    if let Some(code) = error_code {
        report = report.set_error_code(code);
    }
    report
}

fn abort_receive_delivery(
    slot: &mut Slot<'_, InFlightWaiting>,
    delivery: ReceiveDelivery,
    error: ReceivePumpError,
) -> IocpResult<()> {
    slot.with_access_mut(|access| {
        let (_, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO receive delivery payload is missing"))?;
        let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
            return Err(access_error_report(
                "RIO receive delivery payload has the wrong kind",
            ));
        };
        user.receive_pump_mut()
            .abort_delivery(delivery, error)
            .map_err(|error| {
                IocpError::CompletionWait
                    .to_report()
                    .with_ctx("receive_pump_error", error.to_string())
            })
    })
    .map_err(|_| access_error_report("RIO receive delivery access failed"))?
}

fn abort_tcp_receive_delivery(
    slot: &mut Slot<'_, InFlightWaiting>,
    delivery: ReceiveDelivery,
    error: ReceivePumpError,
) -> IocpResult<()> {
    slot.with_access_mut(|access| {
        let (operation, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO TCP receive delivery payload is missing"))?;
        let IocpUserPayload::RecvMulti(_) = &mut *payload else {
            return Err(access_error_report(
                "RIO TCP receive delivery payload has the wrong kind",
            ));
        };
        let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
            return Err(access_error_report(
                "RIO TCP receive delivery has no receive pump",
            ));
        };
        pump.abort_delivery(delivery, error).map_err(|error| {
            IocpError::CompletionWait
                .to_report()
                .with_ctx("receive_pump_error", error.to_string())
        })
    })
    .map_err(|_| access_error_report("RIO TCP receive delivery access failed"))?
}

fn prepare_tcp_receive_replacement(
    hooks: &mut RioCompletionHooks<'_>,
    slot: &mut Slot<'_, InFlightWaiting>,
    init: &RioOpRequestInit,
    delivery: ReceiveDelivery,
    mut output: FixedBuf,
) -> Result<CompletionSettlement<IocpSlotSpec, RioBackendEffect>, Report<IocpError>> {
    let request = delivery.request();
    let replacement = match slot.with_access_mut(|access| {
        let (operation, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO TCP receive replacement payload is missing"))?;
        let IocpUserPayload::RecvMulti(user) = &mut *payload else {
            return Err(access_error_report(
                "RIO TCP receive replacement payload has the wrong kind",
            ));
        };
        let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
            return Err(access_error_report(
                "RIO TCP receive replacement has no receive pump",
            ));
        };
        let source = pump
            .slot(request.slot_id)
            .and_then(|slot| slot.completed_data())
            .ok_or_else(|| access_error_report("RIO TCP receive slot has no completed data"))?;
        if source.len() < delivery.len() {
            return Err(access_error_report(
                "RIO TCP receive completion exceeds written data",
            ));
        }
        output.spare_capacity_mut()[..delivery.len()].copy_from_slice(&source[..delivery.len()]);
        if !delivery.replacement_required() {
            return Ok(ReplacementSubmit::NotSubmitted);
        }
        let reservation = pump
            .reserve_replacement(&delivery)
            .map_err(|error| receive_pump_report(init, error, None, false, None))?;
        let raw = RawHandle::new(init.socket_inflight.socket_key());
        let submitted = hooks.state.try_submit_recv_multi_replacement(
            RioTarget {
                fd: user.fd,
                handle: raw.borrow(),
                token: init.token,
                buf_offset: 0,
                operation: "recv_multi_replacement",
            },
            RioOpKind::TcpRecvMulti,
            pump,
            reservation,
            hooks.registrar,
        );
        Ok(match submitted {
            Ok(submitted) => ReplacementSubmit::Submitted {
                request_id: submitted.request_id,
                reservation,
            },
            Err(_) => ReplacementSubmit::Failed { reservation },
        })
    }) {
        Ok(Ok(replacement)) => replacement,
        Ok(Err(error)) => return Err(error.trans()),
        Err(_) => {
            return Err(access_error_report(
                "RIO TCP receive replacement access failed",
            ));
        }
    };
    let record = match slot.with_access_mut(|access| {
        let (operation, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO TCP receive delivery payload disappeared"))?;
        let IocpUserPayload::RecvMulti(_) = &mut *payload else {
            return Err(access_error_report(
                "RIO TCP receive delivery payload changed kind",
            ));
        };
        let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
            return Err(access_error_report(
                "RIO TCP receive delivery has no receive pump",
            ));
        };
        let record = pump
            .finish_delivery(delivery, output, replacement)
            .map_err(|error| {
                IocpError::CompletionWait
                    .to_report()
                    .with_ctx("receive_pump_error", error.to_string())
            })?;
        Ok(record)
    }) {
        Ok(Ok(record)) => record,
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(access_error_report(
                "RIO TCP receive delivery access failed",
            ));
        }
    };
    if record.continuation.is_final() {
        hooks
            .state
            .finish_receive_pump(init.socket_inflight.socket_key(), init.token);
    }
    let packet_len = record.packet.buf.len();
    let buffer = record
        .packet
        .buf
        .into_fixed_buf()
        .ok_or_else(|| access_error_report("RIO TCP receive record has no output buffer"))?;
    let continuation = record.continuation;
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_RIO, init.token, packet_len as i32, 0);
    let payload = IocpUserPayload::ProvidedBuf(ProvidedBuf { buf: Some(buffer) });
    let detail = Some(Ok(packet_len));
    Ok(CompletionSettlement::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        continuation,
        effect: RioBackendEffect::default(),
    })
}

fn prepare_receive_replacement(
    hooks: &mut RioCompletionHooks<'_>,
    slot: &mut Slot<'_, InFlightWaiting>,
    init: &RioOpRequestInit,
    delivery: ReceiveDelivery,
    mut output: FixedBuf,
) -> Result<CompletionSettlement<IocpSlotSpec, RioBackendEffect>, Report<IocpError>> {
    let request = delivery.request();
    let replacement = match slot.with_access_mut(|access| {
        let (_, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO receive replacement payload is missing"))?;
        let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
            return Err(access_error_report(
                "RIO receive replacement payload has the wrong kind",
            ));
        };
        let source = user
            .receive_pump()
            .slot(request.slot_id)
            .and_then(|slot| slot.completed_data())
            .ok_or_else(|| access_error_report("RIO receive slot has no completed data"))?;
        if source.len() < delivery.len() {
            return Err(access_error_report(
                "RIO receive completion exceeds written data",
            ));
        }
        output.spare_capacity_mut()[..delivery.len()].copy_from_slice(&source[..delivery.len()]);
        if !delivery.replacement_required() {
            return Ok(ReplacementSubmit::NotSubmitted);
        }
        let reservation = user
            .receive_pump_mut()
            .reserve_replacement(&delivery)
            .map_err(|error| receive_pump_report(init, error, None, false, None))?;
        let raw = RawHandle::new(init.socket_inflight.socket_key());
        let submitted = hooks.state.try_submit_recv_multi_replacement(
            RioTarget {
                fd: user.fd,
                handle: raw.borrow(),
                token: init.token,
                buf_offset: 0,
                operation: "udp_recv_multi_replacement",
            },
            RioOpKind::UdpRecvMulti,
            user.receive_pump_mut(),
            reservation,
            hooks.registrar,
        );
        Ok(match submitted {
            Ok(submitted) => ReplacementSubmit::Submitted {
                request_id: submitted.request_id,
                reservation,
            },
            Err(_) => ReplacementSubmit::Failed { reservation },
        })
    }) {
        Ok(Ok(replacement)) => replacement,
        Ok(Err(error)) => return Err(error.trans()),
        Err(_) => return Err(access_error_report("RIO receive replacement access failed")),
    };
    let record = match slot.with_access_mut(|access| {
        let (_, payload) = access
            .operation_and_payload_mut()
            .map_err(|_| access_error_report("RIO receive delivery payload disappeared"))?;
        let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
            return Err(access_error_report(
                "RIO receive delivery payload changed kind",
            ));
        };
        let record = user
            .receive_pump_mut()
            .finish_delivery(delivery, output, replacement)
            .map_err(|error| {
                IocpError::CompletionWait
                    .to_report()
                    .with_ctx("receive_pump_error", error.to_string())
            })?;
        Ok(record)
    }) {
        Ok(Ok(record)) => record,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Err(access_error_report("RIO receive delivery access failed")),
    };
    if record.continuation.is_final() {
        hooks
            .state
            .finish_receive_pump(init.socket_inflight.socket_key(), init.token);
    }
    let packet_len = record.packet.buf.len();
    let continuation = record.continuation;
    let packet = record.packet;
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_RIO, init.token, packet_len as i32, 0);
    let payload = IocpUserPayload::UdpRecvPacket(packet);
    let detail = Some(Ok(packet_len));
    Ok(CompletionSettlement::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        continuation,
        effect: RioBackendEffect::default(),
    })
}

fn complete_rio_failure_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    error: Report<IocpError>,
    effect: RioBackendEffect,
) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
    let completion_result: IocpResult<usize> = Err(error);
    let cleanup = slot
        .with_access_mut(|access| {
            PlatformOp::completion_cleanup(access.operation_mut(), &completion_result)
        })
        .unwrap_or_default();
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (_, detail) = completed.take_completion_data();
    drop(detail);
    let error = match completion_result {
        Err(error) => error,
        Ok(_) => unreachable!("failure result must contain the original completion error"),
    };
    CompletionSettlement::TerminalFailure {
        failure: CompletionFailure::terminal(error, cleanup, effect),
    }
}

fn complete_rio_orphaned_slot(
    hooks: &mut RioCompletionHooks<'_>,
    mut slot: Slot<'_, InFlightOrphaned>,
    ingress: &RioIngress,
) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
    let init = &ingress.init;
    let result = ingress.result;
    let generation = init.token.generation();
    let socket_key = init.socket_inflight.socket_key();
    let effect = RioBackendEffect::from_init(init);
    let generation_mismatch = slot.platform_mut().generation != generation;

    if matches!(
        init.op_kind,
        RioOpKind::TcpRecvMulti | RioOpKind::UdpRecvMulti
    ) {
        let last_request = hooks
            .state
            .socket_runtime
            .get(&socket_key)
            .is_some_and(|state| {
                state.receive_pump_token == Some(init.token) && state.receive_pump_inflight <= 1
            });
        if !last_request {
            return CompletionSettlement::Cleanup {
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::More,
                effect,
            };
        }
        hooks.state.finish_receive_pump(socket_key, init.token);
    }

    let mut guard = slot.complete();
    let orphan_result = if generation_mismatch {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .attach_note("orphaned RIO completion had platform generation mismatch")
    } else if result.status == 0 {
        Ok(result.bytes as usize)
    } else {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .set_error_code(result.status)
            .attach_note("orphaned RIO completion returned os error")
    };
    let cleanup = guard
        .with_access_mut(|access| {
            PlatformOp::orphan_cleanup(access.operation_mut(), &orphan_result)
        })
        .unwrap_or_default();
    let _ = guard.take_op();
    let _ = guard.take_completion_data();
    let _ = take(guard.platform_mut());
    CompletionSettlement::Cleanup {
        cleanup,
        continuation: CompletionContinuation::Final,
        effect,
    }
}

pub(crate) const RIO_ANOMALY_MALFORMED: u16 = 1;
pub(crate) const RIO_ANOMALY_MISSING: u16 = 2;
pub(crate) const RIO_ANOMALY_STALE: u16 = 3;

pub(crate) fn rio_malformed_context_kind(raw_context: u64) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific(RIO_ANOMALY_MALFORMED, COMP_BACKEND_RIO, raw_context)
}

pub(crate) fn rio_missing_context_kind(
    raw_context: u64,
    index: usize,
    generation: Generation,
) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific_missing(
        RIO_ANOMALY_MISSING,
        COMP_BACKEND_RIO,
        raw_context,
        index,
        generation,
    )
}

pub(crate) fn rio_stale_context_kind(
    raw_context: u64,
    index: usize,
    expected_generation: Generation,
    actual_generation: Generation,
) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific_stale(
        RIO_ANOMALY_STALE,
        COMP_BACKEND_RIO,
        raw_context,
        index,
        expected_generation,
        actual_generation,
    )
}

fn rio_result_attach(res: RioResultData) -> AnomalyAttach {
    AnomalyAttach::from_raw_completion(RawCompletion::new(
        COMP_BACKEND_RIO,
        RIO_EVENT_TOKEN,
        res.raw_res(),
        0,
    ))
}

impl RioState {
    pub(crate) fn ensure_actor(
        &mut self,
        target: (IoFd, BorrowedRawHandle<'_>),
        env: RioEnv<'_>,
    ) -> RioResult<&mut RioSocketActor> {
        let (fd, handle) = target;
        let socket_key = handle.raw().actor_key();
        if self.submissions_closed {
            return RioError::InvalidInput
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("rio_outstanding_count", self.rio_outstanding_count)
                .attach_note("RIO runtime is shutting down; rejecting actor creation");
        }

        if let Some(key) = self.actor_by_handle.get(&socket_key).copied() {
            return self
                .actors
                .get_mut(key)
                .ok_or(RioError::Internal)
                .attach_note("failed to retrieve indexed actor");
        }

        let rq = self
            .registry
            .create_rq((handle, fd), env)
            .push_ctx("scope", "rio.runtime.control_flow.ensure_actor")
            .with_ctx("fd", fd.to_string())
            .with_ctx("handle_raw", handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", handle.raw().as_handle() as usize)
            .with_ctx("rq_depth", self.registry.rq_depth)
            .with_ctx("max_outstanding_recvs", self.registry.rq_depth)
            .with_ctx("max_outstanding_sends", self.registry.rq_depth)
            .with_ctx("max_receive_data_buffers", 1_u32)
            .with_ctx("max_send_data_buffers", 1_u32)
            .with_ctx("rio_outstanding_count", self.rio_outstanding_count)
            .with_ctx("actors_len", self.actors.len())
            .with_ctx(
                "actor_index_hit",
                self.actor_by_handle.contains_key(&socket_key),
            )
            .attach_note("RIOCreateRequestQueue failed")?;

        let is_first = self.actor_by_handle.is_empty();
        let actor = RioSocketActor::new(rq);
        let key = self.actors.insert(actor);
        self.actor_by_handle.insert(socket_key, key);
        self.socket_runtime.entry(socket_key).or_default();
        if is_first && !self.cq_armed {
            self.kernel.rearm_notify().trans()?;
            self.cq_armed = true;
        }
        self.actors
            .get_mut(key)
            .ok_or(RioError::Internal)
            .trans()
            .attach_note("failed to retrieve inserted actor")
    }

    pub(crate) fn shutdown_actor(&mut self, socket_key: SocketKey) {
        let Some(key) = self.actor_by_handle.remove(&socket_key) else {
            return;
        };
        let _ = self.actors.remove(key);
    }

    pub(crate) fn stop_accepting_new_submissions(&mut self) {
        self.submissions_closed = true;
        self.actor_by_handle.clear();
        for state in self.socket_runtime.values_mut() {
            state.lifecycle = SocketLifecycleState::Closing;
        }
    }

    pub(crate) fn forget_runtime_after_drain(&mut self) -> RioResult<()> {
        if !self.runtime_ready_for_forget() {
            return RioError::InvalidInput
                .with_ctx("rio_outstanding_count", self.rio_outstanding_count)
                .with_ctx("socket_inflight", self.socket_inflight_count())
                .attach_note("forgetting RIO runtime before all requests drained");
        }
        self.actors.clear();
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
        Ok(())
    }

    pub(crate) fn process_completions(
        &mut self,
        ops: &mut IocpOpRegistry,
        ext: &Extensions,
        registrar: &dyn BufferRegistrar,
        completion_table: &SharedCompletionTable<IocpSlotSpec>,
        diagnostics: &mut IocpDriverCompletionDiagnostics,
    ) -> IocpResult<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { zeroed() };
        if self.kernel.env(registrar, self.registration_mode).is_none() {
            return Ok(0);
        }
        let (completed_count, rio_outstanding_count, completions_found) = {
            let mut completions_found = false;
            let mut hooks = RioCompletionHooks::new(self, registrar, ext);

            loop {
                let count = hooks.state.kernel.dequeue(&mut results);
                if count == RIO_CORRUPT_CQ {
                    return RioError::Internal
                        .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)")
                        .trans();
                }
                if count == 0 {
                    break;
                }
                completions_found = true;

                for res in results.iter().take(count as usize) {
                    let result = RioResultData::from_result(res);
                    match hooks
                        .state
                        .registry
                        .decode_request_context_checked(result.request_context)
                    {
                        RioRequestContextDecode::Valid(kind) => {
                            let RioCompletionKind::Op {
                                init,
                                context: _completed_context,
                            } = *kind;
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Backend(RioIngress { init, result }),
                            )?;
                        }
                        RioRequestContextDecode::Malformed { raw } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: rio_malformed_context_kind(raw),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                        RioRequestContextDecode::Missing { id } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: rio_missing_context_kind(
                                        result.request_context,
                                        id.index(),
                                        id.generation(),
                                    ),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                        RioRequestContextDecode::Stale {
                            id,
                            actual_generation,
                        } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: rio_stale_context_kind(
                                        result.request_context,
                                        id.index(),
                                        id.generation(),
                                        actual_generation,
                                    ),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                    }
                }

                if count < MAX_RIO_RESULTS as u32 {
                    break;
                }
            }

            let completed_count = hooks.completed_count;
            let rio_outstanding_count = hooks.state.rio_outstanding_count;
            (completed_count, rio_outstanding_count, completions_found)
        };

        if completions_found {
            self.cq_armed = false;
        }

        if !self.actor_by_handle.is_empty() && !self.cq_armed {
            self.kernel.rearm_notify().trans()?;
            self.cq_armed = true;
        }

        if rio_outstanding_count == 0
            && let Some(env) = self.kernel.env(registrar, self.registration_mode)
        {
            self.registry.flush_deregs(env);
        }
        Ok(completed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(status: i32, bytes: u32) -> RioResultData {
        RioResultData {
            request_context: 0,
            status,
            bytes,
        }
    }

    #[test]
    fn receive_status_preserves_os_code_and_categories() {
        assert_eq!(receive_completion_result(result(0, 17), false), Ok(17));
        assert_eq!(
            receive_completion_result(result(0, 17), true),
            Err(ReceivePumpError::ReceiveCancelled)
        );
        assert_eq!(
            receive_completion_result(result(WSA_OPERATION_ABORTED, 0), false),
            Err(ReceivePumpError::ReceiveCancelled)
        );
        assert_eq!(
            receive_completion_result(result(WSAENOBUFS, 0), false),
            Err(ReceivePumpError::ReceiveBufferExhausted)
        );
        assert_eq!(
            receive_completion_result(result(10035, 0), true),
            Err(ReceivePumpError::ReceiveOsError { code: 10035 })
        );
    }
}
