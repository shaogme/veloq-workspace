use super::{
    model::{
        RioBackendEffect, RioCompletionHooks, RioCompletionObservation, RioIngress, RioResultData,
    },
    replacement::RioReceiveReplacement,
    single::RioSingleCompletion,
};
use crate::{
    error::{IocpError, IocpResult},
    net::addr::{SockAddrStorage, to_socket_addr},
    op::{IocpSlotSpec, IocpUserPayload, Slot},
    rio::core::{RioOpKind, RioOpRequestInit},
};
use diagweave::prelude::*;
use veloq_buf::FixedBuf;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, CompletionContinuation, CompletionSettlement},
    op::types::UdpRecvMultiBackend,
    platform::receive_pump::{
        ReceiveCompletion, ReceiveCompletionDiagnostics, ReceivePumpError, ReceivePumpEvent,
        ReceiveRequest,
    },
    slot::InFlightWaiting,
};
use veloq_pod::bytes_of;
use veloq_std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    string::ToString,
};
use windows_sys::Win32::Networking::WinSock::{WSA_OPERATION_ABORTED, WSAENOBUFS};

pub(super) struct RioMultiShotCompletion;

pub(super) struct MultiShotCompletionInput<'hook, 'state, 'slot, 'ingress> {
    pub(super) hooks: &'hook mut RioCompletionHooks<'state>,
    pub(super) slot: Slot<'slot, InFlightWaiting>,
    pub(super) ingress: &'ingress RioIngress,
    pub(super) effect: RioBackendEffect,
    pub(super) result: RioResultData,
    pub(super) completion: IocpResult<usize>,
    pub(super) cancelled: bool,
}

impl RioMultiShotCompletion {
    fn receive_remote_addr(
        hooks: &mut RioCompletionHooks<'_>,
        init: &RioOpRequestInit,
    ) -> IocpResult<SocketAddr> {
        let Some(addr) = init.addr else {
            return IocpError::InvalidState
                .attach_note("RIO recv_multi completion is missing its address reservation");
        };
        let mut storage = SockAddrStorage::default();
        hooks
            .core_mut()
            .registry_mut()
            .copy_addr_slot_to(addr, &mut storage)
            .trans()?;
        to_socket_addr(bytes_of(&storage))
    }

    pub(super) fn complete_tcp_slot(
        input: MultiShotCompletionInput<'_, '_, '_, '_>,
    ) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
        let MultiShotCompletionInput {
            hooks,
            mut slot,
            ingress,
            effect,
            result,
            cancelled,
            ..
        } = input;
        let init = &ingress.init;
        let socket_key = init.socket_inflight.socket_key();
        let request = match (init.receive_slot_id, init.receive_slot_generation) {
            (Some(slot_id), Some(slot_generation)) => ReceiveRequest {
                request_id: init.request_id,
                slot_id,
                slot_generation,
            },
            _ => {
                return RioSingleCompletion::complete_failure_slot(
                    slot,
                    IocpError::InvalidState
                        .to_report()
                        .with_ctx("rio_request_id", init.request_id)
                        .attach_note("RIO recv_multi completion lacks slot identity"),
                    effect,
                );
            }
        };
        let receive_result = RioSingleCompletion::receive_completion_result(result, cancelled);
        let observation = RioCompletionObservation::new(init, result);
        let (pump_event, pump_diagnostics) = match slot.with_access_mut(|access| {
            let (operation, payload) = access
                .operation_and_payload_mut()
                .map_err(|_| Self::access_error_report("RIO TCP recv_multi payload is missing"))?;
            let IocpUserPayload::RecvMulti(_) = &mut *payload else {
                return Err(Self::access_error_report(
                    "RIO TCP recv_multi payload has the wrong kind",
                ));
            };
            let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                return Err(Self::access_error_report(
                    "RIO TCP recv_multi payload has no receive pump",
                ));
            };
            pump.record_completion_observation(
                observation.status(),
                observation.bytes(),
                cancelled,
            );
            let event = pump
                .complete(ReceiveCompletion {
                    request,
                    result: receive_result,
                    remote_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                })
                .map_err(|error| {
                    Self::receive_pump_report(init, error, Some(observation), cancelled, None)
                })?;
            Ok((event, pump.completion_diagnostics()))
        }) {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                return RioSingleCompletion::complete_failure_slot(slot, error, effect);
            }
            Err(_) => {
                return RioSingleCompletion::complete_failure_slot(
                    slot,
                    Self::access_error_report("RIO TCP recv_multi slot access failed"),
                    effect,
                );
            }
        };

        if let Err(error) = hooks.release_backend_effect(effect) {
            return RioSingleCompletion::complete_failure_slot(
                slot,
                error,
                RioBackendEffect::default(),
            );
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
                hooks
                    .runtime_mut()
                    .finish_receive_pump(socket_key, init.token);
                RioSingleCompletion::complete_failure_slot(
                    slot,
                    Self::receive_pump_report(
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
                    let (operation, payload) =
                        access.operation_and_payload_mut().map_err(|_| {
                            Self::access_error_report(
                                "RIO TCP recv_multi delivery payload is missing",
                            )
                        })?;
                    let IocpUserPayload::RecvMulti(_) = &mut *payload else {
                        return Err(Self::access_error_report(
                            "RIO TCP recv_multi payload changed during delivery",
                        ));
                    };
                    let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                        return Err(Self::access_error_report(
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
                        return RioSingleCompletion::complete_failure_slot(
                            slot,
                            error,
                            RioBackendEffect::default(),
                        );
                    }
                    Err(_) => {
                        return RioSingleCompletion::complete_failure_slot(
                            slot,
                            Self::access_error_report("RIO TCP recv_multi delivery access failed"),
                            RioBackendEffect::default(),
                        );
                    }
                };
                let output = match FixedBuf::alloc_heap(
                    NonZeroUsize::new(init.diagnostics.data_buffer_length().max(1) as usize)
                        .expect("RIO receive capacity is non-zero"),
                    0,
                ) {
                    Ok(output) => output,
                    Err(_) => {
                        let _ = RioReceiveReplacement::abort_tcp_receive_delivery(
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
                match RioReceiveReplacement::prepare_tcp_receive_replacement(
                    hooks, &mut slot, init, delivery, output,
                ) {
                    Ok(replacement) => replacement,
                    Err(error) => RioSingleCompletion::complete_failure_slot(
                        slot,
                        error,
                        RioBackendEffect::default(),
                    ),
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

    pub(super) fn complete_slot(
        input: MultiShotCompletionInput<'_, '_, '_, '_>,
    ) -> CompletionSettlement<IocpSlotSpec, RioBackendEffect> {
        if input.ingress.init.op_kind == RioOpKind::TcpRecvMulti {
            return Self::complete_tcp_slot(input);
        }
        let MultiShotCompletionInput {
            hooks,
            mut slot,
            ingress,
            effect,
            result,
            completion,
            cancelled,
        } = input;
        let init = &ingress.init;
        let socket_key = init.socket_inflight.socket_key();
        let request = match (init.receive_slot_id, init.receive_slot_generation) {
            (Some(slot_id), Some(slot_generation)) => ReceiveRequest {
                request_id: init.request_id,
                slot_id,
                slot_generation,
            },
            _ => {
                return RioSingleCompletion::complete_failure_slot(
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
            match Self::receive_remote_addr(hooks, init) {
                Ok(addr) => addr,
                Err(error) => {
                    return RioSingleCompletion::complete_failure_slot(slot, error, effect);
                }
            }
        } else {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        };
        let receive_result = RioSingleCompletion::receive_completion_result(result, cancelled);
        let observation = RioCompletionObservation::new(init, result);
        let (pump_event, pump_diagnostics) = match slot.with_access_mut(|access| {
            let (_, payload) = access
                .operation_and_payload_mut()
                .map_err(|_| Self::access_error_report("RIO recv_multi slot payload is missing"))?;
            let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                return Err(Self::access_error_report(
                    "RIO recv_multi slot payload has the wrong kind",
                ));
            };
            user.receive_pump_mut().record_completion_observation(
                observation.status(),
                observation.bytes(),
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
                    Self::receive_pump_report(init, error, Some(observation), cancelled, None)
                })?;
            Ok((event, user.receive_pump().completion_diagnostics()))
        }) {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                return RioSingleCompletion::complete_failure_slot(slot, error, effect);
            }
            Err(_) => {
                return RioSingleCompletion::complete_failure_slot(
                    slot,
                    Self::access_error_report("RIO recv_multi slot access failed"),
                    effect,
                );
            }
        };

        if let Err(error) = hooks.release_backend_effect(effect) {
            return RioSingleCompletion::complete_failure_slot(
                slot,
                error,
                RioBackendEffect::default(),
            );
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
                hooks
                    .runtime_mut()
                    .finish_receive_pump(socket_key, init.token);
                RioSingleCompletion::complete_failure_slot(
                    slot,
                    Self::receive_pump_report(
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
                        Self::access_error_report("RIO recv_multi delivery payload is missing")
                    })?;
                    let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                        return Err(Self::access_error_report(
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
                        return RioSingleCompletion::complete_failure_slot(
                            slot,
                            error,
                            RioBackendEffect::default(),
                        );
                    }
                    Err(_) => {
                        return RioSingleCompletion::complete_failure_slot(
                            slot,
                            Self::access_error_report("RIO recv_multi delivery access failed"),
                            RioBackendEffect::default(),
                        );
                    }
                };
                let output = match FixedBuf::alloc_heap(
                    NonZeroUsize::new(init.diagnostics.data_buffer_length().max(1) as usize)
                        .expect("RIO datagram capacity is non-zero"),
                    0,
                ) {
                    Ok(output) => output,
                    Err(_) => {
                        let _ = RioReceiveReplacement::abort_receive_delivery(
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
                match RioReceiveReplacement::prepare_receive_replacement(
                    hooks, &mut slot, init, delivery, output,
                ) {
                    Ok(replacement) => replacement,
                    Err(error) => RioSingleCompletion::complete_failure_slot(
                        slot,
                        error,
                        RioBackendEffect::default(),
                    ),
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

    pub(super) fn access_error_report(note: &'static str) -> Report<IocpError> {
        IocpError::InvalidState
            .to_report()
            .push_ctx("scope", "rio.runtime.control_flow.multishot")
            .attach_note(note)
    }

    pub(super) fn receive_pump_report(
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
}
