use super::{
    model::{RioBackendEffect, RioCompletionHooks},
    multishot::RioMultiShotCompletion,
};
use crate::{
    config::RawHandle,
    driver::COMP_BACKEND_RIO,
    error::{IocpError, IocpResult},
    op::{IocpSlotSpec, IocpUserPayload, Slot},
    rio::{
        RioTarget,
        core::{RioOpKind, RioOpRequestInit},
    },
};
use diagweave::prelude::*;
use veloq_buf::FixedBuf;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, CompletionSettlement, UserCompletionEvent},
    op::types::{ProvidedBuf, UdpRecvMultiBackend},
    platform::receive_pump::{ReceiveDelivery, ReceivePumpError, ReplacementSubmit},
    slot::InFlightWaiting,
};
use veloq_std::string::ToString;

pub(super) struct RioReceiveReplacement;

impl RioReceiveReplacement {
    pub(super) fn abort_receive_delivery(
        slot: &mut Slot<'_, InFlightWaiting>,
        delivery: ReceiveDelivery,
        error: ReceivePumpError,
    ) -> IocpResult<()> {
        slot.with_access_mut(|access| {
            let (_, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO receive delivery payload is missing",
                )
            })?;
            let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
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
        .map_err(|_| {
            RioMultiShotCompletion::access_error_report("RIO receive delivery access failed")
        })?
    }

    pub(super) fn abort_tcp_receive_delivery(
        slot: &mut Slot<'_, InFlightWaiting>,
        delivery: ReceiveDelivery,
        error: ReceivePumpError,
    ) -> IocpResult<()> {
        slot.with_access_mut(|access| {
            let (operation, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery payload is missing",
                )
            })?;
            let IocpUserPayload::RecvMulti(_) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery payload has the wrong kind",
                ));
            };
            let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery has no receive pump",
                ));
            };
            pump.abort_delivery(delivery, error).map_err(|error| {
                IocpError::CompletionWait
                    .to_report()
                    .with_ctx("receive_pump_error", error.to_string())
            })
        })
        .map_err(|_| {
            RioMultiShotCompletion::access_error_report("RIO TCP receive delivery access failed")
        })?
    }

    pub(super) fn prepare_tcp_receive_replacement(
        hooks: &mut RioCompletionHooks<'_>,
        slot: &mut Slot<'_, InFlightWaiting>,
        init: &RioOpRequestInit,
        delivery: ReceiveDelivery,
        mut output: FixedBuf,
    ) -> Result<CompletionSettlement<IocpSlotSpec, RioBackendEffect>, Report<IocpError>> {
        let request = delivery.request();
        let replacement = match slot.with_access_mut(|access| {
            let (operation, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive replacement payload is missing",
                )
            })?;
            let IocpUserPayload::RecvMulti(user) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive replacement payload has the wrong kind",
                ));
            };
            let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive replacement has no receive pump",
                ));
            };
            let source = pump
                .slot(request.slot_id)
                .and_then(|slot| slot.completed_data())
                .ok_or_else(|| {
                    RioMultiShotCompletion::access_error_report(
                        "RIO TCP receive slot has no completed data",
                    )
                })?;
            if source.len() < delivery.len() {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive completion exceeds written data",
                ));
            }
            output.spare_capacity_mut()[..delivery.len()]
                .copy_from_slice(&source[..delivery.len()]);
            if !delivery.replacement_required() {
                return Ok(ReplacementSubmit::NotSubmitted);
            }
            let reservation = pump.reserve_replacement(&delivery).map_err(|error| {
                RioMultiShotCompletion::receive_pump_report(init, error, None, false, None)
            })?;
            let raw = RawHandle::new(init.socket_inflight.socket_key());
            let submitted = hooks.try_submit_recv_multi_replacement(
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
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive replacement access failed",
                ));
            }
        };
        let record = match slot.with_access_mut(|access| {
            let (operation, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery payload disappeared",
                )
            })?;
            let IocpUserPayload::RecvMulti(_) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery payload changed kind",
                ));
            };
            let Some(pump) = operation.get_mut().recv_multi_pump_mut() else {
                return Err(RioMultiShotCompletion::access_error_report(
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
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO TCP receive delivery access failed",
                ));
            }
        };
        if record.continuation.is_final() {
            hooks
                .runtime_mut()
                .finish_receive_pump(init.socket_inflight.socket_key(), init.token);
        }
        let packet_len = record.packet.buf.len();
        let buffer = record.packet.buf.into_fixed_buf().ok_or_else(|| {
            RioMultiShotCompletion::access_error_report(
                "RIO TCP receive record has no output buffer",
            )
        })?;
        let continuation = record.continuation;
        let event =
            UserCompletionEvent::from_parts(COMP_BACKEND_RIO, init.token, packet_len as i32, 0);
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

    pub(super) fn prepare_receive_replacement(
        hooks: &mut RioCompletionHooks<'_>,
        slot: &mut Slot<'_, InFlightWaiting>,
        init: &RioOpRequestInit,
        delivery: ReceiveDelivery,
        mut output: FixedBuf,
    ) -> Result<CompletionSettlement<IocpSlotSpec, RioBackendEffect>, Report<IocpError>> {
        let request = delivery.request();
        let replacement = match slot.with_access_mut(|access| {
            let (_, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO receive replacement payload is missing",
                )
            })?;
            let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO receive replacement payload has the wrong kind",
                ));
            };
            let source = user
                .receive_pump()
                .slot(request.slot_id)
                .and_then(|slot| slot.completed_data())
                .ok_or_else(|| {
                    RioMultiShotCompletion::access_error_report(
                        "RIO receive slot has no completed data",
                    )
                })?;
            if source.len() < delivery.len() {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO receive completion exceeds written data",
                ));
            }
            output.spare_capacity_mut()[..delivery.len()]
                .copy_from_slice(&source[..delivery.len()]);
            if !delivery.replacement_required() {
                return Ok(ReplacementSubmit::NotSubmitted);
            }
            let reservation = user
                .receive_pump_mut()
                .reserve_replacement(&delivery)
                .map_err(|error| {
                    RioMultiShotCompletion::receive_pump_report(init, error, None, false, None)
                })?;
            let raw = RawHandle::new(init.socket_inflight.socket_key());
            let submitted = hooks.try_submit_recv_multi_replacement(
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
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO receive replacement access failed",
                ));
            }
        };
        let record = match slot.with_access_mut(|access| {
            let (_, payload) = access.operation_and_payload_mut().map_err(|_| {
                RioMultiShotCompletion::access_error_report(
                    "RIO receive delivery payload disappeared",
                )
            })?;
            let IocpUserPayload::UdpRecvMulti(user) = &mut *payload else {
                return Err(RioMultiShotCompletion::access_error_report(
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
            Err(_) => {
                return Err(RioMultiShotCompletion::access_error_report(
                    "RIO receive delivery access failed",
                ));
            }
        };
        if record.continuation.is_final() {
            hooks
                .runtime_mut()
                .finish_receive_pump(init.socket_inflight.socket_key(), init.token);
        }
        let packet_len = record.packet.buf.len();
        let continuation = record.continuation;
        let packet = record.packet;
        let event =
            UserCompletionEvent::from_parts(COMP_BACKEND_RIO, init.token, packet_len as i32, 0);
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
}
