use super::{
    model::{
        RioBackendEffect, RioCompletionHooks, RioCompletionObservation, RioCompletionResult,
        RioIngress, RioResultData,
    },
    multishot::{MultiShotCompletionInput, RioMultiShotCompletion},
};
use crate::{
    driver::COMP_BACKEND_RIO,
    error::{IocpError, IocpResult},
    op::{IocpSlotSpec, Slot},
    rio::core::{RioOpKind, RioOpRequestInit},
};
use diagweave::prelude::*;
use veloq_driver_core::{
    driver::{
        CompletionCleanupGuard, CompletionContinuation, CompletionFailure, CompletionSettlement,
        PlatformOp, UserCompletionEvent,
    },
    op::types::OpKind,
    platform::receive_pump::{ReceiveCompletionResult, ReceivePumpError},
    slot::{InFlightOrphaned, InFlightWaiting},
};
use veloq_std::mem::take;
use windows_sys::Win32::{
    Foundation::ERROR_OPERATION_ABORTED,
    Networking::WinSock::{WSA_OPERATION_ABORTED, WSAENOBUFS},
};

pub(super) struct RioSingleCompletion;

impl RioSingleCompletion {
    pub(super) fn complete_waiting_slot(
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
            return Self::complete_failure_slot(slot, report, effect);
        }

        let cancelled = slot.platform().rio_user_cancel_requested;
        let observation = RioCompletionObservation::new(init, result);
        let mut completion = if cancelled {
            Err(Self::completion_error_report(
                init,
                observation,
                true,
                "RIO operation was cancelled before kernel completion",
            ))
        } else if result.status() == 0 {
            Ok(result.bytes() as usize)
        } else {
            Err(Self::completion_error_report(
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
            return RioMultiShotCompletion::complete_slot(MultiShotCompletionInput {
                hooks,
                slot,
                ingress,
                effect,
                result,
                completion,
                cancelled,
            });
        }

        let _ = slot.with_access_mut(|access| {
            let iocp_op = access.operation_mut().get_mut();
            if iocp_op.header.in_flight {
                iocp_op.header.in_flight = false;
            }
            if !cancelled && let Ok(bytes) = completion.as_ref().copied() {
                completion = iocp_op
                    .on_complete(bytes, hooks.ext())
                    .with_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                    .attach_note("rio op completion hook failed");
            }
        });

        let res_code = RioCompletionResult::event_code(&completion);
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
                // RIO also has no multishot here: one request produces one completion. / 此处 RIO 也没有 multishot：一个请求对应一条完成。
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

    pub(super) fn completion_error_report(
        init: &RioOpRequestInit,
        observation: RioCompletionObservation,
        user_cancelled: bool,
        note: &'static str,
    ) -> Report<IocpError> {
        let error_code = if user_cancelled {
            ERROR_OPERATION_ABORTED as i32
        } else {
            observation.status()
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
                    .with_ctx("rq_raw", init.diagnostics.rq_raw())
                    .with_ctx("data_buffer_id", init.diagnostics.data_buffer_id())
                    .with_ctx("data_buffer_offset", init.diagnostics.data_buffer_offset())
                    .with_ctx("data_buffer_length", init.diagnostics.data_buffer_length())
                    .with_ctx("addr_slot", init.addr_slot.unwrap_or(usize::MAX))
                    .set_error_code(error_code),
                user_cancelled,
            )
            .attach_note(note)
    }

    pub(super) fn receive_completion_result(
        result: RioResultData,
        cancel_requested: bool,
    ) -> ReceiveCompletionResult {
        if result.status() == 0 {
            return if cancel_requested {
                Err(ReceivePumpError::ReceiveCancelled)
            } else {
                Ok(result.bytes() as usize)
            };
        }

        match result.status() {
            WSA_OPERATION_ABORTED => Err(ReceivePumpError::ReceiveCancelled),
            WSAENOBUFS => Err(ReceivePumpError::ReceiveBufferExhausted),
            code => Err(ReceivePumpError::ReceiveOsError { code }),
        }
    }

    pub(super) fn complete_failure_slot(
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

    pub(super) fn complete_orphaned_slot(
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
            let last_request =
                hooks
                    .runtime()
                    .socket_runtime
                    .get(&socket_key)
                    .is_some_and(|state| {
                        state.receive_pump_token == Some(init.token)
                            && state.receive_pump_inflight <= 1
                    });
            if !last_request {
                return CompletionSettlement::Cleanup {
                    cleanup: CompletionCleanupGuard::default(),
                    continuation: CompletionContinuation::More,
                    effect,
                };
            }
            hooks
                .runtime_mut()
                .finish_receive_pump(socket_key, init.token);
        }

        let mut guard = slot.complete();
        let orphan_result = if generation_mismatch {
            IocpError::CompletionWait
                .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("rio_op_kind", init.op_kind.as_str())
                .with_ctx("rio_request_id", init.request_id)
                .attach_note("orphaned RIO completion had platform generation mismatch")
        } else if result.status() == 0 {
            Ok(result.bytes() as usize)
        } else {
            IocpError::CompletionWait
                .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("rio_op_kind", init.op_kind.as_str())
                .with_ctx("rio_request_id", init.request_id)
                .set_error_code(result.status())
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
}
