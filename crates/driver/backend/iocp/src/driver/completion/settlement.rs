use veloq_std::{format, io, mem};

use diagweave::prelude::*;
use veloq_driver_core::{
    driver::{
        CancelMode, CompletionCleanupGuard, CompletionContinuation, CompletionFailure,
        CompletionSettlement, PlatformOp, UserCompletionEvent,
    },
    slot::{InFlightOrphaned, InFlightWaiting},
};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

use crate::{
    error::{IocpError, IocpResult, iocp_report_to_event_res},
    ext::Extensions,
    op::{IocpOp, IocpOpPayload, IocpSlotSpec, Slot},
    rio::SocketInflightToken,
};

use super::protocol::{COMP_BACKEND_IOCP, IocpBackendEffect, IocpSubmissionFailurePhase};

pub(super) struct SettlementService;

impl SettlementService {
    pub(super) fn calculate_io_result_from_slot(
        ext: &Extensions,
        guard: &mut Slot<'_, InFlightWaiting>,
        event_res: i32,
    ) -> IocpResult<IocpResult<usize>> {
        let user_data = guard.snapshot().index;
        let mut io_result = if event_res < 0 {
            Err(IocpError::CompletionWait.io_report(
                "iocp.driver.calculate_io_result_from_slot",
                io::Error::from_raw_os_error(-event_res),
            ))
        } else {
            Ok(event_res as usize)
        };

        let op_res = guard.with_access_mut(|access| {
            let iocp_op = access.operation_mut().get_mut();
            let blocking_res = iocp_op
                .header
                .blocking_completion
                .take()
                .and_then(|completion| completion.take_result());
            if let Some(res) = blocking_res {
                io_result = res;
            } else if matches!(
                &iocp_op.payload,
                IocpOpPayload::Open(_)
                    | IocpOpPayload::Close(_)
                    | IocpOpPayload::Fsync(_)
                    | IocpOpPayload::FsyncRaw(_)
                    | IocpOpPayload::SyncRange(_)
                    | IocpOpPayload::SyncRangeRaw(_)
                    | IocpOpPayload::Fallocate(_)
                    | IocpOpPayload::FallocateRaw(_)
            ) {
                io_result = IocpError::CompletionWait
                    .push_ctx("scope", "iocp/driver")
                    .with_ctx("user_data", user_data)
                    .attach_note("missing blocking result for offloaded file completion");
            } else if let Ok(val) = io_result {
                io_result = iocp_op
                    .on_complete(val, ext)
                    .attach_note("IOCP completion hook failed");
            }
        });

        if let Err(err) = op_res {
            return Err(IocpError::InvalidState.report(
                "iocp.calculate_io_result_from_slot",
                format!("slot op missing on completion: {:?}", err),
            ));
        }

        Ok(io_result)
    }

    pub(super) fn complete_iocp_waiting_slot(
        guard: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        io_result: IocpResult<usize>,
        socket_inflight: Option<SocketInflightToken>,
        accept_multi: bool,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let mut io_detail = Some(io_result);
        let effect = socket_inflight
            .map(IocpBackendEffect::SocketInflight)
            .unwrap_or_default();
        let mut guard = guard.complete();

        if guard.platform_mut().is_background {
            let _ = guard.take_op();
            let _ = guard.take_completion_data();
            let _data = mem::take(guard.platform_mut());
            return CompletionSettlement::Cleanup {
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::Final,
                effect,
            };
        }

        let completion_res = io_detail
            .as_ref()
            .map(Self::io_result_to_event_res)
            .unwrap_or(event.res());
        let cleanup = if let Some(io_result) = io_detail.as_ref() {
            guard
                .with_access_mut(|access| {
                    let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), io_result);
                    if !accept_multi {
                        access.operation_mut().get_mut().unbind_user_payload();
                    }
                    cleanup
                })
                .unwrap_or_default()
        } else {
            let _ = guard.with_access_mut(|access| {
                access.operation_mut().get_mut().unbind_user_payload();
            });
            CompletionCleanupGuard::default()
        };
        let event =
            UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, event.token(), completion_res, 0);

        if accept_multi {
            let record_payload = guard
                .with_access_mut(|access| access.operation().get_ref().accepted_socket_record())
                .ok()
                .flatten();
            if let Some(payload) = record_payload {
                let detail = io_detail.take();
                let continuation = guard
                    .with_access_mut(|access| {
                        access.operation().get_ref().multishot_vtable().map_or(
                            CompletionContinuation::Final,
                            |vtable| {
                                (vtable.continuation)(
                                    detail
                                        .as_ref()
                                        .expect("IOCP completion detail must remain available"),
                                )
                            },
                        )
                    })
                    .unwrap_or(CompletionContinuation::Final);
                return CompletionSettlement::User {
                    event,
                    payload,
                    detail,
                    cleanup,
                    continuation,
                    effect,
                };
            }

            let _ = guard.with_access_mut(|access| {
                access.operation_mut().get_mut().unbind_user_payload();
            });
            let _ = guard.take_op();
            let _ = guard.take_completion_data();
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    IocpError::InvalidState
                        .to_report()
                        .push_ctx("scope", "iocp.complete_iocp_waiting_slot")
                        .attach_note("AcceptMulti record payload encoder is missing"),
                    cleanup,
                    effect,
                ),
            };
        }

        let (payload, detail) = guard.take_completion_data();
        if let Some(payload) = payload {
            let _ = guard.take_op();
            let _data = mem::take(guard.platform_mut());
            CompletionSettlement::User {
                event,
                payload,
                detail: detail.or_else(|| io_detail.take()),
                cleanup,
                continuation: CompletionContinuation::Final,
                effect,
            }
        } else {
            drop(detail);
            let _ = guard.take_op();
            let _data = mem::take(guard.platform_mut());
            CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    IocpError::InvalidState.report(
                        "iocp.complete_iocp_waiting_slot",
                        "slot payload missing on completion",
                    ),
                    cleanup,
                    effect,
                ),
            }
        }
    }

    pub(super) fn complete_iocp_replacement_failure_slot(
        slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        io_result: IocpResult<usize>,
        replacement_error: Report<IocpError>,
        effect: IocpBackendEffect,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let replacement_error = replacement_error
            .with_ctx(
                "submission_phase",
                IocpSubmissionFailurePhase::Replacement.as_str(),
            )
            .attach_note("multishot replacement submission failed after a record completed");
        let terminal_res = iocp_report_to_event_res(&replacement_error);
        let mut guard = slot.complete();
        let record_payload = guard
            .with_access_mut(|access| {
                access
                    .operation()
                    .get_ref()
                    .multishot_vtable()
                    .map(|vtable| (vtable.encode_accepted_socket)())
            })
            .ok()
            .flatten();
        let cleanup = guard
            .with_access_mut(|access| {
                let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
                access.operation_mut().get_mut().unbind_user_payload();
                cleanup
            })
            .unwrap_or_default();

        let Some(payload) = record_payload else {
            let _ = guard.take_op();
            let _ = guard.take_completion_data();
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    IocpError::InvalidState
                        .to_report()
                        .push_ctx("scope", "iocp.complete_iocp_replacement_failure")
                        .attach_note("AcceptMulti record payload encoder is missing"),
                    cleanup,
                    effect,
                ),
            };
        };

        CompletionSettlement::UserThenTerminal {
            event: UserCompletionEvent::from_parts(
                COMP_BACKEND_IOCP,
                event.token(),
                Self::io_result_to_event_res(&io_result),
                0,
            ),
            payload,
            detail: Some(io_result),
            cleanup,
            effect,
            terminal_event: UserCompletionEvent::from_parts(
                COMP_BACKEND_IOCP,
                event.token(),
                terminal_res,
                0,
            ),
            terminal_error: replacement_error,
            terminal_cleanup: CompletionCleanupGuard::default(),
            terminal_effect: IocpBackendEffect::None,
        }
    }

    pub(super) fn complete_iocp_failure_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        error: Report<IocpError>,
        effect: IocpBackendEffect,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let completion_result: IocpResult<usize> = Err(error);
        let cleanup = slot
            .with_access_mut(|access| {
                let cleanup =
                    PlatformOp::completion_cleanup(access.operation_mut(), &completion_result);
                access.operation_mut().get_mut().unbind_user_payload();
                cleanup
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

    pub(super) fn complete_iocp_abandoned_waiting_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        io_result: IocpResult<usize>,
        effect: IocpBackendEffect,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let cleanup = slot
            .with_access_mut(|access| {
                let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
                access.operation_mut().get_mut().unbind_user_payload();
                cleanup
            })
            .unwrap_or_default();
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let _ = completed.take_completion_data();
        CompletionSettlement::Cleanup {
            cleanup,
            continuation: CompletionContinuation::Final,
            effect,
        }
    }

    pub(super) fn complete_iocp_cancelled_accept_multi_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        io_result: IocpResult<usize>,
        effect: IocpBackendEffect,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let cleanup = slot
            .with_access_mut(|access| {
                let cleanup = PlatformOp::completion_cleanup(access.operation_mut(), &io_result);
                access.operation_mut().get_mut().unbind_user_payload();
                cleanup
            })
            .unwrap_or_default();
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let _ = completed.take_completion_data();
        let error = IocpError::CompletionWait.io_report(
            "iocp.driver.accept_multi.cancel",
            io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
        );
        CompletionSettlement::TerminalFailure {
            failure: CompletionFailure::terminal(error, cleanup, effect),
        }
    }

    pub(super) fn complete_timer_waiting_slot(
        slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let io_result: IocpResult<usize> = Ok(0);
        Self::complete_iocp_waiting_slot(slot, event, io_result, None, false)
    }

    pub(super) fn complete_submission_failure_slot(
        slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        report: Option<Report<IocpError>>,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let io_result = report.unwrap_or_else(|| {
            IocpError::Submission
                .to_report()
                .push_ctx("scope", "iocp.driver.submission_failure")
                .set_error_code((-event.res()).max(1))
                .attach_note("IOCP submission failed")
        });
        Self::complete_iocp_waiting_slot(slot, event, Err(io_result), None, false)
    }

    pub(super) fn complete_cancel_waiting_slot(
        slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        mode: CancelMode,
    ) -> CompletionSettlement<IocpSlotSpec, IocpBackendEffect> {
        let abort_result: IocpResult<usize> = IocpError::CompletionWait
            .push_ctx("scope", "iocp.driver.cancel")
            .set_error_code((-event.res()).max(1))
            .attach_note("operation aborted locally");
        if mode == CancelMode::UserVisible {
            Self::complete_iocp_waiting_slot(slot, event, abort_result, None, false)
        } else {
            let mut completed = slot.complete();
            let cleanup = completed
                .with_access_mut(|access| {
                    PlatformOp::orphan_cleanup(access.operation_mut(), &abort_result)
                })
                .unwrap_or_default();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            drop(payload);
            drop(detail);
            CompletionSettlement::Cleanup {
                cleanup,
                continuation: CompletionContinuation::Final,
                effect: IocpBackendEffect::None,
            }
        }
    }

    pub(super) fn complete_iocp_orphaned_slot(
        slot: Slot<'_, InFlightOrphaned>,
        event_res: i32,
    ) -> (CompletionCleanupGuard, Option<SocketInflightToken>) {
        let mut completed = slot.complete();
        let io_result = if event_res >= 0 {
            Ok(event_res as usize)
        } else {
            Err(IocpError::CompletionWait.io_report(
                "iocp.driver.process_completion.orphaned",
                io::Error::from_raw_os_error(-event_res),
            ))
        };
        let (cleanup, socket_inflight) = completed
            .with_access_mut(|access| {
                let cleanup = PlatformOp::orphan_cleanup(access.operation_mut(), &io_result);
                let socket_inflight =
                    Self::take_socket_inflight_from_op(access.operation_mut().get_mut());
                (cleanup, socket_inflight)
            })
            .unwrap_or_default();
        let _ = completed.take_op();
        let _ = completed.take_completion_data();
        (cleanup, socket_inflight)
    }

    #[inline]
    pub(super) fn take_socket_inflight_from_slot(
        slot: &mut Slot<'_, InFlightWaiting>,
    ) -> Option<SocketInflightToken> {
        slot.with_access_mut(|access| {
            Self::take_socket_inflight_from_op(access.operation_mut().get_mut())
        })
        .ok()
        .flatten()
    }

    #[inline]
    fn take_socket_inflight_from_op(op: &mut IocpOp) -> Option<SocketInflightToken> {
        if op.header.in_flight {
            op.header.in_flight = false;
        }
        op.header.socket_inflight.take()
    }

    #[inline]
    fn io_result_to_event_res(res: &IocpResult<usize>) -> i32 {
        match res {
            Ok(v) => (*v).min(i32::MAX as usize) as i32,
            Err(e) => iocp_report_to_event_res(e),
        }
    }
}
