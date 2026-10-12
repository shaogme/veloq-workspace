use veloq_std::format;

use diagweave::prelude::*;
use veloq_io_uring::cqueue;

use crate::{
    driver::{
        completion::{COMP_BACKEND_URING, CompletionEngine, UringBackendEffect},
        env::CqeEnv,
        lifecycle::SubmissionPhase,
        submission::SlotAccessReporter,
    },
    error::{UringError, UringResult},
    op::{Close, RecordPolicy, Slot, UringOperationDescriptor, UringRecordItem, UringSlotSpec},
};
use veloq_driver_core::{
    driver::{
        CancelMode, CompletionCleanupGuard, CompletionContinuation, CompletionFailure,
        CompletionSettlement, OpToken, PlatformOp, RawCompletion, UserCompletionEvent,
    },
    slot::{InFlightOrphaned, InFlightWaiting},
};

pub(super) struct KernelCompletionError {
    pub(super) report: Report<UringError>,
    pub(super) fallback_cleanup: bool,
    pub(super) cleanup: CompletionCleanupGuard,
}

struct KernelWaitingCompletionParts {
    final_result: UringResult<usize>,
    cleanup: CompletionCleanupGuard,
    record_item: UringResult<UringRecordItem>,
    operation_name: &'static str,
}

pub(crate) struct CompletionSlotRouter;

impl CompletionSlotRouter {
    fn record_item_policy_report(operation: &'static str, token: OpToken) -> Report<UringError> {
        UringError::Internal
            .report(
                "uring.driver.completion.record_item",
                "operation record-item policy did not produce a completion item",
            )
            .with_ctx("operation", operation)
            .with_ctx("token_index", token.index())
            .with_ctx("token_generation", token.generation())
    }

    pub(super) fn complete_udp_resume_waiting_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        logical_receiver_generation: u32,
        cqe_env: &mut CqeEnv<'_, '_>,
    ) -> Result<CompletionSettlement<UringSlotSpec, UringBackendEffect>, KernelCompletionError>
    {
        let (record_item, operation_name) = match slot.with_access_mut(|access| {
            let descriptor = access.operation().get_ref().descriptor();
            if descriptor.record_policy != RecordPolicy::UdpMultishot {
                return Err(Self::record_item_policy_report(
                    descriptor.name,
                    event.token(),
                ));
            }
            let record_item = unsafe {
                (descriptor.resume_item)(
                    access,
                    event.token(),
                    logical_receiver_generation,
                    cqe_env,
                )
            };
            Ok((record_item, descriptor.name))
        }) {
            Ok(Ok((record_item, operation_name))) => (record_item, operation_name),
            Ok(Err(report)) => {
                return Err(KernelCompletionError {
                    report,
                    fallback_cleanup: false,
                    cleanup: CompletionCleanupGuard::default(),
                });
            }
            Err(err) => {
                return Err(KernelCompletionError {
                    report: UringError::InvalidState.report(
                        "uring.complete_udp_resume_waiting_slot",
                        format!("slot corruption detected during UDP resume: {:?}", err),
                    ),
                    fallback_cleanup: false,
                    cleanup: CompletionCleanupGuard::default(),
                });
            }
        };
        let record_item = match record_item {
            Ok(record_item) => record_item,
            Err(report) => {
                return Err(KernelCompletionError {
                    report,
                    fallback_cleanup: false,
                    cleanup: CompletionCleanupGuard::default(),
                });
            }
        };
        match record_item {
            UringRecordItem::Ignored => Ok(CompletionSettlement::Ignore {
                effect: UringBackendEffect::None,
            }),
            UringRecordItem::Retained {
                logical_receiver_generation,
            } => {
                let effect =
                    logical_receiver_generation.map_or(UringBackendEffect::None, |generation| {
                        UringBackendEffect::UdpRearm {
                            token: event.token(),
                            logical_receiver_generation: generation,
                        }
                    });
                Ok(CompletionSettlement::Retained {
                    cleanup: CompletionCleanupGuard::default(),
                    effect,
                })
            }
            UringRecordItem::NewWithRearm {
                item,
                logical_receiver_generation,
            } => Ok(CompletionSettlement::User {
                event,
                payload: item,
                detail: None,
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::More,
                effect: UringBackendEffect::UdpRearm {
                    token: event.token(),
                    logical_receiver_generation,
                },
            }),
            UringRecordItem::New(item) => Ok(CompletionSettlement::User {
                event,
                payload: item,
                detail: None,
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::More,
                effect: UringBackendEffect::None,
            }),
            UringRecordItem::UseSubmitPayload => Err(KernelCompletionError {
                report: Self::record_item_policy_report(operation_name, event.token()),
                fallback_cleanup: false,
                cleanup: CompletionCleanupGuard::default(),
            }),
        }
    }

    pub(super) fn complete_kernel_waiting_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        token: OpToken,
        raw: RawCompletion,
        cqe_env: &mut CqeEnv<'_, '_>,
    ) -> Result<CompletionSettlement<UringSlotSpec, UringBackendEffect>, KernelCompletionError>
    {
        // `IORING_CQE_F_MORE`：内核声明这个操作还会继续投递完成。flags 的解读到此为止，
        // core 只见 `CompletionContinuation`。
        // `IORING_CQE_F_MORE` means the kernel will continue this operation; only `CompletionContinuation` crosses into core.
        let continuation = if cqueue::more(raw.flags) {
            CompletionContinuation::More
        } else {
            CompletionContinuation::Final
        };

        let parts = match slot.with_access_mut(|access| {
            let descriptor = access.operation().get_ref().descriptor();
            let final_result = unsafe { (descriptor.on_complete)(access, token, raw.res) };
            let cleanup = (descriptor.completion_cleanup)(raw.res);
            let record_item =
                unsafe { (descriptor.record_item)(access, token, raw.res, raw.flags, cqe_env) };
            KernelWaitingCompletionParts {
                final_result,
                cleanup,
                record_item,
                operation_name: descriptor.name,
            }
        }) {
            Ok(result) => result,
            Err(err) => {
                return Err(KernelCompletionError {
                    report: UringError::InvalidState.report(
                        "uring.complete_kernel_waiting_slot",
                        format!("slot corruption detected on completion: {:?}", err),
                    ),
                    fallback_cleanup: true,
                    cleanup: CompletionCleanupGuard::default(),
                });
            }
        };
        let KernelWaitingCompletionParts {
            final_result,
            cleanup,
            record_item,
            operation_name,
        } = parts;
        let record_item = match record_item {
            Ok(record_item) => record_item,
            Err(report) => {
                return Err(KernelCompletionError {
                    report,
                    fallback_cleanup: false,
                    cleanup,
                });
            }
        };
        let res_code = CompletionEngine::driver_result_to_event_res(&final_result);
        let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags);
        let res_is_ok = final_result.is_ok();
        // 没有更具体的 detail 时，用完成错误作为 detail，保持逐条路由语义。
        // When no specific detail exists, preserve per-CQE routing semantics by using the completion error as detail.
        let mut res_error = final_result.err();

        if let UringRecordItem::Retained {
            logical_receiver_generation,
        } = record_item
        {
            let effect =
                logical_receiver_generation.map_or(UringBackendEffect::None, |generation| {
                    UringBackendEffect::UdpRearm {
                        token,
                        logical_receiver_generation: generation,
                    }
                });
            return Ok(CompletionSettlement::Retained { cleanup, effect });
        }

        if let UringRecordItem::NewWithRearm {
            item,
            logical_receiver_generation,
        } = record_item
        {
            return Ok(CompletionSettlement::User {
                event,
                payload: item,
                detail: res_error.take().map(Err),
                cleanup,
                continuation: CompletionContinuation::More,
                effect: UringBackendEffect::UdpRearm {
                    token,
                    logical_receiver_generation,
                },
            });
        }

        if continuation.is_more() {
            // slot 原地不动：op 与提交 payload 还要给内核后续的完成用，cell 也必须停在
            // `InFlightWaiting` 才能继续路由。
            // Keep the slot in place because the kernel still uses its op and submission payload for later completions.
            let item = match record_item {
                UringRecordItem::New(item) => item,
                UringRecordItem::NewWithRearm { item, .. } => item,
                UringRecordItem::UseSubmitPayload
                | UringRecordItem::Ignored
                | UringRecordItem::Retained { .. } => {
                    return Err(KernelCompletionError {
                        report: Self::record_item_policy_report(operation_name, token),
                        fallback_cleanup: false,
                        cleanup,
                    });
                }
            };
            return Ok(CompletionSettlement::User {
                event,
                payload: item,
                detail: res_error.take().map(Err),
                cleanup,
                continuation,
                effect: UringBackendEffect::None,
            });
        }

        slot.platform_mut()
            .set_submission_phase(SubmissionPhase::Terminal);
        let mut completed = slot.complete();
        let (submit_payload, detail) = completed.take_completion_data();
        // multishot 的终态仍产生一条 item，并在此释放提交 payload（例如监听 socket）。
        // A final multishot completion still produces an item and releases its submit payload, such as a listening socket.
        let payload = match record_item {
            UringRecordItem::New(item) => {
                drop(submit_payload);
                item
            }
            UringRecordItem::UseSubmitPayload => {
                let Some(payload) = submit_payload else {
                    drop(detail);
                    return Err(KernelCompletionError {
                        report: UringError::InvalidState.report(
                            "uring.complete_kernel_waiting_slot",
                            "slot payload missing on completion",
                        ),
                        fallback_cleanup: false,
                        cleanup,
                    });
                };
                payload
            }
            UringRecordItem::Ignored
            | UringRecordItem::NewWithRearm { .. }
            | UringRecordItem::Retained { .. } => {
                drop(submit_payload);
                drop(detail);
                return Err(KernelCompletionError {
                    report: Self::record_item_policy_report(operation_name, token),
                    fallback_cleanup: false,
                    cleanup,
                });
            }
        };

        let effect = if res_is_ok {
            if let Some(close) = <Close as UringOperationDescriptor>::user_payload_ref(&payload) {
                UringBackendEffect::CloseCompleted {
                    token,
                    fd: close.fd,
                }
            } else {
                UringBackendEffect::None
            }
        } else {
            UringBackendEffect::None
        };

        let detail = detail.or_else(|| res_error.take().map(Err));
        let _ = completed.take_op();

        Ok(CompletionSettlement::User {
            event,
            payload,
            detail,
            cleanup,
            continuation,
            effect,
        })
    }

    pub(super) fn complete_timer_waiting_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        slot.platform_mut().clear_timer();
        slot.platform_mut()
            .set_submission_phase(SubmissionPhase::Terminal);
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        let Some(payload) = payload else {
            drop(detail);
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    UringError::InvalidState.report(
                        "uring.complete_timer_waiting_slot",
                        "slot payload missing on timer completion",
                    ),
                    CompletionCleanupGuard::default(),
                    UringBackendEffect::None,
                ),
            };
        };

        CompletionSettlement::User {
            event,
            payload,
            detail,
            cleanup: CompletionCleanupGuard::default(),
            // 软件定时器只触发一次。
            // Software timers fire once.
            continuation: CompletionContinuation::Final,
            effect: UringBackendEffect::None,
        }
    }

    pub(super) fn complete_cancel_waiting_slot(
        slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        mode: CancelMode,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        Self::complete_local_cancel_slot(slot, event, mode, false)
    }

    pub(super) fn complete_submission_failure_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        report: Option<Report<UringError>>,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        let event_res = event.res();
        slot.platform_mut()
            .set_submission_phase(SubmissionPhase::Terminal);
        let cleanup = match slot.with_access_mut(|access| {
            PlatformOp::completion_cleanup(access.operation_mut(), event_res)
        }) {
            Ok(cleanup) => cleanup,
            Err(err) => {
                return CompletionSettlement::TerminalFailure {
                    failure: CompletionFailure::terminal(
                        SlotAccessReporter::report(
                            "uring.complete_submission_failure_slot.cleanup",
                            err,
                        ),
                        CompletionCleanupGuard::default(),
                        UringBackendEffect::None,
                    ),
                };
            }
        };
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        let Some(payload) = payload else {
            drop(detail);
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    UringError::InvalidState.report(
                        "uring.complete_submission_failure_slot",
                        "slot payload missing on submission failure",
                    ),
                    cleanup,
                    UringBackendEffect::None,
                ),
            };
        };

        CompletionSettlement::User {
            event,
            payload,
            detail: detail.or(report.map(Err)),
            cleanup,
            // 提交失败的操作未进入内核，因此不会再收到完成。
            // A submission failure never reached the kernel, so no completion can follow.
            continuation: CompletionContinuation::Final,
            effect: UringBackendEffect::None,
        }
    }

    pub(super) fn complete_local_cancel_slot(
        mut slot: Slot<'_, InFlightWaiting>,
        event: UserCompletionEvent,
        mode: CancelMode,
        orphaned: bool,
    ) -> CompletionSettlement<UringSlotSpec, UringBackendEffect> {
        slot.platform_mut()
            .set_submission_phase(SubmissionPhase::Terminal);
        let cleanup = match slot.with_access_mut(|access| {
            let operation = access.operation_mut();
            if mode == CancelMode::Abandon || orphaned {
                PlatformOp::orphan_cleanup(operation, event.res())
            } else {
                PlatformOp::completion_cleanup(operation, event.res())
            }
        }) {
            Ok(cleanup) => cleanup,
            Err(err) => {
                return CompletionSettlement::TerminalFailure {
                    failure: CompletionFailure::terminal(
                        SlotAccessReporter::report("uring.complete_local_cancel_slot.cleanup", err),
                        CompletionCleanupGuard::default(),
                        UringBackendEffect::None,
                    ),
                };
            }
        };
        let mut completed = slot.complete();
        let (payload, detail) = completed.take_completion_data();
        let _ = completed.take_op();

        match (mode, payload) {
            (CancelMode::UserVisible, Some(payload)) => CompletionSettlement::User {
                event,
                payload,
                detail,
                cleanup,
                // 本地取消终止操作，包括原本的 multishot 操作。
                // Local cancellation ends the operation, including one that was originally multishot.
                continuation: CompletionContinuation::Final,
                effect: UringBackendEffect::None,
            },
            (CancelMode::UserVisible, None) => {
                drop(detail);
                CompletionSettlement::TerminalFailure {
                    failure: CompletionFailure::terminal(
                        UringError::InvalidState.report(
                            "uring.complete_local_cancel_slot",
                            "slot payload missing on cancel",
                        ),
                        cleanup,
                        UringBackendEffect::None,
                    ),
                }
            }
            (CancelMode::Abandon, payload) => {
                drop(payload);
                drop(detail);
                CompletionSettlement::Cleanup {
                    cleanup,
                    continuation: CompletionContinuation::Final,
                    effect: UringBackendEffect::None,
                }
            }
        }
    }

    /// 已放弃的 multishot 收到中间完成时只运行 cleanup，并保留 slot 内容。
    /// An abandoned multishot intermediate completion runs cleanup while preserving the slot contents.
    ///
    /// 终态路由会取出 op 和 completion data；内核还要使用它们投递后续完成。
    /// Final routing takes the operation and completion data, which the kernel still needs for later CQEs.
    pub(super) fn cleanup_orphaned_streaming_slot(
        mut slot: Slot<'_, InFlightOrphaned>,
        cqe_res: i32,
    ) -> UringResult<(CompletionCleanupGuard, bool)> {
        let cleanup = slot
            .with_access_mut(|access| PlatformOp::orphan_cleanup(access.operation_mut(), cqe_res))
            .map_err(|err| {
                SlotAccessReporter::report("uring.cleanup_orphaned_streaming_slot.cleanup", err)
            })?;
        Ok((cleanup, true))
    }

    pub(super) fn cleanup_orphaned_slot(
        mut slot: Slot<'_, InFlightOrphaned>,
        cqe_res: i32,
    ) -> UringResult<(CompletionCleanupGuard, bool)> {
        slot.platform_mut()
            .set_submission_phase(SubmissionPhase::Terminal);
        let cleanup = slot
            .with_access_mut(|access| PlatformOp::orphan_cleanup(access.operation_mut(), cqe_res))
            .map_err(|err| {
                SlotAccessReporter::report("uring.cleanup_orphaned_slot.cleanup", err)
            })?;
        let mut completed = slot.complete();
        let (payload, detail) = completed.take_completion_data();
        let _ = completed.take_op();
        drop(payload);
        drop(detail);
        Ok((cleanup, true))
    }
}
