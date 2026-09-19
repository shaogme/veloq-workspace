use crate::{
    DriverCoreError, DriverError, DriverResult,
    driver::registry::OpRegistry,
    slot::{
        InFlightOrphaned, InFlightWaiting, Slot, SlotCompletion, SlotCompletionDiagnostics,
        SlotError, SlotPayload, SlotRegistryExt, SlotSpec,
    },
};
use diagweave::{DiagnosticError, Report};
use veloq_std::format;

use super::{
    AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, CompletionContinuation,
    CompletionDispatch, CompletionEnvelope, CompletionPacket, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsBackend, RawCompletion, RecordCompletionOutcome,
    RecordCompletionResult, RoutedSlotCompletion, SharedCompletionTable, UserCompletionEvent,
    dispatch_envelope, finalize_orphaned_checked, finalize_waiting_checked, route_user_completion,
    run_rejected_cleanup,
};

pub type HookResult<Spec, T> = DriverResult<T, SlotError<Spec>>;

#[derive(Debug, Clone, Copy)]
pub struct CompletionWritePermit {
    _private: (),
}

impl CompletionWritePermit {
    const fn new() -> Self {
        Self { _private: () }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntheticCompletionSource {
    Timer,
    Cancel,
    SubmissionFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIngress<BackendIngress = ()> {
    Kernel(CompletionEnvelope),
    User(UserCompletionEvent),
    Synthetic {
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
    },
    Backend(BackendIngress),
    Anomaly {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum CompletionSource<'a, BackendIngress> {
    Kernel,
    User,
    Synthetic(SyntheticCompletionSource),
    Backend(&'a BackendIngress),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionControl {
    Waker {
        id: u64,
        raw: RawCompletion,
    },
    Cancel {
        ticket: super::CancelTicket,
        raw: RawCompletion,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionSlotDisposition {
    /// The slot has reached a final state and may be removed from the registry.
    Finalized,
    /// The operation may still produce a completion and must remain addressable.
    Retained,
    /// The completion path failed while the kernel may still reference the operation.
    Quarantined,
    /// The settlement belongs to a control or diagnostic event, not to a slot.
    NotApplicable,
}

/// Error and ownership information for a completion that could not be published normally.
///
/// A backend must return this value after a CQE has been consumed.  In particular, returning a
/// bare `Err` is not sufficient because the core would no longer know whether the operation may
/// be finalized or must remain addressable for a later `MORE` completion.
pub struct CompletionFailure<Spec: SlotSpec, Effect> {
    pub error: Report<SlotError<Spec>>,
    pub cleanup: CompletionCleanupGuard,
    pub continuation: CompletionContinuation,
    pub disposition: CompletionSlotDisposition,
    pub effect: Effect,
}

impl<Spec: SlotSpec, Effect> CompletionFailure<Spec, Effect> {
    pub fn terminal(
        error: Report<SlotError<Spec>>,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    ) -> Self {
        Self {
            error,
            cleanup,
            continuation: CompletionContinuation::Final,
            disposition: CompletionSlotDisposition::Finalized,
            effect,
        }
    }

    pub fn quarantined(
        error: Report<SlotError<Spec>>,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    ) -> Self {
        Self {
            error,
            cleanup,
            continuation: CompletionContinuation::More,
            disposition: CompletionSlotDisposition::Quarantined,
            effect,
        }
    }

    pub fn control(error: Report<SlotError<Spec>>, effect: Effect) -> Self {
        Self {
            error,
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::Final,
            disposition: CompletionSlotDisposition::NotApplicable,
            effect,
        }
    }
}

pub enum CompletionSettlement<Spec, Effect>
where
    Spec: SlotSpec,
{
    User {
        event: UserCompletionEvent,
        payload: SlotPayload<Spec>,
        detail: Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
        cleanup: CompletionCleanupGuard,
        /// 这条完成之后该操作是否还会再投递完成。单发路径一律 `Final`——写成显式字段
        /// 而不是隐式默认，是为了让每个后端 hook 都在自己那一行声明这件事。
        continuation: CompletionContinuation,
        effect: Effect,
    },

    /// Publish one successful record and then a payload-free terminal error.
    ///
    /// This is the core settlement for a multishot replacement failure. The first record is
    /// admitted before the terminal error, so an accepted handle or receive buffer can never be
    /// lost merely because the next request could not be armed.
    UserThenTerminal {
        event: UserCompletionEvent,
        payload: SlotPayload<Spec>,
        detail: Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
        terminal_event: UserCompletionEvent,
        terminal_error: Report<SlotError<Spec>>,
        terminal_cleanup: CompletionCleanupGuard,
        terminal_effect: Effect,
    },

    Cleanup {
        cleanup: CompletionCleanupGuard,
        /// 同 [`CompletionSettlement::User`]：一个**已放弃**的 multishot 仍然会一条条
        /// 投递完成，每一条都要跑 cleanup，但只有最后一条才能归还 slot。
        continuation: CompletionContinuation,
        effect: Effect,
    },
    /// The kernel arm ended, but the logical operation remains owned and will be rearmed.
    ///
    /// This is intentionally separate from [`CompletionSettlement::Cleanup`].  A retained
    /// completion has no user record at all, so publishing an empty record would wake the
    /// consumer and make it observe a fake datagram.  The slot stays addressable and its
    /// operation payload remains pinned while the backend effect queues the rearm.
    Retained {
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Anomaly {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    /// A final completion failed after its CQE was consumed.  The core still owns the ordering
    /// of cleanup and slot finalization.
    TerminalFailure {
        failure: CompletionFailure<Spec, Effect>,
    },
    /// A `MORE` completion failed.  The current CQE is settled, but the slot remains quarantined
    /// and addressable until a safe final completion or shutdown cleanup is observed.
    Quarantined {
        failure: CompletionFailure<Spec, Effect>,
    },
    ControlHandled {
        effect: Effect,
    },
    Ignore {
        effect: Effect,
    },
}

pub enum CompletionBackendIngressAction<Spec, Effect>
where
    Spec: SlotSpec,
{
    RouteUser(UserCompletionEvent),
    Finish(CompletionSettlement<Spec, Effect>),
}

pub trait CompletionBackendHooks<Spec>
where
    Spec: SlotSpec,
{
    type BackendIngress;
    type BackendEffect: Default;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionSettlement<Spec, Self::BackendEffect>;

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<Spec, Self::BackendEffect>;

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<Spec, Self::BackendEffect>;

    fn complete_corrupt(
        &mut self,
        _event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<Spec, Self::BackendEffect> {
        CompletionSettlement::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(_event.raw()),
            cleanup: CompletionCleanupGuard::default(),
            effect: Self::BackendEffect::default(),
        }
    }

    fn complete_backend_ingress(
        &mut self,
        _ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<Spec, Self::BackendEffect> {
        CompletionBackendIngressAction::Finish(CompletionSettlement::Ignore {
            effect: Self::BackendEffect::default(),
        })
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> HookResult<Spec, ()>;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompletionFlowOutcome {
    pub user_completed: usize,
    pub orphan_cleaned: usize,
    pub internal: usize,
    pub anomaly: usize,
    pub ignored: usize,
}

impl CompletionFlowOutcome {
    pub const fn semantic_count(&self) -> usize {
        self.user_completed + self.orphan_cleaned + self.internal + self.anomaly
    }

    pub fn merge(&mut self, other: Self) {
        self.user_completed += other.user_completed;
        self.orphan_cleaned += other.orphan_cleaned;
        self.internal += other.internal;
        self.anomaly += other.anomaly;
        self.ignored += other.ignored;
    }

    const fn user_completed() -> Self {
        Self {
            user_completed: 1,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn orphan_cleaned() -> Self {
        Self {
            user_completed: 0,
            orphan_cleaned: 1,
            internal: 0,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn internal() -> Self {
        Self {
            user_completed: 0,
            orphan_cleaned: 0,
            internal: 1,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn anomaly() -> Self {
        Self {
            user_completed: 0,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 1,
            ignored: 0,
        }
    }

    const fn ignored() -> Self {
        Self {
            user_completed: 0,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 0,
            ignored: 1,
        }
    }
}

#[derive(Clone, Copy)]
enum FinalizeAction {
    Waiting(UserCompletionEvent),
    Orphaned(UserCompletionEvent),
}

pub trait CompletionFlowExt<Spec>
where
    Spec: SlotSpec,
{
    fn accept_completion<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        ingress: CompletionIngress<Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>;
}

impl<Spec> CompletionFlowExt<Spec> for OpRegistry<Spec>
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send + DriverError,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    fn accept_completion<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        ingress: CompletionIngress<Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>,
    {
        match ingress {
            CompletionIngress::Kernel(envelope) => match dispatch_envelope(envelope) {
                CompletionDispatch::User { event } => self.accept_user_event(
                    table,
                    diagnostics,
                    hooks,
                    event,
                    CompletionSource::Kernel,
                ),
                CompletionDispatch::Waker { id, raw } => {
                    let outcome = hooks.handle_control(CompletionControl::Waker { id, raw });
                    finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
                }
                CompletionDispatch::Cancel { ticket, raw } => {
                    let outcome = hooks.handle_control(CompletionControl::Cancel { ticket, raw });
                    finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
                }
                CompletionDispatch::Unknown { envelope } => {
                    use crate::DriverCoreError;
                    Err(SlotError::<Spec>::from_core_report(
                        DriverCoreError::Internal
                            .to_report()
                            .push_ctx("scope", "driver-core/completion")
                            .attach_note(format!(
                                "unknown control or unclassified completion: {:?}",
                                envelope.identity
                            )),
                    ))
                }
            },
            CompletionIngress::User(event) => {
                self.accept_user_event(table, diagnostics, hooks, event, CompletionSource::User)
            }
            CompletionIngress::Synthetic { event, source } => self.accept_user_event(
                table,
                diagnostics,
                hooks,
                event,
                CompletionSource::Synthetic(source),
            ),
            CompletionIngress::Backend(backend) => match hooks.complete_backend_ingress(&backend) {
                CompletionBackendIngressAction::RouteUser(event) => self.accept_user_event(
                    table,
                    diagnostics,
                    hooks,
                    event,
                    CompletionSource::Backend(&backend),
                ),
                CompletionBackendIngressAction::Finish(outcome) => {
                    finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
                }
            },
            CompletionIngress::Anomaly { kind, attach } => {
                diagnostics.record_anomaly_kind(kind, attach);
                Ok(CompletionFlowOutcome::anomaly())
            }
        }
    }
}

trait CompletionFlowOpRegistryExt<Spec>
where
    Spec: SlotSpec,
{
    fn accept_user_event<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        event: UserCompletionEvent,
        source: CompletionSource<'_, Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>;
}

impl<Spec> CompletionFlowOpRegistryExt<Spec> for OpRegistry<Spec>
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send + DriverError,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    fn accept_user_event<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        event: UserCompletionEvent,
        source: CompletionSource<'_, Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>,
    {
        let token = event.token();
        match route_user_completion(event, self.checked_slot_view(token)?)? {
            RoutedSlotCompletion::Waiting(slot) => {
                let outcome = hooks.complete_waiting(event, slot, source);
                finish_hook_outcome(
                    self,
                    table,
                    diagnostics,
                    hooks,
                    outcome,
                    Some(FinalizeAction::Waiting(event)),
                )
            }
            RoutedSlotCompletion::Orphaned(slot) => {
                let outcome = hooks.complete_orphaned(event, slot, source);
                finish_hook_outcome(
                    self,
                    table,
                    diagnostics,
                    hooks,
                    outcome,
                    Some(FinalizeAction::Orphaned(event)),
                )
            }
            RoutedSlotCompletion::Missing(kind)
            | RoutedSlotCompletion::Empty(kind)
            | RoutedSlotCompletion::Stale(kind) => {
                let outcome = hooks.complete_corrupt(event, kind, source);
                finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
            }
        }
    }
}

fn finish_hook_outcome<Spec, Hooks>(
    registry: &mut OpRegistry<Spec>,
    table: &SharedCompletionTable<Spec>,
    diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
    hooks: &mut Hooks,
    outcome: CompletionSettlement<Spec, Hooks::BackendEffect>,
    finalize: Option<FinalizeAction>,
) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send + DriverError,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    Hooks: CompletionBackendHooks<Spec>,
{
    match outcome {
        CompletionSettlement::User {
            event,
            payload,
            detail,
            cleanup,
            continuation,
            effect,
        } => {
            let mut error = hooks.finish_backend_effect(effect).err();
            let record = record_user_completion::<Spec>(
                table,
                diagnostics,
                CompletionPacket::<Spec>::user_with_cleanup(event, payload, detail, cleanup)
                    .with_continuation(continuation),
            );
            // `More` 的完成不归还 slot：op 与 payload 还要留给内核后续的完成，cell 也
            // 必须停在 `InFlightWaiting` 才能继续路由。
            if continuation.is_final() {
                error = merge_settlement_error::<Spec>(
                    error,
                    finish_waiting_if_needed(registry, finalize, event).err(),
                );
            }
            if let Some(error) = error {
                return Err(error);
            }
            Ok(completion_progress_from_record(record))
        }
        CompletionSettlement::UserThenTerminal {
            event,
            payload,
            detail,
            cleanup,
            effect,
            terminal_event,
            terminal_error,
            terminal_cleanup,
            terminal_effect,
        } => {
            let mut error = hooks.finish_backend_effect(effect).err();
            let user_record = record_user_completion::<Spec>(
                table,
                diagnostics,
                CompletionPacket::<Spec>::user_with_cleanup(event, payload, detail, cleanup)
                    .with_continuation(CompletionContinuation::More),
            );

            error = merge_settlement_error::<Spec>(
                error,
                hooks.finish_backend_effect(terminal_effect).err(),
            );
            let terminal_record = record_terminal_completion::<Spec>(
                table,
                diagnostics,
                CompletionPacket::<Spec>::terminal_with_cleanup(
                    terminal_event,
                    Err(terminal_error),
                    terminal_cleanup,
                ),
            );
            if terminal_event.token() == event.token() {
                error = merge_settlement_error::<Spec>(
                    error,
                    finish_waiting_if_needed(registry, finalize, terminal_event).err(),
                );
            } else {
                error = merge_settlement_error::<Spec>(
                    error,
                    Some(invalid_failure_disposition::<Spec>(
                        "terminal completion token differs from successful record",
                    )),
                );
            }
            if let Some(error) = error {
                return Err(error);
            }
            let mut progress = completion_progress_from_record(user_record);
            progress.merge(completion_progress_from_record(terminal_record));
            Ok(progress)
        }
        CompletionSettlement::Cleanup {
            mut cleanup,
            continuation,
            effect,
        } => {
            let mut error = hooks.finish_backend_effect(effect).err();
            error = merge_settlement_error::<Spec>(
                error,
                run_settlement_cleanup::<Spec>(diagnostics, &mut cleanup),
            );
            // `More`：操作还在内核里，slot 必须留着——否则后续的完成落到一个已归还
            // （甚至已被重新分配）的 slot 上，`orphan_cleanup` 再也跑不到。
            if continuation.is_final() {
                match finalize {
                    Some(FinalizeAction::Waiting(event)) => {
                        error = merge_settlement_error::<Spec>(
                            error,
                            finish_waiting_if_needed(registry, finalize, event).err(),
                        );
                    }
                    Some(FinalizeAction::Orphaned(event)) => {
                        error = merge_settlement_error::<Spec>(
                            error,
                            finish_orphaned(registry, event).err(),
                        );
                    }
                    None => {}
                }
            }
            if let Some(error) = error {
                return Err(error);
            }
            Ok(CompletionFlowOutcome::orphan_cleaned())
        }
        CompletionSettlement::Retained {
            mut cleanup,
            effect,
        } => {
            let mut error = hooks.finish_backend_effect(effect).err();
            error = merge_settlement_error::<Spec>(
                error,
                run_settlement_cleanup::<Spec>(diagnostics, &mut cleanup),
            );
            if let Some(error) = error {
                return Err(error);
            }
            // No record was published and no finalization is attempted.  The waiting/orphaned
            // slot remains addressable for the rearmed logical operation.
            Ok(CompletionFlowOutcome::internal())
        }
        CompletionSettlement::Anomaly {
            kind,
            attach,
            mut cleanup,
            effect,
        } => {
            diagnostics.record_anomaly_kind(kind, attach);
            let mut error = hooks.finish_backend_effect(effect).err();
            error = merge_settlement_error::<Spec>(
                error,
                run_settlement_cleanup::<Spec>(diagnostics, &mut cleanup),
            );
            match error {
                Some(error) => Err(error),
                None => Ok(CompletionFlowOutcome::anomaly()),
            }
        }
        CompletionSettlement::TerminalFailure { mut failure } => {
            let mut error = hooks.finish_backend_effect(failure.effect).err();
            error = merge_settlement_error::<Spec>(error, Some(failure.error));
            error = merge_settlement_error::<Spec>(
                error,
                run_settlement_cleanup::<Spec>(diagnostics, &mut failure.cleanup),
            );
            error = merge_settlement_error::<Spec>(
                error,
                settle_slot_after_failure(
                    registry,
                    table,
                    finalize,
                    failure.disposition,
                    failure.continuation,
                ),
            );
            Err(error.expect("terminal failure must retain its primary error"))
        }
        CompletionSettlement::Quarantined { mut failure } => {
            let mut error = hooks.finish_backend_effect(failure.effect).err();
            error = merge_settlement_error::<Spec>(error, Some(failure.error));
            error = merge_settlement_error::<Spec>(
                error,
                run_settlement_cleanup::<Spec>(diagnostics, &mut failure.cleanup),
            );
            error = merge_settlement_error::<Spec>(
                error,
                settle_slot_after_failure(
                    registry,
                    table,
                    finalize,
                    failure.disposition,
                    failure.continuation,
                ),
            );
            Err(error.expect("quarantined failure must retain its primary error"))
        }
        CompletionSettlement::ControlHandled { effect } => {
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::internal())
        }
        CompletionSettlement::Ignore { effect } => {
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::ignored())
        }
    }
}

fn merge_settlement_error<Spec: SlotSpec>(
    primary: Option<Report<SlotError<Spec>>>,
    secondary: Option<Report<SlotError<Spec>>>,
) -> Option<Report<SlotError<Spec>>> {
    match (primary, secondary) {
        (Some(primary), Some(secondary)) => Some(primary.with_diag_src_err(secondary)),
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    }
}

fn run_settlement_cleanup<Spec>(
    diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
    cleanup: &mut CompletionCleanupGuard,
) -> Option<Report<SlotError<Spec>>>
where
    Spec: SlotSpec,
    SlotError<Spec>: DriverError,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    match cleanup.run() {
        Ok(_) => None,
        Err(error) => {
            diagnostics.inc_orphan_cleanup_error();
            Some(SlotError::<Spec>::from_core_report(error))
        }
    }
}

fn settle_slot_after_failure<Spec>(
    registry: &mut OpRegistry<Spec>,
    table: &SharedCompletionTable<Spec>,
    finalize: Option<FinalizeAction>,
    disposition: CompletionSlotDisposition,
    continuation: CompletionContinuation,
) -> Option<Report<SlotError<Spec>>>
where
    Spec: SlotSpec,
    SlotError<Spec>: DriverError,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    let disposition_is_valid = match continuation {
        CompletionContinuation::Final => {
            matches!(
                disposition,
                CompletionSlotDisposition::Finalized | CompletionSlotDisposition::NotApplicable
            )
        }
        CompletionContinuation::More => {
            matches!(
                disposition,
                CompletionSlotDisposition::Quarantined | CompletionSlotDisposition::NotApplicable
            )
        }
    };
    let mut error = if disposition_is_valid {
        None
    } else {
        Some(invalid_failure_disposition::<Spec>(
            "completion failure disposition does not match continuation",
        ))
    };

    // The continuation is the safety boundary.  A malformed settlement must never free a slot
    // while the kernel may still reference it, and a final CQE must never leave an active slot
    // behind merely because a backend supplied an inconsistent disposition.
    let lifecycle_error = match continuation {
        CompletionContinuation::Final => match finalize {
            Some(FinalizeAction::Waiting(event)) => {
                finish_waiting_if_needed(registry, Some(FinalizeAction::Waiting(event)), event)
                    .err()
            }
            Some(FinalizeAction::Orphaned(event)) => finish_orphaned(registry, event).err(),
            None => None,
        },
        CompletionContinuation::More => match finalize {
            Some(FinalizeAction::Waiting(event)) => {
                let outcome = table.mark_orphaned(event.token());
                if outcome.is_applied() {
                    None
                } else {
                    Some(invalid_failure_disposition::<Spec>(
                        "unable to quarantine waiting completion slot",
                    ))
                }
            }
            Some(FinalizeAction::Orphaned(_)) | None => None,
        },
    };
    error = merge_settlement_error::<Spec>(error, lifecycle_error);
    error
}

fn invalid_failure_disposition<Spec: SlotSpec>(note: &'static str) -> Report<SlotError<Spec>> {
    SlotError::<Spec>::from_core_report(
        DriverCoreError::Internal
            .to_report()
            .push_ctx("scope", "driver-core/completion.settlement")
            .attach_note(note),
    )
}

fn record_user_completion<Spec>(
    table: &SharedCompletionTable<Spec>,
    diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
    packet: CompletionPacket<Spec>,
) -> RecordCompletionOutcome
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    match table.record_completion(CompletionWritePermit::new(), packet) {
        RecordCompletionResult::Recorded(outcome) => outcome,
        RecordCompletionResult::Rejected { outcome, packet } => {
            run_rejected_cleanup(diagnostics, *packet);
            outcome
        }
    }
}

fn record_terminal_completion<Spec>(
    table: &SharedCompletionTable<Spec>,
    diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
    packet: CompletionPacket<Spec>,
) -> RecordCompletionOutcome
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    record_user_completion(table, diagnostics, packet)
}

fn finish_waiting_if_needed<Spec>(
    registry: &mut OpRegistry<Spec>,
    finalize: Option<FinalizeAction>,
    fallback_event: UserCompletionEvent,
) -> DriverResult<(), SlotError<Spec>>
where
    Spec: SlotSpec,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    SlotError<Spec>: DriverError,
{
    match finalize {
        Some(FinalizeAction::Waiting(event)) => {
            let _ = finalize_waiting_checked(registry, event.token())?;
        }
        Some(FinalizeAction::Orphaned(event)) => {
            let _ = finalize_orphaned_checked(registry, event.token())?;
        }
        None => {
            let _ = finalize_waiting_checked(registry, fallback_event.token())?;
        }
    }
    Ok(())
}

fn finish_orphaned<Spec>(
    registry: &mut OpRegistry<Spec>,
    event: UserCompletionEvent,
) -> DriverResult<(), SlotError<Spec>>
where
    Spec: SlotSpec,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    SlotError<Spec>: DriverError,
{
    let _ = finalize_orphaned_checked(registry, event.token())?;
    Ok(())
}

#[inline]
fn completion_progress_from_record(outcome: RecordCompletionOutcome) -> CompletionFlowOutcome {
    match outcome {
        RecordCompletionOutcome::RecordedUser => CompletionFlowOutcome::user_completed(),
        RecordCompletionOutcome::OrphanedDropped => CompletionFlowOutcome::orphan_cleaned(),
        RecordCompletionOutcome::Rejected(_) => CompletionFlowOutcome::anomaly(),
    }
}
