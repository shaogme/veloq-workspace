use crate::{
    DriverError, DriverResult,
    driver::registry::OpRegistry,
    slot::{
        InFlightOrphaned, InFlightWaiting, Slot, SlotCompletion, SlotCompletionDiagnostics,
        SlotError, SlotPayload, SlotRegistryExt, SlotSpec,
    },
};
use diagweave::DiagnosticError;

use super::{
    AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, CompletionDispatch,
    CompletionEnvelope, CompletionPacket, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsBackend, RawCompletion, RecordCompletionOutcome,
    RecordCompletionResult, RoutedSlotCompletion, SharedCompletionTable, UserCompletionEvent,
    dispatch_envelope, finalize_orphaned_checked, finalize_waiting_checked, route_user_completion,
    run_completion_cleanup, run_rejected_cleanup,
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
        id: u16,
        raw: RawCompletion,
    },
    Cancel {
        id: super::CancelCompletionId,
        raw: RawCompletion,
    },
}

pub enum CompletionHookOutcome<Spec, Effect>
where
    Spec: SlotSpec,
{
    User {
        event: UserCompletionEvent,
        payload: SlotPayload<Spec>,
        detail: Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Lost {
        event: UserCompletionEvent,
        loss_kind: CompletionAnomalyKind,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Cleanup {
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Anomaly {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
        effect: Effect,
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
    Finish(CompletionHookOutcome<Spec, Effect>),
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
    ) -> HookResult<Spec, CompletionHookOutcome<Spec, Self::BackendEffect>>;

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<Spec, CompletionHookOutcome<Spec, Self::BackendEffect>>;

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<Spec, CompletionHookOutcome<Spec, Self::BackendEffect>>;

    fn complete_corrupt(
        &mut self,
        _event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<Spec, CompletionHookOutcome<Spec, Self::BackendEffect>> {
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(_event.raw()),
            effect: Self::BackendEffect::default(),
        })
    }

    fn complete_backend_ingress(
        &mut self,
        _ingress: &Self::BackendIngress,
    ) -> HookResult<Spec, CompletionBackendIngressAction<Spec, Self::BackendEffect>> {
        Ok(CompletionBackendIngressAction::Finish(
            CompletionHookOutcome::Ignore {
                effect: Self::BackendEffect::default(),
            },
        ))
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> HookResult<Spec, ()>;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompletionFlowOutcome {
    pub user_completed: usize,
    pub user_lost: usize,
    pub orphan_cleaned: usize,
    pub internal: usize,
    pub anomaly: usize,
    pub ignored: usize,
}

impl CompletionFlowOutcome {
    pub const fn semantic_count(&self) -> usize {
        self.user_completed + self.user_lost + self.orphan_cleaned + self.internal + self.anomaly
    }

    pub fn merge(&mut self, other: Self) {
        self.user_completed += other.user_completed;
        self.user_lost += other.user_lost;
        self.orphan_cleaned += other.orphan_cleaned;
        self.internal += other.internal;
        self.anomaly += other.anomaly;
        self.ignored += other.ignored;
    }

    const fn user_completed() -> Self {
        Self {
            user_completed: 1,
            user_lost: 0,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn user_lost() -> Self {
        Self {
            user_completed: 0,
            user_lost: 1,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn orphan_cleaned() -> Self {
        Self {
            user_completed: 0,
            user_lost: 0,
            orphan_cleaned: 1,
            internal: 0,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn internal() -> Self {
        Self {
            user_completed: 0,
            user_lost: 0,
            orphan_cleaned: 0,
            internal: 1,
            anomaly: 0,
            ignored: 0,
        }
    }

    const fn anomaly() -> Self {
        Self {
            user_completed: 0,
            user_lost: 0,
            orphan_cleaned: 0,
            internal: 0,
            anomaly: 1,
            ignored: 0,
        }
    }

    const fn ignored() -> Self {
        Self {
            user_completed: 0,
            user_lost: 0,
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
                    let outcome = hooks.handle_control(CompletionControl::Waker { id, raw })?;
                    finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
                }
                CompletionDispatch::Cancel { id, raw } => {
                    let outcome = hooks.handle_control(CompletionControl::Cancel { id, raw })?;
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
            CompletionIngress::Backend(backend) => {
                match hooks.complete_backend_ingress(&backend)? {
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
                }
            }
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
    SlotError<Spec>: Send,
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
                let outcome = hooks.complete_waiting(event, slot, source)?;
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
                let outcome = hooks.complete_orphaned(event, slot, source)?;
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
                let outcome = hooks.complete_corrupt(event, kind, source)?;
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
    outcome: CompletionHookOutcome<Spec, Hooks::BackendEffect>,
    finalize: Option<FinalizeAction>,
) -> DriverResult<CompletionFlowOutcome, SlotError<Spec>>
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
    Hooks: CompletionBackendHooks<Spec>,
{
    match outcome {
        CompletionHookOutcome::User {
            event,
            payload,
            detail,
            cleanup,
            effect,
        } => {
            let record = record_user_completion::<Spec>(
                table,
                diagnostics,
                CompletionPacket::<Spec>::user_with_cleanup(event, payload, detail, cleanup),
            );
            finish_waiting_if_needed(registry, finalize, event)?;
            hooks.finish_backend_effect(effect)?;
            Ok(completion_progress_from_record(record))
        }
        CompletionHookOutcome::Lost {
            event,
            loss_kind,
            cleanup,
            effect,
        } => {
            let record =
                record_lost_completion::<Spec>(table, diagnostics, event, loss_kind, cleanup);
            let _ = registry.finalize_waiting_completion(event.token());
            hooks.finish_backend_effect(effect)?;
            Ok(completion_progress_from_record(record))
        }
        CompletionHookOutcome::Cleanup {
            mut cleanup,
            effect,
        } => {
            let _ = run_completion_cleanup(diagnostics, &mut cleanup);
            match finalize {
                Some(FinalizeAction::Waiting(event)) => {
                    finish_waiting_if_needed(registry, finalize, event)?;
                }
                Some(FinalizeAction::Orphaned(event)) => {
                    finish_orphaned(registry, event)?;
                }
                None => {}
            }
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::orphan_cleaned())
        }
        CompletionHookOutcome::Anomaly {
            kind,
            attach,
            effect,
        } => {
            let progress = {
                diagnostics.record_anomaly_kind(kind, attach);
                CompletionFlowOutcome::anomaly()
            };
            hooks.finish_backend_effect(effect)?;
            Ok(progress)
        }
        CompletionHookOutcome::ControlHandled { effect } => {
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::internal())
        }
        CompletionHookOutcome::Ignore { effect } => {
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::ignored())
        }
    }
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

fn record_lost_completion<Spec>(
    table: &SharedCompletionTable<Spec>,
    diagnostics: &DriverCompletionDiagnostics<SlotCompletionDiagnostics<Spec>>,
    event: UserCompletionEvent,
    kind: CompletionAnomalyKind,
    cleanup: CompletionCleanupGuard,
) -> RecordCompletionOutcome
where
    Spec: SlotSpec,
    SlotPayload<Spec>: Send,
    SlotError<Spec>: Send,
    SlotCompletion<Spec>: Send,
    SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    record_user_completion::<Spec>(
        table,
        diagnostics,
        CompletionPacket::<Spec>::lost(event, kind, cleanup),
    )
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
    let event = match finalize {
        Some(FinalizeAction::Waiting(event)) => event,
        Some(FinalizeAction::Orphaned(_)) | None => fallback_event,
    };
    let _ = finalize_waiting_checked(registry, event.token())?;
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
        RecordCompletionOutcome::RecordedLost => CompletionFlowOutcome::user_lost(),
        RecordCompletionOutcome::OrphanedDropped => CompletionFlowOutcome::orphan_cleaned(),
        RecordCompletionOutcome::Rejected(_) => CompletionFlowOutcome::anomaly(),
    }
}
