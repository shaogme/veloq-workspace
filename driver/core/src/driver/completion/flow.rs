use crate::DriverResult;
use crate::driver::registry::OpRegistry;
use crate::slot::{self, SlotRegistryExt};

use super::{
    CompletionAnomaly, CompletionAnomalyReason, CompletionBackend, CompletionCleanupGuard,
    CompletionDispatch, CompletionEnvelope, CompletionPacket, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsBackend, RawCompletion, RecordCompletionOutcome,
    RecordCompletionResult, RoutedSlotCompletion, SharedCompletionTable, UserCompletionEvent,
    dispatch_envelope, finalize_corrupt_checked, finalize_orphaned_checked,
    finalize_waiting_checked, route_user_completion, run_completion_cleanup, run_rejected_cleanup,
    unknown_completion_anomaly,
};

#[derive(Debug, Clone, Copy)]
pub struct CompletionWritePermit {
    _private: (),
}

impl CompletionWritePermit {
    #[inline]
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
    Anomaly(CompletionAnomaly),
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
    Spec: slot::SlotSpec,
{
    User {
        event: UserCompletionEvent,
        payload: slot::SlotPayload<Spec>,
        detail: Option<DriverResult<slot::SlotCompletion<Spec>, slot::SlotError<Spec>>>,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Lost {
        event: UserCompletionEvent,
        loss_reason: CompletionAnomaly,
        snapshot: slot::SlotSnapshot,
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Cleanup {
        cleanup: CompletionCleanupGuard,
        effect: Effect,
    },
    Anomaly {
        anomaly: CompletionAnomaly,
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
    Spec: slot::SlotSpec,
{
    RouteUser(UserCompletionEvent),
    Finish(CompletionHookOutcome<Spec, Effect>),
}

pub trait CompletionBackendHooks<Spec>
where
    Spec: slot::SlotSpec,
{
    type BackendIngress;
    type BackendEffect: Default;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionHookOutcome<Spec, Self::BackendEffect>;

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: slot::Slot<'_, slot::InFlightWaiting, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionHookOutcome<Spec, Self::BackendEffect>;

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: slot::Slot<'_, slot::InFlightOrphaned, Spec>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionHookOutcome<Spec, Self::BackendEffect>;

    fn complete_corrupt(
        &mut self,
        _event: UserCompletionEvent,
        anomaly: CompletionAnomaly,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionHookOutcome<Spec, Self::BackendEffect> {
        CompletionHookOutcome::Anomaly {
            anomaly,
            effect: Self::BackendEffect::default(),
        }
    }

    fn complete_backend_ingress(
        &mut self,
        _ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<Spec, Self::BackendEffect> {
        CompletionBackendIngressAction::Finish(CompletionHookOutcome::Ignore {
            effect: Self::BackendEffect::default(),
        })
    }

    fn finish_backend_effect(
        &mut self,
        effect: Self::BackendEffect,
    ) -> DriverResult<(), slot::SlotError<Spec>>;
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
    #[inline]
    pub const fn semantic_count(&self) -> usize {
        self.user_completed + self.user_lost + self.orphan_cleaned + self.internal + self.anomaly
    }

    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.user_completed += other.user_completed;
        self.user_lost += other.user_lost;
        self.orphan_cleaned += other.orphan_cleaned;
        self.internal += other.internal;
        self.anomaly += other.anomaly;
        self.ignored += other.ignored;
    }

    #[inline]
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

    #[inline]
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

    #[inline]
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

    #[inline]
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

    #[inline]
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

    #[inline]
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
    CorruptFromEvent,
}

pub trait CompletionFlowExt<Spec>
where
    Spec: slot::SlotSpec,
{
    fn accept_completion<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        ingress: CompletionIngress<Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, slot::SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>;
}

impl<Spec> CompletionFlowExt<Spec> for OpRegistry<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    fn accept_completion<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        ingress: CompletionIngress<Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, slot::SlotError<Spec>>
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
                CompletionDispatch::Cancel { id, raw } => {
                    let outcome = hooks.handle_control(CompletionControl::Cancel { id, raw });
                    finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
                }
                CompletionDispatch::Unknown { envelope } => {
                    let anomaly = unknown_completion_anomaly(envelope);
                    diagnostics.record_anomaly(&anomaly);
                    Ok(CompletionFlowOutcome::anomaly())
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
            CompletionIngress::Anomaly(anomaly) => Ok(finish_anomaly(self, diagnostics, anomaly)),
        }
    }
}

trait CompletionFlowOpRegistryExt<Spec>
where
    Spec: slot::SlotSpec,
{
    fn accept_user_event<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        event: UserCompletionEvent,
        source: CompletionSource<'_, Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, slot::SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>;
}

impl<Spec> CompletionFlowOpRegistryExt<Spec> for OpRegistry<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    fn accept_user_event<Hooks>(
        &mut self,
        table: &SharedCompletionTable<Spec>,
        diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
        hooks: &mut Hooks,
        event: UserCompletionEvent,
        source: CompletionSource<'_, Hooks::BackendIngress>,
    ) -> DriverResult<CompletionFlowOutcome, slot::SlotError<Spec>>
    where
        Hooks: CompletionBackendHooks<Spec>,
    {
        let token = event.token();
        match route_user_completion(event, self.checked_slot_view(token)) {
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
            RoutedSlotCompletion::Corrupt(anomaly) => {
                let outcome = hooks.complete_corrupt(event, anomaly, source);
                finish_hook_outcome(
                    self,
                    table,
                    diagnostics,
                    hooks,
                    outcome,
                    Some(FinalizeAction::CorruptFromEvent),
                )
            }
            RoutedSlotCompletion::Missing(anomaly)
            | RoutedSlotCompletion::Empty(anomaly)
            | RoutedSlotCompletion::Stale(anomaly) => {
                let outcome = hooks.complete_corrupt(event, anomaly, source);
                finish_hook_outcome(self, table, diagnostics, hooks, outcome, None)
            }
        }
    }
}

fn finish_hook_outcome<Spec, Hooks>(
    registry: &mut OpRegistry<Spec>,
    table: &SharedCompletionTable<Spec>,
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    hooks: &mut Hooks,
    outcome: CompletionHookOutcome<Spec, Hooks::BackendEffect>,
    finalize: Option<FinalizeAction>,
) -> DriverResult<CompletionFlowOutcome, slot::SlotError<Spec>>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
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
            finish_waiting_if_needed(registry, diagnostics, finalize, event);
            hooks.finish_backend_effect(effect)?;
            Ok(completion_progress_from_record(record))
        }
        CompletionHookOutcome::Lost {
            event,
            loss_reason,
            snapshot,
            cleanup,
            effect,
        } => {
            let record =
                record_lost_completion::<Spec>(table, diagnostics, event, loss_reason, cleanup);
            finish_corrupt(registry, diagnostics, snapshot, event.raw());
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
                    finish_waiting_if_needed(registry, diagnostics, finalize, event);
                }
                Some(FinalizeAction::Orphaned(event)) => {
                    finish_orphaned(registry, diagnostics, event);
                }
                Some(FinalizeAction::CorruptFromEvent) | None => {}
            }
            hooks.finish_backend_effect(effect)?;
            Ok(CompletionFlowOutcome::orphan_cleaned())
        }
        CompletionHookOutcome::Anomaly { anomaly, effect } => {
            let progress = finish_anomaly(registry, diagnostics, anomaly);
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
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    packet: CompletionPacket<Spec>,
) -> RecordCompletionOutcome
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
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
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    event: UserCompletionEvent,
    anomaly: CompletionAnomaly,
    cleanup: CompletionCleanupGuard,
) -> RecordCompletionOutcome
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    record_user_completion::<Spec>(
        table,
        diagnostics,
        CompletionPacket::<Spec>::lost(event, anomaly, cleanup),
    )
}

fn finish_waiting_if_needed<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    finalize: Option<FinalizeAction>,
    fallback_event: UserCompletionEvent,
) where
    Spec: slot::SlotSpec,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    let event = match finalize {
        Some(FinalizeAction::Waiting(event)) => event,
        Some(FinalizeAction::CorruptFromEvent) | Some(FinalizeAction::Orphaned(_)) | None => {
            fallback_event
        }
    };
    let raw = event.raw();
    let _ = finalize_waiting_checked(
        registry,
        diagnostics,
        raw.backend,
        event.token(),
        raw.res,
        raw.flags,
    );
}

fn finish_orphaned<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    event: UserCompletionEvent,
) where
    Spec: slot::SlotSpec,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    let raw = event.raw();
    let _ = finalize_orphaned_checked(
        registry,
        diagnostics,
        raw.backend,
        event.token(),
        raw.res,
        raw.flags,
    );
}

fn finish_corrupt<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    snapshot: slot::SlotSnapshot,
    raw: RawCompletion,
) where
    Spec: slot::SlotSpec,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    let _ = finalize_corrupt_checked(
        registry,
        diagnostics,
        raw.backend,
        snapshot,
        raw.res,
        raw.flags,
    );
}

fn finish_anomaly<Spec>(
    registry: &mut OpRegistry<Spec>,
    diagnostics: &DriverCompletionDiagnostics<slot::SlotCompletionDiagnostics<Spec>>,
    anomaly: CompletionAnomaly,
) -> CompletionFlowOutcome
where
    Spec: slot::SlotSpec,
    slot::SlotCompletionDiagnostics<Spec>: DriverCompletionDiagnosticsBackend,
{
    diagnostics.record_anomaly(&anomaly);
    if should_finalize_corrupt_anomaly(&anomaly)
        && let Some(snapshot) = anomaly.slot_snapshot
    {
        let raw = RawCompletion::new(
            anomaly.backend.unwrap_or(CompletionBackend::Core),
            anomaly.token,
            anomaly.raw_result.unwrap_or(0),
            anomaly.flags.unwrap_or(0),
        );
        finish_corrupt(registry, diagnostics, snapshot, raw);
    }
    CompletionFlowOutcome::anomaly()
}

#[inline]
fn should_finalize_corrupt_anomaly(anomaly: &CompletionAnomaly) -> bool {
    matches!(
        anomaly.reason,
        CompletionAnomalyReason::OpMissing
            | CompletionAnomalyReason::PayloadMissing
            | CompletionAnomalyReason::SlotCorruption
            | CompletionAnomalyReason::BackendInvariantBroken
    )
}

#[inline]
fn completion_progress_from_record(outcome: RecordCompletionOutcome) -> CompletionFlowOutcome {
    match outcome {
        RecordCompletionOutcome::RecordedUser => CompletionFlowOutcome::user_completed(),
        RecordCompletionOutcome::RecordedLost => CompletionFlowOutcome::user_lost(),
        RecordCompletionOutcome::OrphanedDropped => CompletionFlowOutcome::orphan_cleaned(),
        RecordCompletionOutcome::Missing(_)
        | RecordCompletionOutcome::Stale(_)
        | RecordCompletionOutcome::NonActive(_)
        | RecordCompletionOutcome::Corrupt(_) => CompletionFlowOutcome::anomaly(),
    }
}
