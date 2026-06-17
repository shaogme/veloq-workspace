use super::*;
use std::task::Waker;

use crate::{
    DriverCoreError,
    driver::{
        AnomalyAttach, AnomalyOutcome, CompletionAnomaly, CompletionAnomalyKind,
        CompletionAnomalyReason, CompletionBackend, CompletionBackendHooks, CompletionCleanup,
        CompletionControl, CompletionFlowExt, CompletionFlowOutcome, CompletionHookOutcome,
        CompletionIngress, CompletionSource, HookResult, OpToken, PlatformOp, SlotIssueReason,
        registry::OpRegistry,
    },
    slot::{
        self, CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotState,
        SlotView,
    },
};
use diagweave::prelude::*;
use veloq_shim::atomic::Ordering;

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {
    type CleanupContext<'a> = ();
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct DummyError;

impl std::fmt::Display for DummyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dummy error")
    }
}

impl std::error::Error for DummyError {}

impl crate::DriverError for DummyError {
    #[inline]
    fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
        report.map_err(|_| DummyError)
    }
}

struct DummySlotSpec;

impl slot::SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = DummyError;
    type Completion = usize;
    type CompletionDiagnostics = ();
}

fn test_token(index: usize, generation: u32) -> OpToken {
    OpToken::from_registry_parts(index, generation).expect("test token should be encodable")
}

fn test_event(token: OpToken, res: i32) -> UserCompletionEvent {
    UserCompletionEvent::from_parts(CompletionBackend::Core, token, res, 0)
}

#[derive(Default)]
struct TestHooks {
    loss_kind: Option<CompletionAnomalyKind>,
    cleanup: Option<CompletionCleanupGuard>,
}

impl CompletionBackendHooks<DummySlotSpec> for TestHooks {
    type BackendIngress = ();
    type BackendEffect = ();

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        Ok(CompletionHookOutcome::Ignore { effect: () })
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: slot::Slot<'_, InFlightWaiting, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        if let Some(loss_kind) = self.loss_kind.take() {
            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            let _ = payload;
            drop(detail);
            return Ok(CompletionHookOutcome::Lost {
                event,
                loss_kind,
                cleanup: self.cleanup.take().unwrap_or_default(),
                effect: (),
            });
        }

        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        Ok(CompletionHookOutcome::User {
            event,
            payload: payload.expect("test slot payload should exist"),
            detail,
            cleanup: self.cleanup.take().unwrap_or_default(),
            effect: (),
        })
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: slot::Slot<'_, InFlightOrphaned, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        let _ = payload;
        drop(detail);
        Ok(CompletionHookOutcome::Cleanup {
            cleanup: self.cleanup.take().unwrap_or_default(),
            effect: (),
        })
    }

    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        if let Some(loss_kind) = self.loss_kind.take() {
            return Ok(CompletionHookOutcome::Lost {
                event,
                loss_kind,
                cleanup: self.cleanup.take().unwrap_or_default(),
                effect: (),
            });
        }
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            effect: (),
        })
    }

    fn finish_backend_effect(
        &mut self,
        _effect: Self::BackendEffect,
    ) -> HookResult<DummySlotSpec, ()> {
        Ok(())
    }
}

fn active_registry() -> (OpRegistry<DummySlotSpec>, OpToken) {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let handle = registry.alloc(()).expect("slot allocation failed").handle;
    let token = test_token(handle.index, handle.generation);
    registry
        .with_slot_storage_mut(token, |_result, payload, _sidecar| {
            *payload = Some(());
        })
        .expect("slot storage should exist");
    let slot = match registry.checked_slot_view(token) {
        CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot
            .init_op_with(DummyPlatformOp, |_| {})
            .expect("reserved slot should accept op"),
        _ => panic!("reserved slot should be available"),
    };
    let _in_flight = slot
        .start_submission_with(None)
        .expect("reserved slot should start submission")
        .persist();
    (registry, token)
}

fn reserved_registry() -> (OpRegistry<DummySlotSpec>, OpToken) {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let handle = registry.alloc(()).expect("slot allocation failed").handle;
    let token = test_token(handle.index, handle.generation);
    (registry, token)
}

fn accept_with_hooks(
    registry: &mut OpRegistry<DummySlotSpec>,
    event: UserCompletionEvent,
    hooks: &mut TestHooks,
) -> CompletionFlowOutcome {
    let diagnostics = registry.shared.completion_diagnostics();
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    registry
        .accept_completion(&table, &diagnostics, hooks, CompletionIngress::User(event))
        .expect("test completion should succeed")
}

fn accept_user(registry: &mut OpRegistry<DummySlotSpec>, token: OpToken, res: i32) {
    let mut hooks = TestHooks::default();
    let _ = accept_with_hooks(registry, test_event(token, res), &mut hooks);
}

fn accept_lost(
    registry: &mut OpRegistry<DummySlotSpec>,
    token: OpToken,
    res: i32,
    kind: CompletionAnomalyKind,
    cleanup: CompletionCleanupGuard,
) -> CompletionFlowOutcome {
    let mut hooks = TestHooks {
        loss_kind: Some(kind),
        cleanup: Some(cleanup),
    };
    accept_with_hooks(registry, test_event(token, res), &mut hooks)
}

fn expected_lost_anomaly(
    kind: CompletionAnomalyKind,
    token: OpToken,
    res: i32,
) -> CompletionAnomaly {
    kind.materialize(AnomalyAttach::from_raw_completion(
        test_event(token, res).raw(),
    ))
}

fn unavailable_materialized(result: PollRecordResult<DummySlotSpec>) -> Option<CompletionAnomaly> {
    match result {
        PollRecordResult::Unavailable { kind, attach } => Some(kind.materialize(attach)),
        _ => None,
    }
}

#[test]
fn record_completion_rejects_idle_future_generation() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let table = registry.shared.clone();
    let token = test_token(0, 1);

    let mut hooks = TestHooks::default();
    let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);

    assert_eq!(outcome.anomaly, 1);
    assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
}

#[test]
fn try_take_record_reports_future_generation_unavailable() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let token = test_token(0, 1);

    match table.try_take_record(token) {
        PollRecordResult::Unavailable { kind, .. } => {
            assert_eq!(kind.reason(), CompletionAnomalyReason::NonActiveSlot);
            assert!(matches!(
                kind,
                CompletionAnomalyKind::NonActive {
                    index: 0,
                    generation: 1,
                    ..
                }
            ));
        }
        PollRecordResult::Pending => panic!("future generation token must not stay pending"),
        PollRecordResult::Ready(_) => panic!("future generation token must not become ready"),
    }
}

#[test]
fn mark_waiting_does_not_activate_idle_future_generation() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let token = test_token(0, 1);

    let outcome = table.mark_waiting(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
}

#[test]
fn mark_waiting_does_not_revive_orphaned_slot() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    table.slots[0].reset(1);
    table.slots[0].set_state(SlotState::InFlightOrphaned, Ordering::Release);
    let token = test_token(0, 1);

    let outcome = table.mark_waiting(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_ORPHANED);
}

#[test]
fn mark_orphaned_reports_stale_generation() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    table.slots[0].reset(2);
    table.slots[0].set_state(SlotState::InFlightWaiting, Ordering::Release);
    let token = test_token(0, 1);

    let outcome = table.mark_orphaned(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Stale(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_WAITING);
    assert_eq!(
        table.completion_diagnostics().snapshot().stale_completion,
        1
    );
}

#[test]
fn register_waker_reports_missing_slot() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let waker = Waker::noop();
    let token = test_token(3, 1);

    let outcome = table.register_waker(token, waker);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Missing(_))
    ));
}

#[test]
fn lost_completion_is_observable_as_unavailable() {
    let (mut registry, token) = reserved_registry();
    let table = registry.shared.clone();
    let res = -5;
    let kind = CompletionAnomalyKind::slot_issue(
        SlotIssueReason::SlotCorruption,
        0,
        1,
        SlotState::Reserved,
    );
    let expected = expected_lost_anomaly(kind, token, res);

    let outcome = accept_lost(
        &mut registry,
        token,
        res,
        kind,
        CompletionCleanupGuard::default(),
    );

    assert_eq!(outcome.user_lost, 1);
    assert_eq!(
        unavailable_materialized(table.try_take_record(token)),
        Some(expected)
    );
    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.user_lost, 1);
    assert_eq!(snapshot.user_completed, 0);
}

#[test]
fn lost_completion_reports_stale_generation() {
    let (mut registry, token) = active_registry();
    let _ = registry.remove(token);
    let fresh = registry.alloc(()).expect("fresh slot").handle;
    registry.shared.slots[0].set_state(SlotState::InFlightWaiting, Ordering::Release);
    let kind = CompletionAnomalyKind::stale(0, 1, fresh.generation, SlotState::InFlightWaiting);

    let outcome = accept_lost(
        &mut registry,
        token,
        -1,
        kind,
        CompletionCleanupGuard::default(),
    );

    assert_eq!(outcome.anomaly, 1);
}

#[test]
fn lost_completion_reports_empty_slot() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let token = test_token(0, 0);
    let kind = CompletionAnomalyKind::non_active(0, 0, SlotState::Idle);

    let outcome = accept_lost(
        &mut registry,
        token,
        -1,
        kind,
        CompletionCleanupGuard::default(),
    );

    assert_eq!(outcome.anomaly, 1);
}

#[test]
fn lost_completion_preserves_payload_missing_reason() {
    let (mut registry, token) = reserved_registry();
    let table = registry.shared.clone();
    let res = -1;
    let kind = CompletionAnomalyKind::corrupt_snapshot(slot::SlotSnapshot {
        index: 0,
        generation: 1,
        state: SlotState::Idle,
        has_op: true,
        has_payload: false,
    });
    let expected = expected_lost_anomaly(kind, token, res);

    let outcome = accept_lost(
        &mut registry,
        token,
        res,
        kind,
        CompletionCleanupGuard::default(),
    );

    assert_eq!(outcome.user_lost, 1);
    assert_eq!(
        unavailable_materialized(table.try_take_record(token)),
        Some(expected)
    );
    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.user_lost, 1);
    assert_eq!(snapshot.payload_missing, 1);
}

#[test]
fn lost_completion_preserves_op_missing_reason() {
    let (mut registry, token) = reserved_registry();
    let table = registry.shared.clone();
    let res = -1;
    let kind = CompletionAnomalyKind::corrupt_snapshot(slot::SlotSnapshot {
        index: 0,
        generation: 1,
        state: SlotState::Idle,
        has_op: false,
        has_payload: false,
    });
    let expected = expected_lost_anomaly(kind, token, res);

    let outcome = accept_lost(
        &mut registry,
        token,
        res,
        kind,
        CompletionCleanupGuard::default(),
    );

    assert_eq!(outcome.user_lost, 1);
    assert_eq!(
        unavailable_materialized(table.try_take_record(token)),
        Some(expected)
    );
    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.user_lost, 1);
    assert_eq!(snapshot.slot_corruption, 1);
}

#[test]
fn duplicate_completion_does_not_clear_ready_data() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    let mut hooks = TestHooks::default();
    let first = accept_with_hooks(&mut registry, test_event(token, 11), &mut hooks);
    let duplicate = accept_with_hooks(&mut registry, test_event(token, 22), &mut hooks);

    assert_eq!(first.user_completed, 1);
    assert_eq!(duplicate.anomaly, 1);
    let record = match table.try_take_record(token) {
        PollRecordResult::Ready(record) => record,
        PollRecordResult::Pending => panic!("first completion should be ready"),
        PollRecordResult::Unavailable { kind, .. } => {
            panic!("first completion should remain available: {kind:?}")
        }
    };
    assert_eq!(record.event.res(), 11);
}

#[test]
fn ready_mark_orphaned_cleanup_leaves_diagnostic_stale_result() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    accept_user(&mut registry, token, 0);
    assert_eq!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Applied
    );

    assert!(matches!(
        table.try_take_record(token),
        PollRecordResult::Unavailable {
            kind,
            ..
        } if kind.reason() == CompletionAnomalyReason::StaleGeneration
    ));
    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.stale_completion, 1);
}

#[test]
fn ready_mark_orphaned_runs_cleanup_and_records_error() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();
    let cleanup = CompletionCleanupGuard::new(CompletionCleanup::new(|| {
        Err(DriverCoreError::Internal
            .to_report()
            .attach_note("test cleanup failure"))
    }));

    let mut hooks = TestHooks {
        cleanup: Some(cleanup),
        ..TestHooks::default()
    };
    let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);
    assert_eq!(outcome.user_completed, 1);
    assert_eq!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Applied
    );

    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.orphan_cleanup_error, 1);
}
