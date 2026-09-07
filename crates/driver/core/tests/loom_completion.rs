#![cfg(feature = "loom")]
use diagweave::Report;
use veloq_driver_core::{
    DriverCoreError, DriverError,
    driver::{registry::OpRegistry, *},
    slot::{
        CheckedSlotView, InFlightOrphaned, InFlightWaiting, Slot, SlotRegistryExt, SlotSpec,
        SlotView,
    },
};
use veloq_std::{
    sync::{Arc, Mutex},
    thread,
};

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

impl DriverError for DummyError {
    #[inline]
    fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
        report.map_err(|_| DummyError)
    }
}

struct DummySlotSpec;

impl SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = DummyError;
    type Completion = usize;
    type CompletionDiagnostics = ();
}

struct TestHooks {
    continuation: CompletionContinuation,
}

impl TestHooks {
    fn with_continuation(continuation: CompletionContinuation) -> Self {
        Self { continuation }
    }
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
        slot: Slot<'_, InFlightWaiting, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        if self.continuation.is_more() {
            // multishot：slot 的 op 与 payload 留给内核后续的完成。
            return Ok(CompletionHookOutcome::User {
                event,
                payload: (),
                detail: None,
                cleanup: CompletionCleanupGuard::default(),
                continuation: self.continuation,
                effect: (),
            });
        }
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        Ok(CompletionHookOutcome::User {
            event,
            payload: payload.expect("loom test payload should exist"),
            detail,
            cleanup: CompletionCleanupGuard::default(),
            continuation: self.continuation,
            effect: (),
        })
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        let _ = payload;
        drop(detail);
        Ok(CompletionHookOutcome::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            continuation: CompletionContinuation::Final,
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

fn active_registry() -> (
    Arc<Mutex<OpRegistry<DummySlotSpec>>>,
    SharedCompletionTable<DummySlotSpec>,
    OpToken,
) {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let handle = registry.alloc(()).expect("slot allocation failed").handle;
    let token = OpToken::from_registry_parts(handle.index, handle.generation)
        .expect("loom test handle should be encodable");
    registry
        .with_slot_storage_mut(token, |_result, payload, _sidecar| {
            *payload = Some(());
        })
        .expect("slot storage should exist");
    let slot = match registry.checked_slot_view(token).unwrap() {
        CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot
            .init_op_with(DummyPlatformOp, |_| {})
            .expect("reserved slot should accept op"),
        _ => panic!("reserved slot should be available"),
    };
    let _in_flight = slot
        .start_submission_with(None)
        .expect("reserved slot should start submission")
        .persist();
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    (Arc::new(Mutex::new(registry)), table, token)
}

fn accept_completion(registry: &Mutex<OpRegistry<DummySlotSpec>>, token: OpToken, res: i32) {
    accept_completion_with(registry, token, res, CompletionContinuation::Final)
}

fn accept_completion_with(
    registry: &Mutex<OpRegistry<DummySlotSpec>>,
    token: OpToken,
    res: i32,
    continuation: CompletionContinuation,
) {
    let mut registry = registry.lock();
    let diagnostics = registry.shared.completion_diagnostics();
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    let mut hooks = TestHooks::with_continuation(continuation);
    registry
        .accept_completion(
            &table,
            &diagnostics,
            &mut hooks,
            CompletionIngress::User(UserCompletionEvent::from_parts(
                CompletionBackend::Core,
                token,
                res,
                0,
            )),
        )
        .unwrap();
}

#[test]
fn test_completion_table_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        let registry_cloned = registry.clone();
        let producer = thread::spawn(move || {
            accept_completion(&registry_cloned, token, 0);
        })
        .unwrap();

        let table_cloned = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned.mark_waiting(token);
            match table_cloned.try_take_record(token).unwrap() {
                PollRecordResult::Ready(record) => {
                    assert_eq!(
                        record.event.completion_token(),
                        CompletionToken::user(token)
                    )
                }
                PollRecordResult::Pending | PollRecordResult::Unavailable { .. } => {
                    table_cloned.mark_orphaned(token);
                }
            }
        })
        .unwrap();

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

#[test]
fn test_detached_drop_race_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        let registry_cloned = registry.clone();
        let producer = thread::spawn(move || {
            accept_completion(&registry_cloned, token, 42);
        })
        .unwrap();

        let table_cloned = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned.mark_waiting(token);
            table_cloned.mark_orphaned(token);
        })
        .unwrap();

        producer.join().unwrap();
        consumer.join().unwrap();

        assert_eq!(
            table.debug_get_state(0),
            CELL_STATE_IDLE,
            "status = {}",
            table.debug_status_string(0)
        );
    });
}

#[test]
fn test_fast_completion_then_waiting_take_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        accept_completion(&registry, token, 7);

        table.mark_waiting(token);
        match table.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => {
                assert_eq!(
                    record.event.completion_token(),
                    CompletionToken::user(token)
                );
                assert_eq!(record.event.res(), 7);
            }
            PollRecordResult::Pending => panic!("expected ready after fast completion"),
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("unexpected unavailable completion: {kind:?}")
            }
        }

        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_stale_after_generation_advance_loom() {
    loom::model(|| {
        let (registry, table, token_g1) = active_registry();

        accept_completion(&registry, token_g1, 1);
        table.mark_waiting(token_g1);
        let _ = table.try_take_record(token_g1).unwrap();

        match table.try_take_record(token_g1).unwrap() {
            PollRecordResult::Unavailable { kind, .. } => {
                assert_eq!(kind.reason(), CompletionAnomalyReason::StaleGeneration);
            }
            PollRecordResult::Ready(_) => panic!("old generation must not become ready"),
            PollRecordResult::Pending => panic!("old generation must be stale"),
        }
    });
}

#[test]
fn test_ready_race_with_mark_orphaned_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        accept_completion(&registry, token, 3);

        let t1 = table.clone();
        let consumer_take = thread::spawn(move || {
            let _ = t1.try_take_record(token).unwrap();
        })
        .unwrap();

        let t2 = table.clone();
        let consumer_drop = thread::spawn(move || {
            t2.mark_orphaned(token);
        })
        .unwrap();

        consumer_take.join().unwrap();
        consumer_drop.join().unwrap();

        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_two_consumers_at_most_one_ready_loom() {
    loom::model(|| {
        use loom::sync::atomic::{AtomicUsize, Ordering};

        let (registry, table, token) = active_registry();
        let ready_count = Arc::new(AtomicUsize::new(0));

        accept_completion(&registry, token, 9);

        let c1_table = table.clone();
        let c1_ready = ready_count.clone();
        let c1 = thread::spawn(move || {
            c1_table.mark_waiting(token);
            if let PollRecordResult::Ready(_) = c1_table.try_take_record(token).unwrap() {
                c1_ready.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

        let c2_table = table.clone();
        let c2_ready = ready_count.clone();
        let c2 = thread::spawn(move || {
            c2_table.mark_waiting(token);
            if let PollRecordResult::Ready(_) = c2_table.try_take_record(token).unwrap() {
                c2_ready.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

        c1.join().unwrap();
        c2.join().unwrap();

        assert!(ready_count.load(Ordering::SeqCst) <= 1);
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

/// 消费方现在也抢 `finalizing`，所以「取一条」与「发布下一条」是一对真正的并发写入者。
/// 断言是：两条记录一条都不丢，且终态完成之后 slot 必须回到可复用。
#[test]
fn test_multishot_take_races_with_publish_loom() {
    loom::model(|| {
        use loom::sync::atomic::{AtomicUsize, Ordering};

        let (registry, table, token) = active_registry();
        let taken = Arc::new(AtomicUsize::new(0));

        // 第一条 `More` 单线程发布，把 cell 带进 streaming 状态。
        accept_completion_with(&registry, token, 1, CompletionContinuation::More);

        let registry_cloned = registry.clone();
        let producer = thread::spawn(move || {
            accept_completion_with(&registry_cloned, token, 2, CompletionContinuation::Final);
        })
        .unwrap();

        let consumer_table = table.clone();
        let consumer_taken = taken.clone();
        let consumer = thread::spawn(move || {
            if let PollRecordResult::Ready(_) = consumer_table.try_take_record(token).unwrap() {
                consumer_taken.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

        producer.join().unwrap();
        consumer.join().unwrap();

        // 不论交错顺序如何，两条记录合计必须恰好能被取到两次。
        while let PollRecordResult::Ready(_) = table.try_take_record(token).unwrap() {
            taken.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(taken.load(Ordering::SeqCst), 2, "no record may be lost");
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

/// 放弃一个仍在途的 multishot 与它的下一条完成竞争：generation 不能被推进，否则内核
/// 后续的完成会被判 stale 丢掉，`orphan_cleanup` 永远跑不到。
#[test]
fn test_orphan_races_with_multishot_publish_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        accept_completion_with(&registry, token, 1, CompletionContinuation::More);

        let registry_cloned = registry.clone();
        let producer = thread::spawn(move || {
            accept_completion_with(&registry_cloned, token, 2, CompletionContinuation::Final);
        })
        .unwrap();

        let table_cloned = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned.mark_orphaned(token);
        })
        .unwrap();

        producer.join().unwrap();
        consumer.join().unwrap();

        // 收尾：无论谁先跑，剩下的记录都要能被排空，slot 最终回到可复用。
        while let PollRecordResult::Ready(_) = table.try_take_record(token).unwrap() {}
        table.discard_ready_records(token);
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}
