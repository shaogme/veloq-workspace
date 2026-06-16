#![cfg(feature = "loom")]
use veloq_driver_core::{
    driver::{registry::OpRegistry, *},
    slot::{
        CheckedSlotView, InFlightOrphaned, InFlightWaiting, Slot, SlotRegistryExt, SlotSpec,
        SlotView,
    },
};
use veloq_shim::{Arc, sync::Mutex, thread};

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {
    type CleanupContext<'a> = ();
}

struct DummySlotSpec;

impl SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = ();
    type Completion = usize;
    type CompletionDiagnostics = ();
}

struct TestHooks;

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
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        Ok(CompletionHookOutcome::User {
            event,
            payload: payload.expect("loom test payload should exist"),
            detail,
            cleanup: CompletionCleanupGuard::default(),
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
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    (Arc::new(Mutex::new(registry)), table, token)
}

fn accept_completion(registry: &Mutex<OpRegistry<DummySlotSpec>>, token: OpToken, res: i32) {
    let mut registry = registry.lock();
    let diagnostics = registry.shared.completion_diagnostics();
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    let mut hooks = TestHooks;
    let _ = registry.accept_completion(
        &table,
        &diagnostics,
        &mut hooks,
        CompletionIngress::User(UserCompletionEvent::from_parts(
            CompletionBackend::Core,
            token,
            res,
            0,
        )),
    );
}

#[test]
fn test_completion_table_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        let registry_cloned = registry.clone();
        let producer = thread::spawn(move || {
            accept_completion(&registry_cloned, token, 0);
        });

        let table_cloned = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned.mark_waiting(token);
            match table_cloned.try_take_record(token) {
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
        });

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
        });

        let table_cloned = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned.mark_waiting(token);
            table_cloned.mark_orphaned(token);
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_fast_completion_then_waiting_take_loom() {
    loom::model(|| {
        let (registry, table, token) = active_registry();

        accept_completion(&registry, token, 7);

        table.mark_waiting(token);
        match table.try_take_record(token) {
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
        let _ = table.try_take_record(token_g1);

        match table.try_take_record(token_g1) {
            PollRecordResult::Unavailable { kind, .. }
                if kind.reason() == CompletionAnomalyReason::StaleGeneration => {}
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
            let _ = t1.try_take_record(token);
        });

        let t2 = table.clone();
        let consumer_drop = thread::spawn(move || {
            t2.mark_orphaned(token);
        });

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
            if let PollRecordResult::Ready(_) = c1_table.try_take_record(token) {
                c1_ready.fetch_add(1, Ordering::SeqCst);
            }
        });

        let c2_table = table.clone();
        let c2_ready = ready_count.clone();
        let c2 = thread::spawn(move || {
            c2_table.mark_waiting(token);
            if let PollRecordResult::Ready(_) = c2_table.try_take_record(token) {
                c2_ready.fetch_add(1, Ordering::SeqCst);
            }
        });

        c1.join().unwrap();
        c2.join().unwrap();

        assert!(ready_count.load(Ordering::SeqCst) <= 1);
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}
