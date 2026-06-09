#![cfg(feature = "loom")]
use veloq_driver_core::driver::registry::OpRegistry;
use veloq_driver_core::driver::*;
use veloq_driver_core::slot::{CheckedSlotView, SlotRegistryExt, SlotSpec, SlotView};
use veloq_shim::thread;

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {}

struct DummySlotSpec;

impl SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = ();
    type Completion = usize;
}

fn active_table() -> (SharedCompletionTable<(), ()>, OpToken) {
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
    let table: SharedCompletionTable<(), ()> = registry.shared.clone();
    (table, token)
}

#[test]
fn test_completion_table_loom() {
    loom::model(|| {
        let (table, token) = active_table();

        let table_cloned = table.clone();
        let producer = thread::spawn(move || {
            // Mock driver producing a completion
            table_cloned.record_completion(CompletionPacket::user(token, 0, 0, (), None));
        });

        let table_cloned2 = table.clone();
        let consumer = thread::spawn(move || {
            // Mock consumer: mark waiting, then either poll or drop
            table_cloned2.mark_waiting(token);
            match table_cloned2.try_take_record(token) {
                PollRecordResult::Ready(record) => {
                    assert_eq!(record.event.token, CompletionToken::user(token))
                }
                PollRecordResult::Pending | PollRecordResult::Unavailable(_) => {
                    table_cloned2.mark_orphaned(token);
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
        let (table, token) = active_table();

        let table_cloned = table.clone();
        let producer = thread::spawn(move || {
            table_cloned.record_completion(CompletionPacket::user(token, 42, 0, (), None));
        });

        let table_cloned2 = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned2.mark_waiting(token);
            // Simulate select! drop: don't poll, just mark orphaned
            table_cloned2.mark_orphaned(token);
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        let state = table.debug_get_state(0);
        assert!(state == CELL_STATE_IDLE || state == CELL_STATE_ORPHANED);
    });
}

#[test]
fn test_fast_completion_then_waiting_take_loom() {
    loom::model(|| {
        let (table, token) = active_table();

        table.record_completion(CompletionPacket::user(token, 7, 0, (), None));

        table.mark_waiting(token);
        match table.try_take_record(token) {
            PollRecordResult::Ready(record) => {
                assert_eq!(record.event.token, CompletionToken::user(token));
                assert_eq!(record.event.res, 7);
            }
            PollRecordResult::Pending => panic!("expected ready after fast completion"),
            PollRecordResult::Unavailable(anomaly) => {
                panic!("unexpected unavailable completion: {anomaly:?}")
            }
        }

        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_stale_after_generation_advance_loom() {
    loom::model(|| {
        let (table, token_g1) = active_table();

        table.record_completion(CompletionPacket::user(token_g1, 1, 0, (), None));
        table.mark_waiting(token_g1);
        let _ = table.try_take_record(token_g1);

        match table.try_take_record(token_g1) {
            PollRecordResult::Unavailable(anomaly)
                if anomaly.reason == CompletionAnomalyReason::StaleGeneration => {}
            PollRecordResult::Ready(_) => panic!("old generation must not become ready"),
            PollRecordResult::Pending => panic!("old generation must be stale"),
            PollRecordResult::Unavailable(anomaly) => {
                panic!("old generation should be stale, got {anomaly:?}")
            }
        }
    });
}

#[test]
fn test_ready_race_with_mark_orphaned_loom() {
    loom::model(|| {
        let (table, token) = active_table();

        table.record_completion(CompletionPacket::user(token, 3, 0, (), None));

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

        let (table, token) = active_table();
        let ready_count = Arc::new(AtomicUsize::new(0));

        table.record_completion(CompletionPacket::user(token, 9, 0, (), None));

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
        let state = table.debug_get_state(0);
        assert!(state == CELL_STATE_IDLE || state == CELL_STATE_WAITING);
    });
}
