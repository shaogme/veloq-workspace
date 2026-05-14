#![cfg(feature = "loom")]
use std::sync::Arc;
use veloq_driver_core::driver::*;
use veloq_driver_core::slot::SlotTable;
use veloq_shim::thread;

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {}

#[test]
fn test_completion_table_loom() {
    loom::model(|| {
        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token = encode_completion_token(0, 1);

        let table_cloned = table.clone();
        let producer = thread::spawn(move || {
            // Mock driver producing a completion
            table_cloned.record_completion_with_data(
                CompletionEvent {
                    user_data: token,
                    res: 0,
                    flags: 0,
                },
                None,
                None,
            );
        });

        let table_cloned2 = table.clone();
        let consumer = thread::spawn(move || {
            // Mock consumer: mark waiting, then either poll or drop
            table_cloned2.mark_waiting(token);
            match table_cloned2.try_take_record(token) {
                PollRecordResult::Ready(record) => assert_eq!(record.event.user_data, token),
                PollRecordResult::Pending | PollRecordResult::Stale => {
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
        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token = encode_completion_token(0, 1);

        let table_cloned = table.clone();
        let producer = thread::spawn(move || {
            table_cloned.record_completion_with_data(
                CompletionEvent {
                    user_data: token,
                    res: 42,
                    flags: 0,
                },
                None,
                None,
            );
        });

        let table_cloned2 = table.clone();
        let consumer = thread::spawn(move || {
            table_cloned2.mark_waiting(token);
            // Simulate select! drop: don't poll, just mark orphaned
            table_cloned2.mark_orphaned(token);
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        // After both finished, state must be IDLE (since producer or consumer cleaned up)
        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_fast_completion_then_waiting_take_loom() {
    loom::model(|| {
        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token = encode_completion_token(0, 1);

        table.record_completion_with_data(
            CompletionEvent {
                user_data: token,
                res: 7,
                flags: 0,
            },
            None,
            None,
        );

        table.mark_waiting(token);
        match table.try_take_record(token) {
            PollRecordResult::Ready(record) => {
                assert_eq!(record.event.user_data, token);
                assert_eq!(record.event.res, 7);
            }
            PollRecordResult::Pending => panic!("expected ready after fast completion"),
            PollRecordResult::Stale => panic!("unexpected stale in same generation"),
        }

        assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
    });
}

#[test]
fn test_stale_after_generation_advance_loom() {
    loom::model(|| {
        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token_g1 = encode_completion_token(0, 1);
        let token_g2 = encode_completion_token(0, 2);

        table.record_completion_with_data(
            CompletionEvent {
                user_data: token_g1,
                res: 1,
                flags: 0,
            },
            None,
            None,
        );
        table.mark_waiting(token_g1);
        let _ = table.try_take_record(token_g1);

        // 推进到更高代，旧 token 必须 stale。
        table.mark_waiting(token_g2);

        match table.try_take_record(token_g1) {
            PollRecordResult::Stale => {}
            PollRecordResult::Ready(_) => panic!("old generation must not become ready"),
            PollRecordResult::Pending => panic!("old generation must be stale"),
        }
    });
}

#[test]
fn test_ready_race_with_mark_orphaned_loom() {
    loom::model(|| {
        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token = encode_completion_token(0, 1);

        table.record_completion_with_data(
            CompletionEvent {
                user_data: token,
                res: 3,
                flags: 0,
            },
            None,
            None,
        );

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

        let table: SharedCompletionTable<()> = Arc::new(SlotTable::<DummyPlatformOp, (), ()>::new(1));
        let token = encode_completion_token(0, 1);
        let ready_count = Arc::new(AtomicUsize::new(0));

        table.record_completion_with_data(
            CompletionEvent {
                user_data: token,
                res: 9,
                flags: 0,
            },
            None,
            None,
        );

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
