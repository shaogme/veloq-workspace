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
        let table: SharedCompletionTable = Arc::new(SlotTable::<DummyPlatformOp, ()>::new(1));
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
        let table: SharedCompletionTable = Arc::new(SlotTable::<DummyPlatformOp, ()>::new(1));
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
