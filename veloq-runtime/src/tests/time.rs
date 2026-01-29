use crate::runtime::Runtime;
use crate::time::{
    MissedTickBehavior, interval, sleep, sleep_local, sleep_until, timeout, timeout_at,
};
use std::time::{Duration, Instant};

// ============ Sleep Tests ============

#[test]
fn test_sleep_basic() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let start = Instant::now();
        sleep(Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
        println!("Sleep 100ms passed, actual: {:?}", elapsed);
    });
}

#[test]
fn test_sleep_local_basic() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let start = Instant::now();
        sleep_local(Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
        println!("LocalSleep 100ms passed, actual: {:?}", elapsed);
    });
}

#[test]
fn test_sleep_until() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_millis(200);
        sleep_until(deadline).await;
        assert!(Instant::now() >= deadline);
        println!("Sleep until deadline passed");
    });
}

#[test]
fn test_sleep_zero_duration() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let start = Instant::now();
        sleep(Duration::ZERO).await;
        // Should complete immediately (or very quickly), likely just a yield
        let elapsed = start.elapsed();
        println!("Sleep zero passed in {:?}", elapsed);
    });
}

#[test]
fn test_sleep_reset() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut s = sleep(Duration::from_secs(10)); // Long sleep
        let start = Instant::now();

        // Reset to short duration
        s.reset(Instant::now() + Duration::from_millis(50));
        s.await;

        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed < Duration::from_secs(1));
        println!("Sleep reset passed, took {:?}", elapsed);
    });
}

// ============ Timeout Tests ============

#[test]
fn test_timeout_success() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let result = timeout(Duration::from_secs(1), async { "success" }).await;

        assert_eq!(result.unwrap(), "success");
    });
}

#[test]
fn test_timeout_elapsed() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let result = timeout(Duration::from_millis(50), async {
            sleep(Duration::from_secs(1)).await;
            "never"
        })
        .await;

        assert!(result.is_err());
        println!("Timeout elapsed as expected");
    });
}

#[test]
fn test_timeout_at() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_millis(50);
        let result = timeout_at(deadline, async {
            sleep(Duration::from_secs(1)).await;
        })
        .await;

        assert!(result.is_err());
    });
}

// ============ Interval Tests ============

#[test]
fn test_interval_basic_burst() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut interval = interval(Duration::from_millis(20));
        let start = Instant::now();

        // Tick 0: Immediate (usually)
        interval.tick().await;

        // Tick 1
        interval.tick().await;
        // Tick 2
        interval.tick().await;

        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(40));
        println!("Interval 3 ticks took {:?}", elapsed);
    });
}

#[test]
fn test_interval_missed_burst() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut interval = interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        // Init
        interval.tick().await;

        // Simulate blocking work ensuring we miss ticks
        sleep(Duration::from_millis(55)).await;

        // Should burst catch up
        // Tick 1 (missed)
        let t1 = Instant::now();
        interval.tick().await;
        let d1 = t1.elapsed();

        // Tick 2 (missed)
        let t2 = Instant::now();
        interval.tick().await;
        let d2 = t2.elapsed();

        println!("Burst catch up deltas: {:?}, {:?}", d1, d2);
        // Bursting should be very fast (no sleep)
        assert!(d1 < Duration::from_millis(5));
        assert!(d2 < Duration::from_millis(5));
    });
}

#[test]
fn test_interval_missed_delay() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut interval = interval(Duration::from_millis(20));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        interval.tick().await; // 0

        // Wait long enough to miss next tick
        sleep(Duration::from_millis(50)).await;

        // This tick should happen immediately but reset schedule based on NOW
        interval.tick().await;

        let start = Instant::now();
        // This next tick should be 20ms from NOW (Delay behavior), not catching up
        interval.tick().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(20));
        println!("Delay behavior interval took {:?}", elapsed);
    });
}

#[test]
fn test_interval_missed_skip() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut interval = interval(Duration::from_millis(20));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        interval.tick().await;

        // Wait 70ms: should miss ~3 ticks (20, 40, 60)
        sleep(Duration::from_millis(70)).await;

        let start = Instant::now();
        // Should compute next tick aligned to period grid but in future
        interval.tick().await;
        let elapsed = start.elapsed();

        // It should wait for the remainder to hit the next grid point
        // if we are at 70ms, next grid is at 80ms, so wait ~10ms
        // The implementation logic: next_tick += period until > now
        println!("Skip behavior wait took {:?}", elapsed);
        assert!(elapsed < Duration::from_millis(20));
    });
}

// ============ Concurrency Tests ============

#[test]
fn test_concurrent_sleeps() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(4))
        .build()
        .unwrap();

    runtime.block_on(async {
        let mut handles = vec![];
        for i in 0..10 {
            handles.push(crate::runtime::context::spawn(async move {
                let duration = Duration::from_millis((i + 1) * 10);
                sleep(duration).await;
                duration
            }));
        }

        for h in handles {
            let dur = h.await;
            println!("Task finished sleeping {:?}", dur);
        }
    });
}

#[test]
fn test_mixed_local_and_sync_sleeps() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(2))
        .build()
        .unwrap();

    runtime.block_on(async {
        let h_sync = crate::runtime::context::spawn(async {
            sleep(Duration::from_millis(50)).await;
            "sync"
        });

        let h_local = crate::runtime::context::spawn_local(async {
            sleep_local(Duration::from_millis(50)).await;
            "local"
        });

        assert_eq!(h_sync.await, "sync");
        assert_eq!(h_local.await, "local");
    });
}

#[test]
fn test_select_timeout() {
    use crate::select;

    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async {
        // Case 1: Value ready before timeout
        let res = select! {
            _ = sleep(Duration::from_millis(100)) => "timeout",
            _ = async { 42 } => "value",
        };
        assert_eq!(res, "value");

        // Case 2: Timeout ready before value
        let res = select! {
            _ = sleep(Duration::from_millis(10)) => "timeout",
            _ = sleep(Duration::from_millis(1000)) => "value",
        };
        assert_eq!(res, "timeout");
    });
}
