use std::{
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    time::{Duration, Instant},
};

use veloq::{
    runtime::{Runtime, context::Ctx, scope, scope_local},
    time::{MissedTickBehavior, interval, sleep, sleep_local, sleep_until, timeout, timeout_at},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime::select;

fn run_test<F, R>(worker_threads: NonZeroUsize, f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(worker_threads))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_sleep_basic() {
    run_test(nz!(1), async |ctx| {
        let start = Instant::now();
        sleep(ctx, Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
    });
}

#[test]
fn test_sleep_local_basic() {
    run_test(nz!(1), async |ctx| {
        scope_local!(ctx, async |s| {
            let handle = s.spawn_boxed_local(async move {
                let start = Instant::now();
                sleep_local(ctx, Duration::from_millis(100)).await;
                start.elapsed()
            });

            let elapsed = handle.await.expect("local sleep task failed");
            assert!(elapsed >= Duration::from_millis(100));
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sleep_until() {
    run_test(nz!(1), async |ctx| {
        let deadline = Instant::now() + Duration::from_millis(200);
        sleep_until(ctx, deadline).await;
        assert!(Instant::now() >= deadline);
    });
}

#[test]
fn test_sleep_zero_duration() {
    run_test(nz!(1), async |ctx| {
        let start = Instant::now();
        sleep(ctx, Duration::ZERO).await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(10));
    });
}

#[test]
fn test_sleep_reset() {
    run_test(nz!(1), async |ctx| {
        let mut s = sleep(ctx, Duration::from_secs(10));
        let start = Instant::now();

        s.reset(Instant::now() + Duration::from_millis(50));
        s.await;

        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed < Duration::from_secs(1));
    });
}

#[test]
fn test_timeout_success() {
    run_test(nz!(1), async |ctx| {
        let result = timeout(ctx, Duration::from_secs(1), async { "success" }).await;
        assert_eq!(result.expect("timeout should not elapse"), "success");
    });
}

#[test]
fn test_timeout_elapsed() {
    run_test(nz!(1), async |ctx| {
        let result = timeout(ctx, Duration::from_millis(50), async {
            sleep(ctx, Duration::from_secs(1)).await;
            "never"
        })
        .await;

        assert!(result.is_err());
    });
}

#[test]
fn test_timeout_at() {
    run_test(nz!(1), async |ctx| {
        let deadline = Instant::now() + Duration::from_millis(50);
        let result = timeout_at(ctx, deadline, async {
            sleep(ctx, Duration::from_secs(1)).await;
        })
        .await;

        assert!(result.is_err());
    });
}

#[test]
fn test_interval_basic_burst() {
    run_test(nz!(1), async |ctx| {
        let start = Instant::now();
        let mut interval = interval(ctx, Duration::from_millis(20));

        interval.tick().await;
        interval.tick().await;
        interval.tick().await;

        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(40));
    });
}

#[test]
fn test_interval_missed_burst() {
    run_test(nz!(1), async |ctx| {
        let mut interval = interval(ctx, Duration::from_millis(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        interval.tick().await;
        sleep(ctx, Duration::from_millis(55)).await;

        let t1 = Instant::now();
        interval.tick().await;
        let d1 = t1.elapsed();

        let t2 = Instant::now();
        interval.tick().await;
        let d2 = t2.elapsed();

        assert!(d1 < Duration::from_millis(5));
        assert!(d2 < Duration::from_millis(5));
    });
}

#[test]
fn test_interval_missed_delay() {
    run_test(nz!(1), async |ctx| {
        let mut interval = interval(ctx, Duration::from_millis(20));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        interval.tick().await;
        sleep(ctx, Duration::from_millis(50)).await;

        let start = Instant::now();
        interval.tick().await;
        interval.tick().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(20));
    });
}

#[test]
fn test_interval_missed_skip() {
    run_test(nz!(1), async |ctx| {
        let mut interval = interval(ctx, Duration::from_millis(20));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        interval.tick().await;
        sleep(ctx, Duration::from_millis(70)).await;

        let start = Instant::now();
        interval.tick().await;
        let elapsed = start.elapsed();

        assert!(elapsed < Duration::from_millis(20));
    });
}

#[test]
fn test_concurrent_sleeps() {
    run_test(nz!(4), async |ctx| {
        scope!(ctx, async |s| {
            let mut handles = Vec::new();
            for i in 0..10 {
                handles.push(s.spawn_boxed(async move {
                    let duration = Duration::from_millis((i + 1) * 10);
                    sleep(ctx, duration).await;
                    duration
                }));
            }

            for h in handles {
                let dur = h.await.expect("sleep task failed");
                let _ = dur;
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_mixed_local_and_sync_sleeps() {
    run_test(nz!(2), async |ctx| {
        scope!(ctx, async |s| {
            let h_sync = s.spawn_boxed(async move {
                sleep(ctx, Duration::from_millis(50)).await;
                "sync"
            });
            assert_eq!(h_sync.await.expect("sync task failed"), "sync");
        })
        .await
        .unwrap();

        scope_local!(ctx, async |s| {
            let h_local = s.spawn_boxed_local(async move {
                sleep_local(ctx, Duration::from_millis(50)).await;
                "local"
            });
            assert_eq!(h_local.await.expect("local task failed"), "local");
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_select_timeout() {
    run_test(nz!(1), async |ctx| {
        let res = select! {
            ctx;
            _ = sleep(ctx, Duration::from_millis(100)) => "timeout",
            _ = async { 42 } => "value",
        };
        assert_eq!(res, "value");

        let res = select! {
            ctx;
            _ = sleep(ctx, Duration::from_millis(10)) => "timeout",
            _ = sleep(ctx, Duration::from_millis(1000)) => "value",
        };
        assert_eq!(res, "timeout");
    });
}
