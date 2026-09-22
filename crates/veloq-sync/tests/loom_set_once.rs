#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;
use veloq_std::future::Future;
use veloq_std::pin::pin;
use veloq_std::ptr;
use veloq_std::task::{Context, RawWaker, RawWakerVTable, Waker};
use veloq_sync::set_once::SetOnce;

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(ptr::null(), &VTABLE)
    }

    unsafe { Waker::from_raw(noop_raw_waker()) }
}

#[test]
fn loom_set_once_basic() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let cell = Arc::new(SetOnce::new());
        let c1 = cell.clone();
        let c2 = cell.clone();

        let h1 = thread::spawn(move || {
            let _ = c1.set(42);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let val = *c2.wait().await;
                assert_eq!(val, 42);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_set_once_concurrent_set() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let cell = Arc::new(SetOnce::new());
        let c1 = cell.clone();
        let c2 = cell.clone();
        let c3 = cell.clone();
        let wins = Arc::new(AtomicUsize::new(0));
        let w1 = wins.clone();
        let w2 = wins.clone();

        let h1 = thread::spawn(move || {
            if c1.set(1).is_ok() {
                w1.fetch_add(1, Ordering::Relaxed);
            }
        });

        let h2 = thread::spawn(move || {
            if c2.set(2).is_ok() {
                w2.fetch_add(1, Ordering::Relaxed);
            }
        });

        let h3 = thread::spawn(move || {
            block_on(async move {
                let val = *c3.wait().await;
                assert!(val == 1 || val == 2);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();

        assert_eq!(wins.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn loom_set_once_two_waiters() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let cell = Arc::new(SetOnce::new());
        let c1 = cell.clone();
        let c2 = cell.clone();
        let c3 = cell.clone();

        let h1 = thread::spawn(move || {
            let _ = c1.set(100);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let val = *c2.wait().await;
                assert_eq!(val, 100);
            });
        });

        let h3 = thread::spawn(move || {
            block_on(async move {
                let val = *c3.wait().await;
                assert_eq!(val, 100);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    });
}

#[test]
fn loom_set_once_cancel() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let cell = Arc::new(SetOnce::new());
        let c1 = cell.clone();
        let c2 = cell.clone();

        let h1 = thread::spawn(move || {
            let _ = c1.set(55);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                {
                    let mut fut = pin!(c2.wait());
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    let _ = fut.as_mut().poll(&mut cx);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();

        assert_eq!(cell.get(), Some(&55));
    });
}
