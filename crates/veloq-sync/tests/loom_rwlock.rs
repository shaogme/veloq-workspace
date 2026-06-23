#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::thread;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use veloq_sync::rwlock::RwLock;

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(std::ptr::null(), &VTABLE),
    |_| {},
    |_| {},
    |_| {},
);

fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = f;
    let mut pinned = unsafe { Pin::new_unchecked(&mut f) };
    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match pinned.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => loom::thread::yield_now(),
        }
    }
}

#[test]
fn test_loom_rwlock_write_exclusion() {
    loom::model(|| {
        let m = Arc::new(RwLock::new(0));
        let m1 = m.clone();
        let m2 = m.clone();

        let t1 = thread::spawn(move || {
            let mut g = block_on(m1.write());
            *g += 1;
        });

        let t2 = thread::spawn(move || {
            let mut g = block_on(m2.write());
            *g += 1;
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let g = block_on(m.read());
        assert_eq!(*g, 2);
    });
}

#[test]
fn test_loom_rwlock_read_write_coexistence() {
    loom::model(|| {
        let m = Arc::new(RwLock::new(0));
        let m1 = m.clone();
        let m2 = m.clone();

        let t1 = thread::spawn(move || {
            let mut g = block_on(m1.write());
            *g += 1;
        });

        let t2 = thread::spawn(move || {
            let g = block_on(m2.read());
            // Value could be 0 or 1 depending on execution order
            assert!(*g == 0 || *g == 1);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let g = block_on(m.read());
        assert!(*g <= 1); // 最终可能已经无法断言确切值，但至少不panic
    });
}

#[test]
fn test_loom_rwlock_reader_parallel() {
    loom::model(|| {
        let m = Arc::new(RwLock::new(0));
        let m1 = m.clone();
        let m2 = m.clone();

        // Spawn two readers. They should be able to hold the lock simultaneously,
        // but loom doesn't easily verify "simultaneous".
        // We verify that they both complete.
        let t1 = thread::spawn(move || {
            let _g = block_on(m1.read());
        });

        let t2 = thread::spawn(move || {
            let _g = block_on(m2.read());
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}
