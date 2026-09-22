#![cfg(feature = "loom")]

use loom::{future::block_on, sync::Arc, thread};
use veloq_std::{
    future::Future,
    pin::Pin,
    ptr,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};
use veloq_sync::mutex::Mutex;

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(ptr::null(), &VTABLE),
    |_| {},
    |_| {},
    |_| {},
);

#[test]
fn test_loom_mutex_exclusion() {
    loom::model(|| {
        let m = Arc::new(Mutex::new(0));
        let m1 = m.clone();
        let m2 = m.clone();

        let t1 = thread::spawn(move || {
            let mut future = m1.lock();
            let mut pinned = unsafe { Pin::new_unchecked(&mut future) };
            let waker = dummy_waker();
            let mut cx = Context::from_waker(&waker);

            loop {
                match pinned.as_mut().poll(&mut cx) {
                    Poll::Ready(mut g) => {
                        *g += 1;
                        break;
                    }
                    Poll::Pending => {
                        loom::thread::yield_now();
                    }
                }
            }
        });

        let t2 = thread::spawn(move || {
            let mut future = m2.lock();
            let mut pinned = unsafe { Pin::new_unchecked(&mut future) };
            let waker = dummy_waker();
            let mut cx = Context::from_waker(&waker);

            loop {
                match pinned.as_mut().poll(&mut cx) {
                    Poll::Ready(mut g) => {
                        *g += 1;
                        break;
                    }
                    Poll::Pending => {
                        loom::thread::yield_now();
                    }
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let mut future = m.lock();
        let mut pinned = unsafe { Pin::new_unchecked(&mut future) };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            match pinned.as_mut().poll(&mut cx) {
                Poll::Ready(g) => {
                    assert_eq!(*g, 2);
                    break;
                }
                Poll::Pending => loom::thread::yield_now(),
            }
        }
    });
}

#[test]
fn test_loom_mutex_contended() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let m = Arc::new(Mutex::new(0usize));
        let m1 = m.clone();
        let m2 = m.clone();
        let m3 = m.clone();

        let t1 = thread::spawn(move || {
            block_on(async move {
                let mut g = m1.lock().await;
                *g += 1;
            });
        });

        let t2 = thread::spawn(move || {
            block_on(async move {
                let mut g = m2.lock().await;
                *g += 1;
            });
        });

        let t3 = thread::spawn(move || {
            block_on(async move {
                let mut g = m3.lock().await;
                *g += 1;
            });
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        block_on(async move {
            let g = m.lock().await;
            assert_eq!(*g, 3);
        });
    });
}
