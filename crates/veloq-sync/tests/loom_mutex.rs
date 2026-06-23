#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::thread;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use veloq_sync::mutex::Mutex;

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(std::ptr::null(), &VTABLE),
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
