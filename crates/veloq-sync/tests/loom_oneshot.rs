#![cfg(feature = "loom")]
use loom::thread;
use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use veloq_sync::oneshot;

// --- Helper for manual polling (Same as loom_mpsc.rs) ---
fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    unsafe { Waker::from_raw(noop_raw_waker()) }
}

#[test]
fn loom_oneshot_send_recv() {
    loom::model(|| {
        let (tx, rx) = oneshot::owned_channel();

        let t1 = thread::spawn(move || {
            let _ = tx.send(1);
        });

        let t2 = thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let mut rx = Box::pin(rx);

            loop {
                match rx.as_mut().poll(&mut cx) {
                    Poll::Ready(val) => {
                        assert_eq!(val, Ok(1));
                        break;
                    }
                    Poll::Pending => {
                        thread::yield_now();
                    }
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn loom_oneshot_drop_sender() {
    loom::model(|| {
        let (tx, rx) = oneshot::owned_channel::<i32>();

        let t1 = thread::spawn(move || {
            drop(tx);
        });

        let t2 = thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let mut rx = Box::pin(rx);

            loop {
                match rx.as_mut().poll(&mut cx) {
                    Poll::Ready(val) => {
                        assert!(val.is_err());
                        break;
                    }
                    Poll::Pending => {
                        thread::yield_now();
                    }
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn loom_oneshot_try_recv() {
    loom::model(|| {
        let (tx, mut rx) = oneshot::owned_channel();

        // Thread 1: sends
        thread::spawn(move || {
            let _ = tx.send(123);
        });

        // Thread 2 eventually sees it, or sees empty then sees it
        // Note: try_recv is not a future, checking it in loop
        loop {
            match rx.try_recv() {
                Ok(v) => {
                    assert_eq!(v, 123);
                    break;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    thread::yield_now();
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    panic!("Should not be closed without value");
                }
            }
        }
    });
}

#[test]
fn loom_oneshot_close_receiver() {
    loom::model(|| {
        let (mut tx, rx) = oneshot::owned_channel::<()>();

        thread::spawn(move || {
            drop(rx);
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // tx.closed() future
        let mut closed_fut = Box::pin(async move {
            tx.closed().await;
            assert!(tx.is_closed());
        });

        loop {
            match closed_fut.as_mut().poll(&mut cx) {
                Poll::Ready(_) => break,
                Poll::Pending => thread::yield_now(),
            }
        }
    });
}
