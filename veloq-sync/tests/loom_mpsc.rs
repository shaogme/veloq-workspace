#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::thread;
use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use veloq_sync::mpsc;

// --- Helper for manual polling ---
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
fn loom_mpsc_unbounded_recv_async() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::owned_unbounded();
        let tx = Arc::new(tx);

        // Thread 1: Sends data
        let tx1 = tx.clone();
        thread::spawn(move || {
            let _ = tx1.send(1);
        });

        // Thread 2: Receives data manually polling the future
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut recv_fut = Box::pin(rx.recv());

        // Loom explores interleavings.
        // 1. We might poll before send -> Pending.
        // 2. We might poll after send -> Ready(Some(1)).

        // We simulate a basic executor loop for the receiver
        loop {
            match recv_fut.as_mut().poll(&mut cx) {
                Poll::Ready(val) => {
                    assert_eq!(val, Some(1));
                    break;
                }
                Poll::Pending => {
                    // In a real executor we would yield.
                    // In loom, we just let the scheduler switch threads.
                    thread::yield_now();
                }
            }
        }
    });
}

#[test]
fn loom_mpsc_bounded_async_send() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::owned_bounded::<usize>(1);
        let tx = Arc::new(tx);

        // Thread 1: Fills the channel and then tries to send more
        let tx1 = tx.clone();
        thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            // First send should be immediate (async but ready) because cap=1
            {
                let mut fut = Box::pin(tx1.send(10));
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => assert!(res.is_ok()),
                    Poll::Pending => {
                        // Should not really happen with empty channel of cap 1,
                        // unless some other weak memory effect?
                        // Actually bounded strategy might not guarantee immediate push if compare_exchange fails?
                        // But eventually it should succeed.
                        // Let's loop poll it.
                        loop {
                            if let Poll::Ready(res) = fut.as_mut().poll(&mut cx) {
                                assert!(res.is_ok());
                                break;
                            }
                            thread::yield_now();
                        }
                    }
                }
            }

            // Second send should block until receiver pops
            {
                let mut fut = Box::pin(tx1.send(20));
                // We poll it once. It MIGHT be pending, or if Rx ran very fast, it MIGHT be ready.
                // We just loop poll until success.
                loop {
                    match fut.as_mut().poll(&mut cx) {
                        Poll::Ready(res) => {
                            assert!(res.is_ok());
                            break;
                        }
                        Poll::Pending => {
                            thread::yield_now();
                        }
                    }
                }
            }
        });

        // Main Thread: Acts as receiver
        // We eventually take 2 items.

        let mut count = 0;
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // We reuse one recv future or create new ones?
        // mpsc::Receiver::recv takes &mut self, so it returns a new future each time usually.
        // Let's check impl:
        // pub async fn recv(&mut self) -> Option<T> { RecvFuture { receiver: self }.await }
        // Yes, new future.

        // Item 1
        loop {
            let mut fut = Box::pin(rx.recv());
            if let Poll::Ready(val) = fut.as_mut().poll(&mut cx) {
                assert_eq!(val, Some(10));
                count += 1;
                break;
            }
            thread::yield_now();
        }

        // Item 2
        loop {
            let mut fut = Box::pin(rx.recv());
            if let Poll::Ready(val) = fut.as_mut().poll(&mut cx) {
                assert_eq!(val, Some(20));
                count += 1;
                break;
            }
            thread::yield_now();
        }

        assert_eq!(count, 2);
    });
}
