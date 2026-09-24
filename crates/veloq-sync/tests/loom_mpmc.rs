#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;
use veloq_std::{
    future::Future,
    mem::ManuallyDrop,
    pin::pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};
use veloq_sync::{TryRecvError, mpmc};

unsafe fn clone_counter(data: *const ()) -> RawWaker {
    let counter =
        ManuallyDrop::new(unsafe { Arc::<AtomicUsize>::from_raw(data as *const AtomicUsize) });
    let cloned = Arc::clone(&counter);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &COUNTER_VTABLE)
}

unsafe fn wake_counter(data: *const ()) {
    let counter = unsafe { Arc::<AtomicUsize>::from_raw(data as *const AtomicUsize) };
    counter.fetch_add(1, Ordering::Release);
}

unsafe fn wake_counter_by_ref(data: *const ()) {
    let counter =
        ManuallyDrop::new(unsafe { Arc::<AtomicUsize>::from_raw(data as *const AtomicUsize) });
    counter.fetch_add(1, Ordering::Release);
}

unsafe fn drop_counter(data: *const ()) {
    drop(unsafe { Arc::<AtomicUsize>::from_raw(data as *const AtomicUsize) });
}

static COUNTER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_counter,
    wake_counter,
    wake_counter_by_ref,
    drop_counter,
);

fn counter_waker(counter: Arc<AtomicUsize>) -> Waker {
    let data = Arc::into_raw(counter) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(data, &COUNTER_VTABLE)) }
}

#[test]
fn loom_mpmc_unbounded_send_recv_async() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, rx) = mpmc::unbounded::<i32>();

        let tx = Arc::new(tx);
        let rx = Arc::new(rx);

        // Sender Thread
        let tx1 = tx.clone();
        let h1 = thread::spawn(move || {
            block_on(async move {
                tx1.send(100).await.unwrap();
            });
        });

        // Receiver Thread
        let rx1 = rx.clone();
        let h2 = thread::spawn(move || {
            block_on(async move {
                let val = rx1.recv().await.unwrap();
                assert_eq!(val, 100);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_mpmc_register_then_send_wakes_receiver() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, rx) = mpmc::unbounded();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counter_waker(wake_count.clone());
        let mut cx = Context::from_waker(&waker);
        let mut recv = pin!(rx.recv());

        assert!(matches!(recv.as_mut().poll(&mut cx), Poll::Pending));
        tx.try_send(1).unwrap();
        assert!(wake_count.load(Ordering::Acquire) > 0);
        assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(Ok(1)));
    });
}

#[test]
fn loom_mpmc_send_then_register_reads_message() {
    loom::model(|| {
        let (tx, rx) = mpmc::unbounded();
        tx.try_send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    });
}

#[test]
fn loom_mpmc_close_wakes_registered_receiver() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, rx) = mpmc::unbounded::<i32>();
        let rx = Arc::new(rx);
        let tx = Arc::new(tx);
        let rx1 = rx.clone();
        let waiter = thread::spawn(move || {
            block_on(async move {
                assert_eq!(rx1.recv().await, Err(TryRecvError::Disconnected));
            });
        });
        drop(tx);
        waiter.join().unwrap();
    });
}

#[test]
fn loom_mpmc_granted_receiver_can_be_cancelled() {
    loom::model(|| {
        let (tx, rx) = mpmc::unbounded();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counter_waker(wake_count.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut recv = pin!(rx.recv());
            assert!(matches!(recv.as_mut().poll(&mut cx), Poll::Pending));
            tx.try_send(3).unwrap();
        }
        tx.try_send(4).unwrap();
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Ok(4));
    });
}
