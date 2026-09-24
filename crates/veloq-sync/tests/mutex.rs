#![cfg(not(feature = "loom"))]
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicUsize, Ordering},
};

use veloq_std::sync::Arc;
use veloq_std::{
    future::Future,
    task::{Context, RawWaker, RawWakerVTable, Waker},
};
use veloq_sync::mutex::Mutex;

struct WakeData {
    wakes: AtomicUsize,
    reentrant_lock: Option<StdArc<Mutex<()>>>,
}

fn counting_waker(data: StdArc<WakeData>) -> Waker {
    unsafe fn clone(data: *const ()) -> RawWaker {
        // SAFETY: `data` is an `Arc<WakeData>` pointer created below.
        unsafe { StdArc::increment_strong_count(data.cast::<WakeData>()) };
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        // SAFETY: `data` owns one strong reference transferred to this callback.
        let data = unsafe { StdArc::from_raw(data.cast::<WakeData>()) };
        data.wakes.fetch_add(1, Ordering::SeqCst);
        if let Some(lock) = &data.reentrant_lock {
            let _ = lock.try_lock();
            let _ = lock.is_locked();
        }
    }

    unsafe fn wake_by_ref(data: *const ()) {
        // SAFETY: Keep the original reference alive after forwarding the wake.
        unsafe { StdArc::increment_strong_count(data.cast::<WakeData>()) };
        unsafe { wake(data) };
    }

    unsafe fn drop(data: *const ()) {
        // SAFETY: Drop the strong reference owned by this raw waker.
        unsafe { StdArc::decrement_strong_count(data.cast::<WakeData>()) };
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    let data = StdArc::into_raw(data).cast::<()>();
    // SAFETY: The vtable retains and releases the `Arc` reference correctly.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

#[tokio::test]
async fn test_mutex_simple() {
    let m = Mutex::new(10);
    {
        let mut guard = m.lock().await;
        *guard += 1;
    }
    assert_eq!(*m.lock().await, 11);
}

#[tokio::test]
async fn test_mutex_contention() {
    let m = Arc::new(Mutex::new(0));
    let mut tasks = vec![];

    for _ in 0..10 {
        let m = m.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let mut guard = m.lock().await;
                *guard += 1;
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(*m.lock().await, 1000);
}

#[tokio::test]
async fn test_mutex_try_lock() {
    let m = Arc::new(Mutex::new(0));
    let m2 = m.clone();

    let guard = m.try_lock().unwrap();

    // Contention
    let t = tokio::spawn(async move {
        assert!(m2.try_lock().is_none());
    });
    t.await.unwrap();

    drop(guard);
    assert!(m.try_lock().is_some());
}

#[test]
fn test_mutex_grant_is_ready_before_wake_callback_returns() {
    let mutex = Mutex::new(());
    let guard = mutex.try_lock().unwrap();
    let mut future = Box::pin(mutex.lock());
    let data = StdArc::new(WakeData {
        wakes: AtomicUsize::new(0),
        reentrant_lock: None,
    });
    let waker = counting_waker(data.clone());
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
        future.as_mut().poll(&mut cx),
        core::task::Poll::Pending
    ));
    drop(guard);
    assert_eq!(data.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        core::task::Poll::Ready(_)
    ));
}

#[test]
fn test_mutex_waker_replacement_only_wakes_latest() {
    let mutex = Mutex::new(());
    let guard = mutex.try_lock().unwrap();
    let mut future = Box::pin(mutex.lock());
    let first = StdArc::new(WakeData {
        wakes: AtomicUsize::new(0),
        reentrant_lock: None,
    });
    let second = StdArc::new(WakeData {
        wakes: AtomicUsize::new(0),
        reentrant_lock: None,
    });
    let first_waker = counting_waker(first.clone());
    let second_waker = counting_waker(second.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);

    assert!(matches!(
        future.as_mut().poll(&mut first_cx),
        core::task::Poll::Pending
    ));
    assert!(matches!(
        future.as_mut().poll(&mut second_cx),
        core::task::Poll::Pending
    ));
    drop(guard);
    assert_eq!(first.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(second.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        future.as_mut().poll(&mut second_cx),
        core::task::Poll::Ready(_)
    ));
}

#[test]
fn test_mutex_waker_can_reenter_after_detach() {
    let mutex = StdArc::new(Mutex::new(()));
    let guard = mutex.try_lock().unwrap();
    let mut future = Box::pin(mutex.lock());
    let data = StdArc::new(WakeData {
        wakes: AtomicUsize::new(0),
        reentrant_lock: Some(mutex.clone()),
    });
    let waker = counting_waker(data.clone());
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
        future.as_mut().poll(&mut cx),
        core::task::Poll::Pending
    ));
    drop(guard);
    assert_eq!(data.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        core::task::Poll::Ready(_)
    ));
}

#[tokio::test]
async fn test_mutex_cancel_cascading() {
    let m = Arc::new(Mutex::new(0));
    let guard = m.lock().await;

    let m1 = m.clone();
    let m2 = m.clone();

    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = m1.lock();
        let _ = tx1.send(());
        fut.await;
    });

    rx1.await.unwrap();
    tokio::task::yield_now().await;

    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let t2 = tokio::spawn(async move {
        let fut = m2.lock();
        let _ = tx2.send(());
        let mut g = fut.await;
        *g += 42;
    });

    rx2.await.unwrap();
    tokio::task::yield_now().await;

    // Drop guard, granting lock to t1
    drop(guard);

    // Abort t1 while holding STATE_GRANTED
    t1.abort();
    let _ = t1.await;

    // t2 should receive the lock cascading from t1
    t2.await.unwrap();

    assert_eq!(*m.lock().await, 42);
}

#[tokio::test]
async fn test_mutex_cancel_reset_unlocked() {
    let m = Arc::new(Mutex::new(0));
    let guard = m.lock().await;

    let m1 = m.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = m1.lock();
        let _ = tx.send(());
        fut.await;
    });

    rx.await.unwrap();
    tokio::task::yield_now().await;

    // Drop guard, granting lock to t1
    drop(guard);

    // Cancel t1 while holding STATE_GRANTED
    t1.abort();
    let _ = t1.await;

    // The lock should be safely reset to UNLOCKED
    assert!(!m.is_locked());
    let mut g = m.try_lock().expect("mutex should be unlocked");
    *g = 100;
    drop(g);
    assert_eq!(*m.lock().await, 100);
}
