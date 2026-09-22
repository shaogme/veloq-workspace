#![cfg(not(feature = "loom"))]

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::task::{Context, Waker};
use std::time::Duration;
use tokio::time::sleep;
use veloq_sync::set_once::{SetOnce, SetOnceError};

#[derive(Clone)]
struct DropCounter {
    drops: Arc<AtomicU32>,
}

impl DropCounter {
    fn new() -> Self {
        Self {
            drops: Arc::new(AtomicU32::new(0)),
        }
    }

    fn assert_num_drops(&self, value: u32) {
        assert_eq!(value, self.drops.load(Ordering::Relaxed));
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn test_set_once_basic() {
    let cell = SetOnce::new();
    assert!(!cell.initialized());
    assert!(!cell.is_initialized());
    assert_eq!(cell.get(), None);

    assert!(cell.set(42).is_ok());
    assert!(cell.initialized());
    assert!(cell.is_initialized());
    assert_eq!(cell.get(), Some(&42));
}

#[test]
fn test_set_once_duplicate_set() {
    let cell = SetOnce::new();
    assert!(cell.set("hello").is_ok());

    let err = cell.set("world").unwrap_err();
    assert_eq!(err, SetOnceError("world"));
    assert_eq!(err.into_inner(), "world");
    assert_eq!(cell.get(), Some(&"hello"));
}

#[test]
fn test_set_once_new_with() {
    let cell_empty: SetOnce<i32> = SetOnce::new_with(None);
    assert!(!cell_empty.initialized());
    assert_eq!(cell_empty.get(), None);

    let cell_full = SetOnce::new_with(Some(100));
    assert!(cell_full.initialized());
    assert_eq!(cell_full.get(), Some(&100));
}

#[test]
fn test_set_once_with_value() {
    let cell = SetOnce::with_value("init");
    assert!(cell.initialized());
    assert_eq!(cell.get(), Some(&"init"));
}

#[test]
fn test_set_once_into_inner() {
    let cell: SetOnce<i32> = SetOnce::new();
    assert_eq!(cell.into_inner(), None);

    let cell2 = SetOnce::new_with(Some(99));
    assert_eq!(cell2.into_inner(), Some(99));
}

#[test]
fn test_set_once_get_mut() {
    let mut cell = SetOnce::new();
    assert_eq!(cell.get_mut(), None);

    let _ = cell.set(10);
    if let Some(val) = cell.get_mut() {
        *val += 5;
    }
    assert_eq!(cell.get(), Some(&15));
}

#[test]
fn test_set_once_take() {
    let mut cell = SetOnce::new_with(Some(123));
    assert!(cell.initialized());

    assert_eq!(cell.take(), Some(123));
    assert!(!cell.initialized());
    assert_eq!(cell.get(), None);

    // Can set again after take
    assert!(cell.set(456).is_ok());
    assert_eq!(cell.get(), Some(&456));
}

#[test]
fn test_set_once_drop_cell() {
    let counter = DropCounter::new();
    let counter_cl = counter.clone();

    {
        let cell = SetOnce::new();
        let prev = cell.set(counter_cl);
        assert!(prev.is_ok());
    }

    counter.assert_num_drops(1);
}

#[test]
fn test_set_once_drop_cell_new_with() {
    let counter = DropCounter::new();

    {
        let cell = SetOnce::new_with(Some(counter.clone()));
        assert!(cell.initialized());
    }

    counter.assert_num_drops(1);
}

#[test]
fn test_set_once_drop_into_inner() {
    let counter = DropCounter::new();
    let cell = SetOnce::new_with(Some(counter.clone()));

    let inner = cell.into_inner();
    assert!(inner.is_some());
    counter.assert_num_drops(0);

    drop(inner);
    counter.assert_num_drops(1);
}

#[tokio::test]
async fn test_set_once_async_wait() {
    let cell = Arc::new(SetOnce::new());
    let cell2 = cell.clone();
    let cell3 = cell.clone();

    let h1 = tokio::spawn(async move { *cell2.wait().await });
    let h2 = tokio::spawn(async move { *cell3.wait().await });

    sleep(Duration::from_millis(20)).await;
    assert!(cell.set(777).is_ok());

    assert_eq!(h1.await.unwrap(), 777);
    assert_eq!(h2.await.unwrap(), 777);
}

#[tokio::test]
async fn test_set_once_wait_already_initialized() {
    let cell = SetOnce::new_with(Some(888));
    assert_eq!(*cell.wait().await, 888);
}

#[tokio::test]
async fn test_set_once_cancellation() {
    let cell = Arc::new(SetOnce::<i32>::new());
    let cell2 = cell.clone();
    let cell3 = cell.clone();

    let completed = Arc::new(AtomicBool::new(false));
    let completed_clone = completed.clone();

    // Task 1: cancelled early via select
    let h1 = tokio::spawn(async move {
        tokio::select! {
            _ = cell2.wait() => {
                completed_clone.store(true, Ordering::Release);
            }
            _ = sleep(Duration::from_millis(10)) => {}
        }
    });

    // Task 2: keeps waiting
    let h2 = tokio::spawn(async move { *cell3.wait().await });

    h1.await.unwrap();
    assert!(!completed.load(Ordering::Acquire));

    // Now set the cell value
    sleep(Duration::from_millis(10)).await;
    assert!(cell.set(999).is_ok());

    assert_eq!(h2.await.unwrap(), 999);
}

#[tokio::test]
async fn test_set_once_concurrent_set() {
    let cell = Arc::new(SetOnce::new());
    let success_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..10 {
        let cell = cell.clone();
        let success_count = success_count.clone();
        handles.push(tokio::spawn(async move {
            if cell.set(i).is_ok() {
                success_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(success_count.load(Ordering::Relaxed), 1);
    assert!(cell.initialized());
    let val = *cell.get().unwrap();
    assert!(val < 10);
    assert_eq!(*cell.wait().await, val);
}

#[tokio::test]
async fn test_set_once_poll_cancel_drop() {
    let cell = SetOnce::<i32>::new();

    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = pin!(cell.wait());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
    }

    // After drop, set should succeed
    assert!(cell.set(1234).is_ok());
    assert_eq!(cell.get(), Some(&1234));
}

#[test]
fn test_set_once_traits() {
    let cell: SetOnce<i32> = SetOnce::default();
    assert!(!cell.initialized());
    assert_eq!(format!("{:?}", cell), "SetOnce { value: <uninitialized> }");

    let cell_from = SetOnce::from(100);
    assert!(cell_from.initialized());
    assert_eq!(format!("{:?}", cell_from), "SetOnce { value: 100 }");

    let cell_clone = cell_from.clone();
    assert_eq!(cell_from, cell_clone);
}
