use std::cell::Cell;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Waker};
use std::time::Duration;
use tokio::task::LocalSet;
use tokio::time::sleep;
use veloq_local::set_once::{SetOnce, SetOnceError};

#[derive(Clone)]
struct DropCounter {
    drops: Rc<Cell<u32>>,
}

impl DropCounter {
    fn new() -> Self {
        Self {
            drops: Rc::new(Cell::new(0)),
        }
    }

    fn assert_num_drops(&self, value: u32) {
        assert_eq!(value, self.drops.get());
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn test_local_set_once_basic() {
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
fn test_local_set_once_duplicate_set() {
    let cell = SetOnce::new();
    assert!(cell.set("hello").is_ok());

    let err = cell.set("world").unwrap_err();
    assert_eq!(err, SetOnceError("world"));
    assert_eq!(err.into_inner(), "world");
    assert_eq!(cell.get(), Some(&"hello"));
}

#[test]
fn test_local_set_once_new_with() {
    let cell_empty: SetOnce<i32> = SetOnce::new_with(None);
    assert!(!cell_empty.initialized());
    assert_eq!(cell_empty.get(), None);

    let cell_full = SetOnce::new_with(Some(100));
    assert!(cell_full.initialized());
    assert_eq!(cell_full.get(), Some(&100));
}

#[test]
fn test_local_set_once_with_value() {
    let cell = SetOnce::with_value("init");
    assert!(cell.initialized());
    assert_eq!(cell.get(), Some(&"init"));
}

#[test]
fn test_local_set_once_into_inner() {
    let cell: SetOnce<i32> = SetOnce::new();
    assert_eq!(cell.into_inner(), None);

    let cell2 = SetOnce::new_with(Some(99));
    assert_eq!(cell2.into_inner(), Some(99));
}

#[test]
fn test_local_set_once_get_mut() {
    let mut cell = SetOnce::new();
    assert_eq!(cell.get_mut(), None);

    let _ = cell.set(10);
    if let Some(val) = cell.get_mut() {
        *val += 5;
    }
    assert_eq!(cell.get(), Some(&15));
}

#[test]
fn test_local_set_once_take() {
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
fn test_local_set_once_drop_cell() {
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
fn test_local_set_once_drop_cell_new_with() {
    let counter = DropCounter::new();

    {
        let cell = SetOnce::new_with(Some(counter.clone()));
        assert!(cell.initialized());
    }

    counter.assert_num_drops(1);
}

#[test]
fn test_local_set_once_drop_into_inner() {
    let counter = DropCounter::new();
    let cell = SetOnce::new_with(Some(counter.clone()));

    let inner = cell.into_inner();
    assert!(inner.is_some());
    counter.assert_num_drops(0);

    drop(inner);
    counter.assert_num_drops(1);
}

#[tokio::test]
async fn test_local_set_once_multiple_waiters() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let cell = Rc::new(SetOnce::new());
            let c1 = cell.clone();
            let c2 = cell.clone();

            let h1 = tokio::task::spawn_local(async move { *c1.wait().await });
            let h2 = tokio::task::spawn_local(async move { *c2.wait().await });

            sleep(Duration::from_millis(10)).await;
            assert!(cell.set(777).is_ok());

            assert_eq!(h1.await.unwrap(), 777);
            assert_eq!(h2.await.unwrap(), 777);
        })
        .await;
}

#[tokio::test]
async fn test_local_set_once_wait_already_initialized() {
    let cell = SetOnce::new_with(Some(888));
    assert_eq!(*cell.wait().await, 888);
}

#[tokio::test]
async fn test_local_set_once_cancellation() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let cell = Rc::new(SetOnce::<i32>::new());
            let c1 = cell.clone();
            let c2 = cell.clone();

            let completed = Rc::new(Cell::new(false));
            let completed_clone = completed.clone();

            let h1 = tokio::task::spawn_local(async move {
                tokio::select! {
                    _ = c1.wait() => {
                        completed_clone.set(true);
                    }
                    _ = sleep(Duration::from_millis(10)) => {}
                }
            });

            let h2 = tokio::task::spawn_local(async move { *c2.wait().await });

            h1.await.unwrap();
            assert!(!completed.get());

            sleep(Duration::from_millis(10)).await;
            assert!(cell.set(999).is_ok());

            assert_eq!(h2.await.unwrap(), 999);
        })
        .await;
}

#[tokio::test]
async fn test_local_set_once_poll_cancel_drop() {
    let cell = SetOnce::<i32>::new();

    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = pin!(cell.wait());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
    }

    assert!(cell.set(1234).is_ok());
    assert_eq!(cell.get(), Some(&1234));
}

#[test]
fn test_local_set_once_traits() {
    let cell: SetOnce<i32> = SetOnce::default();
    assert!(!cell.initialized());
    assert_eq!(format!("{:?}", cell), "SetOnce { value: <uninitialized> }");

    let cell_from = SetOnce::from(100);
    assert!(cell_from.initialized());
    assert_eq!(format!("{:?}", cell_from), "SetOnce { value: 100 }");

    let cell_clone = cell_from.clone();
    assert_eq!(cell_from, cell_clone);
}
