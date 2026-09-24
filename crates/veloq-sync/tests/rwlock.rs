#![cfg(not(feature = "loom"))]
use std::task::{Context, RawWaker, RawWakerVTable, Waker};

use veloq_std::{future::Future, sync::Arc, time::Duration};
use veloq_sync::rwlock::RwLock;

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, |_| {}, |_| {}, |_| {});

    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[tokio::test]
async fn test_rwlock_simple() {
    let lock = RwLock::new(0);
    {
        let mut guard = lock.write().await;
        *guard += 1;
    }
    {
        let guard = lock.read().await;
        assert_eq!(*guard, 1);
    }
}

#[tokio::test]
async fn test_rwlock_contention() {
    let lock = Arc::new(RwLock::new(0));
    let mut tasks = vec![];

    // Writers
    for _ in 0..10 {
        let lock = lock.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let mut guard = lock.write().await;
                *guard += 1;
            }
        }));
    }

    // Readers
    for _ in 0..10 {
        let lock = lock.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let guard = lock.read().await;
                assert!(*guard >= 0);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(*lock.read().await, 1000);
}

#[tokio::test]
async fn test_rwlock_downgrade() {
    let lock = Arc::new(RwLock::new(0));

    // 1. Acquire write lock
    let mut w_guard = lock.write().await;
    *w_guard = 42;

    // 2. Spawn a reader that waits
    let lock_clone = lock.clone();
    let reader_handle = tokio::spawn(async move {
        // This should block until downgrade or unlock
        let guard = lock_clone.read().await;
        *guard
    });

    // Ensure reader spawns and likely hits the lock
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Downgrade
    // This transitions from Write -> Read and should wake the reader.
    let r_guard = w_guard.downgrade();

    // 4. Verification
    assert_eq!(*r_guard, 42);

    // The spawned reader should complete because we are now in shared mode
    let val = reader_handle.await.unwrap();
    assert_eq!(val, 42);
}

#[test]
fn test_rwlock_granted_reader_cancel_clears_contention() {
    let lock = RwLock::new(0);
    let guard = lock.try_write().unwrap();
    let mut reader = Box::pin(lock.read());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(reader.as_mut().poll(&mut cx).is_pending());
    drop(guard);
    drop(reader);

    let read_guard = lock.try_read().unwrap();
    drop(read_guard);
    assert!(lock.try_write().is_some());
}

#[test]
fn test_rwlock_granted_reader_cancel_transfers_to_tail() {
    let lock = RwLock::new(0);
    let guard = lock.try_write().unwrap();
    let mut first = Box::pin(lock.read());
    let mut second = Box::pin(lock.read());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(guard);
    drop(first);

    let second_guard = second.as_mut().poll(&mut cx).unwrap_ready();
    drop(second_guard);
    assert!(lock.try_write().is_some());
}

#[test]
fn test_rwlock_downgrade_granted_reader_cancel() {
    let lock = RwLock::new(0);
    let guard = lock.try_write().unwrap();
    let mut reader = Box::pin(lock.read());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(reader.as_mut().poll(&mut cx).is_pending());
    let downgraded = guard.downgrade();
    drop(reader);
    drop(downgraded);
    assert!(lock.try_write().is_some());
}

#[test]
fn test_rwlock_granted_writer_cancel_transfers_to_reader() {
    let lock = RwLock::new(0);
    let guard = lock.try_read().unwrap();
    let mut writer = Box::pin(lock.write());
    let mut reader = Box::pin(lock.read());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(writer.as_mut().poll(&mut cx).is_pending());
    assert!(reader.as_mut().poll(&mut cx).is_pending());
    drop(guard);
    drop(writer);

    let reader_guard = reader.as_mut().poll(&mut cx).unwrap_ready();
    drop(reader_guard);
    assert!(lock.try_write().is_some());
}

trait PollExt<T> {
    fn unwrap_ready(self) -> T;
}

impl<T> PollExt<T> for std::task::Poll<T> {
    fn unwrap_ready(self) -> T {
        match self {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => panic!("future did not become ready"),
        }
    }
}
