#![cfg(not(feature = "loom"))]

use veloq_std::{
    sync::atomic::{NativeAtomicBool, Ordering},
    thread::{ThreadApi, ThreadErrorKind},
    time::Duration,
};

#[cfg(feature = "std")]
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn test_spawn_and_join() {
    static CALLED: NativeAtomicBool = NativeAtomicBool::new(false);

    let thread = ThreadApi::spawn(|| {
        CALLED.store(true, Ordering::SeqCst);
    })
    .expect("Failed to spawn RawJoinHandle");

    thread.join().expect("Failed to join RawJoinHandle");

    assert!(CALLED.load(Ordering::SeqCst));
}

#[test]
fn test_scope_borrow() {
    let mut val = 42;
    ThreadApi::scope(|s| {
        s.spawn(|| {
            val += 1;
        })
        .expect("Failed to spawn scoped thread");
    });
    assert_eq!(val, 43);
}

#[test]
fn test_scope_join() {
    let res = ThreadApi::scope(|s| {
        let handle = s.spawn(|| 24).expect("Failed to spawn scoped thread");
        handle.join().expect("Failed to join scoped thread")
    });
    assert_eq!(res, 24);
}

#[test]
fn test_scope_nested() {
    let mut val1 = 10;
    let mut val2 = 20;
    ThreadApi::scope(|s1| {
        s1.spawn(|| {
            val1 += 5;
        })
        .expect("Failed to spawn scoped thread 1");

        ThreadApi::scope(|s2| {
            s2.spawn(|| {
                val2 += 5;
            })
            .expect("Failed to spawn scoped thread 2");
        });
    });
    assert_eq!(val1, 15);
    assert_eq!(val2, 25);
}

#[test]
fn test_yield_now() {
    ThreadApi::scope(|s| {
        s.spawn(|| {
            ThreadApi::yield_now().expect("yield should succeed");
        })
        .expect("Failed to spawn scoped thread");
    });
    ThreadApi::yield_now().expect("yield should succeed");
}

#[test]
fn test_thread_abort() {
    let thread = ThreadApi::spawn(|| while ThreadApi::yield_now().is_ok() {})
        .expect("Failed to spawn RawJoinHandle");

    thread.abort().expect("Failed to abort RawJoinHandle");
    let error = thread
        .join()
        .expect_err("aborted thread must report an error");
    assert_eq!(error.kind(), ThreadErrorKind::Aborted);
}

#[test]
fn test_sleep_reports_abort() {
    let thread = ThreadApi::spawn(|| while ThreadApi::sleep(Duration::from_millis(1)).is_ok() {})
        .expect("Failed to spawn RawJoinHandle");

    thread.abort().expect("Failed to abort RawJoinHandle");
    let error = thread
        .join()
        .expect_err("aborted sleep must report an error");
    assert_eq!(error.kind(), ThreadErrorKind::Aborted);
}

#[test]
fn test_spawn_with_return_value() {
    let thread = ThreadApi::spawn(|| 42).expect("Failed to spawn thread");
    let val = thread.join().expect("Failed to join thread");
    assert_eq!(val, 42);
}

#[test]
fn test_thread_abort_error() {
    let thread = ThreadApi::spawn(|| while ThreadApi::yield_now().is_ok() {})
        .expect("Failed to spawn RawJoinHandle");

    thread.abort().expect("Failed to abort RawJoinHandle");
    let res = thread.join();
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ThreadErrorKind::Aborted);
}

#[test]
#[cfg(feature = "std")]
fn test_thread_panic_error() {
    let thread = ThreadApi::spawn(|| {
        panic!("intentional panic");
    })
    .expect("Failed to spawn RawJoinHandle");

    let res = thread.join();
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ThreadErrorKind::Panicked);
}

#[cfg(feature = "std")]
#[test]
fn panicking_reports_the_std_unwind_state() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        assert!(ThreadApi::panicking());
        panic!("intentional panic");
    }));

    assert!(result.is_err());
    assert!(!ThreadApi::panicking());
}

#[cfg(not(feature = "std"))]
#[test]
fn no_std_panic_state_guard_supports_nested_normal_returns() {
    use veloq_std::thread::{PanicState, PanicStateGuard};

    assert_eq!(PanicState::current(), PanicState::NotPanicking);
    let outer = PanicStateGuard::enter();
    assert_eq!(PanicState::current(), PanicState::Panicking);
    let inner = PanicStateGuard::enter();
    assert_eq!(PanicState::current(), PanicState::Panicking);
    inner.finish();
    assert_eq!(PanicState::current(), PanicState::Panicking);
    outer.finish();
    assert_eq!(PanicState::current(), PanicState::NotPanicking);
}
