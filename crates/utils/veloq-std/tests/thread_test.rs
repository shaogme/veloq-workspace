#![cfg(not(feature = "loom"))]

use veloq_std::{
    sync::atomic::{CoreAtomicBool, Ordering},
    thread::spawn,
};

#[test]
fn test_spawn_and_join() {
    static CALLED: CoreAtomicBool = CoreAtomicBool::new(false);

    let thread = spawn(|| {
        CALLED.store(true, Ordering::SeqCst);
    })
    .expect("Failed to spawn RawJoinHandle");

    thread.join().expect("Failed to join RawJoinHandle");

    assert!(CALLED.load(Ordering::SeqCst));
}

#[test]
fn test_scope_borrow() {
    let mut val = 42;
    veloq_std::thread::scope(|s| {
        s.spawn(|| {
            val += 1;
        })
        .expect("Failed to spawn scoped thread");
    });
    assert_eq!(val, 43);
}

#[test]
fn test_scope_join() {
    let res = veloq_std::thread::scope(|s| {
        let handle = s.spawn(|| 24).expect("Failed to spawn scoped thread");
        handle.join().expect("Failed to join scoped thread")
    });
    assert_eq!(res, 24);
}

#[test]
fn test_scope_nested() {
    let mut val1 = 10;
    let mut val2 = 20;
    veloq_std::thread::scope(|s1| {
        s1.spawn(|| {
            val1 += 5;
        })
        .expect("Failed to spawn scoped thread 1");

        veloq_std::thread::scope(|s2| {
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
    veloq_std::thread::scope(|s| {
        s.spawn(|| {
            let _ = veloq_std::thread::yield_now();
        })
        .expect("Failed to spawn scoped thread");
    });
    let _ = veloq_std::thread::yield_now();
}

#[test]
fn test_thread_abort() {
    let thread = spawn(|| while veloq_std::thread::yield_now().is_ok() {})
        .expect("Failed to spawn RawJoinHandle");

    thread.abort().expect("Failed to abort RawJoinHandle");
    let _ = thread.join();
}

#[test]
fn test_spawn_with_return_value() {
    let thread = spawn(|| 42).expect("Failed to spawn thread");
    let val = thread.join().expect("Failed to join thread");
    assert_eq!(val, 42);
}

#[test]
fn test_thread_abort_error() {
    let thread = spawn(|| while veloq_std::thread::yield_now().is_ok() {})
        .expect("Failed to spawn RawJoinHandle");

    thread.abort().expect("Failed to abort RawJoinHandle");
    let res = thread.join();
    assert!(res.is_err());
    assert_eq!(
        res.unwrap_err().kind(),
        veloq_std::thread::ThreadErrorKind::Aborted
    );
}

#[test]
fn test_thread_panic_error() {
    let thread = spawn(|| {
        panic!("intentional panic");
    })
    .expect("Failed to spawn RawJoinHandle");

    let res = thread.join();
    assert!(res.is_err());
    assert_eq!(
        res.unwrap_err().kind(),
        veloq_std::thread::ThreadErrorKind::Panicked
    );
}
