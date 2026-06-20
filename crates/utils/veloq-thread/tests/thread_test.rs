use core::sync::atomic::{AtomicBool, Ordering};
use veloq_thread::Thread;

#[test]
fn test_spawn_and_join() {
    static CALLED: AtomicBool = AtomicBool::new(false);

    let thread = Thread::spawn(|| {
        CALLED.store(true, Ordering::SeqCst);
    })
    .expect("Failed to spawn Thread");

    thread.join().expect("Failed to join Thread");

    assert!(CALLED.load(Ordering::SeqCst));
}

#[test]
fn test_scope_borrow() {
    let mut val = 42;
    veloq_thread::scope(|s| {
        s.spawn(|| {
            val += 1;
        })
        .expect("Failed to spawn scoped thread");
    });
    assert_eq!(val, 43);
}

#[test]
fn test_scope_join() {
    let res = veloq_thread::scope(|s| {
        let handle = s.spawn(|| 24).expect("Failed to spawn scoped thread");
        handle.join().expect("Failed to join scoped thread")
    });
    assert_eq!(res, 24);
}

#[test]
fn test_scope_nested() {
    let mut val1 = 10;
    let mut val2 = 20;
    veloq_thread::scope(|s1| {
        s1.spawn(|| {
            val1 += 5;
        })
        .expect("Failed to spawn scoped thread 1");

        veloq_thread::scope(|s2| {
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
    veloq_thread::scope(|s| {
        s.spawn(|| {
            veloq_thread::yield_now();
        })
        .expect("Failed to spawn scoped thread");
    });
    veloq_thread::yield_now();
}

#[test]
fn test_thread_abort() {
    let thread = Thread::spawn(|| {
        loop {
            veloq_thread::yield_now();
        }
    })
    .expect("Failed to spawn Thread");

    thread.abort().expect("Failed to abort thread");
}
