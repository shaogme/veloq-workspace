#![cfg(feature = "loom")]

use loom::{future::block_on, sync::Arc, thread};
use veloq_sync::{condvar::Condvar, mutex::Mutex};

#[test]
fn loom_condvar_notify_one() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair1 = pair.clone();
        let pair2 = pair.clone();

        let h1 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair1;
                let mut guard = lock.lock().await;
                while !*guard {
                    guard = cvar.wait(guard).await;
                }
                *guard = false;
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair2;
                let mut guard = lock.lock().await;
                *guard = true;
                cvar.notify_one();
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_condvar_notify_all() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let pair = Arc::new((Mutex::new(0usize), Condvar::new()));
        let pair1 = pair.clone();
        let pair2 = pair.clone();
        let pair3 = pair.clone();

        let h1 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair1;
                let mut val = lock.lock().await;
                while *val < 1 {
                    val = cvar.wait(val).await;
                }
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair2;
                let mut val = lock.lock().await;
                while *val < 1 {
                    val = cvar.wait(val).await;
                }
            });
        });

        let h3 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair3;
                let mut val = lock.lock().await;
                *val = 1;
                cvar.notify_all();
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    });
}

#[test]
fn loom_condvar_wait_while() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair1 = pair.clone();
        let pair2 = pair.clone();

        let h1 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair1;
                let guard = lock.lock().await;
                let mut guard = cvar.wait_while(guard, |flag| !*flag).await;
                *guard = false;
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair2;
                let mut guard = lock.lock().await;
                *guard = true;
                cvar.notify_one();
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_condvar_cancellation() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair1 = pair.clone();
        let pair2 = pair.clone();

        let h1 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair1;
                let guard = lock.lock().await;
                let wait_fut = cvar.wait(guard);
                drop(wait_fut);
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let (lock, cvar) = &*pair2;
                let mut guard = lock.lock().await;
                *guard = true;
                cvar.notify_one();
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();

        block_on(async move {
            let guard = pair.0.lock().await;
            assert!(*guard);
        });
    });
}
