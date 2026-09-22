#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::sync::Arc;
use loom::thread;
use veloq_sync::notify::Notify;

#[test]
fn loom_notify_one_basic() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let notify = Arc::new(Notify::new());
        let n1 = notify.clone();
        let n2 = notify.clone();

        let h1 = thread::spawn(move || {
            n1.notify_one();
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                n2.notified().await;
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_notify_permit() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let notify = Arc::new(Notify::new());
        notify.notify_one();
        notify.notify_one();

        let n = notify.clone();
        let h = thread::spawn(move || {
            block_on(async move {
                n.notified().await;
            });
        });

        h.join().unwrap();
    });
}

#[test]
fn loom_notify_two_waiters_cascade() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let notify = Arc::new(Notify::new());
        let n1 = notify.clone();
        let n2 = notify.clone();
        let n3 = notify.clone();

        let h1 = thread::spawn(move || {
            block_on(async move {
                n1.notified().await;
                // Once unparked, cascade notify to the next waiter
                n1.notify_one();
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                n2.notified().await;
                n2.notify_one();
            });
        });

        let h3 = thread::spawn(move || {
            n3.notify_one();
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    });
}

#[test]
fn loom_notify_waiters_semantics() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let notify = Arc::new(Notify::new());
        let n1 = notify.clone();
        let n2 = notify.clone();

        let h = thread::spawn(move || {
            n2.notify_waiters();
        });

        h.join().unwrap();

        // notify_waiters must not store a permit, but notify_one does.
        n1.notify_one();
        block_on(async move {
            n1.notified().await;
        });
    });
}

#[test]
fn loom_notify_drop_restore_permit() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let notify = Arc::new(Notify::new());
        let n1 = notify.clone();
        let n2 = notify.clone();

        let h1 = thread::spawn(move || {
            n1.notify_one();
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let notified = n2.notified();
                drop(notified);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();

        block_on(async move {
            notify.notified().await;
        });
    });
}
