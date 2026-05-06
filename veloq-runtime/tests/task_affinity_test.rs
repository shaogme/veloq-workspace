use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use veloq_runtime::runtime::Runtime;
use veloq_runtime::runtime::current_worker_id;
use veloq_runtime::scope;
use veloq_runtime::task::{TaskAffinityGuard, with_task_affinity, yield_now};

struct ManualGateState {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

#[derive(Clone)]
struct ManualGate {
    state: Arc<ManualGateState>,
}

impl ManualGate {
    fn new() -> Self {
        Self {
            state: Arc::new(ManualGateState {
                ready: AtomicBool::new(false),
                waker: Mutex::new(None),
            }),
        }
    }

    fn wait(&self) -> ManualGateFuture {
        ManualGateFuture {
            state: Arc::clone(&self.state),
        }
    }

    fn open(&self) {
        self.state.ready.store(true, Ordering::Release);
        if let Some(waker) = self.state.waker.lock().expect("gate poisoned").take() {
            waker.wake();
        }
    }
}

struct ManualGateFuture {
    state: Arc<ManualGateState>,
}

impl Future for ManualGateFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *self.state.waker.lock().expect("gate poisoned") = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[test]
fn send_task_affinity_guard_keeps_resume_on_owner_worker() {
    let runtime = Runtime::builder()
        .worker_count(std::num::NonZeroUsize::new(2).expect("2 is non-zero"))
        .build();

    runtime.block_on(async {
        scope!(s, {
            let gate = ManualGate::new();
            let started = Arc::new(AtomicBool::new(false));
            let blocker_started = Arc::new(AtomicBool::new(false));
            let first_worker = Arc::new(AtomicUsize::new(usize::MAX));
            let resumed_worker = Arc::new(AtomicUsize::new(usize::MAX));

            let affinity_handle = s.spawn_boxed_to(1, {
                let gate = gate.clone();
                let started = Arc::clone(&started);
                let first_worker = Arc::clone(&first_worker);
                let resumed_worker = Arc::clone(&resumed_worker);
                async move {
                    let first = current_worker_id();
                    first_worker.store(first, Ordering::Release);

                    let _guard = TaskAffinityGuard::enter();
                    started.store(true, Ordering::Release);
                    gate.wait().await;

                    let resumed = current_worker_id();
                    resumed_worker.store(resumed, Ordering::Release);
                    resumed
                }
            });

            while !started.load(Ordering::Acquire) {
                yield_now().await;
            }

            assert_eq!(first_worker.load(Ordering::Acquire), 1);

            let blocker = s.spawn_boxed_to(1, {
                let blocker_started = Arc::clone(&blocker_started);
                async move {
                    assert_eq!(current_worker_id(), 1);
                    blocker_started.store(true, Ordering::Release);
                    std::thread::sleep(Duration::from_millis(150));
                }
            });

            while !blocker_started.load(Ordering::Acquire) {
                yield_now().await;
            }

            gate.open();

            let resumed = affinity_handle
                .await
                .expect("affinity task should complete");
            blocker.await.expect("blocker task should complete");

            assert_eq!(resumed, 1);
            assert_eq!(resumed_worker.load(Ordering::Acquire), 1);
        });
    });
}

#[test]
fn with_task_affinity_keeps_resume_on_owner_worker() {
    let runtime = Runtime::builder()
        .worker_count(std::num::NonZeroUsize::new(2).expect("2 is non-zero"))
        .build();

    runtime.block_on(async {
        scope!(s, {
            let gate = ManualGate::new();
            let started = Arc::new(AtomicBool::new(false));
            let blocker_started = Arc::new(AtomicBool::new(false));
            let first_worker = Arc::new(AtomicUsize::new(usize::MAX));
            let resumed_worker = Arc::new(AtomicUsize::new(usize::MAX));

            let affinity_handle = s.spawn_boxed_to(1, {
                let gate = gate.clone();
                let started = Arc::clone(&started);
                let first_worker = Arc::clone(&first_worker);
                let resumed_worker = Arc::clone(&resumed_worker);
                with_task_affinity(async move {
                    let first = current_worker_id();
                    first_worker.store(first, Ordering::Release);

                    started.store(true, Ordering::Release);
                    gate.wait().await;

                    let resumed = current_worker_id();
                    resumed_worker.store(resumed, Ordering::Release);
                    resumed
                })
            });

            while !started.load(Ordering::Acquire) {
                yield_now().await;
            }

            assert_eq!(first_worker.load(Ordering::Acquire), 1);

            let blocker = s.spawn_boxed_to(1, {
                let blocker_started = Arc::clone(&blocker_started);
                async move {
                    assert_eq!(current_worker_id(), 1);
                    blocker_started.store(true, Ordering::Release);
                    std::thread::sleep(Duration::from_millis(150));
                }
            });

            while !blocker_started.load(Ordering::Acquire) {
                yield_now().await;
            }

            gate.open();

            let resumed = affinity_handle
                .await
                .expect("affinity task should complete");
            blocker.await.expect("blocker task should complete");

            assert_eq!(resumed, 1);
            assert_eq!(resumed_worker.load(Ordering::Acquire), 1);
        });
    });
}
