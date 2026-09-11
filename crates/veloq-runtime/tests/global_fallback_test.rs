//! Global injector 在目标 NUMA 组没有 idle worker 时的跨组唤醒回归测试。

use veloq_std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        NativeArc as Arc, NativeCondvar as Condvar, NativeMutex as Mutex,
        atomic::{NativeAtomicBool as AtomicBool, NativeAtomicUsize as AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    vec,
};

use veloq_runtime::{
    error::{Result as RuntimeResult, RuntimeWakeError},
    runtime::{
        RuntimeBuilder, RuntimeShared,
        context::{IdleDecision, IdleWaitStrategy},
        primitives::RuntimeWaker,
    },
    scope,
};

struct RecordingWaker {
    calls: Arc<[AtomicUsize]>,
    worker_id: usize,
}

impl RuntimeWaker for RecordingWaker {
    fn wake(&self) -> veloq_std::result::Result<(), RuntimeWakeError> {
        self.calls[self.worker_id].fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct WakeableFuture {
    started: Arc<AtomicBool>,
    awoken: Arc<AtomicBool>,
    task_waker: Arc<Mutex<Option<Waker>>>,
    ready_waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for WakeableFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.awoken.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        *self.task_waker.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
        if !self.started.swap(true, Ordering::AcqRel)
            && let Some(waker) = self
                .ready_waker
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
        {
            waker.wake();
        }
        if self.awoken.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn wakeable_future(
    started: Arc<AtomicBool>,
    awoken: Arc<AtomicBool>,
    task_waker: Arc<Mutex<Option<Waker>>>,
    ready_waker: Arc<Mutex<Option<Waker>>>,
) -> WakeableFuture {
    WakeableFuture {
        started,
        awoken,
        task_waker,
        ready_waker,
    }
}

fn keep_target_group_active(shared: &RuntimeShared<()>) -> RuntimeResult<IdleDecision> {
    if shared.worker_id() < 2 {
        Ok(IdleDecision::continue_now())
    } else {
        Ok(IdleDecision::wait(IdleWaitStrategy::block()))
    }
}

const OTHER_GROUP_MASK: usize = (1 << 2) | (1 << 3);

struct OtherGroupState {
    parked: usize,
    released: bool,
}

static OTHER_GROUP_STATE: Mutex<OtherGroupState> = Mutex::new(OtherGroupState {
    parked: 0,
    released: false,
});
static OTHER_GROUP_CONDVAR: Condvar = Condvar::new();

fn hold_other_group_workers(
    shared: &RuntimeShared<()>,
    _wait_strategy: IdleWaitStrategy,
) -> RuntimeResult<()> {
    let mut state = OTHER_GROUP_STATE.lock().unwrap_or_else(|e| e.into_inner());
    state.parked |= 1 << shared.worker_id();
    OTHER_GROUP_CONDVAR.notify_all();
    while !state.released {
        state = OTHER_GROUP_CONDVAR
            .wait(state)
            .unwrap_or_else(|e| e.into_inner());
    }
    Ok(())
}

struct OtherGroupReleaseGuard;

impl Drop for OtherGroupReleaseGuard {
    fn drop(&mut self) {
        let mut state = OTHER_GROUP_STATE.lock().unwrap_or_else(|e| e.into_inner());
        state.released = true;
        OTHER_GROUP_CONDVAR.notify_all();
    }
}

#[test]
fn global_fallback_wakes_an_idle_worker_in_another_group() {
    {
        let mut state = OTHER_GROUP_STATE.lock().unwrap_or_else(|e| e.into_inner());
        state.parked = 0;
        state.released = false;
    }
    let calls: Arc<[AtomicUsize]> = (0..4)
        .map(|_| AtomicUsize::new(0))
        .collect::<Vec<_>>()
        .into();
    let result = RuntimeBuilder::new()
        .with_worker_count(veloq_std::num::NonZeroUsize::new(4))
        .with_queue_capacity(veloq_std::num::NonZeroUsize::new(1).expect("queue capacity"))
        .with_test_topology(vec![0, 0, 1, 1])
        .with_idle_hook(keep_target_group_active)
        .with_park_hook(hold_other_group_workers)
        .with_worker_factory({
            let calls = calls.clone();
            move |worker_id: usize, shared: &RuntimeShared<()>| {
                shared.unparkers()[worker_id]
                    .bind(Arc::new(RecordingWaker {
                        calls: calls.clone(),
                        worker_id,
                    }))
                    .expect("worker waker must bind once");
            }
        })
        .scope(async |ctx| {
            let _release_guard = OtherGroupReleaseGuard;
            scope!(ctx, async |scope| {
                let ready_waker = Arc::new(Mutex::new(None));
                let a_started = Arc::new(AtomicBool::new(false));
                let a_awoken = Arc::new(AtomicBool::new(false));
                let a_task_waker = Arc::new(Mutex::new(None));
                let b_started = Arc::new(AtomicBool::new(false));
                let b_awoken = Arc::new(AtomicBool::new(false));
                let b_task_waker = Arc::new(Mutex::new(None));

                let a = scope.spawn_boxed(wakeable_future(
                    a_started.clone(),
                    a_awoken.clone(),
                    a_task_waker.clone(),
                    ready_waker.clone(),
                ));
                // The round-robin chooser selects worker 1 for this filler, leaving both wakeable
                // tasks owned by worker 0 in the two-worker target group.
                let _filler = scope.spawn_boxed(async {});
                let b = scope.spawn_boxed(wakeable_future(
                    b_started.clone(),
                    b_awoken.clone(),
                    b_task_waker.clone(),
                    ready_waker.clone(),
                ));

                poll_fn(|cx| {
                    if a_started.load(Ordering::Acquire) && b_started.load(Ordering::Acquire) {
                        Poll::Ready(())
                    } else {
                        *ready_waker.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;

                {
                    let mut state = OTHER_GROUP_STATE
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    while state.parked & OTHER_GROUP_MASK != OTHER_GROUP_MASK {
                        state = OTHER_GROUP_CONDVAR
                            .wait(state)
                            .unwrap_or_else(|e| e.into_inner());
                    }
                }

                // The main worker is still executing this future, so worker 0 is not idle. The
                // idle hook also keeps worker 1 out of the target group's idle index, while
                // worker 2 or 3 remains available for global fallback.
                for calls in calls.iter() {
                    calls.store(0, Ordering::Relaxed);
                }
                a_awoken.store(true, Ordering::Release);
                if let Some(waker) = a_task_waker
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                {
                    waker.wake_by_ref();
                }
                b_awoken.store(true, Ordering::Release);
                if let Some(waker) = b_task_waker
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                {
                    waker.wake_by_ref();
                }
                let calls_after_wakes = calls
                    .iter()
                    .map(|calls| calls.load(Ordering::Relaxed))
                    .collect::<Vec<_>>();
                let backlog_after_wakes = scope.shared().global_queue_backlog();
                let cross_group_wakes =
                    calls[2].load(Ordering::Relaxed) + calls[3].load(Ordering::Relaxed);
                {
                    let mut state = OTHER_GROUP_STATE
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    state.released = true;
                    OTHER_GROUP_CONDVAR.notify_all();
                }
                assert_eq!(
                    cross_group_wakes,
                    1,
                    "one global publication must wake exactly one fallback worker; \
                     calls_after_wakes={calls_after_wakes:?}, backlog_after_wakes={backlog_after_wakes}"
                );
                let _ = a.await;
                let _ = b.await;
                assert_eq!(scope.shared().global_queue_backlog(), 0);
            })
            .await
            .expect("send scope");
        });

    assert!(
        result.is_ok(),
        "global fallback runtime should complete: {result:?}"
    );
}

#[test]
fn rejects_invalid_synthetic_topology() {
    let length_mismatch = RuntimeBuilder::new()
        .with_worker_count(veloq_std::num::NonZeroUsize::new(2))
        .with_test_topology(vec![0])
        .scope(async |_ctx| ());
    assert!(length_mismatch.is_err());

    let invalid_group = RuntimeBuilder::new()
        .with_worker_count(veloq_std::num::NonZeroUsize::new(2))
        .with_test_topology(vec![0, 2])
        .scope(async |_ctx| ());
    assert!(invalid_group.is_err());
}
