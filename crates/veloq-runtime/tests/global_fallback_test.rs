//! Global injector 在目标 NUMA 组没有 idle worker 时的跨组唤醒回归测试。

use veloq_std::{
    future::{Future, poll_fn},
    pin::Pin,
    result::Result,
    sync::{
        NativeArc as Arc, NativeCondvar as Condvar, NativeMutex as Mutex,
        atomic::{NativeAtomicBool as AtomicBool, NativeAtomicUsize as AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
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

const A_STARTED_BIT: usize = 1 << 0;
const B_STARTED_BIT: usize = 1 << 1;
const REQUIRED_STARTED_MASK: usize = A_STARTED_BIT | B_STARTED_BIT;
const OTHER_GROUP_MASK: usize = (1 << 2) | (1 << 3);

struct StartupState {
    started_mask: usize,
    waiter: Option<Waker>,
}

struct StartupRendezvous {
    state: Mutex<StartupState>,
    required_mask: usize,
}

impl StartupRendezvous {
    fn new(required_mask: usize) -> Self {
        Self {
            state: Mutex::new(StartupState {
                started_mask: 0,
                waiter: None,
            }),
            required_mask,
        }
    }

    fn mark_started(&self, bit: usize) -> Option<Waker> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.started_mask & bit != 0 {
            return None;
        }

        state.started_mask |= bit;
        if state.started_mask & self.required_mask == self.required_mask {
            state.waiter.take()
        } else {
            None
        }
    }

    fn poll_until_started(&self, cx: &Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.started_mask & self.required_mask == self.required_mask {
            Poll::Ready(())
        } else {
            state.waiter = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct RecordingWaker {
    state: Arc<GlobalFallbackTestState>,
    worker_id: usize,
}

impl RuntimeWaker for RecordingWaker {
    fn wake(&self) -> Result<(), RuntimeWakeError> {
        self.state.wake_calls[self.worker_id].fetch_add(1, Ordering::AcqRel);
        let worker_bit = 1 << self.worker_id;
        let mut state = self
            .state
            .other_group
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.wake_permits |= worker_bit;
        drop(state);
        self.state.other_group_condvar.notify_all();
        Ok(())
    }
}

struct WakeableFuture {
    started_bit: usize,
    startup: Arc<StartupRendezvous>,
    awoken: Arc<AtomicBool>,
    task_waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for WakeableFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let startup_waker = self.startup.mark_started(self.started_bit);

        if self.awoken.load(Ordering::Acquire) {
            if let Some(waker) = startup_waker {
                waker.wake();
            }
            return Poll::Ready(());
        }

        *self.task_waker.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
        if let Some(waker) = startup_waker {
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
    started_bit: usize,
    startup: Arc<StartupRendezvous>,
    awoken: Arc<AtomicBool>,
    task_waker: Arc<Mutex<Option<Waker>>>,
) -> WakeableFuture {
    WakeableFuture {
        started_bit,
        startup,
        awoken,
        task_waker,
    }
}

struct OtherGroupState {
    parked: usize,
    wake_permits: usize,
    resumed: usize,
    released: bool,
}

struct GlobalFallbackTestState {
    other_group: Mutex<OtherGroupState>,
    other_group_condvar: Condvar,
    wake_calls: Arc<[AtomicUsize]>,
}

type TestExtra = Arc<GlobalFallbackTestState>;

const STAGE_TIMEOUT: Duration = Duration::from_secs(5);

impl GlobalFallbackTestState {
    fn new() -> TestExtra {
        Arc::new(Self {
            other_group: Mutex::new(OtherGroupState {
                parked: 0,
                wake_permits: 0,
                resumed: 0,
                released: false,
            }),
            other_group_condvar: Condvar::new(),
            wake_calls: (0..4)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>()
                .into(),
        })
    }
}

fn keep_target_group_active(shared: &RuntimeShared<TestExtra>) -> RuntimeResult<IdleDecision> {
    let worker_id = shared.worker_id();
    shared.extra_tls.with(|_| {
        if worker_id < 2 {
            Ok(IdleDecision::continue_now())
        } else {
            Ok(IdleDecision::wait(IdleWaitStrategy::block()))
        }
    })
}

fn hold_other_group_workers(
    shared: &RuntimeShared<TestExtra>,
    _wait_strategy: IdleWaitStrategy,
) -> RuntimeResult<()> {
    let fixture = shared.extra_tls.with(Arc::clone);
    let worker_bit = 1 << shared.worker_id();
    let mut state = fixture
        .other_group
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    state.parked |= worker_bit;
    drop(state);
    fixture.other_group_condvar.notify_all();

    let mut state = fixture
        .other_group
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    while !state.released && state.wake_permits & worker_bit == 0 {
        state = fixture
            .other_group_condvar
            .wait(state)
            .unwrap_or_else(|e| e.into_inner());
    }
    if !state.released {
        state.wake_permits &= !worker_bit;
        state.resumed |= worker_bit;
        drop(state);
        fixture.other_group_condvar.notify_all();
    }
    Ok(())
}

struct OtherGroupReleaseGuard {
    state: TestExtra,
}

impl OtherGroupReleaseGuard {
    fn new(state: TestExtra) -> Self {
        Self { state }
    }

    fn release(&self) {
        let mut state = self
            .state
            .other_group
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.released = true;
        drop(state);
        self.state.other_group_condvar.notify_all();
    }
}

impl Drop for OtherGroupReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

fn wait_for_other_group_parked(state: &GlobalFallbackTestState) {
    let deadline = Instant::now() + STAGE_TIMEOUT;
    let mut state_guard = state.other_group.lock().unwrap_or_else(|e| e.into_inner());
    while state_guard.parked & OTHER_GROUP_MASK != OTHER_GROUP_MASK {
        let now = Instant::now();
        if now >= deadline {
            panic_stage_timeout("parked", state, &state_guard);
        }
        let (next_guard, timeout) = state
            .other_group_condvar
            .wait_timeout(state_guard, deadline.duration_since(now));
        state_guard = next_guard.unwrap_or_else(|e| e.into_inner());
        if timeout.timed_out() && state_guard.parked & OTHER_GROUP_MASK != OTHER_GROUP_MASK {
            panic_stage_timeout("parked", state, &state_guard);
        }
    }
}

fn wait_for_other_group_resumed(state: &GlobalFallbackTestState) {
    let deadline = Instant::now() + STAGE_TIMEOUT;
    let mut state_guard = state.other_group.lock().unwrap_or_else(|e| e.into_inner());
    while state_guard.resumed & OTHER_GROUP_MASK == 0 {
        let now = Instant::now();
        if now >= deadline {
            panic_stage_timeout("resumed", state, &state_guard);
        }
        let (next_guard, timeout) = state
            .other_group_condvar
            .wait_timeout(state_guard, deadline.duration_since(now));
        state_guard = next_guard.unwrap_or_else(|e| e.into_inner());
        if timeout.timed_out() && state_guard.resumed & OTHER_GROUP_MASK == 0 {
            panic_stage_timeout("resumed", state, &state_guard);
        }
    }
}

fn panic_stage_timeout(
    stage: &str,
    fixture: &GlobalFallbackTestState,
    state: &OtherGroupState,
) -> ! {
    let wake_calls = fixture
        .wake_calls
        .iter()
        .map(|calls| calls.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    panic!(
        "global fallback stage {stage} timed out: parked={:#x}, resumed={:#x}, \
         wake_permits={:#x}, released={}, wake_calls={wake_calls:?}",
        state.parked, state.resumed, state.wake_permits, state.released,
    );
}

fn other_group_state(state: &GlobalFallbackTestState) -> (usize, usize) {
    let state = state.other_group.lock().unwrap_or_else(|e| e.into_inner());
    (state.parked, state.resumed)
}

fn wake_task(task_waker: &Arc<Mutex<Option<Waker>>>) {
    let waker = task_waker.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[test]
fn global_fallback_wakes_an_idle_worker_in_another_group() {
    let fixture = GlobalFallbackTestState::new();
    let factory_fixture = fixture.clone();
    let scope_fixture = fixture.clone();
    let result = RuntimeBuilder::new()
        .with_worker_count(veloq_std::num::NonZeroUsize::new(4))
        .with_queue_capacity(veloq_std::num::NonZeroUsize::new(1).expect("queue capacity"))
        .with_test_topology(vec![0, 0, 1, 1])
        .with_idle_hook(keep_target_group_active)
        .with_park_hook(hold_other_group_workers)
        .with_worker_factory(move |worker_id: usize, shared: &RuntimeShared<TestExtra>| {
            shared.unparkers()[worker_id]
                .bind(Arc::new(RecordingWaker {
                    state: factory_fixture.clone(),
                    worker_id,
                }))
                .expect("worker waker must bind once");
            factory_fixture.clone()
        })
        .scope(async move |ctx| {
            let _release_guard = OtherGroupReleaseGuard::new(scope_fixture.clone());
            scope!(ctx, async |scope| {
                let startup = Arc::new(StartupRendezvous::new(REQUIRED_STARTED_MASK));
                let a_awoken = Arc::new(AtomicBool::new(false));
                let a_task_waker = Arc::new(Mutex::new(None));
                let b_awoken = Arc::new(AtomicBool::new(false));
                let b_task_waker = Arc::new(Mutex::new(None));

                let a = scope.spawn_boxed(wakeable_future(
                    A_STARTED_BIT,
                    startup.clone(),
                    a_awoken.clone(),
                    a_task_waker.clone(),
                ));
                // The round-robin chooser selects worker 1 for this filler, leaving both wakeable
                // tasks owned by worker 0 in the two-worker target group.
                let _filler = scope.spawn_boxed(async {});
                let b = scope.spawn_boxed(wakeable_future(
                    B_STARTED_BIT,
                    startup.clone(),
                    b_awoken.clone(),
                    b_task_waker.clone(),
                ));

                poll_fn(|cx| startup.poll_until_started(cx)).await;
                wait_for_other_group_parked(&scope_fixture);

                // The main worker is still executing this future, so worker 0 is not idle. The
                // idle hook also keeps worker 1 out of the target group's idle index, while
                // worker 2 or 3 remains available for global fallback.
                for calls in scope_fixture.wake_calls.iter() {
                    calls.store(0, Ordering::Release);
                }
                a_awoken.store(true, Ordering::Release);
                wake_task(&a_task_waker);
                b_awoken.store(true, Ordering::Release);
                wake_task(&b_task_waker);

                wait_for_other_group_resumed(&scope_fixture);
                let (parked, resumed) = other_group_state(&scope_fixture);
                let wake_calls = scope_fixture
                    .wake_calls
                    .iter()
                    .map(|calls| calls.load(Ordering::Acquire))
                    .collect::<Vec<_>>();
                let resumed_other_group = resumed & OTHER_GROUP_MASK;
                let cross_group_wakes = wake_calls[2] + wake_calls[3];
                assert_eq!(parked & OTHER_GROUP_MASK, OTHER_GROUP_MASK);
                assert_eq!(resumed_other_group.count_ones(), 1);
                assert_eq!(cross_group_wakes, 1, "wake_calls={wake_calls:?}");

                _release_guard.release();
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

#[cfg(test)]
mod startup_rendezvous_tests {
    use super::*;

    struct WakerCounters {
        wakes: AtomicUsize,
    }

    static COUNTING_WAKER_VTABLE: veloq_std::task::RawWakerVTable =
        veloq_std::task::RawWakerVTable::new(
            |ptr| unsafe {
                Arc::increment_strong_count(ptr as *const WakerCounters);
                veloq_std::task::RawWaker::new(ptr, &COUNTING_WAKER_VTABLE)
            },
            |ptr| unsafe {
                let counters = Arc::from_raw(ptr as *const WakerCounters);
                counters.wakes.fetch_add(1, Ordering::Relaxed);
            },
            |ptr| unsafe {
                let counters = &*(ptr as *const WakerCounters);
                counters.wakes.fetch_add(1, Ordering::Relaxed);
            },
            |ptr| unsafe {
                drop(Arc::from_raw(ptr as *const WakerCounters));
            },
        );

    fn counting_waker(counters: &Arc<WakerCounters>) -> Waker {
        let raw = Arc::into_raw(Arc::clone(counters)) as *const ();
        unsafe { Waker::from_raw(veloq_std::task::RawWaker::new(raw, &COUNTING_WAKER_VTABLE)) }
    }

    fn poll_startup(rendezvous: &StartupRendezvous, waker: &Waker) -> Poll<()> {
        rendezvous.poll_until_started(&Context::from_waker(waker))
    }

    #[test]
    fn waiter_is_woken_when_second_bit_arrives() {
        let rendezvous = StartupRendezvous::new(REQUIRED_STARTED_MASK);
        let counters = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
        });
        let waker = counting_waker(&counters);

        assert_eq!(poll_startup(&rendezvous, &waker), Poll::Pending);
        let waiter = rendezvous.mark_started(B_STARTED_BIT);
        assert!(waiter.is_none());
        let waiter = rendezvous.mark_started(A_STARTED_BIT).expect("waiter");
        waiter.wake();
        assert_eq!(counters.wakes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn completed_bits_make_later_poll_ready() {
        let rendezvous = StartupRendezvous::new(REQUIRED_STARTED_MASK);
        assert!(rendezvous.mark_started(A_STARTED_BIT).is_none());
        assert!(rendezvous.mark_started(B_STARTED_BIT).is_none());
        assert_eq!(poll_startup(&rendezvous, Waker::noop()), Poll::Ready(()));
    }

    #[test]
    fn waiter_registered_between_the_two_bits_is_woken() {
        let rendezvous = StartupRendezvous::new(REQUIRED_STARTED_MASK);
        assert!(rendezvous.mark_started(A_STARTED_BIT).is_none());
        let waker = Waker::noop().clone();
        assert_eq!(poll_startup(&rendezvous, &waker), Poll::Pending);
        assert!(rendezvous.mark_started(B_STARTED_BIT).is_some());
    }

    #[test]
    fn duplicate_bit_does_not_wake_twice() {
        let rendezvous = StartupRendezvous::new(REQUIRED_STARTED_MASK);
        assert!(rendezvous.mark_started(A_STARTED_BIT).is_none());
        assert!(rendezvous.mark_started(A_STARTED_BIT).is_none());
        assert_eq!(poll_startup(&rendezvous, Waker::noop()), Poll::Pending);
        assert!(rendezvous.mark_started(B_STARTED_BIT).is_some());
        assert!(rendezvous.mark_started(B_STARTED_BIT).is_none());
    }

    #[test]
    fn latest_waiter_replaces_stale_waiter() {
        let rendezvous = StartupRendezvous::new(REQUIRED_STARTED_MASK);
        assert!(rendezvous.mark_started(A_STARTED_BIT).is_none());
        let first = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
        });
        let second = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
        });
        let first_waker = counting_waker(&first);
        let second_waker = counting_waker(&second);

        assert_eq!(poll_startup(&rendezvous, &first_waker), Poll::Pending);
        assert_eq!(poll_startup(&rendezvous, &second_waker), Poll::Pending);
        rendezvous
            .mark_started(B_STARTED_BIT)
            .expect("latest waiter")
            .wake();
        assert_eq!(first.wakes.load(Ordering::Relaxed), 0);
        assert_eq!(second.wakes.load(Ordering::Relaxed), 1);
    }
}
