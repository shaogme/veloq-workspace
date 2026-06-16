use crate::{
    error::{Result as RuntimeResult, RuntimeError},
    runtime::{EnqueuePinnedOutcome, RuntimeCtx, RuntimeShared},
    scope::{GenericScopeCompletion, guard::ScopeTaskGuard},
    task::{
        Arena, GenericArena, GenericTaskHeader, RawScope, RawTask, ScopeRef, ScopeStorage,
        SendBoxedTaskNode, SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
    },
    utils::ownership::{ArcOwnership, Ownership},
};
use diagweave::prelude::*;
use std::{
    alloc::Layout,
    future::{Future, ready},
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::{NonNull, drop_in_place, write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
};
use veloq_atomic_waker::AtomicWaker;
use veloq_storage::{AtomicOptionPtr, AtomicStorage, StateOptionPtr, Storage};

pub(crate) enum RoutedTakeResult<T> {
    Ok(T),
    TaskErr(TaskError),
    RuntimeErr(Report<RuntimeError>),
}

pub(crate) enum RoutedTakeReadyOutcome<'scope_ref, T> {
    Pending,
    Ready(RoutedSpawnReady<'scope_ref, T>),
    TaskErr(TaskError),
    RuntimeErr(Report<RuntimeError>),
}

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> RoutedTakeResult<T>;
    fn reclaim(self: Box<Self>, arena: &dyn Arena);
}

pub(crate) struct RoutedSpawnReady<'scope_ref, T> {
    pub(crate) task: SendTaskRef,
    pub(crate) access: Box<dyn RoutedTaskAccess<T> + 'scope_ref>,
}

pub(crate) enum RoutedSpawnOutcomeInner<'scope_ref, T> {
    Ready(RoutedSpawnReady<'scope_ref, T>),
    FailedTask(TaskError),
    FailedRuntime(Report<RuntimeError>),
}

pub(crate) struct RoutedJobCell<F> {
    job: Option<F>,
}

impl<F> RoutedJobCell<F> {
    pub(crate) fn new(job: F) -> Self {
        Self { job: Some(job) }
    }

    pub(crate) fn take(&mut self) -> RuntimeResult<F> {
        self.job.take().ok_or_else(|| {
            RuntimeError::InvariantViolation {
                site: "RoutedJobCell::take",
                detail: "job has already been taken",
            }
            .to_report()
        })
    }
}

struct SpawnToAccess<'scope_ref, T, S_> {
    task: &'scope_ref S_,
    marker: PhantomData<(T,)>,
}

impl<'scope_ref, T, S_> RoutedTaskAccess<T> for SpawnToAccess<'scope_ref, T, S_>
where
    S_: SendTask<T> + Sized + 'scope_ref,
{
    fn take_result(&self) -> RoutedTakeResult<T> {
        match self.task.take_result() {
            Some(Ok(value)) => RoutedTakeResult::Ok(value),
            Some(Err(err)) => RoutedTakeResult::TaskErr(err),
            None => RoutedTakeResult::RuntimeErr(
                RuntimeError::TaskResultUnavailable {
                    stage: "RoutedTaskAccess::take_result(SpawnToAccess)",
                }
                .to_report(),
            ),
        }
    }

    fn reclaim(self: Box<Self>, _arena: &dyn Arena) {}
}

unsafe impl<'scope_ref, T, S_> Send for SpawnToAccess<'scope_ref, T, S_> where
    S_: SendTask<T> + Sized + 'scope_ref
{
}

pub(crate) fn make_spawn_to_access<'scope_ref, T, S_>(
    task: &'scope_ref S_,
) -> Box<dyn RoutedTaskAccess<T> + 'scope_ref>
where
    T: 'scope_ref,
    S_: SendTask<T> + Sized + 'scope_ref,
{
    Box::new(SpawnToAccess {
        task,
        marker: PhantomData,
    })
}

struct BoxedTaskAccess<'scope_ref, T, Fut> {
    node: &'scope_ref SendBoxedTaskNode<T, Fut>,
    marker: PhantomData<T>,
}

impl<'scope_ref, T, Fut> RoutedTaskAccess<T> for BoxedTaskAccess<'scope_ref, T, Fut>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    fn take_result(&self) -> RoutedTakeResult<T> {
        match self.node.take_result() {
            Some(Ok(value)) => RoutedTakeResult::Ok(value),
            Some(Err(err)) => RoutedTakeResult::TaskErr(err),
            None => RoutedTakeResult::RuntimeErr(
                RuntimeError::TaskResultUnavailable {
                    stage: "RoutedTaskAccess::take_result(BoxedTaskAccess)",
                }
                .to_report(),
            ),
        }
    }

    fn reclaim(self: Box<Self>, arena: &dyn Arena) {
        let layout = Layout::new::<SendBoxedTaskNode<T, Fut>>();
        unsafe {
            arena.drop_object_raw(self.node as *const _ as *mut u8, layout);
        }
    }
}

unsafe impl<'scope_ref, T, Fut> Send for BoxedTaskAccess<'scope_ref, T, Fut>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
}

pub(crate) fn make_boxed_task_access<'scope_ref, T, Fut>(
    node: &'scope_ref SendBoxedTaskNode<T, Fut>,
) -> Box<dyn RoutedTaskAccess<T> + 'scope_ref>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    Box::new(BoxedTaskAccess {
        node,
        marker: PhantomData,
    })
}

pub(crate) struct RoutedSpawnState<'scope_ref, T> {
    outcome: AtomicOptionPtr<RoutedSpawnOutcomeInner<'scope_ref, T>>,
    cancel_requested: AtomicBool,
    waker: AtomicWaker,
}

pub(crate) fn new_failed_routed_state<'scope_ref, T>(
    err: Report<RuntimeError>,
) -> Arc<RoutedSpawnState<'scope_ref, T>> {
    let state = RoutedSpawnState::new();
    state.fail_runtime(err);
    state
}

impl<'scope_ref, T> RoutedSpawnState<'scope_ref, T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: AtomicOptionPtr::new(None),
            cancel_requested: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn request_cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        self.waker.wake();
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
    }

    pub(crate) fn has_failed_outcome(&self) -> bool {
        if let Some(raw) = self.outcome.load(Ordering::Acquire) {
            unsafe {
                matches!(
                    raw.as_ref(),
                    RoutedSpawnOutcomeInner::FailedTask(_)
                        | RoutedSpawnOutcomeInner::FailedRuntime(_)
                )
            }
        } else {
            false
        }
    }

    fn set_outcome(&self, inner: RoutedSpawnOutcomeInner<'scope_ref, T>) {
        let boxed = Box::new(inner);
        let Some(raw) = NonNull::new(Box::into_raw(boxed)) else {
            return;
        };
        match self
            .outcome
            .compare_exchange(None, Some(raw), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.waker.wake();
            }
            Err(_) => {
                // If it's already set (e.g. Taken or Cancelled), drop the box to prevent leak.
                unsafe {
                    let _ = Box::from_raw(raw.as_ptr());
                }
            }
        }
    }

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'scope_ref, T>) {
        self.set_outcome(RoutedSpawnOutcomeInner::Ready(ready));
    }

    pub(crate) fn fail_task(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcomeInner::FailedTask(err));
    }

    pub(crate) fn fail_runtime(&self, err: Report<RuntimeError>) {
        self.set_outcome(RoutedSpawnOutcomeInner::FailedRuntime(err));
    }

    pub(crate) fn try_take_ready(&self) -> RoutedTakeReadyOutcome<'scope_ref, T> {
        if let Some(raw) = self.outcome.swap(None, Ordering::AcqRel) {
            let inner = unsafe { Box::from_raw(raw.as_ptr()) };
            match *inner {
                RoutedSpawnOutcomeInner::Ready(ready) => RoutedTakeReadyOutcome::Ready(ready),
                RoutedSpawnOutcomeInner::FailedTask(err) => RoutedTakeReadyOutcome::TaskErr(err),
                RoutedSpawnOutcomeInner::FailedRuntime(err) => {
                    RoutedTakeReadyOutcome::RuntimeErr(err)
                }
            }
        } else {
            RoutedTakeReadyOutcome::Pending
        }
    }

    pub(crate) fn cancel_ready_task_if_any(&self) {
        if let Some(raw) = self.outcome.load(Ordering::Acquire) {
            let inner = unsafe { raw.as_ref() };
            if let RoutedSpawnOutcomeInner::Ready(ready) = inner {
                ready.task.header().cancel();
            }
        }
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

impl<'scope_ref, T> Drop for RoutedSpawnState<'scope_ref, T> {
    fn drop(&mut self) {
        if let Some(raw) = self.outcome.swap(None, Ordering::Acquire) {
            unsafe {
                let _ = Box::from_raw(raw.as_ptr());
            }
        }
    }
}

unsafe impl<'scope_ref, T> Send for RoutedSpawnState<'scope_ref, T> where T: Send {}
unsafe impl<'scope_ref, T> Sync for RoutedSpawnState<'scope_ref, T> where T: Send {}

pub(crate) fn dispatch_routed<'rt, 'scope_ref, S: ScopeStorage, O: Ownership, T, F, TExtra>(
    context: &RuntimeCtx<'rt, TExtra>,
    mut guard: ScopeTaskGuard<S, O>,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<GenericScopeCompletion<S, O>>: Send,
    F: FnOnce(&mut ScopeTaskGuard<S, O>) + Send + 'scope_ref,
    T: Send + 'scope_ref,
{
    let completion = guard.completion().clone();
    let state_for_route = state.clone();

    if let Err(err) = context.route_to(worker_id, move || {
        let result = catch_unwind(AssertUnwindSafe(|| job(&mut guard)));

        if let Err(panic_err) = result {
            completion.report_panic(panic_err);
            completion.cancel();
            state_for_route.fail_task(TaskError::Panic);
            if guard.is_armed() {
                guard.settle();
            } else {
                completion.settle_task();
            }
        }
        ready(())
    }) {
        state.fail_runtime(err);
    }
}

pub(crate) fn handle_enqueue_pinned_outcome<H: Storage, S: ScopeStorage, O: Ownership>(
    guard: &mut ScopeTaskGuard<S, O>,
    header: &GenericTaskHeader<H>,
    outcome: EnqueuePinnedOutcome,
) -> bool {
    match outcome {
        EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => true,
        EnqueuePinnedOutcome::AbortedAcknowledged | EnqueuePinnedOutcome::AlreadySettled => false,
        EnqueuePinnedOutcome::NeedsCallerSettle => {
            guard.settle_enqueue_failure(header);
            false
        }
    }
}

pub(crate) fn install_routed_pinned_task<'scope_ref, 'rt, T, Fut, TExtra>(
    runtime: &'rt RuntimeShared<TExtra>,
    arena: &GenericArena<AtomicStorage>,
    guard: &mut ScopeTaskGuard<AtomicStorage, ArcOwnership>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    future: Fut,
) where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    let scope_ref = unsafe {
        let non_null = RawScope::clone_raw(guard.completion_ref());
        ScopeRef::new(non_null)
    };
    let node = SendBoxedTaskNode::new(future);
    let node_header_ptr = &node.header as *const GenericTaskHeader<AtomicStorage>;
    unsafe {
        (*node_header_ptr).initialize(&runtime.base, worker_id, scope_ref);
    }
    let layout = Layout::new::<SendBoxedTaskNode<T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<SendBoxedTaskNode<T, Fut>>(
            layout,
            Some(|ptr| drop_in_place(ptr as *mut SendBoxedTaskNode<T, Fut>)),
        )
    };
    let Some(node_ptr) = node_ptr else {
        state.fail_task(TaskError::Panic);
        guard.settle();
        return;
    };
    let node_ptr = node_ptr.as_ptr() as *mut SendBoxedTaskNode<T, Fut>;
    unsafe { write(node_ptr, node) };

    let node_ref = unsafe { &*node_ptr };
    node_ref.header().set_pinned();

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header_ptr = task_ref.header() as *const GenericTaskHeader<AtomicStorage>;
    let task_ctx = unsafe { SendTaskRef::from_header(header_ptr) };
    let header = task_ref.header();

    guard.handoff_to(header);

    let outcome = runtime.enqueue_pinned(worker_id, task_ctx);
    if !handle_enqueue_pinned_outcome(guard, header, outcome) {
        unsafe { arena.drop_object_raw(node_ptr as *mut u8, layout) };
        state.fail_task(TaskError::Panic);
        return;
    }

    if state.is_cancel_requested() {
        header.cancel();
    }

    let task_ready = unsafe {
        SendTaskRef::from_header(task_ref.header() as *const GenericTaskHeader<AtomicStorage>)
    };

    state.set_ready(RoutedSpawnReady {
        task: task_ready,
        access: make_boxed_task_access(node_ref),
    });
}
