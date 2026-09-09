use crate::{
    error::{Result as RuntimeResult, RuntimeError},
    runtime::{EnqueuePinnedOutcome, RuntimeCtx, RuntimeShared},
    scope::{GenericScopeCompletion, SendPtr, guard::ScopeTaskGuard},
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
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::{NonNull, drop_in_place, write},
    sync::Arc,
    task::Waker,
};
use veloq_storage::{AtomicLock, AtomicStorage, StateLock};
use veloq_waker::MwsrWaker;

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

enum RoutedOutcomeCell<'scope_ref, T> {
    Pending {
        cancel_requested: bool,
    },
    Ready {
        ready: RoutedSpawnReady<'scope_ref, T>,
        cancel_requested: bool,
    },
    FailedTask {
        err: TaskError,
        cancel_requested: bool,
    },
    FailedRuntime {
        err: Report<RuntimeError>,
        cancel_requested: bool,
    },
    Consumed {
        cancel_requested: bool,
        failed: bool,
    },
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
                detail: "job has already been taken".into(),
            }
            .to_report()
        })
    }
}

/// `spawn_boxed_to` 中 job cell 的**唯一**所有者。
///
/// cell 分配在 scope arena 上，所有权随被路由的闭包一起移交给目标 worker：无论闭包
/// 正常结束、提前返回还是 unwind，cell 都由本守卫释放；若 `route_to` 投递失败，闭包
/// （连同守卫）会被原地丢弃，同样完成释放。
///
/// 这样就不再需要主线程按「结果状态」反推所有权 —— 结果状态并不携带「cell 归谁释放」
/// 的信息，避免了由此引发的双重释放问题。
pub(crate) struct RoutedJobCellOwner<F> {
    arena: SendPtr<GenericArena<AtomicStorage>>,
    cell: SendPtr<RoutedJobCell<F>>,
    layout: Layout,
    released: bool,
}

impl<F> RoutedJobCellOwner<F> {
    /// # Safety
    ///
    /// `cell` 必须是 `arena` 上刚写入完成的 `RoutedJobCell<F>`，`layout` 与其分配布局
    /// 一致，且 `arena` 的存活期必须覆盖本守卫。
    pub(crate) unsafe fn new(
        arena: &GenericArena<AtomicStorage>,
        cell: NonNull<RoutedJobCell<F>>,
        layout: Layout,
    ) -> Self {
        Self {
            arena: SendPtr::new(NonNull::from(arena)),
            cell: SendPtr::new(cell),
            layout,
            released: false,
        }
    }

    /// 取出 job 并立刻释放 cell（cell 的用途已尽，不必等到守卫析构）。
    pub(crate) fn take_job(&mut self) -> RuntimeResult<F> {
        let job = unsafe { self.cell.as_mut().take() };
        self.release();
        job
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        unsafe {
            self.arena
                .as_ref()
                .drop_object_raw(self.cell.as_ptr() as *mut u8, self.layout);
        }
    }
}

impl<F> Drop for RoutedJobCellOwner<F> {
    fn drop(&mut self) {
        self.release();
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
    outcome: AtomicLock<RoutedOutcomeCell<'scope_ref, T>>,
    waker: MwsrWaker,
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
            outcome: AtomicLock::new(RoutedOutcomeCell::Pending {
                cancel_requested: false,
            }),
            waker: MwsrWaker::new(),
        })
    }

    pub(crate) fn request_cancel(&self) {
        let wake_join = {
            let mut outcome = self.outcome.lock();
            match &mut *outcome {
                RoutedOutcomeCell::Pending { cancel_requested } => {
                    *cancel_requested = true;
                    true
                }
                RoutedOutcomeCell::Ready {
                    ready,
                    cancel_requested,
                } => {
                    *cancel_requested = true;
                    // The lock protects the `ready` owner until cancellation has finished. A
                    // concurrent take can therefore not reclaim the task while this header
                    // access is in progress.
                    ready.task.header().cancel_and_wake();
                    true
                }
                RoutedOutcomeCell::FailedTask {
                    cancel_requested, ..
                }
                | RoutedOutcomeCell::FailedRuntime {
                    cancel_requested, ..
                }
                | RoutedOutcomeCell::Consumed {
                    cancel_requested, ..
                } => {
                    *cancel_requested = true;
                    false
                }
            }
        };

        if wake_join {
            self.waker.wake();
        }
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        let outcome = self.outcome.lock();
        match &*outcome {
            RoutedOutcomeCell::Pending { cancel_requested }
            | RoutedOutcomeCell::Ready {
                cancel_requested, ..
            }
            | RoutedOutcomeCell::FailedTask {
                cancel_requested, ..
            }
            | RoutedOutcomeCell::FailedRuntime {
                cancel_requested, ..
            }
            | RoutedOutcomeCell::Consumed {
                cancel_requested, ..
            } => *cancel_requested,
        }
    }

    pub(crate) fn has_failed_outcome(&self) -> bool {
        let outcome = self.outcome.lock();
        matches!(
            &*outcome,
            RoutedOutcomeCell::FailedTask { .. }
                | RoutedOutcomeCell::FailedRuntime { .. }
                | RoutedOutcomeCell::Consumed { failed: true, .. }
        )
    }

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'scope_ref, T>) {
        let should_wake = {
            let mut outcome = self.outcome.lock();
            let RoutedOutcomeCell::Pending { cancel_requested } = &*outcome else {
                debug_assert!(false, "routed outcome published more than once");
                return;
            };
            let cancel_requested = *cancel_requested;
            if cancel_requested {
                // The task has already been enqueued. Do not wake it here: the worker queue will
                // observe the cancellation, while `request_cancel` handles the ready case with a
                // full cancel-and-wake operation.
                ready.task.header().cancel();
            }
            *outcome = RoutedOutcomeCell::Ready {
                ready,
                cancel_requested,
            };
            true
        };

        if should_wake {
            self.waker.wake();
        }
    }

    pub(crate) fn fail_task(&self, err: TaskError) {
        let should_wake = {
            let mut outcome = self.outcome.lock();
            let RoutedOutcomeCell::Pending { cancel_requested } = &*outcome else {
                debug_assert!(false, "routed task failure published more than once");
                return;
            };
            *outcome = RoutedOutcomeCell::FailedTask {
                err,
                cancel_requested: *cancel_requested,
            };
            true
        };

        if should_wake {
            self.waker.wake();
        }
    }

    pub(crate) fn fail_runtime(&self, err: Report<RuntimeError>) {
        let should_wake = {
            let mut outcome = self.outcome.lock();
            let RoutedOutcomeCell::Pending { cancel_requested } = &*outcome else {
                debug_assert!(false, "routed runtime failure published more than once");
                return;
            };
            *outcome = RoutedOutcomeCell::FailedRuntime {
                err,
                cancel_requested: *cancel_requested,
            };
            true
        };

        if should_wake {
            self.waker.wake();
        }
    }

    pub(crate) fn try_take_ready(&self) -> RoutedTakeReadyOutcome<'scope_ref, T> {
        let mut outcome = self.outcome.lock();
        match mem::replace(
            &mut *outcome,
            RoutedOutcomeCell::Consumed {
                cancel_requested: false,
                failed: false,
            },
        ) {
            RoutedOutcomeCell::Pending { cancel_requested } => {
                *outcome = RoutedOutcomeCell::Pending { cancel_requested };
                RoutedTakeReadyOutcome::Pending
            }
            RoutedOutcomeCell::Ready {
                ready,
                cancel_requested,
            } => {
                *outcome = RoutedOutcomeCell::Consumed {
                    cancel_requested,
                    failed: false,
                };
                RoutedTakeReadyOutcome::Ready(ready)
            }
            RoutedOutcomeCell::FailedTask {
                err,
                cancel_requested,
            } => {
                *outcome = RoutedOutcomeCell::Consumed {
                    cancel_requested,
                    failed: true,
                };
                RoutedTakeReadyOutcome::TaskErr(err)
            }
            RoutedOutcomeCell::FailedRuntime {
                err,
                cancel_requested,
            } => {
                *outcome = RoutedOutcomeCell::Consumed {
                    cancel_requested,
                    failed: true,
                };
                RoutedTakeReadyOutcome::RuntimeErr(err)
            }
            RoutedOutcomeCell::Consumed { .. } => RoutedTakeReadyOutcome::RuntimeErr(
                RuntimeError::InvariantViolation {
                    site: "RoutedSpawnState::try_take_ready",
                    detail: "routed outcome has already been consumed".into(),
                }
                .to_report(),
            ),
        }
    }

    pub(crate) fn register(&self, waker: &Waker) {
        unsafe {
            self.waker.register(waker);
        }
    }
}

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

pub(crate) fn handle_enqueue_pinned_outcome(outcome: EnqueuePinnedOutcome) -> bool {
    match outcome {
        EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => true,
        EnqueuePinnedOutcome::AbortedAcknowledged | EnqueuePinnedOutcome::AlreadySettled => false,
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
    if !handle_enqueue_pinned_outcome(outcome) {
        unsafe { arena.drop_object_raw(node_ptr as *mut u8, layout) };
        state.fail_task(TaskError::Panic);
        return;
    }

    let task_ready = unsafe {
        SendTaskRef::from_header(task_ref.header() as *const GenericTaskHeader<AtomicStorage>)
    };

    state.set_ready(RoutedSpawnReady {
        task: task_ready,
        access: make_boxed_task_access(node_ref),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskVTable;
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        ptr::NonNull,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    static TEST_VTABLE: TaskVTable<AtomicStorage> = TaskVTable {
        wake: |_| {},
        wake_by_ref: |_| {},
        poll: |_, _| Ok(true),
        drop: |_| {},
        drop_after_poll: false,
    };

    struct DropAccess {
        drops: Arc<AtomicUsize>,
    }

    impl RoutedTaskAccess<()> for DropAccess {
        fn take_result(&self) -> RoutedTakeResult<()> {
            RoutedTakeResult::TaskErr(TaskError::Cancelled)
        }

        fn reclaim(self: Box<Self>, _arena: &dyn Arena) {}
    }

    impl Drop for DropAccess {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn ready_for(task: SendTaskRef, drops: Arc<AtomicUsize>) -> RoutedSpawnReady<'static, ()> {
        RoutedSpawnReady {
            task,
            access: Box::new(DropAccess { drops }),
        }
    }

    #[test]
    fn cancellation_before_ready_is_applied_during_publish() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let state = RoutedSpawnState::<()>::new();
        let drops = Arc::new(AtomicUsize::new(0));

        state.request_cancel();
        let task = unsafe { SendTaskRef::from_header(NonNull::from(&header).as_ptr()) };
        state.set_ready(ready_for(task, drops.clone()));

        assert!(header.is_locally_cancelled());
        assert!(state.is_cancel_requested());
        assert!(matches!(
            state.try_take_ready(),
            RoutedTakeReadyOutcome::Ready(_)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancellation_after_ready_cancels_under_the_state_lock() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let state = RoutedSpawnState::<()>::new();
        let drops = Arc::new(AtomicUsize::new(0));

        let task = unsafe { SendTaskRef::from_header(NonNull::from(&header).as_ptr()) };
        state.set_ready(ready_for(task, drops.clone()));
        state.request_cancel();

        assert!(header.is_locally_cancelled());
        assert!(matches!(
            state.try_take_ready(),
            RoutedTakeReadyOutcome::Ready(_)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_state_has_one_consumable_result() {
        let state = RoutedSpawnState::<()>::new();
        assert!(!state.has_failed_outcome());
        state.fail_task(TaskError::Panic);
        assert!(state.has_failed_outcome());
        assert!(matches!(
            state.try_take_ready(),
            RoutedTakeReadyOutcome::TaskErr(TaskError::Panic)
        ));
        assert!(state.has_failed_outcome());
        assert!(matches!(
            state.try_take_ready(),
            RoutedTakeReadyOutcome::RuntimeErr(_)
        ));
    }

    #[test]
    fn duplicate_terminal_publication_is_rejected() {
        let state = RoutedSpawnState::<()>::new();
        state.fail_task(TaskError::Panic);

        let duplicate = catch_unwind(AssertUnwindSafe(|| {
            state.fail_task(TaskError::Cancelled);
        }));

        assert!(duplicate.is_err());
    }

    #[test]
    fn repeated_ready_consumption_reports_an_invariant() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let state = RoutedSpawnState::<()>::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let task = unsafe { SendTaskRef::from_header(NonNull::from(&header).as_ptr()) };
        state.set_ready(ready_for(task, drops.clone()));

        let first = state.try_take_ready();
        drop(first);
        assert!(matches!(
            state.try_take_ready(),
            RoutedTakeReadyOutcome::RuntimeErr(_)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropping_state_releases_unconsumed_ready_once() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let state = RoutedSpawnState::<()>::new();
            let task = unsafe { SendTaskRef::from_header(NonNull::from(&header).as_ptr()) };
            state.set_ready(ready_for(task, drops.clone()));
        }

        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_publish_cancel_take_has_one_ready_owner() {
        let mut builder = loom::model::Builder::new();
        builder.max_threads = 4;
        builder.max_branches = 100;
        builder.preemption_bound = Some(2);
        builder.check(|| {
            use loom::thread;

            let header = Box::new(GenericTaskHeader::<AtomicStorage>::new_placeholder(
                &TEST_VTABLE,
            ));
            let task_header = &*header;
            let task = unsafe { SendTaskRef::from_header(NonNull::from(task_header).as_ptr()) };
            let state = RoutedSpawnState::<()>::new();
            let drops = Arc::new(AtomicUsize::new(0));

            let publish_state = state.clone();
            let publish_drops = drops.clone();
            let publish = thread::spawn(move || {
                publish_state.set_ready(ready_for(task, publish_drops));
            });

            let cancel_state = state.clone();
            let cancel = thread::spawn(move || cancel_state.request_cancel());

            let take_state = state.clone();
            let take = thread::spawn(move || take_state.try_take_ready());

            publish.join().unwrap();
            cancel.join().unwrap();
            let taken = take.join().unwrap();
            drop(taken);

            let final_take = state.try_take_ready();
            drop(final_take);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        });
    }
}
