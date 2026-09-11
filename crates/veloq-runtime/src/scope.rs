use crate::{
    error::{Result, RuntimeError},
    runtime::{RuntimeCtx, RuntimeShared, cancellation::GenericCancellationToken},
    task::{
        AnyScopeRef, Arena, ArenaAllocation, ErasedCancellationToken, GenericArena,
        GenericTaskNode, LocalTask, LocalTaskRef, RawTask, ScopeRef, ScopeStorage, SendTask,
        SendTaskRef, Task, TaskBounds, TaskError, TaskHandleRef, TaskStorage,
    },
    utils::ownership::{ArcOwnership, Ownership, RcOwnership},
};
use diagweave::prelude::*;
use veloq_std::{
    alloc::Layout,
    future::Future,
    marker::PhantomData,
    ops::AsyncFnOnce,
    ptr::{NonNull, drop_in_place, write},
};
use veloq_std::{
    panic::resume_unwind,
    thread::{PanicState, panic_state},
};
use veloq_storage::{AtomicStorage, LocalStorage, StateLock, Storage};

mod completion;
mod guard;
mod join;
mod router;

pub use completion::{GenericScopeCompletion, LocalScopeCompletion, ScopeCompletion};
pub(crate) use completion::{ScopeBlockingWaiter, ScopeCompletionRegistration, ScopeJoinFuture};
pub use join::{JoinHandle, JoinOutcome, LocalAsyncJoinHandle, LocalJoinHandle, SendJoinHandle};

use guard::ScopeTaskGuard;
use router::{
    RoutedJobCell, RoutedJobCellOwner, RoutedSpawnReady, RoutedSpawnState, dispatch_routed,
    handle_enqueue_pinned_outcome, install_routed_pinned_task, make_spawn_to_access,
    new_failed_routed_state,
};

pub(crate) struct SendPtr<T>(NonNull<T>);

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

impl<T> Copy for SendPtr<T> {}

impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> SendPtr<T> {
    pub(crate) fn new(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }

    pub(crate) fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }
}

pub trait ScopeProvider<T> {
    type Storage: ScopeStorage;
    type Ownership: Ownership;
    type Arena: Arena;
    fn runtime(&self) -> &RuntimeShared<T>;
    fn arena(&self) -> &Self::Arena;
    fn completion(
        &self,
    ) -> &<Self::Ownership as Ownership>::Shared<
        GenericScopeCompletion<Self::Storage, Self::Ownership>,
    >;
}

pub(crate) fn new_cancel_slot<S: Storage, O: Ownership>()
-> S::Lock<Option<GenericCancellationToken<S, O>>> {
    S::Lock::new(None)
}

pub(crate) type CancelTokenSlot<S, O> =
    <S as Storage>::Lock<Option<GenericCancellationToken<S, O>>>;

/// 通用的作用域实现，支持通过 Storage 策略切换线程安全或本地分配。
pub struct GenericAsyncScope<
    'rt,
    'scope,
    'env: 'scope,
    S: ScopeStorage,
    O: Ownership + 'static,
    TExtra,
> {
    context: RuntimeCtx<'rt, TExtra>,
    arena: GenericArena<S>,
    completion: O::Shared<GenericScopeCompletion<S, O>>,
    _marker: PhantomData<fn(&'scope ()) -> &'env ()>,
}

pub type AsyncScope<'rt, 'scope, 'env, TExtra> =
    GenericAsyncScope<'rt, 'scope, 'env, AtomicStorage, ArcOwnership, TExtra>;
pub type LocalAsyncScope<'rt, 'scope, 'env, TExtra> =
    GenericAsyncScope<'rt, 'scope, 'env, LocalStorage, RcOwnership, TExtra>;

/// Cancels and joins a scope if the surrounding scope body unwinds.
pub(crate) struct ScopeExitGuard<
    'guard,
    'rt,
    'scope,
    'env: 'scope,
    S: ScopeStorage,
    O: Ownership + 'static,
    TExtra,
> {
    scope: &'guard GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>,
    armed: bool,
}

impl<'guard, 'rt, 'scope, 'env: 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra>
    ScopeExitGuard<'guard, 'rt, 'scope, 'env, S, O, TExtra>
{
    pub(crate) fn new(scope: &'guard GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>) -> Self {
        Self { scope, armed: true }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<'guard, 'rt, 'scope, 'env: 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra> Drop
    for ScopeExitGuard<'guard, 'rt, 'scope, 'env, S, O, TExtra>
{
    fn drop(&mut self) {
        if self.armed && self.scope.cancel_and_join().is_err() {
            // Drop 不能返回错误；wake failure 已由共享 fatal 通道保存。
        }
    }
}

impl<'rt, 'scope, 'env, S: ScopeStorage, O: Ownership + 'static, TExtra> ScopeProvider<TExtra>
    for GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>
{
    type Storage = S;
    type Ownership = O;
    type Arena = GenericArena<S>;
    #[inline]
    fn runtime(&self) -> &RuntimeShared<TExtra> {
        self.context.shared()
    }
    #[inline]
    fn arena(&self) -> &Self::Arena {
        &self.arena
    }
    #[inline]
    fn completion(&self) -> &O::Shared<GenericScopeCompletion<S, O>> {
        &self.completion
    }
}

impl<'rt, 'scope, 'env, S: ScopeStorage, O: Ownership + 'static, TExtra>
    GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>
{
    pub fn new(context: RuntimeCtx<'rt, TExtra>, parent: Option<AnyScopeRef>) -> Self {
        let completion = GenericScopeCompletion::<S, O>::new(parent.clone());

        if let Some(ref parent) = parent {
            let _ = parent.try_link_child(&ErasedCancellationToken::new::<S, O>(
                completion.cancel_token(),
            ));
        }

        Self {
            context,
            arena: GenericArena::new(),
            completion,
            _marker: PhantomData,
        }
    }

    pub fn spawn_local<'scope_ref, T: Send, TTask>(
        &'scope_ref self,
        task: &'env TTask,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        TTask: LocalTask<T> + Sized + 'env,
    {
        unsafe {
            self.spawn_task_impl(
                self.context.worker_id(),
                task,
                |runtime, worker_id, task_ref| runtime.enqueue_local(worker_id, task_ref),
            )
        }
    }

    pub fn spawn_boxed_local<'scope_ref, T, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + 'env,
    {
        unsafe {
            self.spawn_boxed_impl(
                self.context.worker_id(),
                future,
                |runtime, worker_id, task_ref| runtime.enqueue_local(worker_id, task_ref),
            )
        }
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        self.completion.cancel_token()
    }

    pub fn worker_id(&self) -> usize {
        self.context.worker_id()
    }

    /// 等待本作用域内派生的全部子任务结束。
    ///
    /// 真正的 `await`：未完成时把当前任务的 waker 挂到 completion 上并返回 `Pending`，由
    /// worker 顶层循环继续跑别的任务 —— 这本就是 thread-per-core 应有的行为。因此它可以被
    /// `select!` / 超时打断，也不再让栈深度随作用域嵌套增长。
    ///
    /// 子任务的 panic payload 在这里取走并重新抛出；被丢弃而没走到这里的作用域由 `Drop`
    /// 负责上交给父作用域。
    pub async fn wait_all(&self) -> Result<()> {
        ScopeJoinFuture::new(&*self.completion).await;
        if let Some(panic_info) = self.completion.take_panic() {
            resume_unwind(panic_info);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn scope_completion_ref(&self) -> ScopeRef<S> {
        ScopeRef::from_shared::<O>(&self.completion)
    }

    #[inline]
    pub fn shared(&self) -> &RuntimeShared<TExtra> {
        self.context.shared()
    }

    pub(crate) fn cancel_and_join(&self) -> Result<()> {
        if !self.completion.is_done() {
            self.completion.cancel();
            return self.context.shared().join_scope::<S, O>(&self.completion);
        }
        self.context.shared().base.fatal_error().map_or(Ok(()), Err)
    }

    unsafe fn spawn_task_impl<'scope_ref, T, H, TTask>(
        &'scope_ref self,
        worker_id: usize,
        task: &'env TTask,
        enqueue_fn: impl FnOnce(&RuntimeShared<TExtra>, usize, H) -> Result<()>,
    ) -> JoinHandle<'scope_ref, T, H, Self, TExtra>
    where
        H: TaskHandleRef,
        TTask: Task<T, Storage = H::Storage> + Sized + 'env,
    {
        let mut guard = ScopeTaskGuard::<S, O>::new(&self.completion);
        let task_ref = unsafe { H::from_concrete(task as *const TTask) };
        unsafe {
            let scope_ref = self.scope_completion_ref().cast::<H::Storage>();
            task_ref
                .header()
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        guard.handoff_to(task_ref.header());
        if let Err(err) = enqueue_fn(self.context.shared(), worker_id, task_ref) {
            if task_ref.header().has_enqueue_rejection() {
                return JoinHandle::new_direct(self, task_ref, task, None);
            }
            task_ref.header().abandon_before_enqueue();
            return JoinHandle::new_routed(self, new_failed_routed_state(err));
        }

        JoinHandle::new_direct(self, task_ref, task, None)
    }

    unsafe fn spawn_boxed_impl<'scope_ref, T, H, F>(
        &'scope_ref self,
        worker_id: usize,
        future: F,
        enqueue_fn: impl FnOnce(&RuntimeShared<TExtra>, usize, H) -> Result<()>,
    ) -> JoinHandle<'scope_ref, T, H, Self, TExtra>
    where
        H: TaskHandleRef,
        H::Storage: TaskStorage + TaskBounds<T, F>,
        F: Future<Output = T> + 'env,
    {
        let mut guard = ScopeTaskGuard::<S, O>::new(&self.completion);

        let scope_ref = self.scope_completion_ref().cast::<H::Storage>();
        let node = GenericTaskNode::<H::Storage, T, F>::new(future);
        unsafe {
            node.header
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        let layout = Layout::new::<GenericTaskNode<H::Storage, T, F>>();
        let allocation = unsafe {
            self.arena.alloc_managed(layout, |ptr| {
                drop_in_place(ptr as *mut GenericTaskNode<H::Storage, T, F>)
            })
        };
        let Some(allocation) = allocation else {
            guard.settle();
            return JoinHandle::new_routed(
                self,
                new_failed_routed_state(
                    RuntimeError::ArenaAllocationNull {
                        op: "AsyncScope::spawn_boxed_impl::alloc_task_node",
                    }
                    .to_report(),
                ),
            );
        };
        let node_ptr = allocation.data_ptr().as_ptr() as *mut GenericTaskNode<H::Storage, T, F>;
        unsafe { write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        guard.handoff_to(node_ref.header());

        let task_ref = unsafe { H::from_concrete(node_ptr) };
        if let Err(err) = enqueue_fn(self.context.shared(), worker_id, task_ref) {
            if task_ref.header().has_enqueue_rejection() {
                return JoinHandle::new_direct(self, task_ref, node_ref, Some(allocation));
            }
            task_ref.header().abandon_before_enqueue();
            if task_ref.header().is_reclaimable() {
                unsafe { allocation.reclaim() };
            }
            return JoinHandle::new_routed(self, new_failed_routed_state(err));
        }

        JoinHandle::new_direct(self, task_ref, node_ref, Some(allocation))
    }
}

impl<'rt, 'scope, 'env, S: ScopeStorage, O: Ownership + 'static, TExtra> Drop
    for GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>
{
    /// 作用域析构必须 join，而不是只发一次取消信号。
    ///
    /// `spawn*` 接受 `&'env` 借用，子任务可能正在别的 worker 上持有这些借用运行；取消在本
    /// 运行时是协作式的，「已取消」与「已停止」之间有任意长的窗口。正常路径由
    /// `wait_all()` 兜底，但 `f` panic 和「整个 scope future 被丢弃」（`select!` 落败分支、
    /// 超时、外层取消）这两条路径都绕过它 —— 那里若只发信号就返回，借用立刻悬垂。这正是
    /// `thread::scope` 必须在 `Drop` 里阻塞 join 的原因。
    fn drop(&mut self) {
        if self.cancel_and_join().is_err() {
            // Drop 不能返回错误；wake failure 已由共享 fatal 通道保存。
        }

        // panic payload 的归属：正常路径由 `wait_all()` 取走并抛出。走到这里说明没人 join
        // （上面那两条路径），payload 交给上一层 scope，由它的 `wait_all()` 抛出；已经没有
        // 上一层时，只要当前不是在 unwind 中就地抛出，绝不静默丢弃。
        if let Some(payload) = self.completion.take_panic() {
            match self.completion.parent() {
                Some(parent) => parent.report_panic(payload),
                None if matches!(panic_state(), PanicState::NotPanicking) => resume_unwind(payload),
                None => {}
            }
        }
    }
}

// 线程安全作用域特合方法
impl<'rt, 'scope, 'env, TExtra>
    GenericAsyncScope<'rt, 'scope, 'env, AtomicStorage, ArcOwnership, TExtra>
{
    fn spawn_send_impl<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        worker_id: usize,
        task: &'env S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + 'env,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        unsafe {
            self.spawn_task_impl(worker_id, task, |runtime, worker_id, task_ref| {
                runtime.enqueue_send(worker_id, task_ref);
                Ok(())
            })
        }
    }

    pub fn spawn_to<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        worker_id: usize,
        task: &'env S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + Sync + 'env,
    {
        let state = RoutedSpawnState::new();
        if let Err(err) = self.context.shared().validate_worker_id(worker_id) {
            state.fail_runtime(err);
            return JoinHandle::new_routed(self, state);
        }

        let guard: ScopeTaskGuard<AtomicStorage, ArcOwnership> =
            ScopeTaskGuard::new(&self.completion);

        let runtime = self.context.shared();
        let runtime_base_ptr = SendPtr::new(NonNull::from(&runtime.base));
        let state_for_job = state.clone();
        let scope_ref = self.scope_completion_ref();

        dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
            &self.context,
            guard,
            state.clone(),
            worker_id,
            move |guard| {
                if state_for_job.is_cancel_requested() {
                    state_for_job.fail_task(TaskError::Cancelled);
                    guard.settle();
                    return;
                }

                unsafe {
                    task.header()
                        .initialize(&*runtime_base_ptr.as_ptr(), worker_id, scope_ref);
                }
                task.header().set_pinned();

                let task_ref = unsafe { SendTaskRef::from_concrete(task) };
                guard.handoff_to(task.header());

                let outcome =
                    unsafe { &*runtime_base_ptr.as_ptr() }.enqueue_pinned(worker_id, task_ref);
                if !handle_enqueue_pinned_outcome(outcome) {
                    state_for_job.fail_task(TaskError::Panic);
                    return;
                }

                state_for_job.set_ready(RoutedSpawnReady {
                    task: task_ref,
                    access: make_spawn_to_access::<T, S_>(task),
                });
            },
        );

        JoinHandle::new_routed(self, state)
    }

    pub fn spawn<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        task: &'env S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + 'env,
    {
        self.spawn_send_impl(self.context.shared().choose_worker(), task)
    }

    fn spawn_boxed_send_impl<'scope_ref, T: Send, F>(
        &'scope_ref self,
        worker_id: usize,
        future: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + Send + 'env,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        unsafe {
            self.spawn_boxed_impl(worker_id, future, |runtime, worker_id, task_ref| {
                runtime.enqueue_send(worker_id, task_ref);
                Ok(())
            })
        }
    }

    pub fn spawn_boxed_to<'scope_ref, T: Send, F>(
        &'scope_ref self,
        worker_id: usize,
        job: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: AsyncFnOnce() -> T + Send + 'env,
    {
        let state = RoutedSpawnState::new();
        if let Err(err) = self.context.shared().validate_worker_id(worker_id) {
            state.fail_runtime(err);
            return JoinHandle::new_routed(self, state);
        }

        let guard: ScopeTaskGuard<AtomicStorage, ArcOwnership> =
            ScopeTaskGuard::new(&self.completion);

        let runtime = self.context.shared();
        let runtime_ptr = SendPtr::new(NonNull::from(runtime));
        let state_for_job = state.clone();
        let job_layout = Layout::new::<RoutedJobCell<F>>();
        let allocation = unsafe {
            self.arena.alloc_managed(job_layout, |ptr| {
                drop_in_place(ptr as *mut RoutedJobCell<F>)
            })
        };
        let Some(allocation) = allocation else {
            state.fail_runtime(
                RuntimeError::ArenaAllocationNull {
                    op: "AsyncScope::spawn_boxed_to::alloc_job",
                }
                .to_report(),
            );
            return JoinHandle::new_routed(self, state);
        };
        let job_ptr = allocation.data_ptr().as_ptr() as *mut RoutedJobCell<F>;
        unsafe { write(job_ptr, RoutedJobCell::new(job)) };
        // job cell 的所有权自此完全交给守卫，并随闭包一起移交给目标 worker。
        let job_owner: RoutedJobCellOwner<'scope_ref, F> = RoutedJobCellOwner::new(allocation);

        let arena = &self.arena;
        dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
            &self.context,
            guard,
            state.clone(),
            worker_id,
            move |guard| {
                let mut job_owner = job_owner;
                if state_for_job.is_cancel_requested() {
                    // The owner borrows the scope arena. Release it before publishing the
                    // terminal failure; dispatch_routed settles the scope after this closure
                    // returns, otherwise the arena could be dropped while captures unwind.
                    drop(job_owner);
                    state_for_job.fail_task(TaskError::Cancelled);
                    return;
                }

                let job = match job_owner.take_job() {
                    Ok(job) => job,
                    Err(err) => {
                        drop(job_owner);
                        state_for_job.fail_runtime(err);
                        return;
                    }
                };
                let future = job();

                if state_for_job.is_cancel_requested() {
                    drop(future);
                    state_for_job.fail_task(TaskError::Cancelled);
                    return;
                }

                install_routed_pinned_task(
                    unsafe { &*runtime_ptr.as_ptr() },
                    arena,
                    guard,
                    worker_id,
                    state_for_job,
                    future,
                );
            },
        );

        JoinHandle::new_routed(self, state)
    }

    pub fn spawn_boxed<'scope_ref, T: Send, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + Send + 'env,
    {
        self.spawn_boxed_send_impl(self.context.shared().choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'rt, 'scope, 'env, TExtra>
    GenericAsyncScope<'rt, 'scope, 'env, LocalStorage, RcOwnership, TExtra>
{
    pub fn spawn<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        task: &'env S_,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        S_: LocalTask<T> + Sized + 'env,
    {
        self.spawn_local(task)
    }

    pub fn spawn_boxed<'scope_ref, T: Send, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + 'env,
    {
        self.spawn_boxed_local(future)
    }
}
