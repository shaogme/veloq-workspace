use crate::{
    error::Result,
    runtime::{RuntimeScopeContext, RuntimeShared, primitives::GenericCancellationToken},
    task::{
        AnyScopeRef, Arena, ErasedCancellationToken, GenericArena, GenericTaskNode, LocalTask,
        LocalTaskRef, RawScope, RawTask, ScopeRef, ScopeStorage, SendTask, SendTaskRef, Task,
        TaskBounds, TaskError, TaskHandleRef, TaskJoinGate, TaskStorage,
    },
    utils::ownership::{ArcOwnership, Ownership, RcOwnership},
};
use std::{
    alloc::Layout,
    future::Future,
    marker::PhantomData,
    ops::AsyncFnOnce,
    panic::resume_unwind,
    ptr::{NonNull, drop_in_place, write},
};
use veloq_storage::{AtomicStorage, LocalStorage, StateLock, Storage};

mod completion;
mod guard;
mod join;
mod router;

pub(crate) use completion::ScopeCompletionRegistration;
pub use completion::{GenericScopeCompletion, LocalScopeCompletion, ScopeCompletion};
pub use join::{JoinHandle, LocalAsyncJoinHandle, LocalJoinHandle, SendJoinHandle};

use guard::ScopeTaskGuard;
use router::{
    RoutedJobCell, RoutedSpawnReady, RoutedSpawnState, dispatch_routed,
    handle_enqueue_pinned_outcome, install_routed_pinned_task, make_spawn_to_access,
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

    pub(crate) unsafe fn as_ref(&self) -> &T {
        unsafe { self.0.as_ref() }
    }

    pub(crate) unsafe fn as_mut(&mut self) -> &mut T {
        unsafe { self.0.as_mut() }
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
pub struct GenericAsyncScope<'rt, 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra> {
    context: RuntimeScopeContext<'rt, TExtra>,
    arena: GenericArena<S>,
    completion: O::Shared<GenericScopeCompletion<S, O>>,
    _phantom: PhantomData<fn(&'scope ()) -> &'scope ()>,
}

pub type AsyncScope<'rt, 'scope, TExtra> =
    GenericAsyncScope<'rt, 'scope, AtomicStorage, ArcOwnership, TExtra>;
pub type LocalAsyncScope<'rt, 'scope, TExtra> =
    GenericAsyncScope<'rt, 'scope, LocalStorage, RcOwnership, TExtra>;

impl<'rt, 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra> ScopeProvider<TExtra>
    for GenericAsyncScope<'rt, 'scope, S, O, TExtra>
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

impl<'rt, 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra>
    GenericAsyncScope<'rt, 'scope, S, O, TExtra>
{
    pub fn new(context: RuntimeScopeContext<'rt, TExtra>, parent: Option<AnyScopeRef>) -> Self {
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
            _phantom: PhantomData,
        }
    }

    pub fn spawn_local<'scope_ref, T: Send, TTask>(
        &'scope_ref self,
        task: &'scope TTask,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        TTask: LocalTask<T> + Sized + 'scope,
    {
        unsafe {
            self.spawn_task_impl(
                self.context.worker_id(),
                task,
                |runtime, worker_id, task_ref| {
                    runtime.enqueue_local(worker_id, task_ref);
                },
            )
        }
    }

    pub fn spawn_boxed_local<'scope_ref, T, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + 'scope_ref,
    {
        unsafe {
            self.spawn_boxed_impl(
                self.context.worker_id(),
                future,
                |runtime, worker_id, task_ref| {
                    runtime.enqueue_local(worker_id, task_ref);
                },
            )
        }
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        self.completion.cancel_token()
    }

    pub fn worker_id(&self) -> usize {
        self.context.worker_id()
    }

    pub async fn wait_all(&self) {
        self.context
            .shared()
            .drive_worker::<S, O>(Some(&self.completion));
        if let Some(panic_info) = self.completion.take_panic() {
            resume_unwind(panic_info);
        }
    }

    #[inline]
    pub fn scope_completion_ref(&self) -> ScopeRef<S> {
        unsafe {
            let non_null = RawScope::clone_raw(&*self.completion);
            ScopeRef::new(non_null)
        }
    }

    #[inline]
    pub fn shared(&self) -> &RuntimeShared<TExtra> {
        self.context.shared()
    }

    unsafe fn spawn_task_impl<'scope_ref, T, H, TTask>(
        &'scope_ref self,
        worker_id: usize,
        task: &'scope TTask,
        enqueue_fn: impl FnOnce(&RuntimeShared<TExtra>, usize, H),
    ) -> JoinHandle<'scope_ref, T, H, Self, TExtra>
    where
        H: TaskHandleRef,
        TTask: Task<T, Storage = H::Storage> + Sized + 'scope,
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
        enqueue_fn(self.context.shared(), worker_id, task_ref);

        JoinHandle::new_direct(self, task_ref, task, None)
    }

    unsafe fn spawn_boxed_impl<'scope_ref, T, H, F>(
        &'scope_ref self,
        worker_id: usize,
        future: F,
        enqueue_fn: impl FnOnce(&RuntimeShared<TExtra>, usize, H),
    ) -> JoinHandle<'scope_ref, T, H, Self, TExtra>
    where
        H: TaskHandleRef,
        H::Storage: TaskStorage + TaskBounds<T, F>,
        F: Future<Output = T> + 'scope_ref,
    {
        let mut guard = ScopeTaskGuard::<S, O>::new(&self.completion);

        let scope_ref = self.scope_completion_ref().cast::<H::Storage>();
        let node = GenericTaskNode::<H::Storage, T, F>::new(future);
        unsafe {
            node.header
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        let layout = Layout::new::<GenericTaskNode<H::Storage, T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<GenericTaskNode<H::Storage, T, F>>(
                layout,
                Some(|ptr| drop_in_place(ptr as *mut GenericTaskNode<H::Storage, T, F>)),
            ) as *mut GenericTaskNode<H::Storage, T, F>
        };
        unsafe { write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        guard.handoff_to(node_ref.header());

        let task_ref = unsafe { H::from_concrete(node_ptr) };
        enqueue_fn(self.context.shared(), worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            node_ref,
            Some(|arena, gate| unsafe {
                let layout = Layout::new::<GenericTaskNode<H::Storage, T, F>>();
                arena.drop_object_raw(gate as *const dyn TaskJoinGate<T> as *mut u8, layout);
            }),
        )
    }
}

impl<'rt, 'scope, S: ScopeStorage, O: Ownership + 'static, TExtra> Drop
    for GenericAsyncScope<'rt, 'scope, S, O, TExtra>
{
    fn drop(&mut self) {
        if !self.completion.is_done() {
            self.completion.cancel();
        }
    }
}

// 线程安全作用域特合方法
impl<'rt, 'scope, TExtra> GenericAsyncScope<'rt, 'scope, AtomicStorage, ArcOwnership, TExtra> {
    fn spawn_send_impl<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        worker_id: usize,
        task: &'scope S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        unsafe {
            self.spawn_task_impl(worker_id, task, |runtime, worker_id, task_ref| {
                runtime.enqueue_send(worker_id, task_ref);
            })
        }
    }

    pub fn spawn_to<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        worker_id: usize,
        task: &'scope S_,
    ) -> Result<JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>>
    where
        S_: SendTask<T> + Sized + Sync + 'scope,
    {
        self.context.shared().validate_worker_id(worker_id)?;

        let state = RoutedSpawnState::new();
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
                    state_for_job.fail(TaskError::Cancelled);
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
                if !handle_enqueue_pinned_outcome(guard, task_ref.header(), outcome) {
                    state_for_job.fail(TaskError::Panic);
                    return;
                }

                state_for_job.set_ready(RoutedSpawnReady {
                    task: task_ref,
                    access: make_spawn_to_access::<T, S_>(task),
                });
            },
        )?;

        Ok(JoinHandle::new_routed(self, state))
    }

    pub fn spawn<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        task: &'scope S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + 'scope,
    {
        self.spawn_send_impl(self.context.shared().choose_worker(), task)
    }

    fn spawn_boxed_send_impl<'scope_ref, T: Send, F>(
        &'scope_ref self,
        worker_id: usize,
        future: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + Send + 'scope_ref,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        unsafe {
            self.spawn_boxed_impl(worker_id, future, |runtime, worker_id, task_ref| {
                runtime.enqueue_send(worker_id, task_ref);
            })
        }
    }

    pub fn spawn_boxed_to<'scope_ref, T: Send, F>(
        &'scope_ref self,
        worker_id: usize,
        job: F,
    ) -> Result<JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>>
    where
        F: AsyncFnOnce() -> T + Send + 'scope_ref,
    {
        self.context.shared().validate_worker_id(worker_id)?;

        let state = RoutedSpawnState::new();
        let guard: ScopeTaskGuard<AtomicStorage, ArcOwnership> =
            ScopeTaskGuard::new(&self.completion);

        let runtime = self.context.shared();
        let runtime_ptr = SendPtr::new(NonNull::from(runtime));
        let state_for_job = state.clone();
        let job_layout = Layout::new::<RoutedJobCell<F>>();
        let job_ptr = unsafe {
            self.arena.alloc::<RoutedJobCell<F>>(
                job_layout,
                Some(|ptr| drop_in_place(ptr as *mut RoutedJobCell<F>)),
            ) as *mut RoutedJobCell<F>
        };
        unsafe { write(job_ptr, RoutedJobCell::new(job)) };
        let job_ptr: SendPtr<RoutedJobCell<F>> =
            SendPtr::new(unsafe { NonNull::new_unchecked(job_ptr) });
        let mut job_ptr_for_job = job_ptr;

        let arena_ptr = SendPtr::new(NonNull::from(&self.arena));
        if let Err(err) = dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
            &self.context,
            guard,
            state.clone(),
            worker_id,
            move |guard| {
                let arena = unsafe { arena_ptr.as_ref() };
                if state_for_job.is_cancel_requested() {
                    unsafe {
                        arena.drop_object_raw(job_ptr_for_job.as_ptr() as *mut u8, job_layout)
                    };
                    state_for_job.fail(TaskError::Cancelled);
                    guard.settle();
                    return;
                }

                let job = unsafe { job_ptr_for_job.as_mut().take() };
                let future = job();

                unsafe { arena.drop_object_raw(job_ptr_for_job.as_ptr() as *mut u8, job_layout) };

                if state_for_job.is_cancel_requested() {
                    state_for_job.fail(TaskError::Cancelled);
                    guard.settle();
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
        ) {
            unsafe {
                self.arena
                    .drop_object_raw(job_ptr.as_ptr() as *mut u8, job_layout)
            };
            return Err(err);
        }

        Ok(JoinHandle::new_routed(self, state))
    }

    pub fn spawn_boxed<'scope_ref, T: Send, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + Send + 'scope_ref,
    {
        self.spawn_boxed_send_impl(self.context.shared().choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'rt, 'scope, TExtra> GenericAsyncScope<'rt, 'scope, LocalStorage, RcOwnership, TExtra> {
    pub fn spawn<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        task: &'scope S_,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        S_: LocalTask<T> + Sized + 'scope,
    {
        self.spawn_local(task)
    }

    pub fn spawn_boxed<'scope_ref, T: Send, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + 'scope_ref,
    {
        self.spawn_boxed_local(future)
    }
}
