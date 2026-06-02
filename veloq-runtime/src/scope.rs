use crate::runtime::primitives::GenericCancellationToken;
use crate::runtime::{RuntimeScopeContext, RuntimeShared};
use crate::task::{
    AnyScopeRef, Arena, ErasedCancellationToken, GenericArena, LocalBoxedTaskNode, LocalTask,
    LocalTaskRef, RawScope, ScopeRef, SendBoxedTaskNode, SendTask, SendTaskRef, TaskError,
    TaskHandleRef, TaskJoinGate,
};
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateLock, Storage};
use std::alloc::Layout;
use std::future::Future;
use std::ops::AsyncFnOnce;
use std::ptr::{NonNull, drop_in_place, write};

mod completion;
mod join;
mod router;

pub use completion::{GenericScopeCompletion, LocalScopeCompletion, ScopeCompletion};
pub use join::{JoinHandle, LocalAsyncJoinHandle, LocalJoinHandle, SendJoinHandle};

use router::{
    RoutedJobCell, RoutedSpawnReady, RoutedSpawnState, dispatch_routed, install_routed_pinned_task,
    make_spawn_to_access,
};

#[derive(Copy, Clone)]
pub(crate) struct SendPtr<T>(NonNull<T>);

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

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
    type Storage: Storage;
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
pub struct GenericAsyncScope<'scope, S: Storage, O: Ownership + 'static, TExtra> {
    context: RuntimeScopeContext<TExtra>,
    arena: GenericArena<S>,
    completion: O::Shared<GenericScopeCompletion<S, O>>,
    _phantom: std::marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
}

pub type AsyncScope<'scope, TExtra> =
    GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, TExtra>;
pub type LocalAsyncScope<'scope, TExtra> =
    GenericAsyncScope<'scope, LocalStorage, RcOwnership, TExtra>;

impl<'scope, S: Storage, O: Ownership + 'static, TExtra> ScopeProvider<TExtra>
    for GenericAsyncScope<'scope, S, O, TExtra>
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

impl<'scope, S: Storage, O: Ownership + 'static, TExtra> GenericAsyncScope<'scope, S, O, TExtra> {
    pub fn new(context: RuntimeScopeContext<TExtra>, parent: Option<AnyScopeRef>) -> Self {
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
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn spawn_local<'scope_ref, T: Send, TTask>(
        &'scope_ref self,
        task: &'scope TTask,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        TTask: LocalTask<T> + Sized + 'scope,
    {
        self.completion.add_task();

        let worker_id = self.context.worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(task as *const TTask) };
        unsafe {
            let scope_ref = self.scope_completion_ref().cast::<LocalStorage>();
            task_ref
                .header()
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        self.context.shared().enqueue_local(worker_id, task_ref);

        JoinHandle::new_direct(self, task_ref, task, None)
    }

    pub fn spawn_boxed_local<'scope_ref, T, F>(
        &'scope_ref self,
        future: F,
    ) -> JoinHandle<'scope_ref, T, LocalTaskRef, Self, TExtra>
    where
        F: Future<Output = T> + 'scope_ref,
    {
        let worker_id = self.context.worker_id();
        let scope_ref = self.scope_completion_ref().cast::<LocalStorage>();
        let node = LocalBoxedTaskNode::new(future);
        unsafe {
            node.header
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        let layout = Layout::new::<LocalBoxedTaskNode<T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<LocalBoxedTaskNode<T, F>>(
                layout,
                Some(|ptr| drop_in_place(ptr as *mut LocalBoxedTaskNode<T, F>)),
            ) as *mut LocalBoxedTaskNode<T, F>
        };
        unsafe { write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        self.completion.add_task();

        let task_ref = unsafe { LocalTaskRef::from_concrete(node_ptr) };
        self.context.shared().enqueue_local(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            node_ref,
            Some(|arena, gate| unsafe {
                let layout = Layout::new::<LocalBoxedTaskNode<T, F>>();
                arena.drop_object_raw(gate as *const dyn TaskJoinGate<T> as *mut u8, layout);
            }),
        )
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
            std::panic::resume_unwind(panic_info);
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
}

impl<'scope, S: Storage, O: Ownership + 'static, TExtra> Drop
    for GenericAsyncScope<'scope, S, O, TExtra>
{
    fn drop(&mut self) {
        if !self.completion.is_done() {
            self.completion.cancel();
        }
    }
}

// 线程安全作用域特合方法
impl<'scope, TExtra> GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, TExtra> {
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
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(task as *const S_) };
        unsafe {
            task_ref.header().initialize(
                &self.context.shared().base,
                worker_id,
                self.scope_completion_ref(),
            );
        }
        self.context.shared().enqueue_send(worker_id, task_ref);

        JoinHandle::new_direct(self, task_ref, task, None)
    }

    pub fn spawn_to<'scope_ref, T: Send, S_>(
        &'scope_ref self,
        worker_id: usize,
        task: &'scope S_,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        S_: SendTask<T> + Sized + Sync + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared();
        let runtime_base_ptr = SendPtr::new(NonNull::from(&runtime.base));
        let completion = self.completion.clone();
        let state_for_job = state.clone();
        let scope_ref = self.scope_completion_ref();

        dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
            &self.context,
            &self.completion,
            state.clone(),
            worker_id,
            move || {
                if state_for_job.is_cancel_requested() {
                    state_for_job.fail(TaskError::Cancelled);
                    completion.task_done();
                    return;
                }

                unsafe {
                    task.header()
                        .initialize(&*runtime_base_ptr.as_ptr(), worker_id, scope_ref);
                }
                task.header().set_pinned();

                let task_ref = unsafe { SendTaskRef::from_concrete(task) };
                if !unsafe { &*runtime_base_ptr.as_ptr() }.enqueue_pinned(worker_id, task_ref) {
                    state_for_job.fail(TaskError::Panic);
                    completion.task_done();
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
        let scope_ref = self.scope_completion_ref();
        let node = SendBoxedTaskNode::new(future);
        unsafe {
            node.header
                .initialize(&self.context.shared().base, worker_id, scope_ref);
        }
        let layout = Layout::new::<SendBoxedTaskNode<T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<SendBoxedTaskNode<T, F>>(
                layout,
                Some(|ptr| drop_in_place(ptr as *mut SendBoxedTaskNode<T, F>)),
            ) as *mut SendBoxedTaskNode<T, F>
        };
        unsafe { write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
        self.context.shared().enqueue_send(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            node_ref,
            Some(|arena, gate| unsafe {
                let layout = Layout::new::<SendBoxedTaskNode<T, F>>();
                arena.drop_object_raw(gate as *const dyn TaskJoinGate<T> as *mut u8, layout);
            }),
        )
    }

    pub fn spawn_boxed_to<'scope_ref, T: Send, F>(
        &'scope_ref self,
        worker_id: usize,
        job: F,
    ) -> JoinHandle<'scope_ref, T, SendTaskRef, Self, TExtra>
    where
        F: AsyncFnOnce() -> T + Send + 'scope_ref,
    {
        debug_assert!(
            worker_id < self.context.shared().worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared();
        let runtime_ptr = SendPtr::new(NonNull::from(runtime));
        let completion = self.completion.clone();
        let state_for_job = state.clone();
        let job_layout = Layout::new::<RoutedJobCell<F>>();
        let job_ptr = unsafe {
            self.arena.alloc::<RoutedJobCell<F>>(
                job_layout,
                Some(|ptr| drop_in_place(ptr as *mut RoutedJobCell<F>)),
            ) as *mut RoutedJobCell<F>
        };
        unsafe { write(job_ptr, RoutedJobCell::new(job)) };
        let mut job_ptr: SendPtr<RoutedJobCell<F>> =
            SendPtr::new(unsafe { NonNull::new_unchecked(job_ptr) });

        let arena_ptr = SendPtr::new(NonNull::from(&self.arena));
        dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
            &self.context,
            &self.completion,
            state.clone(),
            worker_id,
            move || {
                let arena = unsafe { arena_ptr.as_ref() };
                if state_for_job.is_cancel_requested() {
                    unsafe { arena.drop_object_raw(job_ptr.as_ptr() as *mut u8, job_layout) };
                    state_for_job.fail(TaskError::Cancelled);
                    completion.task_done();
                    return;
                }

                let job = unsafe { job_ptr.as_mut().take() };
                let future = job();

                unsafe { arena.drop_object_raw(job_ptr.as_ptr() as *mut u8, job_layout) };

                if state_for_job.is_cancel_requested() {
                    state_for_job.fail(TaskError::Cancelled);
                    completion.task_done();
                    return;
                }

                install_routed_pinned_task(
                    unsafe { &*runtime_ptr.as_ptr() },
                    arena,
                    completion,
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
        F: Future<Output = T> + Send + 'scope_ref,
    {
        self.spawn_boxed_send_impl(self.context.shared().choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'scope, TExtra> GenericAsyncScope<'scope, LocalStorage, RcOwnership, TExtra> {
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
