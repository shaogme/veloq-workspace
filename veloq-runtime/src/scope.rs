use crate::runtime::RuntimeShared;
use crate::runtime::primitives::GenericCancellationToken;
use crate::task::{
    Arena, GenericArena, LocalBoxedTaskNode, LocalTask, LocalTaskRef, SendTask, SendTaskRef, Task,
    TaskError, TaskHandleRef,
};
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt, StateLock, Storage};
use std::any::Any;
use std::ops::AsyncFnOnce;
use std::ptr::NonNull;
use std::sync::{Arc, atomic::Ordering};
use std::task::Waker;

mod join;

pub use join::{JoinHandle, LocalAsyncJoinHandle, LocalJoinHandle, SendJoinHandle};

#[derive(Copy, Clone)]
struct SendPtr<T>(NonNull<T>);

unsafe impl<T> Send for SendPtr<T> {}

impl<T> SendPtr<T> {
    fn new(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }

    unsafe fn as_ref(&self) -> &T {
        unsafe { self.0.as_ref() }
    }

    unsafe fn as_mut(&mut self) -> &mut T {
        unsafe { self.0.as_mut() }
    }

    fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }
}

/// 作用域级别的完成通知：所有子任务完成后唤醒等待者。
pub struct GenericScopeCompletion<S: Storage, O: Ownership> {
    remaining: S::Usize,
    wakers: S::Lock<Vec<Waker>>,
    cancel_token: GenericCancellationToken<S, O>,
    panic_info: S::Lock<Option<Box<dyn Any + Send + 'static>>>,
    parent: Option<crate::task::AnyScopeCompletionRef>,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: Storage, O: Ownership> GenericScopeCompletion<S, O> {
    pub fn new(parent: Option<crate::task::AnyScopeCompletionRef>) -> O::Shared<Self> {
        O::new(Self {
            remaining: S::Usize::new(0),
            wakers: S::Lock::new(Vec::new()),
            cancel_token: GenericCancellationToken::<S, O>::new(),
            panic_info: S::Lock::new(None),
            parent,
        })
    }

    fn drain_wakers(&self) {
        let wakers = {
            let mut wakers = self.wakers.lock();
            std::mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
        self.drain_wakers();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        &self.cancel_token
    }

    pub fn add_task(&self) {
        self.remaining.fetch_add(1, Ordering::AcqRel);
    }

    pub fn task_done(&self) {
        let remaining = self.remaining.fetch_sub(1, Ordering::AcqRel) - 1;
        if remaining == 0 {
            self.drain_wakers();
        }
    }

    pub fn register(&self, waker: &Waker) {
        if self.remaining.load(Ordering::Acquire) == 0 {
            waker.wake_by_ref();
            return;
        }

        {
            let mut wakers = self.wakers.lock();
            wakers.push(waker.clone());
        }

        if self.remaining.load(Ordering::Acquire) == 0 {
            self.drain_wakers();
        }
    }

    pub fn is_done(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        let mut panic_info = self.panic_info.lock();
        if panic_info.is_none() {
            *panic_info = Some(payload);
        }
    }

    pub fn take_panic(&self) -> Option<Box<dyn Any + Send + 'static>> {
        self.panic_info.lock().take()
    }

    pub fn parent(&self) -> &Option<crate::task::AnyScopeCompletionRef> {
        &self.parent
    }
}

pub trait ScopeProvider<'scope> {
    type Storage: Storage;
    type Ownership: Ownership;
    type Arena: crate::task::Arena;
    fn runtime(&self) -> &RuntimeShared;
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
pub struct GenericAsyncScope<'scope, S: Storage, O: Ownership, M = &'scope ()> {
    context: crate::runtime::RuntimeScopeContext,
    arena: GenericArena<S>,
    completion: O::Shared<GenericScopeCompletion<S, O>>,
    _marker: std::marker::PhantomData<(&'scope (), M)>,
}

pub type AsyncScope<'scope> = GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, &'scope ()>;
pub type LocalAsyncScope<'scope> =
    GenericAsyncScope<'scope, LocalStorage, RcOwnership, *const &'scope ()>;

impl<'scope, S: Storage, O: Ownership, M> ScopeProvider<'scope>
    for GenericAsyncScope<'scope, S, O, M>
{
    type Storage = S;
    type Ownership = O;
    type Arena = GenericArena<S>;
    #[inline]
    fn runtime(&self) -> &RuntimeShared {
        self.context.shared.as_ref()
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

impl<'scope, S: Storage, O: Ownership, M> GenericAsyncScope<'scope, S, O, M> {
    pub fn __private_new(
        context: crate::runtime::RuntimeScopeContext,
        parent: Option<crate::task::AnyScopeCompletionRef>,
    ) -> Self {
        let completion = GenericScopeCompletion::<S, O>::new(parent.clone());

        if let Some(parent) = parent {
            parent.try_link_child(&crate::task::ErasedCancellationToken::new::<S, O>(
                completion.cancel_token(),
            ));
        }

        Self {
            context,
            arena: GenericArena::new(),
            completion,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn spawn_local<T: 'scope, TTask>(
        &self,
        task: &'scope TTask,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        TTask: LocalTask<T> + Sized + 'scope,
    {
        task.set_scope_completion::<S, O>(Some(self.completion.clone()));
        self.completion.add_task();

        let worker_id = self.context.worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(task as *const TTask) };
        unsafe {
            task_ref
                .header()
                .set_runtime_info(Arc::as_ptr(&self.context.shared), worker_id)
        };
        self.context.shared.enqueue_local(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            unsafe {
                NonNull::new_unchecked(
                    task as &dyn crate::task::TaskJoinGate<T>
                        as *const dyn crate::task::TaskJoinGate<T>
                        as *mut dyn crate::task::TaskJoinGate<T>,
                )
            },
            None,
        )
    }

    pub fn spawn_boxed_local<T: 'scope, F>(
        &self,
        future: F,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        F: Future<Output = T> + 'scope,
    {
        let node = LocalBoxedTaskNode::new(future);
        let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'scope, T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<LocalBoxedTaskNode<'scope, T, F>>(
                layout,
                Some(|ptr| std::ptr::drop_in_place(ptr as *mut LocalBoxedTaskNode<'scope, T, F>)),
            ) as *mut LocalBoxedTaskNode<'scope, T, F>
        };
        unsafe { std::ptr::write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        node_ref.set_scope_completion::<S, O>(Some(self.completion.clone()));
        self.completion.add_task();

        let worker_id = self.context.worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(node_ptr) };
        unsafe {
            task_ref
                .header()
                .set_runtime_info(Arc::as_ptr(&self.context.shared), worker_id)
        };
        self.context.shared.enqueue_local(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            unsafe { NonNull::new_unchecked(node_ptr as *mut dyn crate::task::TaskJoinGate<T>) },
            Some(|arena, gate| unsafe {
                let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'scope, T, F>>();
                arena.drop_object_raw(gate.as_ptr() as *mut u8, layout);
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
        self.context.shared.drive_worker(Some(&self.completion));
        if let Some(panic_info) = self.completion.take_panic() {
            std::panic::resume_unwind(panic_info);
        }
    }
}

impl<'scope, S: Storage, O: Ownership, M> Drop for GenericAsyncScope<'scope, S, O, M> {
    fn drop(&mut self) {
        if !self.completion.is_done() {
            self.completion.cancel();
        }
    }
}

// 线程安全作用域特有方法
impl<'scope, M> GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, M> {
    fn spawn_send_impl<T: Send + 'scope, S_>(
        &self,
        worker_id: usize,
        task: &'scope S_,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S_: SendTask<T> + Sized + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        task.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(task as *const S_) };
        let header = task_ref.header();
        unsafe {
            header.set_runtime_info(Arc::as_ptr(&self.context.shared), worker_id);
        }
        self.context.shared.enqueue_send(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            unsafe {
                NonNull::new_unchecked(
                    task as &dyn crate::task::TaskJoinGate<T>
                        as *const dyn crate::task::TaskJoinGate<T>
                        as *mut dyn crate::task::TaskJoinGate<T>,
                )
            },
            None,
        )
    }

    pub fn spawn_to<T: Send + 'scope, S_>(
        &self,
        worker_id: usize,
        task: &'scope S_,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S_: SendTask<T> + Sized + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = self::join::RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared.clone();
        let completion = self.completion.clone();
        let task_ptr = SendPtr::new(NonNull::from(task));
        let state_for_job = state.clone();

        self::join::dispatch_routed::<AtomicStorage, ArcOwnership, T, _>(
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

                let task = unsafe { task_ptr.as_ref() };
                task.header().set_pinned();
                unsafe {
                    task.header()
                        .set_runtime_info(Arc::as_ptr(&runtime), worker_id);
                }
                task.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(completion.clone()));

                let task_ref = unsafe { SendTaskRef::from_concrete(task) };
                if !runtime.enqueue_pinned(worker_id, task_ref) {
                    state_for_job.fail(TaskError::Panic);
                    completion.task_done();
                    return;
                }

                state_for_job.set_ready(self::join::RoutedSpawnReady {
                    task: task_ref,
                    access: self::join::make_spawn_to_access::<T, S_>(task_ptr.0),
                });
            },
        );

        JoinHandle::new_routed(self, state)
    }

    pub fn spawn<T: Send + 'scope, S_>(
        &self,
        task: &'scope S_,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S_: SendTask<T> + Sized + 'scope,
    {
        self.spawn_send_impl(self.context.shared.choose_worker(), task)
    }

    fn spawn_boxed_send_impl<T: Send + 'scope, F>(
        &self,
        worker_id: usize,
        future: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: Future<Output = T> + Send + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        let node = crate::task::SendBoxedTaskNode::new(future);
        let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'scope, T, F>>();
        let node_ptr = unsafe {
            self.arena
                .alloc::<crate::task::SendBoxedTaskNode<'scope, T, F>>(
                    layout,
                    Some(|ptr| {
                        std::ptr::drop_in_place(
                            ptr as *mut crate::task::SendBoxedTaskNode<'scope, T, F>,
                        )
                    }),
                ) as *mut crate::task::SendBoxedTaskNode<'scope, T, F>
        };
        unsafe { std::ptr::write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        node_ref.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
        unsafe {
            task_ref
                .header()
                .set_runtime_info(Arc::as_ptr(&self.context.shared), worker_id)
        };
        self.context.shared.enqueue_send(worker_id, task_ref);

        JoinHandle::new_direct(
            self,
            task_ref,
            unsafe { NonNull::new_unchecked(node_ptr as *mut dyn crate::task::TaskJoinGate<T>) },
            Some(|arena, gate| unsafe {
                let layout =
                    std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'scope, T, F>>();
                arena.drop_object_raw(gate.as_ptr() as *mut u8, layout);
            }),
        )
    }

    pub fn spawn_boxed_to<T: Send + 'scope, F>(
        &self,
        worker_id: usize,
        job: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: AsyncFnOnce() -> T + Send + 'scope,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = self::join::RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared.clone();
        let completion = self.completion.clone();
        let arena_ptr = SendPtr::new(NonNull::from(&self.arena));
        let state_for_job = state.clone();
        let job_layout = std::alloc::Layout::new::<self::join::RoutedJobCell<'scope, F>>();
        let job_ptr = unsafe {
            self.arena.alloc::<self::join::RoutedJobCell<'scope, F>>(
                job_layout,
                Some(|ptr| {
                    std::ptr::drop_in_place(ptr as *mut self::join::RoutedJobCell<'scope, F>)
                }),
            ) as *mut self::join::RoutedJobCell<'scope, F>
        };
        unsafe { std::ptr::write(job_ptr, self::join::RoutedJobCell::new(job)) };
        let mut job_ptr: SendPtr<self::join::RoutedJobCell<'scope, F>> =
            SendPtr::new(unsafe { NonNull::new_unchecked(job_ptr) });

        self::join::dispatch_routed::<AtomicStorage, ArcOwnership, T, _>(
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

                self::join::install_routed_pinned_task(
                    &runtime,
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

    pub fn spawn_boxed<T: Send + 'scope, F>(
        &self,
        future: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: Future<Output = T> + Send + 'scope,
    {
        self.spawn_boxed_send_impl(self.context.shared.choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'scope, M> GenericAsyncScope<'scope, LocalStorage, RcOwnership, M> {
    pub fn spawn<T: 'scope, S_>(
        &self,
        task: &'scope S_,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        S_: LocalTask<T> + Sized + 'scope,
    {
        self.spawn_local(task)
    }

    pub fn spawn_boxed<T: 'scope, F>(
        &self,
        future: F,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        F: Future<Output = T> + 'scope,
    {
        self.spawn_boxed_local(future)
    }
}
