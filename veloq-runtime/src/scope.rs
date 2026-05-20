use crate::runtime::RuntimeShared;
use crate::runtime::primitives::GenericCancellationToken;
use crate::task::{
    AnyScopeCompletionRef, Arena, GenericArena, LocalBoxedTaskNode, LocalTask, LocalTaskRef,
    SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
};
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt, StateLock, Storage};
use std::any::Any;
use std::collections::VecDeque;
use std::mem::take;
use std::ops::AsyncFnOnce;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
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
    parent: Option<AnyScopeCompletionRef>,
    local_queue: S::Lock<VecDeque<LocalTaskRef<'static>>>,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: Storage, O: Ownership> GenericScopeCompletion<S, O> {
    pub fn new(parent: Option<AnyScopeCompletionRef>) -> O::Shared<Self> {
        O::new(Self {
            remaining: S::Usize::new(0),
            wakers: S::Lock::new(Vec::new()),
            cancel_token: GenericCancellationToken::<S, O>::new(),
            panic_info: S::Lock::new(None),
            parent,
            local_queue: S::Lock::new(VecDeque::new()),
        })
    }

    #[inline]
    pub(crate) fn enqueue_local_task(&self, task: LocalTaskRef<'static>) {
        self.local_queue.lock().push_back(task);
    }

    #[inline]
    pub(crate) fn pop_local_task(&self) -> Option<LocalTaskRef<'static>> {
        self.local_queue.lock().pop_front()
    }

    #[inline]
    pub(crate) fn is_local_queue_empty(&self) -> bool {
        self.local_queue.lock().is_empty()
    }

    fn drain_wakers(&self) {
        let wakers = {
            let mut wakers = self.wakers.lock();
            take(&mut *wakers)
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
        if self.cancel_token.is_cancelled() {
            return true;
        }
        if let Some(ref parent) = self.parent
            && parent.is_cancelled()
        {
            return true;
        }
        false
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

impl<S: Storage, O: Ownership> Drop for GenericScopeCompletion<S, O> {
    fn drop(&mut self) {
        if let Some(panic_info) = self.panic_info.lock().take() {
            std::panic::resume_unwind(panic_info);
        }
    }
}

pub trait ScopeProvider<'ctx, T> {
    type Storage: Storage;
    type Ownership: Ownership;
    type Arena: crate::task::Arena;
    fn runtime(&self) -> &RuntimeShared<'ctx, T>;
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
pub struct GenericAsyncScope<'ctx, S: Storage, O: Ownership, TExtra> {
    context: crate::runtime::RuntimeScopeContext<'ctx, TExtra>,
    arena: GenericArena<S>,
    completion: O::Shared<GenericScopeCompletion<S, O>>,
}

pub type AsyncScope<'ctx, TExtra> = GenericAsyncScope<'ctx, AtomicStorage, ArcOwnership, TExtra>;
pub type LocalAsyncScope<'ctx, TExtra> = GenericAsyncScope<'ctx, LocalStorage, RcOwnership, TExtra>;

impl<'ctx, S: Storage, O: Ownership, TExtra> ScopeProvider<'ctx, TExtra>
    for GenericAsyncScope<'ctx, S, O, TExtra>
{
    type Storage = S;
    type Ownership = O;
    type Arena = GenericArena<S>;
    #[inline]
    fn runtime(&self) -> &RuntimeShared<'ctx, TExtra> {
        self.context.shared
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

impl<'ctx, S: Storage, O: Ownership, TExtra> GenericAsyncScope<'ctx, S, O, TExtra> {
    pub fn new(
        context: crate::runtime::RuntimeScopeContext<'ctx, TExtra>,
        parent: Option<crate::task::AnyScopeCompletionRef>,
    ) -> Self {
        let completion = GenericScopeCompletion::<S, O>::new(parent.clone());

        if let Some(ref parent) = parent {
            let linked = parent.try_link_child(&crate::task::ErasedCancellationToken::new::<S, O>(
                completion.cancel_token(),
            ));
            if !linked && let crate::task::AnyScopeCompletionRef::Send(_) = parent {
                let mut cross = completion.cancel_token().inner.cross_parent.lock();
                *cross = Some(parent.clone());
            }
        }

        Self {
            context,
            arena: GenericArena::new(),
            completion,
        }
    }

    pub fn spawn_local<T: Send, TTask>(
        &self,
        task: &TTask,
    ) -> JoinHandle<'ctx, '_, T, LocalTaskRef<'ctx>, Self, TExtra>
    where
        TTask: LocalTask<'ctx, T> + Sized,
    {
        task.set_scope_completion::<S, O>(Some(self.completion.clone()));
        self.completion.add_task();

        let worker_id = self.context.worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(task as *const TTask) };
        task_ref.header().set_worker_id(worker_id);
        self.context.shared.enqueue_local(worker_id, task_ref);

        let gate_ref: &dyn crate::task::TaskJoinGate<T> = task;
        let gate_ctx: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx) =
            unsafe { std::mem::transmute(gate_ref) };

        JoinHandle::new_direct(self, task_ref, gate_ctx, None)
    }

    pub fn spawn_boxed_local<T, F>(
        &self,
        future: F,
    ) -> JoinHandle<'ctx, '_, T, LocalTaskRef<'ctx>, Self, TExtra>
    where
        F: Future<Output = T>,
    {
        let worker_id = self.context.worker_id();
        let node = LocalBoxedTaskNode::new(future, &self.context.shared.base, worker_id);
        let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'ctx, T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<LocalBoxedTaskNode<'ctx, T, F>>(
                layout,
                Some(|ptr| std::ptr::drop_in_place(ptr as *mut LocalBoxedTaskNode<'ctx, T, F>)),
            ) as *mut LocalBoxedTaskNode<'ctx, T, F>
        };
        unsafe { std::ptr::write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        node_ref.set_scope_completion::<S, O>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { LocalTaskRef::from_concrete(node_ptr) };
        self.context.shared.enqueue_local(worker_id, task_ref);

        let gate_ref: &dyn crate::task::TaskJoinGate<T> = node_ref;
        let gate_ctx: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx) =
            unsafe { std::mem::transmute(gate_ref) };

        JoinHandle::new_direct(
            self,
            task_ref,
            gate_ctx,
            Some(|arena, gate| unsafe {
                let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'ctx, T, F>>();
                arena.drop_object_raw(
                    gate as *const dyn crate::task::TaskJoinGate<T> as *mut u8,
                    layout,
                );
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
            .shared
            .drive_worker::<S, O>(Some(&self.completion));
        if let Some(panic_info) = self.completion.take_panic() {
            std::panic::resume_unwind(panic_info);
        }
    }
}

impl<'ctx, S: Storage, O: Ownership, TExtra> Drop for GenericAsyncScope<'ctx, S, O, TExtra> {
    fn drop(&mut self) {
        if !self.completion.is_done() {
            self.completion.cancel();
        }
    }
}

// 线程安全作用域特有方法
impl<'ctx, TExtra> GenericAsyncScope<'ctx, AtomicStorage, ArcOwnership, TExtra> {
    fn spawn_send_impl<T: Send, S_>(
        &self,
        worker_id: usize,
        task: &S_,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        S_: SendTask<'ctx, T> + Sized,
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
        header.set_worker_id(worker_id);
        self.context.shared.enqueue_send(worker_id, task_ref);

        let gate_ref: &dyn crate::task::TaskJoinGate<T> = task;
        let gate_ctx: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx) =
            unsafe { std::mem::transmute(gate_ref) };

        JoinHandle::new_direct(self, task_ref, gate_ctx, None)
    }

    pub fn spawn_to<T: Send, S_>(
        &self,
        worker_id: usize,
        task: &'ctx S_,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        S_: SendTask<'ctx, T> + Sized + Sync + 'ctx,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = self::join::RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared;
        let completion = self.completion.clone();
        let state_for_job = state.clone();

        self::join::dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
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

                task.header().set_pinned();
                task.header().set_worker_id(worker_id);
                task.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(completion.clone()));

                let task_ref = unsafe { SendTaskRef::from_concrete(task) };
                if !runtime.enqueue_pinned(worker_id, task_ref) {
                    state_for_job.fail(TaskError::Panic);
                    completion.task_done();
                    return;
                }

                state_for_job.set_ready(self::join::RoutedSpawnReady {
                    task: task_ref,
                    access: self::join::make_spawn_to_access::<T, S_>(task),
                });
            },
        );

        JoinHandle::new_routed(self, state)
    }

    pub fn spawn<T: Send, S_>(
        &self,
        task: &S_,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        S_: SendTask<'ctx, T> + Sized,
    {
        self.spawn_send_impl(self.context.shared.choose_worker(), task)
    }

    fn spawn_boxed_send_impl<T: Send, F>(
        &self,
        worker_id: usize,
        future: F,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        F: Future<Output = T> + Send,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );
        let node =
            crate::task::SendBoxedTaskNode::new(future, &self.context.shared.base, worker_id);
        let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'ctx, T, F>>();
        let node_ptr = unsafe {
            self.arena
                .alloc::<crate::task::SendBoxedTaskNode<'ctx, T, F>>(
                    layout,
                    Some(|ptr| {
                        std::ptr::drop_in_place(
                            ptr as *mut crate::task::SendBoxedTaskNode<'ctx, T, F>,
                        )
                    }),
                ) as *mut crate::task::SendBoxedTaskNode<'ctx, T, F>
        };
        unsafe { std::ptr::write(node_ptr, node) };

        let node_ref = unsafe { &*node_ptr };
        node_ref.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
        self.context.shared.enqueue_send(worker_id, task_ref);

        let gate_ref: &dyn crate::task::TaskJoinGate<T> = node_ref;
        let gate_ctx: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx) =
            unsafe { std::mem::transmute(gate_ref) };

        JoinHandle::new_direct(
            self,
            task_ref,
            gate_ctx,
            Some(|arena, gate| unsafe {
                let layout =
                    std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'ctx, T, F>>();
                arena.drop_object_raw(
                    gate as *const dyn crate::task::TaskJoinGate<T> as *mut u8,
                    layout,
                );
            }),
        )
    }

    pub fn spawn_boxed_to<T: Send, F>(
        &self,
        worker_id: usize,
        job: F,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        F: AsyncFnOnce() -> T + Send + 'ctx,
    {
        debug_assert!(
            worker_id < self.context.shared.worker_count().get(),
            "worker_id {} is out of bounds",
            worker_id
        );

        let state = self::join::RoutedSpawnState::new();
        self.completion.add_task();

        let runtime = self.context.shared;
        let completion = self.completion.clone();
        let state_for_job = state.clone();
        let job_layout = std::alloc::Layout::new::<self::join::RoutedJobCell<'ctx, F>>();
        let job_ptr = unsafe {
            self.arena.alloc::<self::join::RoutedJobCell<'ctx, F>>(
                job_layout,
                Some(|ptr| std::ptr::drop_in_place(ptr as *mut self::join::RoutedJobCell<'ctx, F>)),
            ) as *mut self::join::RoutedJobCell<'ctx, F>
        };
        unsafe { std::ptr::write(job_ptr, self::join::RoutedJobCell::new(job)) };
        let mut job_ptr: SendPtr<self::join::RoutedJobCell<'ctx, F>> =
            SendPtr::new(unsafe { NonNull::new_unchecked(job_ptr) });

        let arena_ptr = SendPtr::new(NonNull::from(&self.arena));
        self::join::dispatch_routed::<AtomicStorage, ArcOwnership, T, _, TExtra>(
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
                    runtime,
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

    pub fn spawn_boxed<T: Send, F>(
        &self,
        future: F,
    ) -> JoinHandle<'ctx, '_, T, SendTaskRef<'ctx>, Self, TExtra>
    where
        F: Future<Output = T> + Send,
    {
        self.spawn_boxed_send_impl(self.context.shared.choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'ctx, TExtra> GenericAsyncScope<'ctx, LocalStorage, RcOwnership, TExtra> {
    pub fn spawn<T: Send, S_>(
        &self,
        task: &S_,
    ) -> JoinHandle<'ctx, '_, T, LocalTaskRef<'ctx>, Self, TExtra>
    where
        S_: LocalTask<'ctx, T> + Sized,
    {
        self.spawn_local(task)
    }

    pub fn spawn_boxed<T: Send, F>(
        &self,
        future: F,
    ) -> JoinHandle<'ctx, '_, T, LocalTaskRef<'ctx>, Self, TExtra>
    where
        F: Future<Output = T>,
    {
        self.spawn_boxed_local(future)
    }
}
