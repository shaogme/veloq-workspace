use crate::runtime::{
    GenericCancellationToken, RuntimeShared, current_worker_id, with_current_runtime,
};
use crate::task::{
    Arena, GenericArena, GenericWakerNode, LocalBoxedTaskNode, LocalTask, LocalTaskRef,
    SendBoxedTaskNode, SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
};
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, StateOptionPtr, Storage,
};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

unsafe fn take_result_of<T, S>(ptr: *const ()) -> Option<Result<T, TaskError>>
where
    S: Task<T>,
{
    unsafe { (&*(ptr as *const S)).take_result() }
}

/// 作用域级别的完成通知：所有子任务完成后唤醒等待者。
pub struct GenericScopeCompletion<S: Storage, O: Ownership> {
    remaining: S::Usize,
    wakers: S::Lock<Vec<Waker>>,
    cancel_token: GenericCancellationToken<S, O>,
    panic_info: S::Lock<Option<Box<dyn Any + Send + 'static>>>,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: Storage, O: Ownership> GenericScopeCompletion<S, O> {
    pub fn new() -> O::Shared<Self> {
        O::new(Self {
            remaining: S::Usize::new(0),
            wakers: S::Lock::new(Vec::new()),
            cancel_token: GenericCancellationToken::<S, O>::new(),
            panic_info: S::Lock::new(None),
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
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
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
}

pub trait ScopeProvider<'scope> {
    type Storage: Storage;
    type Ownership: Ownership;
    type Arena: crate::task::Arena;
    fn runtime(&self) -> &Arc<RuntimeShared>;
    fn arena(&self) -> &Self::Arena;
    fn completion(
        &self,
    ) -> &<Self::Ownership as Ownership>::Shared<
        GenericScopeCompletion<Self::Storage, Self::Ownership>,
    >;
}

fn new_cancel_slot<S: Storage, O: Ownership>() -> S::Lock<Option<GenericCancellationToken<S, O>>> {
    S::Lock::new(None)
}

pub(crate) type CancelTokenSlot<S, O> =
    <S as Storage>::Lock<Option<GenericCancellationToken<S, O>>>;

/// 通用的作用域实现，支持通过 Storage 策略切换线程安全或本地分配。
pub struct GenericAsyncScope<'scope, S: Storage, O: Ownership, M = &'scope ()> {
    runtime: Arc<RuntimeShared>,
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
    fn runtime(&self) -> &Arc<RuntimeShared> {
        &self.runtime
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
    pub fn __private_new() -> Self {
        let runtime = with_current_runtime(|runtime| runtime.clone())
            .expect("Scope must be created inside Runtime::block_on");
        Self {
            runtime,
            arena: GenericArena::new(),
            completion: GenericScopeCompletion::<S, O>::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn spawn_local<T: 'scope, TTask>(
        &self,
        task: &'scope TTask,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        TTask: LocalTask<T> + 'scope,
    {
        task.set_scope_completion::<S, O>(Some(self.completion.clone()));
        self.completion.add_task();

        let worker_id = current_worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(task as *const TTask) };
        task_ref
            .header()
            .set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_local(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            ptr: task as *const TTask as *const (),
            take_result: take_result_of::<T, TTask>,
            scope: self,
            _marker: std::marker::PhantomData,
            cancel_token: new_cancel_slot::<S, O>(),
            waker_node: None,
            reclaim: None,
        }
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

        let worker_id = current_worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(node_ptr) };
        task_ref
            .header()
            .set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_local(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            ptr: node_ptr as *const (),
            take_result: take_result_of::<T, LocalBoxedTaskNode<'scope, T, F>>,
            scope: self,
            _marker: std::marker::PhantomData,
            cancel_token: new_cancel_slot::<S, O>(),
            waker_node: None,
            reclaim: Some(|arena, ptr| unsafe {
                let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'scope, T, F>>();
                arena.drop_object_raw(ptr as *mut u8, layout);
            }),
        }
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        self.completion.cancel_token()
    }

    pub async fn wait_all(&self) {
        self.runtime.drive_worker(Some(&self.completion));
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
    pub fn spawn_to<T: Send + 'scope, S>(
        &self,
        worker_id: usize,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S: SendTask<T> + 'scope,
    {
        debug_assert!(
            worker_id < self.runtime.worker_count().get(),
            "worker_id {} is out of bounds (max {})",
            worker_id,
            self.runtime.worker_count().get()
        );
        task.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(task as *const S) };
        task_ref
            .header()
            .set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_send(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            ptr: task as *const S as *const (),
            take_result: take_result_of::<T, S>,
            scope: self,
            _marker: std::marker::PhantomData,
            cancel_token: new_cancel_slot::<AtomicStorage, ArcOwnership>(),
            waker_node: None,
            reclaim: None,
        }
    }

    pub fn spawn<T: Send + 'scope, S>(
        &self,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S: SendTask<T> + 'scope,
    {
        self.spawn_to(self.runtime.choose_worker(), task)
    }

    pub fn spawn_boxed_to<T: Send + 'scope, F>(
        &self,
        worker_id: usize,
        future: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: Future<Output = T> + Send + 'scope,
    {
        debug_assert!(
            worker_id < self.runtime.worker_count().get(),
            "worker_id {} is out of bounds (max {})",
            worker_id,
            self.runtime.worker_count().get()
        );
        let node = SendBoxedTaskNode::new(future);
        let layout = std::alloc::Layout::new::<SendBoxedTaskNode<'scope, T, F>>();
        let node_ptr = unsafe {
            self.arena.alloc::<SendBoxedTaskNode<'scope, T, F>>(
                layout,
                Some(|ptr| std::ptr::drop_in_place(ptr as *mut SendBoxedTaskNode<'scope, T, F>)),
            ) as *mut SendBoxedTaskNode<'scope, T, F>
        };
        unsafe { std::ptr::write(node_ptr, node) };
        let node_ref = unsafe { &*node_ptr };

        node_ref.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(self.completion.clone()));
        self.completion.add_task();

        let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
        task_ref
            .header()
            .set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_send(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            ptr: node_ptr as *const (),
            take_result: take_result_of::<T, SendBoxedTaskNode<'scope, T, F>>,
            scope: self,
            _marker: std::marker::PhantomData,
            cancel_token: new_cancel_slot::<AtomicStorage, ArcOwnership>(),
            waker_node: None,
            reclaim: None,
        }
    }

    pub fn spawn_boxed<T: Send + 'scope, F>(
        &self,
        future: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: Future<Output = T> + Send + 'scope,
    {
        self.spawn_boxed_to(self.runtime.choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'scope, M> GenericAsyncScope<'scope, LocalStorage, RcOwnership, M> {
    pub fn spawn<T: 'scope, S>(
        &self,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        S: LocalTask<T> + 'scope,
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

pub struct JoinHandle<
    'scope,
    'scope_ref,
    T,
    R: TaskHandleRef,
    S: ScopeProvider<'scope> = AsyncScope<'scope>,
> {
    pub(crate) task: R,
    pub(crate) ptr: *const (),
    pub(crate) take_result: unsafe fn(*const ()) -> Option<Result<T, TaskError>>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) _marker: std::marker::PhantomData<T>,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<NonNull<GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<unsafe fn(&S::Arena, *const ())>,
}

pub type LocalJoinHandle<'scope, 'scope_ref, T> = JoinHandle<'scope, 'scope_ref, T, LocalTaskRef>;
pub type SendJoinHandle<'scope, 'scope_ref, T> = JoinHandle<'scope, 'scope_ref, T, SendTaskRef>;

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>>
    JoinHandle<'scope, 'scope_ref, T, R, S>
{
    pub fn cancel(&self) {
        let mut cancel_slot = self.cancel_token.lock();
        if let Some(token) = cancel_slot.take() {
            token.cancel();
        }
        self.task.header().cancel();
    }

    pub fn cancel_token(&self) -> GenericCancellationToken<S::Storage, S::Ownership> {
        {
            let cancel_slot = self.cancel_token.lock();
            if let Some(token) = cancel_slot.as_ref() {
                return token.clone();
            }
        }

        let token = self.scope.completion().cancel_token().child();
        if self.task.header().is_cancelled() {
            token.cancel();
        }

        let mut cancel_slot = self.cancel_token.lock();
        if let Some(existing) = cancel_slot.as_ref() {
            existing.clone()
        } else {
            cancel_slot.replace(token.clone());
            token
        }
    }
}

impl<'scope, 'scope_ref, T: 'scope, R: TaskHandleRef, S: ScopeProvider<'scope>> Future
    for JoinHandle<'scope, 'scope_ref, T, R, S>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.scope
            .runtime()
            .drive_worker(Some(self.scope.completion()));

        if self.task.header().is_completed() {
            let res = unsafe { (self.take_result)(self.ptr) }.expect("task result already taken");
            if let Some(reclaim) = self.reclaim {
                unsafe { (reclaim)(self.scope.arena(), self.ptr) };
            }
            return Poll::Ready(res);
        }
        if self.scope.completion().is_cancelled() || self.task.header().is_cancelled() {
            return Poll::Ready(Err(TaskError::Cancelled));
        }

        let this = unsafe { self.get_unchecked_mut() };
        let header = this.task.header();
        if let Some(mut node_ptr) = this.waker_node {
            let node = unsafe { node_ptr.as_mut() };
            if !node.waker.will_wake(cx.waker()) {
                node.waker = cx.waker().clone();
                unsafe { header.register_completion(node as *mut _) };
            }
        } else {
            let node_ptr = unsafe {
                this.scope.arena().alloc_raw(
                    std::alloc::Layout::new::<GenericWakerNode<R::Storage>>(),
                    Some(|ptr| std::ptr::drop_in_place(ptr as *mut GenericWakerNode<R::Storage>)),
                ) as *mut GenericWakerNode<R::Storage>
            };
            unsafe {
                std::ptr::write(
                    node_ptr,
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        next: <R::Storage as Storage>::OptionPtr::new(None),
                    },
                );
            }
            this.waker_node = NonNull::new(node_ptr);
            unsafe { header.register_completion(node_ptr) };
        }
        Poll::Pending
    }
}

#[macro_export]
macro_rules! scope {
    ($scope_name:ident, $body:block) => {{
        let $scope_name = $crate::scope::AsyncScope::__private_new();
        let res = $body;
        $scope_name.wait_all().await;
        res
    }};
}

#[macro_export]
macro_rules! scope_local {
    ($scope_name:ident, $body:block) => {{
        let $scope_name = $crate::scope::LocalAsyncScope::__private_new();
        let res = $body;
        $scope_name.wait_all().await;
        res
    }};
}
