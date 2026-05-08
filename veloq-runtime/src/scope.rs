use crate::runtime::{
    GenericCancellationToken, RuntimeShared, current_worker_id, with_current_context,
    with_current_runtime,
};
use crate::task::{
    Arena, GenericArena, GenericWakerNode, LocalBoxedTaskNode, LocalTask, LocalTaskRef,
    PinnedBoxedTaskNode, SendBoxedTaskNode, SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
};
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt, StateLock, Storage};
use std::any::Any;
use std::future::Future;
use std::ops::AsyncFnOnce;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use veloq_atomic_waker::AtomicWaker;
use veloq_intrusive_linklist::Link;

type ReclaimFn<'scope, T, A> = unsafe fn(&A, NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>);

type PinnedReclaimFn = unsafe fn(&dyn crate::task::Arena, *mut ());
type PinnedTakeResultFn = unsafe fn(*const (), *mut ());

enum RoutedSpawnOutcome {
    Pending,
    Ready(RoutedSpawnReady),
    Failed(TaskError),
    Taken,
}

struct RoutedSpawnReady {
    task: SendTaskRef,
    node_ptr: usize,
    take_result: PinnedTakeResultFn,
    reclaim: PinnedReclaimFn,
}

struct RoutedJobCell<'scope, F> {
    job: Option<F>,
    _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, F> RoutedJobCell<'scope, F> {
    fn new(job: F) -> Self {
        Self {
            job: Some(job),
            _marker: std::marker::PhantomData,
        }
    }

    fn take(&mut self) -> F {
        self.job.take().expect("routed job already taken")
    }
}

struct RoutedSpawnState {
    outcome: Mutex<RoutedSpawnOutcome>,
    cancel_requested: std::sync::atomic::AtomicBool,
    waker: AtomicWaker,
}

impl RoutedSpawnState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(RoutedSpawnOutcome::Pending),
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            waker: AtomicWaker::new(),
        })
    }

    fn request_cancel(&self) {
        self.cancel_requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.waker.wake();
    }

    fn is_cancel_requested(&self) -> bool {
        self.cancel_requested
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn set_ready(&self, ready: RoutedSpawnReady) {
        let should_wake = {
            let mut outcome = self.outcome.lock().expect("routed spawn state poisoned");
            if matches!(*outcome, RoutedSpawnOutcome::Pending) {
                *outcome = RoutedSpawnOutcome::Ready(ready);
                true
            } else {
                false
            }
        };
        if should_wake {
            self.waker.wake();
        }
    }

    fn fail(&self, err: TaskError) {
        let should_wake = {
            let mut outcome = self.outcome.lock().expect("routed spawn state poisoned");
            if matches!(*outcome, RoutedSpawnOutcome::Pending) {
                *outcome = RoutedSpawnOutcome::Failed(err);
                true
            } else {
                false
            }
        };
        if should_wake {
            self.waker.wake();
        }
    }

    fn try_take_ready(&self) -> Result<Option<RoutedSpawnReady>, TaskError> {
        let mut outcome = self.outcome.lock().expect("routed spawn state poisoned");
        match std::mem::replace(&mut *outcome, RoutedSpawnOutcome::Taken) {
            RoutedSpawnOutcome::Ready(ready) => Ok(Some(ready)),
            RoutedSpawnOutcome::Failed(err) => Err(err),
            RoutedSpawnOutcome::Pending => {
                *outcome = RoutedSpawnOutcome::Pending;
                Ok(None)
            }
            RoutedSpawnOutcome::Taken => {
                *outcome = RoutedSpawnOutcome::Taken;
                Ok(None)
            }
        }
    }

    fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

fn install_routed_pinned_task<'scope, T, Fut>(
    runtime: Arc<RuntimeShared>,
    arena_addr: usize,
    completion: Arc<GenericScopeCompletion<AtomicStorage, ArcOwnership>>,
    worker_id: usize,
    state: Arc<RoutedSpawnState>,
    future: Fut,
) where
    T: Send + 'scope,
    Fut: Future<Output = T>,
{
    let arena = unsafe { &*(arena_addr as *const GenericArena<AtomicStorage>) };
    let node = PinnedBoxedTaskNode::new(future);
    let layout = std::alloc::Layout::new::<PinnedBoxedTaskNode<'scope, T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<PinnedBoxedTaskNode<'scope, T, Fut>>(
            layout,
            Some(|ptr| std::ptr::drop_in_place(ptr as *mut PinnedBoxedTaskNode<'scope, T, Fut>)),
        ) as *mut PinnedBoxedTaskNode<'scope, T, Fut>
    };
    unsafe { std::ptr::write(node_ptr, node) };
    let node_ref = unsafe { &*node_ptr };

    node_ref.set_scope_completion::<AtomicStorage, ArcOwnership>(Some(completion.clone()));

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header = task_ref.header();
    header.set_runtime_info(Arc::as_ptr(&runtime), worker_id);
    if !runtime.enqueue_pinned(worker_id, task_ref) {
        unsafe { arena.drop_object_raw(node_ptr as *mut u8, layout) };
        state.fail(TaskError::Panic);
        completion.task_done();
        return;
    }

    if state.is_cancel_requested() {
        header.cancel();
    }

    state.set_ready(RoutedSpawnReady {
        task: task_ref,
        node_ptr: node_ptr as usize,
        take_result: PinnedBoxedTaskNode::<'scope, T, Fut>::take_result_into_erased,
        reclaim: PinnedBoxedTaskNode::<'scope, T, Fut>::reclaim_erased,
    });
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
        let completion = GenericScopeCompletion::<S, O>::new();

        // 自动发现当前正在运行的任务所属的作用域，建立父子层级关系
        let current_scope = crate::task::CURRENT_SCOPE.with(|s| s.borrow().clone());
        if let Some(parent) = current_scope {
            parent.try_link_child(&crate::task::ErasedCancellationToken::new::<S, O>(
                completion.cancel_token(),
            ));
        }

        Self {
            runtime,
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

        let worker_id = current_worker_id();
        let task_ref = unsafe { LocalTaskRef::from_concrete(task as *const TTask) };
        task_ref
            .header()
            .set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_local(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            gate: unsafe {
                NonNull::new_unchecked(
                    task as &dyn crate::task::TaskJoinGate<T>
                        as *const dyn crate::task::TaskJoinGate<T>
                        as *mut dyn crate::task::TaskJoinGate<T>,
                )
            },
            scope: self,
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
            gate: unsafe {
                NonNull::new_unchecked(node_ptr as *mut dyn crate::task::TaskJoinGate<T>)
            },
            scope: self,
            cancel_token: new_cancel_slot::<S, O>(),
            waker_node: None,
            reclaim: Some(|arena, gate| unsafe {
                let layout = std::alloc::Layout::new::<LocalBoxedTaskNode<'scope, T, F>>();
                arena.drop_object_raw(gate.as_ptr() as *mut u8, layout);
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
    fn spawn_send_impl<T: Send + 'scope, S>(
        &self,
        worker_id: usize,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S: SendTask<T> + Sized + 'scope,
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
        let header = task_ref.header();
        header.set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_send(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            gate: unsafe {
                NonNull::new_unchecked(
                    task as &dyn crate::task::TaskJoinGate<T>
                        as *const dyn crate::task::TaskJoinGate<T>
                        as *mut dyn crate::task::TaskJoinGate<T>,
                )
            },
            scope: self,
            cancel_token: new_cancel_slot::<AtomicStorage, ArcOwnership>(),
            waker_node: None,
            reclaim: None,
        }
    }

    pub fn spawn_to<T: Send + 'scope, S>(
        &self,
        worker_id: usize,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S: SendTask<T> + Sized + 'scope,
    {
        self.spawn_send_impl(worker_id, task)
    }

    pub fn spawn<T: Send + 'scope, S>(
        &self,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        S: SendTask<T> + Sized + 'scope,
    {
        self.spawn_send_impl(self.runtime.choose_worker(), task)
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
        let header = task_ref.header();
        header.set_runtime_info(Arc::as_ptr(&self.runtime), worker_id);
        self.runtime.enqueue_send(worker_id, task_ref);

        JoinHandle {
            task: task_ref,
            gate: unsafe {
                NonNull::new_unchecked(node_ptr as *mut dyn crate::task::TaskJoinGate<T>)
            },
            scope: self,
            cancel_token: new_cancel_slot::<AtomicStorage, ArcOwnership>(),
            waker_node: None,
            reclaim: None,
        }
    }

    pub fn spawn_boxed_to<T: Send + 'scope, F>(
        &self,
        worker_id: usize,
        job: F,
    ) -> RoutedJoinHandle<'scope, '_, M, T>
    where
        F: AsyncFnOnce() -> T + Send + 'scope,
    {
        debug_assert!(
            worker_id < self.runtime.worker_count().get(),
            "worker_id {} is out of bounds (max {})",
            worker_id,
            self.runtime.worker_count().get()
        );

        let state = RoutedSpawnState::new();
        self.completion.add_task();

        let Some(dispatcher) = with_current_context(|ctx| ctx.worker_route_dispatcher()) else {
            self.completion.task_done();
            panic!("runtime context not set");
        };

        let runtime = self.runtime.clone();
        let completion = self.completion.clone();
        let arena_addr = &self.arena as *const _ as usize;
        let state_for_job = state.clone();
        let job_layout = std::alloc::Layout::new::<RoutedJobCell<'scope, F>>();
        let job_cell = RoutedJobCell::new(job);
        let job_ptr = unsafe {
            self.arena.alloc::<RoutedJobCell<'scope, F>>(
                job_layout,
                Some(|ptr| {
                    std::ptr::drop_in_place(ptr as *mut RoutedJobCell<'scope, F>);
                }),
            ) as *mut RoutedJobCell<'scope, F>
        };
        unsafe { std::ptr::write(job_ptr, job_cell) };
        let job_addr = job_ptr as usize;

        if !dispatcher.dispatch(worker_id, move || {
            let arena = unsafe { &*(arena_addr as *const GenericArena<AtomicStorage>) };
            let mut job_reclaimed = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if state_for_job.is_cancel_requested() {
                    unsafe { arena.drop_object_raw(job_addr as *mut u8, job_layout) };
                    job_reclaimed = true;
                    state_for_job.fail(TaskError::Cancelled);
                    completion.task_done();
                    return;
                }

                let job_cell = unsafe { &mut *(job_addr as *mut RoutedJobCell<'scope, F>) };
                let job = job_cell.take();
                let future = job();

                unsafe { arena.drop_object_raw(job_addr as *mut u8, job_layout) };
                job_reclaimed = true;

                if state_for_job.is_cancel_requested() {
                    state_for_job.fail(TaskError::Cancelled);
                    completion.task_done();
                    return;
                }

                install_routed_pinned_task(
                    runtime.clone(),
                    arena_addr,
                    completion.clone(),
                    worker_id,
                    state_for_job.clone(),
                    future,
                );
            }));

            if result.is_err() {
                if !job_reclaimed {
                    unsafe { arena.drop_object_raw(job_addr as *mut u8, job_layout) };
                }
                state_for_job.fail(TaskError::Panic);
                completion.task_done();
            }
        }) {
            unsafe { self.arena.drop_object_raw(job_addr as *mut u8, job_layout) };
            self.completion.task_done();
            panic!("failed to dispatch routed pinned task");
        }

        RoutedJoinHandle::new(self, state)
    }

    pub fn spawn_boxed<T: Send + 'scope, F>(
        &self,
        future: F,
    ) -> JoinHandle<'scope, '_, T, SendTaskRef, Self>
    where
        F: Future<Output = T> + Send + 'scope,
    {
        self.spawn_boxed_send_impl(self.runtime.choose_worker(), future)
    }
}

// 本地作用域特有方法
impl<'scope, M> GenericAsyncScope<'scope, LocalStorage, RcOwnership, M> {
    pub fn spawn<T: 'scope, S>(
        &self,
        task: &'scope S,
    ) -> JoinHandle<'scope, '_, T, LocalTaskRef, Self>
    where
        S: LocalTask<T> + Sized + 'scope,
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

pub struct JoinHandle<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>> {
    pub(crate) task: R,
    pub(crate) gate: NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<NonNull<GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
}

unsafe impl<'scope, 'scope_ref, T> Send
    for JoinHandle<'scope, 'scope_ref, T, SendTaskRef, AsyncScope<'scope>>
where
    T: Send + 'scope,
{
}

pub type LocalJoinHandle<'scope, 'scope_ref, T> =
    JoinHandle<'scope, 'scope_ref, T, LocalTaskRef, AsyncScope<'scope>>;
pub type SendJoinHandle<'scope, 'scope_ref, T> =
    JoinHandle<'scope, 'scope_ref, T, SendTaskRef, AsyncScope<'scope>>;
pub type LocalAsyncJoinHandle<'scope, 'scope_ref, T> =
    JoinHandle<'scope, 'scope_ref, T, LocalTaskRef, LocalAsyncScope<'scope>>;

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
            let res = unsafe { self.gate.as_ref().take_result_erased() }
                .expect("task result already taken");
            if let Some(reclaim) = self.reclaim {
                unsafe { (reclaim)(self.scope.arena(), self.gate) };
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
                unsafe { header.register_completion(node_ptr.as_ptr()) };
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
                        link: Link::new(),
                        _marker: std::marker::PhantomData,
                    },
                );
            }
            this.waker_node = NonNull::new(node_ptr);
            unsafe { header.register_completion(node_ptr) };
        }
        Poll::Pending
    }
}

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>> Drop
    for JoinHandle<'scope, 'scope_ref, T, R, S>
{
    fn drop(&mut self) {
        if let Some(node_ptr) = self.waker_node {
            let header = self.task.header();
            if !header.is_completed() {
                let mut wakers = header.wakers.lock();
                if unsafe { node_ptr.as_ref().link.is_linked() } {
                    unsafe {
                        let mut cursor = wakers.cursor_mut_from_ptr(node_ptr);
                        cursor.remove();
                    }
                }
            }
        }
    }
}

pub struct RoutedJoinHandle<'scope, 'scope_ref, M, T> {
    scope: &'scope_ref GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, M>,
    state: Arc<RoutedSpawnState>,
    task: Option<SendTaskRef>,
    node_ptr: Option<usize>,
    take_result: Option<PinnedTakeResultFn>,
    reclaim: Option<PinnedReclaimFn>,
    cancel_token: CancelTokenSlot<AtomicStorage, ArcOwnership>,
    waker_node: Option<NonNull<GenericWakerNode<AtomicStorage>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<'scope, 'scope_ref, M, T> RoutedJoinHandle<'scope, 'scope_ref, M, T> {
    fn new(
        scope: &'scope_ref GenericAsyncScope<'scope, AtomicStorage, ArcOwnership, M>,
        state: Arc<RoutedSpawnState>,
    ) -> Self {
        Self {
            scope,
            state,
            task: None,
            node_ptr: None,
            take_result: None,
            reclaim: None,
            cancel_token: new_cancel_slot::<AtomicStorage, ArcOwnership>(),
            waker_node: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, 'scope_ref, M, T> RoutedJoinHandle<'scope, 'scope_ref, M, T>
where
    T: Send + 'scope,
{
    pub fn cancel(&self) {
        self.state.request_cancel();

        if let Some(task) = self.task {
            task.header().cancel();
            return;
        }

        let outcome = self
            .state
            .outcome
            .lock()
            .expect("routed spawn state poisoned");
        if let RoutedSpawnOutcome::Ready(ready) = &*outcome {
            ready.task.header().cancel();
        }
    }

    pub fn cancel_token(&self) -> GenericCancellationToken<AtomicStorage, ArcOwnership> {
        {
            let cancel_slot = self.cancel_token.lock();
            if let Some(token) = cancel_slot.as_ref() {
                return token.clone();
            }
        }

        let token = self.scope.completion().cancel_token().child();
        if let Some(task) = self.task
            && task.header().is_cancelled()
        {
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

    fn install_ready(&mut self, ready: RoutedSpawnReady) {
        self.task = Some(ready.task);
        self.node_ptr = Some(ready.node_ptr);
        self.take_result = Some(ready.take_result);
        self.reclaim = Some(ready.reclaim);
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, TaskError>> {
        let task = self.task.expect("routed spawn task missing");
        self.scope
            .runtime()
            .drive_worker(Some(self.scope.completion()));

        if task.header().is_completed() {
            let node_ptr = self.node_ptr.expect("routed spawn node pointer missing");
            let take_result = self.take_result.expect("routed spawn take_result missing");
            let mut result = std::mem::MaybeUninit::<Result<T, TaskError>>::uninit();
            unsafe {
                take_result(node_ptr as *const (), result.as_mut_ptr() as *mut ());
            }
            let res = unsafe { result.assume_init() };
            if let Some(reclaim) = self.reclaim.take() {
                unsafe { reclaim(self.scope.arena(), node_ptr as *mut ()) };
            }
            return Poll::Ready(res);
        }
        if self.scope.completion().is_cancelled() || task.header().is_cancelled() {
            return Poll::Ready(Err(TaskError::Cancelled));
        }

        let header = task.header();
        if let Some(mut node_ptr) = self.waker_node {
            let node = unsafe { node_ptr.as_mut() };
            if !node.waker.will_wake(cx.waker()) {
                node.waker = cx.waker().clone();
                unsafe { header.register_completion(node_ptr.as_ptr()) };
            }
        } else {
            let node_ptr = unsafe {
                self.scope.arena().alloc_raw(
                    std::alloc::Layout::new::<GenericWakerNode<AtomicStorage>>(),
                    Some(|ptr| {
                        std::ptr::drop_in_place(ptr as *mut GenericWakerNode<AtomicStorage>)
                    }),
                ) as *mut GenericWakerNode<AtomicStorage>
            };
            unsafe {
                std::ptr::write(
                    node_ptr,
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        link: Link::new(),
                        _marker: std::marker::PhantomData,
                    },
                );
            }
            self.waker_node = NonNull::new(node_ptr);
            unsafe { header.register_completion(node_ptr) };
        }
        Poll::Pending
    }
}

impl<'scope, 'scope_ref, M, T: Send + 'scope> Future
    for RoutedJoinHandle<'scope, 'scope_ref, M, T>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.task.is_some() {
            return this.poll_ready(cx);
        }

        this.scope
            .runtime()
            .drive_worker(Some(this.scope.completion()));

        match this.state.try_take_ready() {
            Ok(Some(ready)) => {
                this.install_ready(ready);
                this.poll_ready(cx)
            }
            Ok(None) => {
                this.state.register(cx.waker());
                match this.state.try_take_ready() {
                    Ok(Some(ready)) => {
                        this.install_ready(ready);
                        this.poll_ready(cx)
                    }
                    Ok(None) => Poll::Pending,
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

impl<'scope, 'scope_ref, M, T> Drop for RoutedJoinHandle<'scope, 'scope_ref, M, T> {
    fn drop(&mut self) {
        if let Some(node_ptr) = self.waker_node
            && let Some(task) = self.task
        {
            let header = task.header();
            if !header.is_completed() {
                let mut wakers = header.wakers.lock();
                if unsafe { node_ptr.as_ref().link.is_linked() } {
                    unsafe {
                        let mut cursor = wakers.cursor_mut_from_ptr(node_ptr);
                        cursor.remove();
                    }
                }
            }
        }
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
