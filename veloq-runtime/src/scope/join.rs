use crate::runtime::{GenericCancellationToken, RuntimeShared};
use crate::task::{Arena, GenericWakerNode, RawTask, SendTaskRef, Task, TaskError, TaskHandleRef};
use crate::utils::storage::{AtomicStorage, StateLock};
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use veloq_atomic_waker::AtomicWaker;
use veloq_intrusive_linklist::Link;

use super::{CancelTokenSlot, ScopeProvider};

pub(crate) type ReclaimFn<'ctx, T, A> =
    unsafe fn(&A, &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx));

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> Result<T, TaskError>;
    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena);
}

pub(crate) struct RoutedSpawnReady<'ctx, T> {
    pub(crate) task: SendTaskRef<'ctx>,
    pub(crate) access: Box<dyn RoutedTaskAccess<T> + 'ctx>,
}

pub(crate) enum RoutedSpawnOutcome<'ctx, T> {
    Pending,
    Ready(RoutedSpawnReady<'ctx, T>),
    Failed(TaskError),
    Taken,
}

pub(crate) struct RoutedJobCell<'ctx, F> {
    job: Option<F>,
    _marker: std::marker::PhantomData<&'ctx ()>,
}

impl<'ctx, F> RoutedJobCell<'ctx, F> {
    pub(crate) fn new(job: F) -> Self {
        Self {
            job: Some(job),
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn take(&mut self) -> F {
        self.job.take().expect("routed job already taken")
    }
}

struct SpawnToAccess<'ctx, T, S_> {
    task: &'ctx S_,
    _marker: std::marker::PhantomData<T>,
}

impl<'ctx, T, S_> RoutedTaskAccess<T> for SpawnToAccess<'ctx, T, S_>
where
    S_: crate::task::SendTask<'ctx, T> + Sized + 'ctx,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.task.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, _arena: &dyn crate::task::Arena) {}
}

unsafe impl<'ctx, T, S_> Send for SpawnToAccess<'ctx, T, S_> where
    S_: crate::task::SendTask<'ctx, T> + Sized + 'ctx
{
}

pub(crate) fn make_spawn_to_access<'ctx, T, S_>(
    task: &'ctx S_,
) -> Box<dyn RoutedTaskAccess<T> + 'ctx>
where
    T: 'ctx,
    S_: crate::task::SendTask<'ctx, T> + Sized + 'ctx,
{
    Box::new(SpawnToAccess {
        task,
        _marker: std::marker::PhantomData,
    })
}

struct BoxedTaskAccess<'ctx, T, Fut> {
    node: &'ctx crate::task::SendBoxedTaskNode<'ctx, T, Fut>,
    _marker: std::marker::PhantomData<T>,
}

impl<'ctx, T, Fut> RoutedTaskAccess<T> for BoxedTaskAccess<'ctx, T, Fut>
where
    T: Send + 'ctx,
    Fut: Future<Output = T> + 'ctx,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.node.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena) {
        let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'ctx, T, Fut>>();
        unsafe {
            arena.drop_object_raw(self.node as *const _ as *mut u8, layout);
        }
    }
}

unsafe impl<'ctx, T, Fut> Send for BoxedTaskAccess<'ctx, T, Fut>
where
    T: Send + 'ctx,
    Fut: Future<Output = T> + 'ctx,
{
}

pub(crate) fn make_boxed_task_access<'ctx, T, Fut>(
    node: &'ctx crate::task::SendBoxedTaskNode<'ctx, T, Fut>,
) -> Box<dyn RoutedTaskAccess<T> + 'ctx>
where
    T: Send + 'ctx,
    Fut: Future<Output = T> + 'ctx,
{
    Box::new(BoxedTaskAccess {
        node,
        _marker: std::marker::PhantomData,
    })
}

pub(crate) struct RoutedSpawnState<'ctx, T> {
    pub(crate) outcome: Mutex<RoutedSpawnOutcome<'ctx, T>>,
    cancel_requested: std::sync::atomic::AtomicBool,
    waker: AtomicWaker,
}

impl<'ctx, T> RoutedSpawnState<'ctx, T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(RoutedSpawnOutcome::Pending),
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn request_cancel(&self) {
        self.cancel_requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.waker.wake();
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        self.cancel_requested
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn set_outcome(&self, new_outcome: RoutedSpawnOutcome<'ctx, T>) {
        let should_wake = {
            let mut outcome = self.outcome.lock().expect("routed spawn state poisoned");
            if matches!(*outcome, RoutedSpawnOutcome::Pending) {
                *outcome = new_outcome;
                true
            } else {
                false
            }
        };
        if should_wake {
            self.waker.wake();
        }
    }

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'ctx, T>) {
        self.set_outcome(RoutedSpawnOutcome::Ready(ready));
    }

    pub(crate) fn fail(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcome::Failed(err));
    }

    pub(crate) fn try_take_ready(&self) -> Result<Option<RoutedSpawnReady<'ctx, T>>, TaskError> {
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

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

pub(crate) fn dispatch_routed<
    'ctx,
    'scope,
    S: crate::utils::storage::Storage,
    O: crate::utils::ownership::Ownership,
    T,
    F,
    TExtra,
>(
    context: &crate::runtime::RuntimeScopeContext<'ctx, TExtra>,
    completion: &O::Shared<super::GenericScopeCompletion<'scope, S, O>>,
    state: Arc<RoutedSpawnState<'ctx, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<super::GenericScopeCompletion<'scope, S, O>>: Send + 'ctx,
    F: FnOnce() + Send + 'ctx,
    T: 'ctx,
{
    let completion_for_job = completion.clone();
    let state_for_job = state.clone();

    if context
        .route_to(worker_id, move || {
            let state_err = state_for_job.clone();
            let completion_err = completion_for_job.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                job();
            }));

            if let Err(panic_err) = result {
                completion_err.report_panic(panic_err);
                completion_err.cancel();
                state_err.fail(TaskError::Panic);
                completion_err.task_done();
            }
            std::future::ready(())
        })
        .is_err()
    {
        completion.task_done();
        state.fail(TaskError::Panic);
        panic!("failed to dispatch routed pinned task");
    }
}

pub(crate) fn install_routed_pinned_task<'ctx, T, Fut, TExtra>(
    runtime: &'ctx RuntimeShared<'ctx, TExtra>,
    arena: &crate::task::GenericArena<AtomicStorage>,
    completion: Arc<crate::scope::ScopeCompletion>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'ctx, T>>,
    future: Fut,
) where
    T: Send + 'ctx,
    Fut: Future<Output = T> + 'ctx,
{
    let scope_ref =
        crate::task::ScopeCompletionRef::new::<crate::utils::ownership::ArcOwnership>(&completion);
    let node = crate::task::SendBoxedTaskNode::new(future, &runtime.base, worker_id, scope_ref);
    let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'ctx, T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<crate::task::SendBoxedTaskNode<'ctx, T, Fut>>(
            layout,
            Some(|ptr| {
                std::ptr::drop_in_place(ptr as *mut crate::task::SendBoxedTaskNode<'ctx, T, Fut>)
            }),
        ) as *mut crate::task::SendBoxedTaskNode<'ctx, T, Fut>
    };
    unsafe { std::ptr::write(node_ptr, node) };

    let node_ref = unsafe { &*node_ptr };
    node_ref.header().set_pinned();

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header = task_ref.header();

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
        access: make_boxed_task_access(node_ref),
    });
}

pub(crate) struct ResolvedRoutedTask<'ctx, T, R: TaskHandleRef<'ctx>> {
    pub(crate) task: R,
    pub(crate) access: Option<Box<dyn RoutedTaskAccess<T> + 'ctx>>,
}

pub(crate) enum JoinSource<'ctx, T, R: TaskHandleRef<'ctx>> {
    Direct {
        task: R,
        gate: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx),
    },
    Routed {
        state: Arc<RoutedSpawnState<'ctx, T>>,
        resolved: Option<ResolvedRoutedTask<'ctx, T, R>>,
    },
}

pub struct JoinHandle<
    'ctx,
    'scope,
    T,
    R: TaskHandleRef<'ctx>,
    S: ScopeProvider<'ctx, TExtra>,
    TExtra,
> {
    pub(crate) source: JoinSource<'ctx, T, R>,
    pub(crate) scope: &'scope S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<Pin<&'ctx mut GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'ctx, T, S::Arena>>,
    pub(crate) _marker: std::marker::PhantomData<TExtra>,
}

unsafe impl<'ctx, 'scope, T, TExtra> Send
    for JoinHandle<
        'ctx,
        'scope,
        T,
        SendTaskRef<'ctx>,
        crate::scope::AsyncScope<'ctx, TExtra>,
        TExtra,
    >
where
    T: Send + 'ctx,
{
}

pub type LocalJoinHandle<'ctx, 'scope, T, TExtra> = JoinHandle<
    'ctx,
    'scope,
    T,
    crate::task::LocalTaskRef<'ctx>,
    crate::scope::AsyncScope<'ctx, TExtra>,
    TExtra,
>;
pub type SendJoinHandle<'ctx, 'scope, T, TExtra> =
    JoinHandle<'ctx, 'scope, T, SendTaskRef<'ctx>, crate::scope::AsyncScope<'ctx, TExtra>, TExtra>;
pub type LocalAsyncJoinHandle<'ctx, 'scope, T, TExtra> = JoinHandle<
    'ctx,
    'scope,
    T,
    crate::task::LocalTaskRef<'ctx>,
    crate::scope::LocalAsyncScope<'ctx, TExtra>,
    TExtra,
>;

impl<'ctx, 'scope, T, R: TaskHandleRef<'ctx>, S: ScopeProvider<'ctx, TExtra>, TExtra>
    JoinHandle<'ctx, 'scope, T, R, S, TExtra>
{
    pub fn cancel(&self) {
        let mut cancel_slot = self.cancel_token.lock();
        if let Some(token) = cancel_slot.take() {
            token.cancel();
        }

        match &self.source {
            JoinSource::Direct { task, .. } => {
                task.header().cancel();
            }
            JoinSource::Routed { state, resolved } => {
                state.request_cancel();
                if let Some(resolved) = resolved {
                    resolved.task.header().cancel();
                } else {
                    let outcome = state.outcome.lock().expect("routed spawn state poisoned");
                    if let RoutedSpawnOutcome::Ready(ready) = &*outcome {
                        ready.task.header().cancel();
                    }
                }
            }
        }
    }

    pub fn cancel_token(&self) -> GenericCancellationToken<S::Storage, S::Ownership> {
        {
            let cancel_slot = self.cancel_token.lock();
            if let Some(token) = cancel_slot.as_ref() {
                return token.clone();
            }
        }

        let token = self.scope.completion().cancel_token().child();
        let is_cancelled = match &self.source {
            JoinSource::Direct { task, .. } => task.header().is_cancelled(),
            JoinSource::Routed { state, resolved } => {
                if state.is_cancel_requested() {
                    true
                } else if let Some(resolved) = resolved {
                    resolved.task.header().is_cancelled()
                } else {
                    false
                }
            }
        };

        if is_cancelled {
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

    pub(crate) fn new_direct(
        scope: &'scope S,
        task: R,
        gate: &'ctx (dyn crate::task::TaskJoinGate<T> + 'ctx),
        reclaim: Option<ReclaimFn<'ctx, T, S::Arena>>,
    ) -> Self {
        Self {
            source: JoinSource::Direct { task, gate },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn new_routed(scope: &'scope S, state: Arc<RoutedSpawnState<'ctx, T>>) -> Self {
        Self {
            source: JoinSource::Routed {
                state,
                resolved: None,
            },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim: None,
            _marker: std::marker::PhantomData,
        }
    }

    fn register_waker_on<St: crate::utils::storage::Storage>(
        waker_node: &mut Option<Pin<&'ctx mut GenericWakerNode<St>>>,
        arena: &dyn crate::task::Arena,
        header: &crate::task::GenericTaskHeader<St>,
        cx: &mut Context<'_>,
    ) {
        if let Some(node) = waker_node {
            let mut node = node.as_mut();
            if !node.waker.will_wake(cx.waker()) {
                unsafe {
                    node.as_mut().get_unchecked_mut().waker = cx.waker().clone();
                    header.register_completion(node.as_mut());
                }
            }
        } else {
            let node_ptr = unsafe {
                arena.alloc_raw(
                    std::alloc::Layout::new::<GenericWakerNode<St>>(),
                    Some(|ptr| std::ptr::drop_in_place(ptr as *mut GenericWakerNode<St>)),
                ) as *mut GenericWakerNode<St>
            };
            let node_ptr = NonNull::new(node_ptr).expect("arena allocation failed");
            unsafe {
                std::ptr::write(
                    node_ptr.as_ptr(),
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        link: Link::new(),
                        _marker: std::marker::PhantomData,
                    },
                );
            }
            let node_ref = unsafe { Pin::new_unchecked(&mut *node_ptr.as_ptr()) };
            *waker_node = Some(node_ref);
            unsafe {
                header.register_completion(waker_node.as_mut().unwrap().as_mut());
            }
        }
    }
}

impl<'ctx, 'scope, T: 'ctx, S: ScopeProvider<'ctx, TExtra>, TExtra: 'ctx> Future
    for JoinHandle<'ctx, 'scope, T, crate::task::LocalTaskRef<'ctx>, S, TExtra>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker::<S::Storage, S::Ownership>(Some(this.scope.completion()));

        let arena = this.scope.arena();
        let completion = this.scope.completion();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate } => {
                let header = task.header();
                if header.is_completed() {
                    let res = gate
                        .take_result_erased()
                        .expect("task result already taken");
                    if let Some(reclaim) = reclaim {
                        unsafe { (reclaim)(arena, *gate) };
                    }
                    return Poll::Ready(res);
                }

                if completion.is_cancelled() || header.is_cancelled() {
                    return Poll::Ready(Err(TaskError::Cancelled));
                }

                Self::register_waker_on::<crate::utils::storage::LocalStorage>(
                    waker_node, arena, header, cx,
                );
                Poll::Pending
            }
            JoinSource::Routed { .. } => unreachable!("local join handle cannot be routed"),
        }
    }
}

impl<'ctx, 'scope, T: 'ctx, S: ScopeProvider<'ctx, TExtra>, TExtra: 'ctx> Future
    for JoinHandle<'ctx, 'scope, T, SendTaskRef<'ctx>, S, TExtra>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker::<S::Storage, S::Ownership>(Some(this.scope.completion()));

        let arena = this.scope.arena();
        let completion = this.scope.completion();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate } => {
                let header = task.header();
                if header.is_completed() {
                    let res = gate
                        .take_result_erased()
                        .expect("task result already taken");
                    if let Some(reclaim) = reclaim {
                        unsafe { (reclaim)(arena, *gate) };
                    }
                    return Poll::Ready(res);
                }

                if completion.is_cancelled() || header.is_cancelled() {
                    return Poll::Ready(Err(TaskError::Cancelled));
                }

                Self::register_waker_on::<AtomicStorage>(waker_node, arena, header, cx);
                Poll::Pending
            }
            JoinSource::Routed { state, resolved } => {
                loop {
                    if let Some(res) = resolved {
                        let header = res.task.header();
                        if header.is_completed() {
                            let access =
                                res.access.take().expect("routed task access already taken");
                            let output = access.take_result();
                            access.reclaim(arena);
                            return Poll::Ready(output);
                        }

                        if completion.is_cancelled() || header.is_cancelled() {
                            return Poll::Ready(Err(TaskError::Cancelled));
                        }

                        Self::register_waker_on::<AtomicStorage>(waker_node, arena, header, cx);
                        return Poll::Pending;
                    } else {
                        match state.try_take_ready() {
                            Ok(Some(ready)) => {
                                *resolved = Some(ResolvedRoutedTask {
                                    task: ready.task,
                                    access: Some(ready.access),
                                });
                                // Continue to poll the newly resolved task
                            }
                            Ok(None) => {
                                state.register(cx.waker());
                                // Double check to avoid race condition
                                if let Some(ready) = state.try_take_ready()? {
                                    *resolved = Some(ResolvedRoutedTask {
                                        task: ready.task,
                                        access: Some(ready.access),
                                    });
                                    continue;
                                }
                                return Poll::Pending;
                            }
                            Err(err) => return Poll::Ready(Err(err)),
                        }
                    }
                }
            }
        }
    }
}

impl<'ctx, 'scope, T, R: TaskHandleRef<'ctx>, S: ScopeProvider<'ctx, TExtra>, TExtra> Drop
    for JoinHandle<'ctx, 'scope, T, R, S, TExtra>
{
    fn drop(&mut self) {
        if let Some(node) = self.waker_node.as_mut() {
            let node_ptr = unsafe { NonNull::from(node.as_mut().get_unchecked_mut()) };
            let task = match &self.source {
                JoinSource::Direct { task, .. } => Some(*task),
                JoinSource::Routed { resolved, .. } => resolved.as_ref().map(|r| r.task),
            };

            if let Some(task) = task {
                let header = task.header();
                if !header.is_completed() {
                    unsafe {
                        header.remove_waker(node_ptr);
                    }
                }
            }
        }
    }
}
