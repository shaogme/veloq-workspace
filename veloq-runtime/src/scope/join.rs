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

pub(crate) type ReclaimFn<'scope, T, A> =
    unsafe fn(&A, NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>);

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> Result<T, TaskError>;
    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena);
}

pub(crate) struct RoutedSpawnReady<'scope, T> {
    pub(crate) task: SendTaskRef,
    pub(crate) access: Box<dyn RoutedTaskAccess<T> + 'scope>,
}

pub(crate) enum RoutedSpawnOutcome<'scope, T> {
    Pending,
    Ready(RoutedSpawnReady<'scope, T>),
    Failed(TaskError),
    Taken,
}

pub(crate) struct RoutedJobCell<'scope, F> {
    job: Option<F>,
    _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, F> RoutedJobCell<'scope, F> {
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

struct SpawnToAccess<'scope, T, S_> {
    task: NonNull<S_>,
    _marker: std::marker::PhantomData<(&'scope (), T)>,
}

impl<'scope, T, S_> RoutedTaskAccess<T> for SpawnToAccess<'scope, T, S_>
where
    S_: crate::task::SendTask<T> + Sized + 'scope,
{
    fn take_result(&self) -> Result<T, TaskError> {
        unsafe {
            self.task
                .as_ref()
                .take_result()
                .expect("task result already taken")
        }
    }

    fn reclaim(self: Box<Self>, _arena: &dyn crate::task::Arena) {}
}

unsafe impl<'scope, T, S_> Send for SpawnToAccess<'scope, T, S_> where
    S_: crate::task::SendTask<T> + Sized + 'scope
{
}

pub(crate) fn make_spawn_to_access<'scope, T, S_>(
    task: NonNull<S_>,
) -> Box<dyn RoutedTaskAccess<T> + 'scope>
where
    T: 'scope,
    S_: crate::task::SendTask<T> + Sized + 'scope,
{
    Box::new(SpawnToAccess {
        task,
        _marker: std::marker::PhantomData,
    })
}

struct BoxedTaskAccess<'scope, T, Fut> {
    node: NonNull<crate::task::SendBoxedTaskNode<'scope, T, Fut>>,
    _marker: std::marker::PhantomData<(&'scope (), T, Fut)>,
}

impl<'scope, T, Fut> RoutedTaskAccess<T> for BoxedTaskAccess<'scope, T, Fut>
where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
    fn take_result(&self) -> Result<T, TaskError> {
        unsafe {
            self.node
                .as_ref()
                .take_result()
                .expect("task result already taken")
        }
    }

    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena) {
        let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'scope, T, Fut>>();
        unsafe {
            arena.drop_object_raw(self.node.as_ptr() as *mut u8, layout);
        }
    }
}

unsafe impl<'scope, T, Fut> Send for BoxedTaskAccess<'scope, T, Fut>
where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
}

pub(crate) fn make_boxed_task_access<'scope, T, Fut>(
    node: NonNull<crate::task::SendBoxedTaskNode<'scope, T, Fut>>,
) -> Box<dyn RoutedTaskAccess<T> + 'scope>
where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
    Box::new(BoxedTaskAccess {
        node,
        _marker: std::marker::PhantomData,
    })
}

pub(crate) struct RoutedSpawnState<'scope, T> {
    pub(crate) outcome: Mutex<RoutedSpawnOutcome<'scope, T>>,
    cancel_requested: std::sync::atomic::AtomicBool,
    waker: AtomicWaker,
}

impl<'scope, T> RoutedSpawnState<'scope, T> {
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

    fn set_outcome(&self, new_outcome: RoutedSpawnOutcome<'scope, T>) {
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

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'scope, T>) {
        self.set_outcome(RoutedSpawnOutcome::Ready(ready));
    }

    pub(crate) fn fail(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcome::Failed(err));
    }

    pub(crate) fn try_take_ready(&self) -> Result<Option<RoutedSpawnReady<'scope, T>>, TaskError> {
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
    'scope,
    S: crate::utils::storage::Storage,
    O: crate::utils::ownership::Ownership,
    T,
    F,
    TExtra,
>(
    context: &crate::runtime::RuntimeScopeContext<TExtra>,
    completion: &O::Shared<super::GenericScopeCompletion<S, O>>,
    state: Arc<RoutedSpawnState<'scope, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<super::GenericScopeCompletion<S, O>>: Send + 'scope,
    F: FnOnce() + Send + 'scope,
    TExtra: crate::runtime::context::RuntimeContextExtra,
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

            if result.is_err() {
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

pub(crate) fn install_routed_pinned_task<'scope, T, Fut, TExtra>(
    runtime: &Arc<RuntimeShared<TExtra>>,
    arena: &crate::task::GenericArena<AtomicStorage>,
    completion: Arc<crate::scope::ScopeCompletion>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'scope, T>>,
    future: Fut,
) where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
    TExtra: crate::runtime::context::RuntimeContextExtra,
{
    let node = crate::task::SendBoxedTaskNode::new(future);
    let layout = std::alloc::Layout::new::<crate::task::SendBoxedTaskNode<'scope, T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<crate::task::SendBoxedTaskNode<'scope, T, Fut>>(
            layout,
            Some(|ptr| {
                std::ptr::drop_in_place(ptr as *mut crate::task::SendBoxedTaskNode<'scope, T, Fut>)
            }),
        ) as *mut crate::task::SendBoxedTaskNode<'scope, T, Fut>
    };
    unsafe { std::ptr::write(node_ptr, node) };

    let node_ref = unsafe { &*node_ptr };
    node_ref.header().set_pinned();
    node_ref.set_scope_completion::<AtomicStorage, crate::utils::ownership::ArcOwnership>(Some(
        completion.clone(),
    ));

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header = task_ref.header();
    header.set_runtime_info(Some(&runtime.base), worker_id);

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
        access: make_boxed_task_access(unsafe { NonNull::new_unchecked(node_ptr) }),
    });
}

pub(crate) struct ResolvedRoutedTask<'scope, T, R: TaskHandleRef> {
    pub(crate) task: R,
    pub(crate) access: Option<Box<dyn RoutedTaskAccess<T> + 'scope>>,
}

pub(crate) enum JoinSource<'scope, T, R: TaskHandleRef> {
    Direct {
        task: R,
        gate: NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>,
    },
    Routed {
        state: Arc<RoutedSpawnState<'scope, T>>,
        resolved: Option<ResolvedRoutedTask<'scope, T, R>>,
    },
}

pub struct JoinHandle<
    'scope,
    'scope_ref,
    T,
    R: TaskHandleRef,
    S: ScopeProvider<'scope, TExtra>,
    TExtra,
> {
    pub(crate) source: JoinSource<'scope, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<NonNull<GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
    pub(crate) _marker: std::marker::PhantomData<TExtra>,
}

unsafe impl<'scope, 'scope_ref, T, TExtra> Send
    for JoinHandle<
        'scope,
        'scope_ref,
        T,
        SendTaskRef,
        crate::scope::AsyncScope<'scope, TExtra>,
        TExtra,
    >
where
    T: Send + 'scope,
{
}

pub type LocalJoinHandle<'scope, 'scope_ref, T, TExtra> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    crate::task::LocalTaskRef,
    crate::scope::AsyncScope<'scope, TExtra>,
    TExtra,
>;
pub type SendJoinHandle<'scope, 'scope_ref, T, TExtra> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    SendTaskRef,
    crate::scope::AsyncScope<'scope, TExtra>,
    TExtra,
>;
pub type LocalAsyncJoinHandle<'scope, 'scope_ref, T, TExtra> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    crate::task::LocalTaskRef,
    crate::scope::LocalAsyncScope<'scope, TExtra>,
    TExtra,
>;

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope, TExtra>, TExtra>
    JoinHandle<'scope, 'scope_ref, T, R, S, TExtra>
where
    TExtra: crate::runtime::context::RuntimeContextExtra,
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
        scope: &'scope_ref S,
        task: R,
        gate: NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>,
        reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
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

    pub(crate) fn new_routed(
        scope: &'scope_ref S,
        state: Arc<RoutedSpawnState<'scope, T>>,
    ) -> Self {
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
        waker_node: &mut Option<NonNull<GenericWakerNode<St>>>,
        arena: &dyn crate::task::Arena,
        header: &crate::task::GenericTaskHeader<St>,
        cx: &mut Context<'_>,
    ) {
        if let Some(mut node_ptr) = *waker_node {
            let node = unsafe { node_ptr.as_mut() };
            if !node.waker.will_wake(cx.waker()) {
                node.waker = cx.waker().clone();
                unsafe { header.register_completion(node_ptr) };
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
            *waker_node = Some(node_ptr);
            unsafe { header.register_completion(node_ptr) };
        }
    }
}

impl<'scope, 'scope_ref, T: 'scope, S: ScopeProvider<'scope, TExtra>, TExtra> Future
    for JoinHandle<'scope, 'scope_ref, T, crate::task::LocalTaskRef, S, TExtra>
where
    TExtra: crate::runtime::context::RuntimeContextExtra,
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker(Some(this.scope.completion()));

        let arena = this.scope.arena();
        let completion = this.scope.completion();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate } => {
                let header = task.header();
                if header.is_completed() {
                    let res = unsafe { gate.as_ref().take_result_erased() }
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

impl<'scope, 'scope_ref, T: 'scope, S: ScopeProvider<'scope, TExtra>, TExtra> Future
    for JoinHandle<'scope, 'scope_ref, T, SendTaskRef, S, TExtra>
where
    TExtra: crate::runtime::context::RuntimeContextExtra,
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker(Some(this.scope.completion()));

        let arena = this.scope.arena();
        let completion = this.scope.completion();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate } => {
                let header = task.header();
                if header.is_completed() {
                    let res = unsafe { gate.as_ref().take_result_erased() }
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

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope, TExtra>, TExtra> Drop
    for JoinHandle<'scope, 'scope_ref, T, R, S, TExtra>
{
    fn drop(&mut self) {
        if let Some(node_ptr) = self.waker_node {
            let task = match &self.source {
                JoinSource::Direct { task, .. } => Some(*task),
                JoinSource::Routed { resolved, .. } => resolved.as_ref().map(|r| r.task),
            };

            if let Some(task) = task {
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
}
