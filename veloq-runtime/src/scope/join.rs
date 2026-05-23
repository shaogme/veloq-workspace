use crate::runtime::{GenericCancellationToken, RuntimeShared};
use crate::task::{
    Arena, GenericTaskHeader, GenericWakerNode, RawScope, RawTask, SendBoxedTaskNode, SendTaskRef,
    Task, TaskError, TaskHandleRef,
};
use crate::utils::storage::{AtomicStorage, StateLock};
use std::alloc::Layout;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::replace;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::ptr::{NonNull, drop_in_place, write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use veloq_atomic_waker::AtomicWaker;
use veloq_intrusive_linklist::Link;

use super::{CancelTokenSlot, ScopeProvider};

pub(crate) type ReclaimFn<'scope, T, A> =
    unsafe fn(&A, &'scope (dyn crate::task::TaskJoinGate<T> + 'scope));

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> Result<T, TaskError>;
    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena);
}

pub(crate) struct RoutedSpawnReady<'scope, T> {
    pub(crate) task: SendTaskRef<'scope>,
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
    _marker: PhantomData<&'scope ()>,
}

impl<'scope, F> RoutedJobCell<'scope, F> {
    pub(crate) fn new(job: F) -> Self {
        Self {
            job: Some(job),
            _marker: PhantomData,
        }
    }

    pub(crate) fn take(&mut self) -> F {
        self.job.take().expect("routed job already taken")
    }
}

struct SpawnToAccess<'scope, T, S_> {
    task: &'scope S_,
    _marker: PhantomData<(T,)>,
}

impl<'scope, T, S_> RoutedTaskAccess<T> for SpawnToAccess<'scope, T, S_>
where
    S_: crate::task::SendTask<'scope, T> + Sized + 'scope,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.task.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, _arena: &dyn crate::task::Arena) {}
}

unsafe impl<'scope, T, S_> Send for SpawnToAccess<'scope, T, S_> where
    S_: crate::task::SendTask<'scope, T> + Sized + 'scope
{
}

pub(crate) fn make_spawn_to_access<'scope, T, S_>(
    task: &'scope S_,
) -> Box<dyn RoutedTaskAccess<T> + 'scope>
where
    T: 'scope,
    S_: crate::task::SendTask<'scope, T> + Sized + 'scope,
{
    Box::new(SpawnToAccess {
        task,
        _marker: PhantomData,
    })
}

struct BoxedTaskAccess<'scope, T, Fut> {
    node: &'scope SendBoxedTaskNode<'scope, T, Fut>,
    _marker: PhantomData<T>,
}

impl<'scope, T, Fut> RoutedTaskAccess<T> for BoxedTaskAccess<'scope, T, Fut>
where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.node.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena) {
        let layout = Layout::new::<SendBoxedTaskNode<'scope, T, Fut>>();
        unsafe {
            arena.drop_object_raw(self.node as *const _ as *mut u8, layout);
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
    node: &'scope SendBoxedTaskNode<'scope, T, Fut>,
) -> Box<dyn RoutedTaskAccess<T> + 'scope>
where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
    Box::new(BoxedTaskAccess {
        node,
        _marker: PhantomData,
    })
}

pub(crate) struct RoutedSpawnState<'scope, T> {
    pub(crate) outcome: Mutex<RoutedSpawnOutcome<'scope, T>>,
    cancel_requested: AtomicBool,
    waker: AtomicWaker,
}

impl<'scope, T> RoutedSpawnState<'scope, T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(RoutedSpawnOutcome::Pending),
            cancel_requested: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn request_cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        self.waker.wake();
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
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
        match replace(&mut *outcome, RoutedSpawnOutcome::Taken) {
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
    'ctx,
    S: crate::utils::storage::Storage,
    O: crate::utils::ownership::Ownership + 'scope + 'ctx,
    T,
    F,
    TExtra,
>(
    context: &crate::runtime::RuntimeScopeContext<'ctx, TExtra>,
    completion: &O::Shared<super::GenericScopeCompletion<'scope, S, O>>,
    state: Arc<RoutedSpawnState<'scope, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<super::GenericScopeCompletion<'scope, S, O>>: Send + 'scope,
    F: FnOnce() + Send + 'scope,
    T: 'scope + 'ctx,
{
    let completion_raw_ptr =
        O::as_ptr(completion) as *const super::GenericScopeCompletion<'scope, S, O> as *const ();
    let completion_send_ptr =
        super::SendPtr::new(NonNull::new(completion_raw_ptr as *mut ()).unwrap());

    let state_raw_ptr = Arc::as_ptr(&state) as *const RoutedSpawnState<'scope, T> as *const ();
    let state_send_ptr = super::SendPtr::new(NonNull::new(state_raw_ptr as *mut ()).unwrap());

    let job_boxed: Box<dyn FnOnce() + Send + 'scope> = Box::new(job);
    let job_ctx = unsafe {
        std::mem::transmute::<Box<dyn FnOnce() + Send + 'scope>, Box<dyn FnOnce() + Send + 'static>>(
            job_boxed,
        )
    };

    if context
        .route_to(worker_id, move || {
            let state_ref: &RoutedSpawnState<'scope, T> =
                unsafe { &*(state_send_ptr.as_ptr() as *const RoutedSpawnState<'scope, T>) };
            let completion_ref: &super::GenericScopeCompletion<'scope, S, O> = unsafe {
                &*(completion_send_ptr.as_ptr()
                    as *const super::GenericScopeCompletion<'scope, S, O>)
            };
            let result = catch_unwind(AssertUnwindSafe(move || {
                job_ctx();
            }));

            if let Err(panic_err) = result {
                completion_ref.report_panic(panic_err);
                completion_ref.cancel();
                state_ref.fail(TaskError::Panic);
                completion_ref.task_done();
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

pub(crate) fn install_routed_pinned_task<'scope, 'rt, T, Fut, TExtra>(
    runtime: &'rt RuntimeShared<TExtra>,
    arena: &crate::task::GenericArena<AtomicStorage>,
    completion: Arc<crate::scope::ScopeCompletion<'scope>>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'scope, T>>,
    future: Fut,
) where
    T: Send + 'scope,
    Fut: Future<Output = T> + 'scope,
{
    let scope_ref = unsafe { RawScope::clone_ref(&*completion) };
    let node = SendBoxedTaskNode::new(future);
    let node_header_ptr = &node.header as *const GenericTaskHeader<'scope, AtomicStorage>;
    unsafe {
        (*node_header_ptr).initialize(&runtime.base, worker_id, scope_ref);
    }
    let layout = Layout::new::<SendBoxedTaskNode<'scope, T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<SendBoxedTaskNode<'scope, T, Fut>>(
            layout,
            Some(|ptr| drop_in_place(ptr as *mut SendBoxedTaskNode<'scope, T, Fut>)),
        ) as *mut SendBoxedTaskNode<'scope, T, Fut>
    };
    unsafe { write(node_ptr, node) };

    let node_ref = unsafe { &*node_ptr };
    node_ref.header().set_pinned();

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header_ptr = task_ref.header() as *const GenericTaskHeader<'scope, AtomicStorage>
        as *const GenericTaskHeader<'static, AtomicStorage>;
    let task_ctx = unsafe { SendTaskRef::from_header(header_ptr) };
    let header = task_ref.header();

    if !runtime.enqueue_pinned(worker_id, task_ctx) {
        unsafe { arena.drop_object_raw(node_ptr as *mut u8, layout) };
        state.fail(TaskError::Panic);
        completion.task_done();
        return;
    }

    if state.is_cancel_requested() {
        header.cancel();
    }

    let task_ready = unsafe {
        SendTaskRef::from_header(
            task_ref.header() as *const GenericTaskHeader<'scope, AtomicStorage>
        )
    };

    state.set_ready(RoutedSpawnReady {
        task: task_ready,
        access: make_boxed_task_access(node_ref),
    });
}

pub(crate) struct ResolvedRoutedTask<'scope, T, R: TaskHandleRef<'scope>> {
    pub(crate) task: R,
    pub(crate) access: Option<Box<dyn RoutedTaskAccess<T> + 'scope>>,
}

pub(crate) enum JoinSource<'scope, T, R: TaskHandleRef<'scope>> {
    Direct {
        task: R,
        gate: &'scope (dyn crate::task::TaskJoinGate<T> + 'scope),
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
    R: TaskHandleRef<'scope>,
    S: ScopeProvider<'scope, TExtra>,
    TExtra,
> {
    pub(crate) source: JoinSource<'scope, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<'scope, S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<Pin<&'scope mut GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
    pub(crate) _marker: std::marker::PhantomData<TExtra>,
}

unsafe impl<'scope, 'scope_ref, T, TExtra> Send
    for JoinHandle<
        'scope,
        'scope_ref,
        T,
        SendTaskRef<'scope>,
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
    crate::task::LocalTaskRef<'scope>,
    crate::scope::AsyncScope<'scope, TExtra>,
    TExtra,
>;
pub type SendJoinHandle<'scope, 'scope_ref, T, TExtra> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    SendTaskRef<'scope>,
    crate::scope::AsyncScope<'scope, TExtra>,
    TExtra,
>;
pub type LocalAsyncJoinHandle<'scope, 'scope_ref, T, TExtra> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    crate::task::LocalTaskRef<'scope>,
    crate::scope::LocalAsyncScope<'scope, TExtra>,
    TExtra,
>;

impl<'scope, 'scope_ref, T, R: TaskHandleRef<'scope>, S: ScopeProvider<'scope, TExtra>, TExtra>
    JoinHandle<'scope, 'scope_ref, T, R, S, TExtra>
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

    pub fn cancel_token(&self) -> GenericCancellationToken<'scope, S::Storage, S::Ownership> {
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
        gate: &'scope (dyn crate::task::TaskJoinGate<T> + 'scope),
        reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
    ) -> Self {
        Self {
            source: JoinSource::Direct { task, gate },
            scope,
            cancel_token: super::new_cancel_slot::<'scope, S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim,
            _marker: PhantomData,
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
            cancel_token: super::new_cancel_slot::<'scope, S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim: None,
            _marker: PhantomData,
        }
    }

    fn register_waker_on<St: crate::utils::storage::Storage>(
        waker_node: &mut Option<Pin<&'scope mut GenericWakerNode<St>>>,
        arena: &dyn crate::task::Arena,
        header: &crate::task::GenericTaskHeader<'scope, St>,
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
                    Layout::new::<GenericWakerNode<St>>(),
                    Some(|ptr| drop_in_place(ptr as *mut GenericWakerNode<St>)),
                ) as *mut GenericWakerNode<St>
            };
            let node_ptr = NonNull::new(node_ptr).expect("arena allocation failed");
            unsafe {
                write(
                    node_ptr.as_ptr(),
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        link: Link::new(),
                        _marker: PhantomData,
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

impl<'scope, 'scope_ref, T: 'scope, S: ScopeProvider<'scope, TExtra> + 'scope, TExtra: 'scope>
    Future for JoinHandle<'scope, 'scope_ref, T, crate::task::LocalTaskRef<'scope>, S, TExtra>
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
            JoinSource::Direct { task, gate, .. } => {
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

impl<'scope, 'scope_ref, T: 'scope, S: ScopeProvider<'scope, TExtra> + 'scope, TExtra: 'scope>
    Future for JoinHandle<'scope, 'scope_ref, T, SendTaskRef<'scope>, S, TExtra>
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
            JoinSource::Direct { task, gate, .. } => {
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
                                let task_cast = unsafe {
                                    SendTaskRef::from_header(ready.task.header()
                                        as *const GenericTaskHeader<'scope, AtomicStorage>)
                                };
                                *resolved = Some(ResolvedRoutedTask {
                                    task: task_cast,
                                    access: Some(ready.access),
                                });
                                // Continue to poll the newly resolved task
                            }
                            Ok(None) => {
                                state.register(cx.waker());
                                // Double check to avoid race condition
                                if let Some(ready) = state.try_take_ready()? {
                                    let task_cast = unsafe {
                                        SendTaskRef::from_header(ready.task.header()
                                            as *const GenericTaskHeader<'scope, AtomicStorage>)
                                    };
                                    *resolved = Some(ResolvedRoutedTask {
                                        task: task_cast,
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

impl<'scope, 'scope_ref, T, R: TaskHandleRef<'scope>, S: ScopeProvider<'scope, TExtra>, TExtra> Drop
    for JoinHandle<'scope, 'scope_ref, T, R, S, TExtra>
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
