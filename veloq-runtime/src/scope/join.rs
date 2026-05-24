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

pub(crate) type ReclaimFn<'scope_ref, T, A> =
    unsafe fn(&A, &'scope_ref dyn crate::task::TaskJoinGate<T>);

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> Result<T, TaskError>;
    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena);
}

pub(crate) struct RoutedSpawnReady<'scope_ref, T> {
    pub(crate) task: SendTaskRef,
    pub(crate) access: Box<dyn RoutedTaskAccess<T> + 'scope_ref>,
}

pub(crate) enum RoutedSpawnOutcome<'scope_ref, T> {
    Pending,
    Ready(RoutedSpawnReady<'scope_ref, T>),
    Failed(TaskError),
    Taken,
}

pub(crate) struct RoutedJobCell<F> {
    job: Option<F>,
}

impl<F> RoutedJobCell<F> {
    pub(crate) fn new(job: F) -> Self {
        Self { job: Some(job) }
    }

    pub(crate) fn take(&mut self) -> F {
        self.job.take().expect("routed job already taken")
    }
}

struct SpawnToAccess<'scope_ref, T, S_> {
    task: &'scope_ref S_,
    marker: PhantomData<(T,)>,
}

impl<'scope_ref, T, S_> RoutedTaskAccess<T> for SpawnToAccess<'scope_ref, T, S_>
where
    S_: crate::task::SendTask<T> + Sized + 'scope_ref,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.task.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, _arena: &dyn crate::task::Arena) {}
}

unsafe impl<'scope_ref, T, S_> Send for SpawnToAccess<'scope_ref, T, S_> where
    S_: crate::task::SendTask<T> + Sized + 'scope_ref
{
}

pub(crate) fn make_spawn_to_access<'scope_ref, T, S_>(
    task: &'scope_ref S_,
) -> Box<dyn RoutedTaskAccess<T> + 'scope_ref>
where
    T: 'scope_ref,
    S_: crate::task::SendTask<T> + Sized + 'scope_ref,
{
    Box::new(SpawnToAccess {
        task,
        marker: PhantomData,
    })
}

struct BoxedTaskAccess<'scope_ref, T, Fut> {
    node: &'scope_ref SendBoxedTaskNode<T, Fut>,
    marker: PhantomData<T>,
}

impl<'scope_ref, T, Fut> RoutedTaskAccess<T> for BoxedTaskAccess<'scope_ref, T, Fut>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.node.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, arena: &dyn crate::task::Arena) {
        let layout = Layout::new::<SendBoxedTaskNode<T, Fut>>();
        unsafe {
            arena.drop_object_raw(self.node as *const _ as *mut u8, layout);
        }
    }
}

unsafe impl<'scope_ref, T, Fut> Send for BoxedTaskAccess<'scope_ref, T, Fut>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
}

pub(crate) fn make_boxed_task_access<'scope_ref, T, Fut>(
    node: &'scope_ref SendBoxedTaskNode<T, Fut>,
) -> Box<dyn RoutedTaskAccess<T> + 'scope_ref>
where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    Box::new(BoxedTaskAccess {
        node,
        marker: PhantomData,
    })
}

pub(crate) struct RoutedSpawnState<'scope_ref, T> {
    pub(crate) outcome: Mutex<RoutedSpawnOutcome<'scope_ref, T>>,
    cancel_requested: AtomicBool,
    waker: AtomicWaker,
}

impl<'scope_ref, T> RoutedSpawnState<'scope_ref, T> {
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

    fn set_outcome(&self, new_outcome: RoutedSpawnOutcome<'scope_ref, T>) {
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

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'scope_ref, T>) {
        self.set_outcome(RoutedSpawnOutcome::Ready(ready));
    }

    pub(crate) fn fail(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcome::Failed(err));
    }

    pub(crate) fn try_take_ready(
        &self,
    ) -> Result<Option<RoutedSpawnReady<'scope_ref, T>>, TaskError> {
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
    'scope_ref,
    S: crate::utils::storage::Storage,
    O: crate::utils::ownership::Ownership,
    T,
    F,
    TExtra,
>(
    context: &crate::runtime::RuntimeScopeContext<'_, TExtra>,
    completion: &O::Shared<super::GenericScopeCompletion<S, O>>,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<super::GenericScopeCompletion<S, O>>: Send,
    F: FnOnce() + Send + 'scope_ref,
    T: 'scope_ref,
{
    let completion_raw_ptr = O::as_ptr(completion) as *const ();
    let completion_send_ptr =
        super::SendPtr::new(NonNull::new(completion_raw_ptr as *mut ()).unwrap());

    let state_raw_ptr = Arc::as_ptr(&state) as *const ();
    let state_send_ptr = super::SendPtr::new(NonNull::new(state_raw_ptr as *mut ()).unwrap());

    let job_boxed: Box<dyn FnOnce() + Send + 'scope_ref> = Box::new(job);

    if context
        .route_to(worker_id, move || {
            let state_ref: &RoutedSpawnState<'scope_ref, T> =
                unsafe { &*(state_send_ptr.as_ptr() as *const RoutedSpawnState<'scope_ref, T>) };
            let completion_ref: &super::GenericScopeCompletion<S, O> = unsafe {
                &*(completion_send_ptr.as_ptr() as *const super::GenericScopeCompletion<S, O>)
            };
            let result = catch_unwind(AssertUnwindSafe(move || {
                job_boxed();
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

pub(crate) fn install_routed_pinned_task<'scope_ref, 'rt, T, Fut, TExtra>(
    runtime: &'rt RuntimeShared<TExtra>,
    arena: &crate::task::GenericArena<AtomicStorage>,
    completion: Arc<crate::scope::ScopeCompletion>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    future: Fut,
) where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    let scope_ref = unsafe {
        let non_null = RawScope::clone_raw(&*completion);
        crate::task::ScopeRef::new(non_null)
    };
    let node = SendBoxedTaskNode::new(future);
    let node_header_ptr = &node.header as *const GenericTaskHeader<AtomicStorage>;
    unsafe {
        (*node_header_ptr).initialize(&runtime.base, worker_id, scope_ref);
    }
    let layout = Layout::new::<SendBoxedTaskNode<T, Fut>>();
    let node_ptr = unsafe {
        arena.alloc::<SendBoxedTaskNode<T, Fut>>(
            layout,
            Some(|ptr| drop_in_place(ptr as *mut SendBoxedTaskNode<T, Fut>)),
        ) as *mut SendBoxedTaskNode<T, Fut>
    };
    unsafe { write(node_ptr, node) };

    let node_ref = unsafe { &*node_ptr };
    node_ref.header().set_pinned();

    let task_ref = unsafe { SendTaskRef::from_concrete(node_ptr) };
    let header_ptr = task_ref.header() as *const GenericTaskHeader<AtomicStorage>;
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
        SendTaskRef::from_header(task_ref.header() as *const GenericTaskHeader<AtomicStorage>)
    };

    state.set_ready(RoutedSpawnReady {
        task: task_ready,
        access: make_boxed_task_access(node_ref),
    });
}

pub(crate) struct ResolvedRoutedTask<'scope_ref, T, R: TaskHandleRef> {
    pub(crate) task: R,
    pub(crate) access: Option<Box<dyn RoutedTaskAccess<T> + 'scope_ref>>,
}

pub(crate) enum JoinSource<'scope_ref, T, R: TaskHandleRef> {
    Direct {
        task: R,
        gate: &'scope_ref dyn crate::task::TaskJoinGate<T>,
    },
    Routed {
        state: Arc<RoutedSpawnState<'scope_ref, T>>,
        resolved: Option<ResolvedRoutedTask<'scope_ref, T, R>>,
    },
}

pub struct JoinHandle<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra> {
    pub(crate) source: JoinSource<'scope_ref, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<Pin<&'scope_ref mut GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope_ref, T, S::Arena>>,
    pub(crate) marker: std::marker::PhantomData<TExtra>,
}

unsafe impl<'scope_ref, T, TExtra> Send
    for JoinHandle<'scope_ref, T, SendTaskRef, crate::scope::AsyncScope<'scope_ref, TExtra>, TExtra>
where
    T: Send,
{
}

pub type LocalJoinHandle<'scope_ref, T, TExtra> = JoinHandle<
    'scope_ref,
    T,
    crate::task::LocalTaskRef,
    crate::scope::AsyncScope<'scope_ref, TExtra>,
    TExtra,
>;
pub type SendJoinHandle<'scope_ref, T, TExtra> =
    JoinHandle<'scope_ref, T, SendTaskRef, crate::scope::AsyncScope<'scope_ref, TExtra>, TExtra>;
pub type LocalAsyncJoinHandle<'scope_ref, T, TExtra> = JoinHandle<
    'scope_ref,
    T,
    crate::task::LocalTaskRef,
    crate::scope::LocalAsyncScope<'scope_ref, TExtra>,
    TExtra,
>;

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra>
    JoinHandle<'scope_ref, T, R, S, TExtra>
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
        gate: &'scope_ref dyn crate::task::TaskJoinGate<T>,
        reclaim: Option<ReclaimFn<'scope_ref, T, S::Arena>>,
    ) -> Self {
        Self {
            source: JoinSource::Direct { task, gate },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim,
            marker: PhantomData,
        }
    }

    pub(crate) fn new_routed(
        scope: &'scope_ref S,
        state: Arc<RoutedSpawnState<'scope_ref, T>>,
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
            marker: PhantomData,
        }
    }

    fn register_waker_on<St: crate::utils::storage::Storage>(
        waker_node: &mut Option<Pin<&'scope_ref mut GenericWakerNode<St>>>,
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
                        marker: PhantomData,
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

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra> + 'scope_ref, TExtra: 'scope_ref>
    Future for JoinHandle<'scope_ref, T, R, S, TExtra>
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

                Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx);
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

                        Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx);
                        return Poll::Pending;
                    } else {
                        match state.try_take_ready() {
                            Ok(Some(ready)) => {
                                let converted_task = unsafe {
                                    R::from_header(ready.task.header()
                                        as *const GenericTaskHeader<AtomicStorage>
                                        as *const GenericTaskHeader<R::Storage>)
                                };
                                *resolved = Some(ResolvedRoutedTask {
                                    task: converted_task,
                                    access: Some(ready.access),
                                });
                                // Continue to poll the newly resolved task
                            }
                            Ok(None) => {
                                state.register(cx.waker());
                                // Double check to avoid race condition
                                if let Some(ready) = state.try_take_ready()? {
                                    let converted_task = unsafe {
                                        R::from_header(ready.task.header()
                                            as *const GenericTaskHeader<AtomicStorage>
                                            as *const GenericTaskHeader<R::Storage>)
                                    };
                                    *resolved = Some(ResolvedRoutedTask {
                                        task: converted_task,
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

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra> Drop
    for JoinHandle<'scope_ref, T, R, S, TExtra>
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
