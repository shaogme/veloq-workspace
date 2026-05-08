use crate::runtime::{GenericCancellationToken, RuntimeShared};
use crate::task::{
    Arena, GenericWakerNode, PinnedBoxedTaskNode, SendTaskRef, Task, TaskError, TaskHandleRef,
};
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

pub(crate) type PinnedReclaimFn = unsafe fn(&dyn crate::task::Arena, *mut ());
pub(crate) type PinnedTakeResultFn = unsafe fn(*const (), *mut ());

pub(crate) enum RoutedSpawnOutcome {
    Pending,
    Ready(RoutedSpawnReady),
    Failed(TaskError),
    Taken,
}

pub(crate) struct RoutedSpawnReady {
    pub(crate) task: SendTaskRef,
    pub(crate) node_ptr: usize,
    pub(crate) take_result: PinnedTakeResultFn,
    pub(crate) reclaim: PinnedReclaimFn,
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

pub(crate) struct RoutedSpawnState {
    pub(crate) outcome: Mutex<RoutedSpawnOutcome>,
    cancel_requested: std::sync::atomic::AtomicBool,
    waker: AtomicWaker,
}

impl RoutedSpawnState {
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

    fn set_outcome(&self, new_outcome: RoutedSpawnOutcome) {
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

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady) {
        self.set_outcome(RoutedSpawnOutcome::Ready(ready));
    }

    pub(crate) fn fail(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcome::Failed(err));
    }

    pub(crate) fn try_take_ready(&self) -> Result<Option<RoutedSpawnReady>, TaskError> {
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

pub(crate) fn install_routed_pinned_task<'scope, T, Fut>(
    runtime: Arc<RuntimeShared>,
    arena: &crate::task::GenericArena<AtomicStorage>,
    completion: Arc<crate::scope::ScopeCompletion>,
    worker_id: usize,
    state: Arc<RoutedSpawnState>,
    future: Fut,
) where
    T: Send + 'scope,
    Fut: Future<Output = T>,
{
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
    node_ref.set_scope_completion::<AtomicStorage, crate::utils::ownership::ArcOwnership>(Some(
        completion.clone(),
    ));

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

pub(crate) struct ResolvedRoutedTask<'scope, T, R: TaskHandleRef> {
    pub(crate) task: R,
    pub(crate) node_ptr: usize,
    pub(crate) take_result: PinnedTakeResultFn,
    pub(crate) reclaim: PinnedReclaimFn,
    pub(crate) _marker: std::marker::PhantomData<(&'scope (), T)>,
}

pub(crate) enum JoinSource<'scope, T, R: TaskHandleRef> {
    Direct {
        task: R,
        gate: NonNull<dyn crate::task::TaskJoinGate<T> + 'scope>,
    },
    Routed {
        state: Arc<RoutedSpawnState>,
        resolved: Option<ResolvedRoutedTask<'scope, T, R>>,
    },
}

pub struct JoinHandle<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>> {
    pub(crate) source: JoinSource<'scope, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<NonNull<GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope, T, S::Arena>>,
}

unsafe impl<'scope, 'scope_ref, T> Send
    for JoinHandle<'scope, 'scope_ref, T, SendTaskRef, crate::scope::AsyncScope<'scope>>
where
    T: Send + 'scope,
{
}

pub type LocalJoinHandle<'scope, 'scope_ref, T> =
    JoinHandle<'scope, 'scope_ref, T, crate::task::LocalTaskRef, crate::scope::AsyncScope<'scope>>;
pub type SendJoinHandle<'scope, 'scope_ref, T> =
    JoinHandle<'scope, 'scope_ref, T, SendTaskRef, crate::scope::AsyncScope<'scope>>;
pub type LocalAsyncJoinHandle<'scope, 'scope_ref, T> = JoinHandle<
    'scope,
    'scope_ref,
    T,
    crate::task::LocalTaskRef,
    crate::scope::LocalAsyncScope<'scope>,
>;

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>>
    JoinHandle<'scope, 'scope_ref, T, R, S>
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
        }
    }

    pub(crate) fn new_routed(scope: &'scope_ref S, state: Arc<RoutedSpawnState>) -> Self {
        Self {
            source: JoinSource::Routed {
                state,
                resolved: None,
            },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim: None,
        }
    }

    fn register_waker_on(
        waker_node: &mut Option<NonNull<GenericWakerNode<R::Storage>>>,
        arena: &dyn crate::task::Arena,
        header: &crate::task::GenericTaskHeader<R::Storage>,
        cx: &mut Context<'_>,
    ) {
        if let Some(node_ptr) = waker_node {
            let node = unsafe { node_ptr.as_mut() };
            if !node.waker.will_wake(cx.waker()) {
                node.waker = cx.waker().clone();
                unsafe { header.register_completion(node_ptr.as_ptr()) };
            }
        } else {
            let node_ptr = unsafe {
                arena.alloc_raw(
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
            *waker_node = NonNull::new(node_ptr);
            unsafe { header.register_completion(node_ptr) };
        }
    }
}

impl<'scope, 'scope_ref, T: 'scope, R: TaskHandleRef, S: ScopeProvider<'scope>> Future
    for JoinHandle<'scope, 'scope_ref, T, R, S>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker(Some(this.scope.completion()));

        loop {
            match &mut this.source {
                JoinSource::Direct { task, gate } => {
                    let header = task.header();
                    if header.is_completed() {
                        let res = unsafe { gate.as_ref().take_result_erased() }
                            .expect("task result already taken");
                        if let Some(reclaim) = this.reclaim {
                            unsafe { (reclaim)(this.scope.arena(), *gate) };
                        }
                        return Poll::Ready(res);
                    }

                    if this.scope.completion().is_cancelled() || header.is_cancelled() {
                        return Poll::Ready(Err(TaskError::Cancelled));
                    }

                    Self::register_waker_on(&mut this.waker_node, this.scope.arena(), header, cx);
                    return Poll::Pending;
                }
                JoinSource::Routed { state, resolved } => {
                    if let Some(res) = resolved {
                        let header = res.task.header();
                        if header.is_completed() {
                            let mut result =
                                std::mem::MaybeUninit::<Result<T, TaskError>>::uninit();
                            unsafe {
                                (res.take_result)(
                                    res.node_ptr as *const (),
                                    result.as_mut_ptr() as *mut (),
                                );
                            }
                            let output = unsafe { result.assume_init() };
                            unsafe { (res.reclaim)(this.scope.arena(), res.node_ptr as *mut ()) };
                            return Poll::Ready(output);
                        }

                        if this.scope.completion().is_cancelled() || header.is_cancelled() {
                            return Poll::Ready(Err(TaskError::Cancelled));
                        }

                        Self::register_waker_on(
                            &mut this.waker_node,
                            this.scope.arena(),
                            header,
                            cx,
                        );
                        return Poll::Pending;
                    } else {
                        match state.try_take_ready() {
                            Ok(Some(ready)) => {
                                *resolved = Some(ResolvedRoutedTask {
                                    task: unsafe {
                                        R::from_header(ready.task.header()
                                            as *const crate::task::GenericTaskHeader<AtomicStorage>
                                            as *const crate::task::GenericTaskHeader<R::Storage>)
                                    },
                                    node_ptr: ready.node_ptr,
                                    take_result: ready.take_result,
                                    reclaim: ready.reclaim,
                                    _marker: std::marker::PhantomData,
                                });
                                continue;
                            }
                            Ok(None) => {
                                state.register(cx.waker());
                                // 二次检查防止竞态
                                if let Some(ready) = state.try_take_ready()? {
                                    *resolved = Some(ResolvedRoutedTask {
                                        task: unsafe {
                                            R::from_header(ready.task.header()
                                                as *const crate::task::GenericTaskHeader<
                                                    AtomicStorage,
                                                >
                                                as *const crate::task::GenericTaskHeader<
                                                    R::Storage,
                                                >)
                                        },
                                        node_ptr: ready.node_ptr,
                                        take_result: ready.take_result,
                                        reclaim: ready.reclaim,
                                        _marker: std::marker::PhantomData,
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

impl<'scope, 'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<'scope>> Drop
    for JoinHandle<'scope, 'scope_ref, T, R, S>
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
