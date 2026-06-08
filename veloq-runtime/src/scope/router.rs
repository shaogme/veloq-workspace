use crate::error::Result as RuntimeResult;
use crate::runtime::{RuntimeScopeContext, RuntimeShared};
use crate::task::{
    Arena, GenericArena, GenericTaskHeader, RawScope, RawTask, ScopeRef, SendBoxedTaskNode,
    SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
};
use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicOptionPtr, AtomicStorage, StateOptionPtr, Storage};
use std::alloc::Layout;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::{NonNull, drop_in_place, write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Waker;
use veloq_atomic_waker::AtomicWaker;

use crate::scope::SendPtr;
use crate::scope::completion::{GenericScopeCompletion, ScopeCompletion};

pub(crate) trait RoutedTaskAccess<T>: Send {
    fn take_result(&self) -> Result<T, TaskError>;
    fn reclaim(self: Box<Self>, arena: &dyn Arena);
}

pub(crate) struct RoutedSpawnReady<'scope_ref, T> {
    pub(crate) task: SendTaskRef,
    pub(crate) access: Box<dyn RoutedTaskAccess<T> + 'scope_ref>,
}

pub(crate) enum RoutedSpawnOutcomeInner<'scope_ref, T> {
    Ready(RoutedSpawnReady<'scope_ref, T>),
    Failed(TaskError),
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
    S_: SendTask<T> + Sized + 'scope_ref,
{
    fn take_result(&self) -> Result<T, TaskError> {
        self.task.take_result().expect("task result already taken")
    }

    fn reclaim(self: Box<Self>, _arena: &dyn Arena) {}
}

unsafe impl<'scope_ref, T, S_> Send for SpawnToAccess<'scope_ref, T, S_> where
    S_: SendTask<T> + Sized + 'scope_ref
{
}

pub(crate) fn make_spawn_to_access<'scope_ref, T, S_>(
    task: &'scope_ref S_,
) -> Box<dyn RoutedTaskAccess<T> + 'scope_ref>
where
    T: 'scope_ref,
    S_: SendTask<T> + Sized + 'scope_ref,
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

    fn reclaim(self: Box<Self>, arena: &dyn Arena) {
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
    outcome: AtomicOptionPtr<RoutedSpawnOutcomeInner<'scope_ref, T>>,
    cancel_requested: AtomicBool,
    waker: AtomicWaker,
}

impl<'scope_ref, T> RoutedSpawnState<'scope_ref, T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: AtomicOptionPtr::new(None),
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

    fn set_outcome(&self, inner: RoutedSpawnOutcomeInner<'scope_ref, T>) {
        let boxed = Box::new(inner);
        let raw = NonNull::new(Box::into_raw(boxed)).unwrap();
        match self
            .outcome
            .compare_exchange(None, Some(raw), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.waker.wake();
            }
            Err(_) => {
                // If it's already set (e.g. Taken or Cancelled), drop the box to prevent leak.
                unsafe {
                    let _ = Box::from_raw(raw.as_ptr());
                }
            }
        }
    }

    pub(crate) fn set_ready(&self, ready: RoutedSpawnReady<'scope_ref, T>) {
        self.set_outcome(RoutedSpawnOutcomeInner::Ready(ready));
    }

    pub(crate) fn fail(&self, err: TaskError) {
        self.set_outcome(RoutedSpawnOutcomeInner::Failed(err));
    }

    pub(crate) fn try_take_ready(
        &self,
    ) -> Result<Option<RoutedSpawnReady<'scope_ref, T>>, TaskError> {
        if let Some(raw) = self.outcome.swap(None, Ordering::AcqRel) {
            let inner = unsafe { Box::from_raw(raw.as_ptr()) };
            match *inner {
                RoutedSpawnOutcomeInner::Ready(ready) => Ok(Some(ready)),
                RoutedSpawnOutcomeInner::Failed(err) => Err(err),
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn cancel_ready_task_if_any(&self) {
        if let Some(raw) = self.outcome.load(Ordering::Acquire) {
            let inner = unsafe { raw.as_ref() };
            if let RoutedSpawnOutcomeInner::Ready(ready) = inner {
                ready.task.header().cancel();
            }
        }
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

impl<'scope_ref, T> Drop for RoutedSpawnState<'scope_ref, T> {
    fn drop(&mut self) {
        if let Some(raw) = self.outcome.swap(None, Ordering::Acquire) {
            unsafe {
                let _ = Box::from_raw(raw.as_ptr());
            }
        }
    }
}

unsafe impl<'scope_ref, T> Send for RoutedSpawnState<'scope_ref, T> where T: Send {}
unsafe impl<'scope_ref, T> Sync for RoutedSpawnState<'scope_ref, T> where T: Send {}

pub(crate) fn dispatch_routed<'scope_ref, S: Storage, O: Ownership, T, F, TExtra>(
    context: &RuntimeScopeContext<TExtra>,
    completion: &O::Shared<GenericScopeCompletion<S, O>>,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    worker_id: usize,
    job: F,
) -> RuntimeResult<()>
where
    O::Shared<GenericScopeCompletion<S, O>>: Send,
    F: FnOnce() + Send + 'scope_ref,
    T: 'scope_ref,
{
    let completion_raw_ptr = O::as_ptr(completion) as *const ();
    let completion_send_ptr = SendPtr::new(NonNull::new(completion_raw_ptr as *mut ()).unwrap());

    let state_raw_ptr = Arc::as_ptr(&state) as *const ();
    let state_send_ptr = SendPtr::new(NonNull::new(state_raw_ptr as *mut ()).unwrap());

    let job_boxed: Box<dyn FnOnce() + Send + 'scope_ref> = Box::new(job);

    match context.route_to(worker_id, move || {
        let state_ref: &RoutedSpawnState<'scope_ref, T> =
            unsafe { &*(state_send_ptr.as_ptr() as *const RoutedSpawnState<'scope_ref, T>) };
        let completion_ref: &GenericScopeCompletion<S, O> =
            unsafe { &*(completion_send_ptr.as_ptr() as *const GenericScopeCompletion<S, O>) };
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
    }) {
        Ok(_) => Ok(()),
        Err(err) => {
            completion.task_done();
            state.fail(TaskError::Panic);
            Err(err)
        }
    }
}

pub(crate) fn install_routed_pinned_task<'scope_ref, 'rt, T, Fut, TExtra>(
    runtime: &'rt RuntimeShared<TExtra>,
    arena: &GenericArena<AtomicStorage>,
    completion: Arc<ScopeCompletion>,
    worker_id: usize,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    future: Fut,
) where
    T: Send + 'scope_ref,
    Fut: Future<Output = T> + 'scope_ref,
{
    let scope_ref = unsafe {
        let non_null = RawScope::clone_raw(&*completion);
        ScopeRef::new(non_null)
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
