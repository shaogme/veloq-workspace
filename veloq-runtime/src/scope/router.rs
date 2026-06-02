use crate::runtime::{RuntimeScopeContext, RuntimeShared};
use crate::task::{
    Arena, GenericArena, GenericTaskHeader, RawScope, RawTask, ScopeRef, SendBoxedTaskNode,
    SendTask, SendTaskRef, Task, TaskError, TaskHandleRef,
};
use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicStorage, Storage};
use std::alloc::Layout;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::replace;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::{NonNull, drop_in_place, write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

pub(crate) fn dispatch_routed<'scope_ref, S: Storage, O: Ownership, T, F, TExtra>(
    context: &RuntimeScopeContext<'_, TExtra>,
    completion: &O::Shared<GenericScopeCompletion<S, O>>,
    state: Arc<RoutedSpawnState<'scope_ref, T>>,
    worker_id: usize,
    job: F,
) where
    O::Shared<GenericScopeCompletion<S, O>>: Send,
    F: FnOnce() + Send + 'scope_ref,
    T: 'scope_ref,
{
    let completion_raw_ptr = O::as_ptr(completion) as *const ();
    let completion_send_ptr = SendPtr::new(NonNull::new(completion_raw_ptr as *mut ()).unwrap());

    let state_raw_ptr = Arc::as_ptr(&state) as *const ();
    let state_send_ptr = SendPtr::new(NonNull::new(state_raw_ptr as *mut ()).unwrap());

    let job_boxed: Box<dyn FnOnce() + Send + 'scope_ref> = Box::new(job);

    if context
        .route_to(worker_id, move || {
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
