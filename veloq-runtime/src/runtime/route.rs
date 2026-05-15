use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use veloq_atomic_waker::AtomicWaker;

use super::context::RuntimeScopeContext;
use crate::utils::storage::StateInt;

pub trait WorkerJob<'scope>: Send {
    fn execute(self: Box<Self>);
}

impl<'scope, F> WorkerJob<'scope> for F
where
    F: FnOnce() + Send + 'scope,
{
    fn execute(self: Box<Self>) {
        (*self)();
    }
}

pub(crate) type WorkerRouteJob<'scope> = Box<dyn WorkerJob<'scope> + Send + 'scope>;

#[derive(Clone)]
pub(crate) struct WorkerRouteDispatcher<'scope> {
    senders: Arc<Vec<mpsc::Sender<WorkerRouteJob<'scope>>>>,
    ctx: RuntimeScopeContext<'scope>,
}

impl<'scope> WorkerRouteDispatcher<'scope> {
    pub(crate) fn new(
        senders: Vec<mpsc::Sender<WorkerRouteJob<'scope>>>,
        ctx: RuntimeScopeContext<'scope>,
    ) -> Self {
        Self {
            senders: Arc::new(senders),
            ctx,
        }
    }

    pub(crate) fn dispatch<F>(&self, worker_id: usize, job: F) -> bool
    where
        F: FnOnce() + Send + 'scope,
    {
        self.dispatch_job(worker_id, Box::new(job))
    }

    pub(crate) fn dispatch_job(&self, worker_id: usize, job: WorkerRouteJob<'scope>) -> bool {
        let Some(sender) = self.senders.get(worker_id) else {
            return false;
        };
        if sender.send(job).is_err() {
            return false;
        }
        self.ctx.wake_worker(worker_id);
        true
    }
}

pub(crate) struct WorkerRouteState<'scope> {
    pub(crate) receiver: mpsc::Receiver<WorkerRouteJob<'scope>>,
    pub(crate) dispatcher: WorkerRouteDispatcher<'scope>,
}

impl<'scope> WorkerRouteState<'scope> {
    pub(crate) fn new(
        receiver: mpsc::Receiver<WorkerRouteJob<'scope>>,
        dispatcher: WorkerRouteDispatcher<'scope>,
    ) -> Self {
        Self {
            receiver,
            dispatcher,
        }
    }
}

struct OpaqueWorkerRouteState;

thread_local! {
    static ROUTE_STATE: RefCell<Option<std::ptr::NonNull<OpaqueWorkerRouteState>>> = const { RefCell::new(None) };
}

pub(crate) fn init_worker_route_state(state: &WorkerRouteState<'_>) {
    ROUTE_STATE.with(|s| {
        *s.borrow_mut() = Some(std::ptr::NonNull::from(state).cast());
    });
}

pub(crate) fn clear_worker_route_state() {
    ROUTE_STATE.with(|s| {
        *s.borrow_mut() = None;
    });
}

pub(crate) fn with_current_worker_route_state<R>(
    f: impl for<'a> FnOnce(&WorkerRouteState<'a>) -> R,
) -> Option<R> {
    ROUTE_STATE.with(|s| {
        s.borrow().map(|ptr| {
            // SAFETY: The state is stored in TLS and is valid for the duration of the worker thread.
            // We use a large lifetime here to satisfy the compiler when moving jobs with 'scope.
            let state = unsafe { ptr.cast::<WorkerRouteState<'static>>().as_ref() };
            f(state)
        })
    })
}

pub(crate) fn drain_pending_worker_route_jobs() {
    with_current_worker_route_state(|state| {
        let mut pending = Vec::new();
        while let Ok(job) = state.receiver.try_recv() {
            pending.push(job);
        }
        for job in pending {
            job.execute();
        }
    });
}

/// Compatibility function for internal crates.
pub(crate) fn dispatch_worker_route_job<'scope, F>(worker_id: usize, job: F) -> bool
where
    F: FnOnce() + Send + 'scope,
{
    ROUTE_STATE
        .with(|s| {
            s.borrow().map(|ptr| {
                // SAFETY: We know that the current worker's state dispatcher is compatible with 'scope
                // because they both belong to the same runtime instance and scope.
                let state = unsafe { ptr.cast::<WorkerRouteState<'scope>>().as_ref() };
                state.dispatcher.dispatch(worker_id, job)
            })
        })
        .unwrap_or(false)
}

pub struct RouteCell<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
}

impl<T> RouteCell<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            waker: AtomicWaker::new(),
        })
    }

    fn set(&self, value: T) {
        let mut slot = self.value.lock().expect("worker route slot poisoned");
        debug_assert!(slot.is_none(), "worker route slot already populated");
        *slot = Some(value);
        self.waker.wake();
    }

    fn take(&self) -> Option<T> {
        self.value
            .lock()
            .expect("worker route slot poisoned")
            .take()
    }

    fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

pub struct RoutedFuture<'a, F> {
    slot: Arc<RouteCell<F>>,
    inner: Option<F>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, F> RoutedFuture<'a, F> {
    fn new(slot: Arc<RouteCell<F>>) -> Self {
        Self {
            slot,
            inner: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, F> Future for RoutedFuture<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.inner.is_none() {
            if let Some(op) = this.slot.take() {
                this.inner = Some(op);
            } else {
                this.slot.register(cx.waker());
                if let Some(op) = this.slot.take() {
                    this.inner = Some(op);
                } else {
                    return Poll::Pending;
                }
            }
        }
        let inner = this.inner.as_mut().expect("route future missing inner op");
        unsafe { Pin::new_unchecked(inner) }.poll(cx)
    }
}

pub fn route_to_worker<'scope, F>(
    worker_id: usize,
    job: impl FnOnce() -> F + Send + 'scope,
) -> io::Result<RoutedFuture<'scope, F>>
where
    F: Future + Send + 'scope,
{
    ROUTE_STATE.with(|s| {
        if let Some(ptr) = *s.borrow() {
            let slot = RouteCell::new();
            let slot_for_job = slot.clone();

            // SAFETY: Same as above.
            let state = unsafe { ptr.cast::<WorkerRouteState<'scope>>().as_ref() };
            let success = state.dispatcher.dispatch(worker_id, move || {
                let value = job();
                slot_for_job.set(value);
            });

            if !success {
                return Err(io::Error::other("failed to dispatch job to worker"));
            }

            Ok(RoutedFuture::new(slot))
        } else {
            Err(io::Error::other("not running on a worker thread"))
        }
    })
}

pub async fn execute_on_owner<'a, F, Fut, R>(
    task: &impl crate::task::TaskHandleRef,
    f: F,
) -> io::Result<R>
where
    F: FnOnce() -> Fut + Send + 'a,
    Fut: Future<Output = R> + Send + 'a,
    R: Send + 'a,
{
    use std::sync::atomic::Ordering;
    let worker_id = task.header().worker_id.load(Ordering::Acquire);
    Ok(route_to_worker(worker_id, f)?.await)
}
