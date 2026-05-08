use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::ops::AsyncFnOnce;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use veloq_atomic_waker::AtomicWaker;

use super::context;

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
}

impl<'scope> WorkerRouteDispatcher<'scope> {
    pub(crate) fn new(senders: Vec<mpsc::Sender<WorkerRouteJob<'scope>>>) -> Self {
        Self {
            senders: Arc::new(senders),
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
        context::wake_worker(worker_id);
        true
    }
}

pub(crate) struct WorkerRouteState<'scope> {
    receiver: mpsc::Receiver<WorkerRouteJob<'scope>>,
    dispatcher: WorkerRouteDispatcher<'scope>,
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

thread_local! {
    static ROUTE_STATE: RefCell<Option<std::ptr::NonNull<OpaqueWorkerRouteState>>> = const { RefCell::new(None) };
}

pub struct OpaqueWorkerRouteState {
    _private: [u8; 0],
}

pub(crate) fn init_worker_route_state<'scope>(_state: &WorkerRouteState<'scope>) {
    ROUTE_STATE.with(|state| {
        *state.borrow_mut() = Some(std::ptr::NonNull::from(_state).cast());
    });
}

pub(crate) fn clear_worker_route_state() {
    ROUTE_STATE.with(|state| {
        *state.borrow_mut() = None;
    });
}

fn with_current_worker_route_state<'scope, R>(
    f: impl FnOnce(&WorkerRouteState<'scope>) -> R,
) -> Option<R> {
    ROUTE_STATE.with(|state| {
        let ptr = *state.borrow();
        let ptr = ptr?;
        let state = unsafe { &*(ptr.as_ptr() as *const WorkerRouteState<'scope>) };
        Some(f(state))
    })
}

pub(crate) fn dispatch_worker_route_job<'scope, F>(worker_id: usize, job: F) -> bool
where
    F: FnOnce() + Send + 'scope,
{
    with_current_worker_route_state(|state| state.dispatcher.dispatch(worker_id, job))
        .unwrap_or(false)
}

pub(crate) fn drain_pending_worker_route_jobs() {
    let _ = with_current_worker_route_state(|state| {
        let mut pending = Vec::new();
        while let Ok(job) = state.receiver.try_recv() {
            pending.push(job);
        }

        for job in pending {
            job.execute();
        }
    });
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

pub struct RoutedFuture<F> {
    slot: Arc<RouteCell<F>>,
    inner: Option<F>,
}

impl<F> RoutedFuture<F> {
    fn new(slot: Arc<RouteCell<F>>) -> Self {
        Self { slot, inner: None }
    }
}

impl<F> Future for RoutedFuture<F>
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

pub async fn execute_on_owner<T, Local, Remote>(
    owner_worker_id: usize,
    local: Local,
    remote: Remote,
) -> T
where
    Local: AsyncFnOnce() -> T,
    Remote: AsyncFnOnce() -> T,
{
    if context::current_worker_id() == owner_worker_id {
        local().await
    } else {
        remote().await
    }
}

pub fn route_to_worker<'scope, F>(
    worker_id: usize,
    job: impl FnOnce() -> F + Send + 'scope,
) -> io::Result<RoutedFuture<F>>
where
    F: Send + 'scope,
{
    let Some(dispatcher) = with_current_worker_route_state(|state| state.dispatcher.clone()) else {
        return Err(io::Error::other("runtime context not set"));
    };

    let slot = RouteCell::new();
    let slot_for_job = slot.clone();

    if !dispatcher.dispatch(worker_id, move || {
        let value = job();
        slot_for_job.set(value);
    }) {
        return Err(io::Error::other("failed to dispatch worker route job"));
    }

    Ok(RoutedFuture::new(slot))
}
