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

pub trait WorkerJob: Send {
    fn execute(self: Box<Self>);
}

impl<F> WorkerJob for F
where
    F: FnOnce() + Send + 'static,
{
    fn execute(self: Box<Self>) {
        (*self)();
    }
}

pub(crate) type WorkerRouteJob = Box<dyn WorkerJob>;

#[derive(Clone)]
pub(crate) struct WorkerRouteDispatcher {
    senders: Arc<Vec<mpsc::Sender<WorkerRouteJob>>>,
}

impl WorkerRouteDispatcher {
    pub(crate) fn new(senders: Vec<mpsc::Sender<WorkerRouteJob>>) -> Self {
        Self {
            senders: Arc::new(senders),
        }
    }

    pub(crate) fn dispatch<F>(&self, worker_id: usize, job: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        let Some(sender) = self.senders.get(worker_id) else {
            return false;
        };
        if sender.send(Box::new(job)).is_err() {
            return false;
        }
        context::wake_worker(worker_id);
        true
    }
}

struct WorkerRouteState {
    receiver: mpsc::Receiver<WorkerRouteJob>,
}

thread_local! {
    static ROUTE_STATE: RefCell<Option<WorkerRouteState>> = const { RefCell::new(None) };
}

pub(crate) fn init_worker_route_state(receiver: mpsc::Receiver<WorkerRouteJob>) {
    ROUTE_STATE.with(|state| {
        *state.borrow_mut() = Some(WorkerRouteState { receiver });
    });
}

pub(crate) fn drain_pending_worker_route_jobs() {
    ROUTE_STATE.with(|state| {
        let mut state_opt = state.borrow_mut();
        let Some(state) = state_opt.as_mut() else {
            return;
        };

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

pub fn route_to_worker<F>(
    worker_id: usize,
    job: impl FnOnce() -> F + Send + 'static,
) -> io::Result<RoutedFuture<F>>
where
    F: Send + 'static,
{
    let Some(dispatcher) = context::with_current_context(|ctx| ctx.worker_route_dispatcher())
    else {
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
