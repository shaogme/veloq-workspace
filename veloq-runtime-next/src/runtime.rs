mod context;
mod primitives;
mod shared;

use crate::task::{LocalTaskRef, SendTaskRef};
use crate::utils::FastRand;
use crate::utils::ownership::ArcOwnership;
use crate::utils::storage::AtomicStorage;
use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::AsyncFn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::task::{Context, Poll};
use std::thread;

pub use primitives::{
    EventCount, GenericCancellationToken, GenericCancellationTokenInner, Parker, Signal, Unparker,
    create_waker,
};

pub(crate) use context::with_current_runtime;
pub use context::{
    IdleHook, RuntimeContext, WorkerInitContext, clear_current_runtime_context, current_worker_id,
    set_current_runtime_context,
};
pub(crate) use shared::RuntimeShared;

fn noop_worker_init(_: WorkerInitContext) -> std::future::Ready<()> {
    std::future::ready(())
}

type NoopWorkerInit = fn(WorkerInitContext) -> std::future::Ready<()>;

pub struct Runtime<I = NoopWorkerInit> {
    shared: Arc<RuntimeShared>,
    local_receivers: Vec<Receiver<LocalTaskRef>>,
    remote_receivers: Vec<Receiver<SendTaskRef>>,
    worker_count: NonZeroUsize,
    worker_init: Option<I>,
    idle_hook: Option<IdleHook>,
}

impl Runtime<NoopWorkerInit> {
    pub fn new() -> Self {
        Self::builder().build()
    }

    pub fn builder() -> RuntimeBuilder<NoopWorkerInit> {
        RuntimeBuilder::default()
    }
}

impl Default for Runtime<NoopWorkerInit> {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl<I> Runtime<I>
where
    I: AsyncFn(WorkerInitContext) -> () + Sync,
{
    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<F: Future>(self, fut: F) -> F::Output {
        let Runtime {
            shared,
            local_receivers,
            remote_receivers,
            worker_count,
            worker_init,
            idle_hook,
        } = self;
        let worker_init = worker_init.expect("worker init missing");
        shared.shutdown.store(false, Ordering::Release);
        let mut local_receivers = local_receivers;
        let mut remote_receivers = remote_receivers;

        let mut fut = fut;
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        let signal = Arc::new(Signal::new(true));
        let waker = create_waker(signal.clone());
        let mut cx = Context::from_waker(&waker);

        thread::scope(|scope| {
            struct ClearContext;
            impl Drop for ClearContext {
                fn drop(&mut self) {
                    clear_current_runtime_context();
                }
            }

            struct ShutdownGuard(Arc<RuntimeShared>);
            impl Drop for ShutdownGuard {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(shared.clone());

            for worker_id in (1..worker_count.get()).rev() {
                let lrx = local_receivers.pop().expect("Receiver missing for worker");
                let rrx = remote_receivers.pop().expect("Receiver missing for worker");
                let runtime = shared.clone();
                let worker_init = &worker_init;
                scope.spawn(move || {
                    set_current_runtime_context(RuntimeContext {
                        shared: runtime.clone(),
                        worker_id,
                        local_rx: lrx,
                        remote_rx: rrx,
                        rand: RefCell::new(FastRand::new(worker_id as u64)),
                        idle_hook,
                    });
                    let _clear_context = ClearContext;

                    let init_ctx = WorkerInitContext::new(worker_id, worker_count);
                    let init_fut = std::pin::pin!(worker_init(init_ctx));
                    runtime.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(None, Some(init_fut));
                });
            }

            let lrx0 = local_receivers
                .pop()
                .expect("Receiver missing for worker 0");
            let rrx0 = remote_receivers
                .pop()
                .expect("Receiver missing for worker 0");
            set_current_runtime_context(RuntimeContext {
                shared: shared.clone(),
                worker_id: 0,
                local_rx: lrx0,
                remote_rx: rrx0,
                rand: RefCell::new(FastRand::new(0)),
                idle_hook,
            });
            let _clear_context = ClearContext;

            let init_ctx = WorkerInitContext::new(0, worker_count);
            let init_fut = std::pin::pin!(worker_init(init_ctx));
            shared.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(None, Some(init_fut));

            loop {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        return res;
                    }
                    Poll::Pending => {
                        let hint = context::run_worker_idle_hook();
                        match hint {
                            Some(duration) => {
                                let _ = signal.wait_timeout(duration);
                            }
                            None => {
                                signal.wait();
                            }
                        }
                    }
                }
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeBuilder<I> {
    worker_count: Option<NonZeroUsize>,
    worker_init: Option<I>,
    queue_capacity: NonZeroUsize,
    idle_hook: Option<IdleHook>,
}

impl Default for RuntimeBuilder<NoopWorkerInit> {
    fn default() -> Self {
        Self {
            worker_count: None,
            worker_init: Some(noop_worker_init),
            queue_capacity: NonZeroUsize::new(1024).unwrap(),
            idle_hook: None,
        }
    }
}

impl RuntimeBuilder<NoopWorkerInit> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<I> RuntimeBuilder<I>
where
    I: AsyncFn(WorkerInitContext) -> () + Sync,
{
    pub fn worker_count(mut self, count: NonZeroUsize) -> Self {
        self.worker_count = Some(count);
        self
    }

    pub fn with_worker_init<NewI>(self, worker_init: NewI) -> RuntimeBuilder<NewI> {
        RuntimeBuilder {
            worker_count: self.worker_count,
            worker_init: Some(worker_init),
            queue_capacity: self.queue_capacity,
            idle_hook: self.idle_hook,
        }
    }

    pub fn idle_hook(mut self, hook: IdleHook) -> Self {
        self.idle_hook = Some(hook);
        self
    }

    pub fn queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    pub fn build(self) -> Runtime<I> {
        let count = self.worker_count.unwrap_or_else(|| {
            thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
        });
        let (shared, local_receivers, remote_receivers) =
            RuntimeShared::new(count, self.queue_capacity);
        let shared = Arc::new(shared);
        Runtime {
            shared,
            local_receivers,
            remote_receivers,
            worker_count: count,
            worker_init: self.worker_init,
            idle_hook: self.idle_hook,
        }
    }
}
