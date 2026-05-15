use std::num::NonZeroUsize;
use std::ops::{AsyncFn, AsyncFnOnce};
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use crate::utils::FastRand;
use crate::utils::ownership::ArcOwnership;
use crate::utils::storage::AtomicStorage;

pub mod context;
pub mod coordinator;
pub mod primitives;
pub mod route;
pub mod shared;

pub use context::{
    IdleDecision, IdleHook, IdleWaitStrategy, RuntimeContext, RuntimeScopeContext,
    WorkerInitContext, WorkerTickHook,
};
pub use primitives::GenericCancellationToken;
pub use shared::{RuntimeShared, RuntimeSharedBase};
use veloq_tls::TlsGuard;

use primitives::{Signal, create_waker};
use shared::RuntimeSharedComponents;

pub struct Runtime<I, T> {
    pub(crate) components: RuntimeSharedComponents<T>,
    pub(crate) worker_count: NonZeroUsize,
    pub(crate) worker_init: Option<I>,
}

pub fn noop_worker_init<T: context::RuntimeContextExtra>(
    _: WorkerInitContext<T>,
) -> std::future::Ready<()> {
    std::future::ready(())
}

pub type NoopWorkerInit<T> = fn(WorkerInitContext<T>) -> std::future::Ready<()>;

impl<T: context::RuntimeContextExtra> Runtime<NoopWorkerInit<T>, T> {
    pub fn new() -> Self {
        RuntimeBuilder::<NoopWorkerInit<T>, T>::new().build()
    }

    pub fn builder() -> RuntimeBuilder<NoopWorkerInit<T>, T> {
        RuntimeBuilder::<NoopWorkerInit<T>, T>::new()
    }
}

impl<T: context::RuntimeContextExtra> Default for Runtime<NoopWorkerInit<T>, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, T> Runtime<I, T>
where
    I: AsyncFn(WorkerInitContext<T>) -> () + Send + Sync,
    T: context::RuntimeContextExtra,
{
    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<R, F>(mut self, f: F) -> R
    where
        F: AsyncFnOnce(&RuntimeScopeContext<T>) -> R,
    {
        let worker_count = self.worker_count;
        let worker_init = self.worker_init.take().expect("worker_init already taken");

        let mut components = self.components;
        let mut local_receivers = std::mem::take(&mut components.local_receivers);
        let mut remote_receivers = std::mem::take(&mut components.remote_receivers);
        let mut pinned_receivers = std::mem::take(&mut components.pinned_receivers);

        let shared = Arc::new(RuntimeShared::new(components));

        thread::scope(|scope| {
            struct ShutdownGuard<T: context::RuntimeContextExtra>(Arc<RuntimeShared<T>>);
            impl<T: context::RuntimeContextExtra> Drop for ShutdownGuard<T> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(shared.clone());

            for worker_id in (1..worker_count.get()).rev() {
                let lrx = local_receivers.pop().expect("local receivers exhausted");
                let rrx = remote_receivers.pop().expect("remote receivers exhausted");
                let prx = pinned_receivers.pop().expect("pinned receivers exhausted");
                let shared_clone = shared.clone();
                let worker_init_ref = &worker_init;

                scope.spawn(move || {
                    let mut context = RuntimeContext {
                        worker_id,
                        local_rx: lrx,
                        remote_rx: rrx,
                        pinned_rx: prx,
                        rand: FastRand::new(worker_id as u64),
                        extra: T::new(worker_id),
                    };
                    let _guard =
                        TlsGuard::new(&shared_clone.context_tls, NonNull::from(&mut context))
                            .expect("failed to set runtime context");

                    let init_ctx =
                        WorkerInitContext::new(shared_clone.clone(), worker_id, worker_count);
                    let init_fut = std::pin::pin!(worker_init_ref(init_ctx));
                    shared_clone.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(
                        None,
                        Some(init_fut),
                    );
                });
            }

            let lrx0 = local_receivers
                .pop()
                .expect("main worker local receiver exhausted");
            let rrx0 = remote_receivers
                .pop()
                .expect("main worker remote receiver exhausted");
            let prx0 = pinned_receivers
                .pop()
                .expect("main worker pinned receiver exhausted");

            let mut context = RuntimeContext {
                worker_id: 0,
                local_rx: lrx0,
                remote_rx: rrx0,
                pinned_rx: prx0,
                rand: FastRand::new(0),
                extra: T::new(0),
            };
            let _guard = TlsGuard::new(&shared.context_tls, NonNull::from(&mut context))
                .expect("failed to set runtime context");

            let signal = Arc::new(Signal::new(true));
            let waker = create_waker(signal.clone());
            let mut cx = Context::from_waker(&waker);
            let runtime_ctx = RuntimeScopeContext {
                shared: shared.clone(),
            };

            let init_ctx = WorkerInitContext::new(shared.clone(), 0, worker_count);
            let init_fut = std::pin::pin!(worker_init(init_ctx));
            shared.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(None, Some(init_fut));

            let mut fut = std::pin::pin!(f(&runtime_ctx));
            loop {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        break res;
                    }
                    Poll::Pending => match shared
                        .idle_hook
                        .map(|h| h(&shared))
                        .unwrap_or(IdleDecision::wait(IdleWaitStrategy::Block))
                    {
                        IdleDecision::Continue => thread::yield_now(),
                        IdleDecision::Wait(IdleWaitStrategy::Timeout(d)) => {
                            let _ = signal.wait_timeout(d);
                        }
                        IdleDecision::Wait(IdleWaitStrategy::Block) => signal.wait(),
                    },
                }
            }
        })
    }
}

pub struct RuntimeBuilder<I, T> {
    worker_count: Option<usize>,
    queue_capacity: usize,
    worker_init: Option<I>,
    idle_hook: Option<IdleHook<T>>,
    worker_tick_hook: Option<WorkerTickHook>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: context::RuntimeContextExtra> RuntimeBuilder<NoopWorkerInit<T>, T> {
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: 1024,
            worker_init: Some(noop_worker_init),
            idle_hook: None,
            worker_tick_hook: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: context::RuntimeContextExtra> Default for RuntimeBuilder<NoopWorkerInit<T>, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, T: context::RuntimeContextExtra> RuntimeBuilder<I, T> {
    pub fn worker_count(mut self, count: NonZeroUsize) -> Self {
        self.worker_count = Some(count.get());
        self
    }

    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = Some(count);
        self
    }

    pub fn queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.queue_capacity = capacity.get();
        self
    }

    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    pub fn with_idle_hook(mut self, hook: IdleHook<T>) -> Self {
        self.idle_hook = Some(hook);
        self
    }

    pub fn with_worker_tick_hook(mut self, hook: WorkerTickHook) -> Self {
        self.worker_tick_hook = Some(hook);
        self
    }

    pub fn with_worker_init<NI>(self, init: NI) -> RuntimeBuilder<NI, T> {
        RuntimeBuilder {
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            worker_init: Some(init),
            idle_hook: self.idle_hook,
            worker_tick_hook: self.worker_tick_hook,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn build(self) -> Runtime<I, T> {
        let count = self
            .worker_count
            .unwrap_or_else(|| thread::available_parallelism().map_or(1, |n| n.get()));
        let components = RuntimeSharedComponents::new(
            NonZeroUsize::new(count).expect("requested worker count must be non-zero"),
            NonZeroUsize::new(self.queue_capacity).expect("queue capacity must be non-zero"),
            self.idle_hook,
            self.worker_tick_hook,
        );
        Runtime {
            components,
            worker_count: NonZeroUsize::new(count).expect("final worker count must be non-zero"),
            worker_init: self.worker_init,
        }
    }
}
