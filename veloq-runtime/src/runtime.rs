use std::num::NonZeroUsize;
use std::ops::{AsyncFn, AsyncFnOnce};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;

use crate::utils::FastRand;
use crate::utils::ownership::ArcOwnership;
use crate::utils::storage::{AtomicStorage, StaticTransfer};

pub mod context;
pub mod coordinator;
pub mod primitives;
pub mod route;
pub mod shared;

pub use context::{
    CONTEXT, IdleDecision, IdleHook, IdleWaitStrategy, RuntimeContext, RuntimeScopeContext,
    WorkerInitContext, WorkerTickHook, current_worker_id, run_worker_idle_hook,
};
pub use primitives::GenericCancellationToken;
pub use shared::RuntimeShared;
use veloq_tls::TlsGuard;

use primitives::{Signal, create_waker};
use shared::RuntimeSharedComponents;

pub struct Runtime<I> {
    pub(crate) components: RuntimeSharedComponents,
    pub(crate) worker_count: NonZeroUsize,
    pub(crate) worker_init: Option<I>,
    pub(crate) idle_hook: Option<IdleHook>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
}

pub fn noop_worker_init(_: WorkerInitContext<'_>) -> std::future::Ready<()> {
    std::future::ready(())
}

pub type NoopWorkerInit = for<'a> fn(WorkerInitContext<'a>) -> std::future::Ready<()>;

impl Runtime<NoopWorkerInit> {
    pub fn new() -> Self {
        RuntimeBuilder::new().build()
    }

    pub fn builder() -> RuntimeBuilder<NoopWorkerInit> {
        RuntimeBuilder::new()
    }
}

impl Default for Runtime<NoopWorkerInit> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Runtime<I>
where
    I: for<'a> AsyncFn(WorkerInitContext<'a>) -> () + Send + Sync,
{
    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<T, F>(mut self, f: F) -> T
    where
        F: for<'a> AsyncFnOnce(&RuntimeScopeContext<'a>) -> T,
    {
        let worker_count = self.worker_count;
        let worker_init = self.worker_init.take().expect("worker_init already taken");
        let idle_hook = self.idle_hook;
        let worker_tick_hook = self.worker_tick_hook;

        let mut components = self.components;
        let mut local_receivers = std::mem::take(&mut components.local_receivers);
        let mut remote_receivers = std::mem::take(&mut components.remote_receivers);
        let mut pinned_receivers = std::mem::take(&mut components.pinned_receivers);

        let shared = RuntimeShared::new(components);

        let mut route_senders = Vec::with_capacity(worker_count.get());
        let mut route_receivers = Vec::with_capacity(worker_count.get());
        for _ in 0..worker_count.get() {
            let (tx, rx) = mpsc::channel();
            route_senders.push(tx);
            route_receivers.push(rx);
        }
        let route_receivers = Arc::new(StaticTransfer::new(route_receivers));
        let route_dispatcher = route::WorkerRouteDispatcher::new(
            route_senders,
            RuntimeScopeContext { shared: &shared },
        );

        thread::scope(|scope| {
            struct ShutdownGuard<'a>(&'a RuntimeShared);
            impl Drop for ShutdownGuard<'_> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(&shared);

            for worker_id in (1..worker_count.get()).rev() {
                let lrx = local_receivers.pop().expect("local receivers exhausted");
                let rrx = remote_receivers.pop().expect("remote receivers exhausted");
                let prx = pinned_receivers.pop().expect("pinned receivers exhausted");
                let route_rx = route_receivers.take(worker_id);
                let shared_ref = &shared;
                let route_dispatcher = route_dispatcher.clone();
                let worker_init_ref = &worker_init;

                scope.spawn(move || {
                    let mut context = RuntimeContext {
                        worker_id,
                        local_rx: lrx,
                        remote_rx: rrx,
                        pinned_rx: prx,
                        rand: FastRand::new(worker_id as u64),
                        idle_hook,
                        worker_tick_hook,
                    };
                    let _guard = TlsGuard::new(&CONTEXT, NonNull::from(&mut context))
                        .expect("failed to set runtime context");
                    let route_state =
                        route::WorkerRouteState::new(route_rx, route_dispatcher.clone());
                    route::init_worker_route_state(&route_state);

                    struct ClearRouteState;
                    impl Drop for ClearRouteState {
                        fn drop(&mut self) {
                            route::clear_worker_route_state();
                        }
                    }
                    let _clear_route_state = ClearRouteState;

                    let init_ctx = WorkerInitContext::new(shared_ref, worker_id, worker_count);
                    let init_fut = std::pin::pin!(worker_init_ref(init_ctx));
                    shared_ref.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(
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
            let route_rx0 = route_receivers.take(0);

            let mut context = RuntimeContext {
                worker_id: 0,
                local_rx: lrx0,
                remote_rx: rrx0,
                pinned_rx: prx0,
                rand: FastRand::new(0),
                idle_hook,
                worker_tick_hook,
            };
            let _guard = TlsGuard::new(&CONTEXT, NonNull::from(&mut context))
                .expect("failed to set runtime context");
            let route_state = route::WorkerRouteState::new(route_rx0, route_dispatcher.clone());
            route::init_worker_route_state(&route_state);

            struct ClearRouteState;
            impl Drop for ClearRouteState {
                fn drop(&mut self) {
                    route::clear_worker_route_state();
                }
            }
            let _clear_route_state = ClearRouteState;

            let signal = Arc::new(Signal::new(true));
            let waker = create_waker(signal.clone());
            let mut cx = Context::from_waker(&waker);
            let runtime_ctx = RuntimeScopeContext { shared: &shared };

            let init_ctx = WorkerInitContext::new(&shared, 0, worker_count);
            let init_fut = std::pin::pin!(worker_init(init_ctx));
            shared.drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(None, Some(init_fut));

            let mut fut = std::pin::pin!(f(&runtime_ctx));
            loop {
                route::drain_pending_worker_route_jobs();
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        break res;
                    }
                    Poll::Pending => match run_worker_idle_hook() {
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

pub struct RuntimeBuilder<I> {
    worker_count: Option<usize>,
    queue_capacity: usize,
    worker_init: Option<I>,
    idle_hook: Option<IdleHook>,
    worker_tick_hook: Option<WorkerTickHook>,
}

impl RuntimeBuilder<NoopWorkerInit> {
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: 1024,
            worker_init: Some(noop_worker_init),
            idle_hook: None,
            worker_tick_hook: None,
        }
    }
}

impl Default for RuntimeBuilder<NoopWorkerInit> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> RuntimeBuilder<I> {
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

    pub fn with_idle_hook(mut self, hook: IdleHook) -> Self {
        self.idle_hook = Some(hook);
        self
    }

    pub fn with_worker_tick_hook(mut self, hook: WorkerTickHook) -> Self {
        self.worker_tick_hook = Some(hook);
        self
    }

    pub fn with_worker_init<NI>(self, init: NI) -> RuntimeBuilder<NI> {
        RuntimeBuilder {
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            worker_init: Some(init),
            idle_hook: self.idle_hook,
            worker_tick_hook: self.worker_tick_hook,
        }
    }

    pub fn build(self) -> Runtime<I> {
        let count = self
            .worker_count
            .unwrap_or_else(|| thread::available_parallelism().map_or(1, |n| n.get()));
        let components = RuntimeSharedComponents::new(
            NonZeroUsize::new(count).expect("requested worker count must be non-zero"),
            NonZeroUsize::new(self.queue_capacity).expect("queue capacity must be non-zero"),
        );
        Runtime {
            components,
            worker_count: NonZeroUsize::new(count).expect("final worker count must be non-zero"),
            worker_init: self.worker_init,
            idle_hook: self.idle_hook,
            worker_tick_hook: self.worker_tick_hook,
        }
    }
}
