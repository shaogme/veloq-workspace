use std::{
    future::Future,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::pin,
    ptr,
    sync::Arc,
    task::{Context, Poll},
    thread,
};

use crate::utils::{FastRand, ownership::ArcOwnership};
use veloq_storage::AtomicStorage;

pub mod context;
pub mod primitives;
pub mod shared;

pub use context::{
    IdleDecision, IdleHook, IdleWaitStrategy, RuntimeContext, RuntimeScopeContext, WorkerTickHook,
};
pub use primitives::GenericCancellationToken;
pub use shared::{RuntimeShared, RuntimeSharedBase};

use primitives::{Signal, create_waker};
use shared::{Receivers, init_runtime_components};

pub struct Runtime<T, WF> {
    pub(crate) shared: RuntimeShared<T>,
    pub(crate) receivers: Option<Receivers>,
    pub(crate) worker_factory: Option<WF>,
    marker: PhantomData<T>,
}

pub type DefaultWorkerFactory = fn(usize, &RuntimeShared<()>) -> ();

pub type DefaultWorkerFactoryFor<T> = fn(usize, &RuntimeShared<T>) -> T;

impl Runtime<(), DefaultWorkerFactoryFor<()>> {
    pub fn new() -> Self {
        RuntimeBuilder::new().build()
    }

    pub fn builder() -> RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
        RuntimeBuilder::new()
    }
}

impl Default for Runtime<(), DefaultWorkerFactoryFor<()>> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, WF> Runtime<T, WF> {
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared.worker_count()
    }

    pub fn block_on<'run, R, F>(mut self, f: F) -> R
    where
        T: 'run,
        WF: Fn(usize, &'run RuntimeShared<T>) -> T + Send + Sync,
        F: AsyncFnOnce(RuntimeScopeContext<'run, T>) -> R,
    {
        struct TlsCleanupGuard<'a, T>(&'a veloq_tls::Tls<T>);
        impl<'a, T> Drop for TlsCleanupGuard<'a, T> {
            fn drop(&mut self) {
                let _ = self.0.take();
            }
        }

        let shared_ref: &'run RuntimeShared<T> = unsafe { &*ptr::from_ref(&self.shared) };
        let ctx = RuntimeScopeContext::new(shared_ref);

        let worker_count = shared_ref.worker_count();
        let worker_factory = self
            .worker_factory
            .take()
            .expect("worker_factory already taken");
        let receivers = self.receivers.take().expect("receivers already taken");
        let mut remote_receivers = receivers.remote_receivers;
        let mut pinned_receivers = receivers.pinned_receivers;
        let mut local_receivers = receivers.local_receivers;
        let mut deques = receivers.deques;

        thread::scope(|scope| {
            struct ShutdownGuard<'a, T>(&'a RuntimeShared<T>);
            impl<'a, T> Drop for ShutdownGuard<'a, T> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(shared_ref);

            for worker_id in (1..worker_count.get()).rev() {
                let rrx = remote_receivers.pop().expect("remote receivers exhausted");
                let prx = pinned_receivers.pop().expect("pinned receivers exhausted");
                let lrx = local_receivers.pop().expect("local receivers exhausted");
                let deque = deques.pop().expect("deques exhausted");
                let worker_factory_ref = &worker_factory;

                let context = RuntimeContext {
                    worker_id,
                    remote_rx: rrx,
                    pinned_rx: prx,
                    local_rx: lrx,
                    rand: FastRand::new(worker_id as u64),
                    worker: deque,
                };

                scope.spawn(move || {
                    shared_ref
                        .base
                        .tls
                        .set_owned(context)
                        .expect("failed to set runtime context");
                    shared_ref
                        .extra_tls
                        .set_owned(worker_factory_ref(worker_id, shared_ref))
                        .expect("failed to set extra TLS");
                    let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
                    let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);

                    shared_ref.drive_worker::<AtomicStorage, ArcOwnership>(None);
                });
            }

            let rrx0 = remote_receivers
                .pop()
                .expect("main worker remote receiver exhausted");
            let prx0 = pinned_receivers
                .pop()
                .expect("main worker pinned receiver exhausted");
            let lrx0 = local_receivers
                .pop()
                .expect("main worker local receiver exhausted");
            let deque0 = deques.pop().expect("main worker deque exhausted");

            let context = RuntimeContext {
                worker_id: 0,
                remote_rx: rrx0,
                pinned_rx: prx0,
                local_rx: lrx0,
                rand: FastRand::new(0),
                worker: deque0,
            };
            shared_ref
                .base
                .tls
                .set_owned(context)
                .expect("failed to set runtime context");
            shared_ref
                .extra_tls
                .set_owned(worker_factory(0, shared_ref))
                .expect("failed to set extra TLS");
            let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
            let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);

            let signal = Arc::new(Signal::new(true));
            let waker = create_waker(signal.clone());
            let mut cx = Context::from_waker(&waker);

            shared_ref.drive_worker::<AtomicStorage, ArcOwnership>(None);

            let mut fut = pin!(f(ctx));
            loop {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        break res;
                    }
                    Poll::Pending => match shared_ref
                        .idle_hook
                        .map(|h| h(shared_ref))
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

pub struct RuntimeBuilder<T, WF> {
    worker_count: Option<usize>,
    queue_capacity: usize,
    worker_factory: Option<WF>,
    idle_hook: Option<IdleHook<T>>,
    worker_tick_hook: Option<WorkerTickHook>,
}

impl RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: 1024,
            worker_factory: Some(|_, _| ()),
            idle_hook: None,
            worker_tick_hook: None,
        }
    }
}

impl Default for RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, WF> RuntimeBuilder<T, WF> {
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

    pub fn with_idle_hook<NewT>(self, hook: IdleHook<NewT>) -> RuntimeBuilder<NewT, WF> {
        RuntimeBuilder {
            idle_hook: Some(hook),
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            worker_factory: self.worker_factory,
            worker_tick_hook: self.worker_tick_hook,
        }
    }

    pub fn with_worker_tick_hook(mut self, hook: WorkerTickHook) -> Self {
        self.worker_tick_hook = Some(hook);
        self
    }

    pub fn with_worker_factory<NWF>(self, factory: NWF) -> RuntimeBuilder<T, NWF> {
        RuntimeBuilder {
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            worker_factory: Some(factory),
            idle_hook: self.idle_hook,
            worker_tick_hook: self.worker_tick_hook,
        }
    }

    pub fn build(self) -> Runtime<T, WF> {
        let count = self
            .worker_count
            .unwrap_or_else(|| thread::available_parallelism().map_or(1, |n| n.get()));
        let worker_count =
            NonZeroUsize::new(count).expect("requested worker count must be non-zero");
        let (registry, topo, receivers) = init_runtime_components(
            worker_count,
            NonZeroUsize::new(self.queue_capacity).expect("queue capacity must be non-zero"),
        );
        let shared = RuntimeShared::new(
            registry,
            topo,
            worker_count,
            self.idle_hook,
            self.worker_tick_hook,
        );
        Runtime {
            shared,
            receivers: Some(receivers),
            worker_factory: self.worker_factory,
            marker: PhantomData,
        }
    }
}
