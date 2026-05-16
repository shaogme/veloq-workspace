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
pub mod primitives;
pub mod shared;

pub use context::{
    IdleDecision, IdleHook, IdleWaitStrategy, RuntimeContext, RuntimeScopeContext,
    WorkerInitContext, WorkerTickHook,
};
pub use primitives::GenericCancellationToken;
pub use shared::{RuntimeShared, RuntimeSharedBase};
use veloq_tls::TlsGuard;

use primitives::{Signal, create_waker};
use shared::{Receivers, init_runtime_components};

pub struct Runtime<'ctx, I, T> {
    pub(crate) shared: RuntimeShared<T>,
    shared_ptr: NonNull<RuntimeShared<T>>,
    pub(crate) receivers: Option<Receivers>,
    pub(crate) worker_init: Option<I>,
    _marker: std::marker::PhantomData<&'ctx ()>,
}

pub fn noop_worker_init<T: context::RuntimeContextExtra>(
    _: WorkerInitContext<T>,
) -> std::future::Ready<()> {
    std::future::ready(())
}

pub type NoopWorkerInit<T> = fn(WorkerInitContext<T>) -> std::future::Ready<()>;

impl<'ctx, T: context::RuntimeContextExtra> Runtime<'ctx, NoopWorkerInit<T>, T> {
    pub fn new() -> Self {
        RuntimeBuilder::<NoopWorkerInit<T>, T>::new().build()
    }

    pub fn builder() -> RuntimeBuilder<NoopWorkerInit<T>, T> {
        RuntimeBuilder::<NoopWorkerInit<T>, T>::new()
    }
}

impl<'ctx, T: context::RuntimeContextExtra> Default for Runtime<'ctx, NoopWorkerInit<T>, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'ctx, I, T: 'ctx> Runtime<'ctx, I, T>
where
    I: AsyncFn(WorkerInitContext<'ctx, T>) -> () + Send + Sync,
    T: context::RuntimeContextExtra,
{
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared.worker_count()
    }

    fn shared_ref(&self) -> &'ctx RuntimeShared<T> {
        unsafe { self.shared_ptr.as_ref() }
    }

    fn runtime_ctx(&self) -> RuntimeScopeContext<'ctx, T> {
        unsafe {
            RuntimeScopeContext {
                shared: self.shared_ptr.as_ref(),
            }
        }
    }

    pub fn block_on<R, F>(mut self, f: F) -> R
    where
        F: AsyncFnOnce(RuntimeScopeContext<'ctx, T>) -> R,
    {
        self.shared_ptr = NonNull::from(&self.shared);
        let shared_ref = self.shared_ref();
        let ctx = self.runtime_ctx();

        let worker_count = self.shared_ref().worker_count();
        let worker_init = self.worker_init.take().expect("worker_init already taken");
        let receivers = self.receivers.take().expect("receivers already taken");
        let mut local_receivers = receivers.local_receivers;
        let mut remote_receivers = receivers.remote_receivers;
        let mut pinned_receivers = receivers.pinned_receivers;

        thread::scope(|scope| {
            struct ShutdownGuard<'ctx, T: context::RuntimeContextExtra>(&'ctx RuntimeShared<T>);
            impl<'ctx, T: context::RuntimeContextExtra> Drop for ShutdownGuard<'ctx, T> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(shared_ref);

            for worker_id in (1..worker_count.get()).rev() {
                let lrx = local_receivers.pop().expect("local receivers exhausted");
                let rrx = remote_receivers.pop().expect("remote receivers exhausted");
                let prx = pinned_receivers.pop().expect("pinned receivers exhausted");
                let worker_init_ref = &worker_init;

                scope.spawn(move || {
                    let mut context = RuntimeContext {
                        worker_id,
                        local_rx: lrx,
                        remote_rx: rrx,
                        pinned_rx: prx,
                        rand: FastRand::new(worker_id as u64),
                        extra: T::new(worker_id, ctx),
                    };
                    // SAFETY: context is valid for the entire duration of the worker thread's execution.
                    // TlsGuard ensures the TLS slot is cleared when this thread scope ends.
                    let _guard = TlsGuard::new(&shared_ref.context_tls, &mut context)
                        .expect("failed to set runtime context");

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

            let mut context = RuntimeContext {
                worker_id: 0,
                local_rx: lrx0,
                remote_rx: rrx0,
                pinned_rx: prx0,
                rand: FastRand::new(0),
                extra: T::new(0, ctx),
            };
            // SAFETY: context is valid for the entire duration of the main worker's block_on execution.
            // TlsGuard ensures the TLS slot is cleared when this scope ends.
            let _guard = TlsGuard::new(&shared_ref.context_tls, &mut context)
                .expect("failed to set runtime context");

            let signal = Arc::new(Signal::new(true));
            let waker = create_waker(signal.clone());
            let mut cx = Context::from_waker(&waker);

            let init_ctx = WorkerInitContext::new(shared_ref, 0, worker_count);
            let init_fut = std::pin::pin!(worker_init(init_ctx));
            shared_ref
                .drive_worker_with_init::<AtomicStorage, ArcOwnership, _>(None, Some(init_fut));

            let mut fut = std::pin::pin!(f(ctx));
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

    pub fn build<'ctx>(self) -> Runtime<'ctx, I, T> {
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
            shared_ptr: NonNull::dangling(),
            receivers: Some(receivers),
            worker_init: self.worker_init,
            _marker: std::marker::PhantomData,
        }
    }
}
