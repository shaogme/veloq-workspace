use std::{
    future::Future,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::pin,
    ptr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
};

use crate::{
    error::{Result, RuntimeError},
    utils::{FastRand, ownership::ArcOwnership},
};
use diagweave::prelude::*;
use veloq_storage::AtomicStorage;

pub mod context;
pub mod primitives;
pub mod shared;

pub use context::{IdleDecision, IdleWaitStrategy, RuntimeScopeContext};
pub(crate) use context::{IdleHook, RuntimeContext, WorkerTickHook};
pub use primitives::GenericCancellationToken;
pub use shared::{EnqueuePinnedOutcome, RuntimeShared, RuntimeSharedBase};

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

    pub fn block_on<'run, R, F>(mut self, f: F) -> Result<R>
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
            .ok_or(RuntimeError::WorkerFactoryAlreadyTaken)?;
        let receivers = self
            .receivers
            .take()
            .ok_or(RuntimeError::ReceiversAlreadyTaken)?;
        let mut deques = receivers.deques;

        let thread_errors = Mutex::new(None);

        let res = thread::scope(|scope| {
            struct ShutdownGuard<'a, T>(&'a RuntimeShared<T>);
            impl<'a, T> Drop for ShutdownGuard<'a, T> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let _guard = ShutdownGuard(shared_ref);

            for worker_id in (1..worker_count.get()).rev() {
                let deque = match deques.pop() {
                    Some(d) => d,
                    None => {
                        return RuntimeError::DequesExhausted { worker_id }.trans();
                    }
                };
                let worker_factory_ref = &worker_factory;
                let thread_errors_ref = &thread_errors;

                let context = RuntimeContext {
                    worker_id,
                    rand: FastRand::new(worker_id as u64),
                    worker: deque,
                };

                scope.spawn(move || {
                    let init_res = (|| {
                        shared_ref.base.tls.set_owned(context).map_err(|source| {
                            RuntimeError::TlsSetOwnedFailed { worker_id, source }
                        })?;
                        shared_ref
                            .extra_tls
                            .set_owned(worker_factory_ref(worker_id, shared_ref))
                            .map_err(|source| RuntimeError::TlsSetOwnedFailed {
                                worker_id,
                                source,
                            })?;
                        Ok(())
                    })();

                    if let Err(err) = init_res {
                        let mut guard = thread_errors_ref.lock().unwrap_or_else(|e| e.into_inner());
                        if guard.is_none() {
                            *guard = Some(err);
                        }
                        return;
                    }

                    let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
                    let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);

                    if let Err(err) = shared_ref.drive_worker::<AtomicStorage, ArcOwnership>(None) {
                        let mut guard = thread_errors_ref.lock().unwrap_or_else(|e| e.into_inner());
                        if guard.is_none() {
                            *guard = Some(err);
                        }
                    }
                });
            }

            let deque0 = deques.pop().ok_or(RuntimeError::MainWorkerDequeExhausted)?;

            let context = RuntimeContext {
                worker_id: 0,
                rand: FastRand::new(0),
                worker: deque0,
            };
            shared_ref.base.tls.set_owned(context).map_err(|source| {
                RuntimeError::TlsSetOwnedFailed {
                    worker_id: 0,
                    source,
                }
                .to_report()
            })?;
            shared_ref
                .extra_tls
                .set_owned(worker_factory(0, shared_ref))
                .map_err(|source| {
                    RuntimeError::TlsSetOwnedFailed {
                        worker_id: 0,
                        source,
                    }
                    .to_report()
                })?;
            let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
            let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);

            if let Some(err) = thread_errors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                return Err(err);
            }

            let signal = Arc::new(Signal::new(true));
            let waker = create_waker(signal.clone());
            let mut cx = Context::from_waker(&waker);

            shared_ref.drive_worker::<AtomicStorage, ArcOwnership>(None)?;

            let mut fut = pin!(f(ctx));
            let block_res = loop {
                if let Some(err) = thread_errors
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    break Err(err);
                }

                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        break Ok(res);
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
            };

            if block_res.is_ok()
                && let Some(err) = thread_errors
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
            {
                return Err(err);
            }

            block_res
        })?;

        Ok(res)
    }
}

pub struct RuntimeBuilder<T, WF> {
    worker_count: Option<NonZeroUsize>,
    queue_capacity: NonZeroUsize,
    worker_factory: Option<WF>,
    idle_hook: Option<IdleHook<T>>,
    worker_tick_hook: Option<WorkerTickHook>,
}

impl RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: NonZeroUsize::new(1024).unwrap(),
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
    pub fn with_worker_count(mut self, count: Option<NonZeroUsize>) -> Self {
        self.worker_count = count;
        self
    }

    pub fn with_queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
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
        let worker_count = self.worker_count.unwrap_or_else(|| {
            thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap())
        });
        let (registry, topo, receivers) =
            init_runtime_components(worker_count, self.queue_capacity);
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
