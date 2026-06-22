use std::{
    future::Future,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::pin,
    ptr,
    sync::Mutex,
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
pub mod wake;

pub(crate) use context::{DriverWaitHook, IdleHook, RuntimeTlsInner, WorkerTickHook};
pub use context::{IdleDecision, IdleWaitStrategy, IntoRuntimeCtx, RuntimeCtx, WaitBackend};
pub use primitives::GenericCancellationToken;
pub use shared::{EnqueuePinnedOutcome, RuntimeShared, RuntimeSharedBase};
pub use wake::ExternalWake;

use shared::{Receivers, init_runtime_components};
use wake::create_runtime_waker;

pub struct Runtime<'rt, 'env: 'rt, T, WF: 'rt> {
    shared: RuntimeShared<T>,
    receivers: Option<Receivers>,
    worker_factory: Option<WF>,
    _marker: std::marker::PhantomData<fn(&'rt ()) -> &'env ()>,
}

pub type DefaultWorkerFactory = fn(usize, &RuntimeShared<()>) -> ();

pub type DefaultWorkerFactoryFor<T> = fn(usize, &RuntimeShared<T>) -> T;

impl<'rt, 'env: 'rt> Runtime<'rt, 'env, (), DefaultWorkerFactoryFor<()>> {
    pub fn scope<F, R>(f: F) -> Result<R>
    where
        F: for<'rt_inner> AsyncFnOnce(RuntimeCtx<'rt_inner, ()>) -> R,
    {
        RuntimeBuilder::new().scope(f)
    }

    pub fn builder() -> RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
        RuntimeBuilder::new()
    }
}

impl<'rt, 'env: 'rt, T, WF> Runtime<'rt, 'env, T, WF> {
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared.worker_count()
    }

    pub fn block_on<R, F>(mut self, f: F) -> Result<R>
    where
        T: 'rt,
        WF: Fn(usize, &'rt RuntimeShared<T>) -> T + Send + Sync,
        F: AsyncFnOnce(RuntimeCtx<'rt, T>) -> R,
    {
        struct TlsCleanupGuard<'a, T>(&'a veloq_tls::Tls<T>);
        impl<'a, T> Drop for TlsCleanupGuard<'a, T> {
            fn drop(&mut self) {
                let _ = self.0.take();
            }
        }

        let shared_ref: &'rt RuntimeShared<T> = unsafe { &*ptr::from_ref(&self.shared) };
        let ctx = RuntimeCtx::new(shared_ref);

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

        let res: Result<R> = veloq_thread::scope(|scope| {
            struct ShutdownGuard<'rt, T>(&'rt RuntimeShared<T>);
            impl<'rt, T> Drop for ShutdownGuard<'rt, T> {
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

                let context = RuntimeTlsInner {
                    worker_id,
                    rand: FastRand::new(worker_id as u64),
                    worker: deque,
                };

                scope
                    .spawn(move || {
                        let init_res = (|| {
                            shared_ref.base.tls.set_owned(context).map_err(|source| {
                                RuntimeError::TlsSetOwnedFailed {
                                    worker_id,
                                    source: source.kind(),
                                }
                            })?;
                            shared_ref
                                .extra_tls
                                .set_owned(worker_factory_ref(worker_id, shared_ref))
                                .map_err(|source| RuntimeError::TlsSetOwnedFailed {
                                    worker_id,
                                    source: source.kind(),
                                })?;
                            Ok(())
                        })();

                        if let Err(err) = init_res {
                            let mut guard =
                                thread_errors_ref.lock().unwrap_or_else(|e| e.into_inner());
                            if guard.is_none() {
                                *guard = Some(err);
                            }
                            return;
                        }

                        let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
                        let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);

                        if let Err(err) =
                            shared_ref.drive_worker::<AtomicStorage, ArcOwnership>(None)
                        {
                            let mut guard =
                                thread_errors_ref.lock().unwrap_or_else(|e| e.into_inner());
                            if guard.is_none() {
                                *guard = Some(err);
                            }
                        }
                    })
                    .map_err(|e| RuntimeError::ThreadSpawnFailed { source: e })?;
            }

            let deque0 = deques.pop().ok_or(RuntimeError::MainWorkerDequeExhausted)?;

            let context = RuntimeTlsInner {
                worker_id: 0,
                rand: FastRand::new(0),
                worker: deque0,
            };
            shared_ref.base.tls.set_owned(context).map_err(|source| {
                RuntimeError::TlsSetOwnedFailed {
                    worker_id: 0,
                    source: source.kind(),
                }
                .to_report()
            })?;
            shared_ref
                .extra_tls
                .set_owned(worker_factory(0, shared_ref))
                .map_err(|source| {
                    RuntimeError::TlsSetOwnedFailed {
                        worker_id: 0,
                        source: source.kind(),
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

            let wake = shared_ref.base.registry.wake_sources[0].clone();
            let waker = create_runtime_waker(wake.clone());
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

                let poll_epoch = wake.current_epoch();
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        break Ok(res);
                    }
                    Poll::Pending => {
                        if wake.current_epoch() != poll_epoch {
                            continue;
                        }

                        let epoch = wake.current_epoch();
                        while wake.current_epoch() == epoch {
                            let mut progressed = false;
                            if let Some(task) = shared_ref.base.fn_pop_send(0) {
                                shared_ref.base.poll_send_task(0, task)?;
                                progressed = true;
                            } else if let Some(task) = shared_ref.base.fn_pop_pinned(0) {
                                shared_ref.base.poll_send_task(0, task)?;
                                progressed = true;
                            } else if let Some(task) = shared_ref.base.fn_pop_local(0) {
                                shared_ref.base.poll_local_task(0, task)?;
                                progressed = true;
                            } else if let Some(task) = shared_ref.base.pop_global() {
                                shared_ref.base.poll_send_task(0, task)?;
                                progressed = true;
                            } else if let Some(task) =
                                shared_ref.base.registry.workers[0].remote_queue.pop()
                            {
                                shared_ref.base.poll_send_task(0, task)?;
                                progressed = true;
                            }

                            if !progressed {
                                break;
                            }
                        }

                        if wake.current_epoch() == epoch {
                            let decision = match shared_ref.idle_hook {
                                Some(h) => match h(shared_ref) {
                                    Ok(dec) => dec,
                                    Err(err) => break Err(err),
                                },
                                None => IdleDecision::wait(
                                    WaitBackend::RuntimePark,
                                    IdleWaitStrategy::Block,
                                ),
                            };
                            match decision {
                                IdleDecision::Continue => thread::yield_now(),
                                IdleDecision::Wait { backend, strategy } => match backend {
                                    WaitBackend::RuntimePark => {
                                        wake.wait_block_on_runtime(epoch, strategy);
                                    }
                                    WaitBackend::Driver => {
                                        wake.wait_block_on_driver(epoch, strategy, |strategy| {
                                            shared_ref.drive_wait(strategy)
                                        })?;
                                    }
                                },
                            }
                        }
                    }
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
        });

        res
    }
}

pub struct RuntimeBuilder<T, WF> {
    worker_count: Option<NonZeroUsize>,
    queue_capacity: NonZeroUsize,
    worker_factory: Option<WF>,
    idle_hook: Option<IdleHook<T>>,
    driver_wait_hook: Option<DriverWaitHook<T>>,
    worker_tick_hook: Option<WorkerTickHook>,
}

impl Default for RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: NonZeroUsize::new(1024).unwrap(),
            worker_factory: Some(|_, _| ()),
            idle_hook: None,
            driver_wait_hook: None,
            worker_tick_hook: None,
        }
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
            driver_wait_hook: None,
            worker_tick_hook: self.worker_tick_hook,
        }
    }

    pub fn with_driver_wait_hook(mut self, hook: DriverWaitHook<T>) -> Self {
        self.driver_wait_hook = Some(hook);
        self
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
            driver_wait_hook: self.driver_wait_hook,
            worker_tick_hook: self.worker_tick_hook,
        }
    }

    pub fn scope<'rt, 'env: 'rt, F, R>(self, f: F) -> Result<R>
    where
        T: 'rt,
        WF: Fn(usize, &'rt RuntimeShared<T>) -> T + Send + Sync + 'rt,
        F: AsyncFnOnce(RuntimeCtx<'rt, T>) -> R,
    {
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
            self.driver_wait_hook,
            self.worker_tick_hook,
        );
        let rt = Runtime {
            shared,
            receivers: Some(receivers),
            worker_factory: self.worker_factory,
            _marker: std::marker::PhantomData,
        };
        rt.block_on(f)
    }
}
