pub mod context;

use veloq_std::{
    boxed::Box,
    cell::RefCell,
    error::Error,
    format,
    mem::transmute,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    sync::{Arc, NativeArc, UnpoisonedMutex},
    thread,
    vec::Vec,
};

use diagweave::{DiagnosticError, Transform, report::ResultReportExt};

use veloq_buf::{BufError, PoolInitContext, PoolTopology};

use veloq_driver_native::driver::{
    Driver, DriverRaw, DriverStartup, DriverStartupResources, PlatformDriver, RemoteWaker,
};
use veloq_runtime::{self as async_runtime, RuntimeError, RuntimeWakeError, RuntimeWaker};

pub use veloq_runtime::{IntoOutcome, Outcome, join, scope, scope_local, select, task};

pub mod scope {
    pub use veloq_runtime::{JoinHandle, ScopeProvider};
}

pub mod task {
    pub use veloq_runtime::TaskHandleRef;
}

use crate::{
    config::{BlockingPoolConfig, Config},
    error::Result as VeloqResult,
    runtime::context::{
        BorrowedRegistrar, Ctx, WorkerState, park_current_driver, poll_current_driver,
    },
};

pub struct RuntimeBuilder<T: PoolTopology> {
    topology: T,
    config: Config,
    buffer_registration_workers: Option<Vec<usize>>,
}

impl<T: PoolTopology> RuntimeBuilder<T> {
    pub fn new(topology: T) -> Self {
        Self {
            topology,
            config: Config::default(),
            buffer_registration_workers: None,
        }
    }

    pub fn worker_count(mut self, worker_count: Option<NonZeroUsize>) -> Self {
        self.config = self.config.worker_threads(worker_count);
        self
    }

    /// Selects workers that register shared buffers and receive backend startup resources.
    /// 选择负责注册共享 buffer 并接收后端启动资源的 worker。
    ///
    /// The default selects every worker; an empty list selects none.
    /// 默认选择所有 worker；空列表表示不选择任何 worker。
    pub fn buffer_registration_workers(mut self, workers: Vec<usize>) -> Self {
        self.buffer_registration_workers = Some(workers);
        self
    }

    pub fn direct_io(mut self, direct_io: bool) -> Self {
        self.config = self.config.direct_io(direct_io);
        self
    }

    pub fn queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.config = self.config.queue_capacity(capacity);
        self
    }

    pub fn blocking_pool(mut self, blocking_pool: BlockingPoolConfig) -> Self {
        self.config = self.config.blocking_pool(blocking_pool);
        self
    }

    pub fn with_config<F>(mut self, f: F) -> Self
    where
        F: FnOnce(Config) -> Config,
    {
        self.config = f(self.config);
        self
    }

    pub fn scope<F, R>(self, f: F) -> VeloqResult<R>
    where
        F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
    {
        let worker_count = self.config.get_worker_threads_opt().unwrap_or_else(|| {
            thread::ThreadApi::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
        });

        let registration_workers = self
            .buffer_registration_workers
            .unwrap_or_else(|| (0..worker_count.get()).collect());
        let pool_context = PoolInitContext::new(
            worker_count.get(),
            registration_workers,
            self.config.get_locked_memory_budget(),
        )
        .trans()?;
        let requirements = <PlatformDriver<'static> as DriverStartup>::startup_requirements(
            self.config.as_ref(),
            pool_context.registration_workers(),
        )
        .trans()?;
        let mut startup_resources_by_worker: Vec<Option<DriverStartupResources>> =
            (0..worker_count.get()).map(|_| None).collect();
        if let Some(bytes) = requirements.locked_memory_bytes_per_worker() {
            let permits = pool_context
                .budget()
                .try_reserve_repeated_unaligned(
                    bytes.get(),
                    pool_context.registration_workers().len(),
                )
                .map_err(|error| BufError::Other(format!("{error:?}")).to_report())
                .trans()?;
            if let Some(permits) = permits {
                for (worker, permit) in pool_context
                    .registration_workers()
                    .iter()
                    .copied()
                    .zip(permits)
                {
                    startup_resources_by_worker[worker] =
                        Some(DriverStartupResources::new(Some(permit)));
                }
            }
        }
        let startup_resources_by_worker =
            Arc::new(UnpoisonedMutex::new(startup_resources_by_worker));
        let state = self.topology.init(pool_context).trans()?;

        let runtime = Runtime {
            worker_count,
            topology: self.topology,
            state,
            config: self.config,
            startup_resources_by_worker,
        };

        runtime.block_on(f)
    }
}

pub struct Runtime<T: PoolTopology> {
    worker_count: NonZeroUsize,
    topology: T,
    state: T::State,
    config: Config,
    startup_resources_by_worker: Arc<UnpoisonedMutex<Vec<Option<DriverStartupResources>>>>,
}

impl<T: PoolTopology> Runtime<T> {
    pub fn builder(topology: T) -> RuntimeBuilder<T> {
        RuntimeBuilder::new(topology)
    }

    pub fn scope<F, R>(topology: T, f: F) -> VeloqResult<R>
    where
        F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
    {
        RuntimeBuilder::new(topology).scope(f)
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<R, F>(self, f: F) -> VeloqResult<R>
    where
        F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
    {
        let Runtime {
            worker_count,
            topology,
            state,
            config,
            startup_resources_by_worker,
            ..
        } = self;

        async_runtime::RuntimeBuilder::new()
            .with_worker_count(Some(worker_count))
            .with_queue_capacity(config.get_queue_capacity())
            .with_idle_hook(poll_current_driver)
            .with_park_hook(park_current_driver)
            .with_worker_factory(
                move |worker_id: usize,
                      shared: &async_runtime::RuntimeShared<WorkerState<'static>>| {
                    let topology = topology.clone();
                    let state = state.clone();
                    let config = config.clone();
                    let startup_resources = startup_resources_by_worker
                        .lock()
                        .get_mut(worker_id)
                        .and_then(Option::take)
                        .unwrap_or_default();

                    let build_result = (|| -> VeloqResult<WorkerState<'static>> {
                        let driver =
                            <PlatformDriver<'static> as DriverStartup>::new_with_startup_resources(
                                config.clone(),
                                startup_resources,
                            )
                            .trans()
                            .attach_note("failed to create worker driver")?;

                        let remote_waker = driver.create_waker();
                        let backend = <PlatformDriver<'static> as DriverRaw>::BACKEND_NAME;
                        struct DriverWakerWrapper<E> {
                            waker: Arc<dyn RemoteWaker<E>>,
                            worker_id: usize,
                            backend: &'static str,
                        }
                        impl<E> RuntimeWaker for DriverWakerWrapper<E>
                        where
                            E: Error + Send + Sync + 'static,
                        {
                            fn wake(&self) -> Result<(), RuntimeWakeError> {
                                self.waker.wake().map_err(|err| {
                                    RuntimeWakeError::new(
                                        self.backend,
                                        self.worker_id,
                                        "remote_waker.wake",
                                        format!("{err:#}"),
                                    )
                                })
                            }
                        }
                        shared.base().unparkers()[worker_id]
                            .bind(unsafe {
                                NativeArc::new_unsized(
                                    DriverWakerWrapper {
                                        waker: remote_waker,
                                        worker_id,
                                        backend,
                                    },
                                    |p| p as *const dyn RuntimeWaker,
                                )
                            })
                            .map_err(|source| {
                                RuntimeWakeError::new(
                                    "runtime",
                                    worker_id,
                                    "bind_worker_remote_waker",
                                    format!("{source}"),
                                )
                            })
                            .map_err(|source| RuntimeError::WakeFailed { source })
                            .trans()?;

                        let driver_cell = RefCell::new(driver);

                        let buf_pool = {
                            let borrowed_registrar = BorrowedRegistrar {
                                driver: &driver_cell,
                            };
                            topology
                                .build(&state, worker_id, &borrowed_registrar)
                                .trans()
                                .attach_note("failed to build worker buffer pool")?
                        };

                        // The driver retains the pool so fixed registrations cannot outlive their backing allocation.
                        // driver 保留 pool，确保固定注册的生命周期不会超过其 backing allocation。
                        driver_cell
                            .borrow_mut()
                            .attach_buffer_pool(buf_pool.clone())
                            .trans()
                            .attach_note("failed to attach the worker buffer pool to the driver")?;

                        Ok(WorkerState {
                            driver: driver_cell,
                            buf_pool,
                        })
                    })();

                    build_result
                        .map_err(move |report| Box::new(report) as Box<dyn Error + Send + Sync>)
                },
            )
            .scope(async move |runtime_ctx| {
                // SAFETY: runtime_ctx 内部包含的 RuntimeShared 悬挂指针在 block_on 期间始终有效，
                // transmute 仅调整句柄的生命周期标记以匹配 Ctx<'s> 结构体定义。
                let runtime_ctx = unsafe {
                    transmute::<
                        async_runtime::RuntimeCtx<'_, WorkerState<'_>>,
                        async_runtime::RuntimeCtx<'_, WorkerState<'_>>,
                    >(runtime_ctx)
                };
                let ctx = Ctx { runtime_ctx };
                f(ctx).await
            })
            .trans()
    }
}
