pub mod context;

use std::num::NonZeroUsize;
use std::ops::AsyncFnOnce;
use std::sync::{Arc, mpsc};

use veloq_blocking::init_blocking_pool;
use veloq_buf::PoolTopology;
use veloq_driver_native::driver::{Driver, PlatformDriver};
use veloq_runtime::runtime::{self as async_runtime, WorkerInitContext};
use veloq_runtime::utils::storage::StaticTransfer;

use crate::config::Config;
use crate::runtime::context::{DriverRegistrar, RegistrarMessage};

use veloq_runtime::runtime::RuntimeScopeContext;

pub struct RuntimeBuilder<T: PoolTopology> {
    topology: T,
    config: Config,
}

impl<T: PoolTopology> RuntimeBuilder<T> {
    pub fn new(topology: T) -> Self {
        Self {
            topology,
            config: Config::default(),
        }
    }

    pub fn worker_count(mut self, worker_count: std::num::NonZeroUsize) -> Self {
        self.config = self.config.worker_threads(worker_count.get());
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

    pub fn blocking_pool(mut self, blocking_pool: crate::config::BlockingPoolConfig) -> Self {
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

    pub fn build(self) -> std::io::Result<Runtime<T>> {
        let worker_count = self.config.get_worker_threads_opt().unwrap_or_else(|| {
            std::thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
        });

        // Initialize blocking pool using config
        init_blocking_pool(self.config.get_blocking_pool_config().clone());

        let state = self.topology.init(worker_count.get())?;

        Ok(Runtime {
            worker_count,
            topology: self.topology,
            state,
            config: self.config,
        })
    }
}

pub struct Runtime<T: PoolTopology> {
    worker_count: std::num::NonZeroUsize,
    topology: T,
    state: T::State,
    config: Config,
}

struct RegistrarDispatcher {
    senders: Vec<mpsc::Sender<RegistrarMessage>>,
}

impl RegistrarDispatcher {
    fn broadcast(&self, msg: RegistrarMessage) {
        for sender in &self.senders {
            let _ = sender.send(msg.clone());
        }
    }
}

impl<T: PoolTopology> Runtime<T> {
    pub fn builder(topology: T) -> RuntimeBuilder<T> {
        RuntimeBuilder::new(topology)
    }

    pub fn worker_count(&self) -> std::num::NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<R, F>(self, f: F) -> R
    where
        F: AsyncFnOnce(&crate::runtime::context::RuntimeContext) -> R,
    {
        let Runtime {
            worker_count,
            topology,
            state,
            config,
        } = self;

        // 预先为每个 Worker 创建消息通道
        let mut senders = Vec::with_capacity(worker_count.get());
        let mut receivers = Vec::with_capacity(worker_count.get());
        for _ in 0..worker_count.get() {
            let (tx, rx) = mpsc::channel();
            senders.push(tx);
            receivers.push(rx);
        }

        let receivers = Arc::new(StaticTransfer::new(receivers));
        let dispatcher = Arc::new(RegistrarDispatcher { senders });

        // 连接内存池监听器到分发器
        let dispatcher_clone = dispatcher.clone();
        topology.connect_listener(
            &state,
            Box::new(move |chunk_info| {
                dispatcher_clone.broadcast(RegistrarMessage::NewChunk(chunk_info));
            }),
        );

        let runtime = async_runtime::RuntimeBuilder::new()
            .with_worker_count(worker_count.get())
            .with_queue_capacity(config.get_queue_capacity().get())
            .with_idle_hook(crate::runtime::context::poll_current_driver)
            .with_worker_init(
                async move |worker_ctx: WorkerInitContext<crate::runtime::context::WorkerState>| {
                    let topology = topology.clone();
                    let state = state.clone();
                    let config = config.clone();
                    let receiver = receivers.take(worker_ctx.worker_id());

                    let mut driver = PlatformDriver::new(&config).expect("failed to create driver");
                    let registration_mode = config.registration_mode();
                    let registrar =
                        DriverRegistrar::new(worker_ctx.shared().clone(), registration_mode);

                    driver.set_registrar(Box::new(registrar.clone()));

                    let tls_ptr = worker_ctx.shared().context_tls.get().unwrap();
                    let ctx = unsafe { &*tls_ptr.as_ptr() };

                    // 必须在调用 topology.build 之前存入驱动，因为 Registrar 需要驱动来注册缓冲区
                    *ctx.extra.driver.borrow_mut() = Some(driver);

                    let buf_pool =
                        topology.build(&state, worker_ctx.worker_id(), Box::new(registrar.clone()));

                    *ctx.extra.buf_pool.borrow_mut() = Some(buf_pool);
                    *ctx.extra.registrar.borrow_mut() = Some(registrar);

                    crate::runtime::context::init_worker_registrar_state(&ctx.extra, receiver);
                },
            )
            .build();

        runtime.block_on(
            async move |scope: &RuntimeScopeContext<crate::runtime::context::WorkerState>| {
                let ctx = crate::runtime::context::RuntimeContext {
                    scope: scope.clone(),
                };
                f(&ctx).await
            },
        )
    }
}
