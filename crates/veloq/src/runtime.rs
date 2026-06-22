pub mod context;

use std::{
    cell::RefCell,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    sync::{Arc, mpsc},
    thread,
};

use diagweave::Transform;

use veloq_buf::PoolTopology;
use veloq_driver_native::driver::PlatformDriver;
use veloq_runtime::{
    runtime::{self as async_runtime},
    utils::StaticTransfer,
};

pub use veloq_runtime::{scope, scope_local};

use crate::{
    config::{BlockingPoolConfig, Config},
    error::Result as VeloqResult,
    runtime::context::{
        BorrowedRegistrar, Ctx, RegistrarMessage, SharedRegistrar, WorkerRegistrarState,
        WorkerState, poll_current_driver, wait_current_driver,
    },
};

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

    pub fn worker_count(mut self, worker_count: Option<NonZeroUsize>) -> Self {
        self.config = self.config.worker_threads(worker_count);
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
        F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
    {
        let worker_count = self.config.get_worker_threads_opt().unwrap_or_else(|| {
            thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
        });

        let state = self.topology.init(worker_count.get()).trans()?;

        let runtime = Runtime {
            worker_count,
            topology: self.topology,
            state,
            config: self.config,
        };

        runtime.block_on(f)
    }
}

pub struct Runtime<T: PoolTopology> {
    worker_count: NonZeroUsize,
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

    pub fn scope<F, R>(topology: T, f: F) -> VeloqResult<R>
    where
        F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
    {
        RuntimeBuilder::new(topology).scope(f)
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<R, F>(self, f: F) -> VeloqResult<R>
    where
        F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
    {
        let Runtime {
            worker_count,
            topology,
            state,
            config,
            ..
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

        async_runtime::RuntimeBuilder::new()
            .with_worker_count(Some(worker_count))
            .with_queue_capacity(config.get_queue_capacity())
            .with_idle_hook(poll_current_driver)
            .with_worker_wait_hook(wait_current_driver)
            .with_worker_factory(move |worker_id, shared| {
                let topology = topology.clone();
                let state = state.clone();
                let config = config.clone();
                let receiver = receivers.take(worker_id);

                let registration_mode = config.registration_mode();
                let registrar = unsafe { SharedRegistrar::from_shared(shared) };
                let registrar_state = RefCell::new(WorkerRegistrarState {
                    receiver,
                    chunks: Vec::new(),
                });

                let driver = PlatformDriver::new(config.clone(), registrar)
                    .expect("failed to create driver");

                let driver_cell = RefCell::new(driver);

                let buf_pool = {
                    let borrowed_registrar = BorrowedRegistrar {
                        driver: &driver_cell,
                        state: &registrar_state,
                        registration_mode,
                    };
                    topology
                        .build(&state, worker_id, &borrowed_registrar)
                        .expect("failed to build worker buffer pool")
                };

                WorkerState {
                    driver: driver_cell,
                    buf_pool,
                    registrar_state,
                    registration_mode,
                }
            })
            .scope(async move |runtime_ctx| {
                let ctx = Ctx { runtime_ctx };
                f(ctx).await
            })
            .trans()
    }
}
