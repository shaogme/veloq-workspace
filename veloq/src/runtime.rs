pub mod context;

use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::AsyncFnOnce;
use std::sync::{Arc, mpsc};
use std::thread;

use veloq_blocking::init_blocking_pool;
use veloq_buf::PoolTopology;
use veloq_driver_native::driver::PlatformDriver;
use veloq_runtime::runtime::{self as async_runtime};
use veloq_runtime::utils::storage::StaticTransfer;

use crate::config::{BlockingPoolConfig, Config};
use crate::runtime::context::{
    BorrowedRegistrar, DriverRegistrar, RegistrarMessage, RuntimeContext, WorkerRegistrarState,
    WorkerState, poll_current_driver,
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

    pub fn worker_count(mut self, worker_count: NonZeroUsize) -> Self {
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

    pub fn build<'a>(self) -> io::Result<Runtime<'a, T>> {
        let worker_count = self.config.get_worker_threads_opt().unwrap_or_else(|| {
            thread::available_parallelism()
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
            marker: PhantomData,
        })
    }
}

pub struct Runtime<'ctx, T: PoolTopology> {
    worker_count: NonZeroUsize,
    topology: T,
    state: T::State,
    config: Config,
    marker: PhantomData<&'ctx ()>,
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

impl<'ctx, T: PoolTopology + 'ctx> Runtime<'ctx, T> {
    pub fn builder(topology: T) -> RuntimeBuilder<T> {
        RuntimeBuilder::new(topology)
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<R, F>(self, f: F) -> R
    where
        F: AsyncFnOnce(RuntimeContext<'ctx>) -> R,
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

        let runtime = async_runtime::RuntimeBuilder::new()
            .with_worker_count(worker_count.get())
            .with_queue_capacity(config.get_queue_capacity().get())
            .with_idle_hook(poll_current_driver)
            .with_worker_extra(move |worker_id, shared| {
                let topology = topology.clone();
                let state = state.clone();
                let config = config.clone();
                let receiver = receivers.take(worker_id);

                let registration_mode = config.registration_mode();
                let registrar = DriverRegistrar::new(shared, registration_mode);

                let registrar_state = RefCell::new(WorkerRegistrarState {
                    receiver,
                    chunks: Vec::new(),
                });

                let driver = PlatformDriver::new(config.clone(), Box::new(registrar.clone()))
                    .expect("failed to create driver");
                let driver_cell = RefCell::new(driver);

                let buf_pool = {
                    let borrowed_registrar = BorrowedRegistrar {
                        driver: &driver_cell,
                        state: &registrar_state,
                        registration_mode,
                    };
                    topology.build(&state, worker_id, Box::new(borrowed_registrar))
                };

                WorkerState {
                    driver: driver_cell,
                    buf_pool,
                    registrar,
                    registrar_state,
                }
            })
            .build();

        runtime.block_on(async move |scope| {
            let ctx = RuntimeContext { scope };
            f(ctx).await
        })
    }
}
