pub mod context;

pub use veloq_runtime::{scope, scope_local};

use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::{Arc, mpsc};

use veloq_blocking::init_blocking_pool;
use veloq_buf::PoolTopology;
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_runtime::runtime::{self as async_runtime, WorkerInitContext};

use crate::config::Config;
use crate::net::route::{SocketRouteDispatcher, init_worker_socket_route_state};
use crate::runtime::context::{DriverCommandDispatcher, init_worker_driver_command_state};
use crate::runtime::context::{DriverRegistrar, RegistrarMessage};

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

struct StaticTransfer<T>(Box<[Option<T>]>);

unsafe impl<T: Send> Sync for StaticTransfer<T> {}

impl<T> StaticTransfer<T> {
    fn new(items: Vec<T>) -> Self {
        Self(items.into_iter().map(Some).collect())
    }

    fn take(&self, index: usize) -> T {
        unsafe {
            let ptr = self.0.as_ptr() as *mut Option<T>;
            (*ptr.add(index))
                .take()
                .expect("Worker item already taken or index out of bounds")
        }
    }
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

    pub fn block_on<F: Future>(self, future: F) -> F::Output {
        let Runtime {
            worker_count,
            topology,
            state,
            config,
        } = self;

        struct ClearCurrentContext;
        impl Drop for ClearCurrentContext {
            fn drop(&mut self) {
                context::clear_current_runtime_context();
            }
        }

        let _clear = ClearCurrentContext;

        // 预先为每个 Worker 创建消息通道
        let mut senders = Vec::with_capacity(worker_count.get());
        let mut receivers = Vec::with_capacity(worker_count.get());
        let mut command_senders = Vec::with_capacity(worker_count.get());
        let mut command_receivers = Vec::with_capacity(worker_count.get());
        let mut socket_route_senders = Vec::with_capacity(worker_count.get());
        let mut socket_route_receivers = Vec::with_capacity(worker_count.get());
        for _ in 0..worker_count.get() {
            let (tx, rx) = mpsc::channel();
            senders.push(tx);
            receivers.push(rx);
            let (cmd_tx, cmd_rx) = mpsc::channel();
            command_senders.push(cmd_tx);
            command_receivers.push(cmd_rx);
            let (socket_tx, socket_rx) = mpsc::channel();
            socket_route_senders.push(socket_tx);
            socket_route_receivers.push(socket_rx);
        }

        let receivers = Arc::new(StaticTransfer::new(receivers));
        let command_receivers = Arc::new(StaticTransfer::new(command_receivers));
        let socket_route_receivers = Arc::new(StaticTransfer::new(socket_route_receivers));
        let dispatcher = Arc::new(RegistrarDispatcher { senders });
        let command_dispatcher = DriverCommandDispatcher::new(command_senders);
        let socket_route_dispatcher = SocketRouteDispatcher::new(socket_route_senders);

        // 连接内存池监听器到分发器
        let dispatcher_clone = dispatcher.clone();
        topology.connect_listener(
            &state,
            Box::new(move |chunk_info| {
                dispatcher_clone.broadcast(RegistrarMessage::NewChunk(chunk_info));
            }),
        );

        let runtime = async_runtime::Runtime::builder()
            .worker_count(worker_count)
            .queue_capacity(config.get_queue_capacity())
            .idle_hook(crate::runtime::context::poll_current_driver)
            .worker_tick_hook(crate::runtime::context::drain_pending_driver_commands)
            .with_worker_init(move |worker_ctx: WorkerInitContext| {
                let topology = topology.clone();
                let state = state.clone();
                let config = config.clone();
                let receiver = receivers.take(worker_ctx.worker_id());
                let command_receiver = command_receivers.take(worker_ctx.worker_id());
                let socket_route_receiver = socket_route_receivers.take(worker_ctx.worker_id());
                let command_dispatcher = command_dispatcher.clone();
                let socket_route_dispatcher = socket_route_dispatcher.clone();

                async move {
                    let driver = Rc::new(RefCell::new(
                        PlatformDriver::new(&config).expect("failed to create driver"),
                    ));

                    // 初始化 TLS 中的注册中心状态
                    context::init_worker_registrar_state(receiver);
                    init_worker_driver_command_state(command_receiver);
                    init_worker_socket_route_state(socket_route_receiver);

                    let registration_mode = config.registration_mode();
                    let registrar = DriverRegistrar::new(Rc::downgrade(&driver), registration_mode);

                    {
                        let mut driver_ref = driver.borrow_mut();
                        driver_ref.set_registrar(Box::new(registrar.clone()));
                    }

                    let buf_pool =
                        topology.build(&state, worker_ctx.worker_id(), Box::new(registrar.clone()));
                    context::set_current_runtime_context(context::RuntimeContext::new(
                        driver,
                        buf_pool,
                        config,
                        registrar,
                        command_dispatcher,
                        socket_route_dispatcher,
                    ));
                }
            })
            .build();

        runtime.block_on(future)
    }
}
