use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::mpsc;
use std::task::Poll;

use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver::driver::{DriveMode, Driver, DriverControlCommand, PlatformDriver};
use veloq_driver::op::{DetachedSubmitter, IntoPlatformOp, Op, OpSubmitter};

use crate::config::{BufferRegistrationMode, Config};
use crate::error::{Result as VeloqResult, from_io_error};
use veloq_runtime::runtime::{IdleDecision, IdleWaitStrategy};

thread_local! {
    /// 线程局部的运行时上下文
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };

    /// 线程局部的注册中心状态
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
    static REGISTRAR_STATE: RefCell<Option<WorkerRegistrarState>> = const { RefCell::new(None) };

    /// 线程局部的驱动控制命令状态
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
    static DRIVER_COMMAND_STATE: RefCell<Option<WorkerDriverCommandState>> = const { RefCell::new(None) };
}

/// 驱动注册中心的消息类型
#[derive(Debug, Clone)]
pub enum RegistrarMessage {
    /// 发现了新的内存块，需要通知驱动注册
    NewChunk(veloq_buf::heap::ChunkInfo),
}

struct WorkerRegistrarState {
    /// 接收来自分发器的广播消息
    receiver: mpsc::Receiver<RegistrarMessage>,
    /// 本地已知的内存块快照
    chunks: Vec<veloq_buf::heap::ChunkInfo>,
}

struct WorkerDriverCommandState {
    receiver: mpsc::Receiver<DriverControlCommand>,
}

/// 初始化当前 Worker 线程的注册中心状态
pub(crate) fn init_worker_registrar_state(receiver: mpsc::Receiver<RegistrarMessage>) {
    REGISTRAR_STATE.with(|state| {
        *state.borrow_mut() = Some(WorkerRegistrarState {
            receiver,
            chunks: Vec::new(),
        });
    });
}

pub(crate) fn init_worker_driver_command_state(receiver: mpsc::Receiver<DriverControlCommand>) {
    DRIVER_COMMAND_STATE.with(|state| {
        *state.borrow_mut() = Some(WorkerDriverCommandState { receiver });
    });
}

#[derive(Clone)]
pub(crate) struct DriverCommandDispatcher {
    senders: Vec<mpsc::Sender<DriverControlCommand>>,
}

impl DriverCommandDispatcher {
    pub(crate) fn new(senders: Vec<mpsc::Sender<DriverControlCommand>>) -> Self {
        Self { senders }
    }

    pub fn dispatch(&self, worker_id: usize, command: DriverControlCommand) {
        if let Some(sender) = self.senders.get(worker_id)
            && sender.send(command).is_ok()
        {
            veloq_runtime::runtime::wake_worker(worker_id);
        }
    }
}

#[derive(Clone)]
pub struct DriverRegistrar {
    driver: Weak<RefCell<PlatformDriver>>,
    registration_mode: BufferRegistrationMode,
}

impl DriverRegistrar {
    pub(crate) fn new(
        driver: Weak<RefCell<PlatformDriver>>,
        registration_mode: BufferRegistrationMode,
    ) -> Self {
        Self {
            driver,
            registration_mode,
        }
    }

    pub fn sync_to_driver(&self) {
        let Some(driver_rc) = self.driver.upgrade() else {
            return;
        };

        REGISTRAR_STATE.with(|state_cell| {
            let mut state_opt = state_cell.borrow_mut();
            let state = state_opt
                .as_mut()
                .expect("Registrar state not initialized for current thread");

            let mut new_chunks = Vec::new();
            while let Ok(msg) = state.receiver.try_recv() {
                match msg {
                    RegistrarMessage::NewChunk(chunk) => {
                        new_chunks.push(chunk);
                    }
                }
            }

            if new_chunks.is_empty() {
                return;
            }

            if matches!(self.registration_mode, BufferRegistrationMode::Compatible) {
                let mut driver = driver_rc.borrow_mut();
                for chunk in &new_chunks {
                    let _ = driver.register_chunk(chunk.id, chunk.ptr.as_ptr(), chunk.len.get());
                }
            }

            // 更新本地快照
            state.chunks.extend(new_chunks);
        });
    }
}

impl veloq_buf::BufferRegistrar for DriverRegistrar {
    fn register(&self, regions: &[veloq_buf::BufferRegion]) -> std::io::Result<Vec<usize>> {
        let driver_rc = self
            .driver
            .upgrade()
            .ok_or_else(|| std::io::Error::other("driver dropped"))?;
        let mut driver = driver_rc.borrow_mut();

        let mut indices = Vec::with_capacity(regions.len());
        let mut new_chunks = Vec::with_capacity(regions.len());

        for (idx, region) in regions.iter().enumerate() {
            let chunk_idx = idx as u16;
            driver
                .register_chunk(chunk_idx, region.as_ptr(), region.len())
                .map_err(|err| std::io::Error::other(format!("{err:#}")))?;

            new_chunks.push(veloq_buf::heap::ChunkInfo {
                id: chunk_idx,
                ptr: unsafe { std::ptr::NonNull::new_unchecked(region.as_ptr() as *mut u8) },
                len: unsafe { std::num::NonZeroUsize::new_unchecked(region.len()) },
            });
            indices.push(idx);
        }

        REGISTRAR_STATE.with(|state_cell| {
            let mut state_opt = state_cell.borrow_mut();
            if let Some(state) = state_opt.as_mut() {
                state.chunks.extend(new_chunks);
            }
        });

        Ok(indices)
    }

    fn resolve_chunk_info(&self, chunk_id: u16) -> Option<veloq_buf::heap::ChunkInfo> {
        // 首先在本地快照中查找
        let found = REGISTRAR_STATE.with(|state_cell| {
            state_cell
                .borrow()
                .as_ref()
                .and_then(|state| state.chunks.iter().find(|c| c.id == chunk_id).copied())
        });

        if let Some(chunk) = found {
            return Some(chunk);
        }

        // 如果没找到，尝试同步一次消息队列后再查找
        self.sync_to_driver();

        REGISTRAR_STATE.with(|state_cell| {
            state_cell
                .borrow()
                .as_ref()
                .and_then(|state| state.chunks.iter().find(|c| c.id == chunk_id).copied())
        })
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeDriverBridge {
    driver: Rc<RefCell<PlatformDriver>>,
    registrar: DriverRegistrar,
}

impl RuntimeDriverBridge {
    fn new(driver: Rc<RefCell<PlatformDriver>>, registrar: DriverRegistrar) -> Self {
        Self { driver, registrar }
    }

    #[inline]
    pub fn sync_registrar(&self) {
        self.registrar.sync_to_driver();
    }

    /// 让当前线程的驱动在空闲时进入等待推进。
    ///
    /// 这个入口会优先利用驱动后端的阻塞等待能力，避免固定轮询兜底。
    pub fn drive_wait(&self) -> IdleDecision {
        self.sync_registrar();
        let driver_rc = self.driver.clone();
        let mut driver = driver_rc.borrow_mut();
        let outcome = driver
            .drive(DriveMode::Wait)
            .unwrap_or_else(|err| panic!("driver drive(Wait) failed: {err:#}"));
        if !outcome.pending_progress {
            return IdleDecision::wait(IdleWaitStrategy::block());
        }
        match outcome.next_timeout_hint {
            Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
            None => IdleDecision::wait(IdleWaitStrategy::block()),
        }
    }
}

#[derive(Clone)]
pub struct RuntimeContext {
    buf_pool: AnyBufPool,
    driver: Rc<RefCell<PlatformDriver>>,
    config: Config,
    registrar: DriverRegistrar,
    driver_commands: DriverCommandDispatcher,
}

impl RuntimeContext {
    pub(crate) fn new(
        driver: Rc<RefCell<PlatformDriver>>,
        buf_pool: AnyBufPool,
        config: Config,
        registrar: DriverRegistrar,
        driver_commands: DriverCommandDispatcher,
    ) -> Self {
        Self {
            buf_pool,
            driver,
            config,
            registrar,
            driver_commands,
        }
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.buf_pool.clone()
    }

    #[inline]
    pub fn driver(&self) -> Rc<RefCell<PlatformDriver>> {
        self.driver.clone()
    }

    #[inline]
    pub fn config(&self) -> Config {
        self.config.clone()
    }

    #[inline]
    pub fn registrar(&self) -> DriverRegistrar {
        self.registrar.clone()
    }

    #[inline]
    pub(crate) fn driver_bridge(&self) -> RuntimeDriverBridge {
        RuntimeDriverBridge::new(self.driver.clone(), self.registrar.clone())
    }

    #[inline]
    pub(crate) fn driver_commands(&self) -> DriverCommandDispatcher {
        self.driver_commands.clone()
    }
}

pub(crate) fn set_current_runtime_context(context: RuntimeContext) {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(context);
    });
}

pub(crate) fn clear_current_runtime_context() {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = None;
    });
}

pub fn try_current() -> Option<RuntimeContext> {
    CONTEXT.with(|ctx| ctx.borrow().clone())
}

pub fn current() -> RuntimeContext {
    try_current()
        .expect("Runtime context not set. Are you running inside veloq::Runtime::block_on?")
}

pub fn current_pool() -> Option<AnyBufPool> {
    try_current().map(|ctx| ctx.buf_pool())
}

pub fn try_alloc_from_pool(size: NonZeroUsize) -> Option<FixedBuf> {
    current_pool().and_then(|pool| pool.alloc(size))
}

pub fn try_alloc(size: NonZeroUsize) -> Result<FixedBuf, veloq_buf::AllocError> {
    try_alloc_from_pool(size).map_or_else(|| FixedBuf::alloc_heap(size), Ok)
}

pub fn alloc(size: NonZeroUsize) -> FixedBuf {
    try_alloc(size).expect("failed to allocate buffer")
}

pub fn poll_current_driver() -> IdleDecision {
    let Some(ctx) = try_current() else {
        return IdleDecision::wait(IdleWaitStrategy::block());
    };
    let bridge = ctx.driver_bridge();
    bridge.sync_registrar();
    let driver_rc = bridge.driver.clone();
    let mut driver = driver_rc.borrow_mut();
    let outcome = driver
        .drive(DriveMode::Poll)
        .unwrap_or_else(|err| panic!("driver drive(Poll) failed: {err:#}"));
    match outcome.next_timeout_hint {
        Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
        None if outcome.pending_progress => IdleDecision::continue_now(),
        None => IdleDecision::wait(IdleWaitStrategy::block()),
    }
}

pub fn wait_current_driver() -> IdleDecision {
    let Some(ctx) = try_current() else {
        return IdleDecision::wait(IdleWaitStrategy::block());
    };
    ctx.driver_bridge().drive_wait()
}

pub(crate) fn drain_pending_driver_commands() {
    let Some(ctx) = try_current() else {
        return;
    };

    DRIVER_COMMAND_STATE.with(|state_cell| {
        let mut state_opt = state_cell.borrow_mut();
        let Some(state) = state_opt.as_mut() else {
            return;
        };

        let mut pending = Vec::new();
        while let Ok(command) = state.receiver.try_recv() {
            pending.push(command);
        }

        if pending.is_empty() {
            return;
        }

        let driver_rc = ctx.driver();
        let mut driver = driver_rc.borrow_mut();
        for command in pending {
            match command {
                DriverControlCommand::UnregisterFiles(files) => {
                    let _ = driver.unregister_files(files);
                }
            }
        }
    });
}

pub fn submit<'a, S, T>(
    submitter: &'a S,
    op: Op<T>,
) -> impl Future<Output = <S::Future<T> as Future>::Output> + 'a
where
    S: OpSubmitter + Copy + 'a,
    T: IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    let ctx = current();
    ctx.driver_bridge().sync_registrar();
    let fut = submitter.submit(op, ctx.driver());
    let mut fut = Box::pin(fut);

    async move {
        std::future::poll_fn(
            move |cx: &mut std::task::Context<'_>| match fut.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => Poll::Pending,
            },
        )
        .await
    }
}

pub async fn yield_now() {
    if let Some(ctx) = try_current() {
        ctx.driver_bridge().sync_registrar();
    }
    veloq_runtime::task::yield_now().await;
}

pub async fn submit_to<T>(
    worker_id: usize,
    op: Op<T>,
) -> VeloqResult<(
    Result<
        <T as IntoPlatformOp<<PlatformDriver as Driver>::Op>>::Completion,
        veloq_driver::error::DriverErrorReport,
    >,
    T,
)>
where
    T: IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    if veloq_runtime::runtime::current_worker_id() == worker_id {
        let (res, op_back) = submit(&DetachedSubmitter::new(), op).await.into_inner();
        let op = op_back.expect("Op lost in local submit");
        Ok((res, op))
    } else {
        let routed = veloq_runtime::runtime::route::route_to_worker(worker_id, move || {
            let ctx = current();
            let driver_rc = ctx.driver();
            let mut driver = driver_rc.borrow_mut();
            op.submit_detached(&mut *driver)
        })
        .map_err(from_io_error)?;
        let (res, op_back) = routed.await.into_inner();
        let op = op_back.expect("Op lost in remote submit");
        Ok((res, op))
    }
}
