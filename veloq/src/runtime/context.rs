use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::mpsc;

use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver_native::driver::{DriveMode, Driver, PlatformDriver};
use veloq_driver_native::op::{DetachedSubmitter, IntoPlatformOp, Op, OpSubmitter};

use crate::config::BufferRegistrationMode;
use crate::error::{Result as VeloqResult, from_io_error};
use veloq_runtime::runtime::{IdleDecision, IdleWaitStrategy, RuntimeShared};

thread_local! {
    /// 线程局部的运行时上下文
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };

    /// 线程局部的注册中心状态
    static REGISTRAR_STATE: RefCell<Option<WorkerRegistrarState>> = const { RefCell::new(None) };
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

/// 初始化当前 Worker 线程的注册中心状态
pub(crate) fn init_worker_registrar_state(receiver: mpsc::Receiver<RegistrarMessage>) {
    REGISTRAR_STATE.with(|state| {
        *state.borrow_mut() = Some(WorkerRegistrarState {
            receiver,
            chunks: Vec::new(),
        });
    });
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
    registrar: DriverRegistrar,
}

impl RuntimeContext {
    pub(crate) fn new(
        driver: Rc<RefCell<PlatformDriver>>,
        buf_pool: AnyBufPool,
        registrar: DriverRegistrar,
    ) -> Self {
        Self {
            buf_pool,
            driver,
            registrar,
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
    pub fn registrar(&self) -> DriverRegistrar {
        self.registrar.clone()
    }

    #[inline]
    pub(crate) fn driver_bridge(&self) -> RuntimeDriverBridge {
        RuntimeDriverBridge::new(self.driver.clone(), self.registrar.clone())
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

pub fn poll_current_driver(_: &RuntimeShared<()>) -> IdleDecision {
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

pub fn submit<'a, S, T>(submitter: &'a S, op: Op<T>) -> S::Future<T>
where
    S: OpSubmitter + Copy + 'a,
    T: IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
            ErasedPayload = <PlatformDriver as Driver>::UP,
        > + Send,
    <S as OpSubmitter>::Future<T>: 'a,
{
    let ctx = current();
    ctx.driver_bridge().sync_registrar();
    submitter.submit(op, ctx.driver())
}

pub async fn yield_now() {
    if let Some(ctx) = try_current() {
        ctx.driver_bridge().sync_registrar();
    }
    veloq_runtime::task::yield_now().await;
}

pub async fn submit_to<'a, T>(
    ctx: &veloq_runtime::runtime::RuntimeScopeContext<()>,
    worker_id: usize,
    op: Op<T>,
) -> VeloqResult<(
    Result<
        <T as IntoPlatformOp<<PlatformDriver as Driver>::Op>>::Completion,
        veloq_driver_native::error::DriverErrorReport,
    >,
    T::Output,
)>
where
    T: IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
            ErasedPayload = <PlatformDriver as Driver>::UP,
        > + Send
        + 'a,
{
    if ctx.worker_id() == worker_id {
        let (res, op_back) = submit(&DetachedSubmitter::new(), op).await.into_inner();
        let op = op_back.expect("Op lost in local submit");
        Ok((res, op))
    } else {
        let routed = ctx
            .route_to(worker_id, move || {
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

pub(crate) fn submit_control_task(
    shared: &veloq_runtime::runtime::shared::RuntimeShared<()>,
    worker_id: usize,
    fd: veloq_driver_native::op::IoFd,
) {
    struct UnregisterFileTask {
        header: veloq_runtime::task::TaskHeader,
        fd: veloq_driver_native::op::IoFd,
    }

    impl veloq_runtime::task::RawTask for UnregisterFileTask {
        type Storage = veloq_runtime::utils::storage::AtomicStorage;

        fn poll_raw(&self, _worker_id: usize) -> bool {
            if let Some(ctx) = try_current() {
                let _ = ctx.driver().borrow_mut().unregister_files(vec![self.fd]);
            }
            self.header.mark_completed_and_notify();
            unsafe {
                let ptr = self as *const Self as *mut Self;
                let _ = Box::from_raw(ptr);
            }
            true
        }

        fn header(&self) -> &veloq_runtime::task::GenericTaskHeader<Self::Storage> {
            &self.header
        }
    }

    impl UnregisterFileTask {
        const VTABLE: &'static veloq_runtime::task::TaskVTable<
            veloq_runtime::utils::storage::AtomicStorage,
        > = &veloq_runtime::task::TaskVTable {
            wake: |_| {},
            wake_by_ref: |_| {},
            poll: |data, worker_id| unsafe {
                let node = &*(data.as_ptr() as *const Self);
                veloq_runtime::task::RawTask::poll_raw(node, worker_id)
            },
        };
    }

    let task = Box::new(UnregisterFileTask {
        header: veloq_runtime::task::TaskHeader::new(UnregisterFileTask::VTABLE),
        fd,
    });

    task.header.set_pinned();
    unsafe {
        task.header.set_runtime_info(
            shared.base() as *const veloq_runtime::runtime::shared::RuntimeSharedBase,
            worker_id,
        );
    }

    let ptr = Box::into_raw(task);
    let task_ref = unsafe { veloq_runtime::task::SendTaskRef::from_concrete(ptr) };

    if !shared.enqueue_pinned(worker_id, task_ref) {
        unsafe {
            let _ = Box::from_raw(ptr);
        }
    }
}
