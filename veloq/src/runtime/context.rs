use std::{cell::RefCell, io, num::NonZeroUsize, ptr::NonNull, sync::mpsc};

use diagweave::prelude::*;
use veloq_buf::{
    AnyBufPool, BufPool, BufResult, BufferRegion, BufferRegistrar, FixedBuf,
    heap::{ChunkId, ChunkInfo},
};
use veloq_driver_native::{
    driver::{
        ContextDriverProvider, DriveMode, Driver, PlatformDriver, PlatformOp, PlatformUP,
        RuntimeContextDriver,
    },
    error::{DriverReport, Error as DriverError},
    op::{DetachedSubmitter, DriverProvider, IntoPlatformOp, IoFd, Op, OpSubmitter},
};
use veloq_runtime::{
    error::Result as RuntimeResult,
    runtime::{
        EnqueuePinnedOutcome, IdleDecision, IdleWaitStrategy, RuntimeScopeContext, RuntimeShared,
    },
    storage::AtomicStorage,
    task::{
        GenericTaskHeader, RawTask, ScopeRef, SendTaskRef, TaskHandleRef, TaskHeader, TaskVTable,
        yield_now,
    },
};

use crate::{config::BufferRegistrationMode, error::Result as VeloqResult};

/// 驱动注册中心的消息类型
#[derive(Debug, Clone)]
pub enum RegistrarMessage {
    /// 发现了新的内存块，需要通知驱动注册
    NewChunk(ChunkInfo),
}

pub struct WorkerRegistrarState {
    /// 接收来自分发器的广播消息
    pub receiver: mpsc::Receiver<RegistrarMessage>,
    /// 本地已知的内存块快照
    pub chunks: Vec<ChunkInfo>,
}

pub struct WorkerState<'ctx> {
    pub driver: RefCell<PlatformDriver<'ctx>>,
    pub buf_pool: AnyBufPool,
    pub registrar_state: RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

#[derive(Clone)]
pub struct DriverRegistrar<'a, 'ctx> {
    shared: &'a RuntimeShared<WorkerState<'ctx>>,
}

impl<'a, 'ctx> DriverRegistrar<'a, 'ctx> {
    pub(crate) fn new(shared: &'a RuntimeShared<WorkerState<'ctx>>) -> Self {
        Self { shared }
    }

    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'ctx>) -> R) -> R {
        self.shared
            .extra_tls
            .try_with(|extra| f(extra))
            .expect("RuntimeContext accessed outside of a worker thread")
    }

    pub fn sync_to_driver(&self) {
        self.extra(|extra| {
            sync_to_driver_internal(
                &extra.driver,
                &extra.registrar_state,
                extra.registration_mode,
            );
        })
    }
}

impl<'a, 'ctx> BufferRegistrar for DriverRegistrar<'a, 'ctx> {
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        self.extra(|extra| register_internal(&extra.driver, &extra.registrar_state, regions))
    }

    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        self.extra(|extra| {
            resolve_chunk_info_internal(
                &extra.driver,
                &extra.registrar_state,
                extra.registration_mode,
                chunk_id,
            )
        })
    }
}

pub(crate) struct BorrowedRegistrar<'a, 'ctx> {
    pub driver: &'a RefCell<PlatformDriver<'ctx>>,
    pub state: &'a RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

impl<'a, 'ctx> BufferRegistrar for BorrowedRegistrar<'a, 'ctx> {
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        register_internal(self.driver, self.state, regions)
    }

    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        resolve_chunk_info_internal(self.driver, self.state, self.registration_mode, chunk_id)
    }
}

fn register_internal(
    driver: &RefCell<PlatformDriver<'_>>,
    state: &RefCell<WorkerRegistrarState>,
    regions: &[BufferRegion],
) -> BufResult<Vec<ChunkId>> {
    let mut indices = Vec::with_capacity(regions.len());
    let mut new_chunks = Vec::with_capacity(regions.len());

    {
        let mut driver = driver.borrow_mut();
        for region in regions {
            let chunk_id = region.id();
            driver
                .register_chunk(chunk_id, region.as_ptr(), region.len())
                .map_err(|err| io::Error::other(format!("{err:#}")))
                .trans()?;

            new_chunks.push(ChunkInfo {
                id: chunk_id,
                ptr: unsafe { NonNull::new_unchecked(region.as_ptr() as *mut u8) },
                len: unsafe { NonZeroUsize::new_unchecked(region.len()) },
            });
            indices.push(chunk_id);
        }
    }

    let mut state = state.borrow_mut();
    state.chunks.extend(new_chunks);

    Ok(indices)
}

fn resolve_chunk_info_internal(
    driver: &RefCell<PlatformDriver<'_>>,
    state: &RefCell<WorkerRegistrarState>,
    registration_mode: BufferRegistrationMode,
    chunk_id: ChunkId,
) -> Option<ChunkInfo> {
    // 首先在本地快照中查找
    let found = {
        let state = state.borrow();
        state.chunks.iter().find(|c| c.id == chunk_id).copied()
    };

    if let Some(chunk) = found {
        return Some(chunk);
    }

    // 如果没找到，尝试同步一次消息队列后再查找
    sync_to_driver_internal(driver, state, registration_mode);

    let state = state.borrow();
    state.chunks.iter().find(|c| c.id == chunk_id).copied()
}

fn sync_to_driver_internal(
    driver: &RefCell<PlatformDriver<'_>>,
    state: &RefCell<WorkerRegistrarState>,
    registration_mode: BufferRegistrationMode,
) {
    let mut driver = driver.borrow_mut();
    let mut state = state.borrow_mut();

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

    if matches!(registration_mode, BufferRegistrationMode::Compatible) {
        for chunk in &new_chunks {
            let _ = driver.register_chunk(chunk.id, chunk.ptr.as_ptr(), chunk.len.get());
        }
    }

    // 更新本地快照
    state.chunks.extend(new_chunks);
}

#[derive(Clone, Copy)]
pub struct RuntimeContext<'a, 'ctx>
where
    'ctx: 'a,
{
    pub scope: RuntimeScopeContext<'a, WorkerState<'ctx>>,
}

impl<'a, 'ctx> ContextDriverProvider<PlatformDriver<'ctx>> for RuntimeContext<'a, 'ctx> {
    #[inline]
    fn with_driver_mut<R>(&self, f: impl FnOnce(&mut PlatformDriver<'ctx>) -> R) -> R {
        self.extra(|extra| f(&mut extra.driver.borrow_mut()))
    }

    #[inline]
    fn with_driver_ref<R>(&self, f: impl FnOnce(&PlatformDriver<'ctx>) -> R) -> R {
        self.extra(|extra| f(&extra.driver.borrow()))
    }
}

impl<'a, 'ctx> DriverProvider for RuntimeContext<'a, 'ctx> {
    type Op = PlatformOp;
    type UP = PlatformUP;
    type Completion = usize;
    type Error = <PlatformDriver<'ctx> as Driver>::Error;
    type SlotSpec = <PlatformDriver<'ctx> as Driver>::SlotSpec;
    type Driver<'d>
        = RuntimeContextDriver<'d, PlatformDriver<'ctx>, RuntimeContext<'a, 'ctx>>
    where
        Self: 'd;

    #[inline]
    fn with_driver<'d, R>(&'d self, f: impl FnOnce(Self::Driver<'d>) -> R) -> R {
        f(RuntimeContextDriver::new(self))
    }
}

impl<'a, 'ctx> RuntimeContext<'a, 'ctx> {
    #[inline]
    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'ctx>) -> R) -> R {
        self.scope
            .shared()
            .extra_tls
            .try_with(|extra| f(extra))
            .expect("RuntimeContext accessed outside of a worker thread")
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.extra(|extra| extra.buf_pool.clone())
    }

    #[inline]
    pub fn registrar(&self) -> DriverRegistrar<'a, 'ctx> {
        DriverRegistrar::new(self.scope.shared())
    }
    #[inline]
    pub fn select_poll_start(&self, branches: u32) -> u32 {
        self.scope.select_poll_start(branches)
    }

    pub fn driver<'d, R>(
        &'d self,
        f: impl FnOnce(RuntimeContextDriver<'d, PlatformDriver<'ctx>, RuntimeContext<'a, 'ctx>>) -> R,
    ) -> R {
        f(RuntimeContextDriver::new(self))
    }

    #[inline]
    pub fn sync_registrar(&self) {
        self.registrar().sync_to_driver();
    }

    pub fn try_alloc_from_pool(&self, size: NonZeroUsize) -> Option<FixedBuf> {
        self.buf_pool().alloc(size)
    }

    pub fn try_alloc(&self, size: NonZeroUsize) -> BufResult<FixedBuf> {
        self.try_alloc_from_pool(size)
            .map_or_else(|| FixedBuf::alloc_heap(size), Ok)
    }

    pub fn alloc(&self, size: NonZeroUsize) -> FixedBuf {
        self.try_alloc(size).expect("failed to allocate buffer")
    }

    /// 让当前线程的驱动在空闲时进入等待推进。
    ///
    /// 这个入口会优先利用驱动后端的阻塞等待能力，避免固定轮询兜底。
    pub fn drive_wait(&self) -> IdleDecision {
        self.sync_registrar();
        self.driver(|mut driver| {
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
        })
    }

    pub fn submit<'d, S, T>(&self, submitter: &'d S, op: Op<T>) -> S::Future<T>
    where
        S: OpSubmitter<'ctx, RuntimeContext<'a, 'ctx>> + Copy + 'd,
        T: IntoPlatformOp<
                <PlatformDriver<'ctx> as Driver>::Op,
                DriverCompletion = <PlatformDriver<'ctx> as Driver>::Completion,
                ErasedPayload = <PlatformDriver<'ctx> as Driver>::UP,
                Error = <PlatformDriver<'ctx> as Driver>::Error,
            > + Send,
    {
        self.sync_registrar();
        submitter.submit(op, *self)
    }

    pub async fn yield_now(&self) {
        self.sync_registrar();
        yield_now().await;
    }

    pub async fn submit_to<'d, T>(
        &self,
        worker_id: usize,
        op: Op<T>,
    ) -> VeloqResult<(
        Result<
            <T as IntoPlatformOp<<PlatformDriver<'ctx> as Driver>::Op>>::Completion,
            DriverReport<DriverError>,
        >,
        T::Output,
    )>
    where
        T: IntoPlatformOp<
                <PlatformDriver<'ctx> as Driver>::Op,
                DriverCompletion = <PlatformDriver<'ctx> as Driver>::Completion,
                ErasedPayload = <PlatformDriver<'ctx> as Driver>::UP,
                Error = <PlatformDriver<'ctx> as Driver>::Error,
            > + Send
            + 'd + 'ctx,
    {
        if self.scope.worker_id() == worker_id {
            let (res, op_back) = self
                .submit(&DetachedSubmitter::new(), op)
                .await
                .into_inner();
            let op = op_back.expect("Op lost in local submit");
            Ok((res, op))
        } else {
            let scope_clone = self.scope;
            let routed = self
                .scope
                .route_to(worker_id, move || {
                    let ctx = RuntimeContext { scope: scope_clone };
                    ctx.driver(|mut driver| op.submit_detached(&mut driver))
                })
                .trans()?;
            let (res, op_back) = routed.await.trans()?.into_inner();
            let op = op_back.expect("Op lost in remote submit");
            Ok((res, op))
        }
    }
}

pub fn poll_current_driver<'ctx>(shared: &RuntimeShared<WorkerState<'ctx>>) -> IdleDecision {
    shared.extra_tls.with(|extra| {
        // sync registrar
        sync_to_driver_internal(
            &extra.driver,
            &extra.registrar_state,
            extra.registration_mode,
        );

        let mut driver = extra.driver.borrow_mut();

        let outcome = driver
            .drive(DriveMode::Poll)
            .unwrap_or_else(|err| panic!("driver drive(Poll) failed: {err:#}"));
        match outcome.next_timeout_hint {
            Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
            None if outcome.pending_progress => IdleDecision::continue_now(),
            None => IdleDecision::wait(IdleWaitStrategy::block()),
        }
    })
}

pub(crate) fn submit_control_task<'a, 'ctx>(
    shared: &'a RuntimeShared<WorkerState<'ctx>>,
    worker_id: usize,
    fd: IoFd,
) {
    struct UnregisterFileTask<'ctx> {
        header: TaskHeader,
        fd: IoFd,
        shared_ptr: *const RuntimeShared<WorkerState<'ctx>>,
    }

    unsafe impl<'ctx> Send for UnregisterFileTask<'ctx> {}
    unsafe impl<'ctx> Sync for UnregisterFileTask<'ctx> {}

    impl<'ctx> RawTask for UnregisterFileTask<'ctx> {
        type Storage = AtomicStorage;

        fn poll_raw(&self, _worker_id: usize) -> RuntimeResult<bool> {
            let shared = unsafe { &*self.shared_ptr };
            let _ = shared.extra_tls.try_with(|extra| {
                let mut driver = extra.driver.borrow_mut();
                let _ = driver.unregister_files(vec![self.fd]);
            });
            self.header.mark_completed_and_notify();
            unsafe {
                let header_ptr = NonNull::from(&self.header);
                GenericTaskHeader::drop_task(header_ptr);
            }
            Ok(true)
        }

        fn header(&self) -> &GenericTaskHeader<Self::Storage> {
            &self.header
        }
    }

    impl<'ctx> UnregisterFileTask<'ctx> {
        const VTABLE: &'static TaskVTable<AtomicStorage> = &TaskVTable {
            wake: |_| {},
            wake_by_ref: |_| {},
            poll: |header, worker_id| unsafe {
                let node = &*(header as *const GenericTaskHeader<AtomicStorage> as *const Self);
                RawTask::poll_raw(node, worker_id)
            },
            drop: |data| unsafe {
                let ptr = data.as_ptr() as *mut Self;
                let _ = Box::from_raw(ptr);
            },
        };
    }

    let task = Box::new(UnregisterFileTask {
        header: TaskHeader::new(
            UnregisterFileTask::<'ctx>::VTABLE,
            &shared.base,
            worker_id,
            ScopeRef::<AtomicStorage>::dummy(),
        ),
        fd,
        shared_ptr: shared as *const _,
    });

    task.header.set_pinned();

    let ptr = Box::into_raw(task);
    let task_ref = unsafe { SendTaskRef::from_concrete(ptr) };
    match shared.enqueue_pinned(worker_id, task_ref) {
        EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => {}
        EnqueuePinnedOutcome::AbortedAcknowledged
        | EnqueuePinnedOutcome::AlreadySettled
        | EnqueuePinnedOutcome::NeedsCallerSettle => unsafe {
            let _ = Box::from_raw(ptr);
        },
    }
}
