use std::{cell::RefCell, io, num::NonZeroUsize, ptr::NonNull, sync::mpsc};

use diagweave::prelude::*;
use veloq_buf::{
    AnyBufPool, BufPool, BufResult, BufferRegion, BufferRegistrar, FixedBuf,
    heap::{ChunkId, ChunkInfo},
};
use veloq_driver_native::{
    driver::{ContextDriverProvider, DriveMode, Driver, PlatformDriver, RuntimeContextDriver},
    error::{DriverReport, Error as DriverError},
    op::{DetachedSubmitter, DriverProvider, IntoPlatformOp, IoFd, Op, OpSubmitter},
};
use veloq_runtime::{
    error::{Result as RuntimeResult, RuntimeError},
    runtime::{
        EnqueuePinnedOutcome, IdleDecision, IdleWaitStrategy, IntoRuntimeCtx, RuntimeCtx,
        RuntimeShared,
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

pub struct WorkerState<'reg> {
    pub driver: RefCell<PlatformDriver<'reg>>,
    pub buf_pool: AnyBufPool,
    pub registrar_state: RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

#[derive(Clone)]
pub struct DriverRegistrar<'rt, 'reg> {
    shared: &'rt RuntimeShared<WorkerState<'reg>>,
}

impl<'rt, 'reg> DriverRegistrar<'rt, 'reg> {
    pub(crate) fn new(shared: &'rt RuntimeShared<WorkerState<'reg>>) -> Self {
        Self { shared }
    }

    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'reg>) -> R) -> R {
        self.shared
            .extra_tls
            .try_with(|extra| f(extra))
            .expect("Ctx accessed outside of a worker thread")
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

impl<'rt, 'reg> BufferRegistrar for DriverRegistrar<'rt, 'reg> {
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

#[repr(transparent)]
pub struct SharedRegistrar<'reg> {
    _shared: RuntimeShared<WorkerState<'reg>>,
}

impl<'reg> SharedRegistrar<'reg> {
    /// # Safety
    /// The memory layout of `SharedRegistrar` is identical to `RuntimeShared<WorkerState<'reg>>`.
    #[inline]
    pub unsafe fn from_shared<'rt>(shared: &'rt RuntimeShared<WorkerState<'reg>>) -> &'rt Self {
        unsafe { &*(shared as *const RuntimeShared<WorkerState<'reg>> as *const Self) }
    }
}

impl<'reg> BufferRegistrar for SharedRegistrar<'reg> {
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        let shared = unsafe { &*(self as *const Self as *const RuntimeShared<WorkerState<'reg>>) };
        shared
            .extra_tls
            .try_with(|extra| register_internal(&extra.driver, &extra.registrar_state, regions))
            .expect("Ctx accessed outside of a worker thread")
    }

    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        let shared = unsafe { &*(self as *const Self as *const RuntimeShared<WorkerState<'reg>>) };
        shared
            .extra_tls
            .try_with(|extra| {
                resolve_chunk_info_internal(
                    &extra.driver,
                    &extra.registrar_state,
                    extra.registration_mode,
                    chunk_id,
                )
            })
            .expect("Ctx accessed outside of a worker thread")
    }
}

pub(crate) struct BorrowedRegistrar<'rt, 'reg> {
    pub driver: &'rt RefCell<PlatformDriver<'reg>>,
    pub state: &'rt RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

impl<'rt, 'reg> BufferRegistrar for BorrowedRegistrar<'rt, 'reg> {
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
pub struct Ctx<'rt, 'reg>
where
    'reg: 'rt,
{
    pub runtime_ctx: RuntimeCtx<'rt, WorkerState<'reg>>,
}

impl<'rt, 'reg> IntoRuntimeCtx<'rt, WorkerState<'reg>> for Ctx<'rt, 'reg> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, WorkerState<'reg>> {
        self.runtime_ctx
    }
}

impl<'rt, 'reg> IntoRuntimeCtx<'rt, WorkerState<'reg>> for &Ctx<'rt, 'reg> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, WorkerState<'reg>> {
        self.runtime_ctx
    }
}

impl<'rt, 'reg> ContextDriverProvider<PlatformDriver<'reg>> for Ctx<'rt, 'reg> {
    #[inline]
    fn with_driver_mut<R>(&self, f: impl FnOnce(&mut PlatformDriver<'reg>) -> R) -> R {
        self.extra(|extra| f(&mut extra.driver.borrow_mut()))
    }

    #[inline]
    fn with_driver_ref<R>(&self, f: impl FnOnce(&PlatformDriver<'reg>) -> R) -> R {
        self.extra(|extra| f(&extra.driver.borrow()))
    }
}

impl<'rt, 'reg> DriverProvider for Ctx<'rt, 'reg> {
    type SlotSpec = <PlatformDriver<'reg> as Driver>::SlotSpec;
    type Driver<'d>
        = RuntimeContextDriver<'d, PlatformDriver<'reg>, Ctx<'rt, 'reg>>
    where
        Self: 'd;

    #[inline]
    fn with_driver<'d, R>(&'d self, f: impl FnOnce(Self::Driver<'d>) -> R) -> R {
        f(RuntimeContextDriver::new(self))
    }
}

impl<'rt, 'reg> Ctx<'rt, 'reg> {
    #[inline]
    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'reg>) -> R) -> R {
        self.runtime_ctx
            .shared()
            .extra_tls
            .try_with(|extra| f(extra))
            .expect("Ctx accessed outside of a worker thread")
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.extra(|extra| extra.buf_pool.clone())
    }

    #[inline]
    pub fn registrar(&self) -> DriverRegistrar<'rt, 'reg> {
        DriverRegistrar::new(self.runtime_ctx.shared())
    }
    #[inline]
    pub fn select_poll_start(&self, branches: u32) -> u32 {
        self.runtime_ctx.select_poll_start(branches)
    }

    pub fn driver<'d, R>(
        &'d self,
        f: impl FnOnce(RuntimeContextDriver<'d, PlatformDriver<'reg>, Ctx<'rt, 'reg>>) -> R,
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

    pub fn drive_wait(&self) -> VeloqResult<IdleDecision> {
        self.sync_registrar();
        self.driver(|mut driver| {
            let outcome = driver
                .drive(DriveMode::Wait)
                .push_ctx("scope", "Ctx::drive_wait")
                .attach_note("driver drive(Wait) failed")
                .trans()?;
            if !outcome.pending_progress {
                return Ok(IdleDecision::wait(IdleWaitStrategy::block()));
            }
            Ok(match outcome.next_timeout_hint {
                Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
                None => IdleDecision::wait(IdleWaitStrategy::block()),
            })
        })
    }

    pub fn submit<'d, S, T>(&self, submitter: &'d S, op: Op<T>) -> S::Future<T>
    where
        S: OpSubmitter<'reg, Ctx<'rt, 'reg>> + Copy + 'd,
        T: IntoPlatformOp<<PlatformDriver<'reg> as Driver>::SlotSpec> + Send,
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
            <T as IntoPlatformOp<<PlatformDriver<'reg> as Driver>::SlotSpec>>::Completion,
            DriverReport<DriverError>,
        >,
        T::Output,
    )>
    where
        T: IntoPlatformOp<<PlatformDriver<'reg> as Driver>::SlotSpec> + Send + 'd + 'reg,
    {
        if self.runtime_ctx.worker_id() == worker_id {
            let (res, op_back) = self
                .submit(&DetachedSubmitter::new(), op)
                .await
                .into_inner();
            let op = op_back.expect("Op lost in local submit");
            Ok((res, op))
        } else {
            let runtime_ctx_clone = self.runtime_ctx;
            let routed = self
                .runtime_ctx
                .route_to(worker_id, move || {
                    let ctx = Ctx {
                        runtime_ctx: runtime_ctx_clone,
                    };
                    ctx.driver(|mut driver| op.submit_detached(&mut driver))
                })
                .trans()?;
            let (res, op_back) = routed.await.trans()?.into_inner();
            let op = op_back.expect("Op lost in remote submit");
            Ok((res, op))
        }
    }
}

pub fn poll_current_driver<'reg>(
    shared: &RuntimeShared<WorkerState<'reg>>,
) -> RuntimeResult<IdleDecision> {
    shared
        .extra_tls
        .try_with(|extra| {
            // sync registrar
            sync_to_driver_internal(
                &extra.driver,
                &extra.registrar_state,
                extra.registration_mode,
            );

            let mut driver = extra.driver.borrow_mut();

            let outcome = driver.drive(DriveMode::Poll).map_err(|err| {
                RuntimeError::InvariantViolation {
                    site: "poll_current_driver",
                    detail: "driver drive(Poll) failed",
                }
                .to_report()
                .with_diag_src_err(err)
            })?;
            Ok(match outcome.next_timeout_hint {
                Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
                None if outcome.pending_progress => IdleDecision::continue_now(),
                None => IdleDecision::wait(IdleWaitStrategy::block()),
            })
        })
        .ok_or_else(|| {
            RuntimeError::TlsSetOwnedFailed {
                worker_id: shared.worker_id(),
                source: veloq_tls::TlsError::AllocationFailed,
            }
            .to_report()
        })?
}

pub(crate) fn submit_control_task<'rt, 'reg>(
    shared: &'rt RuntimeShared<WorkerState<'reg>>,
    worker_id: usize,
    fd: IoFd,
) {
    struct UnregisterFileTask<'reg> {
        header: TaskHeader,
        fd: IoFd,
        shared_ptr: *const RuntimeShared<WorkerState<'reg>>,
    }

    unsafe impl<'reg> Send for UnregisterFileTask<'reg> {}
    unsafe impl<'reg> Sync for UnregisterFileTask<'reg> {}

    impl<'reg> RawTask for UnregisterFileTask<'reg> {
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

    impl<'reg> UnregisterFileTask<'reg> {
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
            UnregisterFileTask::<'reg>::VTABLE,
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
