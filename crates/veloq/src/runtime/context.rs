use std::{cell::RefCell, num::NonZeroUsize, ptr::NonNull, sync::mpsc};

use diagweave::prelude::*;
use veloq_buf::{
    AnyBufPool, BufError, BufPool, BufResult, BufferRegion, BufferRegistrar, FixedBuf,
    heap::{ChunkId, ChunkInfo},
};
use veloq_driver_native::{
    driver::{
        ContextDriverProvider, DriveMode, DriveOutcome, Driver, DriverRaw, PlatformDriver,
        RuntimeContextDriver,
    },
    error::{DriverReport, Error as DriverError},
    op::{DetachedSubmitter, DriverProvider, IntoPlatformOp, IoFd, Op, OpSubmitter, SingleShotOp},
};
use veloq_runtime::{
    error::{Result as RuntimeResult, RuntimeDriverError, RuntimeError},
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

pub struct WorkerState<'rt> {
    pub driver: RefCell<PlatformDriver<'rt>>,
    pub buf_pool: AnyBufPool,
    pub registrar_state: RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

#[derive(Clone)]
pub struct DriverRegistrar<'rt> {
    shared: &'rt RuntimeShared<WorkerState<'rt>>,
}

impl<'rt> DriverRegistrar<'rt> {
    pub(crate) fn new(shared: &'rt RuntimeShared<WorkerState<'rt>>) -> Self {
        Self { shared }
    }

    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'rt>) -> R) -> R {
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

impl<'rt> BufferRegistrar for DriverRegistrar<'rt> {
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
pub struct SharedRegistrar<'rt> {
    _shared: RuntimeShared<WorkerState<'rt>>,
}

impl<'rt> SharedRegistrar<'rt> {
    /// # Safety
    /// The memory layout of `SharedRegistrar` is identical to `RuntimeShared<WorkerState<'rt>>`.
    #[inline]
    pub unsafe fn from_shared(shared: &'rt RuntimeShared<WorkerState<'rt>>) -> &'rt Self {
        unsafe { &*(shared as *const RuntimeShared<WorkerState<'rt>> as *const Self) }
    }
}

impl<'rt> BufferRegistrar for SharedRegistrar<'rt> {
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        let shared = unsafe { &*(self as *const Self as *const RuntimeShared<WorkerState<'rt>>) };
        shared
            .extra_tls
            .try_with(|extra| register_internal(&extra.driver, &extra.registrar_state, regions))
            .expect("Ctx accessed outside of a worker thread")
    }

    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        let shared = unsafe { &*(self as *const Self as *const RuntimeShared<WorkerState<'rt>>) };
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

pub(crate) struct BorrowedRegistrar<'a, 'rt> {
    pub driver: &'a RefCell<PlatformDriver<'rt>>,
    pub state: &'a RefCell<WorkerRegistrarState>,
    pub registration_mode: BufferRegistrationMode,
}

impl<'a, 'rt> BufferRegistrar for BorrowedRegistrar<'a, 'rt> {
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
                .map_err(|err| BufError::Other(format!("{err:#}")))?;

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
pub struct Ctx<'rt> {
    pub runtime_ctx: RuntimeCtx<'rt, WorkerState<'rt>>,
}

impl<'rt> IntoRuntimeCtx<'rt, WorkerState<'rt>> for Ctx<'rt> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, WorkerState<'rt>> {
        self.runtime_ctx
    }
}

impl<'rt> IntoRuntimeCtx<'rt, WorkerState<'rt>> for &Ctx<'rt> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, WorkerState<'rt>> {
        self.runtime_ctx
    }
}

impl<'rt> ContextDriverProvider<PlatformDriver<'rt>> for Ctx<'rt> {
    #[inline]
    fn with_driver_mut<R>(&self, f: impl FnOnce(&mut PlatformDriver<'rt>) -> R) -> R {
        self.extra(|extra| f(&mut extra.driver.borrow_mut()))
    }

    #[inline]
    fn with_driver_ref<R>(&self, f: impl FnOnce(&PlatformDriver<'rt>) -> R) -> R {
        self.extra(|extra| f(&extra.driver.borrow()))
    }
}

impl<'rt> DriverProvider for Ctx<'rt> {
    type SlotSpec = <PlatformDriver<'rt> as DriverRaw>::SlotSpec;
    type Driver<'d>
        = RuntimeContextDriver<'d, PlatformDriver<'rt>, Ctx<'rt>>
    where
        Self: 'd;

    #[inline]
    fn with_driver<'d, R>(&'d self, f: impl FnOnce(Self::Driver<'d>) -> R) -> R {
        f(RuntimeContextDriver::new(self))
    }
}

impl<'rt> Ctx<'rt> {
    #[inline]
    fn extra<R>(&self, f: impl FnOnce(&WorkerState<'rt>) -> R) -> R {
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
    pub fn registrar(&self) -> DriverRegistrar<'rt> {
        DriverRegistrar::new(self.runtime_ctx.shared())
    }

    #[inline]
    pub fn select_poll_start(&self, branches: u32) -> u32 {
        self.runtime_ctx.select_poll_start(branches)
    }

    pub fn driver<'d, R>(
        &'d self,
        f: impl FnOnce(RuntimeContextDriver<'d, PlatformDriver<'rt>, Ctx<'rt>>) -> R,
    ) -> R {
        f(RuntimeContextDriver::new(self))
    }

    #[inline]
    pub fn sync_registrar(&self) {
        self.registrar().sync_to_driver();
    }

    pub fn try_alloc_from_pool(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf> {
        self.buf_pool().alloc(cap, len)
    }

    pub fn try_alloc_from_pool_full(&self, cap: NonZeroUsize) -> Option<FixedBuf> {
        self.try_alloc_from_pool(cap, cap.get())
    }

    pub fn try_alloc(&self, cap: NonZeroUsize, len: usize) -> BufResult<FixedBuf> {
        self.try_alloc_from_pool(cap, len)
            .map_or_else(|| FixedBuf::alloc_heap(cap, len), Ok)
    }

    pub fn try_alloc_full(&self, cap: NonZeroUsize) -> BufResult<FixedBuf> {
        self.try_alloc(cap, cap.get())
    }

    pub fn alloc(&self, cap: NonZeroUsize, len: usize) -> FixedBuf {
        self.try_alloc(cap, len).expect("failed to allocate buffer")
    }

    pub fn alloc_full(&self, cap: NonZeroUsize) -> FixedBuf {
        self.alloc(cap, cap.get())
    }

    pub fn drive_wait(&self) -> VeloqResult<IdleDecision> {
        self.sync_registrar();
        self.driver(|mut driver| {
            let outcome = driver
                .drive(DriveMode::Wait { timeout: None })
                .push_ctx("scope", "Ctx::drive_wait")
                .attach_note("driver drive(Wait) failed")
                .trans()?;
            Ok(idle_decision_from_outcome(outcome))
        })
    }

    pub fn submit<'d, S, T>(&self, submitter: &'d S, op: Op<T>) -> S::Future<T>
    where
        S: OpSubmitter<'rt, Ctx<'rt>> + Copy + 'd,
        T: SingleShotOp<<PlatformDriver<'rt> as DriverRaw>::SlotSpec> + Send,
    {
        self.sync_registrar();
        submitter.submit(op, *self)
    }

    /// 提交一个操作并把它当完成流用。
    ///
    /// 单发操作在这里是只有一项的流；multishot 操作（`AcceptMulti`）只能走这条路。
    /// 与 [`Self::submit`] 的区别只在「怎么看这个句柄」，提交路径完全相同。
    pub fn submit_stream<'d, S, T>(&self, submitter: &'d S, op: Op<T>) -> S::Stream<T>
    where
        S: OpSubmitter<'rt, Ctx<'rt>> + Copy + 'd,
        T: IntoPlatformOp<<PlatformDriver<'rt> as DriverRaw>::SlotSpec> + Send,
    {
        self.sync_registrar();
        submitter.submit_stream(op, *self)
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
            <T as IntoPlatformOp<<PlatformDriver<'rt> as DriverRaw>::SlotSpec>>::Completion,
            DriverReport<DriverError>,
        >,
        T::Output,
    )>
    where
        T: SingleShotOp<<PlatformDriver<'rt> as DriverRaw>::SlotSpec> + Send + 'd + 'rt,
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

pub fn poll_current_driver<'rt>(
    shared: &RuntimeShared<WorkerState<'rt>>,
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

            let outcome = driver
                .drive(DriveMode::Poll)
                .map_err(|err| driver_failure(shared, "poll", "drive", err))?;
            Ok(idle_decision_from_outcome(outcome))
        })
        .map_err(|err| {
            RuntimeError::TlsSetOwnedFailed {
                worker_id: shared.worker_id(),
                source: err,
            }
            .to_report()
        })?
}

pub(crate) fn submit_control_task<'rt>(
    shared: &'rt RuntimeShared<WorkerState<'rt>>,
    worker_id: usize,
    fd: IoFd,
) {
    /// `repr(C)` is load-bearing: the vtable's `poll` casts the header pointer straight to
    /// `*const Self`, which is only sound while `header` sits at offset 0. Under `repr(Rust)`
    /// the compiler is free to reorder the fields — and does, depending on the size and
    /// alignment of `fd`. Same reason `GenericTaskNode` and `RouteJobTask` carry it.
    #[repr(C)]
    struct UnregisterFileTask<'rt> {
        header: TaskHeader,
        fd: IoFd,
        shared_ptr: *const RuntimeShared<WorkerState<'rt>>,
    }

    unsafe impl<'rt> Send for UnregisterFileTask<'rt> {}
    unsafe impl<'rt> Sync for UnregisterFileTask<'rt> {}

    impl<'rt> RawTask for UnregisterFileTask<'rt> {
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

    impl<'rt> UnregisterFileTask<'rt> {
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
            UnregisterFileTask::<'rt>::VTABLE,
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
        EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => {
            if shared.base.unparkers()[worker_id].unpark().is_err() {
                // Unparker 已将 backend 错误写入 runtime fatal 通道。
            }
        }
        EnqueuePinnedOutcome::AbortedAcknowledged
        | EnqueuePinnedOutcome::AlreadySettled
        | EnqueuePinnedOutcome::NeedsCallerSettle => unsafe {
            let _ = Box::from_raw(ptr);
        },
    }
}

pub fn park_current_driver<'rt>(
    shared: &RuntimeShared<WorkerState<'rt>>,
    wait_strategy: IdleWaitStrategy,
) -> RuntimeResult<()> {
    let res = shared.extra_tls.try_with(|extra| {
        // sync registrar
        sync_to_driver_internal(
            &extra.driver,
            &extra.registrar_state,
            extra.registration_mode,
        );

        let mut driver = extra.driver.borrow_mut();

        // Block on the OS event driver
        driver
            .drive(drive_mode_for_wait_strategy(wait_strategy))
            .map_err(|err| driver_failure(shared, "wait", "drive", err))
    });

    match res {
        Ok(Ok(_outcome)) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(RuntimeError::TlsSetOwnedFailed {
            worker_id: shared.worker_id(),
            source: err,
        }
        .to_report()),
    }
}

fn idle_decision_from_outcome(outcome: DriveOutcome) -> IdleDecision {
    if outcome.ready_completion {
        return IdleDecision::continue_now();
    }

    match outcome.next_timeout_hint {
        Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
        None => IdleDecision::wait(IdleWaitStrategy::block()),
    }
}

fn drive_mode_for_wait_strategy(wait_strategy: IdleWaitStrategy) -> DriveMode {
    DriveMode::Wait {
        timeout: wait_strategy.into_timeout(),
    }
}

fn driver_failure<E>(
    shared: &RuntimeShared<WorkerState<'_>>,
    phase: &'static str,
    operation: &'static str,
    error: E,
) -> diagweave::Report<RuntimeError>
where
    E: std::fmt::Display,
{
    RuntimeError::DriverFailed {
        source: RuntimeDriverError {
            backend: if cfg!(target_os = "linux") {
                "io_uring"
            } else if cfg!(target_os = "windows") {
                "iocp"
            } else {
                "unknown"
            },
            worker_id: shared.worker_id(),
            phase,
            operation,
            detail: format!("{error:#}"),
        },
    }
    .to_report()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn outcome(
        next_timeout_hint: Option<Duration>,
        ready_completion: bool,
        in_flight: bool,
    ) -> DriveOutcome {
        DriveOutcome {
            next_timeout_hint,
            ready_completion,
            in_flight,
        }
    }

    #[test]
    fn ready_completion_wins_over_timer_and_in_flight_state() {
        assert_eq!(
            idle_decision_from_outcome(outcome(Some(Duration::from_secs(1)), true, true,)),
            IdleDecision::Continue
        );
    }

    #[test]
    fn no_ready_completion_with_timer_waits_for_timeout() {
        assert_eq!(
            idle_decision_from_outcome(outcome(Some(Duration::from_millis(5)), false, false,)),
            IdleDecision::Wait(IdleWaitStrategy::Timeout(Duration::from_millis(5)))
        );
    }

    #[test]
    fn active_io_without_ready_completion_waits_indefinitely() {
        assert_eq!(
            idle_decision_from_outcome(outcome(None, false, true)),
            IdleDecision::Wait(IdleWaitStrategy::Block)
        );
    }

    #[test]
    fn wait_strategy_is_forwarded_as_external_timeout() {
        assert_eq!(
            drive_mode_for_wait_strategy(IdleWaitStrategy::Block),
            DriveMode::Wait { timeout: None }
        );
        assert_eq!(
            drive_mode_for_wait_strategy(IdleWaitStrategy::Timeout(Duration::from_millis(7))),
            DriveMode::Wait {
                timeout: Some(Duration::from_millis(7))
            }
        );
    }
}
