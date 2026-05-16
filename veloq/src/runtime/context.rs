use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::mpsc;

use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver_native::driver::{DriveMode, Driver, PlatformDriver};
use veloq_driver_native::op::{DetachedSubmitter, IntoPlatformOp, Op};

use crate::config::BufferRegistrationMode;
use crate::error::{Result as VeloqResult, from_io_error};
use veloq_runtime::runtime::{IdleDecision, IdleWaitStrategy, RuntimeScopeContext, RuntimeShared};

/// 驱动注册中心的消息类型
#[derive(Debug, Clone)]
pub enum RegistrarMessage {
    /// 发现了新的内存块，需要通知驱动注册
    NewChunk(veloq_buf::heap::ChunkInfo),
}

pub struct WorkerRegistrarState {
    /// 接收来自分发器的广播消息
    pub receiver: mpsc::Receiver<RegistrarMessage>,
    /// 本地已知的内存块快照
    pub chunks: Vec<veloq_buf::heap::ChunkInfo>,
}

pub struct WorkerState<'ctx> {
    pub driver: RefCell<PlatformDriver<'ctx>>,
    pub buf_pool: AnyBufPool,
    pub registrar: DriverRegistrar<'ctx>,
    pub registrar_state: RefCell<WorkerRegistrarState>,
}

#[derive(Clone)]
pub struct DriverRegistrar<'ctx> {
    shared: &'ctx RuntimeShared<WorkerState<'ctx>>,
    registration_mode: BufferRegistrationMode,
}

impl<'ctx> DriverRegistrar<'ctx> {
    pub(crate) fn new(
        shared: &'ctx RuntimeShared<WorkerState<'ctx>>,
        registration_mode: BufferRegistrationMode,
    ) -> Self {
        Self {
            shared,
            registration_mode,
        }
    }

    fn extra(&self) -> &WorkerState<'ctx> {
        let tls_ptr = self.shared.context_tls.get().expect("Not in runtime");
        unsafe { &tls_ptr.as_ref().extra }
    }

    pub fn sync_to_driver(&self) {
        let extra = self.extra();

        let mut driver = extra.driver.borrow_mut();
        let mut state = extra.registrar_state.borrow_mut();

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
            for chunk in &new_chunks {
                let _ = driver.register_chunk(chunk.id, chunk.ptr.as_ptr(), chunk.len.get());
            }
        }

        // 更新本地快照
        state.chunks.extend(new_chunks);
    }
}

impl<'ctx> veloq_buf::BufferRegistrar for DriverRegistrar<'ctx> {
    fn register(&self, regions: &[veloq_buf::BufferRegion]) -> std::io::Result<Vec<usize>> {
        let extra = self.extra();

        let mut indices = Vec::with_capacity(regions.len());
        let mut new_chunks = Vec::with_capacity(regions.len());

        {
            let mut driver = extra.driver.borrow_mut();
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
        }

        let mut state = extra.registrar_state.borrow_mut();
        state.chunks.extend(new_chunks);

        Ok(indices)
    }

    fn resolve_chunk_info(&self, chunk_id: u16) -> Option<veloq_buf::heap::ChunkInfo> {
        let extra = self.extra();

        // 首先在本地快照中查找
        let found = {
            let state = extra.registrar_state.borrow();
            state.chunks.iter().find(|c| c.id == chunk_id).copied()
        };

        if let Some(chunk) = found {
            return Some(chunk);
        }

        // 如果没找到，尝试同步一次消息队列后再查找
        self.sync_to_driver();

        let state = extra.registrar_state.borrow();
        state.chunks.iter().find(|c| c.id == chunk_id).copied()
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeContext<'ctx> {
    pub scope: RuntimeScopeContext<'ctx, WorkerState<'ctx>>,
}

impl<'ctx> veloq_driver_native::op::DriverProvider for RuntimeContext<'ctx> {
    type Op = veloq_driver_native::driver::PlatformOp;
    type UP = veloq_driver_native::driver::PlatformUP;
    type Completion = usize;
    type Driver<'a>
        = &'a mut veloq_driver_native::driver::PlatformDriver<'ctx>
    where
        Self: 'a;

    #[inline]
    fn with_driver<'a, R>(&'a self, f: impl FnOnce(Self::Driver<'a>) -> R) -> R {
        self.driver(move |driver| f(driver))
    }
}

impl<'ctx> RuntimeContext<'ctx> {
    #[inline]
    fn extra(&self) -> &WorkerState<'ctx> {
        let tls_ptr = self
            .scope
            .shared()
            .context_tls
            .get()
            .expect("Not in runtime");
        unsafe { &tls_ptr.as_ref().extra }
    }

    pub async fn scope<R, F>(&self, f: F) -> R
    where
        F: for<'b, 's, 'm> std::ops::AsyncFnOnce(
                &'b veloq_runtime::scope::GenericAsyncScope<
                    's,
                    veloq_runtime::utils::storage::AtomicStorage,
                    veloq_runtime::utils::ownership::ArcOwnership,
                    WorkerState,
                    &'m (),
                >,
            ) -> R,
    {
        self.scope.scope(f).await
    }

    pub async fn scope_local<R, F>(&self, f: F) -> R
    where
        F: for<'b, 's, 'm> std::ops::AsyncFnOnce(
                &'b veloq_runtime::scope::GenericAsyncScope<
                    's,
                    veloq_runtime::utils::storage::LocalStorage,
                    veloq_runtime::utils::ownership::RcOwnership,
                    WorkerState,
                    *const &'m (),
                >,
            ) -> R,
    {
        self.scope.scope_local(f).await
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.extra().buf_pool.clone()
    }

    #[inline]
    pub fn registrar(&self) -> DriverRegistrar<'ctx> {
        self.extra().registrar.clone()
    }

    pub fn driver<'a, R>(&'a self, f: impl FnOnce(&'a mut PlatformDriver<'ctx>) -> R) -> R {
        let mut driver = self.extra().driver.borrow_mut();
        let driver: &'a mut PlatformDriver<'ctx> =
            unsafe { &mut *(&mut *driver as *mut PlatformDriver<'ctx>) };
        f(driver)
    }

    #[inline]
    pub fn sync_registrar(&self) {
        self.registrar().sync_to_driver();
    }

    pub fn try_alloc_from_pool(&self, size: NonZeroUsize) -> Option<FixedBuf> {
        self.buf_pool().alloc(size)
    }

    pub fn try_alloc(&self, size: NonZeroUsize) -> Result<FixedBuf, veloq_buf::AllocError> {
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
        self.driver(|driver| {
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

    pub fn submit<'a, S, T>(&self, submitter: &'a S, op: Op<T>) -> S::Future<T>
    where
        S: veloq_driver_native::op::OpSubmitter<'ctx, RuntimeContext<'ctx>> + Copy + 'a,
        T: IntoPlatformOp<
                <PlatformDriver<'ctx> as Driver>::Op,
                DriverCompletion = <PlatformDriver<'ctx> as Driver>::Completion,
                ErasedPayload = <PlatformDriver<'ctx> as Driver>::UP,
            > + Send,
    {
        self.sync_registrar();
        submitter.submit(op, *self)
    }

    pub async fn yield_now(&self) {
        self.sync_registrar();
        veloq_runtime::task::yield_now().await;
    }

    pub async fn submit_to<'a, T>(
        &self,
        worker_id: usize,
        op: Op<T>,
    ) -> VeloqResult<(
        Result<
            <T as IntoPlatformOp<<PlatformDriver<'ctx> as Driver>::Op>>::Completion,
            veloq_driver_native::error::DriverErrorReport,
        >,
        T::Output,
    )>
    where
        T: IntoPlatformOp<
                <PlatformDriver<'ctx> as Driver>::Op,
                DriverCompletion = <PlatformDriver<'ctx> as Driver>::Completion,
                ErasedPayload = <PlatformDriver<'ctx> as Driver>::UP,
            > + Send
            + 'a,
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
                    ctx.driver(|driver| op.submit_detached(driver))
                })
                .map_err(from_io_error)?;
            let (res, op_back) = routed.await.into_inner();
            let op = op_back.expect("Op lost in remote submit");
            Ok((res, op))
        }
    }
}

pub fn poll_current_driver<'ctx>(shared: &RuntimeShared<WorkerState<'ctx>>) -> IdleDecision {
    let Some(tls_ptr) = shared.context_tls.get() else {
        return IdleDecision::wait(IdleWaitStrategy::block());
    };
    let extra = unsafe { &tls_ptr.as_ref().extra };

    // sync registrar
    extra.registrar.sync_to_driver();

    let mut driver = extra.driver.borrow_mut();

    let outcome = driver
        .drive(DriveMode::Poll)
        .unwrap_or_else(|err| panic!("driver drive(Poll) failed: {err:#}"));
    match outcome.next_timeout_hint {
        Some(duration) => IdleDecision::wait(IdleWaitStrategy::timeout(duration)),
        None if outcome.pending_progress => IdleDecision::continue_now(),
        None => IdleDecision::wait(IdleWaitStrategy::block()),
    }
}

pub(crate) fn submit_control_task<'ctx>(
    shared: &veloq_runtime::runtime::shared::RuntimeShared<WorkerState<'ctx>>,
    worker_id: usize,
    fd: veloq_driver_native::op::IoFd,
) {
    struct UnregisterFileTask<'ctx> {
        header: veloq_runtime::task::TaskHeader,
        fd: veloq_driver_native::op::IoFd,
        shared_ptr: *const veloq_runtime::runtime::shared::RuntimeShared<WorkerState<'ctx>>,
    }

    unsafe impl<'ctx> Send for UnregisterFileTask<'ctx> {}
    unsafe impl<'ctx> Sync for UnregisterFileTask<'ctx> {}

    impl<'ctx> veloq_runtime::task::RawTask for UnregisterFileTask<'ctx> {
        type Storage = veloq_runtime::utils::storage::AtomicStorage;

        fn poll_raw(&self, _worker_id: usize) -> bool {
            let shared = unsafe { &*self.shared_ptr };
            if let Some(ctx) = shared.context_tls.get() {
                let extra = unsafe { &ctx.as_ref().extra };
                let mut driver = extra.driver.borrow_mut();
                let _ = driver.unregister_files(vec![self.fd]);
            }
            self.header.mark_completed_and_notify();
            unsafe {
                let header_ptr = std::ptr::NonNull::from(&self.header);
                (self.header.vtable.drop)(header_ptr);
            }
            true
        }

        fn header(&self) -> &veloq_runtime::task::GenericTaskHeader<Self::Storage> {
            &self.header
        }
    }

    impl<'ctx> UnregisterFileTask<'ctx> {
        const VTABLE: &'static veloq_runtime::task::TaskVTable<
            veloq_runtime::utils::storage::AtomicStorage,
        > = &veloq_runtime::task::TaskVTable {
            wake: |_| {},
            wake_by_ref: |_| {},
            poll: |header, worker_id| unsafe {
                let node = &*(header
                    as *const veloq_runtime::task::GenericTaskHeader<
                        veloq_runtime::utils::storage::AtomicStorage,
                    > as *const Self);
                veloq_runtime::task::RawTask::poll_raw(node, worker_id)
            },
            drop: |data| unsafe {
                let ptr = data.as_ptr() as *mut Self;
                let _ = Box::from_raw(ptr);
            },
        };
    }

    let task = Box::new(UnregisterFileTask {
        header: veloq_runtime::task::TaskHeader::new(UnregisterFileTask::VTABLE),
        fd,
        shared_ptr: shared as *const _,
    });

    task.header.set_pinned();
    task.header.set_runtime_info(Some(&shared.base), worker_id);

    let ptr = Box::into_raw(task);
    let task_ref = unsafe { veloq_runtime::task::SendTaskRef::from_concrete(ptr) };

    if !shared.enqueue_pinned(worker_id, task_ref) {
        unsafe {
            let _ = Box::from_raw(ptr);
        }
    }
}
