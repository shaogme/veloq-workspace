use std::cell::{Cell, RefCell};
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::Poll;

use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_driver::op::{IntoPlatformOp, Op, OpSubmitter};

use crate::config::{BufferRegistrationMode, Config};

thread_local! {
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct DriverRegistrar {
    driver: Weak<RefCell<PlatformDriver>>,
    pub(crate) chunks: Arc<std::sync::RwLock<Vec<veloq_buf::heap::ChunkInfo>>>,
    processed_count: Cell<usize>,
    registration_mode: BufferRegistrationMode,
}

impl DriverRegistrar {
    pub(crate) fn new(
        driver: Weak<RefCell<PlatformDriver>>,
        registration_mode: BufferRegistrationMode,
    ) -> Self {
        Self {
            driver,
            chunks: Arc::new(std::sync::RwLock::new(Vec::new())),
            processed_count: Cell::new(0),
            registration_mode,
        }
    }

    pub fn sync_to_driver(&self) {
        let Some(driver_rc) = self.driver.upgrade() else {
            return;
        };

        let start = self.processed_count.get();

        let (total_chunks, new_chunks) = {
            let chunks = self.chunks.read().expect("chunk registry poisoned");
            let total = chunks.len();
            if total <= start {
                return;
            }

            let new_chunks = if matches!(self.registration_mode, BufferRegistrationMode::Compatible)
            {
                Some(chunks[start..].to_vec())
            } else {
                None
            };
            (total, new_chunks)
        };

        if let Some(chunks_to_reg) = new_chunks {
            let mut driver = driver_rc.borrow_mut();
            for chunk in chunks_to_reg {
                let _ = driver.register_chunk(chunk.id, chunk.ptr.as_ptr(), chunk.len.get());
            }
        }

        self.processed_count.set(total_chunks);
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
        for (idx, region) in regions.iter().enumerate() {
            let chunk_idx = idx as u16;
            driver
                .register_chunk(chunk_idx, region.as_ptr(), region.len())
                .map_err(|err| std::io::Error::other(format!("{err:#}")))?;
            self.chunks.write().expect("chunk registry poisoned").push(
                veloq_buf::heap::ChunkInfo {
                    id: chunk_idx,
                    ptr: unsafe { std::ptr::NonNull::new_unchecked(region.as_ptr() as *mut u8) },
                    len: unsafe { std::num::NonZeroUsize::new_unchecked(region.len()) },
                },
            );
            indices.push(idx);
        }
        self.processed_count
            .set(self.processed_count.get() + regions.len());
        Ok(indices)
    }

    fn resolve_chunk_info(&self, chunk_id: u16) -> Option<veloq_buf::heap::ChunkInfo> {
        self.chunks
            .read()
            .expect("chunk registry poisoned")
            .iter()
            .find(|chunk| chunk.id == chunk_id)
            .copied()
    }
}

#[derive(Clone)]
pub struct RuntimeContext {
    buf_pool: AnyBufPool,
    driver: Rc<RefCell<PlatformDriver>>,
    config: Config,
    registrar: DriverRegistrar,
}

impl RuntimeContext {
    pub(crate) fn new(
        driver: Rc<RefCell<PlatformDriver>>,
        buf_pool: AnyBufPool,
        config: Config,
        registrar: DriverRegistrar,
    ) -> Self {
        Self {
            buf_pool,
            driver,
            config,
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
    pub fn config(&self) -> Config {
        self.config.clone()
    }

    #[inline]
    pub fn registrar(&self) -> DriverRegistrar {
        self.registrar.clone()
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
        + 'static + 'a,
{
    let ctx = current();
    ctx.registrar().sync_to_driver();
    let fut = submitter.submit(op, ctx.driver());
    let mut fut = Box::pin(fut);

    async move {
        std::future::poll_fn(
            move |cx: &mut std::task::Context<'_>| match fut.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    let ctx = current();
                    ctx.registrar().sync_to_driver();
                    let driver_rc = ctx.driver();
                    let mut driver = driver_rc.borrow_mut();
                    driver
                        .submit_queue()
                        .unwrap_or_else(|err| panic!("driver submit_queue failed: {err:#}"));
                    driver
                        .wait()
                        .unwrap_or_else(|err| panic!("driver wait failed: {err:#}"));
                    driver.process_completions();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
        )
        .await
    }
}

pub async fn yield_now() {
    if let Some(ctx) = try_current() {
        ctx.registrar().sync_to_driver();
    }
    veloq_runtime_next::task::yield_now().await;
}
