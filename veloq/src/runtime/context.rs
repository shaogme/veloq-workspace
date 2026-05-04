use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::task::Poll;

use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_driver::op::{IntoPlatformOp, Op, OpSubmitter};

thread_local! {
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct RuntimeContext {
    buf_pool: AnyBufPool,
    driver: Rc<RefCell<PlatformDriver>>,
}

impl RuntimeContext {
    pub(crate) fn new(driver: Rc<RefCell<PlatformDriver>>, buf_pool: AnyBufPool) -> Self {
        Self { buf_pool, driver }
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.buf_pool.clone()
    }

    #[inline]
    pub fn driver(&self) -> Rc<RefCell<PlatformDriver>> {
        self.driver.clone()
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
    let fut = submitter.submit(op, current().driver());
    let mut fut = Box::pin(fut);

    async move {
        std::future::poll_fn(
            move |cx: &mut std::task::Context<'_>| match fut.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    let driver_rc = current().driver();
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
