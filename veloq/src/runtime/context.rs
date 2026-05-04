use std::cell::RefCell;

use veloq_buf::AnyBufPool;

thread_local! {
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct RuntimeContext {
    buf_pool: AnyBufPool,
}

impl RuntimeContext {
    pub(crate) fn new(buf_pool: AnyBufPool) -> Self {
        Self { buf_pool }
    }

    #[inline]
    pub fn buf_pool(&self) -> AnyBufPool {
        self.buf_pool.clone()
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
