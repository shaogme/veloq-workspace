pub mod context;

use std::future::Future;
use std::num::NonZeroUsize;

use veloq_buf::{NoopRegistrar, PoolTopology};
use veloq_runtime_next::runtime::{self as async_runtime, WorkerInitContext};

pub struct RuntimeBuilder<T: PoolTopology> {
    worker_count: Option<NonZeroUsize>,
    topology: T,
}

impl<T: PoolTopology> RuntimeBuilder<T> {
    pub fn new(topology: T) -> Self {
        Self {
            worker_count: None,
            topology,
        }
    }

    pub fn worker_count(mut self, worker_count: NonZeroUsize) -> Self {
        self.worker_count = Some(worker_count);
        self
    }

    pub fn build(self) -> std::io::Result<Runtime<T>> {
        let worker_count = self.worker_count.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
        });
        let state = self.topology.init(worker_count.get())?;

        Ok(Runtime {
            worker_count,
            topology: self.topology,
            state,
        })
    }
}

pub struct Runtime<T: PoolTopology> {
    worker_count: NonZeroUsize,
    topology: T,
    state: T::State,
}

impl<T: PoolTopology> Runtime<T> {
    pub fn builder(topology: T) -> RuntimeBuilder<T> {
        RuntimeBuilder::new(topology)
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn block_on<F: Future>(self, future: F) -> F::Output {
        let Runtime {
            worker_count,
            topology,
            state,
        } = self;

        struct ClearCurrentContext;
        impl Drop for ClearCurrentContext {
            fn drop(&mut self) {
                context::clear_current_runtime_context();
            }
        }

        let _clear = ClearCurrentContext;

        let runtime = async_runtime::Runtime::builder()
            .worker_count(worker_count)
            .with_worker_init(move |worker_ctx: WorkerInitContext| {
                let topology = topology.clone();
                let state = state.clone();
                async move {
                    let pool =
                        topology.build(&state, worker_ctx.worker_id(), Box::new(NoopRegistrar));
                    context::set_current_runtime_context(context::RuntimeContext::new(pool));
                }
            })
            .build();

        runtime.block_on(future)
    }
}
