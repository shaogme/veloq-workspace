pub mod context;

use std::cell::RefCell;
use std::future::Future;
use std::rc::{Rc, Weak};
use std::sync::{Arc, RwLock};

use veloq_buf::{BufferRegistrar, PoolTopology};
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_runtime_next::runtime::{self as async_runtime, WorkerInitContext};

#[derive(Clone)]
struct DriverRegistrar {
    driver: Weak<RefCell<PlatformDriver>>,
    chunks: Arc<RwLock<Vec<veloq_buf::heap::ChunkInfo>>>,
}

impl DriverRegistrar {
    fn new(driver: Weak<RefCell<PlatformDriver>>) -> Self {
        Self {
            driver,
            chunks: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl BufferRegistrar for DriverRegistrar {
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

pub struct RuntimeBuilder<T: PoolTopology> {
    worker_count: Option<std::num::NonZeroUsize>,
    topology: T,
}

impl<T: PoolTopology> RuntimeBuilder<T> {
    pub fn new(topology: T) -> Self {
        Self {
            worker_count: None,
            topology,
        }
    }

    pub fn worker_count(mut self, worker_count: std::num::NonZeroUsize) -> Self {
        self.worker_count = Some(worker_count);
        self
    }

    pub fn build(self) -> std::io::Result<Runtime<T>> {
        let worker_count = self.worker_count.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .unwrap_or_else(|_| std::num::NonZeroUsize::new(1).expect("1 is non-zero"))
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
    worker_count: std::num::NonZeroUsize,
    topology: T,
    state: T::State,
}

impl<T: PoolTopology> Runtime<T> {
    pub fn builder(topology: T) -> RuntimeBuilder<T> {
        RuntimeBuilder::new(topology)
    }

    pub fn worker_count(&self) -> std::num::NonZeroUsize {
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
                    #[cfg(not(windows))]
                    let driver = Rc::new(RefCell::new(
                        PlatformDriver::new(veloq_driver::config::UringConfig::default())
                            .expect("failed to create uring driver"),
                    ));
                    #[cfg(windows)]
                    let driver = Rc::new(RefCell::new(
                        PlatformDriver::new(veloq_driver::config::IocpConfig::default())
                            .expect("failed to create iocp driver"),
                    ));

                    let registrar = DriverRegistrar::new(Rc::downgrade(&driver));
                    let listener_chunks = registrar.chunks.clone();
                    topology.connect_listener(
                        &state,
                        Box::new(move |chunk_info| {
                            listener_chunks
                                .write()
                                .expect("chunk registry poisoned")
                                .push(chunk_info);
                        }),
                    );
                    {
                        let mut driver_ref = driver.borrow_mut();
                        driver_ref.set_registrar(Box::new(registrar.clone()));
                    }

                    let buf_pool =
                        topology.build(&state, worker_ctx.worker_id(), Box::new(registrar));
                    context::set_current_runtime_context(context::RuntimeContext::new(
                        driver, buf_pool,
                    ));
                }
            })
            .build();

        runtime.block_on(future)
    }
}
