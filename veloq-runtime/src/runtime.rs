pub mod context;
pub mod executor;
pub mod join;
pub mod task;

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;

use crossbeam_deque::Worker;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use tracing::debug;

use crate::config::Config;
use crate::runtime::executor::spawner::LateBoundWaker;
use crate::runtime::executor::{ExecutorHandle, ExecutorRegistry, ExecutorShared, Spawner};
use crate::runtime::task::harness::Runnable;
use crate::runtime::task::{SpawnedTask, Task};
// Re-export common types
pub use context::{RuntimeContext, spawn, spawn_local, spawn_to, yield_now};
pub use executor::LocalExecutor;
pub use join::{JoinHandle, LocalJoinHandle};

use veloq_buf::buddy::BuddySpec;
use veloq_buf::global::{GlobalAllocator, GlobalAllocatorConfig, GlobalMemoryInfo};
use veloq_buf::{PoolSpec, ThreadMemory};
use veloq_driver::driver::RemoteWaker;

use veloq_blocking::init_blocking_pool;

pub mod blocking {
    pub use veloq_blocking::*;
}

struct WorkerPrep<P: PoolSpec> {
    shared: Arc<ExecutorShared>,
    remote_receiver: mpsc::Receiver<Task>,
    pinned_receiver: mpsc::Receiver<SpawnedTask>,
    // Worker local queue for stealable tasks
    stealable_worker: Worker<Runnable>,
    thread_memory: ThreadMemory,
    global_info: GlobalMemoryInfo,
    pool_spec: P,
    config: Config,
    barrier: Arc<Barrier>,
}

pub struct RuntimeBuilder<P: PoolSpec = BuddySpec> {
    config: Config,
    pool_spec: P,
}

impl RuntimeBuilder<BuddySpec> {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
            pool_spec: BuddySpec::default(),
        }
    }
}

impl<P: PoolSpec> RuntimeBuilder<P> {
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn with_buffer_spec<NewP: PoolSpec>(self, spec: NewP) -> RuntimeBuilder<NewP> {
        RuntimeBuilder {
            config: self.config,
            pool_spec: spec,
        }
    }

    pub fn build(self) -> std::io::Result<Runtime<P>> {
        let worker_count = self
            .config
            .worker_threads
            .map(|w| w.get())
            .unwrap_or(num_cpus::get());
        debug!("Building Runtime with {} workers", worker_count);

        // Initialize the blocking pool
        init_blocking_pool(self.config.blocking_pool.clone());

        let memory_req = P::MEMORY_REQUIREMENT;
        let multiplier_val = memory_req.get() / veloq_buf::MIN_THREAD_MEMORY.get();
        // Ensure at least 1 multiplier if memory_req is small
        let multiplier =
            unsafe { std::num::NonZeroUsize::new_unchecked(std::cmp::max(1, multiplier_val)) };

        let alloc_config = GlobalAllocatorConfig {
            multipliers: vec![veloq_buf::ThreadMemoryMultiplier(multiplier); worker_count],
        };

        let (memories, global_info) = GlobalAllocator::new(alloc_config)?;

        // Struct to hold per-worker resources temporarily
        struct WorkerInit {
            shared: Arc<ExecutorShared>,
            remote_rx: mpsc::Receiver<Task>,
            pinned_rx: mpsc::Receiver<SpawnedTask>,
            worker: Worker<Runnable>,
        }

        let queue_capacity = self.config.internal_queue_capacity;

        // 1. Initialize Resources per Worker (Functional / Pipeline)
        let (handles, workers): (Vec<_>, Vec<_>) = (0..worker_count)
            .map(|i| {
                let (remote_tx, remote_rx) = mpsc::channel();
                let (pinned_tx, pinned_rx) = mpsc::channel();

                let worker = Worker::new_fifo();
                let stealer = worker.stealer();

                let shared = Arc::new(ExecutorShared {
                    pinned: pinned_tx,
                    remote_queue: remote_tx,
                    future_injector: ArrayQueue::new(queue_capacity),
                    stealer,
                    waker: LateBoundWaker::new(),
                    injected_load: CachePadded::new(AtomicUsize::new(0)),
                    local_load: CachePadded::new(AtomicUsize::new(0)),
                    state: Arc::new(AtomicU8::new(executor::RUNNING)),
                    shutdown: AtomicBool::new(false),
                });

                let handle = ExecutorHandle {
                    id: i,
                    shared: shared.clone(),
                };

                let init = WorkerInit {
                    shared,
                    remote_rx,
                    pinned_rx,
                    worker,
                };

                (handle, init)
            })
            .unzip();

        let registry = Arc::new(ExecutorRegistry::new(handles));

        // Initialize peer file descriptors storage (initially 0)
        let peer_handles = Arc::new(
            (0..worker_count)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );

        // Barrier for synchronizing worker startup
        let barrier = Arc::new(Barrier::new(worker_count));

        // 2. Separate Worker 0 Resources
        let mut workers_iter = workers.into_iter();
        let worker_0_res = workers_iter
            .next()
            .expect("Worker 0 resources missing (count > 0 checked)");

        let mut memories_iter = memories.into_iter();
        let worker_0_memory = memories_iter
            .next()
            .expect("Worker 0 memory missing (count > 0 checked)");

        let mut thread_handles = Vec::with_capacity(worker_count - 1);

        // 3. Spawn Background Workers (1..N)
        for (i, (res, memory)) in workers_iter.zip(memories_iter).enumerate() {
            let worker_id = i + 1; // Iterator started from index 1

            let registry = registry.clone();
            let peer_handles = peer_handles.clone();
            let barrier = barrier.clone();
            let config = self.config.clone();
            // Assuming global_info is Copy or cheap to Clone (it's usually (ptr, len))
            let global_info = global_info;
            let pool_spec = self.pool_spec.clone();

            let builder = thread::Builder::new().name(format!("veloq-worker-{}", worker_id));

            let handle = builder.spawn(move || {
                debug!("Worker {} thread started", worker_id);
                let mut executor = LocalExecutor::builder()
                    .config(config)
                    .with_shared(res.shared)
                    .with_remote_receiver(res.remote_rx)
                    .with_pinned_receiver(res.pinned_rx)
                    .with_worker(res.worker)
                    .build(|registrar| pool_spec.build(memory, registrar, global_info));

                executor = executor.with_registry(registry);
                executor = executor.with_id(worker_id);

                // Publish Handle
                peer_handles[worker_id].store(executor.raw_driver_handle(), Ordering::Release);

                // Wait for all workers to be ready
                barrier.wait();

                // Run Loop
                executor.run();
            })?;

            thread_handles.push(handle);
        }

        // 4. Prepare Worker 0 Prep State (for block_on)
        let worker_0_prep = WorkerPrep {
            shared: worker_0_res.shared,
            remote_receiver: worker_0_res.remote_rx,
            pinned_receiver: worker_0_res.pinned_rx,
            stealable_worker: worker_0_res.worker,
            thread_memory: worker_0_memory,
            global_info,
            pool_spec: self.pool_spec,
            config: self.config,
            barrier,
        };

        Ok(Runtime {
            handles: thread_handles,
            registry,
            peer_handles,
            worker_count,
            worker_0_prep: Some(worker_0_prep),
        })
    }
}

pub struct Runtime<P: PoolSpec = BuddySpec> {
    handles: Vec<thread::JoinHandle<()>>,
    registry: Arc<ExecutorRegistry>,
    peer_handles: Arc<Vec<AtomicUsize>>,
    #[allow(dead_code)]
    worker_count: usize,
    worker_0_prep: Option<WorkerPrep<P>>,
}

impl<P: PoolSpec> Drop for Runtime<P> {
    fn drop(&mut self) {
        debug!("Runtime shutting down");
        // 1. Notify all workers to stop
        for handle in self.registry.all() {
            handle.shared.shutdown.store(true, Ordering::Relaxed);
            // Wake them up so they see the shutdown signal
            let _ = handle.shared.waker.wake();
        }

        // 2. Wait for worker threads to finish
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Runtime<BuddySpec> {
    pub fn builder() -> RuntimeBuilder<BuddySpec> {
        RuntimeBuilder::new()
    }

    // Legacy New (Optional, can delegate to Builder)
    pub fn new(config: Config) -> Self {
        Self::builder()
            .config(config)
            .build()
            .expect("Failed to build runtime")
    }
}

impl<P: PoolSpec> Runtime<P> {
    pub fn spawner(&self) -> Spawner {
        Spawner::new(self.registry.clone())
    }

    pub fn spawn<F, Output>(&self, future: F) -> JoinHandle<Output>
    where
        F: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        self.spawner().spawn(future)
    }

    /// Block on a future using a local executor on the current thread,
    /// participating in the runtime as Worker 0 (an internal mesh node).
    ///
    /// This method consumes the Runtime, setting up the current thread as the first worker.
    /// It waits for all other pre-spawned workers to be ready before starting execution.
    pub fn block_on<F>(mut self, future: F) -> F::Output
    where
        F: Future,
    {
        debug!("Block_on entered (Worker 0)");
        let prep = self
            .worker_0_prep
            .take()
            .expect("Runtime already started or invalid state");

        let mut executor = LocalExecutor::builder()
            .config(prep.config)
            .with_shared(prep.shared) // Inject shared state
            .with_remote_receiver(prep.remote_receiver) // Inject remote receiver
            .with_pinned_receiver(prep.pinned_receiver) // Inject pinned receiver
            .with_worker(prep.stealable_worker) // Inject stealable worker
            .build(|registrar| {
                // Bind Buffer Pool using stored prep data
                prep.pool_spec
                    .build(prep.thread_memory, registrar, prep.global_info)
            });

        executor = executor.with_registry(self.registry.clone());
        executor = executor.with_id(0); // Set Worker ID (Worker 0)

        // Publish Handle
        let fd = executor.raw_driver_handle();
        self.peer_handles[0].store(fd, Ordering::Release);

        // Wait for all workers to be ready
        prep.barrier.wait();

        // Run Future
        // block_on in LocalExecutor runs the loop until future completes.
        executor.block_on(future)
    }
}
