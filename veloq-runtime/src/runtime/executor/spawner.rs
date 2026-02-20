use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tracing::{trace, warn};

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;

use crate::runtime::task::{SpawnedTask, Task};
use veloq_driver::driver::RemoteWaker;

// --- Job Definition ---

pub type Job = SpawnedTask;
use parking_lot::RwLock;

// Alias for the new stealable task type
use crate::runtime::task::harness::{self, Runnable, Schedule};

#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    pub id: u16,
    pub ptr: usize,
    pub len: usize,
}

pub(crate) fn pack_job<F, Output>(async_fn: F) -> (crate::runtime::join::JoinHandle<Output>, Job)
where
    F: AsyncFnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    let (handle, producer) = crate::runtime::join::JoinHandle::new();
    let task = unsafe {
        // SAFETY: The task is not Send, but it is only scheduled on the local executor.
        SpawnedTask::new_unchecked(async move {
            let output = async_fn();
            let future = async move {
                let output = output.await;
                producer.set(output);
            };
            future.await
        })
    };
    (handle, task)
}

// --- Shared State ---
pub(crate) struct ExecutorShared {
    pub(crate) pinned: std::sync::mpsc::Sender<Job>,
    pub(crate) remote_queue: std::sync::mpsc::Sender<Task>,

    // --- New Stealable Task Support ---
    pub(crate) future_injector: ArrayQueue<Runnable>,
    pub(crate) stealer: crossbeam_deque::Stealer<Runnable>,
    // ----------------------------------
    pub(crate) waker: LateBoundWaker,
    pub(crate) injected_load: CachePadded<AtomicUsize>,
    pub(crate) local_load: CachePadded<AtomicUsize>,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    pub(crate) shutdown: AtomicBool,
}

impl harness::Schedule for ExecutorShared {
    fn schedule(&self, task: Runnable) {
        trace!("Scheduling task via injector");
        if self.future_injector.push(task).is_err() {
            warn!("Internal task queue is full, dropping task");
            return;
        }
        self.injected_load.fetch_add(1, Ordering::Relaxed);
        self.waker
            .wake()
            .expect("Failed to wake executor via scheduler");
    }
}

pub(crate) struct LateBoundWaker {
    waker: std::cell::UnsafeCell<Option<Arc<dyn RemoteWaker>>>,
    ready: std::sync::atomic::AtomicBool,
}

unsafe impl Send for LateBoundWaker {}
unsafe impl Sync for LateBoundWaker {}

impl LateBoundWaker {
    pub fn new() -> Self {
        Self {
            waker: std::cell::UnsafeCell::new(None),
            ready: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn set(&self, waker: Arc<dyn RemoteWaker>) {
        unsafe { *self.waker.get() = Some(waker) };
        self.ready.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl RemoteWaker for LateBoundWaker {
    fn wake(&self) -> std::io::Result<()> {
        if self.ready.load(std::sync::atomic::Ordering::Acquire) {
            let w = unsafe { &*self.waker.get() };
            if let Some(w) = w {
                return w.wake();
            }
        }
        Ok(())
    }
}

// --- Executor Handles ---

/// Handle to a remote executor, used for task injection and load monitoring.
#[derive(Clone)]
pub struct ExecutorHandle {
    pub(crate) id: usize,
    pub(crate) shared: Arc<ExecutorShared>,
}

impl ExecutorHandle {
    pub(crate) fn schedule_pinned(&self, job: Job) {
        // We ignore the error here because if the receiver is dropped,
        // the worker is dead and thus strictly speaking "not available".
        // However, in a robust system we might want to log this.
        let _ = self.shared.pinned.send(job);
        trace!(worker_id = self.id, "Scheduled pinned task");
        self.shared.injected_load.fetch_add(1, Ordering::Relaxed);
        self.shared.waker.wake().expect("Failed to wake executor");
    }

    pub fn total_load(&self) -> usize {
        self.shared.injected_load.load(Ordering::Relaxed)
            + self.shared.local_load.load(Ordering::Relaxed)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

// --- Spawner & Registry ---

/// A static registry that maintains the set of all active executors.
/// Workers are pre-allocated at runtime startup.
pub struct ExecutorRegistry {
    handles: Arc<Vec<ExecutorHandle>>,
    // Pull Model Support
    pub(crate) epoch: AtomicUsize,
    pub(crate) memory_chunks: RwLock<Vec<ChunkInfo>>,
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl ExecutorRegistry {
    pub fn new(handles: Vec<ExecutorHandle>) -> Self {
        Self {
            handles: Arc::new(handles),
            epoch: AtomicUsize::new(0),
            memory_chunks: RwLock::new(Vec::new()),
        }
    }

    pub fn all(&self) -> &[ExecutorHandle] {
        &self.handles
    }

    pub fn register_chunk(&self, id: u16, ptr: usize, len: usize) {
        let chunk = ChunkInfo { id, ptr, len };
        {
            let mut chunks = self.memory_chunks.write();
            chunks.push(chunk);
        }
        self.epoch.fetch_add(1, Ordering::Release);

        // Notify all workers
        for handle in self.handles.iter() {
            let _ = handle.shared.waker.wake();
        }
    }
}

/// Global spawner that acts as a frontend to the Registry.
#[derive(Clone)]
pub struct Spawner {
    registry: Arc<ExecutorRegistry>,
    seed: Cell<usize>,
}

impl Spawner {
    pub fn new(registry: Arc<ExecutorRegistry>) -> Self {
        // Random seed init
        let seed = Box::into_raw(Box::new(0)) as usize;
        Self {
            registry,
            seed: Cell::new(seed),
        }
    }

    fn select_worker(&self) -> ExecutorHandle {
        let workers = self.registry.all();

        match self.p2c_select(workers) {
            Some(target) => target.clone(),
            None => panic!("No workers available in registry"),
        }
    }

    pub fn spawn<F, Output>(&self, future: F) -> crate::runtime::join::JoinHandle<Output>
    where
        F: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        // Select a worker to "own" this task initially.
        // This is important because the Runnable needs a "home" Injector (scheduler)
        // to return to if it is remotely woken.
        let target = self.select_worker();
        trace!(target_worker = target.id, "Spawning task");

        unsafe {
            let (job, handle) = harness::spawn_arc(future, target.shared.clone());
            // We schedule it via the trait, which pushes to future_injector
            target.shared.schedule(job);
            handle
        }
    }

    fn p2c_select<'a>(&self, workers: &'a [ExecutorHandle]) -> Option<&'a ExecutorHandle> {
        let count = workers.len();
        if count == 0 {
            return None;
        }

        let mut seed = self.seed.get();
        // Simple Xorshift
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        self.seed.set(seed);

        let idx1 = seed % count;
        let idx2 = (seed >> 32) % count;

        let w1 = &workers[idx1];
        let w2 = &workers[idx2];

        let load1 = w1.total_load();
        let load2 = w2.total_load();

        if load1 <= load2 { Some(w1) } else { Some(w2) }
    }

    pub fn spawn_to<F, Output>(
        &self,
        worker_id: usize,
        async_fn: F,
    ) -> crate::runtime::join::JoinHandle<Output>
    where
        F: AsyncFnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        let (handle, job) = pack_job(async_fn);
        trace!(worker_id, "Spawning pinned task to worker");
        self.spawn_job_to(job, worker_id);
        handle
    }

    pub(crate) fn spawn_job_to(&self, job: Job, worker_id: usize) {
        let workers = self.registry.all();

        if let Some(target) = workers.get(worker_id) {
            target.schedule_pinned(job);
        } else {
            // Panic if the worker_id is invalid, as this implies a logic error in the caller
            // assuming the existence of a specific worker.
            panic!("Worker {} not found in registry", worker_id);
        }
    }
}
