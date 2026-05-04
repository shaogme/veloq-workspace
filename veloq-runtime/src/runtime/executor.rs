use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crossbeam_deque::Worker;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use tracing::{debug, trace, warn};
use veloq_buf::BufferRegistrar;
use veloq_driver::config::BufferRegistrationMode;
use veloq_driver::driver::{Driver, DriverControlCommand, PlatformDriver, RemoteWaker};

use crate::runtime::context::RuntimeContext;
pub(crate) use crate::runtime::executor::spawner::ExecutorShared;
use crate::runtime::join::LocalJoinHandle;
use crate::runtime::task::harness::Runnable;
use crate::runtime::task::{SpawnedTask, Task};

/// State: The worker is actively running tasks.
pub const RUNNING: u8 = 0;
/// State: The worker is preparing to park (checking queues one last time).
pub const PARKING: u8 = 1;
/// State: The worker is fully parked on the driver (sleeping).
pub const PARKED: u8 = 2;

// Re-export common types from spawner which acts as the definition source for these
pub use self::spawner::{ExecutorHandle, ExecutorRegistry, Job, Spawner};

pub mod spawner;

// ============ LocalExecutor Implementation ============

pub struct LocalExecutorBuilder {
    config: crate::config::Config,
    shared: Option<Arc<ExecutorShared>>,
    remote_receiver: Option<mpsc::Receiver<Task>>,
    pinned_receiver: Option<mpsc::Receiver<SpawnedTask>>,
    control_receiver: Option<mpsc::Receiver<Vec<DriverControlCommand>>>,
    stealable: Option<Worker<Runnable>>,
    stealer: Option<crossbeam_deque::Stealer<Runnable>>,
    registry: Option<Arc<ExecutorRegistry>>,
}

impl Default for LocalExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecutorBuilder {
    pub fn new() -> Self {
        Self {
            config: crate::config::Config::default(),
            shared: None,
            remote_receiver: None,
            pinned_receiver: None,
            control_receiver: None,
            stealable: None,
            stealer: None,
            registry: None,
        }
    }

    pub(crate) fn with_shared(mut self, shared: Arc<ExecutorShared>) -> Self {
        self.shared = Some(shared);
        self
    }

    pub(crate) fn with_remote_receiver(mut self, receiver: mpsc::Receiver<Task>) -> Self {
        self.remote_receiver = Some(receiver);
        self
    }

    pub(crate) fn with_pinned_receiver(mut self, receiver: mpsc::Receiver<SpawnedTask>) -> Self {
        self.pinned_receiver = Some(receiver);
        self
    }

    pub(crate) fn with_control_receiver(
        mut self,
        receiver: mpsc::Receiver<Vec<DriverControlCommand>>,
    ) -> Self {
        self.control_receiver = Some(receiver);
        self
    }

    pub(crate) fn with_worker(mut self, worker: Worker<Runnable>) -> Self {
        self.stealer = Some(worker.stealer());
        self.stealable = Some(worker);
        self
    }

    pub fn config(mut self, config: crate::config::Config) -> Self {
        self.config = config;
        self
    }

    pub fn with_registry(mut self, registry: Arc<ExecutorRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Build the LocalExecutor.
    ///
    /// Requires a `pool_constructor` closure that creates an `AnyBufPool` using the provided `BufferRegistrar`.
    pub fn build<F>(self, pool_constructor: F) -> LocalExecutor
    where
        F: FnOnce(Box<dyn BufferRegistrar>) -> veloq_buf::AnyBufPool,
    {
        let driver_val = PlatformDriver::new(&self.config).expect("Failed to create driver");
        // Wrap driver early to create registrar
        let driver = Rc::new(RefCell::new(driver_val));

        let queue = Rc::new(RefCell::new(VecDeque::new()));

        // Borrow driver to create waker
        let waker = driver.borrow().create_waker();

        // Prepare Worker/Stealer
        let (stealable, stealer) = if let Some(w) = self.stealable {
            (w, self.stealer.expect("Stealer missing"))
        } else {
            let w = Worker::new_fifo();
            let s = w.stealer();
            (w, s)
        };

        let (shared, remote_receiver, pinned_receiver, control_receiver) =
            if let Some(shared) = self.shared {
                let remote_rec = self
                    .remote_receiver
                    .expect("Shared state provided without remote receiver");
                let pinned_rec = self
                    .pinned_receiver
                    .expect("Shared state provided without pinned receiver");
                let control_rec = self
                    .control_receiver
                    .expect("Shared state provided without control receiver");
                (shared, remote_rec, pinned_rec, control_rec)
            } else {
                let (remote_tx, remote_rx) = mpsc::channel();
                let (pinned_tx, pinned_rx) = mpsc::channel();
                let (control_tx, control_rx) = mpsc::channel();
                // Default state is RUNNING
                let state = Arc::new(AtomicU8::new(RUNNING));

                let queue_capacity = self.config.queue_capacity();

                let shared = Arc::new(ExecutorShared {
                    pinned: pinned_tx,
                    remote_queue: remote_tx,
                    control_queue: control_tx,
                    future_injector: ArrayQueue::new(queue_capacity),
                    stealer,
                    waker: crate::runtime::executor::spawner::LateBoundWaker::new(),
                    injected_load: CachePadded::new(AtomicUsize::new(0)),
                    local_load: CachePadded::new(AtomicUsize::new(0)),
                    state,
                    shutdown: AtomicBool::new(false),
                });
                (shared, remote_rx, pinned_rx, control_rx)
            };

        // Bind the driver's waker to the shared state (Late Binding)
        shared.waker.set(waker);

        // Construct registrar for driver lazy registration path.
        {
            let mut driver_ref = driver.borrow_mut();
            driver_ref.set_registrar(Box::new(ExecutorRegistrar {
                driver: Rc::downgrade(&driver),
                registry: self.registry.clone(),
            }));
        }

        // Construct Registrar and Pool
        let registrar = Box::new(ExecutorRegistrar {
            driver: Rc::downgrade(&driver),
            registry: self.registry.clone(),
        });

        let buf_pool = pool_constructor(registrar);

        #[cfg(target_os = "linux")]
        let registration_mode = self.config.get_uring().registration_mode;
        #[cfg(target_os = "windows")]
        let registration_mode = self.config.get_iocp().registration_mode;

        LocalExecutor {
            driver,
            queue,
            shared,
            remote_receiver,
            pinned_receiver,
            control_receiver,
            stealable: Rc::new(stealable),
            registry: self.registry,
            id: usize::MAX,
            buf_pool,
            last_seen_epoch: Cell::new(0),
            processed_chunk_count: Cell::new(0),
            registration_mode,
        }
    }
    pub fn build_with_uniform_pool(self, memory_multiplier: usize) -> LocalExecutor {
        use veloq_buf::{BufferRegion, PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

        let multiplier =
            std::num::NonZeroUsize::new(memory_multiplier).expect("Memory multiplier must be > 0");
        let topology = UniformSlot::new(ThreadMemoryMultiplier(multiplier));

        // We are creating a single-threaded executor, so worker_count = 1
        let global_pool = topology
            .create_pool(1)
            .expect("Failed to create global pool");

        // We are worker 0
        let worker_idx = 0;

        self.build(move |registrar| {
            // Register global memory
            let info = global_pool.global_info();
            let regions = [BufferRegion::new(info.ptr, info.len)];
            registrar
                .register(&regions)
                .expect("Failed to register global memory");

            // Use topology to build pool
            topology.build(&global_pool, worker_idx, registrar)
        })
    }
}

pub struct LocalExecutor {
    driver: Rc<RefCell<PlatformDriver>>,
    queue: Rc<RefCell<VecDeque<Task>>>,

    // Shared components
    shared: Arc<ExecutorShared>,
    remote_receiver: mpsc::Receiver<Task>,
    pinned_receiver: mpsc::Receiver<SpawnedTask>,
    control_receiver: mpsc::Receiver<Vec<DriverControlCommand>>,

    // Local Stealable Queue (Send Tasks)
    stealable: Rc<Worker<Runnable>>,

    // Optional connection to the global registry
    registry: Option<Arc<ExecutorRegistry>>,

    // Cached ID (usize::MAX if not in mesh)
    id: usize,
    // Buffer Pool
    buf_pool: veloq_buf::AnyBufPool,

    // Pull Model State
    last_seen_epoch: Cell<usize>,
    processed_chunk_count: Cell<usize>,
    registration_mode: BufferRegistrationMode,
}

impl LocalExecutor {
    pub fn driver_handle(&self) -> std::rc::Weak<RefCell<PlatformDriver>> {
        Rc::downgrade(&self.driver)
    }

    /// Create a new builder for LocalExecutor.
    pub fn builder() -> LocalExecutorBuilder {
        LocalExecutorBuilder::new()
    }

    /// Create a default LocalExecutor with a UniformSlot pool (multiplier 8).
    /// Suitable for tests and simple single-threaded use cases.
    pub fn new_default() -> Self {
        Self::builder().build_with_uniform_pool(8)
    }

    /// Attach this executor to a registry.
    /// This enables the executor to steal tasks from others in the registry.
    pub fn with_registry(mut self, registry: Arc<ExecutorRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the worker ID for this executor.
    pub fn with_id(mut self, id: usize) -> Self {
        self.id = id;
        self
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn handle(&self) -> ExecutorHandle {
        ExecutorHandle {
            id: self.id,
            shared: self.shared.clone(),
        }
    }

    pub fn registrar(&self) -> Box<dyn BufferRegistrar> {
        Box::new(ExecutorRegistrar {
            driver: Rc::downgrade(&self.driver),
            registry: self.registry.clone(),
        })
    }

    pub fn pool(&self) -> veloq_buf::AnyBufPool {
        self.buf_pool.clone()
    }

    pub fn spawn_local<F, T>(&self, future: F) -> LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let (handle, producer) = LocalJoinHandle::new();
        // SAFETY: This is spawn_local, so we can use new_local (!Send future).
        let task = unsafe {
            SpawnedTask::new_local(async move {
                let output = future.await;
                producer.set(output);
            })
        };
        let task = unsafe {
            task.bind(
                self.handle().id,
                Rc::downgrade(&self.queue),
                self.shared.clone(),
            )
        };
        self.queue.borrow_mut().push_back(task);
        self.shared.local_load.fetch_add(1, Ordering::Relaxed);
        handle
    }

    fn enqueue_job(&self, task: Job) {
        // Enqueue Job (SpawnedTask) to local queue.
        // This is local work.
        self.shared.local_load.fetch_add(1, Ordering::Relaxed);
        let task = unsafe {
            task.bind(
                self.handle().id,
                Rc::downgrade(&self.queue),
                self.shared.clone(),
            )
        };
        self.queue.borrow_mut().push_back(task);
    }

    fn try_poll_injector(&self) -> bool {
        // Check pinned first (strictly specific to this worker)
        if let Ok(job) = self.pinned_receiver.try_recv() {
            self.shared.injected_load.fetch_sub(1, Ordering::Relaxed);
            self.enqueue_job(job);
            return true;
        }

        // Check remote queue (Woken tasks from other threads)
        if let Ok(task) = self.remote_receiver.try_recv() {
            self.queue.borrow_mut().push_back(task);
            return true;
        }

        false
    }

    fn try_poll_driver_control(&self) -> bool {
        let mut did_work = false;
        while let Ok(commands) = self.control_receiver.try_recv() {
            did_work = true;
            let mut driver = self.driver.borrow_mut();
            for command in commands {
                match command {
                    DriverControlCommand::UnregisterFiles(files) => {
                        let _ = driver.unregister_files(files);
                    }
                }
            }
        }
        did_work
    }

    fn try_poll_future_injector(&self) -> bool {
        if let Some(task) = self.shared.future_injector.pop() {
            self.shared.injected_load.fetch_sub(1, Ordering::Relaxed);
            self.shared.local_load.fetch_add(1, Ordering::Relaxed);
            self.stealable.push(task);
            return true;
        }
        false
    }

    fn try_steal(&self, executed: usize) -> bool {
        if let Some(registry) = &self.registry {
            let workers = registry.all();
            let count = workers.len();

            if count > 0 {
                let seed = self as *const _ as usize;
                let start_idx = seed.wrapping_add(executed);

                for i in 0..count {
                    let idx = (start_idx + i) % count;
                    let target = &workers[idx];

                    if Arc::ptr_eq(&target.shared, &self.shared) {
                        continue;
                    }

                    // Steal from Stealer (Runnable)
                    use crossbeam_deque::Steal;
                    match target.shared.stealer.steal() {
                        Steal::Success(task) => {
                            target.shared.local_load.fetch_sub(1, Ordering::Relaxed);
                            trace!("Stole task from worker");
                            task.run();
                            return true;
                        }
                        Steal::Retry => {
                            // We could retry, but let's continue for now to avoid hanging
                        }
                        Steal::Empty => {}
                    }
                }
            }
        }
        false
    }

    fn check_for_memory_updates(&self) {
        if let Some(registry) = &self.registry {
            let global_epoch = registry.epoch.load(Ordering::Acquire);
            if global_epoch > self.last_seen_epoch.get() {
                self.sync_memory_chunks(global_epoch);
            }
        }
    }

    fn sync_memory_chunks(&self, target_epoch: usize) {
        if let Some(registry) = &self.registry {
            let (current_len, new_chunks) = {
                let chunks = registry.memory_chunks.read();
                let current_len = chunks.len();
                let processed = self.processed_chunk_count.get();
                let new_chunks = chunks.iter().skip(processed).copied().collect::<Vec<_>>();
                (current_len, new_chunks)
            };
            let processed = self.processed_chunk_count.get();

            if processed < current_len {
                if matches!(self.registration_mode, BufferRegistrationMode::Compatible) {
                    // Compatibility fallback: best-effort eager registration for newly visible chunks.
                    let mut driver = self.driver.borrow_mut();
                    for chunk in &new_chunks {
                        if let Err(e) =
                            driver.register_chunk(chunk.id, chunk.ptr as *const u8, chunk.len)
                        {
                            warn!(
                                ?e,
                                chunk_id = chunk.id,
                                ptr = chunk.ptr,
                                len = chunk.len,
                                "Compatible registration fallback failed"
                            );
                        }
                    }
                }

                // Strict mode stays metadata-only; lazy kernel registration is owned by driver submit path.
                self.processed_chunk_count.set(current_len);
            }

            self.last_seen_epoch.set(target_epoch);
        }
    }

    fn park_and_wait(&self, main_woken: &AtomicBool) {
        let has_pending_tasks =
            !self.queue.borrow().is_empty() || main_woken.load(Ordering::Acquire);

        if has_pending_tasks {
            let mut driver = self.driver.borrow_mut();
            driver.submit_queue().unwrap();
            driver.process_completions();
        } else {
            let mut can_park = true;
            let state = &self.shared.state;

            // 1. Set PARKING
            // This tells remote wakers: "I might sleep soon, so you should probably syscall wake me".
            state.store(PARKING, Ordering::Release);
            trace!("Entered PARKING state");

            // Double check remote queues
            if can_park {
                if self.try_poll_driver_control() {
                    state.store(RUNNING, Ordering::Relaxed);
                    can_park = false;
                } else if let Ok(job) = self.pinned_receiver.try_recv() {
                    self.shared.injected_load.fetch_sub(1, Ordering::Relaxed);
                    self.enqueue_job(job);
                    state.store(RUNNING, Ordering::Relaxed);
                    can_park = false;
                } else if let Ok(task) = self.remote_receiver.try_recv() {
                    self.queue.borrow_mut().push_back(task);
                    state.store(RUNNING, Ordering::Relaxed);
                    can_park = false;
                } else if !self.shared.future_injector.is_empty() || !self.stealable.is_empty() {
                    state.store(RUNNING, Ordering::Relaxed);
                    can_park = false;
                }
            }

            if can_park {
                // 3. Commit PARKED
                state.store(PARKED, Ordering::Release);
                trace!("Entered PARKED state (wait)");

                let mut driver = self.driver.borrow_mut();
                driver.wait().unwrap();
            }

            // Restore RUNNING
            state.store(RUNNING, Ordering::Release);
        }
    }

    /// Run the executor loop indefinitely.
    /// Used by worker threads in the Runtime.
    pub fn run(&self) {
        let spawner = self.registry.as_ref().map(|reg| Spawner::new(reg.clone()));

        // Pass the Mesh context if available
        let context = RuntimeContext::new(
            self.driver_handle(),
            Rc::downgrade(&self.queue),
            Rc::downgrade(&self.stealable),
            spawner,
            self.handle(),
            self.buf_pool.clone(),
        );

        let _guard = crate::runtime::context::enter(context);

        // Auto-Register removed: Pools should be pre-registered (Scheme 1).

        let main_woken = Arc::new(AtomicBool::new(false));

        const BUDGET: usize = 64;

        loop {
            // Check for dynamic memory updates (Pull Model)
            self.check_for_memory_updates();

            if self.shared.shutdown.load(Ordering::Relaxed) {
                debug!("Shutting down LocalExecutor");
                break;
            }

            let mut executed = 0;

            while executed < BUDGET {
                let mut did_work = false;

                // 1. Poll Stealable (Send Tasks - LIFO)
                if let Some(task) = self.stealable.pop() {
                    self.shared.local_load.fetch_sub(1, Ordering::Relaxed);
                    task.run();
                    executed += 1;
                    continue;
                }
                // 2. Poll Main Future (Normally 0 for worker, but blocked for block_on)
                // (Worker loop doesn't have main future, this is generic run loop)

                // 3. Poll Local Queue
                let task = self.queue.borrow_mut().pop_front();
                if let Some(task) = task {
                    self.shared.local_load.fetch_sub(1, Ordering::Relaxed);
                    task.run();
                    executed += 1;
                    continue;
                }

                // 3. Poll Injector
                if self.try_poll_driver_control() {
                    continue;
                }

                if self.try_poll_injector() {
                    continue;
                }

                if self.try_poll_future_injector() {
                    executed += 1;
                    continue;
                }

                // 4. Steal from Registry
                if self.try_steal(executed) {
                    did_work = true;
                }

                if !did_work {
                    break;
                }
            }

            // 5. IO Wait & Park
            self.park_and_wait(&main_woken);
        }
    }

    pub fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future,
    {
        let spawner = self.registry.as_ref().map(|reg| Spawner::new(reg.clone()));

        // Pass the Mesh context if available
        let context = RuntimeContext::new(
            self.driver_handle(),
            Rc::downgrade(&self.queue),
            Rc::downgrade(&self.stealable),
            spawner,
            self.handle(),
            self.buf_pool.clone(),
        );

        let _guard = crate::runtime::context::enter(context);

        // Auto-Register removed.

        let mut pinned_future = Box::pin(future);
        let remote_waker = self.driver.borrow().create_waker();
        let state = Arc::new(AtomicWakerState {
            flag: AtomicBool::new(true),
            remote: remote_waker,
        });
        let waker = waker_from_state(state.clone());
        let mut cx = Context::from_waker(&waker);

        const BUDGET: usize = 64;

        loop {
            // Check for dynamic memory updates (Pull Model)
            self.check_for_memory_updates();

            let mut executed = 0;

            while executed < BUDGET {
                let mut did_work = false;

                // 1. Poll Stealable (Send Tasks - LIFO)
                if let Some(task) = self.stealable.pop() {
                    self.shared.local_load.fetch_sub(1, Ordering::Relaxed);
                    task.run();
                    executed += 1;
                    continue;
                }

                // 2. Poll Main Future
                if state.flag.swap(false, Ordering::AcqRel) {
                    did_work = true;
                    executed += 1;
                    if let Poll::Ready(val) = pinned_future.as_mut().poll(&mut cx) {
                        return val;
                    }
                }

                // 2. Poll Local Queue
                let task = self.queue.borrow_mut().pop_front();
                if let Some(task) = task {
                    self.shared.local_load.fetch_sub(1, Ordering::Relaxed);
                    // task is Arc<Task>, run() takes self: Arc<Task>
                    task.run();
                    executed += 1;
                    continue;
                }

                // 3. Poll Injector
                if self.try_poll_driver_control() {
                    continue;
                }

                if self.try_poll_injector() {
                    continue;
                }

                if self.try_poll_future_injector() {
                    executed += 1;
                    continue;
                }

                // 4. Steal from Registry
                if self.try_steal(executed) {
                    did_work = true;
                }

                if !did_work {
                    break;
                }
            }

            // 5. IO Wait & Park
            self.park_and_wait(&state.flag);
        }
    }
}

impl Drop for LocalExecutor {
    fn drop(&mut self) {
        debug!("Dropping LocalExecutor");
        // Clear the task queue to drop all futures.
        // This explicitly drops tasks (and their buffers/sockets) before the driver is dropped.
        if let Ok(mut queue) = self.queue.try_borrow_mut() {
            queue.clear();
        }

        // Clear stealable queue
        // We have Rc<Worker>, so we can try to unwrap or just loop pop?
        // Rc is shared with context, but loop ends, context guard drops context.
        // It should be safe to just pop.
        while let Some(task) = self.stealable.pop() {
            drop(task);
        }

        // Pump the driver to process cancellations and completions.
        // This is critical for IOCP safety to avoid Heap Corruption from pending cancellations,
        // as the kernel might try to write to buffers that are being freed.
        if let Ok(mut driver) = self.driver.try_borrow_mut() {
            let _ = driver.submit_queue();
            driver.process_completions();
        }
    }
}

// Default implementation removed as LocalExecutor now requires explicit buffer pool.

// ============ Thread-Safe Main Task Waker Implementation ============

struct AtomicWakerState {
    flag: AtomicBool,
    remote: Arc<dyn RemoteWaker>,
}

fn waker_from_state(state: Arc<AtomicWakerState>) -> Waker {
    let ptr = Arc::into_raw(state) as *const ();
    let raw = RawWaker::new(ptr, &ATOMIC_WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}

const ATOMIC_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(atomic_clone, atomic_wake, atomic_wake_by_ref, atomic_drop);

unsafe fn atomic_clone(ptr: *const ()) -> RawWaker {
    let arc = unsafe { Arc::from_raw(ptr as *const AtomicWakerState) };
    std::mem::forget(arc.clone());
    std::mem::forget(arc);
    RawWaker::new(ptr, &ATOMIC_WAKER_VTABLE)
}

unsafe fn atomic_wake(ptr: *const ()) {
    let arc = unsafe { Arc::from_raw(ptr as *const AtomicWakerState) };
    arc.flag.store(true, Ordering::Release);
    let _ = arc.remote.wake();
}

unsafe fn atomic_wake_by_ref(ptr: *const ()) {
    let state = unsafe { &*(ptr as *const AtomicWakerState) };
    state.flag.store(true, Ordering::Release);
    let _ = state.remote.wake();
}

unsafe fn atomic_drop(ptr: *const ()) {
    let _ = unsafe { Arc::from_raw(ptr as *const AtomicWakerState) };
}

#[derive(Clone)]
struct ExecutorRegistrar<D: Driver> {
    driver: std::rc::Weak<RefCell<D>>,
    registry: Option<Arc<ExecutorRegistry>>,
}

impl<D: Driver> BufferRegistrar for ExecutorRegistrar<D> {
    fn register(&self, regions: &[veloq_buf::BufferRegion]) -> std::io::Result<Vec<usize>> {
        let driver_rc = self
            .driver
            .upgrade()
            .ok_or_else(|| std::io::Error::other("Driver dropped"))?;
        let mut driver = driver_rc.borrow_mut();

        let mut indices = Vec::with_capacity(regions.len());
        for (i, region) in regions.iter().enumerate() {
            driver
                .register_chunk(i as u16, region.as_ptr(), region.len())
                .map_err(|e| std::io::Error::other(format!("{e:#}")))?;
            indices.push(i);
        }
        Ok(indices)
    }

    fn resolve_chunk_info(&self, chunk_id: u16) -> Option<veloq_buf::heap::ChunkInfo> {
        self.registry.as_ref().and_then(|reg| {
            let chunks = reg.memory_chunks.read();
            chunks
                .iter()
                .find(|c| c.id == chunk_id)
                .map(|c| veloq_buf::heap::ChunkInfo {
                    id: c.id,
                    ptr: unsafe { std::ptr::NonNull::new_unchecked(c.ptr as *mut u8) },
                    len: unsafe { std::num::NonZeroUsize::new_unchecked(c.len) },
                })
        })
    }
}
