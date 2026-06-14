use crossbeam_queue::SegQueue;
use parking_lot::{Condvar, Mutex};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BlockingPoolConfig {
    pub core_threads: usize,
    pub max_threads: usize,
    pub queue_capacity: usize,
    pub keep_alive: Duration,
}

impl Default for BlockingPoolConfig {
    fn default() -> Self {
        Self {
            core_threads: 16,
            max_threads: 512,
            queue_capacity: 10000,
            keep_alive: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum ThreadPoolError {
    Overloaded,
}

pub enum BlockingTask {
    /// A generic closure to run in the blocking pool.
    Fn(Box<dyn FnOnce() + Send>),
}

impl BlockingTask {
    pub fn run(self) {
        match self {
            BlockingTask::Fn(f) => f(),
        }
    }
}

pub static BLOCKING_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Initialize the blocking pool with the given config
pub fn init_blocking_pool(config: BlockingPoolConfig) -> &'static ThreadPool {
    BLOCKING_POOL.get_or_init(|| {
        ThreadPool::new(
            config.core_threads,
            config.max_threads,
            config.queue_capacity,
            config.keep_alive,
        )
    })
}

/// Get the global blocking thread pool instance.
/// Initializes it if it hasn't been initialized yet.
pub fn get_blocking_pool() -> &'static ThreadPool {
    BLOCKING_POOL.get().expect("Blocking pool not initialized")
}

struct PoolState {
    queue: SegQueue<BlockingTask>,
    task_count: AtomicUsize,
    sleeper_lock: Mutex<()>,
    cond: Condvar,
    active_workers: AtomicUsize,
    idle_workers: AtomicUsize,
}

#[derive(Clone)]
pub struct ThreadPool {
    state: Arc<PoolState>,
    core_threads: usize,
    max_threads: usize,
    queue_capacity: usize,
    keep_alive: Duration,
}

struct WorkerGuard {
    state: Arc<PoolState>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.state.active_workers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ThreadPool {
    pub fn new(
        core_threads: usize,
        max_threads: usize,
        queue_capacity: usize,
        keep_alive: Duration,
    ) -> Self {
        assert!(core_threads <= max_threads);

        let state = Arc::new(PoolState {
            queue: SegQueue::new(),
            task_count: AtomicUsize::new(0),
            sleeper_lock: Mutex::new(()),
            cond: Condvar::new(),
            active_workers: AtomicUsize::new(0),
            idle_workers: AtomicUsize::new(0),
        });

        Self {
            state,
            core_threads,
            max_threads,
            queue_capacity,
            keep_alive,
        }
    }

    pub fn execute(&self, task: BlockingTask) -> Result<(), ThreadPoolError> {
        let state = &self.state;

        // 1. If there are idle workers, push and notify
        if state.idle_workers.load(Ordering::SeqCst) > 0 {
            state.task_count.fetch_add(1, Ordering::SeqCst);
            state.queue.push(task);

            // Notify one worker
            let _guard = state.sleeper_lock.lock();
            state.cond.notify_one();
            return Ok(());
        }

        // 2. Try to spawn a new worker if under limit
        let active = state.active_workers.load(Ordering::SeqCst);
        if active < self.max_threads {
            state.active_workers.fetch_add(1, Ordering::SeqCst);
            let state_clone = state.clone();
            let keep_alive = self.keep_alive;
            let core_threads = self.core_threads;

            state.task_count.fetch_add(1, Ordering::SeqCst);
            state.queue.push(task);

            let _ = thread::Builder::new()
                .name("veloq-blocking-worker".into())
                .spawn(move || Self::worker_loop(state_clone, keep_alive, core_threads));

            return Ok(());
        }

        // 3. Queue if capable (using atomic count for O(1) check)
        let count = state.task_count.load(Ordering::SeqCst);
        if count < self.queue_capacity {
            state.task_count.fetch_add(1, Ordering::SeqCst);
            state.queue.push(task);

            // Need to notify? Maybe a worker became idle just now
            if state.idle_workers.load(Ordering::SeqCst) > 0 {
                let _guard = state.sleeper_lock.lock();
                state.cond.notify_one();
            }
            Ok(())
        } else {
            Err(ThreadPoolError::Overloaded)
        }
    }

    fn worker_loop(state: Arc<PoolState>, keep_alive: Duration, core_threads: usize) {
        let _guard = WorkerGuard {
            state: state.clone(),
        };

        loop {
            // Task popping logic
            let task = loop {
                // 1. Try to pop from queue (Fast path)
                if let Some(task) = state.queue.pop() {
                    state.task_count.fetch_sub(1, Ordering::SeqCst);
                    break Some(task);
                }

                // 2. Queue empty: Prepare to sleep
                state.idle_workers.fetch_add(1, Ordering::SeqCst);
                let mut guard = state.sleeper_lock.lock();

                // 3. Double check queue under lock to avoid race conditions
                if let Some(task) = state.queue.pop() {
                    drop(guard); // Unlock before running
                    state.idle_workers.fetch_sub(1, Ordering::SeqCst);
                    state.task_count.fetch_sub(1, Ordering::SeqCst);
                    break Some(task);
                }

                // 4. Wait for signal
                let result = state.cond.wait_for(&mut guard, keep_alive);
                drop(guard);
                state.idle_workers.fetch_sub(1, Ordering::SeqCst);

                // 5. If timed out and still empty, maybe exit
                if result.timed_out()
                    && state.queue.is_empty()
                    && state.active_workers.load(Ordering::SeqCst) > core_threads
                {
                    return;
                }
                // Loop back to try pop or sleep again
            };

            if let Some(task) = task {
                let _ = catch_unwind(AssertUnwindSafe(|| task.run()));
            }
        }
    }
}
