//! Explicit context for the async runtime.
//!
//! This module provides the `RuntimeContext` which is passed to tasks
//! allowing them to spawn new tasks and access runtime resources.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};

use crossbeam_deque::Worker;
use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver::driver::PlatformDriver;
use veloq_driver::op::{IntoPlatformOp, Op, OpSubmitter};

use crate::runtime::executor::ExecutorHandle;
use crate::runtime::executor::Spawner;
use crate::runtime::executor::spawner::pack_job;
use crate::runtime::join::{JoinHandle, LocalJoinHandle};
// Runnable is needed for context methods
use crate::runtime::task::harness::{self, Runnable};
use crate::runtime::task::{SpawnedTask, Task};

thread_local! {
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

/// Sets the thread-local runtime context.
pub(crate) fn enter(context: RuntimeContext) -> ContextGuard {
    CONTEXT.with(|ctx| {
        let prev = ctx.borrow_mut().replace(context);
        ContextGuard { prev }
    })
}

pub(crate) fn is_current_worker(id: usize) -> bool {
    CONTEXT.with(|ctx| {
        if let Some(ctx) = ctx.borrow().as_ref() {
            ctx.handle.id == id
        } else {
            false
        }
    })
}

/// Guard that resets the runtime context when dropped.
pub(crate) struct ContextGuard {
    prev: Option<RuntimeContext>,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = self.prev.take();
        });
    }
}

/// Retrieve the current runtime context.
///
/// # Panics
/// Panics if called outside a runtime context.
pub fn current() -> RuntimeContext {
    try_current().expect("Runtime context not set. Are you running inside an executor?")
}

/// Try to retrieve the current runtime context.
pub fn try_current() -> Option<RuntimeContext> {
    CONTEXT.with(|ctx| ctx.borrow().clone())
}

pub fn submit<T, S>(submitter: &S, op: Op<T>) -> S::Future<T>
where
    S: OpSubmitter,
    T: IntoPlatformOp<PlatformDriver> + 'static,
{
    let driver = CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .expect("Runtime context not set. Are you running inside an executor?")
            .driver
            .clone()
    })
    .upgrade()
    .expect("Runtime driver missing");
    submitter.submit(op, driver)
}

/// Try to allocate a buffer from the current runtime context.
pub fn try_alloc(size: usize) -> Option<FixedBuf> {
    let size = NonZeroUsize::new(size)?;
    CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .expect("Runtime context not set. Are you running inside an executor?")
            .buf_pool
            .alloc(size)
    })
}

/// Allocate a buffer from the current runtime context.
///
/// # Panics
/// Panics when called outside a runtime context or when the buffer pool is full.
pub fn alloc(size: usize) -> FixedBuf {
    if size == 0 {
        panic!("Cannot allocate 0 size");
    }
    try_alloc(size).expect("Buffer pool is full")
}

/// Context passed to runtime tasks.
///
/// This provides access to the executor's facilities like spawning tasks
/// and accessing the IO driver.
#[derive(Clone)]
pub struct RuntimeContext {
    pub(crate) driver: Weak<RefCell<PlatformDriver>>,
    pub(crate) queue: Weak<RefCell<VecDeque<Task>>>,
    pub(crate) spawner: Option<Spawner>,
    pub(crate) handle: ExecutorHandle,
    pub(crate) buf_pool: AnyBufPool,
    pub(crate) stealable: Rc<Worker<Runnable>>,
}

impl RuntimeContext {
    /// Create a new RuntimeContext.
    pub(crate) fn new(
        driver: Weak<RefCell<PlatformDriver>>,
        queue: Weak<RefCell<VecDeque<Task>>>,
        spawner: Option<Spawner>,
        handle: ExecutorHandle,
        buf_pool: AnyBufPool,
        stealable: Rc<Worker<Runnable>>,
    ) -> Self {
        Self {
            driver,
            queue,
            spawner,
            handle,
            buf_pool,
            stealable,
        }
    }

    /// Get the current thread's buffer pool.
    pub fn buf_pool(&self) -> AnyBufPool {
        self.buf_pool.clone()
    }

    /// Spawn a new local task on the current executor.
    ///
    /// Local tasks are not Send and are guaranteed to run on the current thread.
    pub fn spawn_local<F, T>(&self, future: F) -> LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let queue = self.queue.upgrade().expect("executor has been dropped");

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
                self.handle.id,
                self.queue.clone(),
                self.handle.shared.clone(),
            )
        };
        queue.borrow_mut().push_back(task);
        handle
    }

    /// Get a weak reference to the current driver.
    pub fn driver(&self) -> Weak<RefCell<PlatformDriver>> {
        self.driver.clone()
    }

    /// Spawn a new task on a specific worker thread.
    ///
    /// # Panics
    /// Panics if called outside of a runtime context, if the executor registry is missing,
    /// or if the `worker_id` is invalid.
    pub fn spawn_to<F, Output>(&self, worker_id: usize, async_fn: F) -> JoinHandle<Output>
    where
        F: AsyncFnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        let spawner = self
            .spawner
            .as_ref()
            .expect("spawn_to() called on a context without a global spawner");

        let (handle, job) = pack_job(async_fn);

        // Optimization: If spawning to self, just push to local queue
        if self.handle.id() == worker_id {
            let queue = self
                .queue
                .upgrade()
                .expect("executor has been dropped but context remains?");

            // Bind it
            let job = unsafe {
                job.bind(
                    self.handle.id,
                    self.queue.clone(),
                    self.handle.shared.clone(),
                )
            };
            queue.borrow_mut().push_back(job);
            return handle;
        }

        // Fallback (e.g., no mesh or driver dropped)
        spawner.spawn_job_to(job, worker_id);
        handle
    }

    /// Spawn a new stealable task (Send Future) on the runtime.
    ///
    /// The task is initially assigned to a worker (via P2C), but can be stolen by other workers
    /// if the target worker is busy. The task is wrapped in an `ArcTask` (Runnabe).
    ///
    /// # Panics
    /// Panics if called outside of a runtime context.
    pub fn spawn<F, Output>(&self, future: F) -> JoinHandle<Output>
    where
        F: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        // We are on a worker that supports stealing.
        // Create task bound to THIS executor's scheduler.
        let scheduler = self.handle.shared.clone();
        unsafe {
            let (job, handle) = harness::spawn_arc(future, scheduler);
            self.stealable.push(job);
            // Increment load
            self.handle
                .shared
                .injected_load
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            handle
        }
    }
}

/// Yields execution back to the executor, allowing other tasks to run.
///
/// This is useful when you want to give other spawned tasks a chance to execute.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by `yield_now()`.
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.yielded {
            std::task::Poll::Ready(())
        } else {
            self.yielded = true;
            // Wake ourselves so we get polled again
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

/// Get the current thread's buffer pool.
pub fn current_pool() -> Option<AnyBufPool> {
    CONTEXT.with(|ctx| {
        ctx.borrow().as_ref().map(|ctx| ctx.buf_pool.clone())
    })
}

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// Spawning a task enables the task to execute concurrently to other tasks. There is no
/// guarantee that the spawned task will execute to completion. When a task is spawned,
/// it triggers the provided future. The returned `JoinHandle` receives the result of
/// the future when the task completes.
///
/// This function requires the future to be `Send` as it may be executed on a different thread.
///
/// # Panics
///
/// Panics if called outside of a runtime context, or if the current runtime does not support
/// global spawning (missing executor registry).
pub fn spawn<F, Output>(future: F) -> JoinHandle<Output>
where
    F: Future<Output = Output> + Send + 'static,
    Output: Send + 'static,
{
    current().spawn(future)
}

/// Spawns a `!Send` future on the current thread.
///
/// The task is guaranteed to run on the exact same thread that called `spawn_local`.
/// Unlike `spawn`, `spawn_local` allows spawning futures that do not implement `Send`.
///
/// # Panics
///
/// Panics if called outside of a runtime context.
pub fn spawn_local<F>(future: F) -> LocalJoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    current().spawn_local(future)
}

/// Spawns a new asynchronous task on a specific worker thread.
///
/// # Panics
///
/// Panics if called outside of a runtime context, or if the `worker_id` is invalid.
pub fn spawn_to<F, Output>(worker_id: usize, async_fn: F) -> JoinHandle<Output>
where
    F: AsyncFnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    current().spawn_to(worker_id, async_fn)
}
