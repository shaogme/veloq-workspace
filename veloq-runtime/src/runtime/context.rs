//! Explicit context for the async runtime.
//!
//! This module provides the `RuntimeContext` which is passed to tasks
//! allowing them to spawn new tasks and access runtime resources.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Weak;

use crossbeam_deque::Worker;
use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_driver::op::{IntoPlatformOp, Op, OpSubmitter};

use crate::runtime::executor::ExecutorHandle;
use crate::runtime::executor::Spawner;
use crate::runtime::executor::spawner::{local_schedule, pack_job};
use crate::runtime::join::{JoinHandle, LocalJoinHandle};
use crate::runtime::task::harness::{self, Runnable};
use crate::runtime::task::{SpawnedTask, Task};

thread_local! {
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), allow(clippy::missing_const_for_thread_local))]
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
    T: IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + 'static,
{
    let driver = CONTEXT
        .with(|ctx| {
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

/// Try to allocate a buffer from the current runtime context pool.
pub fn try_alloc_from_pool(size: NonZeroUsize) -> Option<FixedBuf> {
    CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .expect("Runtime context not set. Are you running inside an executor?")
            .buf_pool
            .alloc(size)
    })
}

/// Try to allocate a buffer from the current runtime context pool.
pub fn try_alloc(size: NonZeroUsize) -> Result<FixedBuf, veloq_buf::AllocError> {
    try_alloc_from_pool(size).map_or_else(|| FixedBuf::alloc_heap(size), Ok)
}

/// Allocate a buffer from the current runtime context.
///
/// If the pool is full, it fallbacks to system heap allocation.
///
/// # Panics
/// Panics when called outside a runtime context or when both pool and heap are exhausted.
pub fn alloc(size: NonZeroUsize) -> FixedBuf {
    try_alloc(size).unwrap()
}

/// Context passed to runtime tasks.
///
/// This provides access to the executor's facilities like spawning tasks
/// and accessing the IO driver.
#[derive(Clone)]
pub struct RuntimeContext {
    pub(crate) driver: Weak<RefCell<PlatformDriver>>,
    pub(crate) queue: Weak<RefCell<VecDeque<Task>>>,
    pub(crate) local_runnable: Weak<Worker<Runnable>>,
    pub(crate) spawner: Option<Spawner>,
    pub(crate) handle: ExecutorHandle,
    pub(crate) buf_pool: AnyBufPool,
}

impl RuntimeContext {
    /// Create a new RuntimeContext.
    pub(crate) fn new(
        driver: Weak<RefCell<PlatformDriver>>,
        queue: Weak<RefCell<VecDeque<Task>>>,
        local_runnable: Weak<Worker<Runnable>>,
        spawner: Option<Spawner>,
        handle: ExecutorHandle,
        buf_pool: AnyBufPool,
    ) -> Self {
        Self {
            driver,
            queue,
            local_runnable,
            spawner,
            handle,
            buf_pool,
        }
    }

    /// Get the current thread's buffer pool.
    pub fn buf_pool(&self) -> AnyBufPool {
        self.buf_pool.clone()
    }

    pub(crate) fn enqueue_local_runnable(&self, task: Runnable) {
        let local_runnable = self
            .local_runnable
            .upgrade()
            .expect("executor runnable queue has been dropped");
        self.handle
            .shared
            .local_load
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        local_runnable.push(task);
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
        self.handle
            .shared
            .local_load
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            self.handle
                .shared
                .local_load
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            queue.borrow_mut().push_back(job);
            return handle;
        }

        // Fallback (e.g., no mesh or driver dropped)
        spawner.spawn_job_to(job, worker_id);
        handle
    }

    /// Spawn a new stealable task (Send Future) on the current worker.
    ///
    /// The task is queued on the local executor and will be polled by the runtime loop.
    /// If you need the legacy "poll once immediately" behavior, use [`RuntimeContext::spawn_eager`].
    ///
    /// # Panics
    /// Panics if called outside of a runtime context.
    pub fn spawn<F, Output>(&self, future: F) -> JoinHandle<Output>
    where
        F: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        let scheduler = local_schedule(self.handle.id(), self.handle.shared.clone());
        let (task, handle) = unsafe { harness::spawn_arc(future, scheduler) };
        self.enqueue_local_runnable(task);
        handle
    }

    /// Spawn a new stealable task (Send Future) and poll it once immediately.
    ///
    /// This preserves the previous eager behavior of [`RuntimeContext::spawn`].
    /// Use this when the task should have a chance to make progress before the
    /// caller continues, for example to submit I/O sooner.
    ///
    /// # Panics
    /// Panics if called outside of a runtime context.
    pub fn spawn_eager<F, Output>(&self, future: F) -> JoinHandle<Output>
    where
        F: Future<Output = Output> + Send + 'static,
        Output: Send + 'static,
    {
        let scheduler = local_schedule(self.handle.id(), self.handle.shared.clone());
        let (task, handle) = unsafe { harness::spawn_arc(future, scheduler) };
        task.run();
        handle
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
    CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|ctx| ctx.buf_pool.clone()))
}

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// Spawning a task enables it to execute concurrently with other tasks. This variant
/// only enqueues the task and does not poll it immediately.
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

/// Spawns a new asynchronous task and polls it once immediately.
///
/// This keeps the previous eager spawning behavior, which can be useful when the
/// spawned task should submit work before the caller continues.
///
/// # Panics
///
/// Panics if called outside of a runtime context, or if the current runtime does not support
/// global spawning (missing executor registry).
pub fn spawn_eager<F, Output>(future: F) -> JoinHandle<Output>
where
    F: Future<Output = Output> + Send + 'static,
    Output: Send + 'static,
{
    current().spawn_eager(future)
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
