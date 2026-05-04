mod primitives;

use crate::utils::{Deque, Steal};
pub use primitives::{
    EventCount, GenericCancellationToken, GenericCancellationTokenInner, Parker, Signal, Unparker,
    create_waker,
};

use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef, TaskHeader};
use crate::utils::ownership::{ArcOwnership, Ownership};
use crate::utils::storage::{AtomicStorage, Storage};
use crate::utils::{AtomicOptionPtr, FastRand};
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll};
use std::thread;

pub struct RuntimeContext {
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) worker_id: usize,
    pub(crate) local_rx: Receiver<LocalTaskRef>,
    pub(crate) remote_rx: Receiver<SendTaskRef>,
    pub(crate) rand: RefCell<FastRand>,
}

thread_local! {
    static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

pub fn current_worker_id() -> usize {
    CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .map(|c| c.worker_id)
            .unwrap_or(usize::MAX)
    })
}

pub fn set_current_runtime_context(context: RuntimeContext) {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(context);
    });
}

pub fn clear_current_runtime_context() {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = None;
    });
}

pub(crate) fn with_current_runtime<R>(f: impl FnOnce(&Arc<RuntimeShared>) -> R) -> Option<R> {
    CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| f(&c.shared)))
}

struct WorkerQueue {
    local_tx: Sender<LocalTaskRef>,
    remote_tx: Sender<SendTaskRef>,
    local_count: AtomicUsize,
    lifo: AtomicOptionPtr<TaskHeader>,
    send: Deque<SendTaskRef>,
}

impl WorkerQueue {
    fn new(local_tx: Sender<LocalTaskRef>, remote_tx: Sender<SendTaskRef>) -> Self {
        Self {
            local_tx,
            remote_tx,
            local_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            send: Deque::new(256),
        }
    }
}

pub struct RuntimeShared {
    workers: Vec<Arc<WorkerQueue>>,
    next_worker: AtomicUsize,
    shutdown: AtomicBool,
    unparkers: Vec<Unparker>,
    idle_mask: AtomicUsize,
    parker_inners: Vec<Arc<primitives::ParkerInner>>,
    event_count: EventCount,
}

impl RuntimeShared {
    fn new(
        worker_count: usize,
    ) -> (
        Self,
        Vec<Receiver<LocalTaskRef>>,
        Vec<Receiver<SendTaskRef>>,
    ) {
        let worker_count = worker_count.max(1);
        assert!(
            worker_count <= usize::BITS as usize,
            "Worker count exceeds bitmask capacity ({})",
            usize::BITS
        );
        let mut unparkers = Vec::with_capacity(worker_count);
        let mut parker_inners = Vec::with_capacity(worker_count);
        let mut local_receivers = Vec::with_capacity(worker_count);
        let mut remote_receivers = Vec::with_capacity(worker_count);
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let inner = Arc::new(primitives::ParkerInner {
                state: AtomicU32::new(0),
            });
            unparkers.push(Unparker::from_inner(inner.clone()));
            parker_inners.push(inner);

            let (ltx, lrx) = mpsc::channel();
            let (rtx, rrx) = mpsc::channel();
            local_receivers.push(lrx);
            remote_receivers.push(rrx);
            workers.push(Arc::new(WorkerQueue::new(ltx, rtx)));
        }

        (
            Self {
                workers,
                next_worker: AtomicUsize::new(0),
                shutdown: AtomicBool::new(false),
                unparkers,
                idle_mask: AtomicUsize::new(0),
                parker_inners,
                event_count: EventCount::new(),
            },
            local_receivers,
            remote_receivers,
        )
    }

    pub fn choose_worker(&self) -> usize {
        let n = self.workers.len();
        if n == 1 {
            return 0;
        }
        self.next_worker.fetch_add(1, Ordering::Relaxed) % n
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            let _ = worker.local_tx.send(task);
            self.event_count.notify();
            self.unparkers[worker_id].unpark();
        }
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().is_completed() {
            return;
        }
        let worker_id = worker_id % self.workers.len();
        if task.header().try_mark_queued() {
            self.event_count.notify();
            let current = current_worker_id();
            let worker = &self.workers[worker_id];

            if current == worker_id {
                // 尝试放入 LIFO Slot (MPSC)
                let header_ptr = task.header() as *const _ as *mut _;
                if worker
                    .lifo
                    .compare_exchange(
                        None,
                        NonNull::new(header_ptr),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.unparkers[worker_id].unpark();
                    return;
                }

                if worker.send.push(task).is_ok() {
                    self.unparkers[worker_id].unpark();
                    return;
                }
            }
            let _ = worker.remote_tx.send(task);
            self.unparkers[worker_id].unpark();
        }
    }

    fn pop_local(&self, worker_id: usize, rx: &Receiver<LocalTaskRef>) -> Option<LocalTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.workers[worker_id]
                .local_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn pop_send(&self, worker_id: usize) -> Option<SendTaskRef> {
        let worker = &self.workers[worker_id];
        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            return Some(unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        worker.send.pop()
    }

    fn steal_send(&self, thief_id: usize) -> Option<SendTaskRef> {
        let thief_queue = &self.workers[thief_id].send;
        let num_workers = self.workers.len();
        if num_workers <= 1 {
            return None;
        }

        let start = CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .map(|c| c.rand.borrow_mut().next_u32(num_workers as u32) as usize)
                .unwrap_or(0)
        });

        for i in 0..num_workers {
            let victim = (start + i) % num_workers;
            if victim == thief_id {
                continue;
            }
            match self.workers[victim].send.steal_batch(thief_queue) {
                Steal::Success(task) => return Some(task),
                Steal::Retry => return self.steal_send(thief_id),
                Steal::Empty => continue,
            }
        }
        None
    }

    fn poll_local_task(&self, worker_id: usize, task: LocalTaskRef) {
        task.header().clear_queued();
        let _ = task.poll_task(worker_id);
    }

    fn poll_send_task(&self, worker_id: usize, task: SendTaskRef) {
        task.header().clear_queued();
        let _ = task.poll_task(worker_id);
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for unparker in &self.unparkers {
            unparker.unpark();
        }
    }

    pub fn drive_worker<S: Storage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let worker_id = current_worker_id();
        CONTEXT.with(|ctx| {
            let ctx = ctx.borrow();
            let ctx = ctx.as_ref().expect("runtime context missing");
            let mut tick = 0u32;
            let mut injector_check_interval = 61;

            while !self.shutdown.load(Ordering::Acquire)
                && completion.map(|c| !c.is_done()).unwrap_or(true)
            {
                let mut progressed = false;
                tick = tick.wrapping_add(1);

                if let Some(task) = self.pop_send(worker_id) {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed {
                    if let Some(task) = self.pop_local(worker_id, &ctx.local_rx) {
                        self.poll_local_task(worker_id, task);
                        progressed = true;
                    }
                }

                if !progressed {
                    if let Some(task) = ctx.remote_rx.try_recv().ok() {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                    }
                }

                if !progressed || tick % injector_check_interval == 0 {
                    if let Some(task) = self.steal_send(worker_id) {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        injector_check_interval = 61;
                    } else if tick % injector_check_interval == 0 {
                        injector_check_interval = (injector_check_interval * 2 + 1).min(1023);
                    }
                }

                if progressed {
                    continue;
                }

                for _ in 0..64 {
                    if let Some(task) = self.steal_send(worker_id) {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        break;
                    }
                    if let Some(task) = ctx.remote_rx.try_recv().ok() {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        break;
                    }
                    std::hint::spin_loop();
                }

                if progressed {
                    continue;
                }

                let seq = self.event_count.load();
                self.idle_mask.fetch_or(1 << worker_id, Ordering::AcqRel);

                if self.event_count.load() != seq
                    || self.has_work(worker_id)
                    || self.shutdown.load(Ordering::Acquire)
                    || completion.map(|c| c.is_done()).unwrap_or(false)
                {
                    self.idle_mask
                        .fetch_and(!(1 << worker_id), Ordering::AcqRel);
                    continue;
                }

                if completion.is_some() {
                    self.idle_mask
                        .fetch_and(!(1 << worker_id), Ordering::AcqRel);
                    std::thread::yield_now();
                    continue;
                }

                let parker = Parker::from_inner(self.parker_inners[worker_id].clone());
                parker.park();

                self.idle_mask
                    .fetch_and(!(1 << worker_id), Ordering::AcqRel);
            }
        });
    }

    fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.workers[worker_id];
        !worker.send.is_empty() || worker.local_count.load(Ordering::Acquire) > 0
    }
}

pub struct Runtime {
    shared: Arc<RuntimeShared>,
    local_receivers: Vec<Receiver<LocalTaskRef>>,
    remote_receivers: Vec<Receiver<SendTaskRef>>,
}

impl Runtime {
    pub fn new(worker_count: usize) -> Self {
        let (shared, local_receivers, remote_receivers) = RuntimeShared::new(worker_count);
        let shared = Arc::new(shared);
        Self {
            shared,
            local_receivers,
            remote_receivers,
        }
    }

    pub fn block_on<F: Future>(self, fut: F) -> F::Output {
        let Runtime {
            shared,
            local_receivers,
            remote_receivers,
        } = self;
        shared.shutdown.store(false, Ordering::Release);
        let mut local_receivers = local_receivers;
        let mut remote_receivers = remote_receivers;
        let worker_count = local_receivers.len();

        let mut fut = fut;
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        let signal = Arc::new(Signal::new(true));
        let waker = create_waker(signal.clone());
        let mut cx = Context::from_waker(&waker);

        thread::scope(|scope| {
            struct ShutdownGuard(Arc<RuntimeShared>);
            impl Drop for ShutdownGuard {
                fn drop(&mut self) {
                    self.0.shutdown.store(true, Ordering::Release);
                    for unparker in &self.0.unparkers {
                        unparker.unpark();
                    }
                }
            }
            let _guard = ShutdownGuard(shared.clone());

            for worker_id in (1..worker_count).rev() {
                let lrx = local_receivers.pop().expect("Receiver missing for worker");
                let rrx = remote_receivers.pop().expect("Receiver missing for worker");
                let runtime = shared.clone();
                scope.spawn(move || {
                    set_current_runtime_context(RuntimeContext {
                        shared: runtime.clone(),
                        worker_id,
                        local_rx: lrx,
                        remote_rx: rrx,
                        rand: RefCell::new(FastRand::new(worker_id as u64)),
                    });
                    runtime.drive_worker::<AtomicStorage, ArcOwnership>(None);
                    clear_current_runtime_context();
                });
            }

            let lrx0 = local_receivers
                .pop()
                .expect("Receiver missing for worker 0");
            let rrx0 = remote_receivers
                .pop()
                .expect("Receiver missing for worker 0");
            set_current_runtime_context(RuntimeContext {
                shared: shared.clone(),
                worker_id: 0,
                local_rx: lrx0,
                remote_rx: rrx0,
                rand: RefCell::new(FastRand::new(0)),
            });

            loop {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => {
                        clear_current_runtime_context();
                        return res;
                    }
                    Poll::Pending => signal.wait(),
                }
            }
        })
    }
}

impl Default for Runtime {
    fn default() -> Self {
        let worker_count = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self::new(worker_count)
    }
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }
}

#[derive(Default)]
pub struct RuntimeBuilder {
    worker_count: Option<usize>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_count(mut self, count: usize) -> Self {
        self.worker_count = Some(count);
        self
    }

    pub fn build(self) -> Runtime {
        let count = self.worker_count.unwrap_or_else(|| {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });
        Runtime::new(count)
    }
}
