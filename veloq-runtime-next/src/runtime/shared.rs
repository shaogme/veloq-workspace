use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef, TaskHeader};
use crate::utils::ownership::Ownership;
use crate::utils::storage::Storage;
use crate::utils::{AtomicOptionPtr, Deque, Steal};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use super::primitives::{self, EventCount, Parker, Unparker};
use super::context::{CONTEXT, current_worker_id};

pub(crate) struct WorkerQueue {
    pub(crate) local_tx: Sender<LocalTaskRef>,
    pub(crate) remote_tx: Sender<SendTaskRef>,
    pub(crate) local_count: AtomicUsize,
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    pub(crate) send: Deque<SendTaskRef>,
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
    pub(crate) workers: Vec<Arc<WorkerQueue>>,
    pub(crate) next_worker: AtomicUsize,
    pub(crate) shutdown: AtomicBool,
    pub(crate) unparkers: Vec<Unparker>,
    pub(crate) idle_mask: AtomicUsize,
    pub(crate) parker_inners: Vec<Arc<primitives::ParkerInner>>,
    pub(crate) event_count: EventCount,
}

impl RuntimeShared {
    pub(crate) fn new(
        worker_count: NonZeroUsize,
    ) -> (
        Self,
        Vec<Receiver<LocalTaskRef>>,
        Vec<Receiver<SendTaskRef>>,
    ) {
        let worker_count = worker_count.get();
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
    pub fn worker_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.workers.len()).expect("runtime must have at least one worker")
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
