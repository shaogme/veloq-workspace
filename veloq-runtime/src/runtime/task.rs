use std::cell::UnsafeCell;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::runtime::executor::ExecutorShared;
use tracing::trace;
use veloq_driver::driver::RemoteWaker;

pub mod harness;
pub mod raw;
pub mod spawned;

pub use spawned::SpawnedTask;

/// The state of the Task in its lifecycle.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// Created, waiting in a queue (injector or local). Context not yet bound.
    Spawned = 0,
    /// Bound to a worker, ready to be polled.
    Bound = 1,
    /// Currently being polled.
    Running = 2,
    /// Completed or Dropped.
    Dead = 3,
}

/// A handle to a bound task (ready to run).
#[repr(transparent)]
pub struct Task {
    ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>,
}

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

// Data specific to Pinned Tasks
#[repr(C)]
pub(crate) struct PinnedData {
    // --- Late-Bound Context ---
    // Mutable via UnsafeCell, synchronized by lifecycle transitions (Spawned -> Bound).
    pub owner_id: UnsafeCell<usize>,

    // We use a Weak pointer to the queue inside UnsafeCell.
    pub queue:
        UnsafeCell<Option<std::rc::Weak<std::cell::RefCell<std::collections::VecDeque<Task>>>>>,

    pub shared: UnsafeCell<Option<Arc<ExecutorShared>>>,
}

// VTable specific to Pinned Tasks
// Note: We don't use the `extra` field for any function pointers here,
// strictly relying on the standard 3 (poll, drop_future, dealloc).
// But we need a type to satisfy the generic V.
pub(crate) struct PinnedVTable {
    pub poll: unsafe fn(NonNull<raw::Header<PinnedData, PinnedVTable>>),
    pub drop_future: unsafe fn(NonNull<raw::Header<PinnedData, PinnedVTable>>),
    pub dealloc: unsafe fn(NonNull<raw::Header<PinnedData, PinnedVTable>>),
}

impl Task {
    /// Run the task.
    /// Creates a Waker and polls the future.
    /// Consumes the Task handle (ownership transfer to the poll/waker cycle).
    pub(crate) fn run(self) {
        let ptr = self.ptr;
        mem::forget(self); // Do not run Drop for Task, referencing is handed off to poll

        unsafe {
            (ptr.as_ref().vtable.poll)(ptr);
        }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        unsafe { drop_reference(self.ptr) }
    }
}

// --- Implementation Callbacks (Public for spawned.rs) ---

pub(crate) unsafe fn poll_future_impl<F: Future<Output = ()>>(
    ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>,
) {
    let header = unsafe { ptr.as_ref() };

    // CAS Loop to transition from Bound -> Running
    loop {
        let state = header.state.load(Ordering::Acquire);

        if state == Lifecycle::Dead as usize {
            // Already dead, drop the executed reference
            unsafe { drop_reference(ptr) };
            return;
        }

        if state == Lifecycle::Running as usize {
            // Contention: Task is already running on another thread.
            // We must reschedule it to run later.
            // We transfer the ownership of this reference (ptr) back to the queue.
            let queue_ptr = header.data.queue.get();
            if let Some(weak_queue) = unsafe { &*queue_ptr }
                && let Some(queue) = weak_queue.upgrade()
            {
                let shared_ptr = header.data.shared.get();
                if let Some(shared) = unsafe { &*shared_ptr } {
                    shared.local_load.fetch_add(1, Ordering::Relaxed);
                }
                let task = Task { ptr };
                queue.borrow_mut().push_back(task);
                return;
            }
            // If queue is gone, we have to drop.
            unsafe { drop_reference(ptr) };
            return;
        }

        // Try to lock (Bound -> Running)
        if header
            .state
            .compare_exchange(
                state,
                Lifecycle::Running as usize,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            break;
        }
        // CAS failed, retry
    }

    // --- Critical Section ---
    // Use raw helper to access generic future
    let slot = unsafe { raw::get_future::<F, _, _>(ptr) };

    if let Some(future) = slot {
        let waker = unsafe { waker_from_ptr(ptr) };
        let mut cx = Context::from_waker(&waker);
        let pinned = unsafe { Pin::new_unchecked(future) };

        match pinned.poll(&mut cx) {
            Poll::Ready(_) => {
                *slot = None;
                header
                    .state
                    .store(Lifecycle::Dead as usize, Ordering::Release);
            }
            Poll::Pending => {
                header
                    .state
                    .store(Lifecycle::Bound as usize, Ordering::Release);
            }
        }
    } else {
        header
            .state
            .store(Lifecycle::Dead as usize, Ordering::Release);
    }

    unsafe { drop_reference(ptr) };
}

pub(crate) unsafe fn drop_future_impl<F>(ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>) {
    unsafe { raw::drop_future::<F, _, _>(ptr) }
}

pub(crate) unsafe fn dealloc_task_impl<F>(ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>) {
    unsafe { raw::dealloc_task::<F, _, _>(ptr) }
}

// --- Helper Functions ---

pub(crate) unsafe fn drop_reference(ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>) {
    unsafe {
        let header = ptr.as_ref();
        if header.references.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Ordering::Acquire);

            (header.vtable.drop_future)(ptr);

            let shared_ptr = header.data.shared.get();
            if let Some(shared) = (*shared_ptr).take() {
                drop(shared);
            }

            let queue_ptr = header.data.queue.get();
            if let Some(queue) = (*queue_ptr).take() {
                drop(queue);
            }

            (header.vtable.dealloc)(ptr);
        }
    }
}

// --- Waker ---

const WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(unsafe_clone, unsafe_wake, unsafe_wake_by_ref, unsafe_drop);

unsafe fn waker_from_ptr(ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>) -> Waker {
    unsafe {
        ptr.as_ref().references.fetch_add(1, Ordering::Relaxed);
        let raw = RawWaker::new(ptr.as_ptr() as *const (), &WAKER_VTABLE);
        Waker::from_raw(raw)
    }
}

unsafe fn unsafe_clone(ptr: *const ()) -> RawWaker {
    unsafe {
        let header = &*(ptr as *const raw::Header<PinnedData, PinnedVTable>);
        header.references.fetch_add(1, Ordering::Relaxed);
        RawWaker::new(ptr, &WAKER_VTABLE)
    }
}

unsafe fn unsafe_wake(ptr: *const ()) {
    unsafe {
        let non_null = NonNull::new_unchecked(ptr as *mut raw::Header<PinnedData, PinnedVTable>);
        wake_task(non_null);
        unsafe_drop(ptr);
    }
}

unsafe fn unsafe_wake_by_ref(ptr: *const ()) {
    unsafe {
        let non_null = NonNull::new_unchecked(ptr as *mut raw::Header<PinnedData, PinnedVTable>);
        wake_task(non_null);
    }
}

unsafe fn unsafe_drop(ptr: *const ()) {
    unsafe {
        let ptr = NonNull::new_unchecked(ptr as *mut raw::Header<PinnedData, PinnedVTable>);
        drop_reference(ptr);
    }
}

unsafe fn wake_task(ptr: NonNull<raw::Header<PinnedData, PinnedVTable>>) {
    let header = unsafe { ptr.as_ref() };

    // If running logic ensures binding, we can safely access owner_id.
    let owner_id = unsafe { *header.data.owner_id.get() };
    trace!("Waking task, owner={}", owner_id);

    // Check if handling locally
    let is_local = crate::runtime::context::is_current_worker(owner_id);

    if is_local {
        // Push to local queue
        let queue_ptr = header.data.queue.get();
        if let Some(weak_queue) = unsafe { &*queue_ptr }
            && let Some(queue) = weak_queue.upgrade()
        {
            // Here we need to reconstruct the Task struct to push it into the queue.
            let shared_ptr = header.data.shared.get();
            if let Some(shared) = unsafe { &*shared_ptr } {
                shared.local_load.fetch_add(1, Ordering::Relaxed);
            }
            header.references.fetch_add(1, Ordering::Relaxed);
            let task = Task { ptr };
            queue.borrow_mut().push_back(task);
        }
    } else {
        // Remote Wake
        let shared_ptr = header.data.shared.get();
        if let Some(shared) = unsafe { &*shared_ptr } {
            // Need to push to remote_queue.
            header.references.fetch_add(1, Ordering::Relaxed);
            let task = Task { ptr };

            let _ = shared.remote_queue.send(task);

            // Optimization: Only wake the driver if the executor is parking or parked.
            let state = shared.state.load(Ordering::Acquire);
            if state != crate::runtime::executor::RUNNING {
                let _ = shared.waker.wake();
            }
        }
    }
}
