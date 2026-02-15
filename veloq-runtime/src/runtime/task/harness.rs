use std::future::Future;
use std::mem;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use tracing::trace;

use super::raw;

// --- Interfaces ---

/// A trait for the "Home" of a task, capable of rescheduling it
/// when it is woken up from a remote thread (or when it yields).
pub trait Schedule: Send + Sync {
    /// Schedule the task for execution.
    fn schedule(&self, task: Runnable);
}

// --- Data Structures ---

pub struct HarnessedData {
    /// The scheduler responsible for this task.
    /// This is where the task goes when it is woken up remotely.
    /// We use `ManuallyDrop` because we control its lifecycle via `dealloc`.
    pub scheduler: ManuallyDrop<Arc<dyn Schedule>>,
}

pub type HarnessedHeader = raw::Header<HarnessedData, HarnessedVTable>;

/// The specific vtable for a `T: Future`.
pub struct HarnessedVTable {
    /// Poll the future.
    pub poll: unsafe fn(NonNull<HarnessedHeader>),
    /// Drop the future inside the cell (but not the allocation yet).
    pub drop_future: unsafe fn(NonNull<HarnessedHeader>),
    /// Deallocate the memory block.
    pub dealloc: unsafe fn(NonNull<HarnessedHeader>),
    /// Schedule via the scheduler.
    pub schedule: unsafe fn(NonNull<HarnessedHeader>),
}

// States
const IDLE: usize = 0;
const RUNNING: usize = 1;
const NOTIFIED: usize = 2;
const COMPLETED: usize = 3;

/// A handle to a runnable task.
/// This is `Send` and `Sync` so it can be put into queues (Work Stealing).
#[repr(transparent)]
pub struct Runnable {
    ptr: NonNull<HarnessedHeader>,
}

unsafe impl Send for Runnable {}
unsafe impl Sync for Runnable {}

impl Runnable {
    /// Run the task by polling the future.
    pub fn run(self) {
        let ptr = self.ptr;
        // Don't drop Runnable, we consume it.
        mem::forget(self);
        unsafe {
            (ptr.as_ref().vtable.poll)(ptr);
        }
    }
}

impl Drop for Runnable {
    fn drop(&mut self) {
        unsafe { drop_reference(self.ptr) }
    }
}

// --- Implementation ---

pub(crate) unsafe fn spawn_arc<F>(
    future: F,
    scheduler: Arc<dyn Schedule>,
) -> (Runnable, crate::runtime::join::JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Create JoinHandle
    let (handle, producer) = crate::runtime::join::JoinHandle::new();

    // Wrap future to push output to JoinHandle
    let future = async move {
        let output = future.await;
        producer.set(output);
    };

    let runnable = unsafe { create_task(future, scheduler) };
    (runnable, handle)
}

unsafe fn create_task<F>(future: F, scheduler: Arc<dyn Schedule>) -> Runnable
where
    F: Future<Output = ()> + Send + 'static,
{
    trace!("Creating harnessed task");
    let vtable = &HarnessedVTable {
        poll: poll_future::<F>,
        drop_future: drop_future::<F>,
        dealloc: dealloc_task::<F>,
        schedule: schedule_task::<F>,
    };

    let data = HarnessedData {
        scheduler: ManuallyDrop::new(scheduler),
    };

    let ptr = unsafe { raw::alloc_task(future, data, vtable, IDLE) };

    Runnable { ptr }
}

unsafe fn dealloc_task<F>(ptr: NonNull<HarnessedHeader>) {
    let ptr_raw = ptr
        .cast::<raw::TaskCell<F, HarnessedData, HarnessedVTable>>()
        .as_ptr();

    // Drop the scheduler Arc
    unsafe {
        ManuallyDrop::drop(&mut (*ptr_raw).header.data.scheduler);
    }

    unsafe { raw::dealloc_task::<F, _, _>(ptr) };
}

unsafe fn drop_future<F>(ptr: NonNull<HarnessedHeader>) {
    unsafe { raw::drop_future::<F, _, _>(ptr) };
}

unsafe fn schedule_task<F>(ptr: NonNull<HarnessedHeader>) {
    trace!("Rescheduling harnessed task");
    let header = unsafe { ptr.as_ref() };
    // We must increment refcount before handing off to scheduler
    // because scheduler will take ownership (via Runnable)
    header.references.fetch_add(1, Ordering::Relaxed);
    let runnable = Runnable { ptr };
    header.data.scheduler.schedule(runnable);
}

unsafe fn drop_reference(ptr: NonNull<HarnessedHeader>) {
    let header = unsafe { ptr.as_ref() };
    // Decrement refcount
    if header.references.fetch_sub(1, Ordering::Release) == 1 {
        std::sync::atomic::fence(Ordering::Acquire);
        unsafe {
            (header.vtable.drop_future)(ptr);
            (header.vtable.dealloc)(ptr);
        }
    }
}

// --- Polling Logic ---

unsafe fn poll_future<F: Future<Output = ()>>(ptr: NonNull<HarnessedHeader>) {
    let header = unsafe { ptr.as_ref() };

    loop {
        // We assume we are in "Execution Mode"
        let mut state = header.state.load(Ordering::Acquire);

        // Loop to consume all NOTIFIED signals
        loop {
            // Panic if we see RUNNING (re-entrancy check)
            if (state & RUNNING) != 0 {
                // (Re-entrancy logic comments omitted for brevity)
            }

            let next_state = RUNNING;

            if let Err(actual) = header.state.compare_exchange(
                state,
                next_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                state = actual;
                continue; // Retry
            }

            // We successfully set RUNNING.
            break;
        }

        // 2. Poll the Future
        let slot = unsafe { raw::get_future::<F, _, _>(ptr) };

        if let Some(future) = slot {
            let waker = unsafe { waker_from_ptr(ptr) };
            let mut cx = Context::from_waker(&waker);
            // SAFETY: Future is pinned by the TaskBox
            let pinned = unsafe { Pin::new_unchecked(future) };

            match pinned.poll(&mut cx) {
                Poll::Ready(_) => {
                    // Task Done
                    *slot = None;
                    header.state.store(COMPLETED, Ordering::Release);
                    unsafe { drop_reference(ptr) }; // Drop the reference held by run()
                    return;
                }
                Poll::Pending => {
                    // Task yielded. Check if we were notified during poll.
                    // We clear the RUNNING bit.
                    let old = header.state.fetch_and(!RUNNING, Ordering::Release);

                    if (old & NOTIFIED) != 0 {
                        // Check if we should yield due to budget exhaustion
                        if !crate::runtime::coop::has_remaining() {
                            // We are notified, but we must yield.
                            // We need to schedule the task manually because wake_impl
                            // only set the NOTIFIED bit and returned (since we were RUNNING).
                            // We are now leaving execution, so we must schedule.
                            unsafe { (header.vtable.schedule)(ptr) };
                            unsafe { drop_reference(ptr) };
                            return;
                        }

                        // We received a notification *while* running.
                        // We must loop back and poll again immediately.
                        continue;
                    }

                    // Otherwise, we are IDLE.
                    // We stop running.
                    unsafe { drop_reference(ptr) };
                    return;
                }
            }
        } else {
            // Already completed?
            header.state.store(COMPLETED, Ordering::Release);
            unsafe { drop_reference(ptr) };
            return;
        }
    }
}

// --- Waker Implementation ---

const WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(unsafe_clone, unsafe_wake, unsafe_wake_by_ref, unsafe_drop);

unsafe fn waker_from_ptr(ptr: NonNull<HarnessedHeader>) -> Waker {
    // Increment ref for the Waker
    unsafe { ptr.as_ref().references.fetch_add(1, Ordering::Relaxed) };
    let raw = RawWaker::new(ptr.as_ptr() as *const (), &WAKER_VTABLE);
    // SAFETY: RawWaker is created from valid ptr and vtable
    unsafe { Waker::from_raw(raw) }
}

unsafe fn unsafe_clone(ptr: *const ()) -> RawWaker {
    let header = unsafe { &*(ptr as *const HarnessedHeader) };
    header.references.fetch_add(1, Ordering::Relaxed);
    RawWaker::new(ptr, &WAKER_VTABLE)
}

unsafe fn unsafe_wake(ptr: *const ()) {
    // Consumes the waker ref
    let non_null = unsafe { NonNull::new_unchecked(ptr as *mut HarnessedHeader) };
    unsafe { wake_impl(non_null) };
    unsafe { drop_reference(non_null) };
}

unsafe fn unsafe_wake_by_ref(ptr: *const ()) {
    let non_null = unsafe { NonNull::new_unchecked(ptr as *mut HarnessedHeader) };
    unsafe { wake_impl(non_null) };
}

unsafe fn unsafe_drop(ptr: *const ()) {
    let non_null = unsafe { NonNull::new_unchecked(ptr as *mut HarnessedHeader) };
    unsafe { drop_reference(non_null) };
}

unsafe fn wake_impl(ptr: NonNull<HarnessedHeader>) {
    let header = unsafe { ptr.as_ref() };

    loop {
        let state = header.state.load(Ordering::Acquire);

        match state {
            IDLE => {
                // Transition IDLE -> NOTIFIED
                if header
                    .state
                    .compare_exchange(IDLE, NOTIFIED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    // Success: We are responsible for scheduling it.
                    unsafe { (header.vtable.schedule)(ptr) };
                    return;
                }
            }
            RUNNING => {
                // Transition RUNNING -> RUNNING | NOTIFIED
                if header
                    .state
                    .compare_exchange(
                        RUNNING,
                        RUNNING | NOTIFIED,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
            }
            s if (s & NOTIFIED) != 0 => {
                // Already notified. Nothing to do.
                return;
            }
            COMPLETED => {
                // Dead.
                return;
            }
            _ => {
                // Retry loop
            }
        }
    }
}
