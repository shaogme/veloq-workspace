use veloq_std::{
    cell::UnsafeCell,
    fmt,
    mem::ManuallyDrop,
    ptr,
    sync::atomic::{
        AtomicPtr,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

const REGISTERED: usize = 0b01;
const WAKING: usize = 0b10;

// A const NOOP_VTABLE as Waker::noop vtable cannot be accessed in const context.
static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(ptr::null(), &NOOP_VTABLE),
    |_| (),
    |_| (),
    |_| (),
);
const NOOP_PTR: *mut RawWakerVTable = &NOOP_VTABLE as *const RawWakerVTable as *mut RawWakerVTable;

trait TaggedPointerExt {
    fn set(self, tag: usize) -> Self;
    fn unset(self, tag: usize) -> Self;
    fn has(self, tag: usize) -> bool;
}

impl<T> TaggedPointerExt for *mut T {
    #[inline(always)]
    fn set(self, tag: usize) -> Self {
        ((self as usize) | tag) as *mut T
    }
    #[inline(always)]
    fn unset(self, tag: usize) -> Self {
        ((self as usize) & !tag) as *mut T
    }
    #[inline(always)]
    fn has(self, tag: usize) -> bool {
        (self as usize) & tag != 0
    }
}

trait WakerExt {
    fn vtable_ptr(&self) -> *mut RawWakerVTable;
}

impl WakerExt for Waker {
    #[inline(always)]
    fn vtable_ptr(&self) -> *mut RawWakerVTable {
        self.vtable() as *const RawWakerVTable as *mut RawWakerVTable
    }
}

/// A specialized synchronization primitive for task wakeup, optimized for
/// Single-Register (单注册者) and Multi-Wake (多唤醒者) scenarios.
///
/// Unlike `AtomicWaker`, `MwsrWaker` requires that at most one thread/task
/// calls `register` concurrently. This allows for simpler state transitions
/// and better performance. Because of this, `MwsrWaker::register` is marked
/// as `unsafe`.
pub struct MwsrWaker {
    vtable: AtomicPtr<RawWakerVTable>,
    data: UnsafeCell<*const ()>,
}

impl MwsrWaker {
    /// Create an `MwsrWaker`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: UnsafeCell::new(ptr::null()),
        }
    }

    /// Create an `MwsrWaker`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: UnsafeCell::new(ptr::null()),
        }
    }

    /// Registers the waker to be notified on calls to `wake`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there are **no concurrent calls** to `register`.
    /// Calling this function concurrently from multiple threads/tasks is undefined behavior.
    /// However, it is fully safe to call `register` concurrently with `wake`.
    pub unsafe fn register(&self, waker: &Waker) {
        let vtable = self.vtable.load(Acquire);

        // 1. Check for cache hit
        let clean_vtable = vtable.unset(REGISTERED | WAKING);
        let current_data = unsafe { self.data.with(|d| *d) };
        if waker.vtable_ptr() == clean_vtable && waker.data() == current_data {
            if vtable.has(REGISTERED) {
                return;
            }
            if vtable.has(WAKING) {
                waker.wake_by_ref();
                return;
            }
            let target = vtable.set(REGISTERED);
            if self
                .vtable
                .compare_exchange(vtable, target, Release, Acquire)
                .is_err()
            {
                waker.wake_by_ref();
            }
            return;
        }

        // 2. Cache miss. If currently waking, wake and return.
        if vtable.has(WAKING) {
            waker.wake_by_ref();
            return;
        }

        // 3. Clear previous registration if any, to safely overwrite data.
        if vtable.has(REGISTERED) {
            match self
                .vtable
                .compare_exchange(vtable, clean_vtable, Relaxed, Acquire)
            {
                Ok(_) => {
                    // Dropping the previous waker
                    if clean_vtable != NOOP_PTR {
                        let old_waker = unsafe { Waker::new(current_data, &*clean_vtable) };
                        drop(old_waker);
                    }
                }
                Err(_) => {
                    waker.wake_by_ref();
                    return;
                }
            }
        }

        // 4. Clone the new waker to take ownership of it.
        let owned_waker = ManuallyDrop::new(waker.clone());
        unsafe {
            self.data.with_mut(|d| *d = owned_waker.data());
        }
        let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);

        // Try CAS store. If failed, a concurrent take must have flagged it as WAKING after step 3.
        if self
            .vtable
            .compare_exchange(clean_vtable, new_vtable, Release, Acquire)
            .is_err()
        {
            // Reset to clean_vtable, clearing WAKING flag.
            self.vtable.store(clean_vtable, Release);
            let rollback_data = if vtable.has(REGISTERED) {
                ptr::null()
            } else {
                current_data
            };
            unsafe {
                self.data.with_mut(|d| *d = rollback_data);
            }
            let raw_waker = ManuallyDrop::into_inner(owned_waker);
            raw_waker.wake();
        }
    }

    /// Calls `wake` on the last `Waker` passed to `register`.
    ///
    /// If `register` has not been called yet, then this does nothing.
    pub fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    /// Returns the last `Waker` passed to `register`, so that the user can wake it.
    ///
    /// If a waker has not been registered, this returns `None`.
    pub fn take(&self) -> Option<Waker> {
        let mut vtable = self.vtable.load(Relaxed);
        loop {
            if !vtable.has(REGISTERED) {
                if vtable.has(WAKING) {
                    return None;
                }
                // On concurrent take without a waker, try to flag it as WAKING.
                match self
                    .vtable
                    .compare_exchange(vtable, vtable.set(WAKING), AcqRel, Acquire)
                {
                    Ok(_) => {
                        // Successfully flagged as WAKING. Since there's no registered waker,
                        // we must release the WAKING lock before returning.
                        let _ = self.vtable.compare_exchange(
                            vtable.set(WAKING),
                            vtable,
                            Release,
                            Relaxed,
                        );
                        return None;
                    }
                    Err(actual) => vtable = actual,
                }
            } else {
                if vtable.has(WAKING) {
                    return None;
                }
                match self
                    .vtable
                    .compare_exchange(vtable, vtable.set(WAKING), AcqRel, Acquire)
                {
                    Ok(_) => {
                        let clean = vtable.unset(REGISTERED | WAKING);
                        let data = unsafe {
                            let d = self.data.with(|d| *d);
                            self.data.with_mut(|d| *d = ptr::null());
                            d
                        };
                        // Reset to NOOP_PTR to avoid stale references
                        // and ensure subsequent registrations hit the cache miss path.
                        self.vtable.store(NOOP_PTR, Release);
                        let waker = unsafe { Waker::new(data, &*clean) };
                        return Some(waker);
                    }
                    Err(actual) => {
                        vtable = actual;
                    }
                }
            }
        }
    }
}

impl Drop for MwsrWaker {
    fn drop(&mut self) {
        let vtable = self.vtable.load(Relaxed);
        if vtable.has(REGISTERED) && !vtable.has(WAKING) {
            let clean = vtable.unset(REGISTERED | WAKING);
            let data = unsafe { self.data.with(|d| *d) };
            if clean != NOOP_PTR {
                let waker = unsafe { Waker::new(data, &*clean) };
                drop(waker);
            }
        }
    }
}

impl Default for MwsrWaker {
    fn default() -> Self {
        MwsrWaker::new()
    }
}

impl fmt::Debug for MwsrWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MwsrWaker")
    }
}

unsafe impl Send for MwsrWaker {}
unsafe impl Sync for MwsrWaker {}
