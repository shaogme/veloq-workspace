use veloq_std::{
    fmt,
    mem::ManuallyDrop,
    ptr,
    sync::atomic::{
        AtomicPtr,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::{RawWakerVTable, Waker},
};

use crate::common::{
    NOOP_PTR, REGISTERED, REGISTERING, TAG_MASK, TaggedPointerExt, WAKING, WakerExt,
};

/// A specialized synchronization primitive for task wakeup, optimized for
/// Single-Register (单注册者) and Multi-Wake (多唤醒者) scenarios.
///
/// Unlike `AtomicWaker`, `MwsrWaker` requires that at most one thread/task
/// calls `register` concurrently. This allows for simpler state transitions
/// and better performance. Because of this, `MwsrWaker::register` is marked
/// as `unsafe`.
pub struct MwsrWaker {
    vtable: AtomicPtr<RawWakerVTable>,
    data: AtomicPtr<()>,
    cached_vtable: AtomicPtr<RawWakerVTable>,
    cached_data: AtomicPtr<()>,
}

impl MwsrWaker {
    /// Create an `MwsrWaker`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
            cached_vtable: AtomicPtr::new(ptr::null_mut()),
            cached_data: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Create an `MwsrWaker`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
            cached_vtable: AtomicPtr::new(ptr::null_mut()),
            cached_data: AtomicPtr::new(ptr::null_mut()),
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
        let waker_vtable = waker.vtable_ptr();
        let waker_data = waker.data();

        // Quick check of the local cache
        let cached_vtable = self.cached_vtable.load(Relaxed);
        let cached_data = self.cached_data.load(Relaxed);

        if waker_vtable == cached_vtable && waker_data == cached_data {
            let expected_vtable = waker_vtable.set(REGISTERED);
            if self.vtable.load(Acquire) == expected_vtable {
                return;
            }
        }

        // Cache Miss path
        let mut vtable = self.vtable.load(Acquire);

        loop {
            let tag = vtable.tag();

            // If currently waking, to avoid losing the wakeup, we must wake the new waker immediately and return
            if tag == WAKING {
                waker.wake_by_ref();
                return;
            }

            if tag == REGISTERING {
                core::hint::spin_loop();
                vtable = self.vtable.load(Acquire);
                continue;
            }

            if tag == 0 {
                // If it is NOOP_PTR, we can safely publish the registration directly because take() has no effect at this point
                if vtable == NOOP_PTR {
                    let owned_waker = ManuallyDrop::new(waker.clone());
                    self.data.store(owned_waker.data() as *mut (), Release);
                    let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);
                    self.vtable.store(new_vtable, Release);

                    // Successfully registered, update local cache
                    self.cached_vtable.store(waker_vtable, Relaxed);
                    self.cached_data.store(waker_data as *mut (), Relaxed);
                    return;
                }
            }

            if tag == REGISTERED {
                // Cache mismatch has been checked outside, so no double check is needed here; modify the state directly
                let clean_vtable = vtable.unset(TAG_MASK);

                // Try to acquire the REGISTERING state lock
                let registering_vtable = vtable.set(REGISTERING);
                match self
                    .vtable
                    .compare_exchange(vtable, registering_vtable, AcqRel, Acquire)
                {
                    Ok(_) => {
                        let current_data = self.data.load(Acquire) as *const ();

                        // Drop the old waker
                        if clean_vtable != NOOP_PTR {
                            let old_waker = unsafe { Waker::new(current_data, &*clean_vtable) };
                            drop(old_waker);
                        }

                        // Copy and store the new waker
                        let owned_waker = ManuallyDrop::new(waker.clone());
                        self.data.store(owned_waker.data() as *mut (), Release);
                        let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);

                        // Try to release lock and publish new registration
                        match self.vtable.compare_exchange(
                            registering_vtable,
                            new_vtable,
                            Release,
                            Acquire,
                        ) {
                            Ok(_) => {
                                // Successfully registered, update local cache
                                self.cached_vtable.store(waker_vtable, Relaxed);
                                self.cached_data.store(waker_data as *mut (), Relaxed);
                                return;
                            }
                            Err(actual) => {
                                debug_assert_eq!(actual.tag(), WAKING);
                                self.vtable.store(NOOP_PTR, Release);
                                self.data.store(ptr::null_mut(), Release);

                                // Registration failed (concurrent wakeup), clear local cache
                                self.cached_vtable.store(ptr::null_mut(), Relaxed);
                                self.cached_data.store(ptr::null_mut(), Relaxed);

                                let raw_waker = ManuallyDrop::into_inner(owned_waker);
                                raw_waker.wake();
                                return;
                            }
                        }
                    }
                    Err(actual) => {
                        vtable = actual;
                        continue;
                    }
                }
            }

            core::hint::spin_loop();
            vtable = self.vtable.load(Acquire);
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
            let tag = vtable.tag();
            if tag == REGISTERING {
                let waking_vtable = vtable.set(WAKING);
                match self
                    .vtable
                    .compare_exchange(vtable, waking_vtable, AcqRel, Acquire)
                {
                    Ok(_) => return None,
                    Err(actual) => vtable = actual,
                }
            } else if tag == REGISTERED {
                let waking_vtable = vtable.set(WAKING);
                match self
                    .vtable
                    .compare_exchange(vtable, waking_vtable, AcqRel, Acquire)
                {
                    Ok(_) => {
                        let clean = vtable.unset(TAG_MASK);
                        // Since the current thread has exclusive access to self.data after a successful transition,
                        // we can optimize the atomic swap to normal exclusive load and store.
                        let data = self.data.load(Acquire);
                        self.data.store(ptr::null_mut(), Release);
                        self.vtable.store(NOOP_PTR, Release);
                        let waker = unsafe { Waker::new(data, &*clean) };
                        return Some(waker);
                    }
                    Err(actual) => {
                        vtable = actual;
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl Drop for MwsrWaker {
    fn drop(&mut self) {
        let vtable = self.vtable.load(Relaxed);
        if vtable.tag() == REGISTERED {
            let clean = vtable.unset(TAG_MASK);
            let data = self.data.load(Acquire) as *const ();
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
