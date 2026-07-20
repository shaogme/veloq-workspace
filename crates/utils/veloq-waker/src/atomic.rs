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

/// A synchronization primitive for task wakeup.
///
/// Sometimes the task interested in a given event will change over time.
/// An `AtomicWaker` can coordinate concurrent notifications with the consumer
/// potentially "updating" the underlying task to wake up. This is useful in
/// scenarios where a computation completes in another thread and wants to
/// notify the consumer, but the consumer is in the process of being migrated to
/// a new logical task.
///
/// Consumers should call `register` before checking the result of a computation
/// and producers should call `wake` after producing the computation (this
/// differs from the usual `thread::park` pattern). It is also permitted for
/// `wake` to be called **before** `register`. This results in a no-op.
///
/// A single `AtomicWaker` may be reused for any number of calls to `register` or
/// `wake`.
///
/// # Memory ordering
///
/// Calling `register` "acquires" all memory "released" by calls to `wake`
/// before the call to `register`.  Later calls to `wake` will wake the
/// registered waker (on contention this wake might be triggered in `register`).
///
/// For concurrent calls to `register` (should be avoided) the ordering is only
/// guaranteed for the winning call.
///
/// # Examples
///
/// Here is a simple example providing a `Flag` that can be signalled manually
/// when it is ready.
///
/// ``` ignore
/// use futures::future::Future;
/// use futures::task::{Context, Poll, AtomicWaker};
/// use std::sync::Arc;
/// use std::sync::atomic::AtomicBool;
/// use std::sync::atomic::Ordering::Relaxed;
/// use std::pin::Pin;
///
/// struct Inner {
///     waker: AtomicWaker,
///     set: AtomicBool,
/// }
///
/// #[derive(Clone)]
/// struct Flag(Arc<Inner>);
///
/// impl Flag {
///     pub fn new() -> Self {
///         Flag(Arc::new(Inner {
///             waker: AtomicWaker::new(),
///             set: AtomicBool::new(false),
///         }))
///     }
///
///     pub fn signal(&self) {
///         self.0.set.store(true, Relaxed);
///         self.0.waker.wake();
///     }
/// }
///
/// impl Future for Flag {
///     type Output = ();
///
///     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
///         // quick check to avoid registration if already done.
///         if self.0.set.load(Relaxed) {
///             return Poll::Ready(());
///         }
///
///         self.0.waker.register(cx.waker());
///
///         // Need to check condition **after** `register` to avoid a race
///         // condition that would result in lost notifications.
///         if self.0.set.load(Relaxed) {
///             Poll::Ready(())
///         } else {
///             Poll::Pending
///         }
///     }
/// }
/// ```
pub struct AtomicWaker {
    vtable: AtomicPtr<RawWakerVTable>,
    data: AtomicPtr<()>,
    cached_vtable: AtomicPtr<RawWakerVTable>,
    cached_data: AtomicPtr<()>,
}

impl AtomicWaker {
    /// Create an `AtomicWaker`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        AtomicWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
            cached_vtable: AtomicPtr::new(ptr::null_mut()),
            cached_data: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Create an `AtomicWaker`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        AtomicWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
            cached_vtable: AtomicPtr::new(ptr::null_mut()),
            cached_data: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Registers the waker to be notified on calls to `wake`.
    ///
    /// The new task will take place of any previous tasks that were registered
    /// by previous calls to `register`. Any calls to `wake` that happen after
    /// a call to `register` (as defined by the memory ordering rules), will
    /// notify the `register` caller's task and deregister the waker from future
    /// notifications. Because of this, callers should ensure `register` gets
    /// invoked with a new `Waker` **each** time they require a wakeup.
    ///
    /// It is safe to call `register` with multiple other threads concurrently
    /// calling `wake`. This will result in the `register` caller's current
    /// task being notified once.
    ///
    /// This function is safe to call concurrently, but this is generally a bad
    /// idea. Concurrent calls to `register` will attempt to register different
    /// tasks to be notified. One of the callers will win and have its task set,
    /// but there is no guarantee as to which caller will succeed.
    pub fn register(&self, waker: &Waker) {
        let waker_vtable = waker.vtable_ptr();
        let waker_data = waker.data();

        // Quick check of the local cache (Relaxed load)
        let cached_vtable = self.cached_vtable.load(Relaxed);
        let cached_data = self.cached_data.load(Relaxed);

        if waker_vtable == cached_vtable && waker_data == cached_data {
            let expected_vtable = waker_vtable.set(REGISTERED);
            if self.vtable.load(Acquire) == expected_vtable {
                return;
            }
        }

        let mut vtable = self.vtable.load(Acquire);

        loop {
            let tag = vtable.tag();

            if tag == WAKING {
                waker.wake_by_ref();
                return;
            }

            if tag == REGISTERING {
                // For concurrent calls to `register`, standard AtomicWaker simply returns
                // if it fails to acquire the registration lock. We match this behavior.
                return;
            }

            if tag == 0
                && vtable == NOOP_PTR {
                    let registering_vtable = vtable.set(REGISTERING);
                    match self
                        .vtable
                        .compare_exchange(vtable, registering_vtable, AcqRel, Acquire)
                    {
                        Ok(_) => {
                            let owned_waker = ManuallyDrop::new(waker.clone());
                            self.data.store(owned_waker.data() as *mut (), Release);
                            let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);
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

                                    // Registration failed, clear local cache
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

            if tag == REGISTERED {
                let clean_vtable = vtable.unset(TAG_MASK);

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

impl Drop for AtomicWaker {
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

impl Default for AtomicWaker {
    fn default() -> Self {
        AtomicWaker::new()
    }
}

impl fmt::Debug for AtomicWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AtomicWaker")
    }
}

unsafe impl Send for AtomicWaker {}
unsafe impl Sync for AtomicWaker {}
