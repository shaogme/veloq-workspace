use crate::shim::atomic::{AtomicUsize, Ordering};
use crate::shim::cell::UnsafeCell;
use crate::shim::lock::SpinLock;
use crate::waker::{WaiterAdapter, WaiterNode};
use std::future::Future;
use std::marker::PhantomPinned;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};
use veloq_intrusive_linklist::ConcurrentLinkedList;

/// An asynchronous mutual exclusion primitive.
pub struct Mutex<T: ?Sized> {
    state: AtomicUsize,
    waiters: SpinLock<ConcurrentLinkedList<WaiterAdapter>>,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Creates a new `Mutex` with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(ConcurrentLinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `Mutex` with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(ConcurrentLinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Consumes the mutex, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

const UNLOCKED: usize = 0;
const LOCKED: usize = 1;
const CONTENDED: usize = 2;

impl<T: ?Sized> Mutex<T> {
    /// Returns true if the lock is currently held.
    ///
    /// This function does not provide any synchronization guarantees, so the
    /// returned value is only a hint.
    pub fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != UNLOCKED
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to take place—
    /// the mutable borrow statically guarantees no locks exist.
    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: We have exclusive access to the Mutex, so we have exclusive access to the data.
        unsafe { &mut *self.data.get_mut() }
    }

    /// Attempts to acquire the lock immediately.
    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self
            .state
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(MutexGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock asynchronously.
    pub fn lock(&self) -> MutexLockFuture<'_, T> {
        MutexLockFuture {
            lock: self,
            node: WaiterNode::new(),
            queued: false,
            _pin: PhantomPinned,
        }
    }
}

/// A RAII guard returned by `Mutex::lock` and `Mutex::try_lock`.
pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
}

// SAFETY: MutexGuard gives exclusive access to the underlying data.
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &mut *self.lock.data.get_mut() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // Fast path: if state is just LOCKED, release it.
        if self
            .lock
            .state
            .compare_exchange(LOCKED, UNLOCKED, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        // Slow path: state is likely CONTENDED, unlock and wake someone.
        self.lock.state.store(UNLOCKED, Ordering::Release);

        // Notify one waiter
        let mut waiters = self.lock.waiters.lock();
        if let Some(node) = waiters.pop_front() {
            node.as_ref().waker.wake();
        }
    }
}

/// A future that resolves to a `MutexGuard`.
pub struct MutexLockFuture<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'a, T: ?Sized> Future for MutexLockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We do not move `node`.
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let state = this.lock.state.load(Ordering::Relaxed);

            // 1. Try to acquire the lock
            if state == UNLOCKED {
                let target = if this.queued { CONTENDED } else { LOCKED };
                if this
                    .lock
                    .state
                    .compare_exchange(UNLOCKED, target, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    // Acquired.
                    if this.queued {
                        // We successfully acquired the lock.
                        // If we were queued, we must ensure we are removed.
                        let mut waiters = this.lock.waiters.lock();
                        if this.node.link.is_linked() {
                            unsafe {
                                let ptr = NonNull::from(&this.node);
                                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                                cursor.remove();
                            }
                        }
                        // Optimization: if queue is empty, demote state to LOCKED.
                        // This avoids next unlock taking slow path.
                        if waiters.is_empty() {
                            this.lock.state.store(LOCKED, Ordering::Release);
                        }

                        this.queued = false;
                    }
                    return Poll::Ready(MutexGuard { lock: this.lock });
                }
                // CAS failed, look again
                continue;
            }

            // Optimization: Bounded Spinning
            // If the lock is held, we spin briefly to wait for it to be released
            // before registering a waker and yielding to the scheduler.
            #[cfg(not(feature = "loom"))]
            if !this.queued {
                for _ in 0..100 {
                    if this.lock.state.load(Ordering::Relaxed) == UNLOCKED {
                        break;
                    }
                    std::hint::spin_loop();
                }
                if this.lock.state.load(Ordering::Relaxed) == UNLOCKED {
                    continue;
                }
            }

            // 2. If locked, ensure it is marked CONTENDED before we wait
            if state == LOCKED
                && this
                    .lock
                    .state
                    .compare_exchange(LOCKED, CONTENDED, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                // State changed, retry
                continue;
            }

            // 3. Enqueue if needed
            if !this.queued {
                let mut waiters = this.lock.waiters.lock();

                // Optimization: Try to acquire if state became free while locking waiters
                if this.lock.state.load(Ordering::Acquire) == UNLOCKED {
                    drop(waiters);
                    continue;
                }

                // Register waker
                this.node.waker.register(cx.waker());

                // Enqueue
                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                this.queued = true;

                // Should we force state to CONTENDED here?
                // We tried CAS(LOCKED, CONTENDED) above. If it failed, it might be UNLOCKED now,
                // which loop will catch. Or it might be CONTENDED already.
                // But what if it was UNLOCKED, we entered "Optimization" check, failed?
                // Let's rely on loop to ensure CONTENDED bit is set or we grab lock.
            } else {
                // If we are queued, check if we were woken
                let is_linked = {
                    let _waiters = this.lock.waiters.lock();
                    this.node.link.is_linked()
                };

                if !is_linked {
                    // Woken up
                    this.queued = false;
                    // Register waker to reset AtomicWaker state
                    this.node.waker.register(cx.waker());
                    continue;
                }

                // Register waker
                this.node.waker.register(cx.waker());
            }

            return Poll::Pending;
        }
    }
}

impl<'a, T: ?Sized> Drop for MutexLockFuture<'a, T> {
    fn drop(&mut self) {
        if self.queued {
            let mut waiters = self.lock.waiters.lock();
            if self.node.link.is_linked() {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
            }
        }
    }
}
