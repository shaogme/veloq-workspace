use crate::{
    wait_queue::{DetachedWaiter, WaitQueue},
    waker::WaiterNode,
};
use veloq_std::{
    cell::UnsafeCell,
    future::Future,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

/// An asynchronous mutual exclusion primitive.
pub struct Mutex<T: ?Sized> {
    state: AtomicUsize,
    waiters: WaitQueue,
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
            waiters: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `Mutex` with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: WaitQueue::new(),
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

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;

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
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock immediately.
    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.waiters.is_empty()
            && self
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
    pub fn lock(&self) -> impl Future<Output = MutexGuard<'_, T>> + '_ {
        self.lock_future()
    }

    pub(crate) fn lock_future(&self) -> MutexLockFuture<'_, T> {
        let mut node = WaiterNode::new();
        node.state = STATE_WAITING;
        MutexLockFuture {
            lock: self,
            node,
            phase: Phase::Initial,
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

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns a reference to the [`Mutex`] that this guard was acquired from.
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.lock
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &mut *self.lock.data.with_mut(|ptr| ptr as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let detached = self.lock.waiters.with_lock(|w| {
            if let Some(mut node) = w.pop_front() {
                // SAFETY: The node remains pinned while it is detached.
                unsafe {
                    node.as_mut().get_unchecked_mut().state = STATE_GRANTED;
                }
                Some(DetachedWaiter {
                    waker: node.as_ref().waker.take(),
                })
            } else {
                self.lock.state.store(UNLOCKED, Ordering::Release);
                None
            }
        });
        if let Some(detached) = detached {
            detached.wake();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Initial,
    Waiting,
    Completed,
}

/// A future that resolves to a `MutexGuard`.
pub(crate) struct MutexLockFuture<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
    node: WaiterNode,
    phase: Phase,
    _pin: PhantomPinned,
}

impl<'a, T: ?Sized> Future for MutexLockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We do not move `node`.
        let this = unsafe { self.get_unchecked_mut() };

        if this.phase == Phase::Completed {
            panic!("polled MutexLockFuture after completion");
        }

        if this.phase == Phase::Waiting {
            if this.lock.waiters.refresh_waker(&mut this.node, cx).linked() {
                return Poll::Pending;
            }

            this.phase = Phase::Completed;
            return Poll::Ready(MutexGuard { lock: this.lock });
        }

        #[cfg(not(feature = "loom"))]
        let mut spin_count = 0;

        #[cfg_attr(feature = "loom", allow(clippy::never_loop))]
        loop {
            // Fast path: try to acquire if unlocked (queue is empty when UNLOCKED).
            if this
                .lock
                .state
                .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                this.phase = Phase::Completed;
                return Poll::Ready(MutexGuard { lock: this.lock });
            }

            // Optimization: Bounded Spinning
            #[cfg(not(feature = "loom"))]
            if spin_count < 100 {
                spin_count += 1;
                veloq_std::hint::spin_loop();
                if this.lock.state.load(Ordering::Relaxed) == UNLOCKED {
                    continue;
                }
            }

            let mut acquired = false;
            let mut stale_waker = None;
            unsafe {
                this.node.waker.register(cx.waker());
            }
            this.lock.waiters.with_lock(|w| {
                // Double check: if it became unlocked while acquiring waiters lock
                let is_empty = w.is_empty();
                if is_empty
                    && this
                        .lock
                        .state
                        .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    acquired = true;
                    return;
                }

                this.node.state = STATE_WAITING;
                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    w.push_back(node_pin);
                }
                this.phase = Phase::Waiting;
            });
            if acquired {
                stale_waker = this.node.waker.take();
            }
            drop(stale_waker);

            if acquired {
                this.phase = Phase::Completed;
                return Poll::Ready(MutexGuard { lock: this.lock });
            }
            return Poll::Pending;
        }
    }
}

impl<'a, T: ?Sized> Drop for MutexLockFuture<'a, T> {
    fn drop(&mut self) {
        if self.phase != Phase::Waiting {
            return;
        }

        let detached = self.lock.waiters.with_lock(|w| {
            if self.node.state == STATE_GRANTED {
                if let Some(mut next_node) = w.pop_front() {
                    // SAFETY: The node remains pinned while it is detached.
                    unsafe {
                        next_node.as_mut().get_unchecked_mut().state = STATE_GRANTED;
                    }
                    Some(DetachedWaiter {
                        waker: next_node.as_ref().waker.take(),
                    })
                } else {
                    self.lock.state.store(UNLOCKED, Ordering::Release);
                    None
                }
            } else if self.node.link.is_linked() {
                // SAFETY: The node is pinned in this future and linked in this list.
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut cursor = w.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
                None
            } else {
                None
            }
        });
        if let Some(detached) = detached {
            detached.wake();
        }
    }
}
