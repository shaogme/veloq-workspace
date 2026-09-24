use crate::{
    wait_queue::{DetachedWaiter, WaitQueue},
    waker::WaiterNode,
};
use veloq_std::{
    cell::{Cell, UnsafeCell},
    fmt,
    future::Future,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;

/// An asynchronous mutual exclusion primitive for local/single-threaded contexts.
pub struct Mutex<T: ?Sized> {
    locked: Cell<bool>,
    waiters: WaitQueue,
    data: UnsafeCell<T>,
}

impl<T> Mutex<T> {
    /// Creates a new `Mutex` in an unlocked state with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            locked: Cell::new(false),
            waiters: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `Mutex` in an unlocked state with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            locked: Cell::new(false),
            waiters: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Consumes the mutex, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Returns `true` if the lock is currently held.
    pub fn is_locked(&self) -> bool {
        self.locked.get()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to take place.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock immediately.
    ///
    /// Returns `Some(MutexGuard)` if the lock was acquired, or `None` if the lock is already held.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if !self.locked.get() && self.waiters.is_empty() {
            self.locked.set(true);
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
        MutexLockFuture {
            lock: self,
            node: WaiterNode::new(),
            phase: Phase::Initial,
            _pin: PhantomPinned,
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Mutex");
        d.field("locked", &self.locked.get());
        if !self.locked.get() {
            unsafe {
                self.data.with(|data| {
                    d.field("data", &data);
                });
            }
        } else {
            d.field("data", &format_args!("<locked>"));
        }
        d.finish_non_exhaustive()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(data: T) -> Self {
        Self::new(data)
    }
}

/// A RAII guard returned by `Mutex::lock` and `Mutex::try_lock`.
pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns a reference to the [`Mutex`] that this guard was acquired from.
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.lock
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.with_mut(|ptr| ptr as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let detached = self.lock.waiters.take_front_with(|next| {
            next.state = STATE_GRANTED;
        });
        if let Some(detached) = detached {
            detached.wake();
        } else {
            self.lock.locked.set(false);
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
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

        if !this.lock.locked.get() && this.lock.waiters.is_empty() {
            this.lock.locked.set(true);
            this.phase = Phase::Completed;
            return Poll::Ready(MutexGuard { lock: this.lock });
        }

        this.node.state = STATE_WAITING;
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.lock.waiters.register_and_push(node_pin, cx);
        }
        this.phase = Phase::Waiting;
        Poll::Pending
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
                    unsafe {
                        next_node.as_mut().get_unchecked_mut().state = STATE_GRANTED;
                    }
                    Some(DetachedWaiter {
                        waker: unsafe { next_node.as_mut().get_unchecked_mut().waker.take() },
                    })
                } else {
                    self.lock.locked.set(false);
                    None
                }
            } else if self.node.link.is_linked() {
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

impl<'a, T: ?Sized> fmt::Debug for MutexLockFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutexLockFuture")
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}
