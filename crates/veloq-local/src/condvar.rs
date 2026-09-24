use crate::{
    mutex::{Mutex, MutexGuard, MutexLockFuture},
    wait_queue::WaitQueue,
    waker::WaiterNode,
};
use futures_core::Future;
use veloq_std::{
    fmt,
    marker::PhantomPinned,
    pin::Pin,
    task::{Context, Poll},
};

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;

/// An asynchronous condition variable for local/single-threaded contexts.
///
/// A condition variable enables tasks to wait for an event or a condition to be met
/// in conjunction with a local [`Mutex`].
pub struct Condvar {
    waiters: WaitQueue,
}

impl Condvar {
    /// Creates a new condition variable.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            waiters: WaitQueue::new(),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            waiters: WaitQueue::new(),
        }
    }

    /// Wakes up one task that is waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_one(&self) {
        let detached = self.waiters.take_front_with(|node| {
            node.state = NOTIFIED_ONE;
        });
        if let Some(detached) = detached {
            detached.wake();
        }
    }

    /// Wakes up all tasks that are waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_all(&self) {
        while let Some(detached) = self.waiters.take_front() {
            detached.wake();
        }
    }

    /// Waits on this condition variable, releasing the mutex guard and re-acquiring it before returning.
    pub fn wait<'c, 'a, T: ?Sized>(&'c self, guard: MutexGuard<'a, T>) -> Wait<'c, 'a, T> {
        let lock = MutexGuard::mutex(&guard);
        Wait {
            cvar: self,
            lock,
            guard: Some(guard),
            node: WaiterNode::new(),
            lock_fut: None,
            queued: false,
            _pin: PhantomPinned,
        }
    }

    /// Waits on this condition variable until the predicate returns `false`.
    pub async fn wait_while<'a, T: ?Sized, F>(
        &self,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> MutexGuard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut *guard) {
            guard = self.wait(guard).await;
        }
        guard
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

/// A future returned by [`Condvar::wait`] that waits for a notification and re-acquires the lock.
pub struct Wait<'c, 'a, T: ?Sized> {
    cvar: &'c Condvar,
    lock: &'a Mutex<T>,
    guard: Option<MutexGuard<'a, T>>,
    node: WaiterNode,
    lock_fut: Option<MutexLockFuture<'a, T>>,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'c, 'a, T: ?Sized> Future for Wait<'c, 'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // 1. If we are already waiting to re-acquire the mutex
        if let Some(lock_fut) = &mut this.lock_fut {
            let res = unsafe { Pin::new_unchecked(lock_fut) }.poll(cx);
            if let Poll::Ready(new_guard) = res {
                this.lock_fut = None;
                this.node.state = NOT_NOTIFIED;
                return Poll::Ready(new_guard);
            }
            return Poll::Pending;
        }

        // 2. Initial poll: register waker, enqueue into cvar, and release mutex
        if !this.queued {
            this.node.state = NOT_NOTIFIED;
            unsafe {
                let node_pin = Pin::new_unchecked(&mut this.node);
                this.cvar.waiters.register_and_push(node_pin, cx);
            }
            this.queued = true;

            // Release the mutex after safely registered in condvar to prevent lost wakeups.
            drop(this.guard.take());

            return Poll::Pending;
        }

        // 3. We are queued in cvar. Check if we were dequeued (notified).
        if this
            .cvar
            .waiters
            .refresh_waker(&mut this.node, cx)
            .detached()
        {
            this.queued = false;

            // Start re-acquiring the mutex lock. Place in this.lock_fut before polling
            // to ensure its pinned address never moves.
            let lock_fut = this.lock_fut.insert(this.lock.lock_future());
            let res = unsafe { Pin::new_unchecked(lock_fut) }.poll(cx);
            if let Poll::Ready(new_guard) = res {
                this.lock_fut = None;
                this.node.state = NOT_NOTIFIED;
                return Poll::Ready(new_guard);
            }
            Poll::Pending
        } else {
            Poll::Pending
        }
    }
}

impl<'c, 'a, T: ?Sized> Drop for Wait<'c, 'a, T> {
    fn drop(&mut self) {
        if self.guard.is_some() {
            // Never polled; guard is dropped automatically.
            return;
        }

        if self.queued {
            if !self.cvar.waiters.remove(&self.node) && self.node.state == NOTIFIED_ONE {
                self.node.state = NOT_NOTIFIED;
                let detached = self.cvar.waiters.take_front_with(|next| {
                    next.state = NOTIFIED_ONE;
                });
                if let Some(detached) = detached {
                    detached.wake();
                }
            }
            self.queued = false;
        } else if self.node.state == NOTIFIED_ONE {
            self.node.state = NOT_NOTIFIED;
            let detached = self.cvar.waiters.take_front_with(|next| {
                next.state = NOTIFIED_ONE;
            });
            if let Some(detached) = detached {
                detached.wake();
            }
        }
    }
}

impl<'c, 'a, T: ?Sized> fmt::Debug for Wait<'c, 'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait")
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}
