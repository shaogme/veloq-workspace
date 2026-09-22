use crate::{
    mutex::{Mutex, MutexGuard, MutexLockFuture},
    waker::{WaiterAdapter, WaiterNode},
};
use futures_core::Future;
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    fmt,
    marker::PhantomPinned,
    ops::AsyncFnMut,
    pin::Pin,
    ptr::NonNull,
    sync::SpinLock,
    task::{Context, Poll},
};

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;
const NOTIFIED_ALL: usize = 2;

/// An asynchronous condition variable.
///
/// A condition variable enables tasks to wait for an event or a condition to be met
/// in conjunction with an asynchronous [`Mutex`].
pub struct Condvar {
    waiters: SpinLock<LinkedList<WaiterAdapter>>,
}

unsafe impl Send for Condvar {}
unsafe impl Sync for Condvar {}

impl Condvar {
    /// Creates a new condition variable.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a new condition variable.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Wakes up one task that is waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_one(&self) {
        let mut waiters = self.waiters.lock();
        waiters.with_mut(|w| {
            if let Some(mut node) = w.pop_front() {
                unsafe {
                    node.as_mut().get_unchecked_mut().kind = NOTIFIED_ONE;
                }
                node.as_ref().waker.wake();
            }
        });
    }

    /// Wakes up all tasks that are waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_all(&self) {
        let mut waiters = self.waiters.lock();
        waiters.with_mut(|w| {
            while let Some(mut node) = w.pop_front() {
                unsafe {
                    node.as_mut().get_unchecked_mut().kind = NOTIFIED_ALL;
                }
                node.as_ref().waker.wake();
            }
        });
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

    /// Waits on this condition variable with an asynchronous predicate until it returns `false`.
    pub async fn wait_while_async<'a, T: ?Sized, F>(
        &self,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> MutexGuard<'a, T>
    where
        F: AsyncFnMut(&mut T) -> bool,
    {
        while condition(&mut *guard).await {
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

unsafe impl<'c, 'a, T: ?Sized + Send> Send for Wait<'c, 'a, T> {}
unsafe impl<'c, 'a, T: ?Sized + Sync> Sync for Wait<'c, 'a, T> {}

impl<'c, 'a, T: ?Sized> Future for Wait<'c, 'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // 1. If we are currently re-acquiring the mutex
        if let Some(lock_fut) = &mut this.lock_fut {
            let res = unsafe { Pin::new_unchecked(lock_fut) }.poll(cx);
            if let Poll::Ready(new_guard) = res {
                this.lock_fut = None;
                this.node.kind = NOT_NOTIFIED;
                return Poll::Ready(new_guard);
            }
            return Poll::Pending;
        }

        // 2. Initial poll: register waker, enqueue into cvar, and release mutex
        if !this.queued {
            let mut waiters = this.cvar.waiters.lock();
            this.node.kind = NOT_NOTIFIED;
            unsafe {
                this.node.waker.register(cx.waker());
                let node_pin = Pin::new_unchecked(&mut this.node);
                waiters.with_mut(|w| w.push_back(node_pin));
            }
            this.queued = true;
            drop(waiters);

            // Release the mutex after safely registered in condvar to prevent lost wakeups.
            drop(this.guard.take());

            return Poll::Pending;
        }

        // 3. We are queued in cvar. Check if we were dequeued (notified).
        let waiters = this.cvar.waiters.lock();
        let is_linked = waiters.with(|_| this.node.link.is_linked());

        if !is_linked {
            this.queued = false;
            drop(waiters);

            // Start re-acquiring the mutex lock.
            let lock_fut = this.lock_fut.insert(this.lock.lock());
            let res = unsafe { Pin::new_unchecked(lock_fut) }.poll(cx);
            if let Poll::Ready(new_guard) = res {
                this.lock_fut = None;
                this.node.kind = NOT_NOTIFIED;
                return Poll::Ready(new_guard);
            }
            Poll::Pending
        } else {
            // Still in queue, refresh waker.
            unsafe {
                this.node.waker.register(cx.waker());
            }
            drop(waiters);
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
            let mut waiters = self.cvar.waiters.lock();
            let is_linked = waiters.with(|_| self.node.link.is_linked());
            if is_linked {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    waiters.with_mut(|w| {
                        let mut cursor = w.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                    });
                }
            } else if self.node.kind == NOTIFIED_ONE {
                self.node.kind = NOT_NOTIFIED;
                waiters.with_mut(|w| {
                    if let Some(mut next) = w.pop_front() {
                        unsafe {
                            next.as_mut().get_unchecked_mut().kind = NOTIFIED_ONE;
                        }
                        next.as_ref().waker.wake();
                    }
                });
            }
            self.queued = false;
        } else if self.node.kind == NOTIFIED_ONE {
            self.node.kind = NOT_NOTIFIED;
            let mut waiters = self.cvar.waiters.lock();
            waiters.with_mut(|w| {
                if let Some(mut next) = w.pop_front() {
                    unsafe {
                        next.as_mut().get_unchecked_mut().kind = NOTIFIED_ONE;
                    }
                    next.as_ref().waker.wake();
                }
            });
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
