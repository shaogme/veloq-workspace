use crate::{
    common::update_waker,
    mutex::{Mutex, MutexGuard, MutexLockFuture},
};
use futures_core::Future;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    cell::{Cell, RefCell},
    fmt,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;
const NOTIFIED_ALL: usize = 2;

struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    kind: Cell<usize>,
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new() -> Self {
        Self {
            waker: RefCell::new(None),
            link: Link::new(),
            kind: Cell::new(NOT_NOTIFIED),
            _p: PhantomPinned,
        }
    }
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    const NEW: Self = Self;
}

/// An asynchronous condition variable for local/single-threaded contexts.
///
/// A condition variable enables tasks to wait for an event or a condition to be met
/// in conjunction with a local [`Mutex`].
pub struct Condvar {
    waiters: RefCell<LinkedList<WaiterAdapter>>,
}

impl Condvar {
    /// Creates a new condition variable.
    pub const fn new() -> Self {
        Self {
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Wakes up one task that is waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_one(&self) {
        let mut waiters = self.waiters.borrow_mut();
        if let Some(node) = waiters.pop_front() {
            node.kind.set(NOTIFIED_ONE);
            let waker = node.waker.borrow_mut().take();
            drop(waiters);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    /// Wakes up all tasks that are waiting on this condition variable.
    ///
    /// If there are no waiting tasks, this call has no effect.
    pub fn notify_all(&self) {
        let mut waiters = self.waiters.borrow_mut();
        while let Some(node) = waiters.pop_front() {
            node.kind.set(NOTIFIED_ALL);
            if let Some(waker) = node.waker.borrow_mut().take() {
                waker.wake();
            }
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
                this.node.kind.set(NOT_NOTIFIED);
                return Poll::Ready(new_guard);
            }
            return Poll::Pending;
        }

        // 2. Initial poll: register waker, enqueue into cvar, and release mutex
        if !this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            this.node.kind.set(NOT_NOTIFIED);

            unsafe {
                let node_pin = Pin::new_unchecked(&mut this.node);
                this.cvar.waiters.borrow_mut().push_back(node_pin);
            }
            this.queued = true;

            // Release the mutex after safely registered in condvar to prevent lost wakeups.
            drop(this.guard.take());

            return Poll::Pending;
        }

        // 3. We were queued in cvar. Check if we were dequeued (notified).
        let is_linked = this.node.link.is_linked();

        if !is_linked {
            this.queued = false;

            // Start re-acquiring the mutex lock. Place in this.lock_fut before polling
            // to ensure its pinned address never moves.
            let lock_fut = this.lock_fut.insert(this.lock.lock());
            let res = unsafe { Pin::new_unchecked(lock_fut) }.poll(cx);
            if let Poll::Ready(new_guard) = res {
                this.lock_fut = None;
                this.node.kind.set(NOT_NOTIFIED);
                return Poll::Ready(new_guard);
            }
            Poll::Pending
        } else {
            // Still in queue, refresh waker.
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
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
            let is_linked = self.node.link.is_linked();
            if is_linked {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut waiters = self.cvar.waiters.borrow_mut();
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
            } else if self.node.kind.get() == NOTIFIED_ONE {
                self.node.kind.set(NOT_NOTIFIED);
                let mut waiters = self.cvar.waiters.borrow_mut();
                if let Some(next) = waiters.pop_front() {
                    next.kind.set(NOTIFIED_ONE);
                    let waker = next.waker.borrow_mut().take();
                    drop(waiters);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
        } else if self.node.kind.get() == NOTIFIED_ONE {
            self.node.kind.set(NOT_NOTIFIED);
            let mut waiters = self.cvar.waiters.borrow_mut();
            if let Some(next) = waiters.pop_front() {
                next.kind.set(NOTIFIED_ONE);
                let waker = next.waker.borrow_mut().take();
                drop(waiters);
                if let Some(waker) = waker {
                    waker.wake();
                }
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
