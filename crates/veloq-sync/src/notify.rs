use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::{
        SpinLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use crate::waker::{WaiterAdapter, WaiterNode};

const EMPTY: usize = 0;
const PERMIT: usize = 1;
const WAITING: usize = 2;

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;
const NOTIFIED_ALL: usize = 2;

/// An asynchronous event notification primitive.
///
/// A `Notify` can be used to wake a single waiting task via `notify_one`,
/// or all waiting tasks via `notify_waiters`.
pub struct Notify {
    state: AtomicUsize,
    waiters: SpinLock<LinkedList<WaiterAdapter>>,
}

unsafe impl Send for Notify {}
unsafe impl Sync for Notify {}

impl Notify {
    /// Creates a new `Notify` in the empty state.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(EMPTY),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a new `Notify` in the empty state.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(EMPTY),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Notifies a single waiting task.
    ///
    /// If there is at least one task waiting, one is woken.
    /// If there are no waiting tasks, a permit is saved for the next call to `notified().await`.
    /// Multiple calls to `notify_one` without intervening awaits will not accumulate permits.
    pub fn notify_one(&self) {
        if self.state.load(Ordering::Acquire) == PERMIT {
            return;
        }

        let mut waiters = self.waiters.lock();
        let mut unparked = false;
        waiters.with_mut(|w| {
            if let Some(mut node) = w.pop_front() {
                unsafe {
                    node.as_mut().get_unchecked_mut().kind = NOTIFIED_ONE;
                }
                node.as_ref().waker.wake();
                unparked = true;
            }
        });

        if unparked {
            let is_empty = waiters.with(|w| w.is_empty());
            self.state
                .store(if is_empty { EMPTY } else { WAITING }, Ordering::Release);
        } else {
            self.state.store(PERMIT, Ordering::Release);
        }
    }

    /// Notifies all waiting tasks.
    ///
    /// If there are no waiting tasks, no permit is saved.
    pub fn notify_waiters(&self) {
        let mut waiters = self.waiters.lock();
        if waiters.with(|w| w.is_empty()) {
            return;
        }

        self.state.store(EMPTY, Ordering::Release);

        waiters.with_mut(|w| {
            while let Some(mut node) = w.pop_front() {
                unsafe {
                    node.as_mut().get_unchecked_mut().kind = NOTIFIED_ALL;
                }
                node.as_ref().waker.wake();
            }
        });
    }

    /// Returns a future that completes once notified.
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            node: WaiterNode::new(),
            queued: false,
            completed: false,
            _pin: PhantomPinned,
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notify")
            .field("state", &self.state.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// A future that waits for a notification from a `Notify`.
pub struct Notified<'a> {
    notify: &'a Notify,
    node: WaiterNode,
    queued: bool,
    completed: bool,
    _pin: PhantomPinned,
}

impl<'a> Notified<'a> {
    /// Pre-registers this waiter in the notification queue.
    pub fn enable(self: Pin<&mut Self>) {
        let this = unsafe { self.get_unchecked_mut() };
        if this.queued || this.completed {
            return;
        }

        let state = this.notify.state.load(Ordering::Acquire);
        if state == PERMIT
            && this
                .notify
                .state
                .compare_exchange(PERMIT, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            this.completed = true;
            return;
        }

        let mut waiters = this.notify.waiters.lock();
        if this.notify.state.load(Ordering::Acquire) == PERMIT {
            this.notify.state.store(EMPTY, Ordering::Release);
            this.completed = true;
            return;
        }

        this.node.kind = NOT_NOTIFIED;
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            waiters.with_mut(|w| w.push_back(node_pin));
        }
        this.notify.state.store(WAITING, Ordering::Release);
        this.queued = true;
    }
}

impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.completed {
            return Poll::Ready(());
        }

        loop {
            if this.queued {
                let waiters = this.notify.waiters.lock();
                let is_linked = waiters.with(|_| this.node.link.is_linked());
                if !is_linked {
                    this.queued = false;
                    this.completed = true;
                    return Poll::Ready(());
                }

                unsafe {
                    this.node.waker.register(cx.waker());
                }
                return Poll::Pending;
            }

            // 1. Fast-path permit check
            let state = this.notify.state.load(Ordering::Acquire);
            if state == PERMIT {
                if this
                    .notify
                    .state
                    .compare_exchange(PERMIT, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    this.completed = true;
                    return Poll::Ready(());
                }
                continue;
            }

            // 2. Lock waiters
            let mut waiters = this.notify.waiters.lock();
            if this.notify.state.load(Ordering::Acquire) == PERMIT {
                this.notify.state.store(EMPTY, Ordering::Release);
                this.completed = true;
                return Poll::Ready(());
            }

            this.node.kind = NOT_NOTIFIED;
            unsafe {
                this.node.waker.register(cx.waker());
                let node_pin = Pin::new_unchecked(&mut this.node);
                waiters.with_mut(|w| w.push_back(node_pin));
            }
            this.notify.state.store(WAITING, Ordering::Release);
            this.queued = true;
            return Poll::Pending;
        }
    }
}

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        if self.queued {
            let mut waiters = self.notify.waiters.lock();
            let is_linked = waiters.with(|_| self.node.link.is_linked());
            if is_linked {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    waiters.with_mut(|w| {
                        let mut cursor = w.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                    });
                }
                if waiters.with(|w| w.is_empty()) {
                    self.notify.state.store(EMPTY, Ordering::Release);
                }
            } else if self.node.kind == NOTIFIED_ONE {
                // Was popped by notify_one, but dropped before poll completion.
                // Transfer notification to next waiter or restore permit.
                let mut unparked = false;
                waiters.with_mut(|w| {
                    if let Some(mut next) = w.pop_front() {
                        unsafe {
                            next.as_mut().get_unchecked_mut().kind = NOTIFIED_ONE;
                        }
                        next.as_ref().waker.wake();
                        unparked = true;
                    }
                });

                if unparked {
                    let is_empty = waiters.with(|w| w.is_empty());
                    self.notify
                        .state
                        .store(if is_empty { EMPTY } else { WAITING }, Ordering::Release);
                } else {
                    self.notify.state.store(PERMIT, Ordering::Release);
                }
            }
        }
    }
}

impl<'a> fmt::Debug for Notified<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notified")
            .field("queued", &self.queued)
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}
