use crate::{wait_queue::WaitQueue, waker::WaiterNode};
use veloq_std::{
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

const EMPTY: usize = 0;
const PERMIT: usize = 1;
const WAITING: usize = 2;

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;

/// An asynchronous event notification primitive.
///
/// A `Notify` can be used to wake a single waiting task via `notify_one`,
/// or all waiting tasks via `notify_waiters`.
pub struct Notify {
    state: AtomicUsize,
    waiters: WaitQueue,
}

unsafe impl Send for Notify {}
unsafe impl Sync for Notify {}

impl Notify {
    /// Creates a new `Notify` in the empty state.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(EMPTY),
            waiters: WaitQueue::new(),
        }
    }

    /// Creates a new `Notify` in the empty state.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(EMPTY),
            waiters: WaitQueue::new(),
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

        let detached = self.waiters.with_lock(|w| {
            if let Some(mut node) = w.pop_front() {
                unsafe {
                    node.as_mut().get_unchecked_mut().state = NOTIFIED_ONE;
                }
                let waker = node.as_ref().waker.take();
                let is_empty = w.is_empty();
                self.state
                    .store(if is_empty { EMPTY } else { WAITING }, Ordering::Release);
                Some(waker)
            } else {
                self.state.store(PERMIT, Ordering::Release);
                None
            }
        });
        if let Some(Some(waker)) = detached {
            waker.wake();
        }
    }

    /// Notifies all waiting tasks.
    ///
    /// If there are no waiting tasks, no permit is saved.
    pub fn notify_waiters(&self) {
        self.state.store(EMPTY, Ordering::Release);
        while let Some(detached) = self.waiters.take_front() {
            detached.wake();
        }
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

        this.notify.waiters.with_lock(|w| {
            if this.notify.state.load(Ordering::Acquire) == PERMIT {
                this.notify.state.store(EMPTY, Ordering::Release);
                this.completed = true;
                return;
            }

            this.node.state = NOT_NOTIFIED;
            unsafe {
                let node_pin = Pin::new_unchecked(&mut this.node);
                w.push_back(node_pin);
            }
            this.notify.state.store(WAITING, Ordering::Release);
            this.queued = true;
        });
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
                if this
                    .notify
                    .waiters
                    .refresh_waker(&mut this.node, cx)
                    .detached()
                {
                    this.queued = false;
                    this.completed = true;
                    return Poll::Ready(());
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
            let mut immediate = false;
            let mut stale_waker = None;
            unsafe {
                this.node.waker.register(cx.waker());
            }
            this.notify.waiters.with_lock(|w| {
                if this.notify.state.load(Ordering::Acquire) == PERMIT {
                    this.notify.state.store(EMPTY, Ordering::Release);
                    this.completed = true;
                    immediate = true;
                    stale_waker = this.node.waker.take();
                    return;
                }

                this.node.state = NOT_NOTIFIED;
                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    w.push_back(node_pin);
                }
                this.notify.state.store(WAITING, Ordering::Release);
                this.queued = true;
            });
            drop(stale_waker);
            if immediate {
                return Poll::Ready(());
            }
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
            let waker = self.notify.waiters.with_lock(|w| {
                if self.node.link.is_linked() {
                    unsafe {
                        let ptr = NonNull::from(&self.node);
                        let mut cursor = w.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                    }
                    if w.is_empty() {
                        self.notify.state.store(EMPTY, Ordering::Release);
                    }
                    None
                } else if self.node.state == NOTIFIED_ONE {
                    // Was popped by notify_one, but dropped before poll completion.
                    // Transfer notification to next waiter or restore permit.
                    if let Some(mut next) = w.pop_front() {
                        unsafe {
                            next.as_mut().get_unchecked_mut().state = NOTIFIED_ONE;
                        }
                        let waker = next.as_ref().waker.take();
                        let is_empty = w.is_empty();
                        self.notify
                            .state
                            .store(if is_empty { EMPTY } else { WAITING }, Ordering::Release);
                        waker
                    } else {
                        self.notify.state.store(PERMIT, Ordering::Release);
                        None
                    }
                } else {
                    None
                }
            });
            if let Some(waker) = waker {
                waker.wake();
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
