use crate::{wait_queue::WaitQueue, waker::WaiterNode};
use futures_core::Future;
use veloq_std::{
    cell::Cell,
    fmt,
    marker::PhantomPinned,
    pin::Pin,
    task::{Context, Poll},
};

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;
const NOTIFIED_CONSUMED: usize = 3;

/// An asynchronous event notification primitive for local/single-threaded contexts.
///
/// A `Notify` can be used to wake a single waiting task via `notify_one`,
/// or all waiting tasks via `notify_waiters`.
pub struct Notify {
    has_permit: Cell<bool>,
    waiters: WaitQueue,
}

impl Notify {
    /// Creates a new `Notify` in the empty state.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            has_permit: Cell::new(false),
            waiters: WaitQueue::new(),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            has_permit: Cell::new(false),
            waiters: WaitQueue::new(),
        }
    }

    /// Notifies a single waiting task.
    ///
    /// If there is at least one task waiting, one is woken.
    /// If there are no waiting tasks, a permit is saved for the next call to `notified().await`.
    /// Multiple calls to `notify_one` without intervening awaits will not accumulate permits.
    pub fn notify_one(&self) {
        if self.has_permit.get() {
            return;
        }

        let detached = self.waiters.take_front_with(|node| {
            node.state = NOTIFIED_ONE;
        });
        if let Some(detached) = detached {
            detached.wake();
        } else {
            self.has_permit.set(true);
        }
    }

    /// Notifies all waiting tasks.
    ///
    /// If there are no waiting tasks, no permit is saved.
    pub fn notify_waiters(&self) {
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
            .field("has_permit", &self.has_permit.get())
            .finish_non_exhaustive()
    }
}

/// A future that waits for a notification from a `Notify`.
pub struct Notified<'a> {
    notify: &'a Notify,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'a> Notified<'a> {
    /// Pre-registers this waiter in the notification queue.
    pub fn enable(self: Pin<&mut Self>) {
        let this = unsafe { self.get_unchecked_mut() };
        if this.queued || this.node.state == NOTIFIED_CONSUMED {
            return;
        }

        if this.notify.has_permit.get() {
            this.notify.has_permit.set(false);
            this.node.state = NOTIFIED_CONSUMED;
            return;
        }

        this.node.state = NOT_NOTIFIED;
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.notify.waiters.push_back(node_pin);
        }
        this.queued = true;
    }
}

impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.node.state == NOTIFIED_CONSUMED {
            return Poll::Ready(());
        }

        if this.queued {
            if this
                .notify
                .waiters
                .refresh_waker(&mut this.node, cx)
                .detached()
            {
                this.queued = false;
                this.node.state = NOTIFIED_CONSUMED;
                return Poll::Ready(());
            }
            return Poll::Pending;
        }

        if this.notify.has_permit.get() {
            this.notify.has_permit.set(false);
            this.node.state = NOTIFIED_CONSUMED;
            return Poll::Ready(());
        }

        this.node.state = NOT_NOTIFIED;
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.notify.waiters.register_and_push(node_pin, cx);
        }
        this.queued = true;
        Poll::Pending
    }
}

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        if self.node.state == NOTIFIED_CONSUMED {
            return;
        }

        if self.queued && !self.notify.waiters.remove(&self.node) && self.node.state == NOTIFIED_ONE
        {
            let detached = self.notify.waiters.take_front_with(|next| {
                next.state = NOTIFIED_ONE;
            });
            if let Some(detached) = detached {
                detached.wake();
            } else {
                self.notify.has_permit.set(true);
            }
        }
    }
}

impl<'a> fmt::Debug for Notified<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notified")
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}
