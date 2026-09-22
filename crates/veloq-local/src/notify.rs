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

use crate::common::update_waker;

const NOT_NOTIFIED: usize = 0;
const NOTIFIED_ONE: usize = 1;
const NOTIFIED_ALL: usize = 2;
const NOTIFIED_CONSUMED: usize = 3;

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

/// An asynchronous event notification primitive for local/single-threaded contexts.
///
/// A `Notify` can be used to wake a single waiting task via `notify_one`,
/// or all waiting tasks via `notify_waiters`.
pub struct Notify {
    has_permit: Cell<bool>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
}

impl Notify {
    /// Creates a new `Notify` in the empty state.
    pub const fn new() -> Self {
        Self {
            has_permit: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
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

        let mut waiters = self.waiters.borrow_mut();
        if let Some(node) = waiters.pop_front() {
            node.kind.set(NOTIFIED_ONE);
            let waker = node.waker.borrow_mut().take();
            drop(waiters);
            if let Some(waker) = waker {
                waker.wake();
            }
        } else {
            self.has_permit.set(true);
        }
    }

    /// Notifies all waiting tasks.
    ///
    /// If there are no waiting tasks, no permit is saved.
    pub fn notify_waiters(&self) {
        let mut waiters = self.waiters.borrow_mut();
        while let Some(node) = waiters.pop_front() {
            node.kind.set(NOTIFIED_ALL);
            if let Some(waker) = node.waker.borrow_mut().take() {
                waker.wake();
            }
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
        if this.queued || this.node.kind.get() == NOTIFIED_CONSUMED {
            return;
        }

        if this.notify.has_permit.get() {
            this.notify.has_permit.set(false);
            this.node.kind.set(NOTIFIED_CONSUMED);
            return;
        }

        this.node.kind.set(NOT_NOTIFIED);
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.notify.waiters.borrow_mut().push_back(node_pin);
        }
        this.queued = true;
    }
}

impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.node.kind.get() == NOTIFIED_CONSUMED {
            return Poll::Ready(());
        }

        if this.queued {
            let is_linked = this.node.link.is_linked();
            if !is_linked {
                this.queued = false;
                this.node.kind.set(NOTIFIED_CONSUMED);
                return Poll::Ready(());
            }
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if this.notify.has_permit.get() {
            this.notify.has_permit.set(false);
            this.node.kind.set(NOTIFIED_CONSUMED);
            return Poll::Ready(());
        }

        this.node.kind.set(NOT_NOTIFIED);
        update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.notify.waiters.borrow_mut().push_back(node_pin);
        }
        this.queued = true;
        Poll::Pending
    }
}

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        if self.node.kind.get() == NOTIFIED_CONSUMED {
            return;
        }

        if self.queued {
            let is_linked = self.node.link.is_linked();
            if is_linked {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut waiters = self.notify.waiters.borrow_mut();
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
            } else if self.node.kind.get() == NOTIFIED_ONE {
                let mut waiters = self.notify.waiters.borrow_mut();
                if let Some(next) = waiters.pop_front() {
                    next.kind.set(NOTIFIED_ONE);
                    let waker = next.waker.borrow_mut().take();
                    drop(waiters);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                } else {
                    self.notify.has_permit.set(true);
                }
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
