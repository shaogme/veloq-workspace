use crate::waker::{WaiterAdapter, WaiterNode};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    fmt,
    pin::Pin,
    ptr::NonNull,
    sync::SpinLock,
    task::{Context, Waker},
    vec::Vec,
};

pub(crate) enum RefreshResult {
    Linked,
    Detached(Option<Waker>),
}

impl RefreshResult {
    pub(crate) fn linked(self) -> bool {
        matches!(self, Self::Linked)
    }

    pub(crate) fn detached(self) -> bool {
        match self {
            Self::Linked => false,
            Self::Detached(waker) => {
                drop(waker);
                true
            }
        }
    }
}

/// A set of waiters whose callbacks are deferred until after a queue lock is released.
pub(crate) struct WakeBatch {
    wakers: Vec<Waker>,
}

impl WakeBatch {
    pub(crate) fn new() -> Self {
        Self { wakers: Vec::new() }
    }

    pub(crate) fn push(&mut self, waker: Option<Waker>) {
        if let Some(waker) = waker {
            self.wakers.push(waker);
        }
    }

    pub(crate) fn wake_all(self) {
        for waker in self.wakers {
            waker.wake();
        }
    }
}

/// A waiter removed from the queue, with ownership of its registered waker.
pub(crate) struct DetachedWaiter {
    pub(crate) waker: Option<Waker>,
}

impl DetachedWaiter {
    /// Wakes the detached waiter after the queue lock has been released.
    pub(crate) fn wake(self) {
        if let Some(waker) = self.waker {
            waker.wake();
        }
    }
}

/// A thread-safe FIFO wait queue for intrusive [`WaiterNode`] elements.
pub(crate) struct WaitQueue {
    inner: SpinLock<LinkedList<WaiterAdapter>>,
}

unsafe impl Send for WaitQueue {}
unsafe impl Sync for WaitQueue {}

impl WaitQueue {
    /// Creates a new, empty `WaitQueue`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            inner: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a new, empty `WaitQueue`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            inner: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Returns `true` if the queue currently contains no waiting nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let list = self.inner.lock();
        list.with(|w| w.is_empty())
    }

    /// Registers a waker without holding the queue lock, then rechecks the link.
    ///
    /// Returns [`RefreshResult::Linked`] while waiting, or
    /// [`RefreshResult::Detached`] after another operation dequeued the node.
    #[inline]
    pub fn refresh_waker(&self, node: &mut WaiterNode, cx: &mut Context<'_>) -> RefreshResult {
        let linked = {
            let list = self.inner.lock();
            list.with(|_| node.link.is_linked())
        };
        if !linked {
            return RefreshResult::Detached(None);
        }

        unsafe {
            node.waker.register(cx.waker());
        }

        let mut list = self.inner.lock();
        list.with_mut(|_| {
            if node.link.is_linked() {
                RefreshResult::Linked
            } else {
                RefreshResult::Detached(node.waker.take())
            }
        })
    }

    /// Enqueues a pinned waiter node into the back of the queue.
    #[inline]
    #[allow(dead_code)]
    pub fn push_back(&self, node: Pin<&mut WaiterNode>) {
        let mut list = self.inner.lock();
        list.with_mut(|w| unsafe {
            w.push_back(node);
        });
    }

    /// Registers the waker from `cx` into `node` and pushes it into the queue.
    #[inline]
    pub fn register_and_push(&self, mut node: Pin<&mut WaiterNode>, cx: &mut Context<'_>) {
        unsafe {
            node.as_mut().get_unchecked_mut().waker.register(cx.waker());
        }
        let mut list = self.inner.lock();
        unsafe {
            list.with_mut(|w| {
                w.push_back(node);
            });
        }
    }

    /// Removes a node from the wait queue if it is currently linked.
    ///
    /// Returns `true` if the node was found and removed, or `false` if it was already dequeued.
    pub fn remove(&self, node: &WaiterNode) -> bool {
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            if node.link.is_linked() {
                unsafe {
                    let ptr = NonNull::from(node);
                    let mut cursor = w.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
                true
            } else {
                false
            }
        })
    }

    /// Detaches the first waiting node in the queue, if any.
    pub fn take_front(&self) -> Option<DetachedWaiter> {
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            let node = w.pop_front()?;
            Some(DetachedWaiter {
                waker: node.as_ref().waker.take(),
            })
        })
    }

    /// Detaches the first waiter after applying a node state transition.
    pub fn take_front_with<F>(&self, f: F) -> Option<DetachedWaiter>
    where
        F: FnOnce(&mut WaiterNode),
    {
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            if let Some(mut node) = w.pop_front() {
                unsafe {
                    f(node.as_mut().get_unchecked_mut());
                }
                Some(DetachedWaiter {
                    waker: node.as_ref().waker.take(),
                })
            } else {
                None
            }
        })
    }

    /// Executes a closure with exclusive access to the underlying [`LinkedList`].
    pub fn with_lock<R>(&self, f: impl FnOnce(&mut LinkedList<WaiterAdapter>) -> R) -> R {
        let mut list = self.inner.lock();
        list.with_mut(f)
    }
}

impl Default for WaitQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WaitQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitQueue").finish_non_exhaustive()
    }
}
