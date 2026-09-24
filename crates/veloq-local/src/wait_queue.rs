use crate::waker::{WaiterAdapter, WaiterNode};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    cell::RefCell,
    fmt,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Waker},
};

pub(crate) enum RefreshResult {
    Linked,
    Detached,
}

impl RefreshResult {
    pub(crate) fn linked(self) -> bool {
        matches!(self, Self::Linked)
    }

    pub(crate) fn detached(self) -> bool {
        matches!(self, Self::Detached)
    }
}

/// A waiter removed from the queue, with ownership of its registered waker.
pub(crate) struct DetachedWaiter {
    pub(crate) waker: Option<Waker>,
}

impl DetachedWaiter {
    /// Wakes the detached waiter after the queue borrow has ended.
    pub(crate) fn wake(self) {
        if let Some(waker) = self.waker {
            waker.wake();
        }
    }
}

/// A FIFO wait queue for intrusive [`WaiterNode`] elements in local/single-threaded contexts.
pub(crate) struct WaitQueue {
    inner: RefCell<LinkedList<WaiterAdapter>>,
}

impl WaitQueue {
    /// Creates a new, empty `WaitQueue`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            inner: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a new, empty `WaitQueue`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Returns `true` if the queue currently contains no waiting nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Checks whether the given node is currently linked in the wait queue.
    #[inline]
    #[allow(dead_code)]
    pub fn is_linked(&self, node: &WaiterNode) -> bool {
        node.link.is_linked()
    }

    /// Checks if the node is linked in the wait queue, and if so, registers the waker from `cx`
    /// while checking linked status to prevent lost wakeups.
    ///
    /// Returns [`RefreshResult::Linked`] while waiting, or
    /// [`RefreshResult::Detached`] after another operation dequeued the node.
    #[inline]
    pub fn refresh_waker(&self, node: &mut WaiterNode, cx: &mut Context<'_>) -> RefreshResult {
        if !node.link.is_linked() {
            return RefreshResult::Detached;
        }

        let new_waker = cx.waker().clone();
        let old_waker = node.waker.replace(new_waker);
        drop(old_waker);

        let _list = self.inner.borrow_mut();
        if node.link.is_linked() {
            RefreshResult::Linked
        } else {
            RefreshResult::Detached
        }
    }

    /// Enqueues a pinned waiter node into the back of the queue.
    #[inline]
    pub fn push_back(&self, node: Pin<&mut WaiterNode>) {
        unsafe {
            self.inner.borrow_mut().push_back(node);
        }
    }

    /// Registers the waker from `cx` into `node` and pushes it into the queue.
    #[inline]
    pub fn register_and_push(&self, mut node: Pin<&mut WaiterNode>, cx: &mut Context<'_>) {
        let new_waker = cx.waker().clone();
        let old_waker = unsafe { node.as_mut().get_unchecked_mut().waker.replace(new_waker) };
        drop(old_waker);
        let mut list = self.inner.borrow_mut();
        unsafe {
            list.push_back(node);
        }
    }

    /// Removes a node from the wait queue if it is currently linked.
    ///
    /// Returns `true` if the node was found and removed, or `false` if it was already dequeued.
    pub fn remove(&self, node: &WaiterNode) -> bool {
        if node.link.is_linked() {
            unsafe {
                let ptr = NonNull::from(node);
                let mut list = self.inner.borrow_mut();
                let mut cursor = list.cursor_mut_from_ptr(ptr);
                cursor.remove();
            }
            true
        } else {
            false
        }
    }

    /// Detaches the first waiting node in the queue, if any.
    pub fn take_front(&self) -> Option<DetachedWaiter> {
        let mut list = self.inner.borrow_mut();
        let mut node = list.pop_front()?;
        let waker = unsafe { node.as_mut().get_unchecked_mut().waker.take() };
        Some(DetachedWaiter { waker })
    }

    /// Detaches the first waiting node after applying a node state transition.
    pub fn take_front_with<F>(&self, f: F) -> Option<DetachedWaiter>
    where
        F: FnOnce(&mut WaiterNode),
    {
        let mut list = self.inner.borrow_mut();
        let mut node = list.pop_front()?;
        let node_mut = unsafe { node.as_mut().get_unchecked_mut() };
        f(node_mut);
        Some(DetachedWaiter {
            waker: node_mut.waker.take(),
        })
    }

    /// Executes a closure with exclusive access to the underlying [`LinkedList`].
    pub fn with_lock<R>(&self, f: impl FnOnce(&mut LinkedList<WaiterAdapter>) -> R) -> R {
        let mut list = self.inner.borrow_mut();
        f(&mut list)
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
