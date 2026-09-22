use crate::{
    common::update_waker,
    waker::{WaiterAdapter, WaiterNode},
};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{cell::RefCell, fmt, pin::Pin, ptr::NonNull, task::Context};

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
    /// Returns `true` if the node is still linked (waiting), or `false` if it was already dequeued (notified).
    #[inline]
    pub fn update_waker_if_linked(&self, node: &mut WaiterNode, cx: &mut Context<'_>) -> bool {
        if node.link.is_linked() {
            update_waker(&mut node.waker, cx.waker());
            true
        } else {
            false
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
        unsafe {
            let node_mut = node.as_mut().get_unchecked_mut();
            update_waker(&mut node_mut.waker, cx.waker());
            self.inner.borrow_mut().push_back(node);
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

    /// Pops the front node, invokes `f` to modify it, and then wakes it up.
    ///
    /// Returns `true` if a waiter was popped and woken, or `false` if empty.
    pub fn wake_one_with<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut WaiterNode),
    {
        let waker = {
            let mut list = self.inner.borrow_mut();
            if let Some(mut node) = list.pop_front() {
                let node_mut = unsafe { node.as_mut().get_unchecked_mut() };
                f(node_mut);
                node_mut.waker.take()
            } else {
                return false;
            }
        };
        if let Some(waker) = waker {
            waker.wake();
            true
        } else {
            false
        }
    }

    /// Wakes up all waiting nodes currently in the queue.
    ///
    /// Returns the number of nodes that were woken.
    pub fn wake_all(&self) -> usize {
        let mut count = 0;
        let mut list = self.inner.borrow_mut();
        while let Some(mut node) = list.pop_front() {
            let waker = unsafe { node.as_mut().get_unchecked_mut().waker.take() };
            if let Some(waker) = waker {
                waker.wake();
            }
            count += 1;
        }
        count
    }

    /// Pops each waiting node, invokes `f` on it, and wakes it up.
    ///
    /// Returns the number of nodes that were woken.
    pub fn wake_all_with<F>(&self, mut f: F) -> usize
    where
        F: FnMut(&mut WaiterNode),
    {
        let mut count = 0;
        let mut list = self.inner.borrow_mut();
        while let Some(mut node) = list.pop_front() {
            let node_mut = unsafe { node.as_mut().get_unchecked_mut() };
            f(node_mut);
            let waker = node_mut.waker.take();
            if let Some(waker) = waker {
                waker.wake();
            }
            count += 1;
        }
        count
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
