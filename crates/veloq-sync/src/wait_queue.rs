use crate::waker::{WaiterAdapter, WaiterNode};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{fmt, pin::Pin, ptr::NonNull, sync::SpinLock, task::Context};

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

    /// Checks whether the given node is currently linked in the wait queue.
    #[inline]
    pub fn is_linked(&self, node: &WaiterNode) -> bool {
        let list = self.inner.lock();
        list.with(|_| node.link.is_linked())
    }

    /// Checks if the node is linked in the wait queue, and if so, registers the waker from `cx`
    /// while holding the queue lock to prevent lost wakeups.
    ///
    /// Returns `true` if the node is still linked (waiting), or `false` if it was already dequeued (notified).
    #[inline]
    pub fn update_waker_if_linked(&self, node: &mut WaiterNode, cx: &mut Context<'_>) -> bool {
        let mut list = self.inner.lock();
        list.with_mut(|_| {
            if node.link.is_linked() {
                unsafe {
                    node.waker.register(cx.waker());
                }
                true
            } else {
                false
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
        let mut list = self.inner.lock();
        unsafe {
            node.as_mut().get_unchecked_mut().waker.register(cx.waker());
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

    /// Wakes up the first waiting node in the queue, if any.
    ///
    /// Returns `true` if a waiter was woken, or `false` if the queue was empty.
    pub fn wake_one(&self) -> bool {
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            if let Some(node) = w.pop_front() {
                node.as_ref().waker.wake();
                true
            } else {
                false
            }
        })
    }

    /// Pops the front node, invokes `f` to modify it, and then wakes it up.
    ///
    /// Returns `true` if a waiter was popped and woken, or `false` if empty.
    pub fn wake_one_with<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut WaiterNode),
    {
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            if let Some(mut node) = w.pop_front() {
                unsafe {
                    f(node.as_mut().get_unchecked_mut());
                }
                node.as_ref().waker.wake();
                true
            } else {
                false
            }
        })
    }

    /// Wakes up all waiting nodes currently in the queue.
    ///
    /// Returns the number of nodes that were woken.
    pub fn wake_all(&self) -> usize {
        let mut count = 0;
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            while let Some(node) = w.pop_front() {
                node.as_ref().waker.wake();
                count += 1;
            }
        });
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
        let mut list = self.inner.lock();
        list.with_mut(|w| {
            while let Some(mut node) = w.pop_front() {
                unsafe {
                    f(node.as_mut().get_unchecked_mut());
                }
                node.as_ref().waker.wake();
                count += 1;
            }
        });
        count
    }

    /// Executes a closure with exclusive access to the underlying [`LinkedList`].
    pub fn with_lock<R>(&self, f: impl FnOnce(&mut LinkedList<WaiterAdapter>) -> R) -> R {
        let mut list = self.inner.lock();
        list.with_mut(f)
    }

    /// Executes a closure with shared access to the underlying [`LinkedList`].
    pub fn with_lock_ref<R>(&self, f: impl FnOnce(&LinkedList<WaiterAdapter>) -> R) -> R {
        let list = self.inner.lock();
        list.with(f)
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
