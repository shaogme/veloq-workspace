use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    cell::{Cell, RefCell, UnsafeCell},
    fmt,
    future::Future,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::common::update_waker;

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;
const STATE_CONSUMED: usize = 2;

struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    state: Cell<usize>,
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new() -> Self {
        Self {
            waker: RefCell::new(None),
            link: Link::new(),
            state: Cell::new(STATE_WAITING),
            _p: PhantomPinned,
        }
    }
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    const NEW: Self = Self;
}

/// An asynchronous mutual exclusion primitive for local/single-threaded contexts.
pub struct Mutex<T: ?Sized> {
    locked: Cell<bool>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
    data: UnsafeCell<T>,
}

impl<T> Mutex<T> {
    /// Creates a new `Mutex` in an unlocked state with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            locked: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `Mutex` in an unlocked state with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            locked: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Consumes the mutex, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Returns `true` if the lock is currently held.
    pub fn is_locked(&self) -> bool {
        self.locked.get()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to take place.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock immediately.
    ///
    /// Returns `Some(MutexGuard)` if the lock was acquired, or `None` if the lock is already held.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if !self.locked.get() {
            self.locked.set(true);
            Some(MutexGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock asynchronously.
    pub fn lock(&self) -> MutexLockFuture<'_, T> {
        MutexLockFuture {
            lock: self,
            node: WaiterNode::new(),
            queued: false,
            _pin: PhantomPinned,
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Mutex");
        d.field("locked", &self.locked.get());
        if !self.locked.get() {
            unsafe {
                self.data.with(|data| {
                    d.field("data", &data);
                });
            }
        } else {
            d.field("data", &format_args!("<locked>"));
        }
        d.finish_non_exhaustive()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(data: T) -> Self {
        Self::new(data)
    }
}

/// A RAII guard returned by `Mutex::lock` and `Mutex::try_lock`.
pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns a reference to the [`Mutex`] that this guard was acquired from.
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.lock
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.with_mut(|ptr| ptr as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let mut waiters = self.lock.waiters.borrow_mut();
        if let Some(next) = waiters.pop_front() {
            next.state.set(STATE_GRANTED);
            let waker = next.waker.borrow_mut().take();
            drop(waiters);
            if let Some(waker) = waker {
                waker.wake();
            }
        } else {
            self.lock.locked.set(false);
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// A future that resolves to a `MutexGuard`.
pub struct MutexLockFuture<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'a, T: ?Sized> Future for MutexLockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.node.state.get() == STATE_GRANTED {
            this.queued = false;
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(MutexGuard { lock: this.lock });
        }

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if !this.lock.locked.get() {
            this.lock.locked.set(true);
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(MutexGuard { lock: this.lock });
        }

        this.node.state.set(STATE_WAITING);
        update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.lock.waiters.borrow_mut().push_back(node_pin);
        }
        this.queued = true;
        Poll::Pending
    }
}

impl<'a, T: ?Sized> Drop for MutexLockFuture<'a, T> {
    fn drop(&mut self) {
        if self.node.state.get() == STATE_CONSUMED {
            return;
        }

        if self.node.state.get() == STATE_GRANTED {
            let mut waiters = self.lock.waiters.borrow_mut();
            if let Some(next) = waiters.pop_front() {
                next.state.set(STATE_GRANTED);
                let waker = next.waker.borrow_mut().take();
                drop(waiters);
                if let Some(waker) = waker {
                    waker.wake();
                }
            } else {
                self.lock.locked.set(false);
            }
            return;
        }

        if self.queued && self.node.link.is_linked() {
            unsafe {
                let ptr = NonNull::from(&self.node);
                let mut waiters = self.lock.waiters.borrow_mut();
                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                cursor.remove();
            }
        }
    }
}

impl<'a, T: ?Sized> fmt::Debug for MutexLockFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutexLockFuture")
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}
