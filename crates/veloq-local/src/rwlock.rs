use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    cell::{Cell, RefCell, UnsafeCell},
    fmt,
    future::Future,
    marker::PhantomPinned,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::common::update_waker;

const KIND_READER: usize = 0;
const KIND_WRITER: usize = 1;

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;
const STATE_CONSUMED: usize = 2;

struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    kind: usize,
    state: Cell<usize>,
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new(kind: usize) -> Self {
        Self {
            waker: RefCell::new(None),
            link: Link::new(),
            kind,
            state: Cell::new(STATE_WAITING),
            _p: PhantomPinned,
        }
    }
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    const NEW: Self = Self;
}

/// An asynchronous reader-writer lock for local/single-threaded contexts.
pub struct RwLock<T: ?Sized> {
    writer_locked: Cell<bool>,
    reader_count: Cell<usize>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
    data: UnsafeCell<T>,
}

impl<T> RwLock<T> {
    /// Creates a new `RwLock` in an unlocked state with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            writer_locked: Cell::new(false),
            reader_count: Cell::new(0),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `RwLock` in an unlocked state with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            writer_locked: Cell::new(false),
            reader_count: Cell::new(0),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Consumes the lock, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Returns `true` if the lock is currently held by a writer.
    pub fn is_write_locked(&self) -> bool {
        self.writer_locked.get()
    }

    /// Returns the current number of read locks held.
    pub fn read_count(&self) -> usize {
        self.reader_count.get()
    }

    /// Returns `true` if the lock is currently held by a writer or any reader.
    pub fn is_locked(&self) -> bool {
        self.writer_locked.get() || self.reader_count.get() > 0
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `RwLock` mutably, no actual locking needs to take place.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock for reading immediately.
    ///
    /// Returns `Some(RwLockReadGuard)` if successful, or `None` if the lock is held by a writer
    /// or if there are pending waiters.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        if self.writer_locked.get() || !self.waiters.borrow().is_empty() {
            return None;
        }

        let readers = self.reader_count.get();
        if readers == usize::MAX {
            return None;
        }

        self.reader_count.set(readers + 1);
        Some(RwLockReadGuard { lock: self })
    }

    /// Acquires the lock for reading asynchronously.
    pub fn read(&self) -> RwLockReadFuture<'_, T> {
        RwLockReadFuture {
            lock: self,
            node: WaiterNode::new(KIND_READER),
            queued: false,
            _pin: PhantomPinned,
        }
    }

    /// Attempts to acquire the lock for writing immediately.
    ///
    /// Returns `Some(RwLockWriteGuard)` if successful, or `None` if the lock is currently
    /// held by any reader or writer, or if there are pending waiters.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if !self.writer_locked.get()
            && self.reader_count.get() == 0
            && self.waiters.borrow().is_empty()
        {
            self.writer_locked.set(true);
            Some(RwLockWriteGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock for writing asynchronously.
    pub fn write(&self) -> RwLockWriteFuture<'_, T> {
        RwLockWriteFuture {
            lock: self,
            node: WaiterNode::new(KIND_WRITER),
            queued: false,
            _pin: PhantomPinned,
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RwLock");
        d.field("writer_locked", &self.writer_locked.get());
        d.field("reader_count", &self.reader_count.get());
        if !self.writer_locked.get() {
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

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(data: T) -> Self {
        Self::new(data)
    }
}

fn release_write_lock<T: ?Sized>(lock: &RwLock<T>) {
    let mut waiters = lock.waiters.borrow_mut();
    if let Some(front) = waiters.front().get()
        && front.kind == KIND_WRITER
    {
        let writer = waiters.pop_front().unwrap();
        lock.writer_locked.set(true);
        writer.state.set(STATE_GRANTED);
        let waker = writer.waker.borrow_mut().take();
        drop(waiters);
        if let Some(waker) = waker {
            waker.wake();
        }
        return;
    }

    lock.writer_locked.set(false);
    while let Some(front) = waiters.front().get() {
        if front.kind == KIND_READER {
            let reader = waiters.pop_front().unwrap();
            lock.reader_count.set(lock.reader_count.get() + 1);
            reader.state.set(STATE_GRANTED);
            let waker = reader.waker.borrow_mut().take();
            drop(waiters);
            if let Some(waker) = waker {
                waker.wake();
            }
            waiters = lock.waiters.borrow_mut();
        } else {
            break;
        }
    }
}

fn release_read_lock<T: ?Sized>(lock: &RwLock<T>) {
    let readers = lock.reader_count.get() - 1;
    lock.reader_count.set(readers);
    if readers == 0 {
        let mut waiters = lock.waiters.borrow_mut();
        if let Some(front) = waiters.front().get()
            && front.kind == KIND_WRITER
        {
            let writer = waiters.pop_front().unwrap();
            lock.writer_locked.set(true);
            writer.state.set(STATE_GRANTED);
            let waker = writer.waker.borrow_mut().take();
            drop(waiters);
            if let Some(waker) = waker {
                waker.wake();
            }
            return;
        }

        while let Some(front) = waiters.front().get() {
            if front.kind == KIND_READER {
                let reader = waiters.pop_front().unwrap();
                lock.reader_count.set(lock.reader_count.get() + 1);
                reader.state.set(STATE_GRANTED);
                let waker = reader.waker.borrow_mut().take();
                drop(waiters);
                if let Some(waker) = waker {
                    waker.wake();
                }
                waiters = lock.waiters.borrow_mut();
            } else {
                break;
            }
        }
    }
}

/// A RAII guard returned by `RwLock::read`.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        release_read_lock(self.lock);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// A RAII guard returned by `RwLock::write` and `RwLock::try_write`.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.with_mut(|ptr| ptr as *mut T) }
    }
}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    /// Downgrades the write guard to a read guard.
    pub fn downgrade(self) -> RwLockReadGuard<'a, T> {
        let lock = self.lock;
        mem::forget(self);

        lock.writer_locked.set(false);
        lock.reader_count.set(1);

        let mut waiters = lock.waiters.borrow_mut();
        while let Some(front) = waiters.front().get() {
            if front.kind == KIND_READER {
                let reader = waiters.pop_front().unwrap();
                lock.reader_count.set(lock.reader_count.get() + 1);
                reader.state.set(STATE_GRANTED);
                let waker = reader.waker.borrow_mut().take();
                drop(waiters);
                if let Some(waker) = waker {
                    waker.wake();
                }
                waiters = lock.waiters.borrow_mut();
            } else {
                break;
            }
        }

        RwLockReadGuard { lock }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        release_write_lock(self.lock);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// A future that resolves to a `RwLockReadGuard`.
pub struct RwLockReadFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.node.state.get() == STATE_GRANTED {
            this.queued = false;
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
        }

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if !this.lock.writer_locked.get() && this.lock.waiters.borrow().is_empty() {
            let readers = this.lock.reader_count.get();
            if readers == usize::MAX {
                panic!("RwLock reader count overflow");
            }
            this.lock.reader_count.set(readers + 1);
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
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

impl<'a, T: ?Sized> Drop for RwLockReadFuture<'a, T> {
    fn drop(&mut self) {
        if self.node.state.get() == STATE_CONSUMED {
            return;
        }

        if self.node.state.get() == STATE_GRANTED {
            release_read_lock(self.lock);
            return;
        }

        if self.queued && self.node.link.is_linked() {
            let mut waiters = self.lock.waiters.borrow_mut();
            unsafe {
                let ptr = NonNull::from(&self.node);
                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                cursor.remove();
            }
            if !self.lock.writer_locked.get()
                && self.lock.reader_count.get() == 0
                && let Some(front) = waiters.front().get()
                && front.kind == KIND_WRITER
            {
                let writer = waiters.pop_front().unwrap();
                self.lock.writer_locked.set(true);
                writer.state.set(STATE_GRANTED);
                let waker = writer.waker.borrow_mut().take();
                drop(waiters);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }
}

impl<'a, T: ?Sized> fmt::Debug for RwLockReadFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockReadFuture")
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}

/// A future that resolves to a `RwLockWriteGuard`.
pub struct RwLockWriteFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.node.state.get() == STATE_GRANTED {
            this.queued = false;
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
        }

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if !this.lock.writer_locked.get()
            && this.lock.reader_count.get() == 0
            && this.lock.waiters.borrow().is_empty()
        {
            this.lock.writer_locked.set(true);
            this.node.state.set(STATE_CONSUMED);
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
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

impl<'a, T: ?Sized> Drop for RwLockWriteFuture<'a, T> {
    fn drop(&mut self) {
        if self.node.state.get() == STATE_CONSUMED {
            return;
        }

        if self.node.state.get() == STATE_GRANTED {
            release_write_lock(self.lock);
            return;
        }

        if self.queued && self.node.link.is_linked() {
            let mut waiters = self.lock.waiters.borrow_mut();
            unsafe {
                let ptr = NonNull::from(&self.node);
                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                cursor.remove();
            }
            if !self.lock.writer_locked.get() {
                while let Some(front) = waiters.front().get() {
                    if front.kind == KIND_READER {
                        let reader = waiters.pop_front().unwrap();
                        self.lock.reader_count.set(self.lock.reader_count.get() + 1);
                        reader.state.set(STATE_GRANTED);
                        let waker = reader.waker.borrow_mut().take();
                        drop(waiters);
                        if let Some(waker) = waker {
                            waker.wake();
                        }
                        waiters = self.lock.waiters.borrow_mut();
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

impl<'a, T: ?Sized> fmt::Debug for RwLockWriteFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockWriteFuture")
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}
