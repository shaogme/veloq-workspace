use crate::{
    wait_queue::{DetachedWaiter, WaitQueue},
    waker::{WaiterAdapter, WaiterNode},
};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    cell::{Cell, UnsafeCell},
    fmt,
    future::Future,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

const KIND_READER: usize = 0;
const KIND_WRITER: usize = 1;

const NODE_WAITING: usize = 0;
const NODE_GRANTED: usize = 1;

/// An asynchronous reader-writer lock for local/single-threaded contexts.
pub struct RwLock<T: ?Sized> {
    writer_locked: Cell<bool>,
    reader_count: Cell<usize>,
    waiters: WaitQueue,
    data: UnsafeCell<T>,
}

impl<T> RwLock<T> {
    /// Creates a new `RwLock` in an unlocked state with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            writer_locked: Cell::new(false),
            reader_count: Cell::new(0),
            waiters: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `RwLock` in an unlocked state with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            writer_locked: Cell::new(false),
            reader_count: Cell::new(0),
            waiters: WaitQueue::new(),
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
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock for reading immediately.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        if self.writer_locked.get() || !self.waiters.is_empty() {
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
    pub fn read(&self) -> impl Future<Output = RwLockReadGuard<'_, T>> + '_ {
        RwLockReadFuture {
            lock: self,
            node: WaiterNode::new_with_kind(KIND_READER),
            phase: Phase::Initial,
        }
    }

    /// Attempts to acquire the lock for writing immediately.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if !self.writer_locked.get() && self.reader_count.get() == 0 && self.waiters.is_empty() {
            self.writer_locked.set(true);
            Some(RwLockWriteGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock for writing asynchronously.
    pub fn write(&self) -> impl Future<Output = RwLockWriteGuard<'_, T>> + '_ {
        RwLockWriteFuture {
            lock: self,
            node: WaiterNode::new_with_kind(KIND_WRITER),
            phase: Phase::Initial,
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RwLock");
        debug.field("writer_locked", &self.writer_locked.get());
        debug.field("reader_count", &self.reader_count.get());
        if !self.writer_locked.get() {
            unsafe {
                self.data.with(|data| {
                    debug.field("data", &data);
                });
            }
        } else {
            debug.field("data", &format_args!("<locked>"));
        }
        debug.finish_non_exhaustive()
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

/// A RAII guard returned by [`RwLock::read`].
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
        let readers = self.lock.reader_count.get();
        assert!(readers > 0, "RwLock reader count underflow");
        self.lock.reader_count.set(readers - 1);
        let detached = if readers == 1 {
            self.lock
                .waiters
                .with_lock(|waiters| grant_front_locked(self.lock, waiters, false))
        } else {
            None
        };
        if let Some(detached) = detached {
            detached.wake();
        }
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

/// A RAII guard returned by [`RwLock::write`].
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
        assert!(lock.writer_locked.get(), "RwLock writer bit is not set");
        lock.writer_locked.set(false);
        lock.reader_count.set(1);
        let detached = lock
            .waiters
            .with_lock(|waiters| grant_front_locked(lock, waiters, true));
        if let Some(detached) = detached {
            detached.wake();
        }
        RwLockReadGuard { lock }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        assert!(
            self.lock.writer_locked.get(),
            "RwLock writer bit is not set"
        );
        self.lock.writer_locked.set(false);
        let detached = self
            .lock
            .waiters
            .with_lock(|waiters| grant_front_locked(self.lock, waiters, false));
        if let Some(detached) = detached {
            detached.wake();
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Initial,
    Waiting,
    Granted,
    Completed,
}

struct RwLockReadFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    phase: Phase,
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.phase == Phase::Completed {
            panic!("polled RwLockReadFuture after completion");
        }
        if this.phase == Phase::Waiting {
            if this.lock.waiters.refresh_waker(&mut this.node, cx).linked() {
                return Poll::Pending;
            }
            assert_eq!(this.node.state, NODE_GRANTED);
            this.phase = Phase::Granted;
        }
        if this.phase == Phase::Granted {
            let detached = this
                .lock
                .waiters
                .with_lock(|waiters| grant_front_locked(this.lock, waiters, true));
            this.phase = Phase::Completed;
            if let Some(detached) = detached {
                detached.wake();
            }
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
        }

        let mut acquired = false;
        if !this.lock.writer_locked.get() && this.lock.waiters.is_empty() {
            let readers = this.lock.reader_count.get();
            if readers == usize::MAX {
                panic!("RwLock reader count overflow");
            }
            this.lock.reader_count.set(readers + 1);
            acquired = true;
        } else {
            let new_waker = cx.waker().clone();
            let old_waker = this.node.waker.replace(new_waker);
            drop(old_waker);
            this.lock.waiters.with_lock(|waiters| {
                unsafe {
                    this.node.state = NODE_WAITING;
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                this.phase = Phase::Waiting;
            });
        }

        if acquired {
            this.phase = Phase::Completed;
            Poll::Ready(RwLockReadGuard { lock: this.lock })
        } else {
            Poll::Pending
        }
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        let detached = match self.phase {
            Phase::Initial | Phase::Completed => None,
            Phase::Granted => cancel_granted(self.lock, &self.node),
            Phase::Waiting => self.lock.waiters.with_lock(|waiters| {
                if self.node.state == NODE_GRANTED {
                    cancel_granted_locked(self.lock, waiters, &self.node)
                } else if self.node.state == NODE_WAITING && self.node.link.is_linked() {
                    remove_waiting_locked(self.lock, waiters, &self.node)
                } else {
                    None
                }
            }),
        };
        if let Some(detached) = detached {
            detached.wake();
        }
    }
}

struct RwLockWriteFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    phase: Phase,
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.phase == Phase::Completed {
            panic!("polled RwLockWriteFuture after completion");
        }
        if this.phase == Phase::Waiting {
            if this.lock.waiters.refresh_waker(&mut this.node, cx).linked() {
                return Poll::Pending;
            }
            assert_eq!(this.node.state, NODE_GRANTED);
            this.phase = Phase::Granted;
        }
        if this.phase == Phase::Granted {
            this.phase = Phase::Completed;
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
        }

        let mut acquired = false;
        if !this.lock.writer_locked.get()
            && this.lock.reader_count.get() == 0
            && this.lock.waiters.is_empty()
        {
            this.lock.writer_locked.set(true);
            acquired = true;
        } else {
            let new_waker = cx.waker().clone();
            let old_waker = this.node.waker.replace(new_waker);
            drop(old_waker);
            this.lock.waiters.with_lock(|waiters| {
                unsafe {
                    this.node.state = NODE_WAITING;
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                this.phase = Phase::Waiting;
            });
        }

        if acquired {
            this.phase = Phase::Completed;
            Poll::Ready(RwLockWriteGuard { lock: this.lock })
        } else {
            Poll::Pending
        }
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        let detached = match self.phase {
            Phase::Initial | Phase::Completed => None,
            Phase::Granted => cancel_granted(self.lock, &self.node),
            Phase::Waiting => self.lock.waiters.with_lock(|waiters| {
                if self.node.state == NODE_GRANTED {
                    cancel_granted_locked(self.lock, waiters, &self.node)
                } else if self.node.state == NODE_WAITING && self.node.link.is_linked() {
                    remove_waiting_locked(self.lock, waiters, &self.node)
                } else {
                    None
                }
            }),
        };
        if let Some(detached) = detached {
            detached.wake();
        }
    }
}

fn cancel_granted<T: ?Sized>(lock: &RwLock<T>, node: &WaiterNode) -> Option<DetachedWaiter> {
    lock.waiters
        .with_lock(|waiters| cancel_granted_locked(lock, waiters, node))
}

fn cancel_granted_locked<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &mut LinkedList<WaiterAdapter>,
    node: &WaiterNode,
) -> Option<DetachedWaiter> {
    if node.kind == KIND_READER {
        let readers = lock.reader_count.get();
        assert!(readers > 0, "RwLock reader count underflow");
        lock.reader_count.set(readers - 1);
        grant_front_locked(lock, waiters, readers > 1)
    } else {
        assert!(lock.writer_locked.get(), "RwLock writer bit is not set");
        lock.writer_locked.set(false);
        grant_front_locked(lock, waiters, false)
    }
}

fn remove_waiting_locked<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &mut LinkedList<WaiterAdapter>,
    node: &WaiterNode,
) -> Option<DetachedWaiter> {
    let kind = node.kind;
    unsafe {
        let ptr = NonNull::from(node);
        let mut cursor = waiters.cursor_mut_from_ptr(ptr);
        cursor.remove();
    }
    if kind == KIND_WRITER && lock.reader_count.get() > 0 {
        grant_front_locked(lock, waiters, true)
    } else {
        None
    }
}

fn grant_front_locked<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &mut LinkedList<WaiterAdapter>,
    allow_reader_with_active_readers: bool,
) -> Option<DetachedWaiter> {
    let readers = lock.reader_count.get();
    let writer_locked = lock.writer_locked.get();
    let kind = waiters.front_mut().get().map(|node| node.kind);
    let can_grant = match kind {
        Some(KIND_WRITER) => !writer_locked && readers == 0,
        Some(KIND_READER) => !writer_locked && (readers == 0 || allow_reader_with_active_readers),
        _ => false,
    };
    if !can_grant {
        return None;
    }

    let mut node = waiters.pop_front().expect("wait queue front disappeared");
    let node_mut = unsafe { node.as_mut().get_unchecked_mut() };
    node_mut.state = NODE_GRANTED;
    match kind {
        Some(KIND_WRITER) => lock.writer_locked.set(true),
        Some(KIND_READER) => {
            if readers == usize::MAX {
                panic!("RwLock reader count overflow");
            }
            lock.reader_count.set(readers + 1);
        }
        _ => unreachable!(),
    }
    let waker = node_mut.waker.take();
    Some(DetachedWaiter { waker })
}
