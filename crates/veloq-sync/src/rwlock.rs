use crate::{
    wait_queue::{DetachedWaiter, WaitQueue},
    waker::{WaiterAdapter, WaiterNode},
};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    cell::UnsafeCell,
    future::Future,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

/// An asynchronous reader-writer lock.
pub struct RwLock<T: ?Sized> {
    state: AtomicUsize,
    waiters: WaitQueue,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

const WRITER_LOCKED: usize = 1 << 0;
const CONTENDED: usize = 1 << 1;
const READER_UNIT: usize = 1 << 2;
const READER_MASK: usize = !(WRITER_LOCKED | CONTENDED);

const KIND_READER: usize = 0;
const KIND_WRITER: usize = 1;

const NODE_WAITING: usize = 0;
const NODE_GRANTED: usize = 1;

impl<T> RwLock<T> {
    /// Creates a new `RwLock` with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `RwLock` with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
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
    /// Returns true if the lock is currently held by a writer.
    pub fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WRITER_LOCKED != 0
    }

    /// Returns a mutable reference to the underlying data.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|p| p as *mut T) }
    }

    /// Attempts to acquire the lock for reading immediately.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state & (WRITER_LOCKED | CONTENDED) != 0 {
                return None;
            }
            if state & READER_MASK == READER_MASK {
                return None;
            }
            match self.state.compare_exchange(
                state,
                state + READER_UNIT,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(RwLockReadGuard { lock: self }),
                Err(next) => state = next,
            }
        }
    }

    /// Acquires the lock for reading asynchronously.
    pub fn read(&self) -> impl Future<Output = RwLockReadGuard<'_, T>> + '_ {
        RwLockReadFuture {
            lock: self,
            node: WaiterNode::new(),
            phase: Phase::Initial,
        }
    }

    /// Attempts to acquire the lock for writing immediately.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if self
            .state
            .compare_exchange(0, WRITER_LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(RwLockWriteGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock for writing asynchronously.
    pub fn write(&self) -> impl Future<Output = RwLockWriteGuard<'_, T>> + '_ {
        RwLockWriteFuture {
            lock: self,
            node: WaiterNode::new(),
            phase: Phase::Initial,
        }
    }
}

/// A RAII guard returned by [`RwLock::read`].
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let detached = self.lock.waiters.with_lock(|waiters| {
            let state = self.lock.state.load(Ordering::Relaxed);
            let readers = state & READER_MASK;
            assert!(readers >= READER_UNIT, "RwLock reader count underflow");
            let state = state - READER_UNIT;
            if readers == READER_UNIT {
                self.lock.state.store(state, Ordering::Release);
                grant_front_locked(self.lock, waiters, false)
            } else {
                store_state_for_queue(self.lock, waiters, state);
                None
            }
        });
        if let Some(detached) = detached {
            detached.wake();
        }
    }
}

/// A RAII guard returned by [`RwLock::write`].
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for RwLockWriteGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.with_mut(|p| p as *mut T) }
    }
}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    /// Downgrades the write guard to a read guard.
    pub fn downgrade(self) -> RwLockReadGuard<'a, T> {
        let lock = self.lock;
        let detached = lock.waiters.with_lock(|waiters| {
            let state = lock.state.load(Ordering::Relaxed);
            assert!(state & WRITER_LOCKED != 0, "RwLock writer bit is not set");
            let state = (state & !WRITER_LOCKED) + READER_UNIT;
            lock.state.store(state, Ordering::Release);
            grant_front_locked(lock, waiters, true)
        });
        mem::forget(self);
        if let Some(detached) = detached {
            detached.wake();
        }
        RwLockReadGuard { lock }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        let detached = self.lock.waiters.with_lock(|waiters| {
            let state = self.lock.state.load(Ordering::Relaxed);
            assert!(state & WRITER_LOCKED != 0, "RwLock writer bit is not set");
            self.lock
                .state
                .store(state & !WRITER_LOCKED, Ordering::Release);
            grant_front_locked(self.lock, waiters, false)
        });
        if let Some(detached) = detached {
            detached.wake();
        }
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

        loop {
            let mut acquired = false;
            let mut retry = false;
            unsafe {
                this.node.waker.register(cx.waker());
            }
            this.lock.waiters.with_lock(|waiters| {
                let state = this.lock.state.load(Ordering::Relaxed);
                if waiters.is_empty() && state & (WRITER_LOCKED | CONTENDED) == 0 {
                    if state & READER_MASK == READER_MASK {
                        panic!("RwLock reader count overflow");
                    }
                    match this.lock.state.compare_exchange(
                        state,
                        state + READER_UNIT,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            acquired = true;
                            return;
                        }
                        Err(_) => {
                            retry = true;
                            return;
                        }
                    }
                }

                this.node.kind = KIND_READER;
                this.node.state = NODE_WAITING;
                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                store_state_for_queue(this.lock, waiters, state | CONTENDED);
                this.phase = Phase::Waiting;
            });

            if retry {
                let stale_waker = this.node.waker.take();
                drop(stale_waker);
                continue;
            }

            if acquired {
                let stale_waker = this.node.waker.take();
                drop(stale_waker);
                this.phase = Phase::Completed;
                return Poll::Ready(RwLockReadGuard { lock: this.lock });
            }
            return Poll::Pending;
        }
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        let detached = match self.phase {
            Phase::Initial | Phase::Completed => None,
            Phase::Granted => cancel_granted(self.lock, &mut self.node),
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

        loop {
            let mut acquired = false;
            let mut retry = false;
            unsafe {
                this.node.waker.register(cx.waker());
            }
            this.lock.waiters.with_lock(|waiters| {
                let state = this.lock.state.load(Ordering::Relaxed);
                if waiters.is_empty() && state == 0 {
                    match this.lock.state.compare_exchange(
                        0,
                        WRITER_LOCKED,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            acquired = true;
                            return;
                        }
                        Err(_) => {
                            retry = true;
                            return;
                        }
                    }
                }

                this.node.kind = KIND_WRITER;
                this.node.state = NODE_WAITING;
                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                store_state_for_queue(this.lock, waiters, state | CONTENDED);
                this.phase = Phase::Waiting;
            });

            if retry {
                let stale_waker = this.node.waker.take();
                drop(stale_waker);
                continue;
            }

            if acquired {
                let stale_waker = this.node.waker.take();
                drop(stale_waker);
                this.phase = Phase::Completed;
                return Poll::Ready(RwLockWriteGuard { lock: this.lock });
            }
            return Poll::Pending;
        }
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        let detached = match self.phase {
            Phase::Initial | Phase::Completed => None,
            Phase::Granted => cancel_granted(self.lock, &mut self.node),
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

fn cancel_granted<T: ?Sized>(lock: &RwLock<T>, node: &mut WaiterNode) -> Option<DetachedWaiter> {
    lock.waiters
        .with_lock(|waiters| cancel_granted_locked(lock, waiters, node))
}

fn cancel_granted_locked<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &mut LinkedList<WaiterAdapter>,
    node: &WaiterNode,
) -> Option<DetachedWaiter> {
    let state = lock.state.load(Ordering::Relaxed);
    let state = if node.kind == KIND_READER {
        let readers = state & READER_MASK;
        assert!(readers >= READER_UNIT, "RwLock reader count underflow");
        state - READER_UNIT
    } else {
        assert!(state & WRITER_LOCKED != 0, "RwLock writer bit is not set");
        state & !WRITER_LOCKED
    };
    lock.state.store(state, Ordering::Release);
    let allow_reader = node.kind == KIND_READER && state & READER_MASK != 0;
    grant_front_locked(lock, waiters, allow_reader)
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
    let state = lock.state.load(Ordering::Relaxed);
    if kind == KIND_WRITER && state & READER_MASK != 0 {
        grant_front_locked(lock, waiters, true)
    } else {
        store_state_for_queue(lock, waiters, state);
        None
    }
}

fn store_state_for_queue<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &LinkedList<WaiterAdapter>,
    mut state: usize,
) {
    state &= !CONTENDED;
    if !waiters.is_empty() {
        state |= CONTENDED;
    }
    lock.state.store(state, Ordering::Release);
}

fn grant_front_locked<T: ?Sized>(
    lock: &RwLock<T>,
    waiters: &mut LinkedList<WaiterAdapter>,
    allow_reader_with_active_readers: bool,
) -> Option<DetachedWaiter> {
    let mut state = lock.state.load(Ordering::Relaxed);
    let readers = state & READER_MASK;
    let writer_locked = state & WRITER_LOCKED != 0;
    let kind = waiters.front_mut().get().map(|node| node.kind);
    let can_grant = match kind {
        Some(KIND_WRITER) => !writer_locked && readers == 0,
        Some(KIND_READER) => !writer_locked && (readers == 0 || allow_reader_with_active_readers),
        _ => false,
    };

    if can_grant {
        let mut node = waiters.pop_front().expect("wait queue front disappeared");
        let node_mut = unsafe { node.as_mut().get_unchecked_mut() };
        node_mut.state = NODE_GRANTED;
        match kind {
            Some(KIND_WRITER) => state |= WRITER_LOCKED,
            Some(KIND_READER) => {
                if readers == READER_MASK {
                    panic!("RwLock reader count overflow");
                }
                state += READER_UNIT;
            }
            _ => unreachable!(),
        }
        let waker = node_mut.waker.take();
        store_state_for_queue(lock, waiters, state);
        Some(DetachedWaiter { waker })
    } else {
        store_state_for_queue(lock, waiters, state);
        None
    }
}
