use crate::shim::atomic::{AtomicUsize, Ordering};
use crate::shim::cell::UnsafeCell;
use crate::shim::lock::SpinLock;
use crate::waker::{WaiterAdapter, WaiterNode};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};
use veloq_intrusive_linklist::LinkedList;

/// An asynchronous reader-writer lock.
pub struct RwLock<T: ?Sized> {
    state: AtomicUsize,
    waiters: SpinLock<LinkedList<WaiterAdapter>>,
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

impl<T> RwLock<T> {
    /// Creates a new `RwLock` with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `RwLock` with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
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
        unsafe { &mut *self.data.get_mut() }
    }

    /// Attempts to acquire the lock for reading immediately.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state & WRITER_LOCKED != 0 {
                return None;
            }
            // Fairness: fail if there are waiters
            if state & CONTENDED != 0 {
                return None;
            }
            // Check for reader overflow
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
                Err(ex) => state = ex,
            }
        }
    }

    /// Acquires the lock for reading asynchronously.
    pub fn read(&self) -> RwLockReadFuture<'_, T> {
        RwLockReadFuture {
            lock: self,
            node: WaiterNode::new(),
            queued: false,
            woken: false,
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
    pub fn write(&self) -> RwLockWriteFuture<'_, T> {
        RwLockWriteFuture {
            lock: self,
            node: WaiterNode::new(),
            queued: false,
            woken: false,
        }
    }
}

/// A RAII guard returned by `RwLock::read`.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let prev = self.lock.state.fetch_sub(READER_UNIT, Ordering::Release);

        // If we were the last reader and there are waiters, wake one.
        if (prev & READER_MASK) == READER_UNIT && (prev & CONTENDED) != 0 {
            let mut waiters = self.lock.waiters.lock();
            if let Some(node) = waiters.pop_front() {
                node.as_ref().waker.wake();
            }
        }
    }
}

/// A RAII guard returned by `RwLock::write`.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for RwLockWriteGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get_mut() }
    }
}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    /// Downgrades the write guard to a read guard.
    pub fn downgrade(self) -> RwLockReadGuard<'a, T> {
        let lock = self.lock;
        let mut state = lock.state.load(Ordering::Relaxed);

        loop {
            // Replace WRITER_LOCKED with READER_UNIT
            // We assume WRITER_LOCKED is set because we have the guard.
            // We preserve CONTENDED bit if it is set.
            let new_state = (state & !WRITER_LOCKED) + READER_UNIT;

            match lock.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed, // Failure load ordering
            ) {
                Ok(_) => break,
                Err(ex) => state = ex,
            }
        }

        // Forget self ensures that `Drop` for RwLockWriteGuard is not called,
        // which would otherwise release the lock completely.
        std::mem::forget(self);

        // If there are waiters, and the first one is a reader, wake it up.
        // It will then likely wake subsequent readers (cascade).
        {
            let mut waiters = lock.waiters.lock();
            if let Some(node) = waiters.front_mut().get()
                && node.kind == KIND_READER
            {
                node.waker.wake();
            }
        }

        RwLockReadGuard { lock }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        // Optimistic release
        if self
            .lock
            .state
            .compare_exchange(WRITER_LOCKED, 0, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        // Slow path
        self.lock.state.store(0, Ordering::Release);

        // Wake the next waiter
        let mut waiters = self.lock.waiters.lock();
        if let Some(node) = waiters.pop_front() {
            node.as_ref().waker.wake();
        }
    }
}

pub struct RwLockReadFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    queued: bool,
    woken: bool,
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        #[cfg(not(feature = "loom"))]
        let mut spin_count = 0;

        loop {
            let state = this.lock.state.load(Ordering::Relaxed);

            // Try to acquire read lock if no writer
            if state & WRITER_LOCKED == 0 {
                // Fairness: if contended, don't barge in unless we are already queued or woken
                if this.queued || this.woken || (state & CONTENDED) == 0 {
                    if state & READER_MASK == READER_MASK {
                        panic!("RwLock reader count overflow");
                    }
                    if this
                        .lock
                        .state
                        .compare_exchange(
                            state,
                            state + READER_UNIT,
                            Ordering::Acquire,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        // Success
                        if this.queued || this.woken {
                            let mut waiters = this.lock.waiters.lock();
                            let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                            // Remove ourselves if still linked
                            if node_pin.as_ref().link.is_linked() {
                                unsafe {
                                    let ptr = NonNull::from(&*node_pin);
                                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                                    cursor.remove();
                                }
                            }

                            // Update CONTENDED bit based on remaining waiters
                            if waiters.is_empty() {
                                this.lock.state.fetch_and(!CONTENDED, Ordering::Release);
                            } else {
                                this.lock.state.fetch_or(CONTENDED, Ordering::Relaxed);
                            }

                            // Cascade wake: if next is reader, wake it
                            if let Some(next) = waiters.front_mut().get()
                                && next.kind == KIND_READER
                            {
                                next.waker.wake();
                            }

                            this.queued = false;
                            this.woken = false;
                        }
                        return Poll::Ready(RwLockReadGuard { lock: this.lock });
                    }
                    continue;
                }
            }

            // Lock held by writer or contended
            // Optimization: spinning
            #[cfg(not(feature = "loom"))]
            if !this.queued && spin_count < 100 {
                spin_count += 1;
                std::hint::spin_loop();
                continue;
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            if !this.queued {
                this.node.kind = KIND_READER;

                let mut waiters = this.lock.waiters.lock();

                // Double check state
                let current = this.lock.state.load(Ordering::Relaxed);

                // If writer lock is NOT held and there are no other waiters, we can just continue and try to acquire.
                // We rely on waiters.is_empty() for fairness check instead of just the CONTENDED bit,
                // because the CONTENDED bit might not be perfectly synchronized with the queue state yet,
                // or we might have raced.
                if (current & WRITER_LOCKED) == 0 && waiters.is_empty() {
                    drop(waiters);
                    continue;
                }

                // We are going to queue. Ensure CONTENDED bit is set.
                // We do this inside the lock to avoid race with the releaser.
                if (current & CONTENDED) == 0 {
                    this.lock.state.fetch_or(CONTENDED, Ordering::Relaxed);
                }

                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                this.queued = true;
            } else {
                // Check if woken
                let is_linked = {
                    let _w = this.lock.waiters.lock();
                    this.node.link.is_linked()
                };
                if !is_linked {
                    this.woken = true;
                    this.queued = false;
                    continue;
                }
            }

            return Poll::Pending;
        }
    }
}

impl<'a, T: ?Sized> Drop for RwLockReadFuture<'a, T> {
    fn drop(&mut self) {
        if self.queued {
            let mut waiters = self.lock.waiters.lock();
            if self.node.link.is_linked() {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
            }
        }
    }
}

pub struct RwLockWriteFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    node: WaiterNode,
    queued: bool,
    woken: bool,
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        #[cfg(not(feature = "loom"))]
        let mut spin_count = 0;

        loop {
            let state = this.lock.state.load(Ordering::Relaxed);

            // Try to acquire if unlocked (0) or only contended (CONTENDED)
            // But if we are queued or woken, we have priority.
            let can_acquire = if this.queued || this.woken {
                (state & !CONTENDED) == 0
            } else {
                state == 0 // Only barge in if strictly 0
            };

            if can_acquire {
                // If we acquire, do we need to set CONTENDED?
                // If we are queued, we check waiters.

                // Let's take global approach: try to swap state to WRITER_LOCKED | (existing CONTENDED)
                // Actually if we success, we are WRITER.

                // If state is UNLOCKED (0), try CAS to WRITER_LOCKED.
                // If state is CONTENDED, try CAS to WRITER_LOCKED | CONTENDED.

                let target = WRITER_LOCKED | (state & CONTENDED);

                if this
                    .lock
                    .state
                    .compare_exchange(state, target, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    if this.queued || this.woken {
                        let mut waiters = this.lock.waiters.lock();
                        let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                        if node_pin.as_ref().link.is_linked() {
                            unsafe {
                                let ptr = NonNull::from(&*node_pin);
                                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                                cursor.remove();
                            }
                        }
                        if waiters.is_empty() {
                            // Clear CONTENDED if no one else
                            this.lock.state.fetch_and(!CONTENDED, Ordering::Release);
                        } else {
                            // Ensure CONTENDED bit is set if there are other waiters
                            this.lock.state.fetch_or(CONTENDED, Ordering::Relaxed);
                        }
                        this.queued = false;
                        this.woken = false;
                    }
                    return Poll::Ready(RwLockWriteGuard { lock: this.lock });
                }
                continue;
            }

            // LOCKED, spin?
            #[cfg(not(feature = "loom"))]
            if !this.queued && spin_count < 100 {
                spin_count += 1;
                std::hint::spin_loop();
                continue;
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            if !this.queued {
                this.node.kind = KIND_WRITER;
                let mut waiters = this.lock.waiters.lock();

                // Double check
                let current = this.lock.state.load(Ordering::Relaxed);
                // If completely unlocked (no writer, no readers) and no waiters,
                // we can retry to acquire.
                if (current & (WRITER_LOCKED | READER_MASK)) == 0 && waiters.is_empty() {
                    drop(waiters);
                    continue;
                }

                // We are going to queue. Ensure CONTENDED bit is set.
                if (current & CONTENDED) == 0 {
                    this.lock.state.fetch_or(CONTENDED, Ordering::Relaxed);
                }

                unsafe {
                    let node_pin = Pin::new_unchecked(&mut this.node);
                    waiters.push_back(node_pin);
                }
                this.queued = true;
            } else {
                let is_linked = {
                    let _w = this.lock.waiters.lock();
                    this.node.link.is_linked()
                };
                if !is_linked {
                    this.woken = true;
                    this.queued = false;
                    continue;
                }
            }

            return Poll::Pending;
        }
    }
}

impl<'a, T: ?Sized> Drop for RwLockWriteFuture<'a, T> {
    fn drop(&mut self) {
        if self.queued {
            let mut waiters = self.lock.waiters.lock();
            if self.node.link.is_linked() {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
            }
        }
    }
}
