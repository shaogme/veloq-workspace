use crate::waker::{WaiterAdapter, WaiterNode};
use veloq_intrusive_linklist::LinkedList;
use veloq_std::{
    cell::UnsafeCell,
    future::Future,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    sync::{
        SpinLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

/// An asynchronous mutual exclusion primitive.
pub struct Mutex<T: ?Sized> {
    state: AtomicUsize,
    waiters: SpinLock<LinkedList<WaiterAdapter>>,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Creates a new `Mutex` with the given data.
    #[cfg(not(feature = "loom"))]
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Creates a new `Mutex` with the given data.
    #[cfg(feature = "loom")]
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
            data: UnsafeCell::new(data),
        }
    }

    /// Consumes the mutex, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

const UNLOCKED: usize = 0;
const LOCKED: usize = 1;

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;

impl<T: ?Sized> Mutex<T> {
    /// Returns true if the lock is currently held.
    ///
    /// This function does not provide any synchronization guarantees, so the
    /// returned value is only a hint.
    pub fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != UNLOCKED
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to take place—
    /// the mutable borrow statically guarantees no locks exist.
    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: We have exclusive access to the Mutex, so we have exclusive access to the data.
        unsafe { &mut *self.data.with_mut(|ptr| ptr as *mut T) }
    }

    /// Attempts to acquire the lock immediately.
    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let waiters = self.waiters.lock();
        if waiters.with(|w| w.is_empty())
            && self
                .state
                .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            Some(MutexGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the lock asynchronously.
    pub fn lock(&self) -> MutexLockFuture<'_, T> {
        let mut node = WaiterNode::new();
        node.kind = STATE_WAITING;
        MutexLockFuture {
            lock: self,
            node,
            queued: false,
            _pin: PhantomPinned,
        }
    }
}

/// A RAII guard returned by `Mutex::lock` and `Mutex::try_lock`.
pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
}

// SAFETY: MutexGuard gives exclusive access to the underlying data.
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}
unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns a reference to the [`Mutex`] that this guard was acquired from.
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.lock
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &*self.lock.data.with(|ptr| ptr as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We hold the lock.
        unsafe { &mut *self.lock.data.with_mut(|ptr| ptr as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let mut waiters = self.lock.waiters.lock();
        waiters.with_mut(|w| {
            if let Some(mut node) = w.pop_front() {
                // 1. 持锁者在释放且队列非空时，直接向队头节点授予锁（STATE_GRANTED），并将状态维持为锁定，外部不能插队。
                // SAFETY: We do not move the node out of Pin.
                unsafe {
                    node.as_mut().get_unchecked_mut().kind = STATE_GRANTED;
                }
                node.as_ref().waker.wake();
            } else {
                self.lock.state.store(UNLOCKED, Ordering::Release);
            }
        });
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
        // SAFETY: We do not move `node`.
        let this = unsafe { self.get_unchecked_mut() };

        // 2. 被唤醒者无需重新参与非公平抢锁，直接接收锁所有权，从源头杜绝唤醒丢失。
        if this.queued {
            let waiters = this.lock.waiters.lock();
            let is_granted = waiters.with(|_| this.node.kind == STATE_GRANTED);
            if is_granted {
                this.queued = false;
                drop(waiters);
                return Poll::Ready(MutexGuard { lock: this.lock });
            }

            // Still waiting in queue, refresh waker.
            unsafe {
                this.node.waker.register(cx.waker());
            }
            drop(waiters);
            return Poll::Pending;
        }

        #[cfg(not(feature = "loom"))]
        let mut spin_count = 0;

        #[cfg_attr(feature = "loom", allow(clippy::never_loop))]
        loop {
            // Fast path: try to acquire if unlocked (queue is empty when UNLOCKED).
            if this
                .lock
                .state
                .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Poll::Ready(MutexGuard { lock: this.lock });
            }

            // Optimization: Bounded Spinning
            #[cfg(not(feature = "loom"))]
            if spin_count < 100 {
                spin_count += 1;
                veloq_std::hint::spin_loop();
                if this.lock.state.load(Ordering::Relaxed) == UNLOCKED {
                    continue;
                }
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            let mut waiters = this.lock.waiters.lock();

            // Double check: if it became unlocked while acquiring waiters lock
            let is_empty = waiters.with(|w| w.is_empty());
            if is_empty
                && this
                    .lock
                    .state
                    .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                drop(waiters);
                return Poll::Ready(MutexGuard { lock: this.lock });
            }

            this.node.kind = STATE_WAITING;
            unsafe {
                let node_pin = Pin::new_unchecked(&mut this.node);
                waiters.with_mut(|w| w.push_back(node_pin));
            }
            this.queued = true;
            drop(waiters);
            return Poll::Pending;
        }
    }
}

impl<'a, T: ?Sized> Drop for MutexLockFuture<'a, T> {
    fn drop(&mut self) {
        if self.queued {
            let mut waiters = self.lock.waiters.lock();
            waiters.with_mut(|w| {
                if self.node.kind == STATE_GRANTED {
                    // 3. 若等待者在持有 STATE_GRANTED 时被取消（Drop），安全地将所有权级联顺延给下一等待者或重置为 UNLOCKED。
                    if let Some(mut next_node) = w.pop_front() {
                        // SAFETY: We do not move the node out of Pin.
                        unsafe {
                            next_node.as_mut().get_unchecked_mut().kind = STATE_GRANTED;
                        }
                        next_node.as_ref().waker.wake();
                    } else {
                        self.lock.state.store(UNLOCKED, Ordering::Release);
                    }
                } else {
                    let is_linked = self.node.link.is_linked();
                    if is_linked {
                        unsafe {
                            let ptr = NonNull::from(&self.node);
                            let mut cursor = w.cursor_mut_from_ptr(ptr);
                            cursor.remove();
                        }
                    }
                }
            });
            self.queued = false;
        }
    }
}
