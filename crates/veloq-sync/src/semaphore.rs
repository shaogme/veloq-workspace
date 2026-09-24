use crate::wait_queue::WakeBatch;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    error::Error,
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::{
        SpinLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use veloq_waker::MwsrWaker;

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;
const STATE_CLOSED: usize = 2;

/// Error returned when acquiring permits fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The semaphore is closed.
    Closed,
    /// The requested number exceeds the semaphore capacity.
    TooManyPermits,
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AcquireError::Closed => write!(f, "semaphore closed"),
            AcquireError::TooManyPermits => write!(f, "requested permits exceed capacity"),
        }
    }
}

impl Error for AcquireError {}

/// Error returned when attempting to acquire a permit without waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryAcquireError {
    /// The semaphore is closed.
    Closed,
    /// No permits are currently available.
    NoPermits,
    /// The requested number exceeds the semaphore capacity.
    TooManyPermits,
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryAcquireError::Closed => write!(f, "semaphore closed"),
            TryAcquireError::NoPermits => write!(f, "no permits available"),
            TryAcquireError::TooManyPermits => write!(f, "requested permits exceed capacity"),
        }
    }
}

impl Error for TryAcquireError {}

/// Error returned when initial permits exceed a semaphore's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityError {
    /// The configured capacity.
    pub capacity: usize,
    /// The requested initial number of permits.
    pub initial: usize,
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "initial permits exceed semaphore capacity")
    }
}

impl Error for CapacityError {}

/// Error returned when adding permits would exceed a semaphore's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddPermitsError {
    /// The requested number of new permits.
    pub requested: usize,
    /// The remaining capacity at the time of the request.
    pub remaining: usize,
}

impl fmt::Display for AddPermitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "adding permits exceeds semaphore capacity")
    }
}

impl Error for AddPermitsError {}

struct SemaphoreWaiterNode {
    waker: MwsrWaker,
    link: Link,
    needed: usize,
    state: AtomicUsize,
    _pin: PhantomPinned,
}

impl SemaphoreWaiterNode {
    fn new(needed: usize) -> Self {
        Self {
            waker: MwsrWaker::new(),
            link: Link::new(),
            needed,
            state: AtomicUsize::new(STATE_WAITING),
            _pin: PhantomPinned,
        }
    }
}

intrusive_adapter!(SemaphoreWaiterAdapter = SemaphoreWaiterNode { link: Link });

impl SemaphoreWaiterAdapter {
    const NEW: Self = Self;
}

/// An asynchronous counting semaphore.
///
/// `capacity` bounds the total number of permits created by this semaphore.
/// `total_permits` includes available permits and permits held by guards or
/// already granted futures; it decreases only when permits are forgotten.
pub struct Semaphore {
    permits: AtomicUsize,
    total_permits: AtomicUsize,
    capacity: usize,
    closed: AtomicBool,
    waiters: SpinLock<LinkedList<SemaphoreWaiterAdapter>>,
}

unsafe impl Send for Semaphore {}
unsafe impl Sync for Semaphore {}

impl Semaphore {
    /// Creates a semaphore with `permits` available and a `usize::MAX` capacity.
    #[cfg(not(feature = "loom"))]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            total_permits: AtomicUsize::new(permits),
            capacity: usize::MAX,
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Creates a semaphore with `permits` available and a `usize::MAX` capacity.
    #[cfg(feature = "loom")]
    pub fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            total_permits: AtomicUsize::new(permits),
            capacity: usize::MAX,
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Creates an empty semaphore with the given maximum capacity.
    #[cfg(not(feature = "loom"))]
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            permits: AtomicUsize::new(0),
            total_permits: AtomicUsize::new(0),
            capacity,
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Creates an empty semaphore with the given maximum capacity.
    #[cfg(feature = "loom")]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            permits: AtomicUsize::new(0),
            total_permits: AtomicUsize::new(0),
            capacity,
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Creates a semaphore with an explicit capacity and initial permit count.
    pub fn with_capacity_and_permits(
        capacity: usize,
        initial: usize,
    ) -> Result<Self, CapacityError> {
        if initial > capacity {
            return Err(CapacityError { capacity, initial });
        }
        Ok(Self {
            permits: AtomicUsize::new(initial),
            total_permits: AtomicUsize::new(initial),
            capacity,
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        })
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire)
    }

    /// Returns the maximum number of permits this semaphore can create.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns true if the semaphore has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Closes the semaphore, waking all pending waiters with `AcquireError`.
    pub fn close(&self) {
        let batch = {
            let mut waiters = self.waiters.lock();
            if self.closed.swap(true, Ordering::Release) {
                return;
            }

            let mut batch = WakeBatch::new();
            waiters.with_mut(|w| {
                while let Some(node) = w.pop_front() {
                    batch.push(node.as_ref().waker.take());
                    node.as_ref().state.store(STATE_CLOSED, Ordering::Release);
                }
            });
            batch
        };
        batch.wake_all();
    }

    /// Adds `n` permits, waking pending waiters in FIFO order.
    ///
    /// If the capacity would be exceeded, this returns an error without
    /// changing the permit count or wait queue.
    pub fn add_permits(&self, n: usize) -> Result<(), AddPermitsError> {
        if n == 0 {
            return Ok(());
        }

        let batch = {
            let mut waiters = self.waiters.lock();
            let total = self.total_permits.load(Ordering::Relaxed);
            let remaining = self
                .capacity
                .checked_sub(total)
                .expect("semaphore total permits exceed capacity");
            if n > remaining {
                return Err(AddPermitsError {
                    requested: n,
                    remaining,
                });
            }

            let new_total = total
                .checked_add(n)
                .expect("semaphore total permit count overflow");
            let available = self.permits.load(Ordering::Relaxed);
            let new_available = available
                .checked_add(n)
                .expect("semaphore available permit count overflow");
            self.total_permits.store(new_total, Ordering::Release);
            self.permits.store(new_available, Ordering::Release);

            if self.closed.load(Ordering::Acquire) {
                WakeBatch::new()
            } else {
                waiters.with_mut(|w| Self::wake_pending_waiters(w, &self.permits))
            }
        };
        batch.wake_all();
        Ok(())
    }

    /// Reduces the number of available permits by `n` without returning them.
    ///
    /// Forgotten permits reduce the total capacity available for future adds.
    pub fn forget_permits(&self, n: usize) {
        let _waiters = self.waiters.lock();
        let current = self.permits.load(Ordering::Relaxed);
        let removed = core::cmp::min(n, current);
        let total = self.total_permits.load(Ordering::Relaxed);
        let new_total = total
            .checked_sub(removed)
            .expect("semaphore total permits below available permits");
        let new_permits = current - removed;
        self.total_permits.store(new_total, Ordering::Release);
        self.permits.store(new_permits, Ordering::Release);
    }

    /// Attempts to acquire a single permit immediately without waiting.
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_many(1)
    }

    /// Attempts to acquire `n` permits immediately without waiting.
    pub fn try_acquire_many(&self, n: usize) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        let waiters = self.waiters.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TryAcquireError::Closed);
        }
        if n > self.capacity {
            return Err(TryAcquireError::TooManyPermits);
        }
        if n == 0 {
            return Ok(SemaphorePermit {
                semaphore: self,
                permits: 0,
            });
        }
        let is_empty = waiters.with(|w| w.is_empty());
        if !is_empty {
            return Err(TryAcquireError::NoPermits);
        }
        let available = self.permits.load(Ordering::Relaxed);
        if available >= n {
            self.permits.store(available - n, Ordering::Release);
            Ok(SemaphorePermit {
                semaphore: self,
                permits: n,
            })
        } else {
            Err(TryAcquireError::NoPermits)
        }
    }

    /// Acquires a single permit asynchronously.
    pub fn acquire(&self) -> SemaphoreAcquireFuture<'_> {
        self.acquire_many(1)
    }

    /// Acquires `n` permits asynchronously.
    pub fn acquire_many(&self, n: usize) -> SemaphoreAcquireFuture<'_> {
        SemaphoreAcquireFuture {
            semaphore: self,
            node: SemaphoreWaiterNode::new(n),
            permits: n,
            queued: false,
            _pin: PhantomPinned,
        }
    }

    fn wake_pending_waiters(
        waiters: &mut LinkedList<SemaphoreWaiterAdapter>,
        permits: &AtomicUsize,
    ) -> WakeBatch {
        let mut batch = WakeBatch::new();
        let mut current = permits.load(Ordering::Relaxed);
        if current == 0 {
            return batch;
        }
        while let Some(front) = waiters.front().get() {
            let needed = front.needed;
            if current >= needed {
                current -= needed;
                let node = waiters.pop_front().unwrap();
                batch.push(node.as_ref().waker.take());
                node.as_ref().state.store(STATE_GRANTED, Ordering::Release);
            } else {
                break;
            }
        }
        permits.store(current, Ordering::Release);
        batch
    }

    fn release_permits(&self, num: usize) {
        if num == 0 {
            return;
        }

        let batch = {
            let mut waiters = self.waiters.lock();
            let available = self.permits.load(Ordering::Relaxed);
            let total = self.total_permits.load(Ordering::Relaxed);
            let outstanding = total
                .checked_sub(available)
                .expect("semaphore available permits exceed total permits");
            if num > outstanding {
                panic!("semaphore permit release exceeds outstanding permits");
            }
            let new_available = available
                .checked_add(num)
                .expect("semaphore available permit count overflow");
            self.permits.store(new_available, Ordering::Release);

            if self.closed.load(Ordering::Acquire) {
                WakeBatch::new()
            } else {
                waiters.with_mut(|w| Self::wake_pending_waiters(w, &self.permits))
            }
        };
        batch.wake_all();
    }

    fn forget_held_permits(&self, num: usize) {
        if num == 0 {
            return;
        }
        let _waiters = self.waiters.lock();
        let total = self.total_permits.load(Ordering::Relaxed);
        let new_total = total
            .checked_sub(num)
            .expect("semaphore permit forget exceeds total permits");
        self.total_permits.store(new_total, Ordering::Release);
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Semaphore")
            .field("permits", &self.available_permits())
            .field("capacity", &self.capacity)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// A RAII guard representing one or more permits acquired from a `Semaphore`.
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

unsafe impl Send for SemaphorePermit<'_> {}
unsafe impl Sync for SemaphorePermit<'_> {}

impl<'a> SemaphorePermit<'a> {
    /// Returns a reference to the `Semaphore` this permit was acquired from.
    pub fn semaphore(&self) -> &'a Semaphore {
        self.semaphore
    }

    /// Returns the number of permits held by this guard.
    pub fn num_permits(&self) -> usize {
        self.permits
    }

    /// Forgets the permit, preventing its permits from being returned to the semaphore.
    pub fn forget(mut self) {
        self.semaphore.forget_held_permits(self.permits);
        self.permits = 0;
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.release_permits(self.permits);
    }
}

impl fmt::Debug for SemaphorePermit<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphorePermit")
            .field("permits", &self.permits)
            .finish_non_exhaustive()
    }
}

/// A future that resolves to a `SemaphorePermit`.
pub struct SemaphoreAcquireFuture<'a> {
    semaphore: &'a Semaphore,
    node: SemaphoreWaiterNode,
    permits: usize,
    queued: bool,
    _pin: PhantomPinned,
}

unsafe impl Send for SemaphoreAcquireFuture<'_> {}
unsafe impl Sync for SemaphoreAcquireFuture<'_> {}

impl<'a> Future for SemaphoreAcquireFuture<'a> {
    type Output = Result<SemaphorePermit<'a>, AcquireError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.permits == 0 {
            if this.semaphore.is_closed() {
                return Poll::Ready(Err(AcquireError::Closed));
            }
            return Poll::Ready(Ok(SemaphorePermit {
                semaphore: this.semaphore,
                permits: 0,
            }));
        }

        if this.queued {
            let state = this.node.state.load(Ordering::Acquire);
            if state == STATE_GRANTED {
                this.queued = false;
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
            if state == STATE_CLOSED {
                this.queued = false;
                return Poll::Ready(Err(AcquireError::Closed));
            }

            // Registration may clone or drop user code, so it must happen before
            // taking the wait queue lock. The state is checked again afterwards.
            unsafe {
                this.node.waker.register(cx.waker());
            }
            let waiters = this.semaphore.waiters.lock();
            let state = this.node.state.load(Ordering::Acquire);
            if state == STATE_GRANTED {
                let stale_waker = this.node.waker.take();
                this.queued = false;
                drop(waiters);
                drop(stale_waker);
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
            if state == STATE_CLOSED {
                let stale_waker = this.node.waker.take();
                this.queued = false;
                drop(waiters);
                drop(stale_waker);
                return Poll::Ready(Err(AcquireError::Closed));
            }
            drop(waiters);
            return Poll::Pending;
        }

        let waiters = this.semaphore.waiters.lock();
        if this.semaphore.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(AcquireError::Closed));
        }
        if this.permits > this.semaphore.capacity {
            return Poll::Ready(Err(AcquireError::TooManyPermits));
        }

        let is_empty = waiters.with(|w| w.is_empty());
        if is_empty {
            let current = this.semaphore.permits.load(Ordering::Relaxed);
            if current >= this.permits {
                this.semaphore
                    .permits
                    .store(current - this.permits, Ordering::Release);
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
        }

        this.node.needed = this.permits;
        this.node.state.store(STATE_WAITING, Ordering::Relaxed);
        drop(waiters);

        // Register only after the locked fast path confirmed that waiting is
        // necessary. Recheck the semaphore after registration before linking.
        unsafe {
            this.node.waker.register(cx.waker());
        }
        let mut waiters = this.semaphore.waiters.lock();
        if this.semaphore.closed.load(Ordering::Acquire) {
            let stale_waker = this.node.waker.take();
            drop(waiters);
            drop(stale_waker);
            return Poll::Ready(Err(AcquireError::Closed));
        }
        if waiters.with(|w| w.is_empty()) {
            let current = this.semaphore.permits.load(Ordering::Relaxed);
            if current >= this.permits {
                this.semaphore
                    .permits
                    .store(current - this.permits, Ordering::Release);
                let stale_waker = this.node.waker.take();
                drop(waiters);
                drop(stale_waker);
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
        }
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            waiters.with_mut(|w| w.push_back(node_pin));
        }
        this.queued = true;
        Poll::Pending
    }
}

impl<'a> Drop for SemaphoreAcquireFuture<'a> {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }

        let mut return_permits = false;
        let batch = {
            let mut waiters = self.semaphore.waiters.lock();
            let state = self.node.state.load(Ordering::Acquire);
            if state == STATE_GRANTED {
                return_permits = true;
                WakeBatch::new()
            } else if self.node.link.is_linked() {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    waiters.with_mut(|w| {
                        let mut cursor = w.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                        Semaphore::wake_pending_waiters(w, &self.semaphore.permits)
                    })
                }
            } else {
                WakeBatch::new()
            }
        };
        if return_permits {
            self.semaphore.release_permits(self.permits);
        } else {
            batch.wake_all();
        }
        self.queued = false;
    }
}

impl<'a> fmt::Debug for SemaphoreAcquireFuture<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphoreAcquireFuture")
            .field("permits", &self.permits)
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}
