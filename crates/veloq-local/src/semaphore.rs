use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
    vec::Vec,
};

use crate::common::update_waker;

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

struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    needed: Cell<usize>,
    state: Cell<usize>,
    _pin: PhantomPinned,
}

impl WaiterNode {
    fn new(needed: usize) -> Self {
        Self {
            waker: RefCell::new(None),
            link: Link::new(),
            needed: Cell::new(needed),
            state: Cell::new(STATE_WAITING),
            _pin: PhantomPinned,
        }
    }
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    const NEW: Self = Self;
}

struct WakeBatch {
    wakers: Vec<Waker>,
}

impl WakeBatch {
    fn new() -> Self {
        Self { wakers: Vec::new() }
    }

    fn push(&mut self, waker: Option<Waker>) {
        if let Some(waker) = waker {
            self.wakers.push(waker);
        }
    }

    fn wake_all(self) {
        for waker in self.wakers {
            waker.wake();
        }
    }
}

/// An asynchronous counting semaphore for local/single-threaded contexts.
///
/// `capacity` bounds the total number of permits created by this semaphore.
/// `total_permits` includes available permits and permits held by guards or
/// already granted futures; it decreases only when permits are forgotten.
pub struct Semaphore {
    permits: Cell<usize>,
    total_permits: Cell<usize>,
    capacity: usize,
    closed: Cell<bool>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
}

impl Semaphore {
    /// Creates a semaphore with `permits` available and a `usize::MAX` capacity.
    #[cfg(not(feature = "loom"))]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: Cell::new(permits),
            total_permits: Cell::new(permits),
            capacity: usize::MAX,
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a semaphore with `permits` available and a `usize::MAX` capacity.
    #[cfg(feature = "loom")]
    pub fn new(permits: usize) -> Self {
        Self {
            permits: Cell::new(permits),
            total_permits: Cell::new(permits),
            capacity: usize::MAX,
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates an empty semaphore with the given maximum capacity.
    #[cfg(not(feature = "loom"))]
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            permits: Cell::new(0),
            total_permits: Cell::new(0),
            capacity,
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates an empty semaphore with the given maximum capacity.
    #[cfg(feature = "loom")]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            permits: Cell::new(0),
            total_permits: Cell::new(0),
            capacity,
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
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
            permits: Cell::new(initial),
            total_permits: Cell::new(initial),
            capacity,
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        })
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.get()
    }

    /// Returns the maximum number of permits this semaphore can create.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns true if the semaphore has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }

    /// Closes the semaphore, waking all pending waiters with `AcquireError`.
    pub fn close(&self) {
        if self.closed.get() {
            return;
        }
        self.closed.set(true);

        let batch = {
            let mut waiters = self.waiters.borrow_mut();
            let mut batch = WakeBatch::new();
            while let Some(node) = waiters.pop_front() {
                node.state.set(STATE_CLOSED);
                batch.push(node.waker.borrow_mut().take());
            }
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
            let mut waiters = self.waiters.borrow_mut();
            let total = self.total_permits.get();
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
            let available = self.permits.get();
            let new_available = available
                .checked_add(n)
                .expect("semaphore available permit count overflow");
            self.total_permits.set(new_total);
            self.permits.set(new_available);

            if self.closed.get() {
                WakeBatch::new()
            } else {
                Self::wake_pending_waiters(&mut waiters, &self.permits)
            }
        };
        batch.wake_all();
        Ok(())
    }

    /// Reduces the number of available permits by `n` without returning them.
    ///
    /// Forgotten permits reduce the total capacity available for future adds.
    pub fn forget_permits(&self, n: usize) {
        let current = self.permits.get();
        let removed = core::cmp::min(n, current);
        let total = self.total_permits.get();
        let new_total = total
            .checked_sub(removed)
            .expect("semaphore total permits below available permits");
        self.total_permits.set(new_total);
        self.permits.set(current - removed);
    }

    /// Attempts to acquire a single permit immediately without waiting.
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_many(1)
    }

    /// Attempts to acquire `n` permits immediately without waiting.
    pub fn try_acquire_many(&self, n: usize) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        if self.closed.get() {
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
        if !self.waiters.borrow().is_empty() {
            return Err(TryAcquireError::NoPermits);
        }

        let available = self.permits.get();
        if available >= n {
            self.permits.set(available - n);
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
            node: WaiterNode::new(n),
            permits: n,
            queued: false,
            _pin: PhantomPinned,
        }
    }

    fn wake_pending_waiters(
        waiters: &mut LinkedList<WaiterAdapter>,
        permits: &Cell<usize>,
    ) -> WakeBatch {
        let mut batch = WakeBatch::new();
        let mut current = permits.get();
        if current == 0 {
            return batch;
        }

        while let Some(front) = waiters.front().get() {
            let needed = front.needed.get();
            if current >= needed {
                current -= needed;
                let node = waiters.pop_front().unwrap();
                node.state.set(STATE_GRANTED);
                batch.push(node.waker.borrow_mut().take());
            } else {
                break;
            }
        }
        permits.set(current);
        batch
    }

    fn release_permits(&self, num: usize) {
        if num == 0 {
            return;
        }

        let batch = {
            let mut waiters = self.waiters.borrow_mut();
            let available = self.permits.get();
            let total = self.total_permits.get();
            let outstanding = total
                .checked_sub(available)
                .expect("semaphore available permits exceed total permits");
            if num > outstanding {
                panic!("semaphore permit release exceeds outstanding permits");
            }
            let new_available = available
                .checked_add(num)
                .expect("semaphore available permit count overflow");
            self.permits.set(new_available);

            if self.closed.get() {
                WakeBatch::new()
            } else {
                Self::wake_pending_waiters(&mut waiters, &self.permits)
            }
        };
        batch.wake_all();
    }

    fn forget_held_permits(&self, num: usize) {
        if num == 0 {
            return;
        }
        let total = self.total_permits.get();
        let new_total = total
            .checked_sub(num)
            .expect("semaphore permit forget exceeds total permits");
        self.total_permits.set(new_total);
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

/// A RAII guard representing one or more permits acquired from a local `Semaphore`.
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

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

/// A future that resolves to a `SemaphorePermit` in local contexts.
pub struct SemaphoreAcquireFuture<'a> {
    semaphore: &'a Semaphore,
    node: WaiterNode,
    permits: usize,
    queued: bool,
    _pin: PhantomPinned,
}

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

        let state = this.node.state.get();
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

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if this.semaphore.closed.get() {
            return Poll::Ready(Err(AcquireError::Closed));
        }
        if this.permits > this.semaphore.capacity {
            return Poll::Ready(Err(AcquireError::TooManyPermits));
        }

        if this.semaphore.waiters.borrow().is_empty() {
            let current = this.semaphore.permits.get();
            if current >= this.permits {
                this.semaphore.permits.set(current - this.permits);
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
        }

        this.node.needed.set(this.permits);
        this.node.state.set(STATE_WAITING);
        update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.semaphore.waiters.borrow_mut().push_back(node_pin);
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

        let state = self.node.state.get();
        if state == STATE_GRANTED {
            self.semaphore.release_permits(self.permits);
            self.queued = false;
            return;
        }

        if self.node.link.is_linked() {
            let mut return_permits = false;
            let batch = {
                let mut waiters = self.semaphore.waiters.borrow_mut();
                if self.node.state.get() == STATE_GRANTED {
                    return_permits = true;
                    WakeBatch::new()
                } else {
                    unsafe {
                        let ptr = NonNull::from(&self.node);
                        let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                    }
                    Semaphore::wake_pending_waiters(&mut waiters, &self.semaphore.permits)
                }
            };
            if return_permits {
                self.semaphore.release_permits(self.permits);
            } else {
                batch.wake_all();
            }
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
