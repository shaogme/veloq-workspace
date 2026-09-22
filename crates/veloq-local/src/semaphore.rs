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
};

use crate::common::update_waker;

const STATE_WAITING: usize = 0;
const STATE_GRANTED: usize = 1;
const STATE_CLOSED: usize = 2;

/// Error returned when acquiring a permit from a closed semaphore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquireError;

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "semaphore closed")
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
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryAcquireError::Closed => write!(f, "semaphore closed"),
            TryAcquireError::NoPermits => write!(f, "no permits available"),
        }
    }
}

impl Error for TryAcquireError {}

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

/// An asynchronous counting semaphore for local/single-threaded contexts.
pub struct Semaphore {
    permits: Cell<usize>,
    closed: Cell<bool>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
}

impl Semaphore {
    /// Creates a new `Semaphore` with the given initial number of permits.
    #[cfg(not(feature = "loom"))]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: Cell::new(permits),
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Creates a new `Semaphore` with the given initial number of permits.
    #[cfg(feature = "loom")]
    pub fn new(permits: usize) -> Self {
        Self {
            permits: Cell::new(permits),
            closed: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
        }
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.get()
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

        let mut waiters = self.waiters.borrow_mut();
        while let Some(node) = waiters.pop_front() {
            node.state.set(STATE_CLOSED);
            if let Some(waker) = node.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    /// Adds `n` permits to the semaphore, waking pending waiters in FIFO order.
    pub fn add_permits(&self, n: usize) {
        self.release_permits(n);
    }

    /// Reduces the number of available permits by `n` without returning them.
    pub fn forget_permits(&self, n: usize) {
        let current = self.permits.get();
        self.permits.set(current.saturating_sub(n));
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

    fn wake_pending_waiters(waiters: &mut LinkedList<WaiterAdapter>, permits: &Cell<usize>) {
        let mut current = permits.get();
        if current == 0 {
            return;
        }

        while let Some(front) = waiters.front().get() {
            let needed = front.needed.get();
            if current >= needed {
                current -= needed;
                let node = waiters.pop_front().unwrap();
                node.state.set(STATE_GRANTED);
                if let Some(waker) = node.waker.borrow_mut().take() {
                    waker.wake();
                }
            } else {
                break;
            }
        }
        permits.set(current);
    }

    fn release_permits(&self, num: usize) {
        if num == 0 {
            return;
        }

        if self.closed.get() {
            self.permits.set(self.permits.get() + num);
            return;
        }

        let current = self.permits.get() + num;
        self.permits.set(current);

        let mut waiters = self.waiters.borrow_mut();
        Self::wake_pending_waiters(&mut waiters, &self.permits);
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Semaphore")
            .field("permits", &self.available_permits())
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
                return Poll::Ready(Err(AcquireError));
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
            return Poll::Ready(Err(AcquireError));
        }

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        if this.semaphore.closed.get() {
            return Poll::Ready(Err(AcquireError));
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
            let mut waiters = self.semaphore.waiters.borrow_mut();
            if self.node.state.get() == STATE_GRANTED {
                drop(waiters);
                self.semaphore.release_permits(self.permits);
            } else {
                unsafe {
                    let ptr = NonNull::from(&self.node);
                    let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                }
                Semaphore::wake_pending_waiters(&mut waiters, &self.semaphore.permits);
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
