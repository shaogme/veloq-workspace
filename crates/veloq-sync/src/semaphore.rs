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
pub struct Semaphore {
    permits: AtomicUsize,
    closed: AtomicBool,
    waiters: SpinLock<LinkedList<SemaphoreWaiterAdapter>>,
}

unsafe impl Send for Semaphore {}
unsafe impl Sync for Semaphore {}

impl Semaphore {
    /// Creates a new `Semaphore` with the given initial number of permits.
    #[cfg(not(feature = "loom"))]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Creates a new `Semaphore` with the given initial number of permits.
    #[cfg(feature = "loom")]
    pub fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            closed: AtomicBool::new(false),
            waiters: SpinLock::new(LinkedList::new(SemaphoreWaiterAdapter::NEW)),
        }
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire)
    }

    /// Returns true if the semaphore has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Closes the semaphore, waking all pending waiters with `AcquireError`.
    pub fn close(&self) {
        let mut waiters = self.waiters.lock();
        if self.closed.swap(true, Ordering::Release) {
            return;
        }

        waiters.with_mut(|w| {
            while let Some(node) = w.pop_front() {
                node.as_ref().state.store(STATE_CLOSED, Ordering::Release);
                node.as_ref().waker.wake();
            }
        });
    }

    /// Adds `n` permits to the semaphore, waking pending waiters in FIFO order.
    pub fn add_permits(&self, n: usize) {
        self.release_permits(n);
    }

    /// Reduces the number of available permits by `n` without returning them.
    pub fn forget_permits(&self, n: usize) {
        let _waiters = self.waiters.lock();
        let current = self.permits.load(Ordering::Relaxed);
        let new_permits = current.saturating_sub(n);
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
    ) {
        let mut current = permits.load(Ordering::Relaxed);
        if current == 0 {
            return;
        }
        while let Some(front) = waiters.front().get() {
            let needed = front.needed;
            if current >= needed {
                current -= needed;
                let node = waiters.pop_front().unwrap();
                node.as_ref().state.store(STATE_GRANTED, Ordering::Release);
                node.as_ref().waker.wake();
            } else {
                break;
            }
        }
        permits.store(current, Ordering::Release);
    }

    fn release_permits(&self, num: usize) {
        if num == 0 {
            return;
        }

        let mut waiters = self.waiters.lock();
        if self.closed.load(Ordering::Acquire) {
            self.permits.fetch_add(num, Ordering::Release);
            return;
        }

        let current = self.permits.load(Ordering::Relaxed) + num;
        self.permits.store(current, Ordering::Relaxed);

        waiters.with_mut(|w| {
            Self::wake_pending_waiters(w, &self.permits);
        });
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
                return Poll::Ready(Err(AcquireError));
            }
            return Poll::Ready(Ok(SemaphorePermit {
                semaphore: this.semaphore,
                permits: 0,
            }));
        }

        if this.queued {
            let waiters = this.semaphore.waiters.lock();
            let state = this.node.state.load(Ordering::Acquire);
            if state == STATE_GRANTED {
                this.queued = false;
                drop(waiters);
                return Poll::Ready(Ok(SemaphorePermit {
                    semaphore: this.semaphore,
                    permits: this.permits,
                }));
            }
            if state == STATE_CLOSED {
                this.queued = false;
                drop(waiters);
                return Poll::Ready(Err(AcquireError));
            }
            unsafe {
                this.node.waker.register(cx.waker());
            }
            drop(waiters);
            return Poll::Pending;
        }

        let mut waiters = this.semaphore.waiters.lock();
        if this.semaphore.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(AcquireError));
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
        unsafe {
            this.node.waker.register(cx.waker());
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

        let mut waiters = self.semaphore.waiters.lock();
        let state = self.node.state.load(Ordering::Acquire);
        if state == STATE_GRANTED {
            drop(waiters);
            self.semaphore.release_permits(self.permits);
        } else if self.node.link.is_linked() {
            unsafe {
                let ptr = NonNull::from(&self.node);
                waiters.with_mut(|w| {
                    let mut cursor = w.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                    Semaphore::wake_pending_waiters(w, &self.semaphore.permits);
                });
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
