//! Multi-producer, single-consumer FIFO queue communication channel.

use crate::{
    error::Error,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{Thread, current, park, park_timeout},
    time::{Duration, Instant},
};

mod queue;
use queue::SegQueue;

/// An error returned from the [`Sender::send`] function.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T: fmt::Debug> Error for SendError<T> {}

/// An error returned from the [`Receiver::recv`] function.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on a closed channel")
    }
}

impl Error for RecvError {}

/// An error returned from the [`Receiver::try_recv`] function.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TryRecvError {
    /// The channel is currently empty, but the sender(s) are still active.
    Empty,
    /// All senders have been disconnected, and no more messages can be received.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a closed channel"),
        }
    }
}

impl Error for TryRecvError {}

/// An error returned from the [`Receiver::recv_timeout`] function.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecvTimeoutError {
    /// The timeout elapsed before a message was received.
    Timeout,
    /// All senders have been disconnected.
    Disconnected,
}

impl fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvTimeoutError::Timeout => write!(f, "timed out waiting on channel"),
            RecvTimeoutError::Disconnected => write!(f, "receiving on a closed channel"),
        }
    }
}

impl Error for RecvTimeoutError {}

struct Shared<T> {
    queue: SegQueue<T>,
    senders: AtomicUsize,
    receiver_alive: AtomicBool,
    blocked_thread: Mutex<Option<Thread>>,
}

/// The sending-half of a channel.
pub struct Sender<T> {
    inner: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::Release) == 1 {
            // Wake up receiver so it can notice that all senders have disconnected.
            let thread = self.inner.blocked_thread.lock().take();
            if let Some(thread) = thread {
                thread.unpark();
            }
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Sender<T> {
    /// Sends a value on this channel.
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if !self.inner.receiver_alive.load(Ordering::Acquire) {
            return Err(SendError(t));
        }
        self.inner.queue.push(t);
        let thread = self.inner.blocked_thread.lock().take();
        if let Some(thread) = thread {
            thread.unpark();
        }
        Ok(())
    }
}

/// The receiving-half of a channel.
pub struct Receiver<T> {
    inner: Arc<Shared<T>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_alive.store(false, Ordering::Release);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a value from the channel without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(val) = self.inner.queue.pop() {
            Ok(val)
        } else if self.inner.senders.load(Ordering::Acquire) == 0 {
            if let Some(val) = self.inner.queue.pop() {
                Ok(val)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Attempts to receive a value from the channel, blocking the current thread until one is available.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            if let Some(val) = self.inner.queue.pop() {
                return Ok(val);
            }
            if self.inner.senders.load(Ordering::Acquire) == 0 {
                if let Some(val) = self.inner.queue.pop() {
                    return Ok(val);
                }
                return Err(RecvError);
            }

            {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = Some(current());
            }

            if !self.inner.queue.is_empty() {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = None;
                continue;
            }

            if self.inner.senders.load(Ordering::Acquire) == 0 {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = None;
                if let Some(val) = self.inner.queue.pop() {
                    return Ok(val);
                }
                return Err(RecvError);
            }

            park();

            let mut blocked = self.inner.blocked_thread.lock();
            *blocked = None;
        }
    }

    /// Attempts to receive a value from the channel, blocking the current thread until one is available or a timeout occurs.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(val) = self.inner.queue.pop() {
                return Ok(val);
            }
            if self.inner.senders.load(Ordering::Acquire) == 0 {
                if let Some(val) = self.inner.queue.pop() {
                    return Ok(val);
                }
                return Err(RecvTimeoutError::Disconnected);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            let remaining = deadline - now;

            {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = Some(current());
            }

            if !self.inner.queue.is_empty() {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = None;
                continue;
            }

            if self.inner.senders.load(Ordering::Acquire) == 0 {
                let mut blocked = self.inner.blocked_thread.lock();
                *blocked = None;
                if let Some(val) = self.inner.queue.pop() {
                    return Ok(val);
                }
                return Err(RecvTimeoutError::Disconnected);
            }

            park_timeout(remaining);

            let mut blocked = self.inner.blocked_thread.lock();
            *blocked = None;
        }
    }

    /// Creates an iterator that will block when there are no elements.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter { rx: self }
    }

    /// Creates an iterator that will never block.
    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter { rx: self }
    }
}

/// An iterator over the values received from a [`Receiver`].
pub struct Iter<'a, T> {
    rx: &'a Receiver<T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

/// An iterator over the values received from a [`Receiver`] that does not block.
pub struct TryIter<'a, T> {
    rx: &'a Receiver<T>,
}

impl<'a, T> Iterator for TryIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.try_recv().ok()
    }
}

/// An owning iterator over the values received from a [`Receiver`].
pub struct IntoIter<T> {
    rx: Receiver<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl<'a, T> IntoIterator for &'a Receiver<T> {
    type Item = T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> IntoIterator for Receiver<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { rx: self }
    }
}

/// Creates a new asynchronous channel, returning the sender/receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: SegQueue::new(),
        senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        blocked_thread: Mutex::new(None),
    });

    (
        Sender {
            inner: shared.clone(),
        },
        Receiver { inner: shared },
    )
}
