use super::{RecvError, RecvTimeoutError, SendError, TryRecvError};
use crate::{
    sync::{
        Arc, UnpoisonedMutex, UnpoisonedRwLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{Thread, current, park, park_timeout},
    time::{Duration, Instant},
};

use super::queue::SegQueue;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Open,
    Closed,
}

pub(super) struct Shared<T> {
    queue: SegQueue<T>,
    senders: AtomicUsize,
    lifecycle: UnpoisonedRwLock<Lifecycle>,
    blocked_thread: UnpoisonedMutex<Option<Thread>>,
}

/// The sending-half of an unbounded channel.
pub struct Sender<T> {
    pub(super) inner: Arc<Shared<T>>,
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

impl<T> core::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Sender<T> {
    /// Sends a value on this channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        {
            let lifecycle = self.inner.lifecycle.read();
            if *lifecycle == Lifecycle::Closed {
                return Err(SendError(value));
            }
            self.inner.queue.push(value);
        }

        let thread = self.inner.blocked_thread.lock().take();
        if let Some(thread) = thread {
            thread.unpark();
        }
        Ok(())
    }
}

impl<T> Shared<T> {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: SegQueue::new(),
            senders: AtomicUsize::new(1),
            lifecycle: UnpoisonedRwLock::new(Lifecycle::Open),
            blocked_thread: UnpoisonedMutex::new(None),
        })
    }

    pub(super) fn close(&self) {
        {
            let mut lifecycle = self.lifecycle.write();
            *lifecycle = Lifecycle::Closed;
        }

        let thread = self.blocked_thread.lock().take();
        while let Some(message) = self.queue.pop() {
            drop(message);
        }

        if let Some(thread) = thread {
            thread.unpark();
        }
    }

    pub(super) fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(value) = self.queue.pop() {
            Ok(value)
        } else if self.senders.load(Ordering::Acquire) == 0 {
            if let Some(value) = self.queue.pop() {
                Ok(value)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub(super) fn recv(&self) -> Result<T, RecvError> {
        loop {
            if let Some(value) = self.queue.pop() {
                return Ok(value);
            }
            if self.senders.load(Ordering::Acquire) == 0 {
                if let Some(value) = self.queue.pop() {
                    return Ok(value);
                }
                return Err(RecvError);
            }

            {
                let mut blocked = self.blocked_thread.lock();
                *blocked = Some(current());
            }

            if !self.queue.is_empty() {
                let mut blocked = self.blocked_thread.lock();
                *blocked = None;
                continue;
            }

            if self.senders.load(Ordering::Acquire) == 0 {
                let mut blocked = self.blocked_thread.lock();
                *blocked = None;
                if let Some(value) = self.queue.pop() {
                    return Ok(value);
                }
                return Err(RecvError);
            }

            park();

            let mut blocked = self.blocked_thread.lock();
            *blocked = None;
        }
    }

    pub(super) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = self.queue.pop() {
                return Ok(value);
            }
            if self.senders.load(Ordering::Acquire) == 0 {
                if let Some(value) = self.queue.pop() {
                    return Ok(value);
                }
                return Err(RecvTimeoutError::Disconnected);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            let remaining = deadline - now;

            {
                let mut blocked = self.blocked_thread.lock();
                *blocked = Some(current());
            }

            if !self.queue.is_empty() {
                let mut blocked = self.blocked_thread.lock();
                *blocked = None;
                continue;
            }

            if self.senders.load(Ordering::Acquire) == 0 {
                let mut blocked = self.blocked_thread.lock();
                *blocked = None;
                if let Some(value) = self.queue.pop() {
                    return Ok(value);
                }
                return Err(RecvTimeoutError::Disconnected);
            }

            park_timeout(remaining);

            let mut blocked = self.blocked_thread.lock();
            *blocked = None;
        }
    }
}
