//! Multi-producer, single-consumer FIFO queue communication channels.

use core::{cell::Cell, marker::PhantomData};

use crate::{error::Error, fmt, sync::Arc, time::Duration};

mod bounded;
mod queue;
mod unbounded;

pub use bounded::SyncSender;
pub use unbounded::Sender;

/// An error returned from [`Sender::send`] or [`SyncSender::send`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T: fmt::Debug> Error for SendError<T> {}

/// An error returned from [`SyncSender::try_send`].
#[derive(PartialEq, Eq, Debug)]
pub enum TrySendError<T> {
    /// The channel is currently full, and the value was not sent.
    Full(T),
    /// All receivers have been disconnected, and the value was not sent.
    Disconnected(T),
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "sending on a full channel"),
            TrySendError::Disconnected(_) => write!(f, "sending on a closed channel"),
        }
    }
}

impl<T: fmt::Debug> Error for TrySendError<T> {}

/// An error returned from [`Receiver::recv`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on a closed channel")
    }
}

impl Error for RecvError {}

/// An error returned from [`Receiver::try_recv`].
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

/// An error returned from [`Receiver::recv_timeout`].
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

enum ReceiverInner<T> {
    Unbounded(Arc<unbounded::Shared<T>>),
    Bounded(Arc<bounded::Shared<T>>),
}

/// The receiving-half of a channel.
pub struct Receiver<T> {
    inner: ReceiverInner<T>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        match &self.inner {
            ReceiverInner::Unbounded(inner) => inner.close(),
            ReceiverInner::Bounded(inner) => inner.close(),
        }
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
        match &self.inner {
            ReceiverInner::Unbounded(inner) => inner.try_recv(),
            ReceiverInner::Bounded(inner) => inner.try_recv(),
        }
    }

    /// Attempts to receive a value from the channel, blocking the current thread until one is available.
    pub fn recv(&self) -> Result<T, RecvError> {
        match &self.inner {
            ReceiverInner::Unbounded(inner) => inner.recv(),
            ReceiverInner::Bounded(inner) => inner.recv(),
        }
    }

    /// Attempts to receive a value from the channel, blocking the current thread until one is available or a timeout occurs.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match &self.inner {
            ReceiverInner::Unbounded(inner) => inner.recv_timeout(timeout),
            ReceiverInner::Bounded(inner) => inner.recv_timeout(timeout),
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

/// Creates a new unbounded channel, returning the sender/receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = unbounded::Shared::new();

    (
        Sender {
            inner: shared.clone(),
        },
        Receiver {
            inner: ReceiverInner::Unbounded(shared),
            _not_sync: PhantomData,
        },
    )
}

/// Creates a new synchronous, bounded channel.
///
/// A `bound` greater than zero limits the number of values waiting in the
/// channel. A zero bound creates a rendezvous channel where each send waits
/// for a receiver to accept the value.
pub fn sync_channel<T>(bound: usize) -> (SyncSender<T>, Receiver<T>) {
    let shared = bounded::Shared::new(bound);

    (
        SyncSender {
            inner: shared.clone(),
        },
        Receiver {
            inner: ReceiverInner::Bounded(shared),
            _not_sync: PhantomData,
        },
    )
}
