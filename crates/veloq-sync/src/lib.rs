pub mod mpmc;
pub mod mpsc;
pub mod mutex;
pub mod oneshot;
pub mod rwlock;
pub(crate) mod shim;
pub(crate) mod waker;

use std::fmt;

/// Error returned when sending to a closed channel.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned when trying to send to a full or closed channel.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The channel is closed.
    Closed(T),
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.debug_tuple("Full").finish_non_exhaustive(),
            TrySendError::Closed(_) => f.debug_tuple("Closed").finish_non_exhaustive(),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "sending on a full channel"),
            TrySendError::Closed(_) => write!(f, "sending on a closed channel"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error returned when trying to receive from an empty or closed channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// The channel is closed.
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

impl std::error::Error for TryRecvError {}

/// Error returned when receiving from a closed channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on a closed channel")
    }
}

impl std::error::Error for RecvError {}
