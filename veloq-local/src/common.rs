use std::fmt;
use std::task::Waker;

/// Channel capacity configuration
#[derive(Debug, Clone, Copy)]
pub enum ChannelCapacity {
    Unbounded,
    Bounded(usize),
}

/// Error types for send operations
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// The receiver has been closed.
    Closed(T),
    /// The channel is full.
    Full(T),
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => write!(f, "SendError::Closed(..)"),
            SendError::Full(_) => write!(f, "SendError::Full(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => write!(f, "SendError::Closed(..)"),
            SendError::Full(_) => write!(f, "SendError::Full(..)"),
        }
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error type for non-blocking receive operations
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TryRecvError {
    Empty,
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "Channel is empty"),
            TryRecvError::Closed => write!(f, "Channel is closed"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Updates the stored waker if it does not match the current context waker.
pub fn update_waker(waker_slot: &mut Option<Waker>, new_waker: &Waker) {
    if let Some(w) = waker_slot {
        if w.will_wake(new_waker) {
            return;
        }
    }
    *waker_slot = Some(new_waker.clone());
}
