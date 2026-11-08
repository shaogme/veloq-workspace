use veloq_intrusive_linklist::{Link, intrusive_adapter};
use veloq_std::{error::Error, fmt, marker::PhantomPinned, task::Waker};

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

impl<T> Error for SendError<T> {}

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

impl Error for TryRecvError {}

pub(crate) struct WaiterNode {
    waker: Option<Waker>,
    link: Link,
    kind: usize,
    state: usize,
    _p: PhantomPinned,
}

impl WaiterNode {
    pub(crate) fn new() -> Self {
        Self::new_with_kind(0)
    }

    pub(crate) fn new_with_kind(kind: usize) -> Self {
        Self {
            waker: None,
            link: Link::new(),
            kind,
            state: 0,
            _p: PhantomPinned,
        }
    }

    pub(crate) fn state(&self) -> usize {
        self.state
    }

    pub(crate) fn set_state(&mut self, state: usize) {
        self.state = state;
    }

    pub(crate) fn kind(&self) -> usize {
        self.kind
    }

    pub(crate) fn is_linked(&self) -> bool {
        self.link.is_linked()
    }

    pub(crate) fn replace_waker(&mut self, new_waker: Waker) -> Option<Waker> {
        self.waker.replace(new_waker)
    }

    pub(crate) fn take_waker(&mut self) -> Option<Waker> {
        self.waker.take()
    }

    pub(crate) fn update_waker_slot(waker_slot: &mut Option<Waker>, new_waker: &Waker) {
        if waker_slot
            .as_ref()
            .is_some_and(|waker| waker.will_wake(new_waker))
        {
            return;
        }
        *waker_slot = Some(new_waker.clone());
    }
}

impl Default for WaiterNode {
    fn default() -> Self {
        Self::new()
    }
}

intrusive_adapter!(pub(crate) WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    pub(crate) const NEW: Self = Self;
}
