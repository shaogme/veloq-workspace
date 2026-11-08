use core::ops::{Deref, DerefMut};

use veloq_intrusive_linklist::{
    ConcurrentLink, Link, concurrent_intrusive_adapter, intrusive_adapter,
};
use veloq_std::{error::Error, fmt, marker::PhantomPinned};
use veloq_waker::MwsrWaker;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> SendError<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

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

impl<T> Error for SendError<T> {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    Full(T),
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

impl<T> Error for TrySendError<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on a closed channel")
    }
}

impl Error for RecvError {}

pub(crate) struct WaiterNodeParts {
    pub(crate) waker: MwsrWaker,
    pub(crate) kind: usize,
    pub(crate) state: usize,
}

pub(crate) struct GenericWaiterNode<L> {
    link: L,
    parts: WaiterNodeParts,
    _pin: PhantomPinned,
}

impl<L> GenericWaiterNode<L> {
    fn new_with(link: L) -> Self {
        Self {
            link,
            parts: WaiterNodeParts {
                waker: MwsrWaker::new(),
                kind: 0,
                state: 0,
            },
            _pin: PhantomPinned,
        }
    }
}

impl<L> GenericWaiterNode<L> {
    pub(crate) fn link(&self) -> &L {
        &self.link
    }
}

impl<L> Deref for GenericWaiterNode<L> {
    type Target = WaiterNodeParts;

    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

impl<L> DerefMut for GenericWaiterNode<L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.parts
    }
}

pub(crate) type WaiterNode = GenericWaiterNode<Link>;

impl WaiterNode {
    pub(crate) fn new() -> Self {
        Self::new_with(Link::new())
    }
}

intrusive_adapter!(pub(crate) WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    pub(crate) const NEW: Self = Self;
}

pub(crate) type ConcurrentWaiterNode = GenericWaiterNode<ConcurrentLink>;

impl ConcurrentWaiterNode {
    pub(crate) fn new() -> Self {
        Self::new_with(ConcurrentLink::new())
    }
}

concurrent_intrusive_adapter!(pub(crate) ConcurrentWaiterAdapter = ConcurrentWaiterNode { link: ConcurrentLink });

impl ConcurrentWaiterAdapter {
    pub(crate) const NEW: Self = Self;
}
