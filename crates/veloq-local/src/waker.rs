use veloq_intrusive_linklist::{Link, intrusive_adapter};
use veloq_std::{marker::PhantomPinned, task::Waker};

pub struct WaiterNode {
    pub(crate) waker: Option<Waker>,
    pub(crate) link: Link,
    pub(crate) kind: usize,
    pub(crate) state: usize,
    _p: PhantomPinned,
}

impl WaiterNode {
    pub fn new() -> Self {
        Self {
            waker: None,
            link: Link::new(),
            kind: 0,
            state: 0,
            _p: PhantomPinned,
        }
    }

    pub fn new_with_kind(kind: usize) -> Self {
        Self {
            waker: None,
            link: Link::new(),
            kind,
            state: 0,
            _p: PhantomPinned,
        }
    }
}

impl Default for WaiterNode {
    fn default() -> Self {
        Self::new()
    }
}

intrusive_adapter!(pub WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    pub const NEW: Self = Self;
}
