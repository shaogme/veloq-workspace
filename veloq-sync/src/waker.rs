use std::marker::PhantomPinned;
use veloq_atomic_waker::AtomicWaker;
use veloq_intrusive_linklist::{ConcurrentLink, concurrent_intrusive_adapter};

pub struct WaiterNode {
    pub(crate) waker: AtomicWaker,
    pub(crate) link: ConcurrentLink,
    pub(crate) kind: usize,
    _p: PhantomPinned,
}

impl WaiterNode {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            link: ConcurrentLink::new(),
            kind: 0,
            _p: PhantomPinned,
        }
    }
}

concurrent_intrusive_adapter!(pub WaiterAdapter = WaiterNode { link: ConcurrentLink });

impl WaiterAdapter {
    pub const NEW: Self = Self;
}
