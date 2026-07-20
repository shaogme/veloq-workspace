use std::marker::PhantomPinned;
use veloq_intrusive_linklist::{
    ConcurrentLink, Link, concurrent_intrusive_adapter, intrusive_adapter,
};
use veloq_waker::MwsrWaker;

pub struct GenericWaiterNode<L> {
    pub(crate) waker: MwsrWaker,
    pub(crate) link: L,
    pub(crate) kind: usize,
    _p: PhantomPinned,
}

impl<L> GenericWaiterNode<L> {
    pub fn new_with(link: L) -> Self {
        Self {
            waker: MwsrWaker::new(),
            link,
            kind: 0,
            _p: PhantomPinned,
        }
    }
}

pub type WaiterNode = GenericWaiterNode<Link>;

impl WaiterNode {
    pub fn new() -> Self {
        Self::new_with(Link::new())
    }
}

intrusive_adapter!(pub WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    pub const NEW: Self = Self;
}

pub type ConcurrentWaiterNode = GenericWaiterNode<ConcurrentLink>;

impl ConcurrentWaiterNode {
    pub fn new() -> Self {
        Self::new_with(ConcurrentLink::new())
    }
}

concurrent_intrusive_adapter!(pub ConcurrentWaiterAdapter = ConcurrentWaiterNode { link: ConcurrentLink });

impl ConcurrentWaiterAdapter {
    pub const NEW: Self = Self;
}
