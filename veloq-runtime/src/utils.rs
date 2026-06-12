pub use veloq_deque::{BatchStealResult, Deque, Steal};

pub mod fast_rand;
pub mod ownership;
pub mod storage;

pub use fast_rand::FastRand;
pub use ownership::{ArcOwnership, Ownership, RcOwnership};
pub use storage::{
    AtomicNonNullPtr, AtomicOptionArc, AtomicOptionBox, AtomicOptionPtr, OptionPtr, StateOptionPtr,
    StaticTransfer,
};
