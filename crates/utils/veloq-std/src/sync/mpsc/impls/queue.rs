#[cfg(feature = "loom")]
use crate::{collections::VecDeque, sync::UnpoisonedMutex};

#[cfg(feature = "loom")]
pub(crate) struct SegQueue<T> {
    inner: UnpoisonedMutex<VecDeque<T>>,
}

#[cfg(feature = "loom")]
impl<T> SegQueue<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: UnpoisonedMutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn push(&self, t: T) {
        self.inner.lock().push_back(t);
    }

    pub(crate) fn pop(&self) -> Option<T> {
        self.inner.lock().pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

#[cfg(not(feature = "loom"))]
pub(crate) use crossbeam_queue::SegQueue;
