use std::sync::atomic::Ordering;

use crate::{AtomicOptionBox, StateOptionBox};

pub struct StaticTransfer<T>(Box<[AtomicOptionBox<T>]>);

unsafe impl<T: Send> Sync for StaticTransfer<T> {}

impl<T: Send> StaticTransfer<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self(
            items
                .into_iter()
                .map(|v| AtomicOptionBox::new(Some(Box::new(v))))
                .collect(),
        )
    }

    pub fn take(&self, index: usize) -> T {
        let boxed = self.0[index]
            .take(Ordering::Acquire)
            .expect("Worker item already taken or index out of bounds");
        *boxed
    }
}
