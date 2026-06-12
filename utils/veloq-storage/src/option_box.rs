use std::cell::Cell;
use std::sync::atomic::Ordering;

use crate::{BoxStrategy, GenericAtomicOption, StateOptionBox};

/// 一个原子存储 `Option<Box<T>>` 的容器。
/// 针对 `!Sized` 类型（如 trait objects），它会自动处理双重包装以保持原子性。
pub struct AtomicOptionBox<T: ?Sized>(GenericAtomicOption<Box<T>, BoxStrategy<T>>);

unsafe impl<T: ?Sized + Send> Send for AtomicOptionBox<T> {}
unsafe impl<T: ?Sized + Send> Sync for AtomicOptionBox<T> {}

impl<T: ?Sized + Send> StateOptionBox<T> for AtomicOptionBox<T> {
    fn new(opt: Option<Box<T>>) -> Self {
        Self(GenericAtomicOption::new(opt))
    }
    fn take(&self, order: Ordering) -> Option<Box<T>> {
        self.0.take(order)
    }
    fn swap(&self, new: Option<Box<T>>, order: Ordering) -> Option<Box<T>> {
        self.0.swap(new, order)
    }
    fn store(&self, val: Option<Box<T>>, order: Ordering) {
        self.0.store(val, order)
    }
    fn compare_exchange_none(
        &self,
        new: Box<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Box<T>> {
        self.0.compare_exchange_none(new, success, failure)
    }
}

pub struct OptionBox<T: ?Sized>(Cell<Option<Box<T>>>);

impl<T: ?Sized + Send> StateOptionBox<T> for OptionBox<T> {
    impl_cell_opt_methods!(Box<T>);
    fn swap(&self, new: Option<Box<T>>, _order: Ordering) -> Option<Box<T>> {
        self.0.replace(new)
    }
}
