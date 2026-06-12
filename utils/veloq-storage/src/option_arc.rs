use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::{ArcStrategy, GenericAtomicOption, StateOptionArc};

/// 一个专门用于原子存储 `Option<Arc<T>>` 的容器。
/// 直接存储 Arc 的原始指针，避免了额外的 Box 包装。
pub struct AtomicOptionArc<T: ?Sized>(GenericAtomicOption<Arc<T>, ArcStrategy<T>>);

unsafe impl<T: ?Sized + Send + Sync> Send for AtomicOptionArc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for AtomicOptionArc<T> {}

impl<T: ?Sized + Send + Sync> StateOptionArc<T> for AtomicOptionArc<T> {
    fn new(opt: Option<Arc<T>>) -> Self {
        Self(GenericAtomicOption::new(opt))
    }
    fn take(&self, order: Ordering) -> Option<Arc<T>> {
        self.0.take(order)
    }
    fn store(&self, opt: Option<Arc<T>>, order: Ordering) {
        self.0.store(opt, order)
    }
    fn load_clone(&self, order: Ordering) -> Option<Arc<T>> {
        self.0.load_clone(order)
    }
    fn compare_exchange_none(
        &self,
        new: Arc<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Arc<T>> {
        self.0.compare_exchange_none(new, success, failure)
    }
}

pub struct OptionArc<T: ?Sized>(Cell<Option<Arc<T>>>);

impl<T: ?Sized + Send + Sync> StateOptionArc<T> for OptionArc<T> {
    impl_cell_opt_methods!(Arc<T>);
    fn load_clone(&self, _order: Ordering) -> Option<Arc<T>> {
        let opt = self.0.take();
        let cloned = opt.clone();
        self.0.set(opt);
        cloned
    }
}
