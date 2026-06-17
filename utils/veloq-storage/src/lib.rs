#[macro_use]
mod macros;

mod atomic;
mod local;
mod transfer;

pub use atomic::{
    ArcStrategy, AtomicLock, AtomicNonNullPtr, AtomicOptionArc, AtomicOptionBox, AtomicOptionPtr,
    AtomicStorage, AtomicWakerQueue, BoxStrategy, GenericAtomicOption, PhysicalHandle,
    PointerStrategy,
};
pub use local::{
    LocalLock, LocalStorage, LocalWakerQueue, NonNullPtr, OptionArc, OptionBox, OptionPtr, Usize,
};
pub use transfer::StaticTransfer;

use std::{
    ops::DerefMut,
    ptr::NonNull,
    sync::{Arc, atomic::Ordering},
    task::Waker,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Local,
    Atomic,
}

mod sealed {
    pub trait Sealed {}
}

pub trait StateGuard {
    /// 延迟释放内存，以防发生 UAF 漏洞。
    /// # Safety
    /// 传入的闭包执行的代码必须是安全的回收操作。
    unsafe fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static;
}

pub trait Storage: 'static {
    fn strategy_type() -> StrategyType;
    type Usize: StateInt;
    type OptionPtr<T>: StateOptionPtr<T>;
    type NonNullPtr<T>: StateNonNullPtr<T>;
    type Lock<T>: StateLock<T>;
    type WakerQueue: StateWakerQueue;
    type OptionBox<T: ?Sized + Send>: StateOptionBox<T>;
    type OptionArc<T: ?Sized + Send + Sync>: StateOptionArc<T>;
    type Guard: StateGuard;

    fn pin() -> Self::Guard;
}

/// 标记所有底层 primitive 都可跨线程共享的存储策略。
///
/// 该 trait 被 sealed，避免外部类型在未满足线程安全不变量时实现它。
pub trait ThreadSafeStorage: Storage + Send + Sync + sealed::Sealed {}

/// 标记只能在线程本地使用的存储策略。
///
/// 该 trait 被 sealed，用于把本地策略从跨线程 API 中排除。
pub trait LocalOnlyStorage: Storage + sealed::Sealed {}

pub trait StateInt {
    fn new(val: usize) -> Self;
    fn load(&self, order: Ordering) -> usize;
    fn store(&self, val: usize, order: Ordering);
    fn fetch_add(&self, val: usize, order: Ordering) -> usize;
    fn fetch_sub(&self, val: usize, order: Ordering) -> usize;
    fn fetch_and(&self, val: usize, order: Ordering) -> usize;
    fn fetch_or(&self, val: usize, order: Ordering) -> usize;
    fn compare_exchange(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, usize>;
    fn compare_exchange_weak(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, usize>;
}

pub trait StateOptionPtr<T> {
    fn new(ptr: Option<NonNull<T>>) -> Self;
    fn load(&self, order: Ordering) -> Option<NonNull<T>>;
    fn store(&self, ptr: Option<NonNull<T>>, order: Ordering);
    fn swap(&self, ptr: Option<NonNull<T>>, order: Ordering) -> Option<NonNull<T>>;
    fn compare_exchange(
        &self,
        current: Option<NonNull<T>>,
        new: Option<NonNull<T>>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Option<NonNull<T>>, Option<NonNull<T>>>;
    fn compare_exchange_weak(
        &self,
        current: Option<NonNull<T>>,
        new: Option<NonNull<T>>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Option<NonNull<T>>, Option<NonNull<T>>>;
}

pub trait StateNonNullPtr<T> {
    fn new(ptr: NonNull<T>) -> Self;
    fn load(&self, order: Ordering) -> NonNull<T>;
    fn store(&self, ptr: NonNull<T>, order: Ordering);
    fn swap(&self, ptr: NonNull<T>, order: Ordering) -> NonNull<T>;
    fn compare_exchange(
        &self,
        current: NonNull<T>,
        new: NonNull<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<NonNull<T>, NonNull<T>>;
    fn compare_exchange_weak(
        &self,
        current: NonNull<T>,
        new: NonNull<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<NonNull<T>, NonNull<T>>;
}

pub trait StateLock<T> {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a,
        T: 'a;
    fn new(val: T) -> Self;
    fn lock(&self) -> Self::Guard<'_>;
}

pub trait StateWakerQueue: 'static {
    fn new() -> Self;
    fn register(&self, waker: &Waker);
    fn take_all(&self) -> Vec<Waker>;
}

pub trait StateOptionBox<T: ?Sized + Send> {
    fn new(opt: Option<Box<T>>) -> Self;
    fn take(&self, order: Ordering) -> Option<Box<T>>;
    fn swap(&self, new: Option<Box<T>>, order: Ordering) -> Option<Box<T>>;
    fn store(&self, val: Option<Box<T>>, order: Ordering);
    fn compare_exchange_none(
        &self,
        new: Box<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Box<T>>;
}

pub trait StateOptionArc<T: ?Sized + Send + Sync> {
    fn new(opt: Option<Arc<T>>) -> Self;
    fn take(&self, order: Ordering) -> Option<Arc<T>>;
    fn store(&self, opt: Option<Arc<T>>, order: Ordering);
    fn load_clone(&self, order: Ordering) -> Option<Arc<T>>;
    fn compare_exchange_none(
        &self,
        new: Arc<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Arc<T>>;
}
