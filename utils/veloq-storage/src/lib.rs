use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Waker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Local,
    Atomic,
}

pub mod sealed {
    pub trait Sealed {}
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
    type Guard<'a>: std::ops::DerefMut<Target = T>
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

macro_rules! impl_state_int {
    ($ty:ty, $self:ident, $order:ident, $val:ident, $curr:ident, $new:ident, $success:ident, $failure:ident,
     new($new_val:ident) $new_expr:block,
     load() $load_expr:block,
     store($store_val:ident) $store_expr:block,
     fetch_add($add_val:ident) $add_expr:block,
     fetch_sub($sub_val:ident) $sub_expr:block,
     fetch_and($and_val:ident) $and_expr:block,
     fetch_or($or_val:ident) $or_expr:block,
     compare_exchange($ce_curr:ident, $ce_new:ident, $ce_s:ident, $ce_f:ident) $ce_expr:block,
     compare_exchange_weak($cew_curr:ident, $cew_new:ident, $cew_s:ident, $cew_f:ident) $cew_expr:block
    ) => {
        impl $crate::StateInt for $ty {
            fn new($new_val: usize) -> Self { $new_expr }
            fn load(&$self, $order: ::std::sync::atomic::Ordering) -> usize { $load_expr }
            fn store(&$self, $store_val: usize, $order: ::std::sync::atomic::Ordering) { $store_expr }
            fn fetch_add(&$self, $add_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $add_expr }
            fn fetch_sub(&$self, $sub_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $sub_expr }
            fn fetch_and(&$self, $and_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $and_expr }
            fn fetch_or(&$self, $or_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $or_expr }
            fn compare_exchange(&$self, $ce_curr: usize, $ce_new: usize, $ce_s: ::std::sync::atomic::Ordering, $ce_f: ::std::sync::atomic::Ordering) -> Result<usize, usize> { $ce_expr }
            fn compare_exchange_weak(&$self, $cew_curr: usize, $cew_new: usize, $cew_s: ::std::sync::atomic::Ordering, $cew_f: ::std::sync::atomic::Ordering) -> Result<usize, usize> { $cew_expr }
        }
    };
}

macro_rules! impl_ptr_state_wrapper {
    ($name:ident, $trait:ident, $val:ty, $inner_ty:ty, $self:ident, $order:ident,
     new($new_ptr:ident) $new_expr:block,
     load() $load_expr:block,
     store($store_ptr:ident) $store_expr:block,
     swap($swap_ptr:ident) $swap_expr:block,
     compare_exchange($ce_curr:ident, $ce_new:ident, $ce_s:ident, $ce_f:ident) $ce_expr:block,
     compare_exchange_weak($cew_curr:ident, $cew_new:ident, $cew_s:ident, $cew_f:ident) $cew_expr:block,
     $(unsafe_impl $unsafe_impl:item)*
    ) => {
        pub struct $name<T>($inner_ty);
        $( $unsafe_impl )*
        impl<T> $crate::$trait<T> for $name<T> {
            fn new(ptr: $val) -> Self { let $new_ptr = ptr; $new_expr }
            fn load(&$self, $order: ::std::sync::atomic::Ordering) -> $val { $load_expr }
            fn store(&$self, ptr: $val, $order: ::std::sync::atomic::Ordering) { let $store_ptr = ptr; $store_expr }
            fn swap(&$self, ptr: $val, $order: ::std::sync::atomic::Ordering) -> $val { let $swap_ptr = ptr; $swap_expr }
            fn compare_exchange(&$self, $ce_curr: $val, $ce_new: $val, $ce_s: ::std::sync::atomic::Ordering, $ce_f: ::std::sync::atomic::Ordering) -> Result<$val, $val> { $ce_expr }
            fn compare_exchange_weak(&$self, $cew_curr: $val, $cew_new: $val, $cew_s: ::std::sync::atomic::Ordering, $cew_f: ::std::sync::atomic::Ordering) -> Result<$val, $val> { $cew_expr }
        }
    };
}

macro_rules! impl_cell_opt_methods {
    ($val:ty) => {
        fn new(opt: Option<$val>) -> Self {
            Self(::std::cell::Cell::new(opt))
        }
        fn take(&self, _order: ::std::sync::atomic::Ordering) -> Option<$val> {
            self.0.take()
        }
        fn store(&self, val: Option<$val>, _order: ::std::sync::atomic::Ordering) {
            self.0.set(val);
        }
        fn compare_exchange_none(
            &self,
            new: $val,
            _success: ::std::sync::atomic::Ordering,
            _failure: ::std::sync::atomic::Ordering,
        ) -> Result<(), $val> {
            let old = self.0.take();
            if old.is_none() {
                self.0.set(Some(new));
                Ok(())
            } else {
                self.0.set(old);
                Err(new)
            }
        }
    };
}

// 声明私有子模块
mod atomic;
mod local;
mod option_arc;
mod option_box;
mod pointer;
mod transfer;

// 重新导出公共 API
pub use atomic::{AtomicLock, AtomicStorage, AtomicWakerQueue};
pub use local::{LocalLock, LocalStorage, LocalWakerQueue, Usize};
pub use option_arc::{AtomicOptionArc, OptionArc};
pub use option_box::{AtomicOptionBox, OptionBox};
pub use pointer::{
    ArcStrategy, AtomicNonNullPtr, AtomicOptionPtr, BoxStrategy, GenericAtomicOption, NonNullPtr,
    OptionPtr, PhysicalHandle, PointerStrategy,
};
pub use transfer::StaticTransfer;
