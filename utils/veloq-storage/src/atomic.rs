use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::task::Waker;

use crate::{
    StateGuard, StateLock, StateOptionArc, StateOptionBox, StateWakerQueue, Storage, StrategyType,
    ThreadSafeStorage, sealed,
};

pub struct AtomicStorage;
impl sealed::Sealed for AtomicStorage {}
impl ThreadSafeStorage for AtomicStorage {}

impl Storage for AtomicStorage {
    fn strategy_type() -> StrategyType {
        StrategyType::Atomic
    }
    type Usize = AtomicUsize;
    type OptionPtr<T> = AtomicOptionPtr<T>;
    type NonNullPtr<T> = AtomicNonNullPtr<T>;
    type Lock<T> = AtomicLock<T>;
    type WakerQueue = AtomicWakerQueue;
    type OptionBox<T: ?Sized + Send> = AtomicOptionBox<T>;
    type OptionArc<T: ?Sized + Send + Sync> = AtomicOptionArc<T>;
    type Guard = AtomicGuard;

    fn pin() -> Self::Guard {
        AtomicGuard(crossbeam_epoch::pin())
    }
}

pub struct AtomicLock<T>(parking_lot::Mutex<T>);
impl<T> StateLock<T> for AtomicLock<T> {
    type Guard<'a>
        = parking_lot::MutexGuard<'a, T>
    where
        T: 'a;
    fn new(val: T) -> Self {
        Self(parking_lot::Mutex::new(val))
    }
    fn lock(&self) -> Self::Guard<'_> {
        self.0.lock()
    }
}
unsafe impl<T> Send for AtomicLock<T> {}
unsafe impl<T> Sync for AtomicLock<T> {}

pub struct AtomicWakerQueue(parking_lot::Mutex<Vec<Waker>>);
impl StateWakerQueue for AtomicWakerQueue {
    fn new() -> Self {
        Self(parking_lot::Mutex::new(Vec::new()))
    }

    fn register(&self, waker: &Waker) {
        let mut wakers = self.0.lock();
        if !wakers.iter().any(|registered| registered.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn take_all(&self) -> Vec<Waker> {
        std::mem::take(&mut *self.0.lock())
    }
}
unsafe impl Send for AtomicWakerQueue {}
unsafe impl Sync for AtomicWakerQueue {}

impl_state_int!(
    AtomicUsize, self, order, val, curr, new, success, failure,
    new(v) { Self::new(v) },
    load() { self.load(order) },
    store(v) { self.store(v, order) },
    fetch_add(v) { self.fetch_add(v, order) },
    fetch_sub(v) { self.fetch_sub(v, order) },
    fetch_and(v) { self.fetch_and(v, order) },
    fetch_or(v) { self.fetch_or(v, order) },
    compare_exchange(c, n, s, f) { self.compare_exchange(c, n, s, f) },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange_weak(c, n, s, f) }
);

pub struct AtomicGuard(crossbeam_epoch::Guard);

impl StateGuard for AtomicGuard {
    unsafe fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.defer(f);
    }
}

// ==================== Pointer Helpers & Strategies ====================

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct PhysicalHandle(NonNull<u8>);

impl PhysicalHandle {
    /// 从原始指针创建句柄。
    ///
    /// # Safety
    /// `ptr` 必须是非空的且满足对齐要求。
    #[inline]
    pub unsafe fn from_ptr(ptr: *mut ()) -> Self {
        debug_assert!(!ptr.is_null());
        // SAFETY: 调用者必须保证 ptr 非空且对齐。
        unsafe { Self(NonNull::new_unchecked(ptr as *mut u8)) }
    }

    /// 获取内部原始指针。
    #[inline]
    pub fn as_ptr(self) -> *mut () {
        self.0.as_ptr() as *mut ()
    }
}

pub trait PointerStrategy<T> {
    /// 具体的中间类型，用于强类型化指针处理。
    type Raw: Copy;

    /// 将值转换为强类型的中间表示。
    fn to_raw(val: T) -> Self::Raw;

    /// 从中间类型恢复原始值。
    ///
    /// # Safety
    /// `raw` 必须是由 `to_raw` 生成的有效句柄。
    unsafe fn from_raw(raw: Self::Raw) -> T;

    /// 获取对值的引用。
    ///
    /// # Safety
    /// `raw` 指向的内容必须存活且有效。
    unsafe fn as_ref<'a>(raw: Self::Raw) -> &'a T;

    /// 将中间类型映射到物理存储指针（如 AtomicPtr 需要的 *mut ()）。
    fn to_physical(raw: Self::Raw) -> *mut ();

    /// 从物理存储指针恢复中间类型。
    ///
    /// # Safety
    /// `ptr` 必须是非空的物理指针。
    unsafe fn from_physical(ptr: *mut ()) -> Self::Raw;

    /// 返回表示空状态的物理指针值。
    fn physical_null() -> *mut ();
}

pub struct BoxStrategy<T: ?Sized>(std::marker::PhantomData<T>);
impl<T: ?Sized> PointerStrategy<Box<T>> for BoxStrategy<T> {
    type Raw = PhysicalHandle;

    fn to_raw(val: Box<T>) -> Self::Raw {
        // 对于 !Sized 类型，Box<T> 是胖指针，需要双重包装成瘦指针
        unsafe { PhysicalHandle::from_ptr(Box::into_raw(Box::new(val)) as *mut ()) }
    }

    unsafe fn from_raw(raw: Self::Raw) -> Box<T> {
        unsafe {
            let double_boxed = Box::from_raw(raw.as_ptr() as *mut Box<T>);
            *double_boxed
        }
    }

    unsafe fn as_ref<'a>(raw: Self::Raw) -> &'a Box<T> {
        unsafe { &*(raw.as_ptr() as *const Box<T>) }
    }

    fn to_physical(raw: Self::Raw) -> *mut () {
        raw.as_ptr()
    }

    unsafe fn from_physical(ptr: *mut ()) -> Self::Raw {
        unsafe { PhysicalHandle::from_ptr(ptr) }
    }

    fn physical_null() -> *mut () {
        std::ptr::null_mut()
    }
}

pub struct ArcStrategy<T: ?Sized>(std::marker::PhantomData<T>);
impl<T: ?Sized> PointerStrategy<Arc<T>> for ArcStrategy<T> {
    type Raw = PhysicalHandle;

    fn to_raw(val: Arc<T>) -> Self::Raw {
        // Arc<T> 对于 !Sized 也是胖指针，同样需要双重包装
        unsafe { PhysicalHandle::from_ptr(Box::into_raw(Box::new(val)) as *mut ()) }
    }

    unsafe fn from_raw(raw: Self::Raw) -> Arc<T> {
        unsafe {
            let boxed_arc = Box::from_raw(raw.as_ptr() as *mut Arc<T>);
            *boxed_arc
        }
    }

    unsafe fn as_ref<'a>(raw: Self::Raw) -> &'a Arc<T> {
        unsafe { &*(raw.as_ptr() as *const Arc<T>) }
    }

    fn to_physical(raw: Self::Raw) -> *mut () {
        raw.as_ptr()
    }

    unsafe fn from_physical(ptr: *mut ()) -> Self::Raw {
        unsafe { PhysicalHandle::from_ptr(ptr) }
    }

    fn physical_null() -> *mut () {
        std::ptr::null_mut()
    }
}

pub struct GenericAtomicOption<T, S: PointerStrategy<T>> {
    inner: AtomicPtr<()>,
    marker: std::marker::PhantomData<fn(T) -> S::Raw>,
}

unsafe impl<T, S: PointerStrategy<T>> Send for GenericAtomicOption<T, S> where T: Send {}
unsafe impl<T, S: PointerStrategy<T>> Sync for GenericAtomicOption<T, S> where T: Sync {}

impl<T, S: PointerStrategy<T>> GenericAtomicOption<T, S> {
    pub fn new(opt: Option<T>) -> Self {
        let ptr = match opt {
            Some(v) => S::to_physical(S::to_raw(v)),
            None => S::physical_null(),
        };
        Self {
            inner: AtomicPtr::new(ptr),
            marker: std::marker::PhantomData,
        }
    }

    pub fn swap(&self, new: Option<T>, order: Ordering) -> Option<T> {
        let new_ptr = match new {
            Some(v) => S::to_physical(S::to_raw(v)),
            None => S::physical_null(),
        };
        let old_ptr = self.inner.swap(new_ptr, order);
        if old_ptr == S::physical_null() {
            None
        } else {
            unsafe { Some(S::from_raw(S::from_physical(old_ptr))) }
        }
    }

    pub fn take(&self, order: Ordering) -> Option<T> {
        self.swap(None, order)
    }

    pub fn store(&self, val: Option<T>, order: Ordering) {
        drop(self.swap(val, order));
    }

    pub fn load_clone(&self, order: Ordering) -> Option<T>
    where
        T: Clone,
    {
        let ptr = self.inner.load(order);
        if ptr == S::physical_null() {
            None
        } else {
            unsafe { Some(S::as_ref(S::from_physical(ptr)).clone()) }
        }
    }

    pub fn compare_exchange_none(
        &self,
        new: T,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), T> {
        let raw = S::to_raw(new);
        let new_ptr = S::to_physical(raw);
        match self
            .inner
            .compare_exchange(S::physical_null(), new_ptr, success, failure)
        {
            Ok(_) => Ok(()),
            Err(_) => unsafe { Err(S::from_raw(raw)) },
        }
    }
}

impl<T, S: PointerStrategy<T>> Drop for GenericAtomicOption<T, S> {
    fn drop(&mut self) {
        self.take(Ordering::Acquire);
    }
}

// OptionPtr helpers
fn opt_to_raw<T>(ptr: Option<NonNull<T>>) -> *mut T {
    ptr.map(|p| p.as_ptr()).unwrap_or(std::ptr::null_mut())
}
fn opt_from_raw<T>(ptr: *mut T) -> Option<NonNull<T>> {
    NonNull::new(ptr)
}

// NonNullPtr helpers
fn nonnull_to_raw<T>(ptr: NonNull<T>) -> *mut T {
    ptr.as_ptr()
}
unsafe fn nonnull_from_raw_unchecked<T>(ptr: *mut T) -> NonNull<T> {
    debug_assert!(!ptr.is_null());
    unsafe { NonNull::new_unchecked(ptr) }
}

impl_ptr_state_wrapper!(
    AtomicOptionPtr, StateOptionPtr, Option<NonNull<T>>, AtomicPtr<T>, self, order,
    new(p) { Self(AtomicPtr::new(opt_to_raw(p))) },
    load() { opt_from_raw(self.0.load(order)) },
    store(p) { self.0.store(opt_to_raw(p), order) },
    swap(p) { opt_from_raw(self.0.swap(opt_to_raw(p), order)) },
    compare_exchange(c, n, s, f) {
        self.0.compare_exchange(opt_to_raw(c), opt_to_raw(n), s, f).map(opt_from_raw).map_err(opt_from_raw)
    },
    compare_exchange_weak(c, n, s, f) {
        self.0.compare_exchange_weak(opt_to_raw(c), opt_to_raw(n), s, f).map(opt_from_raw).map_err(opt_from_raw)
    },
);

impl_ptr_state_wrapper!(
    AtomicNonNullPtr, StateNonNullPtr, NonNull<T>, AtomicPtr<T>, self, order,
    new(p) { Self(AtomicPtr::new(nonnull_to_raw(p))) },
    load() { unsafe { nonnull_from_raw_unchecked(self.0.load(order)) } },
    store(p) { self.0.store(nonnull_to_raw(p), order) },
    swap(p) { unsafe { nonnull_from_raw_unchecked(self.0.swap(nonnull_to_raw(p), order)) } },
    compare_exchange(c, n, s, f) {
        self.0.compare_exchange(nonnull_to_raw(c), nonnull_to_raw(n), s, f)
            .map(|p| unsafe { nonnull_from_raw_unchecked(p) })
            .map_err(|p| unsafe { nonnull_from_raw_unchecked(p) })
    },
    compare_exchange_weak(c, n, s, f) {
        self.0.compare_exchange_weak(nonnull_to_raw(c), nonnull_to_raw(n), s, f)
            .map(|p| unsafe { nonnull_from_raw_unchecked(p) })
            .map_err(|p| unsafe { nonnull_from_raw_unchecked(p) })
    },
);

// ==================== Option Box & Arc ====================

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
