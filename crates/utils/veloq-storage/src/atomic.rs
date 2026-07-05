use veloq_std::{
    boxed::Box,
    marker::PhantomData,
    mem::take,
    ptr::{NonNull, null_mut},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
    task::Waker,
    vec::Vec,
};

use crate::{
    StateLock, StateOptionArc, StateOptionBox, StateWakerQueue, Storage, StrategyType,
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
    type OptionBox<T: Send> = AtomicOptionBox<T>;
    type OptionFatBox<T: ?Sized + Send> = AtomicOptionFatBox<T>;
    type OptionArc<T: Send + Sync> = AtomicOptionArc<T>;
    type OptionFatArc<T: ?Sized + Send + Sync> = AtomicOptionFatArc<T>;
}

pub struct AtomicLock<T>(Mutex<T>);
impl<T> StateLock<T> for AtomicLock<T> {
    type Guard<'a>
        = MutexGuard<'a, T>
    where
        T: 'a;
    fn new(val: T) -> Self {
        Self(Mutex::new(val))
    }
    fn lock(&self) -> Self::Guard<'_> {
        self.0.lock()
    }
}
unsafe impl<T> Send for AtomicLock<T> {}
unsafe impl<T> Sync for AtomicLock<T> {}

pub struct AtomicWakerQueue(Mutex<Vec<Waker>>);
impl StateWakerQueue for AtomicWakerQueue {
    fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    fn register(&self, waker: &Waker) {
        let mut wakers = self.0.lock();
        if !wakers.iter().any(|registered| registered.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn take_all(&self) -> Vec<Waker> {
        take(&mut *self.0.lock())
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

// ==================== Pointer Helpers & Strategies ====================

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

    /// 克隆对值的引用。
    ///
    /// # Safety
    /// `raw` 指向的内容必须存活且有效。
    unsafe fn clone_ref(raw: Self::Raw) -> T
    where
        T: Clone;

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

pub struct FatPointerStrategy<T>(PhantomData<T>);
impl<T> PointerStrategy<T> for FatPointerStrategy<T> {
    type Raw = NonNull<()>;

    fn to_raw(val: T) -> Self::Raw {
        unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(val)) as *mut ()) }
    }

    unsafe fn from_raw(raw: Self::Raw) -> T {
        unsafe {
            let double_boxed = Box::from_raw(raw.as_ptr() as *mut T);
            *double_boxed
        }
    }

    unsafe fn clone_ref(raw: Self::Raw) -> T
    where
        T: Clone,
    {
        unsafe { (*(raw.as_ptr() as *const T)).clone() }
    }

    fn to_physical(raw: Self::Raw) -> *mut () {
        raw.as_ptr()
    }

    unsafe fn from_physical(ptr: *mut ()) -> Self::Raw {
        unsafe { NonNull::new_unchecked(ptr) }
    }

    fn physical_null() -> *mut () {
        null_mut()
    }
}

pub struct GenericAtomicOption<T, S: PointerStrategy<T>> {
    inner: AtomicPtr<()>,
    marker: PhantomData<fn(T) -> S::Raw>,
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
            marker: PhantomData,
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
            unsafe { Some(S::clone_ref(S::from_physical(ptr))) }
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
    ptr.map(|p| p.as_ptr()).unwrap_or(null_mut())
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

/// 一个专门用于原子存储 `Option<Box<T>>` 的容器（针对 Sized 类型）。
/// 直接存储 Box 的原始指针，避免了双重包装。
pub struct AtomicOptionBox<T>(AtomicPtr<T>);

unsafe impl<T: Send> Send for AtomicOptionBox<T> {}
unsafe impl<T: Send> Sync for AtomicOptionBox<T> {}

impl<T: Send> StateOptionBox<T> for AtomicOptionBox<T> {
    fn new(opt: Option<Box<T>>) -> Self {
        Self(AtomicPtr::new(opt.map_or(null_mut(), Box::into_raw)))
    }
    fn take(&self, order: Ordering) -> Option<Box<T>> {
        NonNull::new(self.0.swap(null_mut(), order)).map(|p| unsafe { Box::from_raw(p.as_ptr()) })
    }
    fn swap(&self, new: Option<Box<T>>, order: Ordering) -> Option<Box<T>> {
        let new_ptr = new.map_or(null_mut(), Box::into_raw);
        NonNull::new(self.0.swap(new_ptr, order)).map(|p| unsafe { Box::from_raw(p.as_ptr()) })
    }
    fn store(&self, val: Option<Box<T>>, order: Ordering) {
        let new_ptr = val.map_or(null_mut(), Box::into_raw);
        if let Some(p) = NonNull::new(self.0.swap(new_ptr, order)) {
            unsafe { drop(Box::from_raw(p.as_ptr())) }
        }
    }
    fn compare_exchange_none(
        &self,
        new: Box<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Box<T>> {
        let new_ptr = Box::into_raw(new);
        self.0
            .compare_exchange(null_mut(), new_ptr, success, failure)
            .map(|_| ())
            .map_err(|_| unsafe { Box::from_raw(new_ptr) })
    }
}

impl<T> Drop for AtomicOptionBox<T> {
    fn drop(&mut self) {
        if let Some(p) = NonNull::new(*self.0.get_mut()) {
            unsafe { drop(Box::from_raw(p.as_ptr())) }
        }
    }
}

/// 一个原子存储 `Option<Box<T>>` 的容器。
/// 针对 `!Sized` 类型（如 trait objects），它会自动处理双重包装以保持原子性。
pub struct AtomicOptionFatBox<T: ?Sized>(GenericAtomicOption<Box<T>, FatPointerStrategy<Box<T>>>);

unsafe impl<T: ?Sized + Send> Send for AtomicOptionFatBox<T> {}
unsafe impl<T: ?Sized + Send> Sync for AtomicOptionFatBox<T> {}

impl<T: ?Sized + Send> StateOptionBox<T> for AtomicOptionFatBox<T> {
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

/// 一个专门用于原子存储 `Option<Arc<T>>` 的容器（针对 Sized 类型）。
/// 直接存储 Arc 的原始指针，避免了双重包装。
pub struct AtomicOptionArc<T>(AtomicPtr<T>);

unsafe impl<T: Send + Sync> Send for AtomicOptionArc<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicOptionArc<T> {}

impl<T: Send + Sync> StateOptionArc<T> for AtomicOptionArc<T> {
    fn new(opt: Option<Arc<T>>) -> Self {
        Self(AtomicPtr::new(
            opt.map_or(null_mut(), |a| Arc::into_raw(a) as *mut T),
        ))
    }
    fn take(&self, order: Ordering) -> Option<Arc<T>> {
        NonNull::new(self.0.swap(null_mut(), order)).map(|p| unsafe { Arc::from_raw(p.as_ptr()) })
    }
    fn store(&self, opt: Option<Arc<T>>, order: Ordering) {
        let new_ptr = opt.map_or(null_mut(), |a| Arc::into_raw(a) as *mut T);
        if let Some(p) = NonNull::new(self.0.swap(new_ptr, order)) {
            unsafe { drop(Arc::from_raw(p.as_ptr())) }
        }
    }
    fn load_clone(&self, order: Ordering) -> Option<Arc<T>> {
        NonNull::new(self.0.load(order)).map(|p| unsafe {
            Arc::increment_strong_count(p.as_ptr());
            Arc::from_raw(p.as_ptr())
        })
    }
    fn compare_exchange_none(
        &self,
        new: Arc<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Arc<T>> {
        let new_ptr = Arc::into_raw(new) as *mut T;
        self.0
            .compare_exchange(null_mut(), new_ptr, success, failure)
            .map(|_| ())
            .map_err(|_| unsafe { Arc::from_raw(new_ptr) })
    }
}

impl<T> Drop for AtomicOptionArc<T> {
    fn drop(&mut self) {
        if let Some(p) = NonNull::new(*self.0.get_mut()) {
            unsafe { drop(Arc::from_raw(p.as_ptr())) }
        }
    }
}

/// 一个原子存储 `Option<Arc<T>>` 的容器。
/// 针对 `!Sized` 类型（如 trait objects），它会自动处理双重包装以保持原子性。
pub struct AtomicOptionFatArc<T: ?Sized>(GenericAtomicOption<Arc<T>, FatPointerStrategy<Arc<T>>>);

unsafe impl<T: ?Sized + Send + Sync> Send for AtomicOptionFatArc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for AtomicOptionFatArc<T> {}

impl<T: ?Sized + Send + Sync> StateOptionArc<T> for AtomicOptionFatArc<T> {
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
