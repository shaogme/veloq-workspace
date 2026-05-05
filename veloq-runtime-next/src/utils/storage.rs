use std::cell::{Cell, RefCell};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::task::Waker;

use crossbeam_queue::SegQueue;

pub trait Storage: Send + Sync {
    fn strategy_id() -> *const ();
    type Usize: StateInt;
    type OptionPtr<T>: StateOptionPtr<T>;
    type NonNullPtr<T>: StateNonNullPtr<T>;
    type Lock<T>: StateLock<T>;
    type WakerQueue: StateWakerQueue;
}

pub trait StateInt: Send + Sync {
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

pub trait StateOptionPtr<T>: Send + Sync {
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

pub trait StateNonNullPtr<T>: Send + Sync {
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

pub trait StateLock<T>: Send + Sync {
    type Guard<'a>: std::ops::DerefMut<Target = T>
    where
        Self: 'a,
        T: 'a;
    fn new(val: T) -> Self;
    fn lock(&self) -> Self::Guard<'_>;
}

pub trait StateWakerQueue: Send + Sync + 'static {
    fn new() -> Self;
    fn push(&self, waker: Waker);
    fn take_all(&self) -> Vec<Waker>;
}

// --- Atomic Storage Implementation ---

pub struct AtomicStorage;
impl Storage for AtomicStorage {
    fn strategy_id() -> *const () {
        static ID: u8 = 0;
        &ID as *const _ as *const ()
    }
    type Usize = AtomicUsize;
    type OptionPtr<T> = AtomicOptionPtr<T>;
    type NonNullPtr<T> = AtomicNonNullPtr<T>;
    type Lock<T> = AtomicLock<T>;
    type WakerQueue = AtomicWakerQueue;
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

pub struct AtomicWakerQueue(SegQueue<Waker>);
impl StateWakerQueue for AtomicWakerQueue {
    fn new() -> Self {
        Self(SegQueue::new())
    }

    fn push(&self, waker: Waker) {
        self.0.push(waker);
    }

    fn take_all(&self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        while let Some(waker) = self.0.pop() {
            wakers.push(waker);
        }
        wakers
    }
}
unsafe impl Send for AtomicWakerQueue {}
unsafe impl Sync for AtomicWakerQueue {}

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
        impl StateInt for $ty {
            fn new($new_val: usize) -> Self { $new_expr }
            fn load(&$self, $order: Ordering) -> usize { $load_expr }
            fn store(&$self, $store_val: usize, $order: Ordering) { $store_expr }
            fn fetch_add(&$self, $add_val: usize, $order: Ordering) -> usize { $add_expr }
            fn fetch_sub(&$self, $sub_val: usize, $order: Ordering) -> usize { $sub_expr }
            fn fetch_and(&$self, $and_val: usize, $order: Ordering) -> usize { $and_expr }
            fn fetch_or(&$self, $or_val: usize, $order: Ordering) -> usize { $or_expr }
            fn compare_exchange(&$self, $ce_curr: usize, $ce_new: usize, $ce_s: Ordering, $ce_f: Ordering) -> Result<usize, usize> { $ce_expr }
            fn compare_exchange_weak(&$self, $cew_curr: usize, $cew_new: usize, $cew_s: Ordering, $cew_f: Ordering) -> Result<usize, usize> { $cew_expr }
        }
    };
}

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

// --- Local Storage Implementation ---

pub struct LocalStorage;
impl Storage for LocalStorage {
    fn strategy_id() -> *const () {
        static ID: u8 = 0;
        &ID as *const _ as *const ()
    }
    type Usize = NonAtomicUsize;
    type OptionPtr<T> = NonAtomicOptionPtr<T>;
    type NonNullPtr<T> = NonAtomicNonNullPtr<T>;
    type Lock<T> = LocalLock<T>;
    type WakerQueue = LocalWakerQueue;
}

pub struct LocalLock<T>(RefCell<T>);
impl<T> StateLock<T> for LocalLock<T> {
    type Guard<'a>
        = std::cell::RefMut<'a, T>
    where
        T: 'a;
    fn new(val: T) -> Self {
        Self(RefCell::new(val))
    }
    fn lock(&self) -> Self::Guard<'_> {
        self.0.borrow_mut()
    }
}
unsafe impl<T> Send for LocalLock<T> {}
unsafe impl<T> Sync for LocalLock<T> {}

pub struct LocalWakerQueue(RefCell<Vec<Waker>>);
impl StateWakerQueue for LocalWakerQueue {
    fn new() -> Self {
        Self(RefCell::new(Vec::new()))
    }

    fn push(&self, waker: Waker) {
        self.0.borrow_mut().push(waker);
    }

    fn take_all(&self) -> Vec<Waker> {
        std::mem::take(&mut *self.0.borrow_mut())
    }
}
unsafe impl Send for LocalWakerQueue {}
unsafe impl Sync for LocalWakerQueue {}

pub struct NonAtomicUsize(Cell<usize>);
unsafe impl Send for NonAtomicUsize {}
unsafe impl Sync for NonAtomicUsize {}

impl_state_int!(
    NonAtomicUsize, self, _order, val, curr, new, success, failure,
    new(v) { Self(Cell::new(v)) },
    load() { self.0.get() },
    store(v) { self.0.set(v) },
    fetch_add(v) {
        let old = self.0.get();
        self.0.set(old + v);
        old
    },
    fetch_sub(v) {
        let old = self.0.get();
        self.0.set(old - v);
        old
    },
    fetch_and(v) {
        let old = self.0.get();
        self.0.set(old & v);
        old
    },
    fetch_or(v) {
        let old = self.0.get();
        self.0.set(old | v);
        old
    },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == c {
            self.0.set(n);
            Ok(old)
        } else {
            Err(old)
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) }
);

// --- Pointer Strategy ---

/// 不透明物理存储句柄，用于在原子操作中安全地传递指针。
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
    _marker: std::marker::PhantomData<fn(T) -> S::Raw>,
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
            _marker: std::marker::PhantomData,
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
        impl<T> $name<T> {
            pub fn new(ptr: $val) -> Self { let $new_ptr = ptr; $new_expr }
            pub fn load(&$self, $order: Ordering) -> $val { $load_expr }
            pub fn store(&$self, ptr: $val, $order: Ordering) { let $store_ptr = ptr; $store_expr }
            pub fn swap(&$self, ptr: $val, $order: Ordering) -> $val { let $swap_ptr = ptr; $swap_expr }
            pub fn compare_exchange(&$self, $ce_curr: $val, $ce_new: $val, $ce_s: Ordering, $ce_f: Ordering) -> Result<$val, $val> { $ce_expr }
            pub fn compare_exchange_weak(&$self, $cew_curr: $val, $cew_new: $val, $cew_s: Ordering, $cew_f: Ordering) -> Result<$val, $val> { $cew_expr }
        }
        impl<T> $trait<T> for $name<T> {
            fn new(ptr: $val) -> Self { Self::new(ptr) }
            fn load(&$self, order: Ordering) -> $val { $self.load(order) }
            fn store(&$self, ptr: $val, order: Ordering) { $self.store(ptr, order) }
            fn swap(&$self, ptr: $val, order: Ordering) -> $val { $self.swap(ptr, order) }
            fn compare_exchange(&$self, current: $val, new: $val, success: Ordering, failure: Ordering) -> Result<$val, $val> { $self.compare_exchange(current, new, success, failure) }
            fn compare_exchange_weak(&$self, current: $val, new: $val, success: Ordering, failure: Ordering) -> Result<$val, $val> { $self.compare_exchange_weak(current, new, success, failure) }
        }
    };
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

impl_ptr_state_wrapper!(
    NonAtomicOptionPtr, StateOptionPtr, Option<NonNull<T>>, Cell<*mut T>, self, _order,
    new(p) { Self(Cell::new(opt_to_raw(p))) },
    load() { opt_from_raw(self.0.get()) },
    store(p) { self.0.set(opt_to_raw(p)) },
    swap(p) {
        let old = self.0.get();
        self.0.set(opt_to_raw(p));
        opt_from_raw(old)
    },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == opt_to_raw(c) {
            self.0.set(opt_to_raw(n));
            Ok(opt_from_raw(old))
        } else {
            Err(opt_from_raw(old))
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) },
    unsafe_impl unsafe impl<T> Send for NonAtomicOptionPtr<T> {}
    unsafe_impl unsafe impl<T> Sync for NonAtomicOptionPtr<T> {}
);

impl_ptr_state_wrapper!(
    NonAtomicNonNullPtr, StateNonNullPtr, NonNull<T>, Cell<NonNull<T>>, self, _order,
    new(p) { Self(Cell::new(p)) },
    load() { self.0.get() },
    store(p) { self.0.set(p) },
    swap(p) { self.0.replace(p) },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == c {
            self.0.set(n);
            Ok(old)
        } else {
            Err(old)
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) },
    unsafe_impl unsafe impl<T> Send for NonAtomicNonNullPtr<T> {}
    unsafe_impl unsafe impl<T> Sync for NonAtomicNonNullPtr<T> {}
);

// --- AtomicOptionBox ---

/// 一个原子存储 `Option<Box<T>>` 的容器。
/// 针对 `!Sized` 类型（如 trait objects），它会自动处理双重包装以保持原子性。
pub struct AtomicOptionBox<T: ?Sized>(GenericAtomicOption<Box<T>, BoxStrategy<T>>);

unsafe impl<T: ?Sized + Send> Send for AtomicOptionBox<T> {}
unsafe impl<T: ?Sized + Sync> Sync for AtomicOptionBox<T> {}

impl<T: ?Sized> AtomicOptionBox<T> {
    pub fn new(opt: Option<Box<T>>) -> Self {
        Self(GenericAtomicOption::new(opt))
    }

    /// 获取当前存储的 Box 并将其置为空。
    pub fn take(&self, order: Ordering) -> Option<Box<T>> {
        self.0.take(order)
    }

    /// 交换当前存储的 Box。
    pub fn swap(&self, new: Option<Box<T>>, order: Ordering) -> Option<Box<T>> {
        self.0.swap(new, order)
    }

    /// 存储一个新的 Box。如果之前有存储，则旧的会被释放。
    pub fn store(&self, val: Option<Box<T>>, order: Ordering) {
        self.0.store(val, order)
    }

    /// 仅在当前为空时，存入新的 Box。如果存入失败，则返回原 Box 的所有权。
    pub fn compare_exchange_none(
        &self,
        new: Box<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Box<T>> {
        self.0.compare_exchange_none(new, success, failure)
    }
}

// --- AtomicOptionArc ---

/// 一个专门用于原子存储 `Option<Arc<T>>` 的容器。
/// 直接存储 Arc 的原始指针，避免了额外的 Box 包装。
pub struct AtomicOptionArc<T: ?Sized>(GenericAtomicOption<Arc<T>, ArcStrategy<T>>);

unsafe impl<T: ?Sized + Send + Sync> Send for AtomicOptionArc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for AtomicOptionArc<T> {}

impl<T: ?Sized> AtomicOptionArc<T> {
    pub fn new(opt: Option<Arc<T>>) -> Self {
        Self(GenericAtomicOption::new(opt))
    }

    pub fn take(&self, order: Ordering) -> Option<Arc<T>> {
        self.0.take(order)
    }

    pub fn store(&self, opt: Option<Arc<T>>, order: Ordering) {
        self.0.store(opt, order)
    }

    pub fn load_clone(&self, order: Ordering) -> Option<Arc<T>> {
        self.0.load_clone(order)
    }

    pub fn compare_exchange_none(
        &self,
        new: Arc<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), Arc<T>> {
        self.0.compare_exchange_none(new, success, failure)
    }
}
