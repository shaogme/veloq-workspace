use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

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

impl_ptr_state_wrapper!(
    OptionPtr, StateOptionPtr, Option<NonNull<T>>, Cell<*mut T>, self, _order,
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
);

impl_ptr_state_wrapper!(
    NonNullPtr, StateNonNullPtr, NonNull<T>, Cell<NonNull<T>>, self, _order,
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
);
