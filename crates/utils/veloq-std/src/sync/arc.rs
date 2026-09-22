use core::{
    borrow::Borrow,
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter, Pointer},
    hash::{Hash, Hasher},
    ops::Deref,
    panic::{RefUnwindSafe, UnwindSafe},
    pin::Pin,
    task::Waker,
};

use crate::alloc_crate::{boxed::Box, string::String, sync::Arc as AllocArc, task::Wake, vec::Vec};
use crate::sync::weak::NativeWeak;

#[cfg(feature = "loom")]
use core::{mem::ManuallyDrop, ptr::NonNull};

#[cfg(feature = "loom")]
use crate::{
    alloc_crate::string::ToString,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering, fence},
        weak::LoomWeak,
    },
};

/// 原生强引用计数指针包装类型。无论 Loom 特性是否开启均可用。
#[repr(transparent)]
pub struct NativeArc<T: ?Sized> {
    inner: AllocArc<T>,
}

impl<T> NativeArc<T> {
    /// 构造一个新的 `NativeArc<T>`。
    #[inline]
    #[must_use]
    pub fn new(data: T) -> Self {
        Self {
            inner: AllocArc::new(data),
        }
    }

    /// 构造一个新的固定在堆上的 `Pin<NativeArc<T>>`。
    #[inline]
    #[must_use]
    pub fn pin(data: T) -> Pin<Self> {
        // SAFETY: `NativeArc` derefs to `T`, and `Arc` provides stable heap memory.
        unsafe { Pin::new_unchecked(Self::new(data)) }
    }

    /// 若强引用计数正好为 1，则解包并返回内部值。
    #[inline]
    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        AllocArc::try_unwrap(this.inner).map_err(Self::from_inner)
    }

    /// 统一的 unsized 构造接口：构造值并通过指针转换闭包直接创建目标类型指针包装。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn new_unsized<U: ?Sized, F>(value: T, f: F) -> NativeArc<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        unsafe { Self::cast_unsized(Self::new(value), f) }
    }
}

impl<T: ?Sized> NativeArc<T> {
    /// 消费包装结构，返回底层的 `alloc::sync::Arc`。
    #[inline]
    pub fn into_inner(this: Self) -> AllocArc<T> {
        this.inner
    }

    /// 获取底层 `alloc::sync::Arc` 的不可变引用。
    #[inline]
    pub fn as_inner(this: &Self) -> &AllocArc<T> {
        &this.inner
    }

    /// 从底层的 `alloc::sync::Arc` 包装创建 `NativeArc`。
    #[inline]
    pub fn from_inner(inner: AllocArc<T>) -> Self {
        Self { inner }
    }

    /// 消费 `NativeArc` 并返回裸指针。
    #[inline]
    #[must_use]
    pub fn into_raw(this: Self) -> *const T {
        AllocArc::into_raw(this.inner)
    }

    /// 返回指向底层数据的裸指针。
    #[inline]
    #[must_use]
    pub fn as_ptr(this: &Self) -> *const T {
        AllocArc::as_ptr(&this.inner)
    }

    /// 从之前由 `into_raw` 返回的裸指针重建 `NativeArc`。
    ///
    /// # Safety
    ///
    /// 传入的指针必须源自 `NativeArc::into_raw` 或兼容的标准库 `Arc::into_raw`。
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        Self {
            inner: unsafe { AllocArc::from_raw(ptr) },
        }
    }

    /// 统一的 unsized 转换接口：通过裸指针转换闭包将 `NativeArc<T>` 转换为 `NativeArc<U>`。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn cast_unsized<U: ?Sized, F>(this: Self, f: F) -> NativeArc<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        let raw = Self::into_raw(this);
        let target_raw = f(raw);
        unsafe { NativeArc::from_raw(target_raw) }
    }

    /// 创建一个指向相同分配的 `NativeWeak` 弱指针。
    #[inline]
    #[must_use]
    pub fn downgrade(this: &Self) -> NativeWeak<T> {
        NativeWeak::from_inner(AllocArc::downgrade(&this.inner))
    }

    /// 获取指向同一分配的弱引用计数。
    #[inline]
    #[must_use]
    pub fn weak_count(this: &Self) -> usize {
        AllocArc::weak_count(&this.inner)
    }

    /// 获取指向同一分配的强引用计数。
    #[inline]
    #[must_use]
    pub fn strong_count(this: &Self) -> usize {
        AllocArc::strong_count(&this.inner)
    }

    /// 递增底层强引用计数。
    ///
    /// # Safety
    ///
    /// `ptr` 必须有效且源自 `NativeArc::into_raw`。
    #[inline]
    pub unsafe fn increment_strong_count(ptr: *const T) {
        unsafe { AllocArc::increment_strong_count(ptr) }
    }

    /// 递减底层强引用计数。
    ///
    /// # Safety
    ///
    /// `ptr` 必须有效且源自 `NativeArc::into_raw`。
    #[inline]
    pub unsafe fn decrement_strong_count(ptr: *const T) {
        unsafe { AllocArc::decrement_strong_count(ptr) }
    }

    /// 若没有其他强或弱引用，返回可变借用。
    #[inline]
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        AllocArc::get_mut(&mut this.inner)
    }

    /// 判断两个 `NativeArc` 是否指向相同的分配。
    #[inline]
    #[must_use]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        AllocArc::ptr_eq(&this.inner, &other.inner)
    }
}

impl<T: Clone> NativeArc<T> {
    /// 若独占则返回可变引用，否则克隆内部值后再返回可变引用（写时克隆）。
    #[inline]
    pub fn make_mut(this: &mut Self) -> &mut T {
        AllocArc::make_mut(&mut this.inner)
    }
}

impl<T: ?Sized> Clone for NativeArc<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ?Sized> Deref for NativeArc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Default> Default for NativeArc<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized> From<Box<T>> for NativeArc<T> {
    #[inline]
    fn from(b: Box<T>) -> Self {
        Self::from_inner(AllocArc::from(b))
    }
}

impl<T: Clone> From<&[T]> for NativeArc<[T]> {
    #[inline]
    fn from(s: &[T]) -> Self {
        Self::from_inner(AllocArc::from(s))
    }
}

impl From<&str> for NativeArc<str> {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from_inner(AllocArc::from(s))
    }
}

impl From<String> for NativeArc<str> {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_inner(AllocArc::from(s))
    }
}

impl<T> From<Vec<T>> for NativeArc<[T]> {
    #[inline]
    fn from(v: Vec<T>) -> Self {
        Self::from_inner(AllocArc::from(v))
    }
}

impl<T: ?Sized> From<AllocArc<T>> for NativeArc<T> {
    #[inline]
    fn from(inner: AllocArc<T>) -> Self {
        Self::from_inner(inner)
    }
}

impl<T: ?Sized> From<NativeArc<T>> for AllocArc<T> {
    #[inline]
    fn from(arc: NativeArc<T>) -> Self {
        NativeArc::into_inner(arc)
    }
}

impl<T: ?Sized> AsRef<T> for NativeArc<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized> Borrow<T> for NativeArc<T> {
    #[inline]
    fn borrow(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized + Debug> Debug for NativeArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

impl<T: ?Sized + Display> Display for NativeArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl<T: ?Sized> Pointer for NativeArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.inner, f)
    }
}

impl<T: ?Sized + PartialEq> PartialEq for NativeArc<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T: ?Sized + Eq> Eq for NativeArc<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for NativeArc<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T: ?Sized + Ord> Ord for NativeArc<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T: ?Sized + Hash> Hash for NativeArc<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

unsafe impl<T: ?Sized + Sync + Send> Send for NativeArc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for NativeArc<T> {}

impl<T: ?Sized + RefUnwindSafe> RefUnwindSafe for NativeArc<T> {}
impl<T: ?Sized + RefUnwindSafe> UnwindSafe for NativeArc<T> {}
impl<T: ?Sized> Unpin for NativeArc<T> {}

impl<W: Wake + Send + Sync + 'static> From<NativeArc<W>> for Waker {
    #[inline]
    fn from(arc: NativeArc<W>) -> Self {
        Waker::from(NativeArc::into_inner(arc))
    }
}

#[cfg(feature = "loom")]
pub(crate) struct ArcControlBlock {
    pub(crate) strong: AtomicUsize,
    pub(crate) weak: AtomicUsize,
    pub(crate) drop_data: unsafe fn(NonNull<ArcControlBlock>),
    pub(crate) dealloc: unsafe fn(NonNull<ArcControlBlock>),
}

#[cfg(feature = "loom")]
#[repr(C)]
struct ArcInner<T> {
    control: ArcControlBlock,
    data: ManuallyDrop<T>,
}

#[cfg(feature = "loom")]
unsafe fn drop_inner_data<T>(control: NonNull<ArcControlBlock>) {
    unsafe {
        let inner = control.as_ptr() as *mut ArcInner<T>;
        ManuallyDrop::drop(&mut (*inner).data);
    }
}

#[cfg(feature = "loom")]
unsafe fn dealloc_inner<T>(control: NonNull<ArcControlBlock>) {
    unsafe {
        let inner = control.as_ptr() as *mut ArcInner<T>;
        drop(Box::from_raw(inner));
    }
}

#[cfg(feature = "loom")]
pub(crate) mod loom_raw_registry {
    use super::ArcControlBlock;
    use crate::collections::FastHashMap;
    use crate::sync::NativeSpinLock;
    use core::ptr::NonNull;

    static REGISTRY: NativeSpinLock<Option<FastHashMap<usize, (usize, usize)>>> =
        NativeSpinLock::new(None);

    pub(crate) fn register(addr: usize, control: NonNull<ArcControlBlock>) {
        let mut guard = REGISTRY.lock();
        guard.with_mut(|opt| {
            let map = opt.get_or_insert_with(FastHashMap::default);
            let entry = map.entry(addr).or_insert((control.as_ptr() as usize, 0));
            entry.1 += 1;
        });
    }

    pub(crate) fn unregister(addr: usize) -> Option<NonNull<ArcControlBlock>> {
        let mut guard = REGISTRY.lock();
        guard.with_mut(|opt| {
            if let Some(map) = opt.as_mut()
                && let Some(entry) = map.get_mut(&addr)
            {
                let control_addr = entry.0;
                entry.1 -= 1;
                if entry.1 == 0 {
                    map.remove(&addr);
                }
                return NonNull::new(control_addr as *mut ArcControlBlock);
            }
            None
        })
    }

    pub(crate) fn get(addr: usize) -> Option<NonNull<ArcControlBlock>> {
        let guard = REGISTRY.lock();
        guard.with(|opt| {
            opt.as_ref().and_then(|map| {
                map.get(&addr)
                    .and_then(|(c, _)| NonNull::new(*c as *mut ArcControlBlock))
            })
        })
    }
}

#[cfg(feature = "loom")]
/// Loom 仿真强引用计数指针包装类型。基于控制块和原子操作独立实现，支持弱引用及胖指针动态类型转换。
pub struct LoomArc<T: ?Sized> {
    pub(crate) ptr: *mut T,
    pub(crate) control: NonNull<ArcControlBlock>,
}

#[cfg(feature = "loom")]
impl<T> LoomArc<T> {
    /// 构造一个新的 `LoomArc<T>`。
    #[inline]
    #[track_caller]
    pub fn new(data: T) -> Self {
        let inner = Box::into_raw(Box::new(ArcInner {
            control: ArcControlBlock {
                strong: AtomicUsize::new(1),
                weak: AtomicUsize::new(1),
                drop_data: drop_inner_data::<T>,
                dealloc: dealloc_inner::<T>,
            },
            data: ManuallyDrop::new(data),
        }));
        let data_ptr = unsafe { (&raw mut (*inner).data).cast::<T>() };
        let control = unsafe { NonNull::new_unchecked(&raw mut (*inner).control) };
        Self {
            ptr: data_ptr,
            control,
        }
    }

    /// 构造一个新的固定在堆上的 `Pin<LoomArc<T>>`。
    #[inline]
    pub fn pin(data: T) -> Pin<Self> {
        // SAFETY: `LoomArc` derefs to `T`, and `Arc` provides stable heap memory.
        unsafe { Pin::new_unchecked(Self::new(data)) }
    }

    /// 若强引用计数正好为 1，则解包并返回内部值。
    #[inline]
    #[track_caller]
    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        let c_ref = unsafe { this.control.as_ref() };
        if c_ref
            .strong
            .compare_exchange(1, 0, AtomicOrdering::Acquire, AtomicOrdering::Relaxed)
            .is_ok()
        {
            let control = this.control;
            let inner = control.as_ptr() as *mut ArcInner<T>;
            let val = unsafe { ManuallyDrop::take(&mut (*inner).data) };
            core::mem::forget(this);
            if unsafe { control.as_ref().weak.fetch_sub(1, AtomicOrdering::Release) } == 1 {
                fence(AtomicOrdering::Acquire);
                unsafe {
                    (control.as_ref().dealloc)(control);
                }
            }
            Ok(val)
        } else {
            Err(this)
        }
    }

    /// 统一的 unsized 构造接口：构造值并通过指针转换闭包直接创建目标类型指针包装。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn new_unsized<U: ?Sized, F>(value: T, f: F) -> LoomArc<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        unsafe { Self::cast_unsized(Self::new(value), f) }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> LoomArc<T> {
    /// 消费 `LoomArc` 并返回裸指针。
    #[inline]
    #[must_use]
    pub fn into_raw(this: Self) -> *const T {
        let ptr = this.ptr;
        let control = this.control;
        core::mem::forget(this);
        loom_raw_registry::register(ptr as *const () as usize, control);
        ptr as *const T
    }

    /// 返回指向底层数据的裸指针。
    #[inline]
    #[must_use]
    pub fn as_ptr(this: &Self) -> *const T {
        this.ptr as *const T
    }

    /// 从之前由 `into_raw` 返回的裸指针重建 `LoomArc`。
    ///
    /// # Safety
    ///
    /// 传入的指针必须源自 `LoomArc::into_raw`。
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        let control = loom_raw_registry::unregister(ptr as *const () as usize)
            .expect("LoomArc::from_raw called on pointer not registered via into_raw");
        Self {
            ptr: ptr as *mut T,
            control,
        }
    }

    /// 从底层的标准库 `AllocArc` 创建 `LoomArc`。
    #[inline]
    #[track_caller]
    pub fn from_std(std: AllocArc<T>) -> Self
    where
        T: Clone,
    {
        Self::new((*std).clone())
    }

    /// 从 `NativeArc` 创建 `LoomArc`。
    #[inline]
    #[track_caller]
    pub fn from_native(native: NativeArc<T>) -> Self
    where
        T: Clone,
    {
        Self::new((*native).clone())
    }

    /// 统一的 unsized 转换接口：通过裸指针转换闭包将 `LoomArc<T>` 转换为 `LoomArc<U>`。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn cast_unsized<U: ?Sized, F>(this: Self, f: F) -> LoomArc<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        let new_ptr = f(this.ptr as *const T) as *mut U;
        let control = this.control;
        core::mem::forget(this);
        LoomArc {
            ptr: new_ptr,
            control,
        }
    }

    /// 创建一个弱引用指针 `LoomWeak<T>`。
    #[inline]
    #[must_use]
    pub fn downgrade(this: &Self) -> LoomWeak<T> {
        unsafe {
            this.control
                .as_ref()
                .weak
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        LoomWeak::from_raw_parts(this.ptr, Some(this.control))
    }

    /// 获取指向同一分配的强引用计数。
    #[inline]
    #[must_use]
    pub fn strong_count(this: &Self) -> usize {
        unsafe { this.control.as_ref().strong.load(AtomicOrdering::SeqCst) }
    }

    /// 获取指向同一分配的弱引用计数。
    #[inline]
    #[must_use]
    pub fn weak_count(this: &Self) -> usize {
        let weak = unsafe { this.control.as_ref().weak.load(AtomicOrdering::SeqCst) };
        weak.saturating_sub(1)
    }

    /// 递增底层强引用计数。
    ///
    /// # Safety
    ///
    /// `ptr` 必须有效且源自 `LoomArc::into_raw`。
    #[inline]
    #[track_caller]
    pub unsafe fn increment_strong_count(ptr: *const T) {
        let control = loom_raw_registry::get(ptr as *const () as usize)
            .expect("increment_strong_count called on pointer not registered via into_raw");
        unsafe {
            control
                .as_ref()
                .strong
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    /// 递减底层强引用计数。
    ///
    /// # Safety
    ///
    /// `ptr` 必须有效且源自 `LoomArc::into_raw`。
    #[inline]
    #[track_caller]
    pub unsafe fn decrement_strong_count(ptr: *const T) {
        let arc = unsafe { Self::from_raw(ptr) };
        drop(arc);
    }

    /// 若没有其他引用，返回可变借用。
    #[inline]
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        let c_ref = unsafe { this.control.as_ref() };
        if c_ref.strong.load(AtomicOrdering::Acquire) == 1
            && c_ref.weak.load(AtomicOrdering::Acquire) == 1
        {
            Some(unsafe { &mut *this.ptr })
        } else {
            None
        }
    }

    /// 判断两个 `LoomArc` 是否指向相同的分配。
    #[inline]
    #[must_use]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        core::ptr::eq(this.ptr, other.ptr)
    }
}

#[cfg(feature = "loom")]
impl<T: Clone> LoomArc<T> {
    /// 若独占则返回可变引用，否则克隆内部值后再返回可变引用（写时克隆）。
    #[inline]
    pub fn make_mut(this: &mut Self) -> &mut T {
        if Self::get_mut(this).is_none() {
            let new_arc = Self::new((**this).clone());
            *this = new_arc;
        }
        Self::get_mut(this).expect("must be unique after make_mut allocation")
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Clone for LoomArc<T> {
    #[inline]
    #[track_caller]
    fn clone(&self) -> Self {
        let old = unsafe {
            self.control
                .as_ref()
                .strong
                .fetch_add(1, AtomicOrdering::Relaxed)
        };
        if old > usize::MAX / 2 {
            panic!("too many references");
        }
        Self {
            ptr: self.ptr,
            control: self.control,
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Drop for LoomArc<T> {
    #[inline]
    #[track_caller]
    fn drop(&mut self) {
        let control = self.control;
        let c_ref = unsafe { control.as_ref() };
        if c_ref.strong.fetch_sub(1, AtomicOrdering::Release) == 1 {
            fence(AtomicOrdering::Acquire);
            unsafe {
                (c_ref.drop_data)(control);
            }
            if c_ref.weak.fetch_sub(1, AtomicOrdering::Release) == 1 {
                fence(AtomicOrdering::Acquire);
                unsafe {
                    (c_ref.dealloc)(control);
                }
            }
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Deref for LoomArc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[cfg(feature = "loom")]
impl<T: Default> Default for LoomArc<T> {
    #[inline]
    #[track_caller]
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(feature = "loom")]
impl<T> From<Box<T>> for LoomArc<T> {
    #[inline]
    fn from(b: Box<T>) -> Self {
        Self::new(*b)
    }
}

#[cfg(feature = "loom")]
impl<T: Clone> From<&[T]> for LoomArc<[T]> {
    #[inline]
    fn from(s: &[T]) -> Self {
        let mut v = Vec::with_capacity(s.len());
        v.extend_from_slice(s);
        Self::from(v)
    }
}

#[cfg(feature = "loom")]
impl From<&str> for LoomArc<str> {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from(ToString::to_string(s))
    }
}

#[cfg(feature = "loom")]
impl From<String> for LoomArc<str> {
    #[inline]
    fn from(s: String) -> Self {
        let arc_string = LoomArc::new(s);
        unsafe {
            LoomArc::cast_unsized(arc_string, |p| {
                let s_ref: &str = (*p).as_str();
                s_ref as *const str
            })
        }
    }
}

#[cfg(feature = "loom")]
impl<T> From<Vec<T>> for LoomArc<[T]> {
    #[inline]
    fn from(v: Vec<T>) -> Self {
        let arc_vec = LoomArc::new(v);
        unsafe {
            LoomArc::cast_unsized(arc_vec, |p| {
                let slice: &[T] = (*p).as_slice();
                slice as *const [T]
            })
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Debug> Debug for LoomArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&**self, f)
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Display> Display for LoomArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&**self, f)
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Pointer for LoomArc<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&Self::as_ptr(self), f)
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + PartialEq> PartialEq for LoomArc<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Eq> Eq for LoomArc<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized + PartialOrd> PartialOrd for LoomArc<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Ord> Ord for LoomArc<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Hash> Hash for LoomArc<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> AsRef<T> for LoomArc<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Borrow<T> for LoomArc<T> {
    #[inline]
    fn borrow(&self) -> &T {
        self
    }
}

#[cfg(feature = "loom")]
unsafe impl<T: ?Sized + Sync + Send> Send for LoomArc<T> {}

#[cfg(feature = "loom")]
unsafe impl<T: ?Sized + Sync + Send> Sync for LoomArc<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized + RefUnwindSafe> RefUnwindSafe for LoomArc<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized + RefUnwindSafe> UnwindSafe for LoomArc<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized> Unpin for LoomArc<T> {}

#[cfg(not(feature = "loom"))]
pub type Arc<T> = NativeArc<T>;

#[cfg(feature = "loom")]
pub type Arc<T> = LoomArc<T>;
