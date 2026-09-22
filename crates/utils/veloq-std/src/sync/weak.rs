use core::{
    fmt::{self, Debug, Formatter, Pointer},
    panic::{RefUnwindSafe, UnwindSafe},
};

use crate::alloc_crate::sync::Weak as AllocWeak;
use crate::sync::arc::NativeArc;

#[cfg(feature = "loom")]
use core::ptr::NonNull;

#[cfg(feature = "loom")]
use crate::sync::{
    arc::{ArcControlBlock, LoomArc, loom_raw_registry},
    atomic::{Ordering as AtomicOrdering, fence},
};

/// 原生弱引用计数指针包装类型。无论 Loom 特性是否开启均可用。
#[repr(transparent)]
pub struct NativeWeak<T: ?Sized> {
    inner: AllocWeak<T>,
}

impl<T> NativeWeak<T> {
    /// 创建一个新的空 `NativeWeak` 指针。
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: AllocWeak::new(),
        }
    }
}

impl<T: ?Sized> NativeWeak<T> {
    /// 消费包装结构，返回底层的 `alloc::sync::Weak`。
    #[inline]
    pub fn into_inner(this: Self) -> AllocWeak<T> {
        this.inner
    }

    /// 获取底层 `alloc::sync::Weak` 的不可变引用。
    #[inline]
    pub fn as_inner(this: &Self) -> &AllocWeak<T> {
        &this.inner
    }

    /// 从底层的 `alloc::sync::Weak` 包装创建 `NativeWeak`。
    #[inline]
    pub fn from_inner(inner: AllocWeak<T>) -> Self {
        Self { inner }
    }

    /// 返回指向底层数据的裸指针。
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        self.inner.as_ptr()
    }

    /// 消费 `NativeWeak` 并返回裸指针。
    #[inline]
    #[must_use]
    pub fn into_raw(self) -> *const T {
        self.inner.into_raw()
    }

    /// 从之前由 `into_raw` 返回的裸指针重建 `NativeWeak`。
    ///
    /// # Safety
    ///
    /// 传入的指针必须源自 `NativeWeak::into_raw` 或兼容的标准库 `Weak::into_raw`。
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        Self {
            inner: unsafe { AllocWeak::from_raw(ptr) },
        }
    }

    /// 尝试升级为 `NativeArc`。
    #[inline]
    #[must_use]
    pub fn upgrade(&self) -> Option<NativeArc<T>> {
        self.inner.upgrade().map(NativeArc::from_inner)
    }

    /// 获取指向同一分配的强引用计数。
    #[inline]
    #[must_use]
    pub fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    /// 获取指向同一分配的弱引用计数。
    #[inline]
    #[must_use]
    pub fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }

    /// 判断两个 `NativeWeak` 是否指向相同的分配。
    #[inline]
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.inner.ptr_eq(&other.inner)
    }

    /// 统一的 unsized 转换接口：通过裸指针转换闭包将 `NativeWeak<T>` 转换为 `NativeWeak<U>`。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn cast_unsized<U: ?Sized, F>(this: Self, f: F) -> NativeWeak<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        let raw = Self::into_raw(this);
        let target_raw = f(raw);
        unsafe { NativeWeak::from_raw(target_raw) }
    }
}

impl<T: ?Sized> Clone for NativeWeak<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for NativeWeak<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ?Sized + Debug> Debug for NativeWeak<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

impl<T: ?Sized> Pointer for NativeWeak<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.as_ptr(), f)
    }
}

unsafe impl<T: ?Sized + Sync + Send> Send for NativeWeak<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for NativeWeak<T> {}

impl<T: ?Sized + RefUnwindSafe> RefUnwindSafe for NativeWeak<T> {}
impl<T: ?Sized + RefUnwindSafe> UnwindSafe for NativeWeak<T> {}
impl<T: ?Sized> Unpin for NativeWeak<T> {}

impl<T: ?Sized> From<AllocWeak<T>> for NativeWeak<T> {
    #[inline]
    fn from(inner: AllocWeak<T>) -> Self {
        Self::from_inner(inner)
    }
}

impl<T: ?Sized> From<NativeWeak<T>> for AllocWeak<T> {
    #[inline]
    fn from(weak: NativeWeak<T>) -> Self {
        NativeWeak::into_inner(weak)
    }
}

#[cfg(feature = "loom")]
/// Loom 仿真弱引用计数指针包装类型。基于控制块和原子操作独立实现。
pub struct LoomWeak<T: ?Sized> {
    pub(crate) ptr: *mut T,
    pub(crate) control: Option<NonNull<ArcControlBlock>>,
}

#[cfg(feature = "loom")]
impl<T> LoomWeak<T> {
    /// 创建一个新的悬空 `LoomWeak` 指针。不进行任何堆内存分配。
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            ptr: NonNull::dangling().as_ptr(),
            control: None,
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> LoomWeak<T> {
    #[inline]
    pub(crate) fn from_raw_parts(ptr: *mut T, control: Option<NonNull<ArcControlBlock>>) -> Self {
        Self { ptr, control }
    }

    /// 返回指向底层数据的裸指针。
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        self.ptr as *const T
    }

    /// 消费 `LoomWeak` 并返回裸指针。
    #[inline]
    #[must_use]
    pub fn into_raw(self) -> *const T {
        let ptr = self.ptr;
        if let Some(control) = self.control {
            loom_raw_registry::register(ptr as *const () as usize, control);
        }
        core::mem::forget(self);
        ptr as *const T
    }

    /// 从之前由 `into_raw` 返回的裸指针重建 `LoomWeak`。
    ///
    /// # Safety
    ///
    /// 传入的指针必须源自 `LoomWeak::into_raw`。
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        let control = loom_raw_registry::unregister(ptr as *const () as usize);
        Self {
            ptr: ptr as *mut T,
            control,
        }
    }

    /// 尝试升级为 `LoomArc<T>`。
    #[inline]
    #[must_use]
    pub fn upgrade(&self) -> Option<LoomArc<T>> {
        let control = self.control?;
        let c_ref = unsafe { control.as_ref() };
        let mut strong = c_ref.strong.load(AtomicOrdering::Relaxed);
        loop {
            if strong == 0 {
                return None;
            }
            match c_ref.strong.compare_exchange_weak(
                strong,
                strong + 1,
                AtomicOrdering::Acquire,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(LoomArc {
                        ptr: self.ptr,
                        control,
                    });
                }
                Err(actual) => strong = actual,
            }
        }
    }

    /// 获取指向同一分配的强引用计数。
    #[inline]
    #[must_use]
    pub fn strong_count(&self) -> usize {
        match self.control {
            Some(control) => unsafe { control.as_ref().strong.load(AtomicOrdering::SeqCst) },
            None => 0,
        }
    }

    /// 获取指向同一分配的弱引用计数。
    #[inline]
    #[must_use]
    pub fn weak_count(&self) -> usize {
        match self.control {
            Some(control) => {
                let weak = unsafe { control.as_ref().weak.load(AtomicOrdering::SeqCst) };
                weak.saturating_sub(1)
            }
            None => 0,
        }
    }

    /// 判断两个 `LoomWeak` 是否指向相同的分配。
    #[inline]
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (self.control, other.control) {
            (Some(a), Some(b)) => a == b && core::ptr::eq(self.ptr, other.ptr),
            (None, None) => true,
            _ => false,
        }
    }

    /// 统一的 unsized 转换接口：通过裸指针转换闭包将 `LoomWeak<T>` 转换为 `LoomWeak<U>`。
    ///
    /// # Safety
    ///
    /// 闭包 `f` 必须仅进行合法保持底层数据地址不变的类型转换（例如 `|p| p as *const dyn Trait`）。
    #[inline]
    pub unsafe fn cast_unsized<U: ?Sized, F>(this: Self, f: F) -> LoomWeak<U>
    where
        F: FnOnce(*const T) -> *const U,
    {
        let new_ptr = f(this.ptr as *const T) as *mut U;
        let control = this.control;
        core::mem::forget(this);
        LoomWeak {
            ptr: new_ptr,
            control,
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Clone for LoomWeak<T> {
    #[inline]
    fn clone(&self) -> Self {
        if let Some(control) = self.control {
            unsafe {
                control.as_ref().weak.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
        Self {
            ptr: self.ptr,
            control: self.control,
        }
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Drop for LoomWeak<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(control) = self.control {
            let c_ref = unsafe { control.as_ref() };
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
impl<T> Default for LoomWeak<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Debug> Debug for LoomWeak<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(Weak)")
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> Pointer for LoomWeak<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.as_ptr(), f)
    }
}

#[cfg(feature = "loom")]
unsafe impl<T: ?Sized + Sync + Send> Send for LoomWeak<T> {}

#[cfg(feature = "loom")]
unsafe impl<T: ?Sized + Sync + Send> Sync for LoomWeak<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized + RefUnwindSafe> RefUnwindSafe for LoomWeak<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized + RefUnwindSafe> UnwindSafe for LoomWeak<T> {}

#[cfg(feature = "loom")]
impl<T: ?Sized> Unpin for LoomWeak<T> {}

#[cfg(not(feature = "loom"))]
pub type Weak<T> = NativeWeak<T>;

#[cfg(feature = "loom")]
pub type Weak<T> = LoomWeak<T>;
