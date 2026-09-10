use crate::{
    cell::NativeUnsafeCell,
    fmt,
    sync::atomic::{NativeAtomicBool, Ordering},
};
use crossbeam_utils::Backoff;

#[cfg(feature = "loom")]
use loom::sync::{Mutex as LoomMutex, MutexGuard as LoomMutexGuard};

/// 原生自旋锁，基于原子布尔值与自适应退避实现。无论 Loom 特性是否开启均可用。
pub struct NativeSpinLock<T> {
    locked: NativeAtomicBool,
    data: NativeUnsafeCell<T>,
}

unsafe impl<T: Send> Send for NativeSpinLock<T> {}
unsafe impl<T: Send> Sync for NativeSpinLock<T> {}

impl<T> NativeSpinLock<T> {
    /// 创建一个新的原生自旋锁。
    #[inline]
    pub const fn new(data: T) -> Self {
        Self {
            locked: NativeAtomicBool::new(false),
            data: NativeUnsafeCell::new(data),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }

    /// 获取原生自旋锁。
    pub fn lock(&self) -> NativeSpinLockGuard<'_, T> {
        let backoff = Backoff::new();
        while self.locked.swap(true, Ordering::Acquire) {
            backoff.snooze();
        }
        NativeSpinLockGuard { lock: self }
    }

    /// 尝试获取原生自旋锁。
    pub fn try_lock(&self) -> Option<NativeSpinLockGuard<'_, T>> {
        if !self.locked.swap(true, Ordering::Acquire) {
            Some(NativeSpinLockGuard { lock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Relaxed)
    }
}

/// 原生自旋锁守卫。
pub struct NativeSpinLockGuard<'a, T> {
    lock: &'a NativeSpinLock<T>,
}

impl<T> Drop for NativeSpinLockGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> NativeSpinLockGuard<'_, T> {
    #[inline]
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        unsafe { self.lock.data.with(f) }
    }

    #[inline]
    pub fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        unsafe { self.lock.data.with_mut(f) }
    }
}

#[cfg(feature = "loom")]
/// Loom 验证专用的自旋锁（内部降级为互斥锁以避免状态空间爆炸）。
pub struct LoomSpinLock<T> {
    inner: LoomMutex<T>,
}

#[cfg(feature = "loom")]
impl<T> LoomSpinLock<T> {
    /// 创建一个新的 Loom 自旋锁替代实现。
    #[inline]
    #[track_caller]
    pub fn new(data: T) -> Self {
        Self {
            inner: LoomMutex::new(data),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        match self.inner.into_inner() {
            Ok(data) => data,
            Err(error) => error.into_inner(),
        }
    }

    /// 获取 Loom 自旋锁替代实现。
    #[inline]
    pub fn lock(&self) -> LoomSpinLockGuard<'_, T> {
        LoomSpinLockGuard {
            inner: self.inner.lock().unwrap(),
        }
    }

    /// 尝试获取 Loom 自旋锁替代实现。
    #[inline]
    pub fn try_lock(&self) -> Option<LoomSpinLockGuard<'_, T>> {
        self.inner
            .try_lock()
            .ok()
            .map(|inner| LoomSpinLockGuard { inner })
    }

    /// 返回锁当前是否被占用。
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.inner.try_lock().is_err()
    }
}

#[cfg(feature = "loom")]
/// Loom 自旋锁守卫。
pub struct LoomSpinLockGuard<'a, T> {
    inner: LoomMutexGuard<'a, T>,
}

#[cfg(feature = "loom")]
impl<T> LoomSpinLockGuard<'_, T> {
    #[inline]
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.inner)
    }

    #[inline]
    pub fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut *self.inner)
    }
}

macro_rules! impl_spinlock_traits {
    ($lock_ty:ident) => {
        impl<T: Default> Default for $lock_ty<T> {
            #[inline]
            fn default() -> Self {
                Self::new(T::default())
            }
        }

        impl<T> From<T> for $lock_ty<T> {
            #[inline]
            fn from(data: T) -> Self {
                Self::new(data)
            }
        }

        impl<T: fmt::Debug> fmt::Debug for $lock_ty<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = f.debug_struct(stringify!($lock_ty));
                if let Some(guard) = self.try_lock() {
                    guard.with(|value| {
                        debug.field("data", value);
                    });
                } else {
                    debug.field("data", &"<locked>");
                }
                debug.finish_non_exhaustive()
            }
        }
    };
}

impl_spinlock_traits!(NativeSpinLock);

#[cfg(feature = "loom")]
impl_spinlock_traits!(LoomSpinLock);

#[cfg(not(feature = "loom"))]
pub type SpinLock<T> = NativeSpinLock<T>;

#[cfg(not(feature = "loom"))]
pub type SpinLockGuard<'a, T> = NativeSpinLockGuard<'a, T>;

#[cfg(feature = "loom")]
pub type SpinLock<T> = LoomSpinLock<T>;

#[cfg(feature = "loom")]
pub type SpinLockGuard<'a, T> = LoomSpinLockGuard<'a, T>;

/// 创建可用于静态初始化的原生自旋锁。
#[inline]
pub const fn const_native_spin_lock<T>(data: T) -> NativeSpinLock<T> {
    NativeSpinLock::new(data)
}

#[cfg(not(feature = "loom"))]
/// 创建当前配置下的自旋锁。
#[inline]
pub const fn const_spin_lock<T>(data: T) -> SpinLock<T> {
    const_native_spin_lock(data)
}
