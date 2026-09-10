use core::{fmt, marker::PhantomData, ops::Deref};

use crate::{
    cell::NativeUnsafeCell,
    sync::{
        atomic::{NativeAtomicU64, NativeAtomicUsize, Ordering},
        mutex::raw::NativeRawMutex,
    },
    thread::native_current_id,
    time::{Duration, Instant},
};

#[cfg(feature = "loom")]
use crate::thread::current_id;

#[cfg(feature = "loom")]
use crate::{
    cell::LoomUnsafeCell,
    sync::{
        atomic::{LoomAtomicU64, LoomAtomicUsize},
        mutex::raw::LoomRawMutex,
    },
};

macro_rules! impl_reentrant_mutex {
    (
        $(#[$meta:meta])*
        struct $mutex_name:ident,
        guard: $guard_name:ident,
        raw_mutex: $raw_mutex_ty:ident,
        atomic_u64: $atomic_u64_ty:ident,
        atomic_usize: $atomic_usize_ty:ident,
        cell: $cell_ty:ident,
        current_id: $current_id:ident,
        new: $new:item
    ) => {
        $(#[$meta])*
        pub struct $mutex_name<T: ?Sized> {
            raw: $raw_mutex_ty,
            owner: $atomic_u64_ty,
            count: $atomic_usize_ty,
            data: $cell_ty<T>,
        }

        unsafe impl<T: ?Sized + Send> Send for $mutex_name<T> {}
        unsafe impl<T: ?Sized + Send> Sync for $mutex_name<T> {}

        impl<T> $mutex_name<T> {
            $new

            #[inline]
            pub fn into_inner(self) -> T {
                self.data.into_inner()
            }
        }

        impl<T: ?Sized> $mutex_name<T> {
            #[inline]
            fn guard(&self) -> $guard_name<'_, T> {
                $guard_name {
                    lock: self,
                    _marker: PhantomData,
                }
            }

            /// 获取锁；同一线程可重复获取该锁。
            pub fn lock(&self) -> $guard_name<'_, T> {
                let current_thread = $current_id().as_u64();
                if self.owner.load(Ordering::Relaxed) == current_thread {
                    let cur = self.count.load(Ordering::Relaxed);
                    self.count.store(
                        cur.checked_add(1).expect("reentrant lock count overflow"),
                        Ordering::Relaxed,
                    );
                } else {
                    self.raw.lock();
                    self.owner.store(current_thread, Ordering::Relaxed);
                    self.count.store(1, Ordering::Relaxed);
                }
                self.guard()
            }

            /// 尝试获取锁；同一线程重复获取时始终成功。
            pub fn try_lock(&self) -> Option<$guard_name<'_, T>> {
                let current_thread = $current_id().as_u64();
                if self.owner.load(Ordering::Relaxed) == current_thread {
                    let cur = self.count.load(Ordering::Relaxed);
                    self.count.store(
                        cur.checked_add(1).expect("reentrant lock count overflow"),
                        Ordering::Relaxed,
                    );
                    Some(self.guard())
                } else if self.raw.try_lock() {
                    self.owner.store(current_thread, Ordering::Relaxed);
                    self.count.store(1, Ordering::Relaxed);
                    Some(self.guard())
                } else {
                    None
                }
            }

            /// 在指定时长内尝试获取锁；同一线程重复获取时始终成功。
            pub fn try_lock_for(&self, timeout: Duration) -> Option<$guard_name<'_, T>> {
                let current_thread = $current_id().as_u64();
                if self.owner.load(Ordering::Relaxed) == current_thread {
                    let cur = self.count.load(Ordering::Relaxed);
                    self.count.store(
                        cur.checked_add(1).expect("reentrant lock count overflow"),
                        Ordering::Relaxed,
                    );
                    Some(self.guard())
                } else if self.raw.try_lock_for(timeout) {
                    self.owner.store(current_thread, Ordering::Relaxed);
                    self.count.store(1, Ordering::Relaxed);
                    Some(self.guard())
                } else {
                    None
                }
            }

            /// 截止指定时刻前尝试获取锁；同一线程重复获取时始终成功。
            pub fn try_lock_until(&self, timeout: Instant) -> Option<$guard_name<'_, T>> {
                let current_thread = $current_id().as_u64();
                if self.owner.load(Ordering::Relaxed) == current_thread {
                    let cur = self.count.load(Ordering::Relaxed);
                    self.count.store(
                        cur.checked_add(1).expect("reentrant lock count overflow"),
                        Ordering::Relaxed,
                    );
                    Some(self.guard())
                } else if self.raw.try_lock_until(timeout) {
                    self.owner.store(current_thread, Ordering::Relaxed);
                    self.count.store(1, Ordering::Relaxed);
                    Some(self.guard())
                } else {
                    None
                }
            }

            #[inline]
            pub fn is_locked(&self) -> bool {
                self.owner.load(Ordering::Relaxed) != 0
            }

            #[inline]
            pub fn is_owned_by_current_thread(&self) -> bool {
                self.owner.load(Ordering::Relaxed) == $current_id().as_u64()
            }

            #[inline]
            pub fn get_mut(&mut self) -> &mut T {
                unsafe { &mut *self.data.with_mut(|p| p as *mut T) }
            }

            #[inline]
            pub fn reentrancy_count(&self) -> usize {
                if self.is_owned_by_current_thread() {
                    self.count.load(Ordering::Relaxed)
                } else {
                    0
                }
            }

            #[inline]
            pub fn raw(&self) -> &$raw_mutex_ty {
                &self.raw
            }
        }

        /// 可重入互斥锁守卫。
        ///
        /// 该守卫只实现 [`Deref`]，不实现 `DerefMut`。同一线程可以同时持有多个
        /// 重入守卫，实现 `DerefMut` 会允许为同一数据制造多个别名可变引用。
        pub struct $guard_name<'a, T: ?Sized> {
            lock: &'a $mutex_name<T>,
            _marker: PhantomData<*const ()>,
        }

        unsafe impl<T: ?Sized + Sync> Sync for $guard_name<'_, T> {}

        impl<'a, T: ?Sized> $guard_name<'a, T> {
            #[inline]
            pub fn mutex(guard: &Self) -> &'a $mutex_name<T> {
                guard.lock
            }
        }

        impl<T: ?Sized> Deref for $guard_name<'_, T> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &Self::Target {
                unsafe { &*self.lock.data.with(|p| p as *const T) }
            }
        }

        impl<T: ?Sized> Drop for $guard_name<'_, T> {
            #[inline]
            fn drop(&mut self) {
                let cur = self.lock.count.load(Ordering::Relaxed);
                if cur > 1 {
                    self.lock.count.store(cur - 1, Ordering::Relaxed);
                } else {
                    self.lock.count.store(0, Ordering::Relaxed);
                    self.lock.owner.store(0, Ordering::Relaxed);
                    unsafe {
                        self.lock.raw.unlock();
                    }
                }
            }
        }

        impl<T: Default> Default for $mutex_name<T> {
            #[inline]
            fn default() -> Self {
                Self::new(T::default())
            }
        }

        impl<T> From<T> for $mutex_name<T> {
            #[inline]
            fn from(value: T) -> Self {
                Self::new(value)
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $mutex_name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = f.debug_struct(stringify!($mutex_name));
                if let Some(guard) = self.try_lock() {
                    debug.field("data", &&*guard);
                } else {
                    debug.field("data", &"<locked>");
                }
                debug.finish_non_exhaustive()
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + fmt::Display> fmt::Display for $guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&**self, f)
            }
        }
    };
}

impl_reentrant_mutex!(
    /// 原生可重入互斥锁，始终可用。
    struct NativeReentrantMutex,
    guard: NativeReentrantMutexGuard,
    raw_mutex: NativeRawMutex,
    atomic_u64: NativeAtomicU64,
    atomic_usize: NativeAtomicUsize,
    cell: NativeUnsafeCell,
    current_id: native_current_id,
    new: #[inline]
    pub const fn new(val: T) -> Self {
        Self {
            raw: NativeRawMutex::new(),
            owner: NativeAtomicU64::new(0),
            count: NativeAtomicUsize::new(0),
            data: NativeUnsafeCell::new(val),
        }
    }
);

#[cfg(feature = "loom")]
impl_reentrant_mutex!(
    /// Loom 验证专用的可重入互斥锁。
    struct LoomReentrantMutex,
    guard: LoomReentrantMutexGuard,
    raw_mutex: LoomRawMutex,
    atomic_u64: LoomAtomicU64,
    atomic_usize: LoomAtomicUsize,
    cell: LoomUnsafeCell,
    current_id: current_id,
    new: #[inline]
    #[track_caller]
    pub fn new(val: T) -> Self {
        Self {
            raw: LoomRawMutex::new(),
            owner: LoomAtomicU64::new(0),
            count: LoomAtomicUsize::new(0),
            data: LoomUnsafeCell::new(val),
        }
    }
);

#[cfg(not(feature = "loom"))]
pub type ReentrantMutex<T> = NativeReentrantMutex<T>;

#[cfg(not(feature = "loom"))]
pub type ReentrantMutexGuard<'a, T> = NativeReentrantMutexGuard<'a, T>;

#[cfg(feature = "loom")]
pub type ReentrantMutex<T> = LoomReentrantMutex<T>;

#[cfg(feature = "loom")]
pub type ReentrantMutexGuard<'a, T> = LoomReentrantMutexGuard<'a, T>;

/// 创建可用于静态初始化的原生可重入互斥锁。
#[inline]
pub const fn const_native_reentrant_mutex<T>(val: T) -> NativeReentrantMutex<T> {
    NativeReentrantMutex::new(val)
}

#[cfg(not(feature = "loom"))]
/// 创建当前配置下的可重入互斥锁。
#[inline]
pub const fn const_reentrant_mutex<T>(val: T) -> ReentrantMutex<T> {
    const_native_reentrant_mutex(val)
}
