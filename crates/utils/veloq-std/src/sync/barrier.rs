use crate::{
    fmt,
    sync::{condvar::NativeUnpoisonedCondvar, unpoisoned_mutex::NativeUnpoisonedMutex},
};

#[cfg(feature = "loom")]
use crate::sync::{condvar::LoomUnpoisonedCondvar, unpoisoned_mutex::LoomUnpoisonedMutex};

struct BarrierState {
    count: usize,
    generation_id: usize,
}

/// [`Barrier::wait`] 返回的结果。
pub struct BarrierWaitResult(bool);

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl BarrierWaitResult {
    /// 返回当前线程是否是本轮屏障的 leader。
    #[must_use]
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.0
    }
}

macro_rules! impl_barrier {
    (
        $(#[$meta:meta])*
        struct $name:ident,
        mutex: $mutex_ty:ident,
        condvar: $cvar_ty:ident,
        new: $new:item
    ) => {
        $(#[$meta])*
        pub struct $name {
            lock: $mutex_ty<BarrierState>,
            cvar: $cvar_ty,
            num_threads: usize,
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }

        impl $name {
            $new

            /// 阻塞当前线程，直到本轮所有线程都到达屏障。
            ///
            /// 每轮恰有一个任意线程会获得 leader 结果，其余线程获得普通结果。
            pub fn wait(&self) -> BarrierWaitResult {
                let mut state = self.lock.lock();
                let local_generation = state.generation_id;
                state.count += 1;

                if state.count < self.num_threads {
                    while state.generation_id == local_generation {
                        state = self.cvar.wait(state);
                    }
                    BarrierWaitResult(false)
                } else {
                    state.count = 0;
                    state.generation_id = state.generation_id.wrapping_add(1);
                    self.cvar.notify_all();
                    BarrierWaitResult(true)
                }
            }
        }
    };
}

impl_barrier!(
    /// 原生多线程同步屏障，始终可用。
    struct NativeBarrier,
    mutex: NativeUnpoisonedMutex,
    condvar: NativeUnpoisonedCondvar,
    new: #[inline]
    pub const fn new(n: usize) -> Self {
        Self {
            lock: NativeUnpoisonedMutex::new(BarrierState {
                count: 0,
                generation_id: 0,
            }),
            cvar: NativeUnpoisonedCondvar::new(),
            num_threads: n,
        }
    }
);

#[cfg(feature = "loom")]
impl_barrier!(
    /// Loom 验证专用的多线程同步屏障。
    struct LoomBarrier,
    mutex: LoomUnpoisonedMutex,
    condvar: LoomUnpoisonedCondvar,
    new: #[inline]
    #[track_caller]
    pub fn new(n: usize) -> Self {
        Self {
            lock: LoomUnpoisonedMutex::new(BarrierState {
                count: 0,
                generation_id: 0,
            }),
            cvar: LoomUnpoisonedCondvar::new(),
            num_threads: n,
        }
    }
);

#[cfg(not(feature = "loom"))]
pub type Barrier = NativeBarrier;

#[cfg(feature = "loom")]
pub type Barrier = LoomBarrier;

/// 创建可用于静态初始化的原生屏障。
#[inline]
pub const fn const_native_barrier(n: usize) -> NativeBarrier {
    NativeBarrier::new(n)
}

#[cfg(not(feature = "loom"))]
/// 创建当前配置下的屏障。
#[inline]
pub const fn const_barrier(n: usize) -> Barrier {
    const_native_barrier(n)
}
