use crate::{
    sync::{
        atomic::{NativeAtomicU32, Ordering},
        sys::native,
    },
    time::{Duration, Instant},
};

use lock_api::{GuardSend, RawMutex as RawMutexTrait, RawMutexFair, RawMutexTimed};

#[cfg(feature = "loom")]
use crate::sync::{atomic::LoomAtomicU32, sys::loom::WaitChannel};

const LOCKED_BIT: u32 = 1;
const WAITER_ONE: u32 = 2;
const WAITER_MASK: u32 = !1;

macro_rules! impl_raw_mutex {
    (
        $(#[$meta:meta])*
        struct $name:ident,
        atomic: $atomic_ty:ty,
        $(channel: $channel_field:ident: $channel_ty:ty, init_channel: $init_channel:expr,)?
        wait: |$s_wait:ident, $obs_wait:ident| $wait_expr:expr,
        wait_timeout: |$s_wt:ident, $obs_wt:ident, $rem_wt:ident| $wait_timeout_expr:expr,
        wake_one: |$s_wake:ident| $wake_expr:expr,
        const_new: $is_const:ident
    ) => {
        $(#[$meta])*
        pub struct $name {
            state: $atomic_ty,
            $($channel_field: $channel_ty,)?
        }

        impl $name {
            impl_raw_mutex!(@new_fn $is_const, $atomic_ty $(, $channel_field: $init_channel)?);

            #[inline]
            pub fn lock(&self) {
                if self
                    .state
                    .compare_exchange(0, LOCKED_BIT, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }

                self.lock_slow();
            }

            fn lock_slow(&self) {
                self.state.fetch_add(WAITER_ONE, Ordering::Relaxed);

                let mut observed = self.state.load(Ordering::Relaxed);
                loop {
                    if observed & LOCKED_BIT == 0 {
                        let current = (observed - WAITER_ONE) | LOCKED_BIT;
                        match self.state.compare_exchange_weak(
                            observed,
                            current,
                            Ordering::Acquire,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => return,
                            Err(actual) => {
                                observed = actual;
                                continue;
                            }
                        }
                    }

                    let $s_wait = self;
                    let $obs_wait = observed;
                    let _ = $wait_expr;

                    observed = self.state.load(Ordering::Relaxed);
                }
            }

            #[inline]
            pub fn try_lock(&self) -> bool {
                let mut observed = self.state.load(Ordering::Relaxed);
                loop {
                    if observed & LOCKED_BIT != 0 {
                        return false;
                    }
                    match self.state.compare_exchange_weak(
                        observed,
                        observed | LOCKED_BIT,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return true,
                        Err(actual) => observed = actual,
                    }
                }
            }

            /// Unlocks the mutex.
            ///
            /// # Safety
            ///
            /// The caller must currently hold the lock.
            #[inline]
            pub unsafe fn unlock(&self) {
                let prev = self.state.fetch_sub(LOCKED_BIT, Ordering::Release);
                assert!(prev & LOCKED_BIT != 0, "Mutex is not locked");
                if prev & WAITER_MASK != 0 {
                    let $s_wake = self;
                    $wake_expr;
                }
            }

            #[inline]
            pub fn is_locked(&self) -> bool {
                self.state.load(Ordering::Relaxed) & LOCKED_BIT != 0
            }

            #[inline]
            pub fn try_lock_for(&self, timeout: Duration) -> bool {
                let now = Instant::now();
                self.try_lock_until(now + timeout)
            }

            #[inline]
            pub fn try_lock_until(&self, timeout: Instant) -> bool {
                if self.try_lock() {
                    return true;
                }

                self.state.fetch_add(WAITER_ONE, Ordering::Relaxed);
                let mut observed = self.state.load(Ordering::Relaxed);
                loop {
                    if observed & LOCKED_BIT == 0 {
                        let current = (observed - WAITER_ONE) | LOCKED_BIT;
                        match self.state.compare_exchange_weak(
                            observed,
                            current,
                            Ordering::Acquire,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => return true,
                            Err(actual) => {
                                observed = actual;
                                continue;
                            }
                        }
                    }

                    let now = Instant::now();
                    if now >= timeout {
                        let mut cur = self.state.load(Ordering::Relaxed);
                        loop {
                            let next = cur - WAITER_ONE;
                            match self.state.compare_exchange_weak(
                                cur,
                                next,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => {
                                    if cur & LOCKED_BIT == 0 && next & WAITER_MASK != 0 {
                                        let $s_wake = self;
                                        $wake_expr;
                                    }
                                    return false;
                                }
                                Err(actual) => cur = actual,
                            }
                        }
                    }

                    let remaining = timeout.duration_since(now);
                    let $s_wt = self;
                    let $obs_wt = observed;
                    let $rem_wt = remaining;
                    let _ = $wait_timeout_expr;

                    observed = self.state.load(Ordering::Relaxed);
                }
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }
    };

    (@new_fn const, $atomic_ty:ty) => {
        #[inline]
        pub const fn new() -> Self {
            Self {
                state: <$atomic_ty>::new(0),
            }
        }
    };

    (@new_fn non_const, $atomic_ty:ty, $channel_field:ident: $init_channel:expr) => {
        #[inline]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                state: <$atomic_ty>::new(0),
                $channel_field: $init_channel,
            }
        }
    };
}

impl_raw_mutex!(
    #[derive(Debug)]
    struct NativeRawMutex,
    atomic: NativeAtomicU32,
    wait: |s, obs| native::wait_on_address(&s.state, obs),
    wait_timeout: |s, obs, rem| {
        native::wait_on_address_timeout(&s.state, obs, Some(rem))
    },
    wake_one: |s| native::wake_by_address(&s.state),
    const_new: const
);

unsafe impl RawMutexTrait for NativeRawMutex {
    const INIT: Self = Self::new();

    type GuardMarker = GuardSend;

    #[inline]
    fn lock(&self) {
        self.lock();
    }

    #[inline]
    fn try_lock(&self) -> bool {
        self.try_lock()
    }

    #[inline]
    unsafe fn unlock(&self) {
        unsafe {
            self.unlock();
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.is_locked()
    }
}

unsafe impl RawMutexFair for NativeRawMutex {
    #[inline]
    unsafe fn unlock_fair(&self) {
        unsafe {
            self.unlock();
        }
    }

    #[inline]
    unsafe fn bump(&self) {
        unsafe {
            self.unlock();
        }
        self.lock();
    }
}

unsafe impl RawMutexTimed for NativeRawMutex {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_for(&self, timeout: Self::Duration) -> bool {
        self.try_lock_for(timeout)
    }

    #[inline]
    fn try_lock_until(&self, timeout: Self::Instant) -> bool {
        self.try_lock_until(timeout)
    }
}

#[cfg(feature = "loom")]
impl_raw_mutex!(
    struct LoomRawMutex,
    atomic: LoomAtomicU32,
    channel: channel: WaitChannel, init_channel: WaitChannel::new(),
    wait: |s, obs| s.channel.wait(&s.state, obs),
    wait_timeout: |s, obs, rem| s.channel.wait_timeout(&s.state, obs, rem),
    wake_one: |s| s.channel.wake_one(),
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type RawMutex = NativeRawMutex;

#[cfg(feature = "loom")]
pub type RawMutex = LoomRawMutex;
