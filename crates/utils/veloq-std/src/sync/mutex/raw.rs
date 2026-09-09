use crate::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

#[cfg(not(feature = "loom"))]
use crate::sync::sys;
#[cfg(not(feature = "loom"))]
use lock_api::{GuardSend, RawMutex as RawMutexTrait, RawMutexFair, RawMutexTimed};

#[cfg(feature = "loom")]
use crate::sync::sys::loom::WaitChannel;

pub struct RawMutex {
    state: AtomicU32,
    #[cfg(feature = "loom")]
    channel: WaitChannel,
}

const LOCKED_BIT: u32 = 1;
const WAITER_ONE: u32 = 2;
const WAITER_MASK: u32 = !1;

impl RawMutex {
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            channel: WaitChannel::new(),
        }
    }

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

            #[cfg(not(feature = "loom"))]
            sys::wait_on_address(&self.state, observed);

            #[cfg(feature = "loom")]
            self.channel.wait(&self.state, observed);

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
            #[cfg(not(feature = "loom"))]
            sys::wake_by_address(&self.state);

            #[cfg(feature = "loom")]
            self.channel.wake_one();
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
                                #[cfg(not(feature = "loom"))]
                                sys::wake_by_address(&self.state);

                                #[cfg(feature = "loom")]
                                self.channel.wake_one();
                            }
                            return false;
                        }
                        Err(actual) => cur = actual,
                    }
                }
            }

            let remaining = timeout.duration_since(now);

            #[cfg(not(feature = "loom"))]
            sys::wait_on_address_timeout(&self.state, observed, Some(remaining));

            #[cfg(feature = "loom")]
            self.channel.wait_timeout(&self.state, observed, remaining);

            observed = self.state.load(Ordering::Relaxed);
        }
    }
}

impl Default for RawMutex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(feature = "loom"))]
unsafe impl RawMutexTrait for RawMutex {
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

#[cfg(not(feature = "loom"))]
unsafe impl RawMutexFair for RawMutex {
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

#[cfg(not(feature = "loom"))]
unsafe impl RawMutexTimed for RawMutex {
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
