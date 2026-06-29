use crate::{
    sync::atomic::{AtomicU32, Ordering},
    sys,
    time::{Duration, Instant},
};
use lock_api::{GuardSend, RawMutex as RawMutexTrait, RawMutexFair, RawMutexTimed};

pub struct RawMutex {
    state: AtomicU32,
}

unsafe impl RawMutexTrait for RawMutex {
    const INIT: Self = Self {
        state: AtomicU32::new(0),
    };

    type GuardMarker = GuardSend;

    #[inline]
    fn lock(&self) {
        let c = self
            .state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed);
        if let Err(mut c) = c {
            if c != 2 {
                c = self.state.swap(2, Ordering::Acquire);
            }
            while c != 0 {
                sys::wait_on_address_timeout(&self.state, 2, None);
                c = self.state.swap(2, Ordering::Acquire);
            }
        }
    }

    #[inline]
    fn try_lock(&self) -> bool {
        self.state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    unsafe fn unlock(&self) {
        if self.state.fetch_sub(1, Ordering::Release) != 1 {
            self.state.store(0, Ordering::Release);
            sys::wake_by_address(&self.state);
        }
    }
}

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

unsafe impl RawMutexTimed for RawMutex {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_for(&self, timeout: Self::Duration) -> bool {
        let now = Instant::now();
        self.try_lock_until(now + timeout)
    }

    #[inline]
    fn try_lock_until(&self, timeout: Self::Instant) -> bool {
        let c = self
            .state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed);
        if c.is_ok() {
            return true;
        }
        let mut c = c.unwrap_err();
        if c != 2 {
            c = self.state.swap(2, Ordering::Acquire);
        }
        while c != 0 {
            let now = Instant::now();
            if now >= timeout {
                return false;
            }
            let dur = timeout.duration_since(now);
            if sys::wait_on_address_timeout(&self.state, 2, Some(dur)) {
                if self.state.load(Ordering::Relaxed) == 0 {
                    c = self.state.swap(2, Ordering::Acquire);
                    if c == 0 {
                        return true;
                    }
                }
                if Instant::now() >= timeout {
                    return false;
                }
            }
            c = self.state.swap(2, Ordering::Acquire);
        }
        true
    }
}
