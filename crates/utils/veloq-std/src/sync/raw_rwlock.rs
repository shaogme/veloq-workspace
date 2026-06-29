use crate::{
    sync::atomic::{AtomicU32, Ordering},
    sys,
    time::{Duration, Instant},
};
use lock_api::{
    GuardSend, RawRwLock as RawRwLockTrait, RawRwLockDowngrade, RawRwLockFair, RawRwLockTimed,
};

const READER_MASK: u32 = 0x0000_FFFF;
const WRITER_WAITING_MASK: u32 = 0x3FFF_0000;
const WRITER_WAITING_ONE: u32 = 0x0001_0000;
const WRITER_BIT: u32 = 0x8000_0000;

pub struct RawRwLock {
    state: AtomicU32,
}

unsafe impl RawRwLockTrait for RawRwLock {
    const INIT: Self = Self {
        state: AtomicU32::new(0),
    };

    type GuardMarker = GuardSend;

    #[inline]
    fn lock_shared(&self) {
        if self.try_lock_shared() {
            return;
        }
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if (state & WRITER_BIT) == 0 && (state & WRITER_WAITING_MASK) == 0 {
                let new_state = state + 1;
                if (new_state & READER_MASK) == 0 {
                    panic!("RwLock reader count overflow");
                }
                if self
                    .state
                    .compare_exchange_weak(state, new_state, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
            }
            sys::wait_on_address(&self.state, state);
            state = self.state.load(Ordering::Relaxed);
        }
    }

    #[inline]
    fn try_lock_shared(&self) -> bool {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if (state & WRITER_BIT) != 0 || (state & WRITER_WAITING_MASK) != 0 {
                return false;
            }
            let new_state = state + 1;
            if (new_state & READER_MASK) == 0 {
                panic!("RwLock reader count overflow");
            }
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(s) => state = s,
            }
        }
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let new_state = state - 1;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if (new_state & READER_MASK) == 0 && (new_state & WRITER_WAITING_MASK) != 0 {
                        sys::wake_by_address(&self.state);
                    }
                    return;
                }
                Err(s) => state = s,
            }
        }
    }

    #[inline]
    fn lock_exclusive(&self) {
        if self.try_lock_exclusive() {
            return;
        }
        let mut state = self.state.load(Ordering::Relaxed);
        let mut incremented = false;
        loop {
            if (state & (WRITER_BIT | READER_MASK)) == 0 {
                let mut new_state = state | WRITER_BIT;
                if incremented {
                    new_state -= WRITER_WAITING_ONE;
                }
                match self.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }
            if !incremented {
                let new_state = state + WRITER_WAITING_ONE;
                match self.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        state = new_state;
                        incremented = true;
                    }
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }
            sys::wait_on_address(&self.state, state);
            state = self.state.load(Ordering::Relaxed);
        }
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if (state & (WRITER_BIT | READER_MASK)) != 0 {
                return false;
            }
            let new_state = state | WRITER_BIT;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(s) => state = s,
            }
        }
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let new_state = state & !WRITER_BIT;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    sys::wake_all_by_address(&self.state);
                    return;
                }
                Err(s) => state = s,
            }
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        (state & (WRITER_BIT | READER_MASK)) != 0
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        (self.state.load(Ordering::Relaxed) & WRITER_BIT) != 0
    }
}

unsafe impl RawRwLockFair for RawRwLock {
    #[inline]
    unsafe fn unlock_shared_fair(&self) {
        unsafe {
            self.unlock_shared();
        }
    }

    #[inline]
    unsafe fn unlock_exclusive_fair(&self) {
        unsafe {
            self.unlock_exclusive();
        }
    }

    #[inline]
    unsafe fn bump_shared(&self) {
        unsafe {
            self.unlock_shared();
        }
        self.lock_shared();
    }

    #[inline]
    unsafe fn bump_exclusive(&self) {
        unsafe {
            self.unlock_exclusive();
        }
        self.lock_exclusive();
    }
}

unsafe impl RawRwLockDowngrade for RawRwLock {
    #[inline]
    unsafe fn downgrade(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let new_state = (state & !WRITER_BIT) + 1;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    sys::wake_all_by_address(&self.state);
                    return;
                }
                Err(s) => state = s,
            }
        }
    }
}

unsafe impl RawRwLockTimed for RawRwLock {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_shared_for(&self, timeout: Self::Duration) -> bool {
        let now = Instant::now();
        self.try_lock_shared_until(now + timeout)
    }

    #[inline]
    fn try_lock_shared_until(&self, timeout: Self::Instant) -> bool {
        if self.try_lock_shared() {
            return true;
        }
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let now = Instant::now();
            if now >= timeout {
                return false;
            }
            if (state & WRITER_BIT) == 0 && (state & WRITER_WAITING_MASK) == 0 {
                let new_state = state + 1;
                if (new_state & READER_MASK) == 0 {
                    panic!("RwLock reader count overflow");
                }
                if self
                    .state
                    .compare_exchange_weak(state, new_state, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return true;
                }
            }
            let dur = timeout.duration_since(now);
            sys::wait_on_address_timeout(&self.state, state, Some(dur));
            state = self.state.load(Ordering::Relaxed);
        }
    }

    #[inline]
    fn try_lock_exclusive_for(&self, timeout: Self::Duration) -> bool {
        let now = Instant::now();
        self.try_lock_exclusive_until(now + timeout)
    }

    #[inline]
    fn try_lock_exclusive_until(&self, timeout: Self::Instant) -> bool {
        if self.try_lock_exclusive() {
            return true;
        }
        let mut state = self.state.load(Ordering::Relaxed);
        let mut incremented = false;
        loop {
            let now = Instant::now();
            if now >= timeout {
                if incremented {
                    let mut s = self.state.load(Ordering::Relaxed);
                    loop {
                        let new_s = s - WRITER_WAITING_ONE;
                        match self.state.compare_exchange_weak(
                            s,
                            new_s,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => s = actual,
                        }
                    }
                }
                return false;
            }
            if (state & (WRITER_BIT | READER_MASK)) == 0 {
                let mut new_state = state | WRITER_BIT;
                if incremented {
                    new_state -= WRITER_WAITING_ONE;
                }
                match self.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }
            if !incremented {
                let new_state = state + WRITER_WAITING_ONE;
                match self.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        state = new_state;
                        incremented = true;
                    }
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }
            let dur = timeout.duration_since(now);
            sys::wait_on_address_timeout(&self.state, state, Some(dur));
            state = self.state.load(Ordering::Relaxed);
        }
    }
}
