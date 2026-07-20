use crate::time::Duration;

#[cfg(not(feature = "loom"))]
use crate::sync::{
    atomic::{AtomicU32, Ordering},
    sys::{wait_on_address, wait_on_address_timeout, wake_by_address},
};

#[cfg(feature = "loom")]
use loom::sync::{Condvar, Mutex};

#[cfg(not(feature = "loom"))]
const EMPTY: u32 = 0;
#[cfg(not(feature = "loom"))]
const PARKED: u32 = 1;
#[cfg(not(feature = "loom"))]
const NOTIFIED: u32 = 2;

#[cfg(not(feature = "loom"))]
pub(crate) struct Parker {
    state: AtomicU32,
}

#[cfg(not(feature = "loom"))]
impl Parker {
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(EMPTY),
        }
    }

    pub fn park(&self) {
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
        if self
            .state
            .compare_exchange(EMPTY, PARKED, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            while self.state.load(Ordering::Acquire) == PARKED {
                wait_on_address(&self.state, PARKED);
            }
        }
    }

    pub fn park_timeout(&self, dur: Duration) {
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
        if self
            .state
            .compare_exchange(EMPTY, PARKED, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            wait_on_address_timeout(&self.state, PARKED, Some(dur));
            self.state.swap(EMPTY, Ordering::Acquire);
        }
    }

    pub fn unpark(&self) {
        let old = self.state.swap(NOTIFIED, Ordering::Release);
        if old == PARKED {
            wake_by_address(&self.state);
        }
    }
}

#[cfg(feature = "loom")]
const EMPTY: u32 = 0;
#[cfg(feature = "loom")]
const PARKED: u32 = 1;
#[cfg(feature = "loom")]
const NOTIFIED: u32 = 2;

#[cfg(feature = "loom")]
pub(crate) struct Parker {
    inner: Mutex<u32>,
    condvar: Condvar,
}

#[cfg(feature = "loom")]
impl Parker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(EMPTY),
            condvar: Condvar::new(),
        }
    }

    pub fn park(&self) {
        let mut state = self.inner.lock().unwrap();
        if *state == NOTIFIED {
            *state = EMPTY;
            return;
        }
        if *state == EMPTY {
            *state = PARKED;
            while *state == PARKED {
                state = self.condvar.wait(state).unwrap();
            }
        }
    }

    pub fn park_timeout(&self, dur: Duration) {
        let mut state = self.inner.lock().unwrap();
        if *state == NOTIFIED {
            *state = EMPTY;
            return;
        }
        if *state == EMPTY {
            *state = PARKED;
            let (g, _) = self.condvar.wait_timeout(state, dur).unwrap();
            let mut state = g;
            *state = EMPTY;
        }
    }

    pub fn unpark(&self) {
        let mut state = self.inner.lock().unwrap();
        let old = *state;
        *state = NOTIFIED;
        if old == PARKED {
            self.condvar.notify_one();
        }
    }
}
