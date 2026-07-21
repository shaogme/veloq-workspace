use core::fmt;

#[cfg(not(feature = "loom"))]
use crate::{
    sync::{
        MutexGuard,
        atomic::{AtomicU32, Ordering},
        sys,
    },
    time::Instant,
};
#[cfg(not(feature = "loom"))]
use core::time::Duration;

#[cfg(feature = "loom")]
use crate::sync::MutexGuard;
#[cfg(feature = "loom")]
use core::time::Duration;

/// 状态等待结果，用于表示等待是否超时。
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    /// 如果等待超时返回 `true`，否则返回 `false`。
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

/// 条件变量
#[cfg(not(feature = "loom"))]
pub struct Condvar {
    state: AtomicU32,
}

#[cfg(not(feature = "loom"))]
impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

#[cfg(not(feature = "loom"))]
impl Condvar {
    /// 创建一个新的条件变量。
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    /// 阻塞当前线程，直到此条件变量收到通知。
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        let mutex = MutexGuard::mutex(&guard);
        let current_state = self.state.load(Ordering::Relaxed);

        drop(guard);

        while self.state.load(Ordering::Relaxed) == current_state {
            sys::wait_on_address(&self.state, current_state);
        }

        mutex.lock()
    }

    /// 阻塞当前线程，直到此条件变量收到通知，或达到指定的超时时间。
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        dur: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        let mutex = MutexGuard::mutex(&guard);
        let current_state = self.state.load(Ordering::Relaxed);

        drop(guard);

        let mut timeout = false;
        let start = Instant::now();

        while self.state.load(Ordering::Relaxed) == current_state {
            let elapsed = start.elapsed();
            if elapsed >= dur {
                timeout = true;
                break;
            }
            let remaining = dur - elapsed;
            if sys::wait_on_address_timeout(&self.state, current_state, Some(remaining)) {
                timeout = true;
                break;
            }
        }

        (mutex.lock(), WaitTimeoutResult(timeout))
    }

    /// 唤醒在此条件变量上等待的其中一个线程。
    pub fn notify_one(&self) {
        self.state.fetch_add(1, Ordering::Relaxed);
        sys::wake_by_address(&self.state);
    }

    /// 唤醒在此条件变量上等待的所有线程。
    pub fn notify_all(&self) {
        self.state.fetch_add(1, Ordering::Relaxed);
        sys::wake_all_by_address(&self.state);
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

/// 条件变量的 Loom 仿真版本包装。
#[cfg(feature = "loom")]
pub struct Condvar {
    inner: loom::sync::Condvar,
}

#[cfg(feature = "loom")]
impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

#[cfg(feature = "loom")]
impl Condvar {
    /// 创建一个新的仿真条件变量。
    pub fn new() -> Self {
        Self {
            inner: loom::sync::Condvar::new(),
        }
    }

    /// 阻塞当前线程，直到此条件变量收到通知。
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.inner.wait(guard).unwrap()
    }

    /// 阻塞当前线程，直到此条件变量收到通知，或达到指定的超时时间。
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        dur: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        let (g, res) = self.inner.wait_timeout(guard, dur).unwrap();
        (g, WaitTimeoutResult(res.timed_out()))
    }

    /// 唤醒在此条件变量上等待的其中一个线程。
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// 唤醒在此条件变量上等待的所有线程。
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }
}
