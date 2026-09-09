use core::{fmt, time::Duration};

use crate::{
    sync::{
        MutexGuard,
        atomic::{AtomicU32, Ordering},
    },
    time::Instant,
};

#[cfg(not(feature = "loom"))]
use crate::sync::sys;

#[cfg(feature = "loom")]
use crate::sync::sys::loom::WaitChannel;

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
pub struct Condvar {
    state: AtomicU32,
    #[cfg(feature = "loom")]
    channel: WaitChannel,
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

impl Condvar {
    /// 创建一个新的条件变量。
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    /// 创建一个新的条件变量。
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            channel: WaitChannel::new(),
        }
    }

    /// 阻塞当前线程，直到此条件变量收到通知。
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        let mutex = MutexGuard::mutex(&guard);
        let current_state = self.state.load(Ordering::Relaxed);

        drop(guard);

        #[cfg(not(feature = "loom"))]
        while self.state.load(Ordering::Relaxed) == current_state {
            sys::wait_on_address(&self.state, current_state);
        }

        #[cfg(feature = "loom")]
        while self.state.load(Ordering::Relaxed) == current_state {
            self.channel.wait(&self.state, current_state);
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

            #[cfg(not(feature = "loom"))]
            if sys::wait_on_address_timeout(&self.state, current_state, Some(remaining)) {
                timeout = true;
                break;
            }

            #[cfg(feature = "loom")]
            if !self
                .channel
                .wait_timeout(&self.state, current_state, remaining)
            {
                timeout = true;
                break;
            }
        }

        (mutex.lock(), WaitTimeoutResult(timeout))
    }

    /// 唤醒在此条件变量上等待的其中一个线程。
    pub fn notify_one(&self) {
        self.state.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(feature = "loom"))]
        sys::wake_by_address(&self.state);

        #[cfg(feature = "loom")]
        self.channel.wake_one();
    }

    /// 唤醒在此条件变量上等待的所有线程。
    pub fn notify_all(&self) {
        self.state.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(feature = "loom"))]
        sys::wake_all_by_address(&self.state);

        #[cfg(feature = "loom")]
        self.channel.wake_all();
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}
