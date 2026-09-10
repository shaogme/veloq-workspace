use crate::{
    sync::atomic::{LoomAtomicU32, Ordering},
    time::Duration,
};

/// 用于模拟共享原始锁的多等待者通道。
pub struct WaitChannel {
    mutex: loom::sync::Mutex<()>,
    cvar: loom::sync::Condvar,
}

impl WaitChannel {
    pub fn new() -> Self {
        Self {
            mutex: loom::sync::Mutex::new(()),
            cvar: loom::sync::Condvar::new(),
        }
    }

    /// 模拟操作系统 Futex 挂起：在持锁保护下检查 address == expected，若相等则等待一次唤醒后返回。
    pub fn wait(&self, address: &LoomAtomicU32, expected: u32) {
        let guard = self.mutex.lock().unwrap();
        if address.load(Ordering::Acquire) == expected {
            let _ = self.cvar.wait(guard).unwrap();
        }
    }

    /// 模拟操作系统 Futex 超时挂起。返回是否未超时。
    pub fn wait_timeout(&self, address: &LoomAtomicU32, expected: u32, dur: Duration) -> bool {
        let guard = self.mutex.lock().unwrap();
        if address.load(Ordering::Acquire) == expected {
            let (_guard, res) = self.cvar.wait_timeout(guard, dur).unwrap();
            !res.timed_out()
        } else {
            true
        }
    }

    /// 精准唤醒一个挂起的等待者。
    pub fn wake_one(&self) {
        let _g = self.mutex.lock().unwrap();
        self.cvar.notify_one();
    }

    /// 广播唤醒所有挂起的等待者。
    pub fn wake_all(&self) {
        let _g = self.mutex.lock().unwrap();
        self.cvar.notify_all();
    }
}

impl Default for WaitChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// 用于模拟单个条件变量 waiter 的一次性通知通道。
///
/// 每个条件变量 waiter 都独占一个通道，因此不需要再用一把 Loom 锁保护
/// 条件变量；`Notify` 自身会保留尚未消费的通知，从而关闭检查状态与进入等待
/// 之间的通知窗口。
pub struct WaiterChannel {
    notify: loom::sync::Notify,
}

impl WaiterChannel {
    pub fn new() -> Self {
        Self {
            notify: loom::sync::Notify::new(),
        }
    }

    pub fn wait(&self, address: &LoomAtomicU32, expected: u32) {
        if address.load(Ordering::Acquire) == expected {
            self.notify.wait();
        }
    }

    pub fn wait_timeout(&self, address: &LoomAtomicU32, expected: u32, _dur: Duration) -> bool {
        self.wait(address, expected);
        true
    }

    pub fn finish_notify(&self, address: &LoomAtomicU32, notified: u32) -> u32 {
        let previous = address.swap(notified, Ordering::AcqRel);
        self.notify.notify();
        previous
    }
}

impl Default for WaiterChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// 条件变量内部队列使用的单层 Loom 互斥锁。
///
/// 条件变量的公共互斥锁和 `LoomRawMutex` 仍然使用项目自己的实现；队列锁只是
/// 内部串行化链表访问，不应在条件变量模型中再次展开原始锁的等待后端。
pub struct LoomQueueMutex<T>(loom::sync::Mutex<T>);

pub struct LoomQueueMutexGuard<'a, T> {
    _inner: loom::sync::MutexGuard<'a, T>,
}

impl<T> LoomQueueMutex<T> {
    pub fn new(value: T) -> Self {
        Self(loom::sync::Mutex::new(value))
    }

    pub fn lock(&self) -> LoomQueueMutexGuard<'_, T> {
        LoomQueueMutexGuard {
            _inner: self.0.lock().unwrap(),
        }
    }
}

pub fn wait_on_address(address: &LoomAtomicU32, expected: u32) {
    wait_on_address_timeout(address, expected, None);
}

pub fn wait_on_address_timeout(
    address: &LoomAtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    let is_timeout = timeout.is_some();
    let mut limit = if is_timeout { 10 } else { 1000 };

    while address.load(Ordering::Acquire) == expected {
        if is_timeout {
            if limit == 0 {
                return true;
            }
            limit -= 1;
        }
        loom::thread::yield_now();
    }
    false
}

pub fn wake_by_address(_address: &LoomAtomicU32) {}

pub fn wake_all_by_address(_address: &LoomAtomicU32) {}
