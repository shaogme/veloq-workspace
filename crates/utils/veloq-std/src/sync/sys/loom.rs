use crate::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

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
    pub fn wait(&self, address: &AtomicU32, expected: u32) {
        let guard = self.mutex.lock().unwrap();
        if address.load(Ordering::Acquire) == expected {
            let _ = self.cvar.wait(guard).unwrap();
        }
    }

    /// 模拟操作系统 Futex 超时挂起。返回是否未超时。
    pub fn wait_timeout(&self, address: &AtomicU32, expected: u32, dur: Duration) -> bool {
        let guard = self.mutex.lock().unwrap();
        if address.load(Ordering::Acquire) == expected {
            let (_guard, res) = self.cvar.wait_timeout(guard, dur).unwrap();
            !res.timed_out()
        } else {
            true
        }
    }

    /// 等待满足特定条件。若 condition 返回 true，则在 Condvar 上挂起当前线程。
    #[allow(dead_code)]
    pub fn wait_while<F: Fn() -> bool>(&self, condition: F) {
        let mut guard = self.mutex.lock().unwrap();
        while condition() {
            guard = self.cvar.wait(guard).unwrap();
        }
    }

    /// 尝试等待，带有超时判定。
    #[allow(dead_code)]
    pub fn wait_timeout_while<F: Fn() -> bool>(&self, condition: F, dur: Duration) -> bool {
        let mut guard = self.mutex.lock().unwrap();
        while condition() {
            let (next_guard, res) = self.cvar.wait_timeout(guard, dur).unwrap();
            guard = next_guard;
            if res.timed_out() {
                return false;
            }
        }
        true
    }

    /// 精准唤醒一个挂起的等待者。
    pub fn wake_one(&self) {
        let _g = self.mutex.lock().unwrap();
        self.cvar.notify_one();
    }

    /// 在节点通道锁内发布最终通知，避免状态检查和通道唤醒之间出现窗口。
    pub fn finish_notify(&self, address: &AtomicU32, notified: u32) -> u32 {
        let _g = self.mutex.lock().unwrap();
        let previous = address.swap(notified, Ordering::AcqRel);
        self.cvar.notify_one();
        previous
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

pub fn wait_on_address(address: &AtomicU32, expected: u32) {
    wait_on_address_timeout(address, expected, None);
}

pub fn wait_on_address_timeout(
    address: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    use core::sync::atomic::Ordering;
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

pub fn wake_by_address(_address: &AtomicU32) {}

pub fn wake_all_by_address(_address: &AtomicU32) {}
