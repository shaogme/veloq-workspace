use core::{fmt, ptr::null_mut, sync::atomic::Ordering, time::Duration};

use crate::{
    sync::{
        Mutex, MutexGuard, RawMutex,
        atomic::{AtomicPtr, AtomicU32},
    },
    time::Instant,
};

#[cfg(not(feature = "loom"))]
use crate::sync::sys;

#[cfg(feature = "loom")]
use crate::sync::sys::loom::WaitChannel;

const WAITING: u32 = 0;
const NOTIFYING: u32 = 1;
const NOTIFIED: u32 = 2;
const TIMED_OUT: u32 = 3;

/// 状态等待结果，用于表示等待是否超时。
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    /// 如果等待超时返回 `true`，否则返回 `false`。
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

/// 条件变量的栈上等待节点。
///
/// 节点的地址只在所属 `wait` 调用的整个生命周期内进入队列。队列锁保护所有
/// 链接字段；`NOTIFYING` 状态则把从队列摘下到通知完成之间的生命周期交接给
/// 通知者，等待者在此状态下不能返回。
struct Waiter {
    state: AtomicU32,
    prev: AtomicPtr<Waiter>,
    next: AtomicPtr<Waiter>,
    #[cfg(feature = "loom")]
    channel: WaitChannel,
}

impl Waiter {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(WAITING),
            prev: AtomicPtr::new(null_mut()),
            next: AtomicPtr::new(null_mut()),
            #[cfg(feature = "loom")]
            channel: WaitChannel::new(),
        }
    }

    fn wait(&self, queue: &WaitQueue) {
        loop {
            match self.state.load(Ordering::Acquire) {
                WAITING => self.wait_once(WAITING),
                NOTIFYING => self.wait_once(NOTIFYING),
                NOTIFIED => {
                    // 通知者在同一个队列锁临界区内完成最后一次节点访问。
                    let _guard = queue.lock();
                    return;
                }
                TIMED_OUT => panic!("an untimed condvar waiter timed out"),
                _ => unreachable!("invalid condvar waiter state"),
            }
        }
    }

    fn wait_once(&self, expected: u32) {
        #[cfg(not(feature = "loom"))]
        sys::wait_on_address(&self.state, expected);

        #[cfg(feature = "loom")]
        self.channel.wait(&self.state, expected);
    }

    fn wait_timeout(&self, queue: &WaitQueue, dur: Duration) -> bool {
        let start = Instant::now();

        loop {
            match self.state.load(Ordering::Acquire) {
                WAITING => {}
                NOTIFYING => {
                    self.wait(queue);
                    return false;
                }
                NOTIFIED => {
                    let _guard = queue.lock();
                    return false;
                }
                TIMED_OUT => unreachable!("timed-out condvar waiter was reused"),
                _ => unreachable!("invalid condvar waiter state"),
            }

            let elapsed = start.elapsed();
            if elapsed >= dur {
                return self.cancel(queue);
            }
            let remaining = dur - elapsed;

            #[cfg(not(feature = "loom"))]
            let timed_out = sys::wait_on_address_timeout(&self.state, WAITING, Some(remaining));

            #[cfg(feature = "loom")]
            let timed_out = !self.channel.wait_timeout(&self.state, WAITING, remaining);

            if timed_out && start.elapsed() >= dur {
                return self.cancel(queue);
            }
        }
    }

    fn cancel(&self, queue: &WaitQueue) -> bool {
        let guard = queue.lock();
        match self
            .state
            .compare_exchange(WAITING, TIMED_OUT, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // SAFETY: the node was registered before this call and both its links
                // and the queue endpoints are protected by `guard`.
                unsafe { guard.unlink(self as *const Self as *mut Self) };
                true
            }
            Err(NOTIFYING) => {
                drop(guard);
                self.wait(queue);
                false
            }
            Err(NOTIFIED) => false,
            Err(TIMED_OUT) => unreachable!("condvar waiter was cancelled twice"),
            Err(_) => unreachable!("invalid condvar waiter state"),
        }
    }

    fn finish_notify(&self) {
        #[cfg(not(feature = "loom"))]
        {
            let previous = self.state.swap(NOTIFIED, Ordering::AcqRel);
            debug_assert_eq!(previous, NOTIFYING);
            sys::wake_by_address(&self.state);
        }

        #[cfg(feature = "loom")]
        {
            let previous = self.channel.finish_notify(&self.state, NOTIFIED);
            debug_assert_eq!(previous, NOTIFYING);
        }
    }
}

/// 由内部锁保护的 intrusive FIFO 等待队列。
///
/// `head`、`tail` 以及节点的 `prev`/`next` 永远不能脱离 `WaitQueueGuard` 被读写。
/// 队列保存的节点都是调用方栈上的对象；节点必须保持存活到它被超时摘除，或由
/// 通知者完成 `NOTIFYING -> NOTIFIED` 并释放队列锁之后。
struct WaitQueue {
    lock: RawMutex,
    head: AtomicPtr<Waiter>,
    tail: AtomicPtr<Waiter>,
}

#[cfg(not(feature = "loom"))]
impl WaitQueue {
    const fn new() -> Self {
        Self {
            lock: RawMutex::new(),
            head: AtomicPtr::new(null_mut()),
            tail: AtomicPtr::new(null_mut()),
        }
    }
}

#[cfg(feature = "loom")]
impl WaitQueue {
    fn new() -> Self {
        Self {
            lock: RawMutex::new(),
            head: AtomicPtr::new(null_mut()),
            tail: AtomicPtr::new(null_mut()),
        }
    }
}

impl WaitQueue {
    fn lock(&self) -> WaitQueueGuard<'_> {
        self.lock.lock();
        WaitQueueGuard { queue: self }
    }
}

struct WaitQueueGuard<'a> {
    queue: &'a WaitQueue,
}

impl WaitQueueGuard<'_> {
    /// 将节点加入队尾。
    ///
    /// # Safety
    ///
    /// 调用者必须持有该队列锁；`waiter` 必须是尚未入队的有效栈上节点，并且其
    /// 生命周期覆盖从注册到等待返回的整个过程。
    unsafe fn push_back(&self, waiter: *mut Waiter) {
        let tail = self.queue.tail.load(Ordering::Relaxed);
        unsafe {
            (*waiter).prev.store(tail, Ordering::Relaxed);
            (*waiter).next.store(null_mut(), Ordering::Relaxed);
        }
        if tail.is_null() {
            self.queue.head.store(waiter, Ordering::Relaxed);
        } else {
            unsafe { (*tail).next.store(waiter, Ordering::Relaxed) };
        }
        self.queue.tail.store(waiter, Ordering::Relaxed);
    }

    /// 从队首摘取一个节点。
    ///
    /// # Safety
    ///
    /// 调用者必须持有该队列锁。返回的节点仍由其所属等待线程保持存活，直到
    /// 通知者完成最终通知，或调用方依据状态协议放弃该指针。
    unsafe fn pop_front(&self) -> Option<*mut Waiter> {
        let head = self.queue.head.load(Ordering::Relaxed);
        if head.is_null() {
            return None;
        }

        let next = unsafe { (*head).next.load(Ordering::Relaxed) };
        self.queue.head.store(next, Ordering::Relaxed);
        if next.is_null() {
            self.queue.tail.store(null_mut(), Ordering::Relaxed);
        } else {
            unsafe { (*next).prev.store(null_mut(), Ordering::Relaxed) };
        }
        unsafe {
            (*head).prev.store(null_mut(), Ordering::Relaxed);
            (*head).next.store(null_mut(), Ordering::Relaxed);
        }
        Some(head)
    }

    /// 从队列中摘除一个仍在等待的节点。
    ///
    /// # Safety
    ///
    /// 调用者必须持有该队列锁；`waiter` 必须是当前队列中的节点。
    unsafe fn unlink(&self, waiter: *mut Waiter) {
        let prev = unsafe { (*waiter).prev.load(Ordering::Relaxed) };
        let next = unsafe { (*waiter).next.load(Ordering::Relaxed) };

        if prev.is_null() {
            self.queue.head.store(next, Ordering::Relaxed);
        } else {
            unsafe { (*prev).next.store(next, Ordering::Relaxed) };
        }
        if next.is_null() {
            self.queue.tail.store(prev, Ordering::Relaxed);
        } else {
            unsafe { (*next).prev.store(prev, Ordering::Relaxed) };
        }
        unsafe {
            (*waiter).prev.store(null_mut(), Ordering::Relaxed);
            (*waiter).next.store(null_mut(), Ordering::Relaxed);
        }
    }

    /// 摘下整条队列，并把其中每个等待者转为 `NOTIFYING`。
    ///
    /// # Safety
    ///
    /// 调用者必须持有该队列锁。返回链上的节点在最终通知完成前都由调用者持有，
    /// 其后继指针必须在当前节点的最终通知临界区内保存。
    unsafe fn detach_all_and_mark_notifying(&self) -> *mut Waiter {
        let head = self.queue.head.load(Ordering::Relaxed);
        self.queue.head.store(null_mut(), Ordering::Relaxed);
        self.queue.tail.store(null_mut(), Ordering::Relaxed);

        let mut current = head;
        while !current.is_null() {
            let previous = unsafe {
                (*current).state.compare_exchange(
                    WAITING,
                    NOTIFYING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
            };
            debug_assert!(previous.is_ok(), "invalid state in condvar queue");
            current = unsafe { (*current).next.load(Ordering::Relaxed) };
        }
        head
    }

    /// 在队列锁内完成一个节点的最终通知，并返回其后继。
    ///
    /// # Safety
    ///
    /// 调用者必须持有该队列锁；`waiter` 必须来自此前由
    /// `detach_all_and_mark_notifying` 返回的链表。
    unsafe fn finish_detached(&self, waiter: *mut Waiter) -> *mut Waiter {
        let next = unsafe { (*waiter).next.load(Ordering::Relaxed) };
        unsafe { (*waiter).finish_notify() };
        unsafe {
            (*waiter).prev.store(null_mut(), Ordering::Relaxed);
            (*waiter).next.store(null_mut(), Ordering::Relaxed);
        }
        next
    }
}

impl Drop for WaitQueueGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: this guard is created only after locking `queue.lock` and is dropped
        // exactly once while owning that lock.
        unsafe { self.queue.lock.unlock() };
    }
}

/// 条件变量。
pub struct Condvar {
    waiters: WaitQueue,
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
            waiters: WaitQueue::new(),
        }
    }

    /// 创建一个新的条件变量。
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            waiters: WaitQueue::new(),
        }
    }

    fn register<'a, T>(&self, waiter: &mut Waiter, guard: MutexGuard<'a, T>) -> &'a Mutex<T> {
        let mutex = MutexGuard::mutex(&guard);
        {
            let queue = self.waiters.lock();
            // SAFETY: `waiter` remains on this stack frame until all notification or
            // cancellation ownership has been completed.
            unsafe { queue.push_back(waiter) };
            // Keep the queue lock while releasing the external mutex. This closes the
            // registration window for callers that update their predicate under it.
            drop(guard);
        }
        mutex
    }

    /// 阻塞当前线程，直到此条件变量收到通知。
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        let mut waiter = Waiter::new();
        let mutex = self.register(&mut waiter, guard);
        waiter.wait(&self.waiters);
        mutex.lock()
    }

    /// 阻塞当前线程，直到此条件变量收到通知，或达到指定的超时时间。
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        dur: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        let mut waiter = Waiter::new();
        let mutex = self.register(&mut waiter, guard);
        let timed_out = waiter.wait_timeout(&self.waiters, dur);
        (mutex.lock(), WaitTimeoutResult(timed_out))
    }

    /// 唤醒在此条件变量上等待的其中一个线程。
    pub fn notify_one(&self) {
        let selected = {
            let queue = self.waiters.lock();
            loop {
                // SAFETY: the queue lock protects the intrusive links and keeps the
                // waiting thread from unlinking or destroying this node.
                let Some(waiter) = (unsafe { queue.pop_front() }) else {
                    break None;
                };
                let result = unsafe {
                    (*waiter).state.compare_exchange(
                        WAITING,
                        NOTIFYING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                };
                if result.is_ok() {
                    break Some(waiter);
                }
                debug_assert_eq!(result, Err(TIMED_OUT));
            }
        };

        if let Some(waiter) = selected {
            // Do not hold the first queue lock across the handoff. Reacquiring it here
            // keeps loom channel locking in the fixed queue-lock -> channel-lock order.
            let queue = self.waiters.lock();
            unsafe { (*waiter).finish_notify() };
            drop(queue);
        }
    }

    /// 唤醒在此条件变量上等待的所有线程。
    pub fn notify_all(&self) {
        let mut current = {
            let queue = self.waiters.lock();
            // SAFETY: the queue lock protects the entire detached chain and each node
            // stays alive until its notification handoff completes.
            unsafe { queue.detach_all_and_mark_notifying() }
        };

        while !current.is_null() {
            let queue = self.waiters.lock();
            // SAFETY: `current` belongs to the detached chain and the queue lock is held;
            // `finish_detached` saves its successor before the node's final access.
            current = unsafe { queue.finish_detached(current) };
            drop(queue);
        }
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn queue_notify_one_selects_one_waiter() {
        let queue = WaitQueue::new();
        let mut first = Waiter::new();
        let mut second = Waiter::new();

        {
            let guard = queue.lock();
            unsafe {
                guard.push_back(&mut first);
                guard.push_back(&mut second);
            }
        }

        let selected = {
            let guard = queue.lock();
            let selected = unsafe { guard.pop_front() }.unwrap();
            assert!(unsafe {
                (*selected)
                    .state
                    .compare_exchange(WAITING, NOTIFYING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            });
            selected
        };

        assert_eq!(first.state.load(Ordering::Acquire), NOTIFYING);
        assert_eq!(second.state.load(Ordering::Acquire), WAITING);
        {
            let guard = queue.lock();
            unsafe { (*selected).finish_notify() };
            assert_eq!(
                guard.queue.head.load(Ordering::Relaxed),
                &mut second as *mut Waiter,
            );
        }
    }

    #[test]
    fn queue_unlink_handles_all_positions() {
        let queue = WaitQueue::new();
        let mut first = Waiter::new();
        let mut second = Waiter::new();
        let mut third = Waiter::new();

        {
            let guard = queue.lock();
            unsafe {
                guard.push_back(&mut first);
                guard.push_back(&mut second);
                guard.push_back(&mut third);
                guard.unlink(&mut second);
            }
            assert_eq!(
                guard.queue.head.load(Ordering::Relaxed),
                &mut first as *mut Waiter,
            );
            assert_eq!(
                guard.queue.tail.load(Ordering::Relaxed),
                &mut third as *mut Waiter,
            );
        }

        {
            let guard = queue.lock();
            unsafe {
                guard.unlink(&mut first);
                guard.unlink(&mut third);
            }
            assert!(guard.queue.head.load(Ordering::Relaxed).is_null());
            assert!(guard.queue.tail.load(Ordering::Relaxed).is_null());
        }
    }

    #[test]
    fn queue_cancel_removes_the_waiter() {
        let queue = WaitQueue::new();
        let mut waiter = Waiter::new();

        {
            let guard = queue.lock();
            unsafe { guard.push_back(&mut waiter) };
        }

        assert!(waiter.cancel(&queue));
        assert_eq!(waiter.state.load(Ordering::Acquire), TIMED_OUT);
        let guard = queue.lock();
        assert!(guard.queue.head.load(Ordering::Relaxed).is_null());
        assert!(guard.queue.tail.load(Ordering::Relaxed).is_null());
    }

    #[test]
    fn queue_detach_marks_every_waiter() {
        let queue = WaitQueue::new();
        let mut first = Waiter::new();
        let mut second = Waiter::new();
        let mut third = Waiter::new();

        {
            let guard = queue.lock();
            unsafe {
                guard.push_back(&mut first);
                guard.push_back(&mut second);
                guard.push_back(&mut third);
            }
        }

        let mut current = {
            let guard = queue.lock();
            unsafe { guard.detach_all_and_mark_notifying() }
        };
        while !current.is_null() {
            let guard = queue.lock();
            current = unsafe { guard.finish_detached(current) };
            drop(guard);
        }

        assert_eq!(first.state.load(Ordering::Acquire), NOTIFIED);
        assert_eq!(second.state.load(Ordering::Acquire), NOTIFIED);
        assert_eq!(third.state.load(Ordering::Acquire), NOTIFIED);
        let guard = queue.lock();
        assert!(guard.queue.head.load(Ordering::Relaxed).is_null());
        assert!(guard.queue.tail.load(Ordering::Relaxed).is_null());
    }
}
