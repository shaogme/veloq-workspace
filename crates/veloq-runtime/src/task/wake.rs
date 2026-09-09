use super::header::GenericTaskHeader;
use crate::runtime::primitives::{EventCount, Unparker};
use crossbeam_queue::SegQueue;
use std::{
    hint::spin_loop,
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
    sync::{Arc, OnceLock, Weak},
    thread::{self, ThreadId, yield_now},
};
use veloq_storage::{AtomicOptionPtr, LocalStorage, StateOptionPtr, Storage};

const WAKE_TOKEN_ALIVE: u32 = 1 << 0;
const WAKE_TOKEN_PENDING: u32 = 1 << 1;
const WAKE_TOKEN_ACTIVE_SHIFT: u32 = 2;
const WAKE_TOKEN_ACTIVE_UNIT: u32 = 1 << WAKE_TOKEN_ACTIVE_SHIFT;
const SPIN_LIMIT: u32 = 6;

/// Owner worker 的 local wake mailbox。
///
/// mailbox 只跨线程传递 token，不传递 `LocalTaskRef` 或 local header。队列中的 token
/// 可能在任务释放后继续存活，但 token 的 `ALIVE` 位会使 owner dispatch 安全地丢弃它。
pub(crate) struct LocalWakeTarget {
    requests: SegQueue<Arc<TaskWakeToken<LocalStorage>>>,
    pub(crate) unparker: Unparker,
    pub(crate) event_count: Arc<EventCount>,
    pub(crate) worker_id: usize,
}

impl LocalWakeTarget {
    pub(crate) fn new(worker_id: usize, unparker: Unparker, event_count: Arc<EventCount>) -> Self {
        Self {
            requests: SegQueue::new(),
            unparker,
            event_count,
            worker_id,
        }
    }

    #[inline]
    pub(crate) fn push(&self, token: Arc<TaskWakeToken<LocalStorage>>) {
        self.requests.push(token);
        // 请求先入队，再发布事件和 unpark，避免 worker 观察到事件却看不到请求。
        self.event_count.notify();
        if self.unparker.unpark().is_err() {
            // Unparker 已将错误记录到共享 fatal 通道；raw Waker 没有返回错误的 ABI。
        }
    }

    #[inline]
    pub(crate) fn pop(&self) -> Option<Arc<TaskWakeToken<LocalStorage>>> {
        self.requests.pop()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    #[inline]
    pub(crate) fn worker_id(&self) -> usize {
        self.worker_id
    }
}

/// 任务 waker 的 token。
///
/// send task 使用 token 的 header 直接唤醒路径；local task 使用同一套生命周期保护，
/// 但 wake callback 只设置 `PENDING` 并投递 owner mailbox。local token 的显式 `Send` /
/// `Sync` 证明只覆盖原子状态、弱 mailbox 引用和 unpark，不覆盖 local header 的访问。
pub(crate) struct TaskWakeToken<S: Storage> {
    state: AtomicU32,
    header: AtomicOptionPtr<GenericTaskHeader<S>>,
    local_target: OnceLock<Weak<LocalWakeTarget>>,
    owner_thread: OnceLock<ThreadId>,
    marker: PhantomData<fn() -> S>,
}

unsafe impl Send for TaskWakeToken<LocalStorage> {}
unsafe impl Sync for TaskWakeToken<LocalStorage> {}

pub(crate) struct TaskWakeGuard<'a, S: Storage> {
    token: &'a TaskWakeToken<S>,
}

/// 只有 owner worker 才能取得的 local header 访问保护。
///
/// guard 存活期间 header 不会被 `GenericTaskHeader::drop` 释放；调用者也已经通过
/// `owner_thread` 检查，因此这里的引用不会把 local 状态暴露给 foreign thread。
pub(crate) struct LocalWakeHeaderGuard<'a> {
    _active: TaskWakeGuard<'a, LocalStorage>,
    header: &'a GenericTaskHeader<LocalStorage>,
}

impl Deref for LocalWakeHeaderGuard<'_> {
    type Target = GenericTaskHeader<LocalStorage>;

    fn deref(&self) -> &Self::Target {
        self.header
    }
}

impl<S: Storage> TaskWakeToken<S> {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU32::new(WAKE_TOKEN_ALIVE),
            header: AtomicOptionPtr::new(None),
            local_target: OnceLock::new(),
            owner_thread: OnceLock::new(),
            marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn bind_header(&self, header: NonNull<GenericTaskHeader<S>>) {
        let header_ptr = Some(header);
        let current = self.header.load(Ordering::Acquire);
        debug_assert!(current.is_none() || current == header_ptr);
        self.header.store(header_ptr, Ordering::Release);
    }

    #[inline]
    pub(crate) fn bind_local_target(&self, target: &Arc<LocalWakeTarget>) {
        let target_result = self.local_target.set(Arc::downgrade(target));
        let owner_result = self.owner_thread.set(thread::current().id());
        debug_assert!(target_result.is_ok(), "local wake target is bound twice");
        debug_assert!(owner_result.is_ok(), "local wake owner is bound twice");
    }

    #[inline]
    pub(crate) fn header(&self) -> Option<&GenericTaskHeader<S>> {
        if self.state.load(Ordering::Acquire) & WAKE_TOKEN_ALIVE == 0 {
            return None;
        }

        let header = self.header.load(Ordering::Acquire)?;
        Some(unsafe { header.as_ref() })
    }

    #[inline]
    fn try_acquire(&self) -> Option<TaskWakeGuard<'_, S>> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & WAKE_TOKEN_ALIVE == 0 {
                return None;
            }

            match self.state.compare_exchange_weak(
                state,
                state + WAKE_TOKEN_ACTIVE_UNIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(TaskWakeGuard { token: self }),
                Err(actual) => {
                    state = actual;
                    spin_loop();
                }
            }
        }
    }

    /// send task 的原子 header 直接唤醒路径。
    #[inline]
    pub(crate) fn wake_impl(&self) {
        let Some(_guard) = self.try_acquire() else {
            return;
        };

        let Some(header) = self.header() else {
            return;
        };

        header.wake_by_ref();
    }

    pub(crate) fn deactivate_and_wait(&self) {
        self.state.fetch_and(!WAKE_TOKEN_ALIVE, Ordering::AcqRel);
        let mut spin_count = 0;
        loop {
            let curr = self.state.load(Ordering::Acquire);
            if curr & !WAKE_TOKEN_PENDING == 0 {
                break;
            }

            if spin_count < SPIN_LIMIT {
                spin_loop();
                spin_count += 1;
            } else if spin_count == SPIN_LIMIT {
                yield_now();
                spin_count += 1;
            } else {
                unsafe { crate::runtime::primitives::sys::wait(&self.state, curr) };
                spin_count = 0;
            }
        }
        self.header.store(None, Ordering::Release);
    }
}

impl TaskWakeToken<LocalStorage> {
    /// local raw-waker 的 foreign-safe 路径：只触碰 token 状态和 owner mailbox。
    #[inline]
    pub(crate) fn request_local_wake(self: &Arc<Self>) {
        let Some(_guard) = self.try_acquire() else {
            return;
        };

        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & WAKE_TOKEN_ALIVE == 0 {
                return;
            }
            if state & WAKE_TOKEN_PENDING != 0 {
                return;
            }

            match self.state.compare_exchange_weak(
                state,
                state | WAKE_TOKEN_PENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }

        let Some(target) = self.local_target.get().and_then(Weak::upgrade) else {
            // runtime 已经释放；不能为了补救而读取 header。
            self.state.fetch_and(!WAKE_TOKEN_PENDING, Ordering::AcqRel);
            return;
        };

        target.push(Arc::clone(self));
    }

    /// 在 owner worker 上消费一个 mailbox entry。
    pub(crate) fn dispatch_on_owner(self: &Arc<Self>) {
        let Some(owner) = self.owner_thread.get().copied() else {
            return;
        };
        if thread::current().id() != owner {
            return;
        }

        let Some(_guard) = self.try_acquire() else {
            return;
        };
        let old_state = self.state.fetch_and(!WAKE_TOKEN_PENDING, Ordering::AcqRel);
        if old_state & WAKE_TOKEN_PENDING == 0 {
            // 只有一个 pending entry 合约；重复或 stale entry 不得重复派发。
            return;
        }

        let Some(header) = self.header.load(Ordering::Acquire) else {
            return;
        };
        // `ALIVE` 已由 try_acquire 证明，active guard 保证 header 在这次 owner dispatch
        // 完成前不会被 Drop 清理。
        unsafe { header.as_ref().wake_by_ref() };
    }

    /// 仅供 owner-side `RuntimeContextExt` 反查；foreign thread 一律返回 `None`。
    pub(crate) fn local_header_on_owner(&self) -> Option<LocalWakeHeaderGuard<'_>> {
        let owner = self.owner_thread.get().copied()?;
        if thread::current().id() != owner {
            return None;
        }

        let active = self.try_acquire()?;
        let header = self.header.load(Ordering::Acquire)?;
        Some(LocalWakeHeaderGuard {
            _active: active,
            header: unsafe { header.as_ref() },
        })
    }
}

impl<S: Storage> Drop for TaskWakeGuard<'_, S> {
    fn drop(&mut self) {
        let prev = self
            .token
            .state
            .fetch_sub(WAKE_TOKEN_ACTIVE_UNIT, Ordering::AcqRel);
        if prev == WAKE_TOKEN_ACTIVE_UNIT {
            unsafe { crate::runtime::primitives::sys::wake_all(&self.token.state) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, mpsc::sync_channel};

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn local_wake_token_is_send_and_sync() {
        assert_send_sync::<TaskWakeToken<LocalStorage>>();
    }

    #[test]
    fn local_wake_requests_are_coalesced() {
        let event_count = Arc::new(EventCount::new());
        let target = Arc::new(LocalWakeTarget::new(
            0,
            Unparker::new(),
            event_count.clone(),
        ));
        let token = Arc::new(TaskWakeToken::<LocalStorage>::new());
        token.bind_local_target(&target);

        for _ in 0..32 {
            token.request_local_wake();
        }

        assert!(target.pop().is_some());
        assert!(target.pop().is_none());
        assert_eq!(event_count.load(), 1);

        token.deactivate_and_wait();
    }

    #[test]
    fn stale_local_wake_request_is_dropped_after_close() {
        let target = Arc::new(LocalWakeTarget::new(
            0,
            Unparker::new(),
            Arc::new(EventCount::new()),
        ));
        let token = Arc::new(TaskWakeToken::<LocalStorage>::new());
        token.bind_local_target(&target);
        token.request_local_wake();

        let queued = target.pop().expect("wake request");
        token.deactivate_and_wait();
        queued.dispatch_on_owner();
    }

    #[test]
    fn deactivation_waits_for_active_wake_callback() {
        let token = Arc::new(TaskWakeToken::<LocalStorage>::new());
        let active = token.try_acquire().expect("token must start alive");
        let barrier = Arc::new(Barrier::new(2));
        let (done_tx, done_rx) = sync_channel(0);
        let token_for_thread = Arc::clone(&token);
        let barrier_for_thread = Arc::clone(&barrier);
        let thread = thread::spawn(move || {
            barrier_for_thread.wait();
            token_for_thread.deactivate_and_wait();
            done_tx.send(()).unwrap();
        });

        barrier.wait();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err(),
            "deactivation must wait while a wake callback is active"
        );
        drop(active);
        done_rx
            .recv_timeout(std::time::Duration::from_millis(200))
            .expect("deactivation must finish after callback quiescence");
        thread.join().unwrap();
    }
}
