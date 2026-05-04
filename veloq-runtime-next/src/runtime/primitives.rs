use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::{RawWaker, RawWakerVTable, Waker};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

use crate::utils::ownership::Ownership;
use crate::utils::storage::{StateInt, StateLock, StateWakerQueue, Storage};

// --- 系统级同步原语 (WaitOnAddress / Futex) ---

mod sys {
    use std::sync::atomic::AtomicU32;

    #[cfg(windows)]
    mod win {
        #[link(name = "Synchronization")]
        unsafe extern "system" {
            pub fn WaitOnAddress(
                address: *const std::ffi::c_void,
                compare_address: *const std::ffi::c_void,
                address_size: usize,
                milliseconds: u32,
            ) -> i32;
            pub fn WakeByAddressSingle(address: *const std::ffi::c_void);
            pub fn WakeByAddressAll(address: *const std::ffi::c_void);
        }
    }

    #[cfg(windows)]
    pub unsafe fn wait(addr: &AtomicU32, expected: u32) {
        let expected_val = expected;
        unsafe {
            win::WaitOnAddress(
                addr as *const _ as *const _,
                &expected_val as *const _ as *const _,
                4,
                0xFFFFFFFF, // INFINITE
            );
        }
    }

    #[cfg(windows)]
    pub unsafe fn wake_one(addr: &AtomicU32) {
        unsafe {
            win::WakeByAddressSingle(addr as *const _ as *const _);
        }
    }

    #[cfg(windows)]
    pub unsafe fn wake_all(addr: &AtomicU32) {
        unsafe {
            win::WakeByAddressAll(addr as *const _ as *const _);
        }
    }

    #[cfg(target_os = "linux")]
    pub unsafe fn wait(addr: &AtomicU32, expected: u32) {
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr as *const _ as *mut i32,
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                expected as i32,
                std::ptr::null::<libc::timespec>(),
            );
        }
    }

    #[cfg(target_os = "linux")]
    pub unsafe fn wake_one(addr: &AtomicU32) {
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr as *const _ as *mut i32,
                libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                1,
            );
        }
    }

    #[cfg(target_os = "linux")]
    pub unsafe fn wake_all(addr: &AtomicU32) {
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr as *const _ as *mut i32,
                libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                i32::MAX,
            );
        }
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    pub unsafe fn wait(_addr: &AtomicU32, _expected: u32) {
        std::thread::yield_now();
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    pub unsafe fn wake_one(_addr: &AtomicU32) {}
    #[cfg(not(any(windows, target_os = "linux")))]
    pub unsafe fn wake_all(_addr: &AtomicU32) {}
}

// --- 事件通知机制 ---

pub struct Signal {
    state: AtomicU32, // 0: initial, 1: notified
}

impl Signal {
    pub fn new(ready: bool) -> Self {
        Self {
            state: AtomicU32::new(if ready { 1 } else { 0 }),
        }
    }

    pub fn notify(&self) {
        if self.state.swap(1, Ordering::AcqRel) == 0 {
            unsafe { sys::wake_all(&self.state) };
        }
    }

    pub fn wait(&self) {
        loop {
            // Fast-path: try to consume the notification
            if self
                .state
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            // Slow-path: block until notified
            unsafe { sys::wait(&self.state, 0) };
        }
    }
}

pub fn create_waker(signal: Arc<Signal>) -> Waker {
    let raw = Arc::into_raw(signal) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &VTABLE)) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const Signal);
        RawWaker::new(p, &VTABLE)
    },
    |p| unsafe {
        Arc::from_raw(p as *const Signal).notify();
    },
    |p| unsafe {
        ManuallyDrop::new(Arc::from_raw(p as *const Signal)).notify();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const Signal));
    },
);

// --- 高性能唤醒原语 (Parker/Unparker) ---

pub struct Parker {
    inner: Arc<ParkerInner>,
}

pub struct Unparker {
    inner: Arc<ParkerInner>,
}

pub(crate) struct ParkerInner {
    pub(crate) state: AtomicU32,
}

const EMPTY: u32 = 0;
const NOTIFIED: u32 = 1;
const PARKED: u32 = 2;

impl Default for Parker {
    fn default() -> Self {
        Self::new()
    }
}

impl Parker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ParkerInner {
                state: AtomicU32::new(EMPTY),
            }),
        }
    }

    pub fn unparker(&self) -> Unparker {
        Unparker {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn from_inner(inner: Arc<ParkerInner>) -> Self {
        Self { inner }
    }

    pub fn park(&self) {
        loop {
            // 1. Try to consume notification
            if self
                .inner
                .state
                .compare_exchange(NOTIFIED, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }

            // 2. Try to mark as parked
            if self
                .inner
                .state
                .compare_exchange(EMPTY, PARKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Wait until state changes from PARKED
                unsafe { sys::wait(&self.inner.state, PARKED) };
            } else {
                // State must be NOTIFIED or PARKED (by another thread - though Parker is thread-exclusive)
                // Continue to retry.
            }
        }
    }
}

impl Unparker {
    pub fn unpark(&self) {
        // Set state to NOTIFIED
        let prev = self.inner.state.swap(NOTIFIED, Ordering::AcqRel);
        if prev == PARKED {
            // Wake the thread if it was parked
            unsafe { sys::wake_one(&self.inner.state) };
        }
    }

    pub(crate) fn from_inner(inner: Arc<ParkerInner>) -> Self {
        Self { inner }
    }
}

// --- 显式结构化取消系统 (CancellationToken) ---

pub struct GenericCancellationToken<S: Storage, O: Ownership> {
    inner: O::Shared<GenericCancellationTokenInner<S, O>>,
}

pub type ChildList<S, O> = <S as Storage>::Lock<LinkedList<CancellationTokenAdapter<S, O>>>;
pub type ParentSlot<S, O> = <S as Storage>::Lock<Option<<O as Ownership>::Weak<GenericCancellationTokenInner<S, O>>>>;

pub struct GenericCancellationTokenInner<S: Storage, O: Ownership> {
    cancelled: S::Usize,
    wakers: S::WakerQueue,
    children: ChildList<S, O>,
    pub(crate) link: Link,
    parent: ParentSlot<S, O>,
}

intrusive_adapter!(pub CancellationTokenAdapter<S, O> = GenericCancellationTokenInner<S, O> { link: Link } where S: Storage, O: Ownership);

impl<S: Storage, O: Ownership> GenericCancellationTokenInner<S, O> {
    fn cancel_internal(&self) {
        if self
            .cancelled
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let wakers = self.wakers.take_all();
        for waker in wakers {
            waker.wake();
        }

        let mut children = self.children.lock();
        while let Some(child_inner) = children.pop_front() {
            child_inner.cancel_internal();
        }
    }
}

impl<S: Storage, O: Ownership> Default for GenericCancellationToken<S, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage, O: Ownership> GenericCancellationToken<S, O> {
    pub fn new() -> Self {
        Self {
            inner: O::new(GenericCancellationTokenInner {
                cancelled: S::Usize::new(0),
                wakers: S::WakerQueue::new(),
                children: S::Lock::new(LinkedList::new(CancellationTokenAdapter::<S, O>::new())),
                link: Link::new(),
                parent: S::Lock::new(None),
            }),
        }
    }

    pub fn child(&self) -> Self {
        let child = Self::new();
        if self.is_cancelled() {
            child.cancel();
            return child;
        }

        {
            let mut parent_slot = child.inner.parent.lock();
            *parent_slot = Some(O::downgrade(&self.inner));
        }

        let mut children = self.inner.children.lock();
        if self.is_cancelled() {
            drop(children);
            child.cancel();
            return child;
        }

        unsafe {
            let child_ptr = NonNull::new_unchecked(
                O::as_ptr(&child.inner) as *mut GenericCancellationTokenInner<S, O>
            );
            children.push_back(Pin::new_unchecked(&mut *child_ptr.as_ptr()));
        }
        child
    }

    pub fn cancel(&self) {
        self.inner.cancel_internal();
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst) != 0
    }

    pub fn cancelled(&self) -> CancelledFuture<S, O> {
        CancelledFuture {
            token: self.clone(),
        }
    }

    pub fn from_inner(inner: O::Shared<GenericCancellationTokenInner<S, O>>) -> Self {
        Self { inner }
    }
}

impl<S: Storage, O: Ownership> Drop for GenericCancellationToken<S, O> {
    fn drop(&mut self) {
        if O::strong_count(&O::downgrade(&self.inner)) == 1 {
            let parent_guard = self.inner.parent.lock();
            if let Some(parent_weak) = parent_guard.as_ref()
                && let Some(parent_inner) = O::upgrade(parent_weak) {
                    let mut children = parent_inner.children.lock();
                    if self.inner.link.is_linked() {
                        unsafe {
                            let node_ptr =
                                NonNull::new_unchecked(O::as_ptr(&self.inner)
                                    as *mut GenericCancellationTokenInner<S, O>);
                            let mut cursor = children.cursor_mut_from_ptr(node_ptr);
                            cursor.remove();
                        }
                    }
                }
        }
    }
}

impl<S: Storage, O: Ownership> Clone for GenericCancellationToken<S, O> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct CancelledFuture<S: Storage, O: Ownership> {
    token: GenericCancellationToken<S, O>,
}

impl<S: Storage, O: Ownership> std::future::Future for CancelledFuture<S, O> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.token.is_cancelled() {
            return std::task::Poll::Ready(());
        }

        {
            self.token.inner.wakers.push(cx.waker().clone());
        }

        if self.token.is_cancelled() {
            let wakers = self.token.inner.wakers.take_all();
            for waker in wakers {
                waker.wake();
            }
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}

// --- 调度器精确唤醒原语 (EventCount) ---

/// EventCount 用于解决调度器中“检查任务”与“进入睡眠”之间的竞态条件。
/// 它通过一个单调递增的序列号来跟踪系统中“工作可用性”的变化。
pub struct EventCount {
    state: AtomicUsize,
}

impl Default for EventCount {
    fn default() -> Self {
        Self::new()
    }
}

impl EventCount {
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
        }
    }

    /// 获取当前的事件序列号。
    /// 在准备进入睡眠前调用此方法获取快照。
    pub fn load(&self) -> usize {
        self.state.load(Ordering::Acquire)
    }

    /// 产生一个新事件（例如有新任务入队）。
    /// 这将递增序列号，从而使所有持有旧快照的 Worker 意识到状态已变。
    pub fn notify(&self) {
        self.state.fetch_add(1, Ordering::Release);
    }
}
