use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::{RawWaker, RawWakerVTable, Waker};
use std::time::Duration;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

use crate::task::{AnyScopeRef, OpaqueToken};
use crate::utils::ownership::Ownership;
use crate::utils::storage::{StateInt, StateLock, StateWakerQueue, Storage};

// --- 系统级同步原语 (WaitOnAddress / Futex) ---

mod sys {
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

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
    pub unsafe fn wait_timeout(addr: &AtomicU32, expected: u32, timeout: Duration) -> bool {
        let expected_val = expected;
        let millis = if timeout.is_zero() {
            0
        } else {
            let nanos = timeout.as_nanos();
            nanos
                .saturating_add(999_999)
                .checked_div(1_000_000)
                .unwrap_or(u128::MAX)
                .min(u32::MAX as u128) as u32
        };
        unsafe {
            win::WaitOnAddress(
                addr as *const _ as *const _,
                &expected_val as *const _ as *const _,
                4,
                millis,
            ) != 0
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
    pub unsafe fn wait_timeout(addr: &AtomicU32, expected: u32, timeout: Duration) -> bool {
        let ts = libc::timespec {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_nsec: timeout.subsec_nanos() as libc::c_long,
        };
        let ret = unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr as *const _ as *mut i32,
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                expected as i32,
                &ts as *const libc::timespec,
            )
        };
        ret == 0
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
    pub unsafe fn wait_timeout(_addr: &AtomicU32, _expected: u32, timeout: Duration) -> bool {
        std::thread::sleep(timeout);
        false
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

    pub fn wait_timeout(&self, duration: Duration) -> bool {
        if self
            .state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }

        unsafe { sys::wait_timeout(&self.state, 0, duration) };

        self.state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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

pub fn create_unpark_waker(unparker: Unparker) -> Waker {
    let raw = Arc::into_raw(unparker.inner) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &UNPARK_VTABLE)) }
}

static UNPARK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const ParkerInner);
        RawWaker::new(p, &UNPARK_VTABLE)
    },
    |p| unsafe {
        let inner = Arc::from_raw(p as *const ParkerInner);
        Unparker::from_inner(inner).unpark();
    },
    |p| unsafe {
        let inner = ManuallyDrop::new(Arc::from_raw(p as *const ParkerInner));
        Unparker::from_inner((*inner).clone()).unpark();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const ParkerInner));
    },
);

// --- 高性能唤醒原语 (Parker/Unparker) ---

pub struct Parker {
    inner: Arc<ParkerInner>,
}

#[derive(Clone)]
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

    pub fn park_timeout(&self, duration: Duration) -> bool {
        loop {
            if self
                .inner
                .state
                .compare_exchange(NOTIFIED, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }

            if self
                .inner
                .state
                .compare_exchange(EMPTY, PARKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _ = unsafe { sys::wait_timeout(&self.inner.state, PARKED, duration) };

                if self
                    .inner
                    .state
                    .compare_exchange(NOTIFIED, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }

                if self
                    .inner
                    .state
                    .compare_exchange(PARKED, EMPTY, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return false;
                }
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
    pub(crate) inner: O::Shared<GenericCancellationTokenInner<S, O>>,
}

pub type ChildList<S, O> = <S as Storage>::Lock<LinkedList<CancellationTokenAdapter<S, O>>>;
pub type ParentSlot<S, O> =
    <S as Storage>::Lock<Option<<O as Ownership>::Weak<GenericCancellationTokenInner<S, O>>>>;

pub struct GenericCancellationTokenInner<S: Storage, O: Ownership> {
    cancelled: S::Usize,
    wakers: S::WakerQueue,
    children: ChildList<S, O>,
    link: Link,
    parent: ParentSlot<S, O>,
    cross_parent: Option<AnyScopeRef>,
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
        Self::new_with_parent(None)
    }

    pub fn new_with_parent(cross_parent: Option<AnyScopeRef>) -> Self {
        Self {
            inner: O::new(GenericCancellationTokenInner {
                cancelled: S::Usize::new(0),
                wakers: S::WakerQueue::new(),
                children: S::Lock::new(LinkedList::new(CancellationTokenAdapter::<S, O>::new())),
                link: Link::new(),
                parent: S::Lock::new(None),
                cross_parent,
            }),
        }
    }

    pub fn link_child(&self, child: &Self) {
        if self.is_cancelled() {
            child.cancel();
            return;
        }

        {
            let mut parent_slot = child.inner.parent.lock();
            *parent_slot = Some(O::downgrade(&self.inner));
        }

        let mut children = self.inner.children.lock();
        if self.is_cancelled() {
            drop(children);
            child.cancel();
            return;
        }

        unsafe {
            let child_ptr = NonNull::new_unchecked(
                O::as_ptr(&child.inner) as *mut GenericCancellationTokenInner<S, O>
            );
            children.push_back(Pin::new_unchecked(&mut *child_ptr.as_ptr()));
        }
    }

    pub(crate) unsafe fn try_link_child_raw(&self, child_token_ptr: *const OpaqueToken) -> bool {
        let child = unsafe { &*(child_token_ptr as *const Self) };
        self.link_child(child);
        true
    }

    pub fn child(&self) -> Self {
        let child = Self::new();
        self.link_child(&child);
        child
    }

    pub fn cancel(&self) {
        self.inner.cancel_internal();
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        if self.inner.cancelled.load(Ordering::SeqCst) != 0 {
            return true;
        }
        if let Some(ref parent) = self.inner.cross_parent
            && parent.is_cancelled()
        {
            return true;
        }
        false
    }

    pub fn register_waker(&self, waker: &Waker) {
        if self.is_cancelled() {
            waker.wake_by_ref();
            return;
        }
        self.inner.wakers.register(waker);
        if let Some(ref parent) = self.inner.cross_parent {
            parent.register_cancel_waker(waker);
        }
        if self.is_cancelled() {
            let wakers = self.inner.wakers.take_all();
            for w in wakers {
                w.wake();
            }
        }
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
                && let Some(parent_inner) = O::upgrade(parent_weak)
            {
                let mut children = parent_inner.children.lock();
                if self.inner.link.is_linked() {
                    unsafe {
                        let node_ptr = NonNull::new_unchecked(
                            O::as_ptr(&self.inner) as *mut GenericCancellationTokenInner<S, O>
                        );
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

        self.token.register_waker(cx.waker());

        if self.token.is_cancelled() {
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
