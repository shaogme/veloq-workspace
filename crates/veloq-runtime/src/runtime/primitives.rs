use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    ptr::NonNull,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_storage::{StateInt, StateLock, StateWakerQueue, Storage};

use crate::{
    error::RuntimeWakeError,
    task::{AnySendScopeRef, OpaqueToken, ScopeCancelWaiter, ScopeCancelWaiterAdapter},
    utils::ownership::Ownership,
};

// --- 系统级同步原语 (WaitOnAddress / Futex) ---

pub(crate) mod sys {
    use std::{sync::atomic::AtomicU32, time::Duration};

    #[cfg(not(any(windows, target_os = "linux")))]
    use std::thread;

    #[cfg(windows)]
    mod win {
        use std::ffi::c_void;

        #[link(name = "synchronization")]
        unsafe extern "system" {
            pub fn WaitOnAddress(
                address: *const c_void,
                compare_address: *const c_void,
                address_size: usize,
                milliseconds: u32,
            ) -> i32;
            pub fn WakeByAddressAll(address: *const c_void);
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
    pub unsafe fn wake_all(addr: &AtomicU32) {
        unsafe {
            win::WakeByAddressAll(addr as *const _ as *const _);
        }
    }

    #[cfg(target_os = "linux")]
    pub unsafe fn wait(addr: &AtomicU32, expected: u32) {
        use std::ptr::null;
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr as *const _ as *mut i32,
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                expected as i32,
                null::<libc::timespec>(),
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
        thread::yield_now();
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    pub unsafe fn wait_timeout(_addr: &AtomicU32, _expected: u32, timeout: Duration) -> bool {
        thread::sleep(timeout);
        false
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    pub unsafe fn wake_all(_addr: &AtomicU32) {}
}

// --- 事件通知机制 ---

pub struct Signal {
    state: AtomicU32, // 0: initial, 1: notified
}

impl Signal {
    pub fn is_notified(&self) -> bool {
        self.state.load(Ordering::Acquire) == 1
    }

    pub fn try_reset(&self) -> bool {
        self.state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum ShutdownPhase {
    Running = 0,
    StopRequested = 1,
    AllWorkersQuiescent = 2,
    DrainingGlobal = 3,
    Drained = 4,
}

impl ShutdownPhase {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Running,
            1 => Self::StopRequested,
            2 => Self::AllWorkersQuiescent,
            3 => Self::DrainingGlobal,
            4 => Self::Drained,
            _ => panic!("invalid shutdown phase: {raw}"),
        }
    }
}

/// Runtime-wide shutdown protocol.
///
/// The publication gate linearizes task publication with the transition out of
/// [`ShutdownPhase::Running`]. Worker arrival slots make each barrier arrival
/// idempotent, which is required because initialization failures and normal
/// worker exits share the same cleanup path.
pub(crate) struct ShutdownCoordinator {
    phase: AtomicU8,
    worker_count: usize,
    quiescent_workers: AtomicUsize,
    local_drained_workers: AtomicUsize,
    quiescent_arrivals: Box<[AtomicBool]>,
    local_drained_arrivals: Box<[AtomicBool]>,
    publication_gate: Mutex<()>,
    barrier_lock: Mutex<()>,
    barrier: Condvar,
    shutdown_targets: Mutex<Option<Box<[Unparker]>>>,
    drain_failed: AtomicBool,
}

impl ShutdownCoordinator {
    pub(crate) fn new(worker_count: usize) -> Self {
        let quiescent_arrivals = (0..worker_count).map(|_| AtomicBool::new(false)).collect();
        let local_drained_arrivals = (0..worker_count).map(|_| AtomicBool::new(false)).collect();
        Self {
            phase: AtomicU8::new(ShutdownPhase::Running as u8),
            worker_count,
            quiescent_workers: AtomicUsize::new(0),
            local_drained_workers: AtomicUsize::new(0),
            quiescent_arrivals,
            local_drained_arrivals,
            publication_gate: Mutex::new(()),
            barrier_lock: Mutex::new(()),
            barrier: Condvar::new(),
            shutdown_targets: Mutex::new(None),
            drain_failed: AtomicBool::new(false),
        }
    }

    pub(crate) fn install_shutdown_targets(&self, targets: Box<[Unparker]>) {
        let mut guard = self
            .shutdown_targets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(guard.is_none(), "shutdown targets installed twice");
        *guard = Some(targets);
    }

    pub(crate) fn lock_publication(&self) -> MutexGuard<'_, ()> {
        self.publication_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn is_running(&self) -> bool {
        self.phase() == ShutdownPhase::Running
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.phase() >= ShutdownPhase::StopRequested
    }

    pub(crate) fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_raw(self.phase.load(Ordering::Acquire))
    }

    /// Request shutdown while holding the publication gate, then wake workers
    /// only after releasing it. Wake callbacks can re-enter task publication.
    pub(crate) fn request_shutdown(&self) {
        {
            let _gate = self.lock_publication();
            if self.phase() == ShutdownPhase::Running {
                self.phase
                    .store(ShutdownPhase::StopRequested as u8, Ordering::Release);
            }
        }
        self.notify_shutdown_targets();
        self.notify_barrier();
    }

    fn notify_shutdown_targets(&self) {
        let targets = self
            .shutdown_targets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|targets| targets.to_vec());
        if let Some(targets) = targets {
            for target in targets {
                target.shutdown_wake();
            }
        }
    }

    fn notify_barrier(&self) {
        let _guard = self
            .barrier_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.barrier.notify_all();
    }

    pub(crate) fn arrive_quiescent(&self, worker_id: usize) {
        debug_assert!(worker_id < self.worker_count);
        if self.quiescent_arrivals[worker_id].swap(true, Ordering::AcqRel) {
            return;
        }
        let arrived = self.quiescent_workers.fetch_add(1, Ordering::AcqRel) + 1;
        if arrived == self.worker_count {
            let _ = self.phase.compare_exchange(
                ShutdownPhase::StopRequested as u8,
                ShutdownPhase::AllWorkersQuiescent as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            self.notify_barrier();
        }
    }

    /// Mark a worker that never started as having crossed both shutdown barriers.
    ///
    /// Thread creation and initialization happen before task publication, so an absent worker
    /// has no owner-only queue that needs draining. Treating it as arrived keeps the coordinator
    /// finite when the operating system refuses a thread or a worker setup fails before its loop
    /// is entered. The arrival slots make this operation idempotent if an error path races with a
    /// late cleanup callback.
    pub(crate) fn mark_worker_unavailable(&self, worker_id: usize) {
        debug_assert!(worker_id < self.worker_count);
        debug_assert!(self.is_shutdown());
        self.arrive_quiescent(worker_id);
        self.arrive_local_drained(worker_id);
    }

    pub(crate) fn arrive_local_drained(&self, worker_id: usize) {
        debug_assert!(worker_id < self.worker_count);
        if self.local_drained_arrivals[worker_id].swap(true, Ordering::AcqRel) {
            return;
        }
        let arrived = self.local_drained_workers.fetch_add(1, Ordering::AcqRel) + 1;
        if arrived == self.worker_count {
            self.notify_barrier();
        }
    }

    pub(crate) fn wait_for_quiescent(&self) {
        let mut guard = self
            .barrier_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while self.quiescent_workers.load(Ordering::Acquire) < self.worker_count {
            guard = self
                .barrier
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub(crate) fn wait_for_local_drained(&self) {
        let mut guard = self
            .barrier_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while self.local_drained_workers.load(Ordering::Acquire) < self.worker_count {
            guard = self
                .barrier
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub(crate) fn begin_global_drain(&self, worker_id: usize) -> bool {
        debug_assert_eq!(worker_id, 0, "worker 0 owns the global drain");
        let _gate = self.lock_publication();
        assert_eq!(
            self.quiescent_workers.load(Ordering::Acquire),
            self.worker_count,
            "global drain started before all workers became quiescent"
        );
        assert_eq!(
            self.local_drained_workers.load(Ordering::Acquire),
            self.worker_count,
            "global drain started before all worker queues were drained"
        );
        let phase = self.phase();
        if phase == ShutdownPhase::Drained || phase == ShutdownPhase::DrainingGlobal {
            return false;
        }
        assert!(
            phase >= ShutdownPhase::AllWorkersQuiescent,
            "global drain started before worker barriers"
        );
        self.phase
            .store(ShutdownPhase::DrainingGlobal as u8, Ordering::Release);
        true
    }

    pub(crate) fn finish_global_drain(&self) {
        let _gate = self.lock_publication();
        assert_eq!(
            self.phase(),
            ShutdownPhase::DrainingGlobal,
            "global drain completed in an invalid phase"
        );
        self.phase
            .store(ShutdownPhase::Drained as u8, Ordering::Release);
        self.notify_barrier();
    }

    pub(crate) fn wait_for_drained(&self) {
        let mut guard = self
            .barrier_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while self.phase() != ShutdownPhase::Drained {
            guard = self
                .barrier
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub(crate) fn record_drain_failure(&self) {
        self.drain_failed.store(true, Ordering::Release);
    }

    pub(crate) fn drain_failed(&self) -> bool {
        self.drain_failed.load(Ordering::Acquire)
    }
}

/// 运行时共享的 remote wake 故障状态。
///
/// 唤醒入口可能来自 raw `Waker`，因此不能依赖返回值把错误交给调用者。所有入口都把
/// 第一个错误写入这里，并设置 shutdown；仍能处理返回值的同步调用者会同时收到原始的
/// `RuntimeWakeError`。
pub(crate) struct WakeFailureState {
    first_error: Mutex<Option<RuntimeWakeError>>,
    failed: AtomicBool,
    error_count: AtomicU64,
    coordinator: Weak<ShutdownCoordinator>,
}

impl WakeFailureState {
    pub(crate) fn new(coordinator: Weak<ShutdownCoordinator>) -> Self {
        Self {
            first_error: Mutex::new(None),
            failed: AtomicBool::new(false),
            error_count: AtomicU64::new(0),
            coordinator,
        }
    }

    pub(crate) fn record(&self, error: RuntimeWakeError) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let mut first_error = self.first_error.lock().unwrap_or_else(|e| e.into_inner());
        if first_error.is_none() {
            *first_error = Some(error);
        }
        drop(first_error);
        self.failed.store(true, Ordering::Release);
        if let Some(coordinator) = self.coordinator.upgrade() {
            coordinator.request_shutdown();
        }
    }

    pub(crate) fn first_error(&self) -> Option<RuntimeWakeError> {
        self.first_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

/// `block_on` 那个外层 future 的唤醒目标。
///
/// 主线程既是 0 号 worker、又是唯一驱动外层 future 的地方，所以一次唤醒必须同时做两件
/// 事：记下「外层 future 需要重新 poll」，以及把主线程从调度循环的 park 里叫回来 ——
/// park 可能是 [`Unparker`] 的内置信号，也可能是 `park_hook` 里的驱动等待，两者都只认
/// unpark。
pub(crate) struct BlockOnSignal {
    ready: Signal,
    unparker: Unparker,
}

impl BlockOnSignal {
    /// 初始状态就是「待 poll」：外层 future 必须先被 poll 一次才可能注册任何 waker。
    pub(crate) fn new(unparker: Unparker) -> Arc<Self> {
        Arc::new(Self {
            ready: Signal::new(true),
            unparker,
        })
    }

    pub(crate) fn notify(&self) {
        self.ready.notify();
        if self.unparker.unpark().is_err() {
            // raw task Waker 无法返回错误；Unparker 已保存共享 fatal 状态。
        }
    }

    /// 取走「待 poll」标记；返回 `true` 表示本轮应当 poll 外层 future。
    pub(crate) fn take_ready(&self) -> bool {
        self.ready.try_reset()
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.is_notified()
    }
}

pub(crate) fn create_block_on_waker(signal: Arc<BlockOnSignal>) -> Waker {
    let raw = Arc::into_raw(signal) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &BLOCK_ON_VTABLE)) }
}

static BLOCK_ON_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const BlockOnSignal);
        RawWaker::new(p, &BLOCK_ON_VTABLE)
    },
    |p| unsafe {
        Arc::from_raw(p as *const BlockOnSignal).notify();
    },
    |p| unsafe {
        ManuallyDrop::new(Arc::from_raw(p as *const BlockOnSignal)).notify();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const BlockOnSignal));
    },
);

pub fn create_unpark_waker(unparker: Unparker) -> Waker {
    let raw = Arc::into_raw(unparker.inner) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &UNPARK_VTABLE)) }
}

static UNPARK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const UnparkerInner);
        RawWaker::new(p, &UNPARK_VTABLE)
    },
    |p| unsafe {
        let inner = Arc::from_raw(p as *const UnparkerInner);
        inner.wake_from_raw();
    },
    |p| unsafe {
        let inner = ManuallyDrop::new(Arc::from_raw(p as *const UnparkerInner));
        inner.wake_from_raw();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const UnparkerInner));
    },
);

// --- 高性能唤醒原语 (Unparker) ---

pub trait RuntimeWaker: Send + Sync {
    fn wake(&self) -> Result<(), RuntimeWakeError>;
}

pub(crate) struct UnparkerInner {
    /// 没有 `park_hook` 时 worker 就阻塞在这个信号上。
    signal: Signal,
    waker: OnceLock<Arc<dyn RuntimeWaker>>,
    failure: Arc<WakeFailureState>,
}

impl UnparkerInner {
    /// 两个目标都要通知。
    ///
    /// `bind` 之前的唤醒只能落到内置信号上 —— 未绑定时若静默什么都不做，等于丢唤醒；
    /// 而绑定了驱动 waker 的 worker 阻塞在驱动里、看不到信号，只能靠 waker 叫醒。
    /// 信号侧的额外成本只有一次 swap：状态停在「已通知」之后就不会再发系统调用。
    fn wake(&self) -> Result<(), RuntimeWakeError> {
        self.signal.notify();
        if self.failure.is_failed()
            && let Some(error) = self.failure.first_error()
        {
            return Err(error);
        }
        if let Some(waker) = self.waker.get()
            && let Err(error) = waker.wake()
        {
            self.failure.record(error.clone());
            return Err(error);
        }
        Ok(())
    }

    /// raw `Waker` ABI 没有返回错误的通道；错误已在这里写入共享 fatal 状态。
    fn wake_from_raw(&self) {
        if self.wake().is_err() {
            // raw Waker ABI 无返回值；wake() 已保存共享 fatal 状态。
        }
    }

    /// Shutdown notification must still reach a bound driver waker after a
    /// wake failure has been recorded. Secondary shutdown wake failures are
    /// intentionally not recursively recorded.
    fn shutdown_wake(&self) {
        self.signal.notify();
        if let Some(waker) = self.waker.get() {
            let _ = waker.wake();
        }
    }
}

#[derive(Clone)]
pub struct Unparker {
    pub(crate) inner: Arc<UnparkerInner>,
}

impl Default for Unparker {
    fn default() -> Self {
        Self::new()
    }
}

impl Unparker {
    pub fn new() -> Self {
        Self::with_failure_state(Arc::new(WakeFailureState::new(Weak::new())))
    }

    pub(crate) fn with_failure_state(failure: Arc<WakeFailureState>) -> Self {
        Self {
            inner: Arc::new(UnparkerInner {
                signal: Signal::new(false),
                waker: OnceLock::new(),
                failure,
            }),
        }
    }

    pub fn bind(&self, waker: Arc<dyn RuntimeWaker>) -> Result<(), RuntimeWakeError> {
        self.inner.waker.set(waker).map_err(|_| RuntimeWakeError {
            backend: "runtime",
            worker_id: usize::MAX,
            operation: "bind",
            detail: "remote waker is already bound".into(),
        })
    }

    pub fn unpark(&self) -> Result<(), RuntimeWakeError> {
        self.inner.wake()
    }

    pub(crate) fn shutdown_wake(&self) {
        self.inner.shutdown_wake();
    }

    /// 阻塞直到本 worker 被 unpark。运行时未安装 `park_hook` 时的默认 park 实现。
    pub(crate) fn park(&self) {
        self.inner.signal.wait();
    }

    /// 带超时的 [`Self::park`]。
    pub(crate) fn park_timeout(&self, timeout: Duration) {
        self.inner.signal.wait_timeout(timeout);
    }
}

// --- 显式结构化取消系统 (CancellationToken) ---

pub struct GenericCancellationToken<S: Storage, O: Ownership> {
    pub(crate) inner: O::Shared<GenericCancellationTokenInner<S, O>>,
}

/// 任务取消等待节点的注册结果。
///
/// `Linked` 表示节点已经由本令牌保护，调用方必须在节点摘除或被取消排空前
/// 保持其地址有效。`RejectedByCancellation` 只表示本令牌的本地取消，且本次
/// 调用不会把节点留在链表中；跨作用域父取消由注册后的任务状态复查处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelWaiterLinkResult {
    Linked,
    RejectedByCancellation,
}

pub type ChildList<S, O> = <S as Storage>::Lock<LinkedList<CancellationTokenAdapter<S, O>>>;
pub type ParentSlot<S, O> =
    <S as Storage>::Lock<Option<<O as Ownership>::Weak<GenericCancellationTokenInner<S, O>>>>;
pub type CancelWaiterList<S> = <S as Storage>::Lock<LinkedList<ScopeCancelWaiterAdapter>>;

pub struct GenericCancellationTokenInner<S: Storage, O: Ownership> {
    cancelled: S::Usize,
    wakers: S::WakerQueue,
    /// 挂在本令牌上的任务等待节点（节点内联在 task header 里，可摘除）。
    task_waiters: CancelWaiterList<S>,
    children: ChildList<S, O>,
    link: Link,
    parent: ParentSlot<S, O>,
    cross_parent: Option<AnySendScopeRef>,
}

intrusive_adapter!(pub CancellationTokenAdapter<S, O> = GenericCancellationTokenInner<S, O> { link: Link } where S: Storage, O: Ownership);

impl<S: Storage, O: Ownership> GenericCancellationTokenInner<S, O> {
    /// 摘下并唤醒本令牌上的全部等待者。
    ///
    /// 唤醒一律发生在**锁外**：`wake` 会把任务重新入队，可能同步走到
    /// `unlink_cancel_waiter`（任务立刻完成），持锁唤醒即自锁。
    fn wake_waiters(&self) {
        let mut ready = self.wakers.take_all();
        {
            let mut waiters = self.task_waiters.lock();
            while let Some(waiter) = waiters.pop_front() {
                if let Some(waker) = unsafe { waiter.as_ref().get_ref().take_waker() } {
                    ready.push(waker);
                }
            }
        }

        for waker in ready {
            waker.wake();
        }
    }

    /// 取消本令牌及其整棵子树。
    ///
    /// 用**显式工作栈**代替递归：递归深度等于令牌树深度（避免栈溢出），且避免持父锁
    /// 递归遍历子树。这里每层只在锁内摘链并为子节点加一次强引用（否则出锁后子节点可能
    /// 已被析构），唤醒与继续下探都在锁外进行。
    fn cancel_internal(&self) {
        if self
            .cancelled
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        // `cancelled` 的发布先于取得 `task_waiters` 锁。注册者若已经持锁入链，
        // 本次排空会在释放锁后看到它；注册者若之后才取得锁，则锁内检查必然拒绝。
        // 因此成功发布取消后，不会再有路径把新节点写入本令牌的等待链表。
        self.wake_waiters();

        let mut pending = self.detach_children();
        while let Some(child) = pending.pop() {
            let child_ref = unsafe { child.as_ref() };
            if child_ref
                .cancelled
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                child_ref.wake_waiters();
                pending.extend(child_ref.detach_children());
            }
            // 归还上面在锁内加的那次强引用。
            unsafe { O::decrement_strong_count(child.as_ptr() as *const Self) };
        }
    }

    /// 在锁内摘下所有子节点，并为每个子节点加一次强引用后交给调用方。
    ///
    /// 加引用是必需的：出锁之后父链表已不再保护子节点，`Drop for
    /// GenericCancellationToken` 随时可能释放它们。
    fn detach_children(&self) -> Vec<NonNull<Self>> {
        let mut detached = Vec::new();
        let mut children = self.children.lock();
        while let Some(child) = children.pop_front() {
            let ptr = unsafe { NonNull::from(child.get_unchecked_mut()) };
            unsafe { O::increment_strong_count(ptr.as_ptr() as *const Self) };
            detached.push(ptr);
        }
        detached
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

    pub fn new_with_parent(cross_parent: Option<AnySendScopeRef>) -> Self {
        Self {
            inner: O::new(GenericCancellationTokenInner {
                cancelled: S::Usize::new(0),
                wakers: S::WakerQueue::new(),
                task_waiters: S::Lock::new(LinkedList::new(ScopeCancelWaiterAdapter)),
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

        // 同一个令牌被 link 到两个父亲会直接覆盖 prev/next 并损坏链表，`push_back` 自身
        // 只会 panic。这里显式拒绝重复挂载。
        debug_assert!(
            !child.inner.link.is_linked(),
            "cancellation token is already linked to a parent"
        );
        if child.inner.link.is_linked() {
            return;
        }

        unsafe {
            let child_ptr = NonNull::new_unchecked(
                O::as_ptr(&child.inner) as *mut GenericCancellationTokenInner<S, O>
            );
            children.push_back(Pin::new_unchecked(&mut *child_ptr.as_ptr()));
        }
    }

    /// 把任务的等待节点挂到本令牌上。
    ///
    /// 本地取消判断、waker 更新和首次入链在线性化锁内完成。取消标志发布后，
    /// 只有已经持有这把锁的注册者能够先入链，并随后由同一次取消排空；之后取得
    /// 锁的注册者会得到 [`CancelWaiterLinkResult::RejectedByCancellation`]。
    ///
    /// # Safety
    ///
    /// `waiter` 必须在被 `unlink_cancel_waiter` 摘除之前保持有效且地址稳定。
    pub(crate) unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<ScopeCancelWaiter>,
        waker: &Waker,
    ) -> CancelWaiterLinkResult {
        let mut waiters = self.inner.task_waiters.lock();
        if self.inner.cancelled.load(Ordering::Acquire) != 0 {
            return CancelWaiterLinkResult::RejectedByCancellation;
        }

        unsafe {
            let waiter_ref = waiter.as_ref();
            waiter_ref.set_waker(waker);
            if !waiter_ref.link.is_linked() {
                waiters.push_back(Pin::new_unchecked(&mut *waiter.as_ptr()));
            }
        }
        drop(waiters);

        if let Some(ref parent) = self.inner.cross_parent {
            // 跨策略嵌套（local scope 挂在 send scope 下）走不了令牌树，只能退回
            // 父 scope 的 waker 队列。父取消不改变本地注册结果；任务头随后复查。
            parent.register_cancel_waker(waker);
        }

        CancelWaiterLinkResult::Linked
    }

    /// # Safety
    ///
    /// `waiter` 必须是先前传给 `link_cancel_waiter` 的同一个节点。
    pub(crate) unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<ScopeCancelWaiter>) {
        let mut waiters = self.inner.task_waiters.lock();
        if unsafe { waiter.as_ref().link.is_linked() } {
            unsafe {
                let mut cursor = waiters.cursor_mut_from_ptr(waiter);
                cursor.remove();
            }
        }
        let _ = unsafe { waiter.as_ref().take_waker() };
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
        if self.inner.cancelled.load(Ordering::Acquire) != 0 {
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

impl<S: Storage, O: Ownership> Future for CancelledFuture<S, O> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }

        self.token.register_waker(cx.waker());

        if self.token.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
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
    ///
    /// **必须在工作真正可见之后调用**（任务已 push 进队列）。反过来先 bump 再入队会打开
    /// 一个丢唤醒的窗口：worker 读到新序列号 → 检查队列（任务还没进去）→ `should_retry`
    /// 认为无事发生 → 安心 park。
    pub fn notify(&self) {
        self.state.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "loom"))]
    use crate::scope::GenericScopeCompletion;
    #[cfg(not(feature = "loom"))]
    use crate::task::{RawScope, ScopeRef};
    use crate::utils::ownership::ArcOwnership;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread::{scope, sleep},
        time::Duration,
    };
    use veloq_storage::AtomicStorage;

    struct FailingWaker {
        calls: AtomicUsize,
    }

    impl RuntimeWaker for FailingWaker {
        fn wake(&self) -> Result<(), RuntimeWakeError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(RuntimeWakeError {
                backend: "test",
                worker_id: 7,
                operation: "test.wake",
                detail: "injected failure".into(),
            })
        }
    }

    /// 已经发生过的 unpark 必须被记住：worker 检查完队列到真正睡下去之间存在窗口，
    /// 落在窗口里的唤醒若被丢弃就是死锁。
    #[test]
    fn unpark_before_park_does_not_block() {
        let unparker = Unparker::new();
        unparker.unpark().expect("unpark failed");
        unparker.park();
    }

    /// 未 `bind` 任何驱动 waker 时，`unpark` 也必须能把线程从 `park` 里叫回来 ——
    /// 旧实现在这种情况下静默什么都不做。
    #[test]
    fn park_wakes_on_unpark_from_another_thread() {
        let unparker = Unparker::new();
        // 消耗掉可能存在的初始状态，确保真的会睡下去。
        assert!(!unparker.inner.signal.is_notified());

        scope(|threads| {
            threads.spawn(|| {
                sleep(Duration::from_millis(20));
                unparker.unpark().expect("unpark failed");
            });
            unparker.park();
        });
    }

    /// 带超时的 park 不会因为没人唤醒而永久阻塞。
    #[test]
    fn park_timeout_returns_without_an_unpark() {
        let unparker = Unparker::new();
        unparker.park_timeout(Duration::from_millis(5));
    }

    /// `BlockOnSignal` 初始就是「待 poll」，且取走一次之后不会重复触发。
    #[test]
    fn block_on_signal_starts_ready_and_is_consumed_once() {
        let signal = BlockOnSignal::new(Unparker::new());
        assert!(signal.is_ready());
        assert!(signal.take_ready());
        assert!(!signal.take_ready());

        signal.notify();
        assert!(signal.take_ready());
    }

    #[test]
    fn wake_failure_keeps_local_signal_and_first_error() {
        let unparker = Unparker::new();
        let waker = Arc::new(FailingWaker {
            calls: AtomicUsize::new(0),
        });
        unparker.bind(waker.clone()).expect("bind failed");

        let first = unparker.unpark().expect_err("wake should fail");
        let second = unparker.unpark().expect_err("wake should fail");

        assert_eq!(first.worker_id, 7);
        assert_eq!(second, first);
        assert!(unparker.inner.signal.is_notified());
        assert_eq!(waker.calls.load(Ordering::Relaxed), 1);
        assert_eq!(unparker.inner.failure.error_count(), 1);
        assert_eq!(unparker.inner.failure.first_error(), Some(first));
    }

    #[test]
    fn shutdown_barriers_are_idempotent_and_ordered() {
        let coordinator = ShutdownCoordinator::new(2);
        coordinator.request_shutdown();
        assert_eq!(coordinator.phase(), ShutdownPhase::StopRequested);

        coordinator.arrive_quiescent(1);
        coordinator.arrive_quiescent(1);
        coordinator.arrive_quiescent(0);
        coordinator.wait_for_quiescent();
        assert_eq!(coordinator.phase(), ShutdownPhase::AllWorkersQuiescent);

        coordinator.arrive_local_drained(0);
        coordinator.arrive_local_drained(0);
        coordinator.arrive_local_drained(1);
        coordinator.wait_for_local_drained();

        assert!(coordinator.begin_global_drain(0));
        coordinator.finish_global_drain();
        coordinator.wait_for_drained();
        assert_eq!(coordinator.phase(), ShutdownPhase::Drained);
    }

    #[test]
    fn unavailable_workers_close_both_barriers() {
        let coordinator = ShutdownCoordinator::new(3);
        coordinator.request_shutdown();
        coordinator.mark_worker_unavailable(2);
        coordinator.arrive_quiescent(0);
        coordinator.arrive_quiescent(1);
        coordinator.wait_for_quiescent();
        assert_eq!(coordinator.phase(), ShutdownPhase::AllWorkersQuiescent);

        coordinator.arrive_local_drained(0);
        coordinator.arrive_local_drained(1);
        coordinator.wait_for_local_drained();

        assert!(coordinator.begin_global_drain(0));
        assert!(!coordinator.begin_global_drain(0));
        coordinator.finish_global_drain();
        coordinator.wait_for_drained();
    }

    #[test]
    fn shutdown_request_waits_for_publication_gate() {
        let coordinator = Arc::new(ShutdownCoordinator::new(1));
        let gate = coordinator.lock_publication();

        scope(|threads| {
            let coordinator_for_thread = coordinator.clone();
            let handle = threads.spawn(move || coordinator_for_thread.request_shutdown());
            std::thread::yield_now();
            assert_eq!(coordinator.phase(), ShutdownPhase::Running);
            drop(gate);
            handle.join().expect("shutdown requester panicked");
        });

        assert_eq!(coordinator.phase(), ShutdownPhase::StopRequested);
    }

    #[test]
    fn raw_waker_records_wake_failure_without_return_channel() {
        let unparker = Unparker::new();
        unparker
            .bind(Arc::new(FailingWaker {
                calls: AtomicUsize::new(0),
            }))
            .expect("bind failed");
        let waker = create_unpark_waker(unparker.clone());

        waker.wake_by_ref();

        assert!(unparker.inner.failure.is_failed());
        assert!(unparker.inner.failure.first_error().is_some());
        assert!(unparker.inner.signal.is_notified());
    }

    fn new_cancel_waiter() -> ScopeCancelWaiter {
        ScopeCancelWaiter::new()
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn cancel_waiter_registration_rejects_after_local_cancel() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        token.cancel();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        let result = unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) };

        assert_eq!(result, CancelWaiterLinkResult::RejectedByCancellation);
        assert!(!waiter.link.is_linked());
        assert!(token.inner.task_waiters.lock().is_empty());

        unsafe {
            token.unlink_cancel_waiter(waiter_ptr);
            token.unlink_cancel_waiter(waiter_ptr);
        }
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn cancel_drains_registered_waiter_and_repeated_unlink_is_safe() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert!(waiter.link.is_linked());
        assert_eq!(token.inner.task_waiters.lock().len(), 1);

        token.cancel();

        assert!(!waiter.link.is_linked());
        assert!(token.inner.task_waiters.lock().is_empty());
        unsafe {
            token.unlink_cancel_waiter(waiter_ptr);
            token.unlink_cancel_waiter(waiter_ptr);
        }
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn registration_after_cancel_publication_is_rejected_under_lock() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        token.inner.cancelled.store(1, Ordering::Release);
        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::RejectedByCancellation
        );

        assert!(!waiter.link.is_linked());
        assert!(token.inner.task_waiters.lock().is_empty());
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn repeated_waiter_registration_updates_without_duplicate_link() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert_eq!(token.inner.task_waiters.lock().len(), 1);

        unsafe { token.unlink_cancel_waiter(waiter_ptr) };
        assert!(!waiter.link.is_linked());
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn parent_cancellation_does_not_reject_local_registration() {
        let parent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let parent_scope = unsafe { ScopeRef::new(RawScope::clone_raw(parent.as_ref())) };
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new_with_parent(Some(
            AnySendScopeRef::new(parent_scope),
        ));
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        parent.cancel();
        assert!(token.is_cancelled());
        assert!(waiter.link.is_linked());

        unsafe { token.unlink_cancel_waiter(waiter_ptr) };
        assert!(!waiter.link.is_linked());
        assert!(token.inner.task_waiters.lock().is_empty());
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_cancel_and_register_leave_waiter_unlinked() {
        struct SharedWaiter(ScopeCancelWaiter);

        unsafe impl Send for SharedWaiter {}
        unsafe impl Sync for SharedWaiter {}

        loom::model(|| {
            let token = loom::sync::Arc::new(
                GenericCancellationToken::<AtomicStorage, ArcOwnership>::new(),
            );
            let waiter = loom::sync::Arc::new(SharedWaiter(new_cancel_waiter()));
            let register_token = token.clone();
            let register_waiter = waiter.clone();
            let register = loom::thread::spawn(move || {
                let waiter_ptr = NonNull::from(&register_waiter.0);
                unsafe { register_token.link_cancel_waiter(waiter_ptr, Waker::noop()) }
            });

            let cancel_token = token.clone();
            let cancel = loom::thread::spawn(move || {
                cancel_token.cancel();
            });

            let result = register.join().expect("registration thread panicked");
            cancel.join().expect("cancellation thread panicked");

            assert!(matches!(
                result,
                CancelWaiterLinkResult::Linked | CancelWaiterLinkResult::RejectedByCancellation
            ));
            assert!(!waiter.0.link.is_linked());
            assert!(token.inner.task_waiters.lock().is_empty());
        });
    }
}
