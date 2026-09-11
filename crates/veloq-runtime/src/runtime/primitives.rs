use crate::error::{RuntimeError, RuntimeWakeError};
use diagweave::{Report, prelude::*};
use veloq_std::{
    boxed::Box,
    mem::ManuallyDrop,
    result::Result as StdResult,
    sync::{
        NativeArc as Arc, NativeWeak as Weak, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    task::{RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
use veloq_std::string::ToString;

use veloq_std::sync::{UnpoisonedCondvar, UnpoisonedMutex, UnpoisonedMutexGuard};

// --- 系统级同步原语 (WaitOnAddress / Futex) ---

pub(crate) mod sys {
    use veloq_std::{
        fmt::{self, Display, Formatter},
        result::Result as StdResult,
        sync::atomic::AtomicU32,
        thread::AbortedError,
        time::Duration,
    };

    #[cfg(any(target_os = "linux", target_os = "android"))]
    use veloq_futex::{FutexError, WaitOutcome, wait as futex_wait, wake as futex_wake};

    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    use veloq_std::thread;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum WaitError {
        Aborted(AbortedError),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Futex(FutexError),
    }

    impl Display for WaitError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            match self {
                Self::Aborted(error) => Display::fmt(error, formatter),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                Self::Futex(error) => Display::fmt(error, formatter),
            }
        }
    }

    impl veloq_std::error::Error for WaitError {}

    #[cfg(windows)]
    mod win {
        use veloq_std::ffi::c_void;

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
    pub unsafe fn wait(addr: &AtomicU32, expected: u32) -> StdResult<(), WaitError> {
        let expected_val = expected;
        unsafe {
            win::WaitOnAddress(
                addr as *const _ as *const _,
                &expected_val as *const _ as *const _,
                4,
                0xFFFFFFFF, // INFINITE
            );
        }
        Ok(())
    }

    #[cfg(windows)]
    pub unsafe fn wait_timeout(
        addr: &AtomicU32,
        expected: u32,
        timeout: Duration,
    ) -> StdResult<(), WaitError> {
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
            );
        }
        Ok(())
    }

    #[cfg(windows)]
    pub unsafe fn wake_all(addr: &AtomicU32) -> StdResult<(), WaitError> {
        unsafe {
            win::WakeByAddressAll(addr as *const _ as *const _);
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub unsafe fn wait(addr: &AtomicU32, expected: u32) -> StdResult<(), WaitError> {
        match unsafe { futex_wait(addr as *const AtomicU32 as *const u32, expected, None) }
            .map_err(WaitError::Futex)?
        {
            WaitOutcome::Woken | WaitOutcome::TimedOut => Ok(()),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub unsafe fn wait_timeout(
        addr: &AtomicU32,
        expected: u32,
        timeout: Duration,
    ) -> StdResult<(), WaitError> {
        unsafe {
            futex_wait(
                addr as *const AtomicU32 as *const u32,
                expected,
                Some(timeout),
            )
        }
        .map(|_| ())
        .map_err(WaitError::Futex)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub unsafe fn wake_all(addr: &AtomicU32) -> StdResult<(), WaitError> {
        unsafe { futex_wake(addr as *const AtomicU32 as *const u32, i32::MAX as u32) }
            .map(|_| ())
            .map_err(WaitError::Futex)
    }

    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    pub unsafe fn wait(_addr: &AtomicU32, _expected: u32) -> StdResult<(), WaitError> {
        thread::yield_now().map(|_| ()).map_err(WaitError::Aborted)
    }

    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    pub unsafe fn wait_timeout(
        _addr: &AtomicU32,
        _expected: u32,
        timeout: Duration,
    ) -> StdResult<(), WaitError> {
        thread::sleep(timeout).map_err(WaitError::Aborted)
    }

    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    pub unsafe fn wake_all(_addr: &AtomicU32) -> StdResult<(), WaitError> {
        Ok(())
    }
}

pub(crate) use sys::WaitError;

pub(crate) fn wait_error_to_runtime(error: WaitError, worker_id: usize) -> Report<RuntimeError> {
    match error {
        WaitError::Aborted(_) => RuntimeError::ThreadAborted { worker_id }.to_report(),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        WaitError::Futex(error) => RuntimeError::WaitFailed {
            worker_id,
            detail: error.to_string(),
        }
        .to_report(),
    }
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
            unsafe { sys::wake_all(&self.state) }
                .unwrap_or_else(|error| panic!("runtime futex wake_all failed: {error:?}"));
        }
    }

    pub(crate) fn wait(&self) -> StdResult<(), WaitError> {
        loop {
            // Fast-path: try to consume the notification
            if self
                .state
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            // Slow-path: block until notified
            unsafe { sys::wait(&self.state, 0) }?;
        }
    }

    pub(crate) fn wait_timeout(&self, duration: Duration) -> StdResult<bool, WaitError> {
        if self
            .state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(true);
        }

        unsafe { sys::wait_timeout(&self.state, 0, duration) }?;

        Ok(self
            .state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok())
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
    publication_gate: UnpoisonedMutex<()>,
    barrier_lock: UnpoisonedMutex<()>,
    barrier: UnpoisonedCondvar,
    shutdown_targets: UnpoisonedMutex<Option<Box<[Unparker]>>>,
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
            publication_gate: UnpoisonedMutex::new(()),
            barrier_lock: UnpoisonedMutex::new(()),
            barrier: UnpoisonedCondvar::new(),
            shutdown_targets: UnpoisonedMutex::new(None),
            drain_failed: AtomicBool::new(false),
        }
    }

    pub(crate) fn install_shutdown_targets(&self, targets: Box<[Unparker]>) {
        let mut guard = self.shutdown_targets.lock();
        assert!(guard.is_none(), "shutdown targets installed twice");
        *guard = Some(targets);
    }

    pub(crate) fn lock_publication(&self) -> UnpoisonedMutexGuard<'_, ()> {
        self.publication_gate.lock()
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
            .as_ref()
            .map(|targets| targets.to_vec());
        if let Some(targets) = targets {
            for target in targets {
                target.shutdown_wake();
            }
        }
    }

    fn notify_barrier(&self) {
        let _guard = self.barrier_lock.lock();
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
        let mut guard = self.barrier_lock.lock();
        while self.quiescent_workers.load(Ordering::Acquire) < self.worker_count {
            guard = self.barrier.wait(guard);
        }
    }

    pub(crate) fn wait_for_local_drained(&self) {
        let mut guard = self.barrier_lock.lock();
        while self.local_drained_workers.load(Ordering::Acquire) < self.worker_count {
            guard = self.barrier.wait(guard);
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
        let mut guard = self.barrier_lock.lock();
        while self.phase() != ShutdownPhase::Drained {
            guard = self.barrier.wait(guard);
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
    first_error: UnpoisonedMutex<Option<RuntimeWakeError>>,
    failed: AtomicBool,
    error_count: AtomicU64,
    coordinator: Weak<ShutdownCoordinator>,
}

impl WakeFailureState {
    pub(crate) fn new(coordinator: Weak<ShutdownCoordinator>) -> Self {
        Self {
            first_error: UnpoisonedMutex::new(None),
            failed: AtomicBool::new(false),
            error_count: AtomicU64::new(0),
            coordinator,
        }
    }

    pub(crate) fn record(&self, error: RuntimeWakeError) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let mut first_error = self.first_error.lock();
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
        self.first_error.lock().clone()
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
    fn wake(&self) -> StdResult<(), RuntimeWakeError>;
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
    fn wake(&self) -> StdResult<(), RuntimeWakeError> {
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

    pub fn bind(&self, waker: Arc<dyn RuntimeWaker>) -> StdResult<(), RuntimeWakeError> {
        self.inner.waker.set(waker).map_err(|_| RuntimeWakeError {
            backend: "runtime",
            worker_id: usize::MAX,
            operation: "bind",
            detail: "remote waker is already bound".into(),
        })
    }

    pub fn unpark(&self) -> StdResult<(), RuntimeWakeError> {
        self.inner.wake()
    }

    pub(crate) fn shutdown_wake(&self) {
        self.inner.shutdown_wake();
    }

    /// 阻塞直到本 worker 被 unpark。运行时未安装 `park_hook` 时的默认 park 实现。
    pub(crate) fn park(&self) -> StdResult<(), WaitError> {
        self.inner.signal.wait()
    }

    /// 带超时的 [`Self::park`]。
    pub(crate) fn park_timeout(&self, timeout: Duration) -> StdResult<bool, WaitError> {
        self.inner.signal.wait_timeout(timeout)
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
    use veloq_std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread::{AbortedError, scope, sleep},
        time::Duration,
    };

    struct FailingWaker {
        calls: AtomicUsize,
    }

    impl RuntimeWaker for FailingWaker {
        fn wake(&self) -> StdResult<(), RuntimeWakeError> {
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
        unparker.park().expect("park failed");
    }

    /// 未 `bind` 任何驱动 waker 时，`unpark` 也必须能把线程从 `park` 里叫回来 ——
    /// 旧实现在这种情况下静默什么都不做。
    #[test]
    fn park_wakes_on_unpark_from_another_thread() {
        let unparker = Unparker::new();
        // 消耗掉可能存在的初始状态，确保真的会睡下去。
        assert!(!unparker.inner.signal.is_notified());

        scope(|threads| {
            threads
                .spawn(|| {
                    sleep(Duration::from_millis(20)).expect("sleep failed");
                    unparker.unpark().expect("unpark failed");
                })
                .expect("sleep worker failed to spawn");
            unparker.park().expect("park failed");
        });
    }

    /// 带超时的 park 不会因为没人唤醒而永久阻塞。
    #[test]
    fn park_timeout_returns_without_an_unpark() {
        let unparker = Unparker::new();
        unparker
            .park_timeout(Duration::from_millis(5))
            .expect("timed park failed");
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
    fn cooperative_abort_converts_to_runtime_error() {
        let report = wait_error_to_runtime(WaitError::Aborted(AbortedError), 7);

        assert!(matches!(
            report.inner(),
            RuntimeError::ThreadAborted { worker_id: 7 }
        ));
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
            veloq_std::thread::yield_now().expect("yield failed");
            assert_eq!(coordinator.phase(), ShutdownPhase::Running);
            drop(gate);
            handle
                .expect("shutdown requester failed to spawn")
                .join()
                .expect("shutdown requester panicked");
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
}
