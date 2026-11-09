#[cfg(feature = "loom")]
mod loom;
mod scope;
mod shared;
mod sys;

use crate::{
    alloc_crate::sync::Arc,
    error::Error,
    fmt::{self, Formatter, Result as FmtResult},
    num::NonZeroUsize,
    string::String,
    time::Duration,
};

use parker::Parker;
use traits::*;

pub use panic_state::{PanicState, PanicStateGuard};
pub use scope::{RawScope, RawScopedJoinHandle, Scope, ScopeBuilder, ScopedJoinHandle};
pub use sys::{RawJoinHandle, RawThreadError, System};
pub use veloq_tls::{Tls, TlsError, TlsErrorKind, veloq_tls};

#[cfg(any(target_os = "linux", target_os = "android", target_os = "windows"))]
pub(crate) use native::NativeThread;

#[cfg(feature = "loom")]
pub(crate) use loom::LoomThread;

pub mod traits {
    use core::num::NonZeroUsize;

    use crate::{
        error::Error,
        string::String,
        thread::{AbortedError, ThreadErrorKind, ThreadId},
        time::Duration,
    };

    use super::{
        scope::{RawScope, RawScopeApi},
        shared::ThreadPanicPayload,
    };

    /// 原始线程错误接口，供平台特有错误实现
    pub trait RawThreadErrorTrait: Error + Send + Sync + 'static {
        /// 获取线程错误种类
        fn kind(&self) -> ThreadErrorKind;

        /// 从原始 panic payload 构造错误实例
        fn from_panic(payload: ThreadPanicPayload) -> Self;

        /// 提取原始的 panic payload 并在原处留下 None
        fn take_panic(&mut self) -> ThreadPanicPayload {
            Default::default()
        }
    }

    /// 原始线程加入句柄的抽象接口
    pub trait RawJoinHandleTrait<T: Send>: Send + Sync {
        /// 错误类型
        type Error: RawThreadErrorTrait;

        /// 等待线程执行结束 (Join)
        fn join(self) -> Result<T, Self::Error>;

        /// 中止 (Abort) 线程的执行
        fn abort(&self) -> Result<(), Self::Error>;
    }

    /// 平台线程实现的统一抽象接口
    pub trait SystemImpl: Sized {
        /// 错误类型
        type Error: RawThreadErrorTrait;

        /// 原始加入句柄类型，带有生命周期约束
        type RawJoinHandle<'a, T: Send>: RawJoinHandleTrait<T, Error = Self::Error>
        where
            T: 'a;

        /// 产生一个带有自定义配置（如线程名和栈大小）的新线程，并执行传入的 `f` 闭包
        fn spawn<'a, F, T>(
            name: Option<String>,
            stack_size: Option<usize>,
            f: F,
        ) -> Result<Self::RawJoinHandle<'a, T>, Self::Error>
        where
            F: FnOnce() -> T + Send + 'a,
            T: Send + 'a;

        /// 让出当前线程的 CPU 执行时间片。
        ///
        /// 如果成功让出或切换到了另一个线程，返回 `Ok(true)`；否则返回 `Ok(false)`。
        /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
        fn yield_now() -> Result<bool, AbortedError>;

        /// 创建一个结构化并发作用域，并在其中执行闭包 `f`。
        fn scope<'env, F, R>(f: F) -> R
        where
            F: for<'scope> FnOnce(&'scope RawScope<'scope, 'env, Self>) -> R,
        {
            RawScopeApi::run::<Self, F, R>(f)
        }

        /// 使当前线程睡眠指定的时长。
        ///
        /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
        fn sleep(dur: Duration) -> Result<(), AbortedError>;

        /// 获取当前线程的 ID
        fn current_id() -> ThreadId;

        /// 获取当前系统的可用并行度 (逻辑 CPU 核心数)
        fn available_parallelism() -> Result<NonZeroUsize, Self::Error>;
    }
}

mod panic_state {
    //! Thread-local panic state for targets without `std::thread::panicking`.

    #[cfg(not(feature = "std"))]
    use core::cell::Cell;

    #[cfg(feature = "std")]
    use std::thread::panicking as std_panicking;

    /// The panic state visible to destructors on the current thread.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum PanicState {
        /// No panic is currently being unwound.
        NotPanicking,
        /// The target panic backend has started unwinding.
        Panicking,
        /// The state could not be tracked because the TLS backend was unavailable.
        Untracked,
    }

    #[cfg(not(feature = "std"))]
    static CURRENT_PANIC_STATE: veloq_tls::Tls<Cell<PanicState>> = veloq_tls::Tls::new();

    impl PanicState {
        /// Returns the state of the current thread.
        #[inline]
        pub fn current() -> Self {
            #[cfg(feature = "std")]
            {
                if std_panicking() {
                    PanicState::Panicking
                } else {
                    PanicState::NotPanicking
                }
            }

            #[cfg(not(feature = "std"))]
            {
                CURRENT_PANIC_STATE
                    .try_with_or_init(|state| state.get(), || Cell::new(PanicState::NotPanicking))
                    .unwrap_or(PanicState::Untracked)
            }
        }

        /// Returns whether the current thread is known to be panicking.
        #[inline]
        pub fn is_panicking() -> bool {
            matches!(Self::current(), Self::Panicking)
        }
    }

    /// Marks the current thread as panicking for a target panic backend.
    ///
    /// A backend must call [`PanicStateGuard::finish`] on every normal return
    /// path. If control flow unwinds through the guard, dropping it intentionally
    /// leaves the state as `Panicking` so later destructors do not mistake the
    /// unwind for normal execution. A backend that catches that unwind must reset
    /// the state before continuing.
    #[must_use = "call finish on the backend's normal return path"]
    pub struct PanicStateGuard {
        #[cfg(not(feature = "std"))]
        previous: PanicState,
        #[cfg(not(feature = "std"))]
        finished: bool,
    }

    impl PanicStateGuard {
        /// Enters the `Panicking` state on the current thread.
        #[inline]
        pub fn enter() -> Self {
            #[cfg(feature = "std")]
            {
                Self {}
            }

            #[cfg(not(feature = "std"))]
            {
                let previous = CURRENT_PANIC_STATE
                    .try_with_or_init(
                        |state| {
                            let previous = state.get();
                            state.set(PanicState::Panicking);
                            previous
                        },
                        || Cell::new(PanicState::Panicking),
                    )
                    .unwrap_or(PanicState::Untracked);
                Self {
                    previous,
                    finished: false,
                }
            }
        }
    }

    impl PanicStateGuard {
        /// Restores the state that was active before [`PanicStateGuard::enter`].
        #[inline]
        pub fn finish(self) {
            #[cfg(not(feature = "std"))]
            {
                let mut this = self;
                if !this.finished {
                    let previous = this.previous;
                    let _ = CURRENT_PANIC_STATE.try_with(|state| state.set(previous));
                    this.finished = true;
                }
            }
        }
    }

    impl Drop for PanicStateGuard {
        fn drop(&mut self) {
            // Do not restore state implicitly. Stable no_std code cannot tell a
            // normal drop from a drop caused by unwinding; restoring here would
            // hide the panic from destructors that run later in the unwind.
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "windows"))]
mod native {
    use super::ThreadId;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    use libc::pthread_self;
    #[cfg(target_os = "windows")]
    use windows_sys::Win32::System::Threading::GetCurrentThreadId;

    #[cfg(any(target_os = "linux", target_os = "android", target_os = "windows"))]
    pub(crate) struct NativeThread;

    impl NativeThread {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        pub(crate) fn current_id() -> ThreadId {
            ThreadId(unsafe { pthread_self() as u64 })
        }

        #[cfg(target_os = "windows")]
        pub(crate) fn current_id() -> ThreadId {
            ThreadId(unsafe { GetCurrentThreadId() } as u64)
        }
    }
}

mod parker {
    use crate::{
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    #[cfg(not(feature = "loom"))]
    use crate::sync::Wait;

    #[cfg(feature = "loom")]
    use loom::sync::{Condvar, Mutex};

    #[cfg(not(feature = "loom"))]
    const EMPTY: u32 = 0;
    #[cfg(not(feature = "loom"))]
    const PARKED: u32 = 1;
    #[cfg(not(feature = "loom"))]
    const NOTIFIED: u32 = 2;

    #[cfg(not(feature = "loom"))]
    pub(crate) struct Parker {
        state: AtomicU32,
    }

    #[cfg(not(feature = "loom"))]
    impl Parker {
        pub const fn new() -> Self {
            Self {
                state: AtomicU32::new(EMPTY),
            }
        }

        pub fn park(&self) {
            if self
                .state
                .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            if self
                .state
                .compare_exchange(EMPTY, PARKED, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                while self.state.load(Ordering::Acquire) == PARKED {
                    Wait::wait_on_address(&self.state, PARKED);
                }
            }
        }

        pub fn park_timeout(&self, dur: Duration) {
            if self
                .state
                .compare_exchange(NOTIFIED, EMPTY, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            if self
                .state
                .compare_exchange(EMPTY, PARKED, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                Wait::wait_on_address_timeout(&self.state, PARKED, Some(dur));
                self.state.swap(EMPTY, Ordering::Acquire);
            }
        }

        pub fn unpark(&self) {
            let old = self.state.swap(NOTIFIED, Ordering::Release);
            if old == PARKED {
                Wait::wake_by_address(&self.state);
            }
        }
    }

    #[cfg(feature = "loom")]
    const EMPTY: u32 = 0;
    #[cfg(feature = "loom")]
    const PARKED: u32 = 1;
    #[cfg(feature = "loom")]
    const NOTIFIED: u32 = 2;

    #[cfg(feature = "loom")]
    pub(crate) struct Parker {
        inner: Mutex<u32>,
        condvar: Condvar,
    }

    #[cfg(feature = "loom")]
    impl Parker {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(EMPTY),
                condvar: Condvar::new(),
            }
        }

        pub fn park(&self) {
            let mut state = self.inner.lock().unwrap();
            if *state == NOTIFIED {
                *state = EMPTY;
                return;
            }
            if *state == EMPTY {
                *state = PARKED;
                while *state == PARKED {
                    state = self.condvar.wait(state).unwrap();
                }
            }
        }

        pub fn park_timeout(&self, dur: Duration) {
            let mut state = self.inner.lock().unwrap();
            if *state == NOTIFIED {
                *state = EMPTY;
                return;
            }
            if *state == EMPTY {
                *state = PARKED;
                let (g, _) = self.condvar.wait_timeout(state, dur).unwrap();
                let mut state = g;
                *state = EMPTY;
            }
        }

        pub fn unpark(&self) {
            let mut state = self.inner.lock().unwrap();
            let old = *state;
            *state = NOTIFIED;
            if old == PARKED {
                self.condvar.notify_one();
            }
        }
    }
}

/// 线程错误种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadErrorKind {
    /// 线程创建失败
    CreationFailed,
    /// 线程加入失败
    JoinFailed,
    /// 线程中止失败
    AbortFailed,
    /// 线程已被加入
    AlreadyJoined,
    /// 线程返回值已被获取
    ResultAlreadyTaken,
    /// 线程没有返回值
    ResultMissing,
    /// 线程执行时发生了 Panic
    Panicked,
    /// 线程执行被中止 (Abort)
    Aborted,
}

/// 线程被中止退出时的错误类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedError;

impl fmt::Display for AbortedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "thread execution was aborted")
    }
}

impl Error for AbortedError {}

/// 统一的线程错误，封装了后端的原始 `RawThreadError`。
#[derive(Debug)]
pub struct ThreadError {
    inner: RawThreadError,
}

impl ThreadError {
    /// 包装一个平台原始线程错误为统一的 `ThreadError`
    pub fn new(err: RawThreadError) -> Self {
        Self { inner: err }
    }

    /// 获取底层的错误种类
    pub fn kind(&self) -> ThreadErrorKind {
        self.inner.kind()
    }
}

impl fmt::Display for ThreadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error for ThreadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

struct Inner {
    name: Option<String>,
    parker: Parker,
}

/// 统一的线程表示结构体
#[derive(Clone)]
pub struct Thread {
    inner: Arc<Inner>,
}

/// 统一的线程加入句柄，封装了后端特定平台的原始 `RawJoinHandle`。
pub struct JoinHandle<'a, T> {
    inner: RawJoinHandle<'a, T>,
}

unsafe impl<T: Send> Send for JoinHandle<'_, T> {}

unsafe impl<T: Send> Sync for JoinHandle<'_, T> {}

impl<'a, T: Send> JoinHandle<'a, T> {
    /// 获取与该加入句柄关联的线程的引用。
    pub fn thread(&self) -> &Thread {
        self.inner.thread()
    }

    /// 等待线程执行结束 (Join)
    pub fn join(self) -> Result<T, ThreadError> {
        self.inner.join().map_err(ThreadError::new)
    }

    /// 中止 (Abort) 线程的执行
    pub fn abort(&self) -> Result<(), ThreadError> {
        self.inner.abort().map_err(ThreadError::new)
    }
}

pub struct ThreadApi;

impl ThreadApi {
    /// 产生一个新线程，并执行传入的 `f` 闭包。
    pub fn spawn<'a, F, T>(f: F) -> Result<JoinHandle<'a, T>, ThreadError>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        System::spawn(None, None, f)
            .map(|inner| JoinHandle { inner })
            .map_err(ThreadError::new)
    }

    /// 让出当前线程的 CPU 执行时间片。
    ///
    /// 如果成功让出或切换到了另一个线程，返回 `Ok(true)`；否则返回 `Ok(false)`。
    /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
    pub fn yield_now() -> Result<bool, AbortedError> {
        System::yield_now()
    }

    /// 使当前线程睡眠指定的时长。
    ///
    /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
    pub fn sleep(dur: Duration) -> Result<(), AbortedError> {
        System::sleep(dur)
    }
}

/// 线程的唯一标识符
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ThreadId(u64);

impl ThreadId {
    /// 将 ThreadId 转换为 u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for Thread {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Thread")
            .field("name", &self.name())
            .finish_non_exhaustive()
    }
}

impl Thread {
    pub(crate) fn new(name: Option<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                name,
                parker: Parker::new(),
            }),
        }
    }

    /// 获取线程的名称（如果有）
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// 原子地使该线程的令牌可用。
    pub fn unpark(&self) {
        self.inner.parker.unpark();
    }

    /// 阻塞当前线程。
    pub fn park(&self) {
        self.inner.parker.park();
    }

    /// 阻塞当前线程，最多等待指定的时长。
    pub fn park_timeout(&self, dur: Duration) {
        self.inner.parker.park_timeout(dur);
    }
}

impl ThreadApi {
    /// 获取当前线程
    pub fn current() -> Thread {
        shared::CURRENT_THREAD.with_or_init(|t| t.clone(), || Thread::new(None))
    }

    /// 获取当前线程的唯一标识符
    #[inline]
    pub fn current_id() -> ThreadId {
        System::current_id()
    }

    /// 阻塞当前线程。
    pub fn park_current() {
        Self::current().park();
    }

    /// 阻塞当前线程，最多等待指定的时长。
    pub fn park_current_timeout(dur: Duration) {
        Self::current().park_timeout(dur);
    }

    /// 获取系统的可用并行度 (逻辑 CPU 核心数)
    pub fn available_parallelism() -> Result<NonZeroUsize, ThreadError> {
        System::available_parallelism().map_err(ThreadError::new)
    }

    /// 获取当前线程是否正在 panic。
    pub fn panicking() -> bool {
        PanicState::is_panicking()
    }

    pub fn scope<'env, F, T>(f: F) -> T
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
    {
        scope::ScopeApi::run(f)
    }
}

/// 线程工厂，可用于配置新线程的属性。
#[must_use = "must eventually spawn the thread"]
#[derive(Debug)]
pub struct Builder {
    name: Option<String>,
    stack_size: Option<usize>,
}

impl Builder {
    /// 产生一个默认配置的 Builder。
    pub fn new() -> Self {
        Self {
            name: None,
            stack_size: None,
        }
    }

    /// 设置新线程的名称。
    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// 设置新线程的栈大小（字节）。
    pub fn stack_size(mut self, size: usize) -> Self {
        self.stack_size = Some(size);
        self
    }

    /// 使用配置启动新线程。
    pub fn spawn<'a, F, T>(self, f: F) -> Result<JoinHandle<'a, T>, ThreadError>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        System::spawn(self.name, self.stack_size, f)
            .map(|inner| JoinHandle { inner })
            .map_err(ThreadError::new)
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}
