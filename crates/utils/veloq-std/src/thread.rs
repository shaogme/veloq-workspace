pub mod traits;
use traits::*;

mod parker;
mod platform;
pub use platform::{Platform, RawJoinHandle, RawThreadError};

mod scope;
pub use scope::{
    Scope, ScopeBuilder, ScopedJoinHandle,
    raw::{RawScope, RawScopedJoinHandle},
    scope,
};

use crate::{
    alloc_crate::sync::Arc,
    error::Error,
    fmt::{self, Formatter, Result as FmtResult},
    num::NonZeroUsize,
    string::String,
    time::Duration,
};

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

/// 统一的线程加入句柄，封装了后端特定平台的原始 `RawJoinHandle`。
pub struct JoinHandle<'a, T> {
    inner: RawJoinHandle<'a, T>,
}

unsafe impl<T: Send> Send for JoinHandle<'_, T> {}
unsafe impl<T: Send> Sync for JoinHandle<'_, T> {}

impl<'a, T: Send> JoinHandle<'a, T> {
    /// 获取与该加入句柄关联的线程的引用。
    pub fn thread(&self) -> &Thread {
        &self.inner.thread
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

/// 产生一个新线程，并执行传入的 `f` 闭包。
pub fn spawn<'a, F, T>(f: F) -> Result<JoinHandle<'a, T>, ThreadError>
where
    F: FnOnce() -> T + Send + 'a,
    T: Send + 'a,
{
    Platform::spawn(None, None, f)
        .map(|inner| JoinHandle { inner })
        .map_err(ThreadError::new)
}

/// 让出当前线程的 CPU 执行时间片。
///
/// 如果成功让出或切换到了另一个线程，返回 `Ok(true)`；否则返回 `Ok(false)`。
/// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
pub fn yield_now() -> Result<bool, AbortedError> {
    Platform::yield_now()
}

/// 使当前线程睡眠指定的时长。
///
/// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
pub fn sleep(dur: Duration) -> Result<(), AbortedError> {
    Platform::sleep(dur)
}

/// 线程的唯一标识符
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ThreadId(pub(crate) u64);

impl ThreadId {
    /// 将 ThreadId 转换为 u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

use parker::Parker;

struct Inner {
    name: Option<String>,
    parker: Parker,
}

/// 统一的线程表示结构体
#[derive(Clone)]
pub struct Thread {
    inner: Arc<Inner>,
}

impl fmt::Debug for Thread {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Thread")
            .field("id", &self.id())
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

    /// 获取当前线程的唯一标识符
    pub fn id(&self) -> ThreadId {
        ThreadId(Arc::as_ptr(&self.inner) as *const () as u64)
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

/// 获取当前线程
pub fn current() -> Thread {
    platform::CURRENT_THREAD.with_or_init(|t| t.clone(), || Thread::new(None))
}

/// 阻塞当前线程。
pub fn park() {
    current().park();
}

/// 阻塞当前线程，最多等待指定的时长。
pub fn park_timeout(dur: Duration) {
    current().park_timeout(dur);
}

/// 获取系统的可用并行度 (逻辑 CPU 核心数)
pub fn available_parallelism() -> Result<NonZeroUsize, ThreadError> {
    Platform::available_parallelism().map_err(ThreadError::new)
}

/// 获取当前线程是否正在 panic。
pub fn panicking() -> bool {
    if cfg!(feature = "std") {
        std::thread::panicking()
    } else {
        false
    }
}

/// 线程工厂，可用于配置新线程的属性。
#[must_use = "must eventually spawn the thread"]
#[derive(Debug)]
pub struct Builder {
    pub(crate) name: Option<String>,
    pub(crate) stack_size: Option<usize>,
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
        Platform::spawn(self.name, self.stack_size, f)
            .map(|inner| JoinHandle { inner })
            .map_err(ThreadError::new)
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}
