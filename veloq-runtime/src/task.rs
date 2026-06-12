mod arena;
mod header;
mod nodes;
mod scope;

pub use arena::{Arena, GenericArena};
pub use header::{
    GenericTaskHeader, GenericWakerNode, INTRUSIVE_WAKER_VTABLE, LOCAL_INTRUSIVE_WAKER_VTABLE,
    PollStatus, STATE_CANCELLED, STATE_COMPLETED, STATE_POLLING, STATE_QUEUED, STATE_READY,
    STATE_WOKEN, TaskVTable,
};
pub use nodes::{LocalBoxedTaskNode, LocalTaskNode, SendBoxedTaskNode, SendTaskNode};
pub use scope::{
    AnyScopeRef, AnySendScopeRef, ErasedCancellationToken, OpaqueScope, OpaqueToken, RawScope,
    ScopeParent, ScopeRef, ScopeStorage,
};

use crate::utils::storage::{AtomicStorage, LocalStorage, StateLock, Storage};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

pub type TaskHeader = GenericTaskHeader<AtomicStorage>;
pub type LocalTaskHeader = GenericTaskHeader<LocalStorage>;

// --- 任务错误与结果扩展 ---

pub enum TaskError {
    /// 任务在执行过程中发生了 Panic
    Panic,
    /// 任务因作用域被取消而终止
    Cancelled,
}

impl std::fmt::Debug for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Panic => write!(f, "Task panicked"),
            Self::Cancelled => write!(f, "Task cancelled"),
        }
    }
}

pub trait RuntimeContextExt {
    fn is_cancelled(&self) -> bool;
    fn scope_completion(&self) -> Option<AnyScopeRef>;
}

impl RuntimeContextExt for Context<'_> {
    fn is_cancelled(&self) -> bool {
        unsafe {
            if let Some(h) = TaskHeader::from_waker(self.waker(), &INTRUSIVE_WAKER_VTABLE) {
                return h.is_cancelled();
            }
            if let Some(h) =
                LocalTaskHeader::from_waker(self.waker(), &LOCAL_INTRUSIVE_WAKER_VTABLE)
            {
                return h.is_cancelled();
            }
            false
        }
    }

    fn scope_completion(&self) -> Option<AnyScopeRef> {
        unsafe {
            if let Some(h) = TaskHeader::from_waker(self.waker(), &INTRUSIVE_WAKER_VTABLE) {
                return Some(h.scope_completion_ref().into_any());
            }
            if let Some(h) =
                LocalTaskHeader::from_waker(self.waker(), &LOCAL_INTRUSIVE_WAKER_VTABLE)
            {
                return Some(h.scope_completion_ref().into_any());
            }
            None
        }
    }
}

pub trait TaskHandleRef: Copy {
    type Storage: Storage;
    fn header(&self) -> &GenericTaskHeader<Self::Storage>;
    /// # Safety
    /// The `header` pointer must be a valid pointer to a `GenericTaskHeader`.
    unsafe fn from_header(header: *const GenericTaskHeader<Self::Storage>) -> Self;

    /// Polls the task through the handle.
    fn poll_task(&self, worker_id: usize) -> bool;

    /// # Safety
    /// The `ptr` must be a valid pointer to a task node implementing `RawTask` with the correct storage.
    unsafe fn from_concrete<U>(ptr: *const U) -> Self
    where
        U: RawTask<Storage = Self::Storage>;
}

pub trait RawTask {
    type Storage: Storage;
    fn poll_raw(&self, worker_id: usize) -> bool;
    fn header(&self) -> &GenericTaskHeader<Self::Storage>;
}

pub trait Task<T>: RawTask {
    fn poll_task(&self, cx: &mut Context<'_>) -> bool;
    fn take_result(&self) -> Option<Result<T, TaskError>>;
}

pub trait LocalTask<T>: Task<T, Storage = LocalStorage> {}
impl<T, U: Task<T, Storage = LocalStorage> + ?Sized> LocalTask<T> for U {}

pub trait SendTask<T>: Task<T, Storage = AtomicStorage> + Send {}
impl<T, U: Task<T, Storage = AtomicStorage> + Send + ?Sized> SendTask<T> for U {}

pub trait TaskLock<T> {
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T, L: StateLock<T>> TaskLock<T> for L {
    #[inline]
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut *self.lock())
    }
}

pub struct LifecycleManager<'a, S: Storage> {
    header: &'a GenericTaskHeader<S>,
}

impl<'a, S: Storage> LifecycleManager<'a, S> {
    #[inline]
    pub fn new(header: &'a GenericTaskHeader<S>) -> Self {
        Self { header }
    }

    pub fn enter_poll(&self, is_local: bool) -> PollStatus {
        if is_local {
            if self.header.is_completed() {
                return PollStatus::Complete;
            }
            return PollStatus::Proceed;
        }
        self.header.try_enter_poll()
    }

    pub fn exit_pending(&self, is_local: bool) -> bool {
        if is_local {
            return false;
        }
        self.header.exit_poll_to_pending()
    }
}

pub trait TaskResultSetter<T> {
    fn set_result(&self, res: Result<T, TaskError>);
}

pub struct TaskFinalizer<'a, T, R, S: Storage>
where
    R: TaskResultSetter<T>,
{
    header: &'a GenericTaskHeader<S>,
    result_setter: &'a R,
    marker: std::marker::PhantomData<T>,
}

impl<'a, T, R, S: Storage> TaskFinalizer<'a, T, R, S>
where
    R: TaskResultSetter<T>,
{
    #[inline]
    pub fn new(header: &'a GenericTaskHeader<S>, result_setter: &'a R) -> Self {
        Self {
            header,
            result_setter,
            marker: std::marker::PhantomData,
        }
    }

    pub fn complete(&self, res: Result<T, TaskError>, is_local: bool) {
        self.result_setter.set_result(res);
        self.finalize(is_local);
    }

    pub fn complete_panic(&self, panic_err: Box<dyn Any + Send + 'static>, is_local: bool) {
        let is_cancelled = if let Some(e) = panic_err.downcast_ref::<TaskError>() {
            matches!(e, TaskError::Cancelled)
        } else {
            false
        };

        if !is_cancelled {
            let scope_ref = self.header.scope_completion_ref();
            scope_ref.report_panic(panic_err);
            scope_ref.cancel();
        }

        let error = if is_cancelled {
            TaskError::Cancelled
        } else {
            TaskError::Panic
        };

        self.result_setter.set_result(Err(error));
        self.finalize(is_local);
    }

    fn finalize(&self, is_local: bool) {
        self.header.mark_completed_and_notify();

        let should_acknowledge = self.header.decrement_ref_count();

        if !is_local {
            self.header.exit_poll();
        }

        if should_acknowledge {
            self.header.acknowledge_completion();
        }
    }
}

pub(crate) fn poll_task_internal<T, R, F, S: Storage>(
    header: &GenericTaskHeader<S>,
    result_setter: &R,
    cx: &mut Context<'_>,
    mut poll_fn: F,
    is_local: bool,
) -> bool
where
    R: TaskResultSetter<T>,
    F: FnMut(&mut Context<'_>) -> Poll<T>,
{
    let lifecycle = LifecycleManager::new(header);
    let finalizer = TaskFinalizer::new(header, result_setter);

    match lifecycle.enter_poll(is_local) {
        PollStatus::Proceed => {}
        PollStatus::Yield => return false,
        PollStatus::Complete => return true,
    }

    loop {
        if header.is_cancelled() {
            finalizer.complete(Err(TaskError::Cancelled), is_local);
            return true;
        }

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll_fn(cx)));
        match res {
            Ok(Poll::Ready(val)) => {
                finalizer.complete(Ok(val), is_local);
                return true;
            }
            Ok(Poll::Pending) => {
                if header.is_cancelled() {
                    finalizer.complete(Err(TaskError::Cancelled), is_local);
                    return true;
                }
                if lifecycle.exit_pending(is_local) {
                    continue;
                }
                return false;
            }
            Err(panic_err) => {
                finalizer.complete_panic(panic_err, is_local);
                return true;
            }
        }
    }
}

// --- 任务引用基础结构 (Direct Implementation) ---

pub struct LocalTaskRef {
    header: NonNull<GenericTaskHeader<LocalStorage>>,
}

impl Copy for LocalTaskRef {}
impl Clone for LocalTaskRef {
    fn clone(&self) -> Self {
        *self
    }
}

impl TaskHandleRef for LocalTaskRef {
    type Storage = LocalStorage;

    #[inline]
    fn header(&self) -> &GenericTaskHeader<LocalStorage> {
        unsafe { self.header.as_ref() }
    }

    #[inline]
    unsafe fn from_header(header: *const GenericTaskHeader<LocalStorage>) -> Self {
        Self {
            header: unsafe { NonNull::new_unchecked(header as *mut _) },
        }
    }

    #[inline]
    fn poll_task(&self, worker_id: usize) -> bool {
        let header = unsafe { self.header.as_ref() };
        unsafe { header.poll(worker_id) }
    }

    #[inline]
    unsafe fn from_concrete<U>(ptr: *const U) -> Self
    where
        U: RawTask<Storage = LocalStorage>,
    {
        Self {
            header: unsafe { NonNull::from((&*ptr).header()) },
        }
    }
}

pub struct SendTaskRef {
    header: NonNull<GenericTaskHeader<AtomicStorage>>,
}

impl Copy for SendTaskRef {}
impl Clone for SendTaskRef {
    fn clone(&self) -> Self {
        *self
    }
}

impl TaskHandleRef for SendTaskRef {
    type Storage = AtomicStorage;

    #[inline]
    fn header(&self) -> &GenericTaskHeader<AtomicStorage> {
        unsafe { self.header.as_ref() }
    }

    #[inline]
    unsafe fn from_header(header: *const GenericTaskHeader<AtomicStorage>) -> Self {
        Self {
            header: unsafe { NonNull::new_unchecked(header as *mut _) },
        }
    }

    #[inline]
    fn poll_task(&self, worker_id: usize) -> bool {
        let header = unsafe { self.header.as_ref() };
        unsafe { header.poll(worker_id) }
    }

    #[inline]
    unsafe fn from_concrete<U>(ptr: *const U) -> Self
    where
        U: RawTask<Storage = AtomicStorage>,
    {
        Self {
            header: unsafe { NonNull::from((&*ptr).header()) },
        }
    }
}

unsafe impl Send for SendTaskRef {}
unsafe impl Sync for SendTaskRef {}

pub trait TaskJoinGate<T> {
    fn take_result_erased(&self) -> Option<Result<T, TaskError>>;
}

impl<T, S: Task<T>> TaskJoinGate<T> for S {
    #[inline]
    fn take_result_erased(&self) -> Option<Result<T, TaskError>> {
        self.take_result()
    }
}

// --- 实用工具与宏 (Public API) ---

pub fn yield_now() -> YieldNow {
    YieldNow(false)
}

pub struct YieldNow(pub bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if cx.is_cancelled() {
            return Poll::Ready(());
        }
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[macro_export]
macro_rules! task_local {
    ($name:ident, $future_expr:expr) => {
        let mut __fut = $future_expr;
        let mut __fut = unsafe { std::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::task::LocalTaskNode::new(__fut);
    };
}
#[macro_export]
macro_rules! task {
    ($name:ident, $future_expr:expr) => {
        let mut __fut = $future_expr;
        let mut __fut = unsafe { std::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::task::SendTaskNode::new(__fut);
    };
}
