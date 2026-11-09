mod arena;
mod header;
mod nodes;
mod scope;
mod wake;

pub(crate) use arena::TaskLease;
pub use arena::{Arena, ArenaAllocation, GenericArena, ManagedAllocation};
pub use header::{GenericTaskHeader, PollStatus};
pub(crate) use header::{GenericWakerNode, INTRUSIVE_WAKER_VTABLE, LOCAL_INTRUSIVE_WAKER_VTABLE};
pub(crate) use nodes::{DeferredFuture, GenericTaskNode, TaskBounds, TaskStorage};
pub use nodes::{LocalBoxedTaskNode, LocalTaskNode, SendBoxedTaskNode, SendTaskNode};
pub(crate) use scope::{CancellationWaiter, CancellationWaiterAdapter};
pub use scope::{ErasedCancellationToken, OpaqueScope, OpaqueToken, RawScope, ScopeRef};
pub(crate) use wake::{LocalWakeHeaderGuard, TaskWakeToken};

use crate::{
    error::Result as RuntimeResult,
    runtime::{CancelWaiterLinkResult, EventCount, Unparker},
};
use crossbeam_queue::SegQueue;
use veloq_std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    future::Future,
    marker::PhantomData,
    panic::{AssertUnwindSafe, Panic, PanicPayload},
    pin::Pin,
    ptr::NonNull,
    sync::NativeArc as Arc,
    task::{Context, Poll, Waker},
};
use veloq_storage::{AtomicStorage, LocalStorage, StateLock, Storage};

pub type TaskHeader = GenericTaskHeader<AtomicStorage>;
pub type LocalTaskHeader = GenericTaskHeader<LocalStorage>;

pub struct TaskVTable<S: Storage> {
    pub wake: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
    pub wake_by_ref: unsafe fn(data: &GenericTaskHeader<S>),
    pub poll: unsafe fn(data: &GenericTaskHeader<S>, worker_id: usize) -> RuntimeResult<bool>,
    pub drop: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
    pub drop_after_poll: bool,
}

pub(crate) struct LocalWakeTarget {
    requests: SegQueue<Arc<TaskWakeToken<LocalStorage>>>,
    unparker: Unparker,
    event_count: Arc<EventCount>,
    worker_id: usize,
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
        self.event_count.notify();
        if self.unparker.unpark().is_err() {
            // Wake failures are stored in the runtime's fatal error channel.
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

#[derive(Debug)]
pub struct AnySendScopeRef(ScopeRef<AtomicStorage>);

#[derive(Debug)]
pub enum AnyScopeRef {
    Local(ScopeRef<LocalStorage>),
    Send(AnySendScopeRef),
}

unsafe impl Send for AnySendScopeRef {}

unsafe impl Sync for AnySendScopeRef {}

impl Clone for AnySendScopeRef {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl AnySendScopeRef {
    pub fn new(scope: ScopeRef<AtomicStorage>) -> Self {
        Self(scope)
    }

    pub fn as_scope_ref(&self) -> &ScopeRef<AtomicStorage> {
        &self.0
    }

    pub fn into_any(self) -> AnyScopeRef {
        AnyScopeRef::Send(self)
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        unsafe { self.0.as_ref().try_link_child(child_token) }
    }

    pub(crate) fn parent_send(&self) -> Option<Self> {
        self.0.parent().and_then(|parent| parent.as_send())
    }

    pub(crate) unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<CancellationWaiter>,
        waker: &Waker,
    ) -> CancelWaiterLinkResult {
        unsafe { self.0.link_cancel_waiter(waiter, waker) }
    }

    pub(crate) unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<CancellationWaiter>) {
        unsafe { self.0.unlink_cancel_waiter(waiter) }
    }

    pub fn report_panic(&self, payload: PanicPayload) {
        self.0.report_panic(payload);
    }
}

impl Clone for AnyScopeRef {
    fn clone(&self) -> Self {
        match self {
            Self::Local(scope) => Self::Local(scope.clone()),
            Self::Send(scope) => Self::Send(scope.clone()),
        }
    }
}

impl AnyScopeRef {
    pub fn as_send(&self) -> Option<AnySendScopeRef> {
        match self {
            Self::Local(_) => None,
            Self::Send(scope) => Some(scope.clone()),
        }
    }

    pub fn into_send(self) -> Option<AnySendScopeRef> {
        match self {
            Self::Local(_) => None,
            Self::Send(scope) => Some(scope),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Local(scope) => unsafe { scope.as_ref().is_cancelled() },
            Self::Send(scope) => scope.is_cancelled(),
        }
    }

    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        match self {
            Self::Local(scope) => unsafe { scope.as_ref().try_link_child(child_token) },
            Self::Send(scope) => scope.try_link_child(child_token),
        }
    }

    pub fn report_panic(&self, payload: PanicPayload) {
        match self {
            Self::Local(scope) => scope.report_panic(payload),
            Self::Send(scope) => scope.report_panic(payload),
        }
    }
}

pub trait ScopeParent: Clone + 'static {
    fn from_any(parent: Option<AnyScopeRef>) -> Self;

    fn as_any(&self) -> Option<AnyScopeRef>;

    fn as_send(&self) -> Option<AnySendScopeRef>;

    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct LocalScopeParent(Option<AnyScopeRef>);

impl ScopeParent for LocalScopeParent {
    fn from_any(parent: Option<AnyScopeRef>) -> Self {
        Self(parent)
    }

    fn as_any(&self) -> Option<AnyScopeRef> {
        self.0.clone()
    }

    fn as_send(&self) -> Option<AnySendScopeRef> {
        self.0.as_ref().and_then(AnyScopeRef::as_send)
    }

    fn is_cancelled(&self) -> bool {
        self.0.as_ref().is_some_and(AnyScopeRef::is_cancelled)
    }
}

#[derive(Debug, Clone)]
pub struct ThreadSafeScopeParent(Option<AnySendScopeRef>);

impl ScopeParent for ThreadSafeScopeParent {
    fn from_any(parent: Option<AnyScopeRef>) -> Self {
        Self(parent.and_then(AnyScopeRef::into_send))
    }

    fn as_any(&self) -> Option<AnyScopeRef> {
        self.0.clone().map(AnySendScopeRef::into_any)
    }

    fn as_send(&self) -> Option<AnySendScopeRef> {
        self.0.clone()
    }

    fn is_cancelled(&self) -> bool {
        self.0.as_ref().is_some_and(AnySendScopeRef::is_cancelled)
    }
}

pub trait ScopeStorage: Storage {
    type Parent: ScopeParent;
}

impl ScopeStorage for LocalStorage {
    type Parent = LocalScopeParent;
}

impl ScopeStorage for AtomicStorage {
    type Parent = ThreadSafeScopeParent;
}

// --- 任务错误与结果扩展 ---
pub enum TaskError {
    /// 任务在执行过程中发生了 Panic
    Panic,
    /// 任务因作用域被取消而终止
    Cancelled,
}

impl Debug for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
                LocalTaskHeader::local_from_waker(self.waker(), &LOCAL_INTRUSIVE_WAKER_VTABLE)
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
                LocalTaskHeader::local_from_waker(self.waker(), &LOCAL_INTRUSIVE_WAKER_VTABLE)
            {
                return Some(h.scope_completion_ref().into_any());
            }
            None
        }
    }
}

pub trait RawTask {
    type Storage: Storage;

    fn poll_raw(&self, worker_id: usize) -> RuntimeResult<bool>;

    fn header(&self) -> &GenericTaskHeader<Self::Storage>;
}

pub trait TaskHandleRef: Copy {
    type Storage: Storage;

    fn header(&self) -> &GenericTaskHeader<Self::Storage>;

    /// # Safety
    /// The `header` pointer must be a valid pointer to a `GenericTaskHeader`.
    unsafe fn from_header(header: *const GenericTaskHeader<Self::Storage>) -> Self;

    /// Polls the task through the handle.
    fn poll_task(&self, worker_id: usize) -> RuntimeResult<bool>;

    /// # Safety
    /// The `ptr` must be a valid pointer to a task node implementing `RawTask` with the correct storage.
    unsafe fn from_concrete<U>(ptr: *const U) -> Self
    where
        U: RawTask<Storage = Self::Storage>;
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

pub trait TaskResultSetter<T> {
    fn set_result(&self, res: Result<T, TaskError>);
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
            if self.header.is_result_ready() {
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

    pub(crate) fn poll_task_internal<T, R, F>(
        header: &GenericTaskHeader<S>,
        result_setter: &R,
        cx: &mut Context<'_>,
        poll_fn: F,
        is_local: bool,
    ) -> bool
    where
        R: TaskResultSetter<T>,
        F: FnMut(&mut Context<'_>) -> Poll<T>,
    {
        poll_task_internal(header, result_setter, cx, poll_fn, is_local)
    }
}

pub struct TaskFinalizer<'a, T, R, S: Storage>
where
    R: TaskResultSetter<T>,
{
    header: &'a GenericTaskHeader<S>,
    result_setter: &'a R,
    marker: PhantomData<T>,
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
            marker: PhantomData,
        }
    }

    pub fn complete(&self, res: Result<T, TaskError>, is_local: bool) {
        self.result_setter.set_result(res);
        self.finalize(is_local);
    }

    pub fn complete_panic(&self, panic_err: PanicPayload, is_local: bool) {
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
        // Publish the result before closing the wake token so a concurrent waker can
        // still observe a live header. This is notification only; it is not a reclaim
        // permission.
        self.header.publish_result_and_notify();

        if !is_local {
            self.header.exit_poll();
        }

        // The execution reference is returned before finalization is published. The
        // finalizer then waits for all active wake callbacks before any joiner may
        // observe RECLAIMABLE.
        self.header.decrement_ref_count();
        self.header.finish_finalization();
    }
}

fn poll_task_internal<T, R, F, S: Storage>(
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

        let res = Panic::catch_unwind(AssertUnwindSafe::new(|| poll_fn(cx)));
        match res {
            Ok(Poll::Ready(val)) => {
                finalizer.complete(Ok(val), is_local);
                return true;
            }
            Ok(Poll::Pending) => {
                // 任务即将离开所有队列：先把自己挂到 scope 的取消队列上，之后取消才有人
                // 唤醒它。挂载失败意味着 scope 已经取消，队列可能已被 drain，只能就地结束。
                // 挂载后仍要复查一次，覆盖「入链」与「取消」交错的窗口。
                if !header.arm_scope_cancel_waiter(cx.waker()) || header.is_cancelled() {
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
    fn poll_task(&self, worker_id: usize) -> RuntimeResult<bool> {
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
    fn poll_task(&self, worker_id: usize) -> RuntimeResult<bool> {
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
pub struct YieldNow(bool);

impl YieldNow {
    pub(crate) fn new() -> Self {
        Self(false)
    }
}

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
        let mut __fut = unsafe { core::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::LocalTaskNode::new(__fut);
    };
}
#[macro_export]
macro_rules! task {
    ($name:ident, $future_expr:expr) => {
        let mut __fut = $future_expr;
        let mut __fut = unsafe { core::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::SendTaskNode::new(__fut);
    };
}
