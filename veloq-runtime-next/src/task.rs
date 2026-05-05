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
    AnyScopeCompletionRef, CURRENT_SCOPE, ErasedCancellationToken, OpaqueScope, OpaqueToken,
    ScopeCompletionRef, ScopeGuard,
};

use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicStorage, LocalStorage, StateLock, StateOptionPtr, Storage};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
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

pub trait IntoAnyScope {
    fn into_any(self) -> AnyScopeCompletionRef;
}

impl IntoAnyScope for ScopeCompletionRef<LocalStorage> {
    fn into_any(self) -> AnyScopeCompletionRef {
        AnyScopeCompletionRef::Local(self)
    }
}

impl IntoAnyScope for ScopeCompletionRef<AtomicStorage> {
    fn into_any(self) -> AnyScopeCompletionRef {
        AnyScopeCompletionRef::Send(self)
    }
}

pub trait RuntimeContextExt {
    fn is_cancelled(&self) -> bool;
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
            if let Some(scope) = scope::CURRENT_SCOPE.with(|s| s.borrow().clone()) {
                return scope.is_cancelled();
            }
            false
        }
    }
}

pub trait TaskHandleRef: Copy + Send {
    type Storage: Storage;
    fn header(&self) -> &GenericTaskHeader<Self::Storage>;
}

pub trait RawTask {
    type Storage: Storage;
    fn poll_raw(&self, worker_id: usize) -> bool;
    fn header(&self) -> &GenericTaskHeader<Self::Storage>;
}

pub trait Task<T>: RawTask {
    fn poll_task(&self, cx: &mut Context<'_>) -> bool;
    fn take_result(&self) -> Option<Result<T, TaskError>>;
    fn set_scope_completion<SS: Storage, O: Ownership>(
        &self,
        scope: Option<O::Shared<crate::scope::GenericScopeCompletion<SS, O>>>,
    );
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

pub struct TaskFinalizer<'a, T, L, S: Storage>
where
    L: TaskLock<Option<Result<T, TaskError>>>,
{
    header: &'a GenericTaskHeader<S>,
    result: &'a L,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T, L, S: Storage> TaskFinalizer<'a, T, L, S>
where
    L: TaskLock<Option<Result<T, TaskError>>>,
{
    #[inline]
    pub fn new(header: &'a GenericTaskHeader<S>, result: &'a L) -> Self {
        Self {
            header,
            result,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn complete(&self, res: Result<T, TaskError>, is_local: bool) {
        self.result.lock_mut(|r| *r = Some(res));
        self.finalize(is_local);
    }

    pub fn complete_panic(&self, panic_err: Box<dyn Any + Send + 'static>, is_local: bool) {
        let is_cancelled = if let Some(e) = panic_err.downcast_ref::<TaskError>() {
            matches!(e, TaskError::Cancelled)
        } else {
            false
        };

        if let Some(ptr) = self.header.scope_ptr.load(Ordering::Acquire)
            && let Some(vtable_ptr) = self.header.scope_vtable.load(Ordering::Acquire)
            && !is_cancelled
        {
            unsafe {
                let scope_ref = ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref());
                scope_ref.report_panic(panic_err);
                scope_ref.cancel();
                std::mem::forget(scope_ref);
            }
        }

        let error = if is_cancelled {
            TaskError::Cancelled
        } else {
            TaskError::Panic
        };

        self.result.lock_mut(|r| *r = Some(Err(error)));
        self.finalize(is_local);
    }

    fn finalize(&self, is_local: bool) {
        self.header.mark_completed_and_notify();
        self.header.clear_queued();
        let ptr = self.header.scope_ptr.swap(None, Ordering::AcqRel);
        let vtable_ptr = self.header.scope_vtable.swap(None, Ordering::AcqRel);

        if let (Some(ptr), Some(vtable_ptr)) = (ptr, vtable_ptr) {
            unsafe {
                let scope_ref = ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref());
                scope_ref.task_done();
                drop(scope_ref);
            }
        }
        if !is_local {
            self.header.exit_poll();
        }
    }
}

pub(crate) fn poll_task_internal<T, L, F, S: Storage>(
    header: &GenericTaskHeader<S>,
    result: &L,
    cx: &mut Context<'_>,
    mut poll_fn: F,
    is_local: bool,
) -> bool
where
    L: TaskLock<Option<Result<T, TaskError>>>,
    F: FnMut(&mut Context<'_>) -> Poll<T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    let lifecycle = LifecycleManager::new(header);
    let finalizer = TaskFinalizer::new(header, result);

    match lifecycle.enter_poll(is_local) {
        PollStatus::Proceed => {}
        PollStatus::Yield => return false,
        PollStatus::Complete => return true,
    }

    // 设置当前作用域上下文，用于嵌套作用域自动建立父子关系
    let _scope_guard = match (
        header.scope_ptr.load(Ordering::Acquire),
        header.scope_vtable.load(Ordering::Acquire),
    ) {
        (Some(ptr), Some(vtable_ptr)) => {
            let scope_ref =
                unsafe { ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref()) };
            let guard = scope::ScopeGuard::enter(scope_ref.clone().into_any());
            std::mem::forget(scope_ref);
            Some(guard)
        }
        _ => None,
    };

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

// --- 基础设施宏 (Internal) ---

macro_rules! define_task_infrastructure {
    ($ref_name:ident, $storage:ty) => {
        pub struct $ref_name {
            header: *const GenericTaskHeader<$storage>,
        }

        impl Copy for $ref_name {}
        impl Clone for $ref_name {
            fn clone(&self) -> Self {
                *self
            }
        }
        unsafe impl Send for $ref_name {}

        impl $ref_name {
            /// # Safety
            /// The `ptr` must be a valid pointer to a task node implementing `RawTask` with the correct storage.
            pub unsafe fn from_concrete<U>(ptr: *const U) -> Self
            where
                U: RawTask<Storage = $storage>,
            {
                Self {
                    header: unsafe { (&*ptr).header() as *const GenericTaskHeader<$storage> },
                }
            }

            /// # Safety
            /// The `header` pointer must be a valid pointer to a `GenericTaskHeader`.
            pub unsafe fn from_header(header: *const GenericTaskHeader<$storage>) -> Self {
                Self { header }
            }

            pub fn into_local(self) -> LocalTaskRef {
                unsafe { LocalTaskRef::from_header(self.header as *const _) }
            }

            #[inline]
            pub fn poll_task(&self, worker_id: usize) -> bool {
                let header = unsafe { &*self.header };
                unsafe {
                    (header.vtable.poll)(NonNull::new_unchecked(self.header as *mut _), worker_id)
                }
            }
        }

        impl TaskHandleRef for $ref_name {
            type Storage = $storage;
            #[inline]
            fn header(&self) -> &GenericTaskHeader<$storage> {
                unsafe { &*self.header }
            }
        }
    };
}

define_task_infrastructure!(LocalTaskRef, LocalStorage);
define_task_infrastructure!(SendTaskRef, AtomicStorage);

macro_rules! impl_raw_task_common {
    ($is_local:expr, $storage:ty, $vtable:expr) => {
        fn poll_raw(&self, _worker_id: usize) -> bool {
            let waker = self.header.create_waker($vtable);
            let mut cx = $crate::task::Context::from_waker(&waker);
            self.poll_task(&mut cx)
        }
        fn header(&self) -> &$crate::task::GenericTaskHeader<$storage> {
            &self.header
        }
        type Storage = $storage;
    };
}

macro_rules! impl_task_typed_common {
    ($self:ident, $cx:ident, $poll_expr:expr, $is_local:expr) => {
        fn poll_task(&$self, $cx: &mut $crate::task::Context<'_>) -> bool {
            $crate::task::poll_task_internal(
                &$self.header,
                &$self.result,
                $cx,
                |$cx| $poll_expr,
                $is_local,
            )
        }
        fn take_result(&$self) -> Option<Result<T, TaskError>> {
            $self.result.lock_mut(|r| r.take())
        }
        fn set_scope_completion<SS: $crate::utils::storage::Storage, O: $crate::utils::ownership::Ownership>(
            &$self,
            scope: Option<<O as $crate::utils::ownership::Ownership>::Shared<$crate::scope::GenericScopeCompletion<SS, O>>>,
        ) {
            use std::sync::atomic::Ordering;
            if let Some(scope) = scope {
                let scope_ref = $crate::task::ScopeCompletionRef::new::<O>(&scope);
                let (ptr, vtable) = scope_ref.into_parts();
                $self
                    .header
                    .scope_ptr
                    .store(Some(ptr), Ordering::Release);
                $self
                    .header
                    .scope_vtable
                    .store(Some(std::ptr::NonNull::new(vtable as *const _ as *mut _).unwrap()), Ordering::Release);
            } else {
                $self
                    .header
                    .scope_ptr
                    .store(None, Ordering::Release);
                $self
                    .header
                    .scope_vtable
                    .store(None, Ordering::Release);
            }
        }
    };
}

pub trait TaskJoinGate<T> {
    fn take_result_erased(&self) -> Option<Result<T, TaskError>>;
}

impl<T, S: Task<T>> TaskJoinGate<T> for S {
    #[inline]
    fn take_result_erased(&self) -> Option<Result<T, TaskError>> {
        self.take_result()
    }
}

pub(crate) use impl_raw_task_common;
pub(crate) use impl_task_typed_common;

// --- 实用工具与宏 (Public API) ---

pub fn yield_now() -> YieldNow {
    YieldNow(false)
}

pub struct YieldNow(pub bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if cx.is_cancelled() {
            std::panic::panic_any(TaskError::Cancelled);
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
