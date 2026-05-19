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
    AnyScopeCompletionRef, ErasedCancellationToken, OpaqueScope, OpaqueToken, ScopeCompletionRef,
};

use crate::utils::ownership::Ownership;
use crate::utils::storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, StateOptionPtr, Storage,
};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub type TaskHeader<'ctx> = GenericTaskHeader<'ctx, AtomicStorage>;
pub type LocalTaskHeader<'ctx> = GenericTaskHeader<'ctx, LocalStorage>;

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
    fn scope_completion(&self) -> Option<AnyScopeCompletionRef>;
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

    fn scope_completion(&self) -> Option<AnyScopeCompletionRef> {
        unsafe {
            if let Some(h) = TaskHeader::from_waker(self.waker(), &INTRUSIVE_WAKER_VTABLE) {
                let ptr = h.scope_ptr.load(Ordering::Acquire)?;
                let vtable_ptr = h.scope_vtable.load(Ordering::Acquire)?;
                let scope_ref = ScopeCompletionRef::from_parts(ptr, vtable_ptr.as_ref());
                let result = scope_ref.clone();
                std::mem::forget(scope_ref);
                return Some(result.into_any());
            }
            if let Some(h) =
                LocalTaskHeader::from_waker(self.waker(), &LOCAL_INTRUSIVE_WAKER_VTABLE)
            {
                let ptr = h.scope_ptr.load(Ordering::Acquire)?;
                let vtable_ptr = h.scope_vtable.load(Ordering::Acquire)?;
                let scope_ref = ScopeCompletionRef::from_parts(ptr, vtable_ptr.as_ref());
                let result = scope_ref.clone();
                std::mem::forget(scope_ref);
                return Some(result.into_any());
            }
            None
        }
    }
}

pub trait TaskHandleRef<'ctx>: Copy {
    type Storage: Storage;
    fn header(&self) -> &GenericTaskHeader<'ctx, Self::Storage>;
    /// # Safety
    /// The `header` pointer must be a valid pointer to a `GenericTaskHeader`.
    unsafe fn from_header(header: *const GenericTaskHeader<Self::Storage>) -> Self;
}

pub trait RawTask<'ctx> {
    type Storage: Storage;
    fn poll_raw(&self, worker_id: usize) -> bool;
    fn header(&self) -> &GenericTaskHeader<'ctx, Self::Storage>;
}

pub trait Task<'ctx, T>: RawTask<'ctx> {
    fn poll_task(&self, cx: &mut Context<'_>) -> bool;
    fn take_result(&self) -> Option<Result<T, TaskError>>;
    fn set_scope_completion<SS: Storage, O: Ownership>(
        &self,
        scope: Option<O::Shared<crate::scope::GenericScopeCompletion<SS, O>>>,
    );
}

pub trait LocalTask<'ctx, T>: Task<'ctx, T, Storage = LocalStorage> {}
impl<'ctx, T, U: Task<'ctx, T, Storage = LocalStorage> + ?Sized> LocalTask<'ctx, T> for U {}

pub trait SendTask<'ctx, T>: Task<'ctx, T, Storage = AtomicStorage> + Send {}
impl<'ctx, T, U: Task<'ctx, T, Storage = AtomicStorage> + Send + ?Sized> SendTask<'ctx, T> for U {}

pub trait TaskLock<T> {
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T, L: StateLock<T>> TaskLock<T> for L {
    #[inline]
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut *self.lock())
    }
}

pub struct LifecycleManager<'a, 'ctx, S: Storage> {
    header: &'a GenericTaskHeader<'ctx, S>,
}

impl<'a, 'ctx, S: Storage> LifecycleManager<'a, 'ctx, S> {
    #[inline]
    pub fn new(header: &'a GenericTaskHeader<'ctx, S>) -> Self {
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

pub struct TaskFinalizer<'a, 'ctx, T, R, S: Storage>
where
    R: TaskResultSetter<T>,
{
    header: &'a GenericTaskHeader<'ctx, S>,
    result_setter: &'a R,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, 'ctx, T, R, S: Storage> TaskFinalizer<'a, 'ctx, T, R, S>
where
    R: TaskResultSetter<T>,
{
    #[inline]
    pub fn new(header: &'a GenericTaskHeader<'ctx, S>, result_setter: &'a R) -> Self {
        Self {
            header,
            result_setter,
            _marker: std::marker::PhantomData,
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

        self.result_setter.set_result(Err(error));
        self.finalize(is_local);
    }

    fn finalize(&self, is_local: bool) {
        self.header.mark_completed_and_notify();

        let should_acknowledge = self.header.ref_count.fetch_sub(1, Ordering::AcqRel) == 1;

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
    ScopeCompletionRef<S>: IntoAnyScope,
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
        pub struct $ref_name<'ctx> {
            header: NonNull<GenericTaskHeader<'ctx, $storage>>,
        }

        impl<'ctx> Copy for $ref_name<'ctx> {}
        impl<'ctx> Clone for $ref_name<'ctx> {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl<'ctx> $ref_name<'ctx> {
            /// # Safety
            /// The `ptr` must be a valid pointer to a task node implementing `RawTask` with the correct storage.
            pub unsafe fn from_concrete<U>(ptr: *const U) -> Self
            where
                U: RawTask<'ctx, Storage = $storage>,
            {
                Self {
                    header: unsafe { NonNull::from((&*ptr).header()) },
                }
            }

            /// # Safety
            /// The `header` pointer must be a valid pointer to a `GenericTaskHeader`.
            pub unsafe fn from_header(header: *const GenericTaskHeader<$storage>) -> Self {
                Self {
                    header: unsafe { NonNull::new_unchecked(header as *mut _) },
                }
            }

            pub fn into_local(self) -> LocalTaskRef<'ctx> {
                unsafe { LocalTaskRef::from_header(self.header.as_ptr() as *const _) }
            }

            #[inline]
            pub fn poll_task(&self, worker_id: usize) -> bool {
                let header = unsafe { self.header.as_ref() };
                unsafe { (header.vtable.poll)(header, worker_id) }
            }
        }

        impl<'ctx> TaskHandleRef<'ctx> for $ref_name<'ctx> {
            type Storage = $storage;
            #[inline]
            fn header(&self) -> &GenericTaskHeader<'ctx, $storage> {
                unsafe { self.header.as_ref() }
            }
            #[inline]
            unsafe fn from_header(header: *const GenericTaskHeader<$storage>) -> Self {
                Self {
                    header: unsafe { NonNull::new_unchecked(header as *mut _) },
                }
            }
        }
    };
}

define_task_infrastructure!(LocalTaskRef, LocalStorage);
define_task_infrastructure!(SendTaskRef, AtomicStorage);

unsafe impl Send for SendTaskRef<'_> {}

macro_rules! impl_raw_task_common {
    ($is_local:expr, $storage:ty, $vtable:expr, $lt:lifetime) => {
        fn poll_raw(&self, _worker_id: usize) -> bool {
            let waker = self.header.create_waker($vtable);
            let mut cx = $crate::task::Context::from_waker(&waker);
            self.poll_task(&mut cx)
        }
        fn header(&self) -> &$crate::task::GenericTaskHeader<$lt, $storage> {
            unsafe {
                &*(&self.header as *const $crate::task::GenericTaskHeader<'_, $storage>
                    as *const $crate::task::GenericTaskHeader<$lt, $storage>)
            }
        }
        type Storage = $storage;
    };
}

pub trait TaskJoinGate<T> {
    fn take_result_erased(&self) -> Option<Result<T, TaskError>>;
}

impl<'ctx, T, S: Task<'ctx, T>> TaskJoinGate<T> for S {
    #[inline]
    fn take_result_erased(&self) -> Option<Result<T, TaskError>> {
        self.take_result()
    }
}

pub(crate) use impl_raw_task_common;

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
    ($name:ident, $ctx:expr, $future_expr:expr) => {
        let mut __fut = $future_expr;
        let mut __fut = unsafe { std::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::task::LocalTaskNode::new(__fut, &$ctx.shared().base, $ctx.worker_id());
    };
}

#[macro_export]
macro_rules! task {
    ($name:ident, $ctx:expr, $future_expr:expr) => {
        let mut __fut = $future_expr;
        let mut __fut = unsafe { std::pin::Pin::new_unchecked(&mut __fut) };
        let $name = $crate::task::SendTaskNode::new(__fut, &$ctx.shared().base, $ctx.worker_id());
    };
}
