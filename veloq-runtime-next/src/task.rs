mod arena;
mod nodes;

pub use arena::{Arena, GenericArena};
pub use nodes::{LocalBoxedTaskNode, LocalTaskNode, SendBoxedTaskNode, SendTaskNode};

use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt, StateOptionPtr, Storage};
use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

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

pub const STATE_COMPLETED: usize = 1 << 0;
pub const STATE_QUEUED: usize = 1 << 1;
pub const STATE_READY: usize = 1 << 2;
pub const STATE_CANCELLED: usize = 1 << 3;
pub const STATE_POLLING: usize = 1 << 4;
pub const STATE_WOKEN: usize = 1 << 5;

pub struct TaskVTable {
    pub(crate) wake: unsafe fn(data: *const ()),
    pub(crate) wake_by_ref: unsafe fn(data: *const ()),
    pub(crate) poll: unsafe fn(data: *const (), worker_id: usize) -> bool,
}

pub struct GenericWakerNode<S: Storage> {
    pub(crate) waker: Waker,
    pub(crate) next: S::OptionPtr<GenericWakerNode<S>>,
}

pub type WakerNode = GenericWakerNode<AtomicStorage>;
pub type LocalWakerNode = GenericWakerNode<LocalStorage>;

#[derive(Copy, Clone)]
pub(crate) struct ScopeCompletionRef {
    ptr: NonNull<()>,
    task_done: unsafe fn(*const ()),
    cancel: unsafe fn(*const ()),
    report_panic: unsafe fn(*const (), Box<dyn Any + Send + 'static>),
}

impl ScopeCompletionRef {
    pub fn new<S: Storage, O: Ownership>(
        scope: &O::Shared<crate::scope::GenericScopeCompletion<S, O>>,
    ) -> Self {
        Self {
            ptr: NonNull::new(O::as_ptr(scope) as *mut _)
                .expect("scope completion pointer is null"),
            task_done: |ptr| unsafe {
                (&*(ptr as *const crate::scope::GenericScopeCompletion<S, O>)).task_done();
            },
            cancel: |ptr| unsafe {
                (&*(ptr as *const crate::scope::GenericScopeCompletion<S, O>)).cancel();
            },
            report_panic: |ptr, payload| unsafe {
                (&*(ptr as *const crate::scope::GenericScopeCompletion<S, O>))
                    .report_panic(payload);
            },
        }
    }

    #[inline]
    pub unsafe fn task_done(&self) {
        unsafe { (self.task_done)(self.ptr.as_ptr()) };
    }

    #[inline]
    pub unsafe fn cancel(&self) {
        unsafe { (self.cancel)(self.ptr.as_ptr()) };
    }

    #[inline]
    pub unsafe fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        unsafe { (self.report_panic)(self.ptr.as_ptr(), payload) };
    }
}

pub struct GenericTaskHeader<S: Storage> {
    pub(crate) state: S::Usize,
    pub(crate) waker_head: S::OptionPtr<GenericWakerNode<S>>,
    pub(crate) scope_completion: S::OptionPtr<ScopeCompletionRef>,
    pub(crate) runtime_ptr: S::OptionPtr<crate::runtime::RuntimeShared>,
    pub(crate) worker_id: S::Usize,
    pub(crate) vtable: &'static TaskVTable,
}

pub type TaskHeader = GenericTaskHeader<AtomicStorage>;
pub type LocalTaskHeader = GenericTaskHeader<LocalStorage>;

impl<S: Storage> GenericTaskHeader<S> {
    pub fn new(vtable: &'static TaskVTable) -> Self {
        Self {
            state: S::Usize::new(0),
            waker_head: S::OptionPtr::new(None),
            scope_completion: S::OptionPtr::new(None),
            runtime_ptr: S::OptionPtr::new(None),
            worker_id: S::Usize::new(0),
            vtable,
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_COMPLETED != 0
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_CANCELLED != 0
    }

    #[inline]
    pub fn cancel(&self) {
        self.state.fetch_or(STATE_CANCELLED, Ordering::Release);
    }

    #[inline]
    pub fn try_mark_queued(&self) -> bool {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if state & STATE_QUEUED != 0 || state & STATE_COMPLETED != 0 {
                return false;
            }
            if self
                .state
                .compare_exchange(
                    state,
                    state | STATE_QUEUED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    #[inline]
    pub fn clear_queued(&self) {
        self.state.fetch_and(!STATE_QUEUED, Ordering::Release);
    }

    /// # Safety
    /// The `node` pointer must be a valid pointer to a `GenericWakerNode`.
    pub unsafe fn register_completion(&self, node: *mut GenericWakerNode<S>) {
        loop {
            let head = self.waker_head.load(Ordering::Acquire);
            if self.is_completed() {
                unsafe { (&*node).waker.wake_by_ref() };
                return;
            }
            unsafe { (&*node).next.store(head, Ordering::Relaxed) };
            if self
                .waker_head
                .compare_exchange(
                    head,
                    NonNull::new(node),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn notify_completion(&self) {
        let old_state = self
            .state
            .fetch_or(STATE_READY | STATE_COMPLETED, Ordering::AcqRel);
        if old_state & STATE_COMPLETED != 0 {
            return;
        }
        let mut head = self.waker_head.swap(None, Ordering::AcqRel);
        while let Some(node_ptr) = head {
            let node = unsafe { node_ptr.as_ref() };
            let next = node.next.load(Ordering::Acquire);
            node.waker.wake_by_ref();
            head = next;
        }
    }

    pub fn set_runtime_info(
        &self,
        runtime_ptr: *const crate::runtime::RuntimeShared,
        worker_id: usize,
    ) {
        self.runtime_ptr
            .store(NonNull::new(runtime_ptr as *mut _), Ordering::Release);
        self.worker_id.store(worker_id, Ordering::Release);
    }

    #[inline]
    pub fn runtime_shared(&self) -> Option<&crate::runtime::RuntimeShared> {
        self.runtime_ptr
            .load(Ordering::Acquire)
            .map(|p| unsafe { p.as_ref() })
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_READY != 0
    }

    pub fn create_waker(&self, vtable: &'static RawWakerVTable) -> Waker {
        let data = self as *const Self as *const ();
        unsafe { Waker::from_raw(RawWaker::new(data, vtable)) }
    }

    /// # Safety
    /// The `waker` must have been created by a call to `create_waker` on a `TaskHeader` instance,
    /// and `vtable` must match the vtable used for its creation.
    pub unsafe fn from_waker<'a>(
        waker: &'a Waker,
        vtable: &'static RawWakerVTable,
    ) -> Option<&'a Self> {
        struct RawWakerLayout {
            data: *const (),
            vtable: *const RawWakerVTable,
        }
        let raw = unsafe { &*(waker as *const Waker as *const RawWakerLayout) };
        if std::ptr::eq(raw.vtable, vtable) {
            unsafe { Some(&*(raw.data as *const Self)) }
        } else {
            None
        }
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
            false
        }
    }
}

pub static INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = &*(data as *const TaskHeader);
        (header.vtable.wake)(data);
    },
    |data| unsafe {
        let header = &*(data as *const TaskHeader);
        (header.vtable.wake_by_ref)(data);
    },
    |_data| {},
);

pub static LOCAL_INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &LOCAL_INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = &*(data as *const LocalTaskHeader);
        (header.vtable.wake)(data);
    },
    |data| unsafe {
        let header = &*(data as *const LocalTaskHeader);
        (header.vtable.wake_by_ref)(data);
    },
    |_data| {},
);

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
    fn set_scope_completion<S: Storage, O: Ownership>(
        &self,
        scope: Option<O::Shared<crate::scope::GenericScopeCompletion<S, O>>>,
    );
}

pub trait LocalTask<T>: Task<T, Storage = LocalStorage> {}
impl<T, U: Task<T, Storage = LocalStorage> + ?Sized> LocalTask<T> for U {}

pub trait SendTask<T>: Task<T, Storage = AtomicStorage> + Send {}
impl<T, U: Task<T, Storage = AtomicStorage> + Send + ?Sized> SendTask<T> for U {}

pub trait TaskLock<T> {
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T> TaskLock<T> for RefCell<T> {
    #[inline]
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut *self.borrow_mut())
    }
}

impl<T> TaskLock<T> for std::cell::UnsafeCell<T> {
    #[inline]
    fn lock_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(unsafe { &mut *self.get() })
    }
}

pub enum PollStatus {
    Proceed,
    Yield,
    Complete,
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

        let mut state = self.header.state.load(Ordering::Acquire);
        loop {
            if state & STATE_COMPLETED != 0 {
                return PollStatus::Complete;
            }
            if state & STATE_POLLING != 0 {
                match self.header.state.compare_exchange_weak(
                    state,
                    state | STATE_WOKEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return PollStatus::Yield,
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }
            match self.header.state.compare_exchange_weak(
                state,
                state | STATE_POLLING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return PollStatus::Proceed,
                Err(s) => {
                    state = s;
                    continue;
                }
            }
        }
    }

    pub fn exit_pending(&self, is_local: bool) -> bool {
        if is_local {
            return false;
        }
        let old_state = self
            .header
            .state
            .fetch_and(!STATE_POLLING, Ordering::AcqRel);
        if old_state & STATE_WOKEN != 0 {
            self.header.state.fetch_and(!STATE_WOKEN, Ordering::Release);
            let state = self.header.state.load(Ordering::Acquire);
            if state & STATE_POLLING == 0
                && self
                    .header
                    .state
                    .compare_exchange_weak(
                        state,
                        state | STATE_POLLING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                return true;
            }
        }
        false
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

        if let Some(scope_ptr) = self.header.scope_completion.load(Ordering::Acquire)
            && !is_cancelled
        {
            unsafe {
                scope_ptr.as_ref().report_panic(panic_err);
                scope_ptr.as_ref().cancel();
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
        self.header.notify_completion();
        self.header.clear_queued();
        if let Some(scope_ptr) = self.header.scope_completion.swap(None, Ordering::AcqRel) {
            unsafe {
                scope_ptr.as_ref().task_done();
                drop(Box::from_raw(scope_ptr.as_ptr()));
            }
        }
        if !is_local {
            self.header
                .state
                .fetch_and(!STATE_POLLING, Ordering::Release);
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
{
    let lifecycle = LifecycleManager::new(header);
    let finalizer = TaskFinalizer::new(header, result);

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
            /// The `ptr` must be a valid pointer to an object implementing `RawTask`.
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
                unsafe { (header.vtable.poll)(self.header as *const (), worker_id) }
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
        fn set_scope_completion<S: $crate::utils::storage::Storage, O: $crate::utils::ownership::Ownership>(
            &$self,
            scope: Option<<O as $crate::utils::ownership::Ownership>::Shared<$crate::scope::GenericScopeCompletion<S, O>>>,
        ) {
            use $crate::task::Ordering;
            if let Some(scope) = scope {
                let scope_ref = Box::into_raw(Box::new($crate::task::ScopeCompletionRef::new::<S, O>(&scope)));
                $self
                    .header
                    .scope_completion
                    .store(std::ptr::NonNull::new(scope_ref), Ordering::Release);
            } else {
                $self
                    .header
                    .scope_completion
                    .store(None, Ordering::Release);
            }
        }
    };
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
