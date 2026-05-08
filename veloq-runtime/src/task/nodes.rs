use crate::task::{
    GenericTaskHeader, INTRUSIVE_WAKER_VTABLE, IntoAnyScope, LOCAL_INTRUSIVE_WAKER_VTABLE,
    LocalTaskRef, RawTask, ScopeCompletionRef, SendTaskRef, Task, TaskError, TaskLock,
    TaskResultSetter, TaskVTable, impl_raw_task_common, poll_task_internal,
};
use crate::utils::storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, StateOptionPtr, Storage,
};
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{Context, RawWakerVTable};

/// 任务存储特性，用于统一本地和发送任务的存储行为。
pub trait TaskStorage: Storage + Sized
where
    ScopeCompletionRef<Self>: IntoAnyScope,
{
    const IS_LOCAL: bool;
    const WAKER_VTABLE: &'static RawWakerVTable;
    fn enqueue(
        runtime: &crate::runtime::RuntimeShared,
        worker_id: usize,
        data: NonNull<GenericTaskHeader<Self>>,
    );
}

impl TaskStorage for LocalStorage {
    const IS_LOCAL: bool = true;
    const WAKER_VTABLE: &'static RawWakerVTable = &LOCAL_INTRUSIVE_WAKER_VTABLE;
    fn enqueue(
        runtime: &crate::runtime::RuntimeShared,
        worker_id: usize,
        data: NonNull<GenericTaskHeader<Self>>,
    ) {
        unsafe { runtime.enqueue_local(worker_id, LocalTaskRef::from_header(data.as_ptr())) };
    }
}

impl TaskStorage for AtomicStorage {
    const IS_LOCAL: bool = false;
    const WAKER_VTABLE: &'static RawWakerVTable = &INTRUSIVE_WAKER_VTABLE;
    fn enqueue(
        runtime: &crate::runtime::RuntimeShared,
        worker_id: usize,
        data: NonNull<GenericTaskHeader<Self>>,
    ) {
        unsafe { runtime.enqueue_send(worker_id, SendTaskRef::from_header(data.as_ptr())) };
    }
}

/// 任务约束特性，用于在编译期区分 Local 和 Send 任务的 Bound。
pub trait TaskBounds<T, F> {}
impl<T, F> TaskBounds<T, F> for LocalStorage {}
impl<T, F> TaskBounds<T, F> for AtomicStorage where T: Send {}

/// 任务状态枚举，合并了运行中的 Future 和完成后的 Result。
/// 这种设计减少了锁的层数，并允许 Future 和 Result 共享内存空间。
pub enum TaskState<T, F> {
    Running(F),
    Done(Result<T, TaskError>),
    Empty,
}

#[repr(C)]
pub struct GenericTaskNode<'scope, S: TaskStorage, T, F>
where
    ScopeCompletionRef<S>: IntoAnyScope,
{
    header: GenericTaskHeader<S>,
    state: S::Lock<TaskState<T, F>>,
    _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, S: TaskStorage, T, F> GenericTaskNode<'scope, S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    /// 优化 VTable 的定义，确保其作为静态引用在编译期完全内联。
    const VTABLE: &'static TaskVTable<S> = &TaskVTable {
        wake: |data| unsafe {
            let header = data.as_ref();
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                if !S::IS_LOCAL && header.is_pinned() {
                    let task = SendTaskRef::from_header(data.as_ptr() as *const _);
                    runtime.enqueue_pinned(worker_id, task);
                    return;
                }
                S::enqueue(runtime, worker_id, data);
            }
        },
        wake_by_ref: |data| unsafe {
            let header = data.as_ref();
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                if !S::IS_LOCAL && header.is_pinned() {
                    let task = SendTaskRef::from_header(data.as_ptr() as *const _);
                    runtime.enqueue_pinned(worker_id, task);
                    return;
                }
                S::enqueue(runtime, worker_id, data);
            }
        },
        poll: |data, worker_id| unsafe {
            let node = &*(data.as_ptr() as *const Self);
            node.poll_raw(worker_id)
        },
    };

    pub fn new(future: F) -> Self {
        Self {
            header: GenericTaskHeader::new(Self::VTABLE),
            state: S::Lock::new(TaskState::Running(future)),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, S: TaskStorage, T, F> TaskResultSetter<T> for GenericTaskNode<'scope, S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    #[inline]
    fn set_result(&self, res: Result<T, TaskError>) {
        self.state.lock_mut(|s| *s = TaskState::Done(res));
    }
}

impl<'scope, S: TaskStorage, T, F> RawTask for GenericTaskNode<'scope, S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    impl_raw_task_common!(S::IS_LOCAL, S, S::WAKER_VTABLE);
}

impl<'scope, S: TaskStorage, T, F> Task<T> for GenericTaskNode<'scope, S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    fn poll_task(&self, cx: &mut Context<'_>) -> bool {
        poll_task_internal(
            &self.header,
            self,
            cx,
            |cx| {
                self.state.lock_mut(|s| {
                    if let TaskState::Running(f) = s {
                        unsafe { Pin::new_unchecked(f) }.poll(cx)
                    } else {
                        std::task::Poll::Pending
                    }
                })
            },
            S::IS_LOCAL,
        )
    }

    fn take_result(&self) -> Option<Result<T, TaskError>> {
        self.state.lock_mut(|s| {
            if let TaskState::Done(_) = s
                && let TaskState::Done(res) = std::mem::replace(s, TaskState::Empty)
            {
                return Some(res);
            }
            None
        })
    }

    fn set_scope_completion<
        SS: crate::utils::storage::Storage,
        O: crate::utils::ownership::Ownership,
    >(
        &self,
        scope: Option<
            <O as crate::utils::ownership::Ownership>::Shared<
                crate::scope::GenericScopeCompletion<SS, O>,
            >,
        >,
    ) {
        if let Some(scope) = scope {
            let scope_ref = crate::task::ScopeCompletionRef::new::<O>(&scope);
            let (ptr, vtable) = scope_ref.into_parts();
            self.header.scope_ptr.store(Some(ptr), Ordering::Release);
            self.header.scope_vtable.store(
                Some(NonNull::new(vtable as *const _ as *mut _).unwrap()),
                Ordering::Release,
            );
        } else {
            self.header.scope_ptr.store(None, Ordering::Release);
            self.header.scope_vtable.store(None, Ordering::Release);
        }
    }
}

/// 栈上本地任务：future 本身不进行任何堆分配。
pub type LocalTaskNode<'scope, 'future, T, F> =
    GenericTaskNode<'scope, LocalStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的本地任务。
pub type LocalBoxedTaskNode<'scope, T, F> = GenericTaskNode<'scope, LocalStorage, T, F>;

/// 栈上 Send 任务：future 固定在调用栈里，不进行堆分配。
pub type SendTaskNode<'scope, 'future, T, F> =
    GenericTaskNode<'scope, AtomicStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的 Send 任务。
pub type SendBoxedTaskNode<'scope, T, F> = GenericTaskNode<'scope, AtomicStorage, T, F>;
