use crate::task::{
    GenericTaskHeader, INTRUSIVE_WAKER_VTABLE, IntoAnyScope, LOCAL_INTRUSIVE_WAKER_VTABLE,
    LocalTaskRef, RawTask, ScopeCompletionRef, SendTaskRef, Task, TaskError, TaskLock, TaskVTable,
    impl_raw_task_common, impl_task_typed_common,
};
use crate::utils::storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, StateOptionPtr, Storage,
};
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::RawWakerVTable;

/// 任务存储特性，用于统一本地和发送任务的存储行为。
pub trait TaskStorage: Storage + Sized + 'static
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
impl<T, F> TaskBounds<T, F> for AtomicStorage
where
    T: Send,
    F: Send,
{
}

/// 通用的任务节点实现。
/// 通过 `S: TaskStorage` 统一了 `LocalStorage` (Local) 和 `AtomicStorage` (Send)。
/// 通过 `F` 统一了栈上任务 (Pin<&mut F>) 和堆上任务 (F)。
#[repr(C)]
pub struct GenericTaskNode<'scope, S: TaskStorage, T, F>
where
    ScopeCompletionRef<S>: IntoAnyScope,
{
    header: GenericTaskHeader<S>,
    future: S::Lock<F>,
    result: S::Lock<Option<Result<T, TaskError>>>,
    _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, S: TaskStorage, T, F> GenericTaskNode<'scope, S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
    ScopeCompletionRef<S>: IntoAnyScope,
{
    const VTABLE: TaskVTable<S> = TaskVTable {
        wake: |data| unsafe {
            let header = data.as_ref();
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                S::enqueue(runtime, worker_id, data);
            }
        },
        wake_by_ref: |data| unsafe {
            let header = data.as_ref();
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
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
            header: GenericTaskHeader::new(&Self::VTABLE),
            future: S::Lock::new(future),
            result: S::Lock::new(None),
            _marker: std::marker::PhantomData,
        }
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
    impl_task_typed_common!(
        self,
        cx,
        self.future
            .lock_mut(|f| unsafe { std::pin::Pin::new_unchecked(f) }.poll(cx)),
        S::IS_LOCAL
    );
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
