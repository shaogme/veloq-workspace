use crate::task::{
    GenericTaskHeader, INTRUSIVE_WAKER_VTABLE, LOCAL_INTRUSIVE_WAKER_VTABLE, LocalTaskRef, RawTask,
    SendTaskRef, Task, TaskError, TaskLock, TaskResultSetter, TaskVTable, impl_raw_task_common,
    poll_task_internal,
};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateLock, Storage};
use std::future::Future;
use std::mem::replace;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, RawWakerVTable};

/// 任务存储特性，用于统一本地和发送任务的存储行为。
pub trait TaskStorage: Storage + Sized {
    const IS_LOCAL: bool;
    const WAKER_VTABLE: &'static RawWakerVTable;
    fn enqueue(
        runtime: &crate::runtime::RuntimeSharedBase,
        worker_id: usize,
        data: NonNull<GenericTaskHeader<Self>>,
    );
}

impl TaskStorage for LocalStorage {
    const IS_LOCAL: bool = true;
    const WAKER_VTABLE: &'static RawWakerVTable = &LOCAL_INTRUSIVE_WAKER_VTABLE;
    fn enqueue(
        runtime: &crate::runtime::RuntimeSharedBase,
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
        runtime: &crate::runtime::RuntimeSharedBase,
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
pub struct GenericTaskNode<S: TaskStorage, T, F> {
    pub(crate) header: GenericTaskHeader<S>,
    pub(crate) state: S::Lock<TaskState<T, F>>,
}

impl<S: TaskStorage, T, F> GenericTaskNode<S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
{
    /// 优化 VTable 的定义，确保其作为静态引用在编译期完全内联。
    const VTABLE: &'static TaskVTable<S> = &TaskVTable {
        wake: |data| unsafe {
            data.as_ref().enqueue_self(data);
        },
        wake_by_ref: |header| unsafe {
            header.enqueue_self(NonNull::from(header));
        },
        poll: |header, worker_id| unsafe {
            let raw_ptr = header as *const GenericTaskHeader<S> as *const Self;
            (*raw_ptr).poll_raw(worker_id)
        },
        drop: |_| {},
    };

    pub fn new(future: F) -> Self {
        Self {
            header: GenericTaskHeader::new_placeholder(Self::VTABLE),
            state: S::Lock::new(TaskState::Running(future)),
        }
    }
}

impl<S: TaskStorage, T, F> TaskResultSetter<T> for GenericTaskNode<S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
{
    #[inline]
    fn set_result(&self, res: Result<T, TaskError>) {
        self.state.lock_mut(|s| *s = TaskState::Done(res));
    }
}

impl<S: TaskStorage, T, F> RawTask for GenericTaskNode<S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
{
    impl_raw_task_common!(S::IS_LOCAL, S, S::WAKER_VTABLE);
}

impl<S: TaskStorage, T, F> Task<T> for GenericTaskNode<S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
{
    fn poll_task(&self, cx: &mut Context<'_>) -> bool {
        poll_task_internal(
            self.header(),
            self,
            cx,
            |cx| {
                self.state.lock_mut(|s| {
                    if let TaskState::Running(f) = s {
                        unsafe { Pin::new_unchecked(f) }.poll(cx)
                      } else {
                        Poll::Pending
                    }
                })
            },
            S::IS_LOCAL,
        )
    }

    fn take_result(&self) -> Option<Result<T, TaskError>> {
        self.state.lock_mut(|s| {
            if let TaskState::Done(_) = s
                && let TaskState::Done(res) = replace(s, TaskState::Empty)
            {
                return Some(res);
            }
            None
        })
    }
}

/// 栈上本地任务：future 本身不进行 any 堆分配。
pub type LocalTaskNode<'future, T, F> =
    GenericTaskNode<LocalStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的本地任务。
pub type LocalBoxedTaskNode<T, F> = GenericTaskNode<LocalStorage, T, F>;

/// 栈上 Send 任务：future 固定在调用栈里，不进行堆分配。
pub type SendTaskNode<'future, T, F> =
    GenericTaskNode<AtomicStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的 Send 任务。
pub type SendBoxedTaskNode<T, F> = GenericTaskNode<AtomicStorage, T, F>;
