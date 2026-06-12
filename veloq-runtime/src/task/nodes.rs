use crate::task::{
    GenericTaskHeader, INTRUSIVE_WAKER_VTABLE, LOCAL_INTRUSIVE_WAKER_VTABLE, LocalTaskRef, RawTask,
    SendTaskRef, Task, TaskError, TaskHandleRef, TaskResultSetter, TaskVTable, poll_task_internal,
};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt, Storage, ThreadSafeStorage};
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, RawWakerVTable};

const STATUS_RUNNING: usize = 0;
const STATUS_DONE: usize = 1;
const STATUS_EMPTY: usize = 2;

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

#[repr(C)]
pub struct GenericTaskNode<S: TaskStorage, T, F> {
    pub(crate) header: GenericTaskHeader<S>,
    pub(crate) status: S::Usize,
    pub(crate) future: UnsafeCell<Option<F>>,
    pub(crate) result: UnsafeCell<Option<Result<T, TaskError>>>,
}

unsafe impl<S: TaskStorage, T, F> Send for GenericTaskNode<S, T, F>
where
    S: ThreadSafeStorage,
    S: TaskBounds<T, F>,
    F: Send,
    T: Send,
{
}

unsafe impl<S: TaskStorage, T, F> Sync for GenericTaskNode<S, T, F>
where
    S: ThreadSafeStorage,
    S: TaskBounds<T, F>,
    F: Send,
    T: Send,
{
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
            status: S::Usize::new(STATUS_RUNNING),
            future: UnsafeCell::new(Some(future)),
            result: UnsafeCell::new(None),
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
        unsafe {
            *self.result.get() = Some(res);
            self.status.store(STATUS_DONE, Ordering::Release);
            *self.future.get() = None;
        }
    }
}

impl<S: TaskStorage, T, F> RawTask for GenericTaskNode<S, T, F>
where
    S: TaskBounds<T, F>,
    F: Future<Output = T>,
{
    type Storage = S;

    fn poll_raw(&self, _worker_id: usize) -> bool {
        let waker = self.header.create_waker(S::WAKER_VTABLE);
        let mut cx = Context::from_waker(&waker);
        self.poll_task(&mut cx)
    }

    fn header(&self) -> &GenericTaskHeader<Self::Storage> {
        &self.header
    }
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
                if self.status.load(Ordering::Acquire) == STATUS_RUNNING {
                    let fut_opt = unsafe { &mut *self.future.get() };
                    if let Some(f) = fut_opt {
                        unsafe { Pin::new_unchecked(f) }.poll(cx)
                    } else {
                        Poll::Pending
                    }
                } else {
                    Poll::Pending
                }
            },
            S::IS_LOCAL,
        )
    }

    fn take_result(&self) -> Option<Result<T, TaskError>> {
        if self
            .status
            .compare_exchange(
                STATUS_DONE,
                STATUS_EMPTY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            unsafe {
                let res = &mut *self.result.get();
                res.take()
            }
        } else {
            None
        }
    }
}

/// 栈上本地任务：future 本身不进行 any 堆分配。
pub type LocalTaskNode<'future, T, F> = GenericTaskNode<LocalStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的本地任务。
pub type LocalBoxedTaskNode<T, F> = GenericTaskNode<LocalStorage, T, F>;

/// 栈上 Send 任务：future 固定在调用栈里，不进行堆分配。
pub type SendTaskNode<'future, T, F> = GenericTaskNode<AtomicStorage, T, Pin<&'future mut F>>;

/// 堆上/拥有所有权的 Send 任务。
pub type SendBoxedTaskNode<T, F> = GenericTaskNode<AtomicStorage, T, F>;
