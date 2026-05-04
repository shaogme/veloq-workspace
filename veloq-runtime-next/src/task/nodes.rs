use crate::task::{
    INTRUSIVE_WAKER_VTABLE, LOCAL_INTRUSIVE_WAKER_VTABLE, LocalTaskHeader, LocalTaskRef, RawTask,
    SendTaskRef, Task, TaskError, TaskHeader, TaskLock, TaskVTable, impl_raw_task_common,
    impl_task_typed_common,
};
use crate::utils::storage::{AtomicStorage, LocalStorage, StateInt};
use std::cell::{RefCell, UnsafeCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;

/// 栈上本地任务：future 本身不进行任何堆分配。
pub struct LocalTaskNode<'scope, 'future, T, F>
where
    F: Future<Output = T> + 'future,
{
    pub(crate) header: LocalTaskHeader,
    pub(crate) future: RefCell<Pin<&'future mut F>>,
    pub(crate) result: RefCell<Option<Result<T, TaskError>>>,
    pub(crate) _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, 'future, T, F> LocalTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    T: 'scope,
    F: Future<Output = T> + 'future,
{
    const VTABLE: TaskVTable = TaskVTable {
        wake: |data| unsafe {
            let header = &*(data as *const LocalTaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_local(worker_id, LocalTaskRef::from_concrete(node));
            }
        },
        wake_by_ref: |data| unsafe {
            let header = &*(data as *const LocalTaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_local(worker_id, LocalTaskRef::from_concrete(node));
            }
        },
        poll: |data, _worker_id| unsafe {
            let node = &*(data as *const Self);
            node.poll_raw(_worker_id)
        },
    };

    pub fn new(future: Pin<&'future mut F>) -> Self {
        Self {
            header: LocalTaskHeader::new(&Self::VTABLE),
            future: RefCell::new(future),
            result: RefCell::new(None),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, 'future, T: 'scope, F> RawTask for LocalTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    F: Future<Output = T> + 'future,
{
    impl_raw_task_common!(true, LocalStorage, &LOCAL_INTRUSIVE_WAKER_VTABLE);
}

impl<'scope, 'future, T: 'scope, F> Task<T> for LocalTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    F: Future<Output = T> + 'future,
{
    impl_task_typed_common!(
        self,
        cx,
        self.future.lock_mut(|f| f.as_mut().poll(cx)),
        true
    );
}

pub struct LocalBoxedTaskNode<'scope, T, F>
where
    F: Future<Output = T> + 'scope,
{
    pub(crate) header: LocalTaskHeader,
    pub(crate) future: RefCell<F>,
    pub(crate) result: RefCell<Option<Result<T, TaskError>>>,
    pub(crate) _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, T: 'scope, F> LocalBoxedTaskNode<'scope, T, F>
where
    F: Future<Output = T> + 'scope,
{
    const VTABLE: TaskVTable = TaskVTable {
        wake: |data| unsafe {
            let header = &*(data as *const LocalTaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_local(worker_id, LocalTaskRef::from_concrete(node));
            }
        },
        wake_by_ref: |data| unsafe {
            let header = &*(data as *const LocalTaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_local(worker_id, LocalTaskRef::from_concrete(node));
            }
        },
        poll: |data, _worker_id| unsafe {
            let node = &*(data as *const Self);
            node.poll_raw(_worker_id)
        },
    };

    pub fn new(future: F) -> Self {
        Self {
            header: LocalTaskHeader::new(&Self::VTABLE),
            future: RefCell::new(future),
            result: RefCell::new(None),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, T: 'scope, F> RawTask for LocalBoxedTaskNode<'scope, T, F>
where
    F: Future<Output = T> + 'scope,
{
    impl_raw_task_common!(true, LocalStorage, &LOCAL_INTRUSIVE_WAKER_VTABLE);
}

impl<'scope, T: 'scope, F> Task<T> for LocalBoxedTaskNode<'scope, T, F>
where
    F: Future<Output = T> + 'scope,
{
    impl_task_typed_common!(
        self,
        cx,
        self.future
            .lock_mut(|f| unsafe { std::pin::Pin::new_unchecked(f) }.poll(cx)),
        true
    );
}

pub struct SendBoxedTaskNode<'scope, T, F>
where
    F: Future<Output = T> + Send + 'scope,
{
    pub(crate) header: TaskHeader,
    pub(crate) future: UnsafeCell<F>,
    pub(crate) result: UnsafeCell<Option<Result<T, TaskError>>>,
    pub(crate) _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, T, F> SendBoxedTaskNode<'scope, T, F>
where
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'scope,
{
    const VTABLE: TaskVTable = TaskVTable {
        wake: |data| unsafe {
            let header = &*(data as *const TaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_send(worker_id, SendTaskRef::from_concrete(node));
            }
        },
        wake_by_ref: |data| unsafe {
            let header = &*(data as *const TaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_send(worker_id, SendTaskRef::from_concrete(node));
            }
        },
        poll: |data, _worker_id| unsafe {
            let node = &*(data as *const Self);
            node.poll_raw(_worker_id)
        },
    };

    pub fn new(future: F) -> Self {
        Self {
            header: TaskHeader::new(&Self::VTABLE),
            future: UnsafeCell::new(future),
            result: UnsafeCell::new(None),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, T: 'scope, F> RawTask for SendBoxedTaskNode<'scope, T, F>
where
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'scope,
{
    impl_raw_task_common!(false, AtomicStorage, &INTRUSIVE_WAKER_VTABLE);
}

impl<'scope, T: 'scope, F> Task<T> for SendBoxedTaskNode<'scope, T, F>
where
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'scope,
{
    impl_task_typed_common!(
        self,
        cx,
        self.future
            .lock_mut(|f| unsafe { std::pin::Pin::new_unchecked(f) }.poll(cx)),
        false
    );
}

/// 栈上 Send 任务：future 固定在调用栈里，不进行堆分配。
pub struct SendTaskNode<'scope, 'future, T, F>
where
    F: Future<Output = T> + Send + 'future,
{
    pub(crate) header: TaskHeader,
    pub(crate) future: UnsafeCell<Pin<&'future mut F>>,
    pub(crate) result: UnsafeCell<Option<Result<T, TaskError>>>,
    pub(crate) _marker: std::marker::PhantomData<&'scope ()>,
}

impl<'scope, 'future, T, F> SendTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'future,
{
    const VTABLE: TaskVTable = TaskVTable {
        wake: |data| unsafe {
            let header = &*(data as *const TaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_send(worker_id, SendTaskRef::from_concrete(node));
            }
        },
        wake_by_ref: |data| unsafe {
            let header = &*(data as *const TaskHeader);
            let node = data as *const Self;
            if let Some(runtime) = header.runtime_shared() {
                let worker_id = header.worker_id.load(Ordering::Acquire);
                runtime.enqueue_send(worker_id, SendTaskRef::from_concrete(node));
            }
        },
        poll: |data, _worker_id| unsafe {
            let node = &*(data as *const Self);
            node.poll_raw(_worker_id)
        },
    };

    pub fn new(future: Pin<&'future mut F>) -> Self {
        Self {
            header: TaskHeader::new(&Self::VTABLE),
            future: UnsafeCell::new(future),
            result: UnsafeCell::new(None),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'scope, 'future, T: Send + 'scope, F> RawTask for SendTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'future,
{
    impl_raw_task_common!(false, AtomicStorage, &INTRUSIVE_WAKER_VTABLE);
}

impl<'scope, 'future, T: Send + 'scope, F> Task<T> for SendTaskNode<'scope, 'future, T, F>
where
    'future: 'scope,
    T: Send + 'scope,
    F: Future<Output = T> + Send + 'future,
{
    impl_task_typed_common!(
        self,
        cx,
        self.future.lock_mut(|f| f.as_mut().poll(cx)),
        false
    );
}
