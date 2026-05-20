use crate::runtime::RuntimeSharedBase;
use crate::task::scope::{OpaqueScope, ScopeCompletionRef, ScopeVTable};
use crate::task::{IntoAnyScope, SendTaskRef};
use crate::utils::storage::{StateInt, StateLock, StateOptionPtr, Storage};
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{RawWaker, RawWakerVTable, Waker};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

pub const STATE_COMPLETED: usize = 1 << 0;
pub const STATE_QUEUED: usize = 1 << 1;
pub const STATE_READY: usize = 1 << 2;
pub const STATE_CANCELLED: usize = 1 << 3;
pub const STATE_POLLING: usize = 1 << 4;
pub const STATE_WOKEN: usize = 1 << 5;
pub const STATE_PINNED: usize = 1 << 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    Proceed,
    Yield,
    Complete,
}

pub struct TaskVTable<S: Storage> {
    pub wake: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
    pub wake_by_ref: unsafe fn(data: &GenericTaskHeader<S>),
    pub poll: unsafe fn(data: &GenericTaskHeader<S>, worker_id: usize) -> bool,
    pub drop: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
}

pub struct GenericWakerNode<S: Storage> {
    pub(crate) waker: Waker,
    pub(crate) link: Link,
    pub(crate) _marker: std::marker::PhantomData<S>,
}

intrusive_adapter!(pub WakerAdapter<S> = GenericWakerNode<S> { link: Link } where S: Storage);

pub struct GenericTaskHeader<'ctx, S: Storage> {
    state: S::Usize,
    ref_count: S::Usize,
    wakers: S::Lock<LinkedList<WakerAdapter<S>>>,
    scope_ptr: S::OptionPtr<OpaqueScope>,
    scope_vtable: S::OptionPtr<ScopeVTable<S>>,
    runtime: &'ctx RuntimeSharedBase<'ctx>,
    worker_id: S::Usize,
    injector_next: S::OptionPtr<GenericTaskHeader<'ctx, S>>,
    vtable: &'static TaskVTable<S>,
}

impl<'ctx, S: Storage> GenericTaskHeader<'ctx, S> {
    pub fn new(
        vtable: &'static TaskVTable<S>,
        runtime: &'ctx RuntimeSharedBase<'ctx>,
        worker_id: usize,
    ) -> Self {
        Self {
            state: S::Usize::new(0),
            ref_count: S::Usize::new(1),
            wakers: S::Lock::new(LinkedList::new(WakerAdapter::<S>::new())),
            scope_ptr: S::OptionPtr::new(None),
            scope_vtable: S::OptionPtr::new(None),
            runtime,
            worker_id: S::Usize::new(worker_id),
            injector_next: S::OptionPtr::new(None),
            vtable,
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_COMPLETED != 0
    }

    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_PINNED != 0
    }

    #[inline]
    pub fn set_pinned(&self) {
        self.state.fetch_or(STATE_PINNED, Ordering::Release);
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        if self.state.load(Ordering::Acquire) & STATE_CANCELLED != 0 {
            return true;
        }
        if let Some(ptr) = self.scope_ptr.load(Ordering::Acquire)
            && let Some(vtable_ptr) = self.scope_vtable.load(Ordering::Acquire)
        {
            let scope_ref =
                unsafe { ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref()) };
            let cancelled = scope_ref.is_cancelled();
            std::mem::forget(scope_ref);
            return cancelled;
        }
        false
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
                self.ref_count.fetch_add(1, Ordering::Release);
                return true;
            }
        }
    }

    #[inline]
    pub fn clear_queued(&self) -> bool {
        let old_state = self.state.fetch_and(!STATE_QUEUED, Ordering::Release);
        if old_state & STATE_QUEUED != 0 && self.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            return true;
        }
        false
    }

    /// 尝试进入 Poll 状态。
    #[inline]
    pub fn try_enter_poll(&self) -> PollStatus {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & STATE_COMPLETED != 0 {
                return PollStatus::Complete;
            }
            if state & STATE_POLLING != 0 {
                match self.state.compare_exchange_weak(
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
            match self.state.compare_exchange_weak(
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

    /// 退出 Poll 状态并检查是否需要重新进入。
    #[inline]
    pub fn exit_poll_to_pending(&self) -> bool {
        let old_state = self.state.fetch_and(!STATE_POLLING, Ordering::AcqRel);
        if old_state & STATE_WOKEN != 0 {
            self.state.fetch_and(!STATE_WOKEN, Ordering::Release);
            let state = self.state.load(Ordering::Acquire);
            if state & STATE_POLLING == 0
                && self
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

    /// 显式退出 Poll 状态，不检查唤醒标记。
    #[inline]
    pub fn exit_poll(&self) {
        self.state.fetch_and(!STATE_POLLING, Ordering::Release);
    }

    /// # Safety
    ///
    /// The caller must ensure that the `node` remains valid and pinned at its current memory location
    /// until it is either woken or explicitly removed from the task's waker list.
    pub unsafe fn register_completion(&self, node: std::pin::Pin<&mut GenericWakerNode<S>>) {
        if self.is_completed() {
            node.waker.wake_by_ref();
            return;
        }

        let mut wakers = self.wakers.lock();
        if self.is_completed() {
            drop(wakers);
            node.waker.wake_by_ref();
            return;
        }

        unsafe {
            wakers.push_back(node);
        }
    }

    /// 标记任务为完成状态，并通知所有等待完成的 waker。
    pub fn mark_completed_and_notify(&self) {
        let old_state = self
            .state
            .fetch_or(STATE_READY | STATE_COMPLETED, Ordering::AcqRel);
        if old_state & STATE_COMPLETED != 0 {
            return;
        }

        let mut wakers = self.wakers.lock();
        while let Some(node) = wakers.pop_front() {
            node.waker.wake_by_ref();
        }
    }

    #[inline]
    pub fn set_worker_id(&self, worker_id: usize) {
        self.worker_id.store(worker_id, Ordering::Relaxed)
    }

    #[inline]
    pub fn worker_id(&self) -> usize {
        self.worker_id.load(Ordering::Acquire)
    }

    pub fn acknowledge_completion(&self) {
        let ptr = self.scope_ptr.swap(None, Ordering::AcqRel);
        let vtable_ptr = self.scope_vtable.swap(None, Ordering::AcqRel);

        if let (Some(ptr), Some(vtable_ptr)) = (ptr, vtable_ptr) {
            unsafe {
                let scope_ref = ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref());
                scope_ref.task_done();
                drop(scope_ref);
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_READY != 0
    }

    pub fn create_waker(&self, vtable: &'static RawWakerVTable) -> Waker {
        let data = self as *const Self as *const ();
        unsafe { Waker::from_raw(RawWaker::new(data, vtable)) }
    }

    /// 从 RawWaker 的 data 指针安全（带对齐检查）地转换为 NonNull<Self>
    ///
    /// # Safety
    ///
    /// 调用者必须确保 `data` 是由 `create_waker` 生成的有效指针，且指向的对象尚未被释放。
    #[inline]
    pub unsafe fn from_raw_data(data: *const ()) -> NonNull<Self> {
        debug_assert!(!data.is_null());
        debug_assert!((data as usize).is_multiple_of(std::mem::align_of::<Self>()));
        unsafe { NonNull::new_unchecked(data as *mut Self) }
    }

    /// # Safety
    /// The `waker` must have been created by a call to `create_waker` on a `TaskHeader` instance,
    /// and `vtable` must match the vtable used for its creation.
    pub unsafe fn from_waker<'a>(
        waker: &'a Waker,
        vtable: &'static RawWakerVTable,
    ) -> Option<&'a Self> {
        if std::ptr::eq(waker.vtable(), vtable) {
            unsafe { Some(&*(waker.data() as *const Self)) }
        } else {
            None
        }
    }

    #[inline]
    pub fn decrement_ref_count(&self) -> bool {
        self.ref_count.fetch_sub(1, Ordering::AcqRel) == 1
    }

    #[inline]
    pub fn scope_vtable(&self) -> Option<&'static ScopeVTable<S>> {
        self.scope_vtable
            .load(Ordering::Acquire)
            .map(|p| unsafe { &*p.as_ptr() })
    }

    #[inline]
    pub fn set_scope_vtable(&self, vtable: Option<NonNull<ScopeVTable<S>>>) {
        self.scope_vtable.store(vtable, Ordering::Release);
    }

    #[inline]
    pub fn scope_ptr(&self) -> Option<NonNull<OpaqueScope>> {
        self.scope_ptr.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_scope_ptr(&self, ptr: Option<NonNull<OpaqueScope>>) {
        self.scope_ptr.store(ptr, Ordering::Release);
    }

    #[inline]
    pub fn scope_completion_ref(&self) -> Option<ScopeCompletionRef<S>> {
        let ptr = self.scope_ptr.load(Ordering::Acquire)?;
        let vtable_ptr = self.scope_vtable.load(Ordering::Acquire)?;
        unsafe {
            let scope_ref = ScopeCompletionRef::<S>::from_parts(ptr, vtable_ptr.as_ref());
            let cloned = scope_ref.clone();
            std::mem::forget(scope_ref);
            Some(cloned)
        }
    }

    #[inline]
    pub fn notify_runtime_active(&self) {
        self.runtime.idle.event_count.notify();
        self.runtime.wake_worker(self.worker_id());
    }

    #[inline]
    pub(crate) fn next(&self) -> Option<NonNull<GenericTaskHeader<'ctx, S>>> {
        self.injector_next.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set_next(&self, next: Option<NonNull<GenericTaskHeader<'ctx, S>>>) {
        self.injector_next.store(next, Ordering::Release);
    }

    /// 唤醒任务（消耗所有权）。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 的有效非空指针。
    #[inline]
    pub unsafe fn wake(self_ptr: NonNull<Self>) {
        let vtable = unsafe { self_ptr.as_ref().vtable };
        unsafe { (vtable.wake)(self_ptr) };
    }

    /// 通过引用唤醒任务。
    #[inline]
    pub fn wake_by_ref(&self) {
        unsafe { (self.vtable.wake_by_ref)(self) };
    }

    /// 执行任务的 poll。
    ///
    /// # Safety
    /// 调用者必须确保 `self` 处于可被 poll 的正确状态下。
    #[inline]
    pub unsafe fn poll(&self, worker_id: usize) -> bool {
        unsafe { (self.vtable.poll)(self, worker_id) }
    }

    /// 释放任务。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 且未被释放的有效非空指针。
    #[inline]
    pub unsafe fn drop_task(self_ptr: NonNull<Self>) {
        let vtable = unsafe { self_ptr.as_ref().vtable };
        unsafe { (vtable.drop)(self_ptr) };
    }

    /// 入队当前任务。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 的有效非空指针。
    pub unsafe fn enqueue_self(&self, self_ptr: NonNull<Self>)
    where
        S: crate::task::nodes::TaskStorage,
        ScopeCompletionRef<S>: IntoAnyScope,
    {
        if !S::IS_LOCAL && self.is_pinned() {
            let task = unsafe { SendTaskRef::from_header(self_ptr.as_ptr() as *const _) };
            self.runtime.enqueue_pinned(self.worker_id(), task);
            return;
        }
        S::enqueue(self.runtime, self.worker_id(), self_ptr);
    }

    /// 尝试将一个 waker 节点从任务的 waker 列表中移除。
    ///
    /// # Safety
    /// `node` 指向的节点必须是由 `register_completion` 注册的相同节点。
    pub unsafe fn remove_waker(&self, node: NonNull<GenericWakerNode<S>>) {
        if self.is_completed() {
            return;
        }
        let mut wakers = self.wakers.lock();
        if unsafe { node.as_ref().link.is_linked() } {
            unsafe {
                let mut cursor = wakers.cursor_mut_from_ptr(node);
                cursor.remove();
            }
        }
    }
}

pub static INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::AtomicStorage>::from_raw_data(data);
        GenericTaskHeader::wake(header);
    },
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::AtomicStorage>::from_raw_data(data);
        header.as_ref().wake_by_ref();
    },
    |_data| {},
);

pub static LOCAL_INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &LOCAL_INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::LocalStorage>::from_raw_data(data);
        GenericTaskHeader::wake(header);
    },
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::LocalStorage>::from_raw_data(data);
        header.as_ref().wake_by_ref();
    },
    |_data| {},
);
