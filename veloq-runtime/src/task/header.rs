use crate::task::scope::{OpaqueScope, ScopeCompletionRef, ScopeVTable};
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
    pub wake_by_ref: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
    pub poll: unsafe fn(data: NonNull<GenericTaskHeader<S>>, worker_id: usize) -> bool,
    pub drop: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
}

pub struct GenericWakerNode<S: Storage> {
    pub(crate) waker: Waker,
    pub(crate) link: Link,
    pub(crate) _marker: std::marker::PhantomData<S>,
}

intrusive_adapter!(pub WakerAdapter<S> = GenericWakerNode<S> { link: Link } where S: Storage);

pub struct GenericTaskHeader<S: Storage> {
    pub(crate) state: S::Usize,
    pub(crate) ref_count: S::Usize,
    pub(crate) wakers: S::Lock<LinkedList<WakerAdapter<S>>>,
    pub(crate) scope_ptr: S::OptionPtr<OpaqueScope>,
    pub(crate) scope_vtable: S::OptionPtr<ScopeVTable<S>>,
    pub(crate) runtime_ptr: S::OptionPtr<crate::runtime::shared::RuntimeSharedBase>,
    pub(crate) worker_id: S::Usize,
    pub(crate) injector_next: S::OptionPtr<GenericTaskHeader<S>>,
    pub vtable: &'static TaskVTable<S>,
}

impl<S: Storage> GenericTaskHeader<S> {
    pub fn new(vtable: &'static TaskVTable<S>) -> Self {
        Self {
            state: S::Usize::new(0),
            ref_count: S::Usize::new(1),
            wakers: S::Lock::new(LinkedList::new(WakerAdapter::<S>::new())),
            scope_ptr: S::OptionPtr::new(None),
            scope_vtable: S::OptionPtr::new(None),
            runtime_ptr: S::OptionPtr::new(None),
            worker_id: S::Usize::new(0),
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
    pub fn set_runtime_info(
        &self,
        runtime: Option<&std::sync::Arc<crate::runtime::shared::RuntimeSharedBase>>,
        worker_id: usize,
    ) {
        if let Some(runtime) = runtime {
            let arc = runtime.clone();
            self.runtime_ptr.store(
                NonNull::new(std::sync::Arc::into_raw(arc) as *mut _),
                Ordering::Release,
            );
        } else {
            self.runtime_ptr.store(None, Ordering::Release);
        }
        self.worker_id.store(worker_id, Ordering::Release);
    }

    #[inline]
    pub fn runtime_shared(&self) -> Option<&crate::runtime::shared::RuntimeSharedBase> {
        self.runtime_ptr
            .load(Ordering::Acquire)
            .map(|p| unsafe { p.as_ref() })
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

    pub fn release_runtime(&self) {
        if let Some(ptr) = self.runtime_ptr.swap(None, Ordering::AcqRel) {
            unsafe {
                let _ = std::sync::Arc::from_raw(ptr.as_ptr());
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

pub static INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::AtomicStorage>::from_raw_data(data);
        (header.as_ref().vtable.wake)(header);
    },
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::AtomicStorage>::from_raw_data(data);
        (header.as_ref().vtable.wake_by_ref)(header);
    },
    |_data| {},
);

pub static LOCAL_INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| RawWaker::new(data, &LOCAL_INTRUSIVE_WAKER_VTABLE),
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::LocalStorage>::from_raw_data(data);
        (header.as_ref().vtable.wake)(header);
    },
    |data| unsafe {
        let header = GenericTaskHeader::<crate::utils::storage::LocalStorage>::from_raw_data(data);
        (header.as_ref().vtable.wake_by_ref)(header);
    },
    |_data| {},
);
