use crate::runtime::primitives::GenericCancellationToken;
use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, TaskHandleRef};
use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicStorage, LocalStorage, Storage, StrategyId};
use std::any::Any;
use std::marker::PhantomData;
use std::mem::forget;
use std::ptr::NonNull;
use std::task::Waker;

/// 不透明的作用域句柄。
///
/// 该类型作为一个类型擦除的标记，用于在不暴露具体 `Storage` 和 `Ownership` 策略的情况下传递作用域引用。
/// 实际指向的是 `crate::scope::GenericScopeCompletion<S, O>`。
#[repr(C)]
pub struct OpaqueScope {
    _private: [u8; 0],
}
/// 不透明的取消令牌句柄
#[repr(C)]
pub struct OpaqueToken {
    _private: [u8; 0],
}

impl OpaqueScope {
    /// 将不透明指针转换为具体类型的引用。
    ///
    /// # Safety
    ///
    /// 调用者必须确保 `ptr` 确实指向一个 `GenericScopeCompletion<S, O>` 实例，
    /// 且 `S` 和 `O` 与调用处的泛型参数匹配。通常通过 `ScopeVTable` 或 `StrategyId` 进行校验。
    #[inline]
    pub unsafe fn as_concrete<'a, S: Storage, O: Ownership>(
        ptr: NonNull<Self>,
    ) -> &'a GenericScopeCompletion<S, O> {
        unsafe { &*(ptr.as_ptr() as *const GenericScopeCompletion<S, O>) }
    }
}

pub struct ErasedCancellationToken {
    ptr: NonNull<OpaqueToken>,
    s_id: StrategyId,
    o_id: StrategyId,
}

impl ErasedCancellationToken {
    pub fn new<S: Storage, O: Ownership>(token: &GenericCancellationToken<S, O>) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(token as *const _ as *mut OpaqueToken) },
            s_id: S::strategy_id(),
            o_id: O::strategy_id(),
        }
    }

    /// 尝试将擦除类型的令牌向下转换为具体类型
    ///
    /// # Safety
    ///
    /// 调用者必须确保该令牌确实是 `GenericCancellationToken<S, O>` 类型。
    /// 虽然内部有类型 ID 检查，但该函数仍被标记为 unsafe 以提醒调用者注意指针生命周期。
    pub unsafe fn downcast<S: Storage, O: Ownership>(
        &self,
    ) -> Option<&GenericCancellationToken<S, O>> {
        if self.s_id == S::strategy_id() && self.o_id == O::strategy_id() {
            unsafe { Some(&*(self.ptr.as_ptr() as *const GenericCancellationToken<S, O>)) }
        } else {
            None
        }
    }
}

pub struct ScopeVTable<S: Storage> {
    task_done: unsafe fn(NonNull<OpaqueScope>),
    cancel: unsafe fn(NonNull<OpaqueScope>),
    report_panic: unsafe fn(NonNull<OpaqueScope>, Box<dyn Any + Send + 'static>),
    is_cancelled: unsafe fn(NonNull<OpaqueScope>) -> bool,
    try_link_child: unsafe fn(NonNull<OpaqueScope>, &ErasedCancellationToken) -> bool,
    parent: unsafe fn(NonNull<OpaqueScope>) -> Option<AnyScopeCompletionRef>,
    clone: unsafe fn(NonNull<OpaqueScope>) -> ScopeCompletionRef<S>,
    drop: unsafe fn(NonNull<OpaqueScope>),
    register_cancel_waker: unsafe fn(NonNull<OpaqueScope>, &Waker),
    enqueue_local: unsafe fn(NonNull<OpaqueScope>, LocalTaskRef<'static>),
    pop_local: unsafe fn(NonNull<OpaqueScope>) -> Option<LocalTaskRef<'static>>,
    is_local_empty: unsafe fn(NonNull<OpaqueScope>) -> bool,
    _marker: PhantomData<S>,
}

pub struct ScopeCompletionRef<S: Storage> {
    ptr: NonNull<OpaqueScope>,
    vtable: &'static ScopeVTable<S>,
}

unsafe impl<S: Storage> Send for ScopeCompletionRef<S> {}
unsafe impl<S: Storage> Sync for ScopeCompletionRef<S> {}

impl<S: Storage> ScopeCompletionRef<S> {
    #[inline]
    pub fn into_parts(self) -> (NonNull<OpaqueScope>, &'static ScopeVTable<S>) {
        let parts = (self.ptr, self.vtable);
        forget(self);
        parts
    }

    #[inline]
    /// # Safety
    ///
    /// 调用者必须确保 `ptr` 是一个有效的 `OpaqueScope` 指针，且与 `vtable` 匹配。
    pub unsafe fn from_parts(ptr: NonNull<OpaqueScope>, vtable: &'static ScopeVTable<S>) -> Self {
        Self { ptr, vtable }
    }

    pub fn new<O: Ownership>(scope: &O::Shared<GenericScopeCompletion<S, O>>) -> Self {
        let ptr = O::as_ptr(scope);
        unsafe { O::increment_strong_count(ptr) };

        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut OpaqueScope) },
            vtable: &VTableContainer::<S, O>::VTABLE,
        }
    }

    #[inline]
    pub fn task_done(&self) {
        unsafe { (self.vtable.task_done)(self.ptr) };
    }

    #[inline]
    pub fn cancel(&self) {
        unsafe { (self.vtable.cancel)(self.ptr) };
    }

    #[inline]
    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        unsafe { (self.vtable.report_panic)(self.ptr, payload) };
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        unsafe { (self.vtable.try_link_child)(self.ptr, child_token) }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        unsafe { (self.vtable.is_cancelled)(self.ptr) }
    }

    #[inline]
    pub fn parent(&self) -> Option<AnyScopeCompletionRef> {
        unsafe { (self.vtable.parent)(self.ptr) }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        unsafe { (self.vtable.register_cancel_waker)(self.ptr, waker) }
    }

    #[inline]
    pub fn pop_local(&self) -> Option<LocalTaskRef<'static>> {
        unsafe { (self.vtable.pop_local)(self.ptr) }
    }

    #[inline]
    pub fn is_local_empty(&self) -> bool {
        unsafe { (self.vtable.is_local_empty)(self.ptr) }
    }

    #[inline]
    pub fn enqueue_local(&self, task: LocalTaskRef<'static>) {
        unsafe { (self.vtable.enqueue_local)(self.ptr, task) }
    }
}

impl<S: Storage> Clone for ScopeCompletionRef<S> {
    fn clone(&self) -> Self {
        unsafe { (self.vtable.clone)(self.ptr) }
    }
}

impl<S: Storage> Drop for ScopeCompletionRef<S> {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.ptr) }
    }
}

struct VTableContainer<S: Storage, O: Ownership>(PhantomData<(S, O)>);

impl<S: Storage, O: Ownership> VTableContainer<S, O> {
    const VTABLE: ScopeVTable<S> = ScopeVTable::<S> {
        task_done: |ptr| unsafe {
            OpaqueScope::as_concrete::<S, O>(ptr).task_done();
        },
        cancel: |ptr| unsafe {
            OpaqueScope::as_concrete::<S, O>(ptr).cancel();
        },
        report_panic: |ptr, payload| unsafe {
            OpaqueScope::as_concrete::<S, O>(ptr).report_panic(payload);
        },
        is_cancelled: |ptr| unsafe { OpaqueScope::as_concrete::<S, O>(ptr).is_cancelled() },
        try_link_child: |ptr, child_token| unsafe {
            if child_token.s_id != S::strategy_id() || child_token.o_id != O::strategy_id() {
                return false;
            }
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope
                .cancel_token()
                .try_link_child_raw(child_token.ptr.as_ptr());
            true
        },
        parent: |ptr| unsafe {
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope.parent().clone()
        },
        clone: |ptr| unsafe {
            O::increment_strong_count(ptr.as_ptr() as *const GenericScopeCompletion<S, O>);
            ScopeCompletionRef::<S> {
                ptr,
                vtable: &VTableContainer::<S, O>::VTABLE,
            }
        },
        drop: |ptr| unsafe {
            O::decrement_strong_count(ptr.as_ptr() as *const GenericScopeCompletion<S, O>);
        },
        register_cancel_waker: |ptr, waker| unsafe {
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope.cancel_token().register_waker(waker);
        },
        enqueue_local: |ptr, task| unsafe {
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope.enqueue_local_task(task);
            let header = task.header();
            header.notify_runtime_active();
        },
        pop_local: |ptr| unsafe {
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope.pop_local_task()
        },
        is_local_empty: |ptr| unsafe {
            let scope = OpaqueScope::as_concrete::<S, O>(ptr);
            scope.is_local_queue_empty()
        },
        _marker: PhantomData,
    };
}

#[derive(Clone)]
pub enum AnyScopeCompletionRef {
    Local(ScopeCompletionRef<LocalStorage>),
    Send(ScopeCompletionRef<AtomicStorage>),
}

impl AnyScopeCompletionRef {
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Local(s) => s.is_cancelled(),
            Self::Send(s) => s.is_cancelled(),
        }
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        match self {
            Self::Local(s) => s.try_link_child(child_token),
            Self::Send(s) => s.try_link_child(child_token),
        }
    }

    #[inline]
    pub fn parent(&self) -> Option<AnyScopeCompletionRef> {
        match self {
            Self::Local(s) => s.parent(),
            Self::Send(s) => s.parent(),
        }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        match self {
            Self::Local(s) => s.register_cancel_waker(waker),
            Self::Send(s) => s.register_cancel_waker(waker),
        }
    }

    #[inline]
    pub fn pop_local(&self) -> Option<LocalTaskRef<'static>> {
        match self {
            Self::Local(s) => s.pop_local(),
            Self::Send(s) => s.pop_local(),
        }
    }

    #[inline]
    pub fn is_local_empty(&self) -> bool {
        match self {
            Self::Local(s) => s.is_local_empty(),
            Self::Send(s) => s.is_local_empty(),
        }
    }

    #[inline]
    pub fn enqueue_local(&self, task: LocalTaskRef<'static>) {
        match self {
            Self::Local(s) => s.enqueue_local(task),
            Self::Send(s) => s.enqueue_local(task),
        }
    }
}
