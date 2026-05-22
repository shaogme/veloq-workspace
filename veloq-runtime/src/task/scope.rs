use crate::runtime::primitives::GenericCancellationToken;
use crate::scope::GenericScopeCompletion;
use crate::task::LocalTaskRef;
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
    pub unsafe fn as_concrete<'a, 'scope, S: Storage, O: Ownership>(
        ptr: NonNull<Self>,
    ) -> &'a GenericScopeCompletion<'scope, S, O> {
        unsafe { &*(ptr.as_ptr() as *const GenericScopeCompletion<'scope, S, O>) }
    }
}

pub struct ErasedCancellationToken {
    pub(crate) ptr: NonNull<OpaqueToken>,
    pub(crate) s_id: StrategyId,
    pub(crate) o_id: StrategyId,
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

pub trait RawScope<'scope, S: Storage> {
    fn task_done(&self);
    fn cancel(&self);
    fn report_panic(&self, payload: Box<dyn Any + Send + 'static>);
    fn is_cancelled(&self) -> bool;
    fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool;
    fn parent(&self) -> Option<AnyScopeCompletionRef<'scope>>;
    fn register_cancel_waker(&self, waker: &Waker);
    fn enqueue_local(&self, task: LocalTaskRef<'scope>);
    fn pop_local(&self) -> Option<LocalTaskRef<'scope>>;
    fn is_local_empty(&self) -> bool;
    /// # Safety
    ///
    /// The caller must ensure the returned reference is dropped before the underlying scope is deallocated.
    unsafe fn clone_ref(&self) -> ScopeCompletionRef<'scope, S>;
    /// # Safety
    ///
    /// The caller must ensure the reference is not dropped twice.
    unsafe fn drop_ref(&self);
}

pub struct ScopeCompletionRef<'scope, S: Storage> {
    ptr: NonNull<dyn RawScope<'scope, S> + 'scope>,
}

unsafe impl<'scope, S: Storage> Send for ScopeCompletionRef<'scope, S> {}
unsafe impl<'scope, S: Storage> Sync for ScopeCompletionRef<'scope, S> {}

impl<'scope, S: Storage> ScopeCompletionRef<'scope, S> {
    pub fn dummy() -> Self {
        let ptr: NonNull<dyn RawScope<'scope, S> + 'scope> = if S::strategy_id() == LocalStorage::strategy_id() {
            let local_ptr: NonNull<dyn RawScope<'static, LocalStorage>> = NonNull::from(&DUMMY_LOCAL_SCOPE);
            unsafe {
                std::mem::transmute::<
                    NonNull<dyn RawScope<'static, LocalStorage>>,
                    NonNull<dyn RawScope<'scope, S> + 'scope>,
                >(local_ptr)
            }
        } else if S::strategy_id() == AtomicStorage::strategy_id() {
            let send_ptr: NonNull<dyn RawScope<'static, AtomicStorage>> = NonNull::from(&DUMMY_SEND_SCOPE);
            unsafe {
                std::mem::transmute::<
                    NonNull<dyn RawScope<'static, AtomicStorage>>,
                    NonNull<dyn RawScope<'scope, S> + 'scope>,
                >(send_ptr)
            }
        } else {
            panic!("unknown storage strategy");
        };
        Self { ptr }
    }

    /// # Safety
    /// The caller must ensure the underlying scope outlives the casted reference.
    #[inline]
    pub unsafe fn cast<T: Storage>(self) -> ScopeCompletionRef<'scope, T> {
        unsafe { std::mem::transmute(self) }
    }

    #[inline]
    pub fn into_ptr(self) -> NonNull<dyn RawScope<'scope, S> + 'scope> {
        let ptr = self.ptr;
        forget(self);
        ptr
    }

    #[inline]
    /// # Safety
    /// The caller must ensure `ptr` is a valid pointer to `dyn RawScope<'scope, S>`.
    pub unsafe fn from_ptr(ptr: NonNull<dyn RawScope<'scope, S> + 'scope>) -> Self {
        Self { ptr }
    }

    pub fn new<O: Ownership>(scope: &O::Shared<GenericScopeCompletion<'scope, S, O>>) -> Self
    where
        GenericScopeCompletion<'scope, S, O>: RawScope<'scope, S> + 'scope,
    {
        let ptr = O::as_ptr(scope);
        unsafe { O::increment_strong_count(ptr) };

        let raw_ptr: *const GenericScopeCompletion<'scope, S, O> = ptr;
        let dyn_ptr: *const (dyn RawScope<'scope, S> + 'scope) = raw_ptr;

        Self {
            ptr: unsafe { NonNull::new_unchecked(dyn_ptr as *mut _) },
        }
    }

    #[inline]
    pub fn task_done(&self) {
        unsafe { self.ptr.as_ref().task_done() };
    }

    #[inline]
    pub fn cancel(&self) {
        unsafe { self.ptr.as_ref().cancel() };
    }

    #[inline]
    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        unsafe { self.ptr.as_ref().report_panic(payload) };
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        unsafe { self.ptr.as_ref().try_link_child(child_token) }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        unsafe { self.ptr.as_ref().is_cancelled() }
    }

    #[inline]
    pub fn parent(&self) -> Option<AnyScopeCompletionRef<'scope>> {
        unsafe { self.ptr.as_ref().parent() }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        unsafe { self.ptr.as_ref().register_cancel_waker(waker) }
    }

    #[inline]
    pub fn pop_local(&self) -> Option<LocalTaskRef<'scope>> {
        unsafe { self.ptr.as_ref().pop_local() }
    }

    #[inline]
    pub fn is_local_empty(&self) -> bool {
        unsafe { self.ptr.as_ref().is_local_empty() }
    }

    #[inline]
    pub fn enqueue_local(&self, task: LocalTaskRef<'scope>) {
        unsafe { self.ptr.as_ref().enqueue_local(task) }
    }
}

impl<'scope, S: Storage> Clone for ScopeCompletionRef<'scope, S> {
    fn clone(&self) -> Self {
        unsafe { self.ptr.as_ref().clone_ref() }
    }
}

impl<'scope, S: Storage> Drop for ScopeCompletionRef<'scope, S> {
    fn drop(&mut self) {
        unsafe { self.ptr.as_ref().drop_ref() }
    }
}

struct DummyScope<'scope, S: Storage>(PhantomData<(&'scope (), S)>);

impl<'scope, S: Storage> RawScope<'scope, S> for DummyScope<'scope, S> {
    fn task_done(&self) {}
    fn cancel(&self) {}
    fn report_panic(&self, _payload: Box<dyn Any + Send + 'static>) {}
    fn is_cancelled(&self) -> bool {
        false
    }
    fn try_link_child(&self, _child_token: &ErasedCancellationToken) -> bool {
        false
    }
    fn parent(&self) -> Option<AnyScopeCompletionRef<'scope>> {
        None
    }
    fn register_cancel_waker(&self, _waker: &Waker) {}
    fn enqueue_local(&self, _task: LocalTaskRef<'scope>) {}
    fn pop_local(&self) -> Option<LocalTaskRef<'scope>> {
        None
    }
    fn is_local_empty(&self) -> bool {
        true
    }
    unsafe fn clone_ref(&self) -> ScopeCompletionRef<'scope, S> {
        let dyn_ptr: *const (dyn RawScope<'scope, S> + 'scope) = self;
        unsafe { ScopeCompletionRef::from_ptr(NonNull::new_unchecked(dyn_ptr as *mut _)) }
    }
    unsafe fn drop_ref(&self) {}
}

static DUMMY_LOCAL_SCOPE: DummyScope<'static, LocalStorage> = DummyScope(PhantomData);
static DUMMY_SEND_SCOPE: DummyScope<'static, AtomicStorage> = DummyScope(PhantomData);

#[derive(Clone)]
pub enum AnyScopeCompletionRef<'scope> {
    Local(ScopeCompletionRef<'scope, LocalStorage>),
    Send(ScopeCompletionRef<'scope, AtomicStorage>),
}

impl<'scope> AnyScopeCompletionRef<'scope> {
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
    pub fn parent(&self) -> Option<AnyScopeCompletionRef<'scope>> {
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
    pub fn pop_local(&self) -> Option<LocalTaskRef<'scope>> {
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
    pub fn enqueue_local(&self, task: LocalTaskRef<'scope>) {
        match self {
            Self::Local(s) => s.enqueue_local(task),
            Self::Send(s) => s.enqueue_local(task),
        }
    }
}
