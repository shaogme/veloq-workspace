use crate::runtime::primitives::GenericCancellationToken;
use crate::scope::GenericScopeCompletion;
use crate::task::LocalTaskRef;
use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicStorage, LocalStorage, Storage, StrategyType};
use std::any::Any;
use std::marker::PhantomData;
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
    /// 且 `S` 和 `O` 与调用处的泛型参数匹配。通常通过 `ScopeVTable` 或 `StrategyType` 进行校验。
    #[inline]
    pub unsafe fn as_concrete<'a, S: Storage, O: Ownership>(
        ptr: NonNull<Self>,
    ) -> &'a GenericScopeCompletion<S, O> {
        unsafe { &*(ptr.as_ptr() as *const GenericScopeCompletion<S, O>) }
    }
}

pub struct ErasedCancellationToken {
    pub(crate) ptr: NonNull<OpaqueToken>,
    pub(crate) s_type: StrategyType,
    pub(crate) o_type: StrategyType,
}

impl ErasedCancellationToken {
    pub fn new<S: Storage, O: Ownership>(
        token: &GenericCancellationToken<S, O>,
    ) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(token as *const _ as *mut OpaqueToken) },
            s_type: S::strategy_type(),
            o_type: O::strategy_type(),
        }
    }

    /// 尝试将擦除类型的令牌向下转换为具体类型
    ///
    /// # Safety
    ///
    /// 调用者必须确保该令牌确实是 `GenericCancellationToken<S, O>` 类型。
    /// 虽然内部有类型检查，但该函数仍被标记为 unsafe 以提醒调用者注意指针生命周期。
    pub unsafe fn downcast<S: Storage, O: Ownership>(
        &self,
    ) -> Option<&GenericCancellationToken<S, O>> {
        if self.s_type == S::strategy_type() && self.o_type == O::strategy_type() {
            unsafe { Some(&*(self.ptr.as_ptr() as *const GenericCancellationToken<S, O>)) }
        } else {
            None
        }
    }
}

pub trait RawScope<S: Storage> {
    fn task_done(&self);
    fn cancel(&self);
    fn report_panic(&self, payload: Box<dyn Any + Send + 'static>);
    fn is_cancelled(&self) -> bool;
    fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool;
    fn parent(&self) -> Option<AnyScopeCompletionRef>;
    fn register_cancel_waker(&self, waker: &Waker);
    fn enqueue_local(&self, task: LocalTaskRef);
    fn pop_local(&self) -> Option<LocalTaskRef>;
    fn is_local_empty(&self) -> bool;
    /// # Safety
    ///
    /// The caller must ensure the returned reference is dropped before the underlying scope is deallocated.
    unsafe fn clone_ref(&self) -> AnyScopeCompletionRef;
    /// # Safety
    ///
    /// The caller must ensure the reference is not dropped twice.
    unsafe fn drop_ref(&self);
}

struct DummyScope<S: Storage>(PhantomData<S>);

impl<S: Storage> RawScope<S> for DummyScope<S> {
    fn task_done(&self) {}
    fn cancel(&self) {}
    fn report_panic(&self, _payload: Box<dyn Any + Send + 'static>) {}
    fn is_cancelled(&self) -> bool {
        false
    }
    fn try_link_child(&self, _child_token: &ErasedCancellationToken) -> bool {
        false
    }
    fn parent(&self) -> Option<AnyScopeCompletionRef> {
        None
    }
    fn register_cancel_waker(&self, _waker: &Waker) {}
    fn enqueue_local(&self, _task: LocalTaskRef) {}
    fn pop_local(&self) -> Option<LocalTaskRef> {
        None
    }
    fn is_local_empty(&self) -> bool {
        true
    }
    unsafe fn clone_ref(&self) -> AnyScopeCompletionRef {
        let dyn_ptr: *const dyn RawScope<S> = self;
        match S::strategy_type() {
            StrategyType::Local => unsafe {
                let local_dyn_ptr = std::mem::transmute::<
                    *const dyn crate::task::RawScope<S>,
                    *mut dyn crate::task::RawScope<LocalStorage>,
                >(dyn_ptr);
                AnyScopeCompletionRef::Local(NonNull::new_unchecked(local_dyn_ptr))
            },
            StrategyType::Atomic => unsafe {
                let send_dyn_ptr = std::mem::transmute::<
                    *const dyn crate::task::RawScope<S>,
                    *mut dyn crate::task::RawScope<AtomicStorage>,
                >(dyn_ptr);
                AnyScopeCompletionRef::Send(NonNull::new_unchecked(send_dyn_ptr))
            },
        }
    }
    unsafe fn drop_ref(&self) {}
}

static DUMMY_LOCAL_SCOPE: DummyScope<LocalStorage> = DummyScope(PhantomData);
static DUMMY_SEND_SCOPE: DummyScope<AtomicStorage> = DummyScope(PhantomData);

#[derive(Debug)]
pub enum AnyScopeCompletionRef {
    Local(NonNull<dyn RawScope<LocalStorage>>),
    Send(NonNull<dyn RawScope<AtomicStorage>>),
}

unsafe impl Send for AnyScopeCompletionRef {}
unsafe impl Sync for AnyScopeCompletionRef {}

impl Clone for AnyScopeCompletionRef {
    #[inline]
    fn clone(&self) -> Self {
        match *self {
            Self::Local(ptr) => unsafe { ptr.as_ref().clone_ref() },
            Self::Send(ptr) => unsafe { ptr.as_ref().clone_ref() },
        }
    }
}

impl Drop for AnyScopeCompletionRef {
    #[inline]
    fn drop(&mut self) {
        match *self {
            Self::Local(ptr) => unsafe { ptr.as_ref().drop_ref() },
            Self::Send(ptr) => unsafe { ptr.as_ref().drop_ref() },
        }
    }
}

impl AnyScopeCompletionRef {
    pub fn dummy<S: Storage>() -> Self {
        match S::strategy_type() {
            StrategyType::Local => {
                let local_ptr: NonNull<dyn RawScope<LocalStorage>> =
                    NonNull::from(&DUMMY_LOCAL_SCOPE);
                Self::Local(local_ptr)
            }
            StrategyType::Atomic => {
                let send_ptr: NonNull<dyn RawScope<AtomicStorage>> =
                    NonNull::from(&DUMMY_SEND_SCOPE);
                Self::Send(send_ptr)
            }
        }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Local(s) => unsafe { s.as_ref().is_cancelled() },
            Self::Send(s) => unsafe { s.as_ref().is_cancelled() },
        }
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        match self {
            Self::Local(s) => unsafe { s.as_ref().try_link_child(child_token) },
            Self::Send(s) => unsafe { s.as_ref().try_link_child(child_token) },
        }
    }

    #[inline]
    pub fn parent(&self) -> Option<AnyScopeCompletionRef> {
        match self {
            Self::Local(s) => unsafe { s.as_ref().parent() },
            Self::Send(s) => unsafe { s.as_ref().parent() },
        }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().register_cancel_waker(waker) },
            Self::Send(s) => unsafe { s.as_ref().register_cancel_waker(waker) },
        }
    }

    #[inline]
    pub fn pop_local(&self) -> Option<LocalTaskRef> {
        match self {
            Self::Local(s) => unsafe { s.as_ref().pop_local() },
            Self::Send(s) => unsafe { s.as_ref().pop_local() },
        }
    }

    #[inline]
    pub fn is_local_empty(&self) -> bool {
        match self {
            Self::Local(s) => unsafe { s.as_ref().is_local_empty() },
            Self::Send(s) => unsafe { s.as_ref().is_local_empty() },
        }
    }

    #[inline]
    pub fn enqueue_local(&self, task: LocalTaskRef) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().enqueue_local(task) },
            Self::Send(s) => unsafe { s.as_ref().enqueue_local(task) },
        }
    }

    #[inline]
    pub fn task_done(&self) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().task_done() },
            Self::Send(s) => unsafe { s.as_ref().task_done() },
        }
    }

    #[inline]
    pub fn cancel(&self) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().cancel() },
            Self::Send(s) => unsafe { s.as_ref().cancel() },
        }
    }

    #[inline]
    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().report_panic(payload) },
            Self::Send(s) => unsafe { s.as_ref().report_panic(payload) },
        }
    }
}
