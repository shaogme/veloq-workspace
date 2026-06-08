use crate::runtime::primitives::GenericCancellationToken;
use crate::scope::GenericScopeCompletion;
use crate::utils::ownership::Ownership;
use crate::utils::storage::{
    AtomicStorage, LocalStorage, Storage, StrategyType, ThreadSafeStorage,
};
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
    pub unsafe fn as_concrete<'a, S: ScopeStorage, O: Ownership>(
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
    pub fn new<S: Storage, O: Ownership>(token: &GenericCancellationToken<S, O>) -> Self {
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

pub trait RawScope {
    fn task_done(&self);
    fn cancel(&self);
    fn report_panic(&self, payload: Box<dyn Any + Send + 'static>);
    fn is_cancelled(&self) -> bool;
    fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool;
    fn parent(&self) -> Option<AnyScopeRef>;
    fn register_cancel_waker(&self, waker: &Waker);
    /// # Safety
    ///
    /// The caller must ensure the returned reference is dropped before the underlying scope is deallocated.
    unsafe fn clone_raw(&self) -> NonNull<dyn RawScope>;
    /// # Safety
    ///
    /// The caller must ensure the reference is not dropped twice.
    unsafe fn drop_raw(&self);
}

struct DummyScope<S: Storage>(PhantomData<S>);

unsafe impl<S: Storage> Sync for DummyScope<S> {}

impl<S: Storage> RawScope for DummyScope<S> {
    fn task_done(&self) {}
    fn cancel(&self) {}
    fn report_panic(&self, _payload: Box<dyn Any + Send + 'static>) {}
    fn is_cancelled(&self) -> bool {
        false
    }
    fn try_link_child(&self, _child_token: &ErasedCancellationToken) -> bool {
        false
    }
    fn parent(&self) -> Option<AnyScopeRef> {
        None
    }
    fn register_cancel_waker(&self, _waker: &Waker) {}
    unsafe fn clone_raw(&self) -> NonNull<dyn RawScope> {
        let dyn_ptr: *const dyn RawScope = self;
        unsafe { NonNull::new_unchecked(dyn_ptr as *mut _) }
    }
    unsafe fn drop_raw(&self) {}
}

static DUMMY_LOCAL_SCOPE: DummyScope<LocalStorage> = DummyScope(PhantomData);
static DUMMY_SEND_SCOPE: DummyScope<AtomicStorage> = DummyScope(PhantomData);

pub struct ScopeRef<S: Storage> {
    inner: NonNull<dyn RawScope>,
    _marker: PhantomData<S>,
}

impl<S: Storage> std::fmt::Debug for ScopeRef<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeRef")
            .field("inner", &self.inner)
            .finish()
    }
}

unsafe impl<S: ThreadSafeStorage> Send for ScopeRef<S> {}
unsafe impl<S: ThreadSafeStorage> Sync for ScopeRef<S> {}

impl<S: Storage> ScopeRef<S> {
    /// 创建一个新的 `ScopeRef`。
    ///
    /// # Safety
    ///
    /// 调用者必须确保指针有效。
    #[inline]
    pub const unsafe fn new(inner: NonNull<dyn RawScope>) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// 获取内部的 `NonNull` 指针。
    #[inline]
    pub fn as_non_null(&self) -> NonNull<dyn RawScope> {
        self.inner
    }

    /// 获取对内部 `RawScope` 的引用。
    ///
    /// # Safety
    ///
    /// 必须保证指针依然有效。
    #[inline]
    pub unsafe fn as_ref(&self) -> &dyn RawScope {
        unsafe { self.inner.as_ref() }
    }

    /// 在 crate 内将 ScopeRef 从一种 Storage 模式转换为另一种 Storage 模式。
    #[inline]
    pub(crate) fn cast<T: Storage>(self) -> ScopeRef<T> {
        let this = std::mem::ManuallyDrop::new(self);
        ScopeRef {
            inner: this.inner,
            _marker: PhantomData,
        }
    }

    /// 创建一个虚拟的 `ScopeRef`。
    pub fn dummy() -> Self {
        match S::strategy_type() {
            StrategyType::Local => {
                let local_ptr: NonNull<dyn RawScope> = NonNull::from(&DUMMY_LOCAL_SCOPE);
                Self {
                    inner: local_ptr,
                    _marker: PhantomData,
                }
            }
            StrategyType::Atomic => {
                let send_ptr: NonNull<dyn RawScope> = NonNull::from(&DUMMY_SEND_SCOPE);
                Self {
                    inner: send_ptr,
                    _marker: PhantomData,
                }
            }
        }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        unsafe { self.as_ref().is_cancelled() }
    }

    #[inline]
    pub fn parent(&self) -> Option<AnyScopeRef> {
        unsafe { self.as_ref().parent() }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        unsafe { self.as_ref().register_cancel_waker(waker) }
    }

    #[inline]
    pub fn task_done(&self) {
        unsafe { self.as_ref().task_done() }
    }

    #[inline]
    pub fn cancel(&self) {
        unsafe { self.as_ref().cancel() }
    }

    #[inline]
    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        unsafe { self.as_ref().report_panic(payload) }
    }

    /// 将具体类型的 `ScopeRef` 转换成不透明类型的 `AnyScopeRef`。
    #[inline]
    pub fn into_any(self) -> AnyScopeRef {
        let this = std::mem::ManuallyDrop::new(self);
        match S::strategy_type() {
            StrategyType::Local => AnyScopeRef::Local(ScopeRef {
                inner: this.inner,
                _marker: PhantomData,
            }),
            StrategyType::Atomic => AnyScopeRef::Send(AnySendScopeRef(ScopeRef {
                inner: this.inner,
                _marker: PhantomData,
            })),
        }
    }
}

impl<S: Storage> Clone for ScopeRef<S> {
    #[inline]
    fn clone(&self) -> Self {
        let non_null = unsafe { self.as_ref().clone_raw() };
        unsafe { Self::new(non_null) }
    }
}

impl<S: Storage> Drop for ScopeRef<S> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.as_ref().drop_raw() }
    }
}

#[derive(Debug)]
pub enum AnyScopeRef {
    Local(ScopeRef<LocalStorage>),
    Send(AnySendScopeRef),
}

#[derive(Debug)]
pub struct AnySendScopeRef(ScopeRef<AtomicStorage>);

unsafe impl Send for AnySendScopeRef {}
unsafe impl Sync for AnySendScopeRef {}

impl Clone for AnySendScopeRef {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl AnySendScopeRef {
    #[inline]
    pub fn new(scope: ScopeRef<AtomicStorage>) -> Self {
        Self(scope)
    }

    #[inline]
    pub fn as_scope_ref(&self) -> &ScopeRef<AtomicStorage> {
        &self.0
    }

    #[inline]
    pub fn into_any(self) -> AnyScopeRef {
        AnyScopeRef::Send(self)
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        unsafe { self.0.as_ref().try_link_child(child_token) }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        unsafe { self.0.as_ref().register_cancel_waker(waker) }
    }
}

impl Clone for AnyScopeRef {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Local(s) => Self::Local(s.clone()),
            Self::Send(s) => Self::Send(s.clone()),
        }
    }
}

impl AnyScopeRef {
    #[inline]
    pub fn as_send(&self) -> Option<AnySendScopeRef> {
        match self {
            Self::Local(_) => None,
            Self::Send(s) => Some(s.clone()),
        }
    }

    #[inline]
    pub fn into_send(self) -> Option<AnySendScopeRef> {
        match self {
            Self::Local(_) => None,
            Self::Send(s) => Some(s),
        }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Local(s) => unsafe { s.as_ref().is_cancelled() },
            Self::Send(s) => s.is_cancelled(),
        }
    }

    #[inline]
    pub(crate) fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        match self {
            Self::Local(s) => unsafe { s.as_ref().try_link_child(child_token) },
            Self::Send(s) => s.try_link_child(child_token),
        }
    }

    #[inline]
    pub fn register_cancel_waker(&self, waker: &Waker) {
        match self {
            Self::Local(s) => unsafe { s.as_ref().register_cancel_waker(waker) },
            Self::Send(s) => s.register_cancel_waker(waker),
        }
    }
}

pub trait ScopeParent: Clone + 'static {
    fn from_any(parent: Option<AnyScopeRef>) -> Self;
    fn as_any(&self) -> Option<AnyScopeRef>;
    fn as_send(&self) -> Option<AnySendScopeRef>;
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct LocalScopeParent(Option<AnyScopeRef>);

impl ScopeParent for LocalScopeParent {
    #[inline]
    fn from_any(parent: Option<AnyScopeRef>) -> Self {
        Self(parent)
    }

    #[inline]
    fn as_any(&self) -> Option<AnyScopeRef> {
        self.0.clone()
    }

    #[inline]
    fn as_send(&self) -> Option<AnySendScopeRef> {
        self.0.as_ref().and_then(AnyScopeRef::as_send)
    }

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.0
            .as_ref()
            .map(AnyScopeRef::is_cancelled)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct ThreadSafeScopeParent(Option<AnySendScopeRef>);

impl ScopeParent for ThreadSafeScopeParent {
    #[inline]
    fn from_any(parent: Option<AnyScopeRef>) -> Self {
        Self(parent.and_then(AnyScopeRef::into_send))
    }

    #[inline]
    fn as_any(&self) -> Option<AnyScopeRef> {
        self.0.clone().map(AnySendScopeRef::into_any)
    }

    #[inline]
    fn as_send(&self) -> Option<AnySendScopeRef> {
        self.0.clone()
    }

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.0
            .as_ref()
            .map(AnySendScopeRef::is_cancelled)
            .unwrap_or(false)
    }
}

pub trait ScopeStorage: Storage {
    type Parent: ScopeParent;
}

impl ScopeStorage for LocalStorage {
    type Parent = LocalScopeParent;
}

impl ScopeStorage for AtomicStorage {
    type Parent = ThreadSafeScopeParent;
}
