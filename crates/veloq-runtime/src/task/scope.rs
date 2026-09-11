use crate::{
    runtime::cancellation::{CancelWaiterLinkResult, GenericCancellationToken},
    scope::GenericScopeCompletion,
    utils::ownership::Ownership,
};
use veloq_intrusive_linklist::{Link, intrusive_adapter};
use veloq_std::cell::UnsafeCell;
use veloq_std::panic::PanicPayload;
use veloq_std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr::NonNull,
    task::Waker,
    vec::Vec,
};
use veloq_storage::{AtomicStorage, LocalStorage, Storage, StrategyType, ThreadSafeStorage};

/// 任务挂在所属 scope 取消队列上的等待节点。
///
/// 节点内联在 `GenericTaskHeader` 里，因此**不随存储策略泛型化** —— 一个
/// `AtomicStorage` 的 scope 可以同时持有 local 与 send 两种任务（`spawn_local`），
/// 类型化的链表装不下它们。`Link` 与 `Waker` 本身都与策略无关，节点字段一律在持有
/// scope 取消令牌的锁时访问。
#[doc(hidden)]
pub struct CancellationWaiter {
    pub(crate) link: Link,
    pub(crate) waker: UnsafeCell<Option<Waker>>,
}

intrusive_adapter!(pub(crate) CancellationWaiterAdapter = CancellationWaiter { link: Link });

impl CancellationWaiter {
    pub(crate) fn new() -> Self {
        Self {
            link: Link::new(),
            waker: UnsafeCell::new(None),
        }
    }

    /// # Safety
    ///
    /// 调用者必须持有所属取消令牌的锁。该锁同时保证没有其它线程访问
    /// `waker`，因为 `UnsafeCell` 本身不提供同步或锁。
    pub(crate) unsafe fn set_waker(&self, waker: &Waker) {
        unsafe {
            self.waker.with_mut(|slot| match slot {
                Some(existing) if existing.will_wake(waker) => {}
                slot => *slot = Some(waker.clone()),
            });
        }
    }

    /// # Safety
    ///
    /// 调用者必须持有所属取消令牌的锁。该锁同时保证没有其它线程访问
    /// `waker`，因为 `UnsafeCell` 本身不提供同步或锁。
    pub(crate) unsafe fn take_waker(&self) -> Option<Waker> {
        unsafe { self.waker.with_mut(|slot| slot.take()) }
    }
}

impl Default for CancellationWaiter {
    fn default() -> Self {
        Self::new()
    }
}

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
    fn report_panic(&self, payload: PanicPayload);
    fn is_cancelled(&self) -> bool;
    fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool;
    fn parent(&self) -> Option<AnyScopeRef>;
    fn cancel_parent_chain(&self) -> Vec<AnySendScopeRef>;
    /// 把任务自己的等待节点挂到本 scope 的取消队列上。
    ///
    /// # Safety
    ///
    /// `waiter` 必须在被 `unlink_cancel_waiter` 摘除之前保持有效且地址稳定。
    unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<CancellationWaiter>,
        waker: &veloq_std::task::Waker,
    ) -> CancelWaiterLinkResult;
    /// # Safety
    ///
    /// `waiter` 必须是先前传给 `link_cancel_waiter` 的同一个节点。
    unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<CancellationWaiter>);
    /// # Safety
    ///
    /// The caller must ensure the returned reference is dropped before the underlying scope is deallocated.
    unsafe fn clone_raw(&self, raw: NonNull<dyn RawScope>) -> NonNull<dyn RawScope>;
    /// # Safety
    ///
    /// The caller must ensure the reference is not dropped twice.
    unsafe fn drop_raw(&self, raw: NonNull<dyn RawScope>);
}

struct DummyScope<S: Storage>(PhantomData<S>);

unsafe impl<S: Storage> Sync for DummyScope<S> {}

impl<S: Storage> RawScope for DummyScope<S> {
    fn task_done(&self) {}
    fn cancel(&self) {}
    fn report_panic(&self, _payload: PanicPayload) {}
    fn is_cancelled(&self) -> bool {
        false
    }
    fn try_link_child(&self, _child_token: &ErasedCancellationToken) -> bool {
        false
    }
    fn parent(&self) -> Option<AnyScopeRef> {
        None
    }
    fn cancel_parent_chain(&self) -> Vec<AnySendScopeRef> {
        Vec::new()
    }
    /// 哑作用域永远不会被取消，因此「已挂载」是成立的（虽然什么都没挂）。
    unsafe fn link_cancel_waiter(
        &self,
        _waiter: NonNull<CancellationWaiter>,
        _waker: &veloq_std::task::Waker,
    ) -> CancelWaiterLinkResult {
        CancelWaiterLinkResult::Linked
    }
    unsafe fn unlink_cancel_waiter(&self, _waiter: NonNull<CancellationWaiter>) {}
    unsafe fn clone_raw(&self, raw: NonNull<dyn RawScope>) -> NonNull<dyn RawScope> {
        raw
    }
    unsafe fn drop_raw(&self, _raw: NonNull<dyn RawScope>) {}
}

static DUMMY_LOCAL_SCOPE: DummyScope<LocalStorage> = DummyScope(PhantomData);
static DUMMY_SEND_SCOPE: DummyScope<AtomicStorage> = DummyScope(PhantomData);

pub struct ScopeRef<S: Storage> {
    inner: NonNull<dyn RawScope>,
    _marker: PhantomData<S>,
}

impl<S: Storage> Debug for ScopeRef<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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

    /// 从一个仍由调用者持有的作用域共享所有权中创建一份类型擦除引用。
    pub(crate) fn from_shared<O>(shared: &O::Shared<GenericScopeCompletion<S, O>>) -> Self
    where
        S: ScopeStorage,
        O: Ownership + 'static,
    {
        let ptr = O::as_ptr(shared);
        unsafe { O::increment_strong_count(ptr) };
        let dyn_ptr: *const dyn RawScope = ptr;
        unsafe { Self::new(NonNull::new_unchecked(dyn_ptr as *mut _)) }
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
        let this = ManuallyDrop::new(self);
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
    pub(crate) fn cancel_parent_chain(&self) -> Vec<AnySendScopeRef> {
        unsafe { self.as_ref().cancel_parent_chain() }
    }

    /// # Safety
    ///
    /// 见 [`RawScope::link_cancel_waiter`]。
    #[inline]
    pub(crate) unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<CancellationWaiter>,
        waker: &veloq_std::task::Waker,
    ) -> CancelWaiterLinkResult {
        unsafe { self.as_ref().link_cancel_waiter(waiter, waker) }
    }

    /// # Safety
    ///
    /// 见 [`RawScope::unlink_cancel_waiter`]。
    #[inline]
    pub(crate) unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<CancellationWaiter>) {
        unsafe { self.as_ref().unlink_cancel_waiter(waiter) }
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
    pub fn report_panic(&self, payload: PanicPayload) {
        unsafe { self.as_ref().report_panic(payload) }
    }

    /// 将具体类型的 `ScopeRef` 转换成不透明类型的 `AnyScopeRef`。
    #[inline]
    pub fn into_any(self) -> AnyScopeRef {
        let this = ManuallyDrop::new(self);
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
        let non_null = unsafe { self.as_ref().clone_raw(self.inner) };
        unsafe { Self::new(non_null) }
    }
}

impl<S: Storage> Drop for ScopeRef<S> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.as_ref().drop_raw(self.inner) }
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
    pub(crate) fn parent_send(&self) -> Option<Self> {
        self.0.parent().and_then(|parent| parent.as_send())
    }

    #[inline]
    pub(crate) unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<CancellationWaiter>,
        waker: &veloq_std::task::Waker,
    ) -> CancelWaiterLinkResult {
        unsafe { self.0.link_cancel_waiter(waiter, waker) }
    }

    #[inline]
    pub(crate) unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<CancellationWaiter>) {
        unsafe { self.0.unlink_cancel_waiter(waiter) }
    }

    #[inline]
    pub fn report_panic(&self, payload: PanicPayload) {
        self.0.report_panic(payload);
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

    /// 把一份未被认领的 panic payload 交给上一层 scope。
    ///
    /// 子 scope 在没有 `wait_all()` 的路径上被丢弃时（`select!` 落败分支、外层 panic），
    /// payload 不能就地 `resume_unwind`（那会在任意 worker 线程上抛出），也不能静默丢弃，
    /// 于是沿 scope 树上交，由某一层的 `wait_all()` 抛出。
    #[inline]
    pub fn report_panic(&self, payload: PanicPayload) {
        match self {
            Self::Local(s) => s.report_panic(payload),
            Self::Send(s) => s.report_panic(payload),
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
