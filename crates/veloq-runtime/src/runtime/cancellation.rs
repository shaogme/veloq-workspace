use std::{
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
};

use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::cell::UnsafeCell;
use veloq_storage::{StateInt, StateLock, Storage};

use crate::{
    task::{AnySendScopeRef, CancellationWaiter, CancellationWaiterAdapter, OpaqueToken},
    utils::ownership::Ownership,
};

/// 任务取消等待节点的注册结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum CancelWaiterLinkResult {
    /// 节点已经由本取消源保护。
    Linked,
    /// 取消已经在线性化锁内发布，节点没有加入链表。
    RejectedByCancellation,
}

pub(crate) type ChildList<S, O> = <S as Storage>::Lock<LinkedList<CancellationTokenAdapter<S, O>>>;
pub(crate) type ParentSlot<S, O> =
    <S as Storage>::Lock<Option<<O as Ownership>::Weak<GenericCancellationTokenInner<S, O>>>>;
pub(crate) type CancelWaiterList<S> = <S as Storage>::Lock<LinkedList<CancellationWaiterAdapter>>;

pub struct GenericCancellationTokenInner<S: Storage, O: Ownership> {
    cancelled: S::Usize,
    /// 当前仍处于 pending 的取消等待者。
    waiters: CancelWaiterList<S>,
    children: ChildList<S, O>,
    link: Link,
    parent: ParentSlot<S, O>,
    cross_parent: Option<AnySendScopeRef>,
}

intrusive_adapter!(pub(crate) CancellationTokenAdapter<S, O> = GenericCancellationTokenInner<S, O> { link: Link } where S: Storage, O: Ownership);

impl<S: Storage, O: Ownership> GenericCancellationTokenInner<S, O> {
    /// 在锁内摘下所有等待节点，并把其中的 waker 移交给调用者。
    ///
    /// 节点引用不会离开这个锁保护的作用域。调用者只能在锁外访问已经移出的 waker，
    /// 以避免 `wake` 路径重新进入取消链而发生自锁。
    fn take_waiters(&self) -> Vec<Waker> {
        let mut ready = Vec::new();
        let mut waiters = self.waiters.lock();
        while let Some(waiter) = unsafe { waiters.pop_front_ptr() } {
            if let Some(waker) = unsafe { waiter.as_ref().take_waker() } {
                ready.push(waker);
            }
        }
        ready
    }

    fn wake_waiters(&self) {
        for waker in self.take_waiters() {
            waker.wake();
        }
    }

    /// 取消本令牌及其整棵子树。
    fn cancel_internal(&self) {
        if self
            .cancelled
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        // `cancelled` 的发布先于取得 `waiters` 锁。注册者若已经持锁入链，本次排空会
        // 在释放锁后看到它；注册者若之后才取得锁，则锁内检查必然拒绝。
        self.wake_waiters();

        let mut pending = self.detach_children();
        while let Some(child) = pending.pop() {
            let child_ref = unsafe { child.as_ref() };
            if child_ref
                .cancelled
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                child_ref.wake_waiters();
                pending.extend(child_ref.detach_children());
            }
            // 归还上面在锁内加的那次强引用。
            unsafe { O::decrement_strong_count(child.as_ptr() as *const Self) };
        }
    }

    /// 在锁内摘下所有子节点，并为每个子节点加一次强引用后交给调用方。
    fn detach_children(&self) -> Vec<NonNull<Self>> {
        let mut detached = Vec::new();
        let mut children = self.children.lock();
        while let Some(ptr) = unsafe { children.pop_front_ptr() } {
            unsafe { O::increment_strong_count(ptr.as_ptr() as *const Self) };
            detached.push(ptr);
        }
        detached
    }
}

/// 一个 future 在单个跨 scope parent 上的稳定注册。
pub(crate) struct AncestorRegistration {
    pub(crate) scope: AnySendScopeRef,
    pub(crate) node: Pin<Box<CancellationWaiter>>,
}

impl AncestorRegistration {
    pub(crate) fn new(scope: AnySendScopeRef) -> Self {
        Self {
            scope,
            node: Box::pin(CancellationWaiter::new()),
        }
    }

    pub(crate) fn link(&self, waker: &Waker) -> CancelWaiterLinkResult {
        let waiter = NonNull::from(self.node.as_ref().get_ref());
        unsafe { self.scope.link_cancel_waiter(waiter, waker) }
    }

    pub(crate) fn unlink(&self) {
        let waiter = NonNull::from(self.node.as_ref().get_ref());
        unsafe { self.scope.unlink_cancel_waiter(waiter) }
    }
}

/// `CancelledFuture` 对当前 token 和完整跨 scope parent 链的 RAII 注册。
pub(crate) struct CancellationRegistration {
    /// 本地节点地址在注册期间必须保持稳定，因此不放入 `UnsafeCell`。
    pub(crate) local: CancellationWaiter,
    pub(crate) ancestors: UnsafeCell<Vec<AncestorRegistration>>,
    armed: std::sync::atomic::AtomicBool,
}

impl CancellationRegistration {
    pub(crate) fn new() -> Self {
        Self {
            local: CancellationWaiter::new(),
            ancestors: UnsafeCell::new(Vec::new()),
            armed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    pub(crate) fn ensure_ancestor_nodes<S: Storage, O: Ownership>(
        &self,
        token: &GenericCancellationToken<S, O>,
    ) {
        let parent_chain = token.parent_chain();
        unsafe {
            self.ancestors.with_mut(|ancestors| {
                if ancestors.is_empty() {
                    ancestors.extend(parent_chain.into_iter().map(AncestorRegistration::new));
                }
            });
        }
    }

    #[cfg(test)]
    fn ancestor_count(&self) -> usize {
        unsafe { self.ancestors.with(|ancestors| ancestors.len()) }
    }

    /// 注册当前 token 和所有跨 scope parent。
    pub(crate) fn arm<S: Storage, O: Ownership>(
        &self,
        token: &GenericCancellationToken<S, O>,
        waker: &Waker,
    ) -> bool {
        self.armed.store(true, Ordering::Relaxed);

        let local = NonNull::from(&self.local);
        if unsafe { token.link_cancel_waiter(local, waker) }
            == CancelWaiterLinkResult::RejectedByCancellation
        {
            return false;
        }

        let ancestor_rejected = unsafe {
            self.ancestors.with(|ancestors| {
                ancestors.iter().any(|ancestor| {
                    ancestor.link(waker) == CancelWaiterLinkResult::RejectedByCancellation
                })
            })
        };
        if ancestor_rejected {
            return false;
        }
        true
    }

    /// 先摘当前 token，再按逆 parent 顺序摘除所有 ancestor。
    pub(crate) fn disarm<S: Storage, O: Ownership>(&self, token: &GenericCancellationToken<S, O>) {
        if !self.is_armed() {
            return;
        }

        let local = NonNull::from(&self.local);
        unsafe { token.unlink_cancel_waiter(local) };
        unsafe {
            self.ancestors.with_mut(|ancestors| {
                for ancestor in ancestors.iter().rev() {
                    ancestor.unlink();
                }
                ancestors.clear();
            });
        }
        self.armed.store(false, Ordering::Relaxed);
    }
}

pub struct GenericCancellationToken<S: Storage, O: Ownership> {
    pub(crate) inner: O::Shared<GenericCancellationTokenInner<S, O>>,
}

impl<S: Storage, O: Ownership> Default for GenericCancellationToken<S, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage, O: Ownership> GenericCancellationToken<S, O> {
    pub fn new() -> Self {
        Self::new_with_parent(None)
    }

    pub fn new_with_parent(cross_parent: Option<AnySendScopeRef>) -> Self {
        Self {
            inner: O::new(GenericCancellationTokenInner {
                cancelled: S::Usize::new(0),
                waiters: S::Lock::new(LinkedList::new(CancellationWaiterAdapter)),
                children: S::Lock::new(LinkedList::new(CancellationTokenAdapter::<S, O>::new())),
                link: Link::new(),
                parent: S::Lock::new(None),
                cross_parent,
            }),
        }
    }

    pub fn link_child(&self, child: &Self) {
        if self.is_cancelled() {
            child.cancel();
            return;
        }

        {
            let mut parent_slot = child.inner.parent.lock();
            *parent_slot = Some(O::downgrade(&self.inner));
        }

        let mut children = self.inner.children.lock();
        if self.is_cancelled() {
            drop(children);
            child.cancel();
            return;
        }

        // 同一个令牌被 link 到两个父亲会覆盖 prev/next 并损坏链表，显式拒绝重复挂载。
        debug_assert!(
            !child.inner.link.is_linked(),
            "cancellation token is already linked to a parent"
        );
        if child.inner.link.is_linked() {
            return;
        }

        unsafe {
            let child_ptr = NonNull::new_unchecked(
                O::as_ptr(&child.inner) as *mut GenericCancellationTokenInner<S, O>
            );
            children.push_back_ptr(child_ptr);
        }
    }

    /// 把任务的等待节点挂到本 token 上。
    ///
    /// 本地取消判断、waker 更新和首次入链在线性化锁内完成。跨 scope parent 由
    /// [`CancellationRegistration`] 使用独立节点注册，避免同一个侵入式 Link 同时属于
    /// 多条链表。
    ///
    /// # Safety
    ///
    /// `waiter` 必须在被 `unlink_cancel_waiter` 摘除之前保持有效且地址稳定。
    pub(crate) unsafe fn link_cancel_waiter(
        &self,
        waiter: NonNull<CancellationWaiter>,
        waker: &Waker,
    ) -> CancelWaiterLinkResult {
        let mut waiters = self.inner.waiters.lock();
        if self.inner.cancelled.load(Ordering::Acquire) != 0 {
            return CancelWaiterLinkResult::RejectedByCancellation;
        }

        unsafe {
            let waiter_ref = waiter.as_ref();
            waiter_ref.set_waker(waker);
            if !waiter_ref.link.is_linked() {
                waiters.push_back_ptr(waiter);
            }
        }
        CancelWaiterLinkResult::Linked
    }

    /// # Safety
    ///
    /// `waiter` 必须是先前传给 `link_cancel_waiter` 的同一个节点。
    pub(crate) unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<CancellationWaiter>) {
        let mut waiters = self.inner.waiters.lock();
        if unsafe { waiter.as_ref().link.is_linked() } {
            unsafe {
                let mut cursor = waiters.cursor_mut_from_ptr(waiter);
                cursor.remove_ptr();
            }
        }
        let _ = unsafe { waiter.as_ref().take_waker() };
    }

    pub(crate) unsafe fn try_link_child_raw(&self, child_token_ptr: *const OpaqueToken) -> bool {
        let child = unsafe { &*(child_token_ptr as *const Self) };
        self.link_child(child);
        true
    }

    pub fn child(&self) -> Self {
        let child = Self::new();
        self.link_child(&child);
        child
    }

    pub fn cancel(&self) {
        self.inner.cancel_internal();
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        if self.inner.cancelled.load(Ordering::Acquire) != 0 {
            return true;
        }
        if let Some(ref parent) = self.inner.cross_parent
            && parent.is_cancelled()
        {
            return true;
        }
        false
    }

    /// 返回从最近跨 scope parent 到最远 send parent 的完整取消源链。
    pub(crate) fn parent_chain(&self) -> Vec<AnySendScopeRef> {
        let mut chain = Vec::new();
        let mut current = self.inner.cross_parent.clone();
        while let Some(scope) = current {
            current = scope.parent_send();
            chain.push(scope);
        }
        chain
    }

    pub fn cancelled(&self) -> CancelledFuture<S, O> {
        CancelledFuture {
            token: self.clone(),
            registration: CancellationRegistration::new(),
            _pin: PhantomPinned,
        }
    }

    pub fn from_inner(inner: O::Shared<GenericCancellationTokenInner<S, O>>) -> Self {
        Self { inner }
    }
}

impl<S: Storage, O: Ownership> Drop for GenericCancellationToken<S, O> {
    fn drop(&mut self) {
        if O::strong_count(&O::downgrade(&self.inner)) == 1 {
            let parent_guard = self.inner.parent.lock();
            if let Some(parent_weak) = parent_guard.as_ref()
                && let Some(parent_inner) = O::upgrade(parent_weak)
            {
                let mut children = parent_inner.children.lock();
                if self.inner.link.is_linked() {
                    unsafe {
                        let node_ptr = NonNull::new_unchecked(
                            O::as_ptr(&self.inner) as *mut GenericCancellationTokenInner<S, O>
                        );
                        let mut cursor = children.cursor_mut_from_ptr(node_ptr);
                        cursor.remove_ptr();
                    }
                }
            }
        }
    }
}

impl<S: Storage, O: Ownership> Clone for GenericCancellationToken<S, O> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct CancelledFuture<S: Storage, O: Ownership> {
    token: GenericCancellationToken<S, O>,
    registration: CancellationRegistration,
    _pin: PhantomPinned,
}

impl<S: Storage, O: Ownership> Future for CancelledFuture<S, O> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.token.is_cancelled() {
            this.registration.disarm(&this.token);
            return Poll::Ready(());
        }

        if !this.registration.is_armed() {
            this.registration.ensure_ancestor_nodes(&this.token);
        }
        if !this.registration.arm(&this.token, cx.waker()) {
            this.registration.disarm(&this.token);
            return Poll::Ready(());
        }

        if this.token.is_cancelled() {
            this.registration.disarm(&this.token);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<S: Storage, O: Ownership> Drop for CancelledFuture<S, O> {
    fn drop(&mut self) {
        self.registration.disarm(&self.token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scope::GenericScopeCompletion,
        task::{AnySendScopeRef, RawScope, ScopeRef},
        utils::ownership::ArcOwnership,
    };
    use std::{
        mem::ManuallyDrop,
        sync::{Arc, atomic::AtomicUsize},
        task::{Context, RawWaker, RawWakerVTable},
    };
    use veloq_storage::AtomicStorage;

    fn new_cancel_waiter() -> CancellationWaiter {
        CancellationWaiter::new()
    }

    #[test]
    fn cancel_waiter_registration_rejects_after_local_cancel() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        token.cancel();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        let result = unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) };

        assert_eq!(result, CancelWaiterLinkResult::RejectedByCancellation);
        assert!(!waiter.link.is_linked());
        assert!(token.inner.waiters.lock().is_empty());

        unsafe {
            token.unlink_cancel_waiter(waiter_ptr);
            token.unlink_cancel_waiter(waiter_ptr);
        }
    }

    #[test]
    fn cancel_drains_registered_waiter_and_repeated_unlink_is_safe() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert!(waiter.link.is_linked());
        assert_eq!(token.inner.waiters.lock().len(), 1);

        token.cancel();

        assert!(!waiter.link.is_linked());
        assert!(token.inner.waiters.lock().is_empty());
        unsafe {
            token.unlink_cancel_waiter(waiter_ptr);
            token.unlink_cancel_waiter(waiter_ptr);
        }
    }

    #[test]
    fn registration_after_cancel_publication_is_rejected_under_lock() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        token.inner.cancelled.store(1, Ordering::Release);
        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::RejectedByCancellation
        );

        assert!(!waiter.link.is_linked());
        assert!(token.inner.waiters.lock().is_empty());
    }

    #[test]
    fn repeated_waiter_registration_updates_without_duplicate_link() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waiter = new_cancel_waiter();
        let waiter_ptr = NonNull::from(&waiter);

        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert_eq!(
            unsafe { token.link_cancel_waiter(waiter_ptr, Waker::noop()) },
            CancelWaiterLinkResult::Linked
        );
        assert_eq!(token.inner.waiters.lock().len(), 1);

        unsafe { token.unlink_cancel_waiter(waiter_ptr) };
        assert!(!waiter.link.is_linked());
    }

    #[test]
    fn pending_future_drop_removes_its_waiter() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        {
            let mut future = std::pin::pin!(token.cancelled());
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
            assert_eq!(token.inner.waiters.lock().len(), 1);
        }
        assert!(token.inner.waiters.lock().is_empty());
    }

    #[test]
    fn repeated_poll_keeps_one_waiter_and_refreshes_waker() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let mut future = std::pin::pin!(token.cancelled());
        let first = Waker::noop().clone();
        let second = Waker::noop().clone();
        let mut first_cx = Context::from_waker(&first);
        let mut second_cx = Context::from_waker(&second);

        assert!(matches!(future.as_mut().poll(&mut first_cx), Poll::Pending));
        assert!(matches!(
            future.as_mut().poll(&mut second_cx),
            Poll::Pending
        ));
        assert_eq!(token.inner.waiters.lock().len(), 1);
    }

    #[test]
    fn ready_future_disarms_before_it_is_dropped() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let mut future = std::pin::pin!(token.cancelled());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        token.cancel();
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(token.inner.waiters.lock().is_empty());
    }

    struct WakerCounters {
        wakes: AtomicUsize,
        drops: AtomicUsize,
    }

    static COUNTING_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |ptr| unsafe {
            Arc::increment_strong_count(ptr as *const WakerCounters);
            RawWaker::new(ptr, &COUNTING_WAKER_VTABLE)
        },
        |ptr| unsafe {
            let counters = Arc::from_raw(ptr as *const WakerCounters);
            counters.wakes.fetch_add(1, Ordering::Relaxed);
        },
        |ptr| unsafe {
            let counters = ManuallyDrop::new(Arc::from_raw(ptr as *const WakerCounters));
            counters.wakes.fetch_add(1, Ordering::Relaxed);
        },
        |ptr| unsafe {
            let counters = Arc::from_raw(ptr as *const WakerCounters);
            counters.drops.fetch_add(1, Ordering::Relaxed);
        },
    );

    fn counting_waker(counters: &Arc<WakerCounters>) -> Waker {
        let ptr = Arc::into_raw(Arc::clone(counters)) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &COUNTING_WAKER_VTABLE)) }
    }

    #[test]
    fn drop_removes_waker_and_cancel_does_not_wake_it() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let counters = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });

        {
            let waker = counting_waker(&counters);
            let mut cx = Context::from_waker(&waker);
            {
                let mut future = std::pin::pin!(token.cancelled());
                assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
                assert_eq!(token.inner.waiters.lock().len(), 1);
            }
            assert_eq!(counters.wakes.load(Ordering::Relaxed), 0);
            assert_eq!(counters.drops.load(Ordering::Relaxed), 1);
            token.cancel();
        }

        assert_eq!(counters.wakes.load(Ordering::Relaxed), 0);
        assert_eq!(counters.drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cancellation_uses_the_latest_waker_only() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let first = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });
        let second = Arc::new(WakerCounters {
            wakes: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });
        let mut future = std::pin::pin!(token.cancelled());
        let first_waker = counting_waker(&first);
        let second_waker = counting_waker(&second);
        let mut first_cx = Context::from_waker(&first_waker);
        let mut second_cx = Context::from_waker(&second_waker);

        assert!(matches!(future.as_mut().poll(&mut first_cx), Poll::Pending));
        assert!(matches!(
            future.as_mut().poll(&mut second_cx),
            Poll::Pending
        ));
        token.cancel();

        assert_eq!(first.wakes.load(Ordering::Relaxed), 0);
        assert_eq!(second.wakes.load(Ordering::Relaxed), 1);
        assert!(token.inner.waiters.lock().is_empty());
    }

    #[test]
    fn independent_futures_have_independent_registrations() {
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new();
        let mut second = std::pin::pin!(token.cancelled());
        let mut cx = Context::from_waker(Waker::noop());

        {
            let mut first = std::pin::pin!(token.cancelled());
            assert!(matches!(first.as_mut().poll(&mut cx), Poll::Pending));
            assert!(matches!(second.as_mut().poll(&mut cx), Poll::Pending));
            assert_eq!(token.inner.waiters.lock().len(), 2);
        }

        assert_eq!(token.inner.waiters.lock().len(), 1);

        token.cancel();
        assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(token.inner.waiters.lock().is_empty());
    }

    #[test]
    fn cancelled_before_first_poll_does_not_build_ancestor_nodes() {
        let parent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        parent.cancel();
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new_with_parent(Some(
            scope_ref(&parent),
        ));
        let mut future = std::pin::pin!(token.cancelled());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(())));
        assert_eq!(future.registration.ancestor_count(), 0);
        assert!(parent.cancel_token().inner.waiters.lock().is_empty());
    }

    fn scope_ref(
        completion: &Arc<GenericScopeCompletion<AtomicStorage, ArcOwnership>>,
    ) -> AnySendScopeRef {
        let raw = Arc::into_raw(Arc::clone(completion));
        let dyn_ptr: *const dyn RawScope = raw;
        let scope = unsafe { ScopeRef::new(NonNull::new_unchecked(dyn_ptr as *mut _)) };
        AnySendScopeRef::new(scope)
    }

    #[test]
    fn parent_and_grandparent_sources_are_registered_and_cleared() {
        let grandparent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let parent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(Some(
            scope_ref(&grandparent).into_any(),
        ));
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new_with_parent(Some(
            scope_ref(&parent),
        ));
        let mut future = std::pin::pin!(token.cancelled());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(token.inner.waiters.lock().len(), 1);
        assert_eq!(parent.cancel_token().inner.waiters.lock().len(), 1);
        assert_eq!(grandparent.cancel_token().inner.waiters.lock().len(), 1);

        grandparent.cancel();
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(token.inner.waiters.lock().is_empty());
        assert!(parent.cancel_token().inner.waiters.lock().is_empty());
        assert!(grandparent.cancel_token().inner.waiters.lock().is_empty());
    }

    #[test]
    fn local_cancel_clears_parent_nodes_when_future_is_polled() {
        let parent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new_with_parent(Some(
            scope_ref(&parent),
        ));
        let mut future = std::pin::pin!(token.cancelled());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        token.cancel();
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(parent.cancel_token().inner.waiters.lock().is_empty());
    }

    #[test]
    fn dropping_pending_future_clears_every_parent_source() {
        let grandparent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let parent = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(Some(
            scope_ref(&grandparent).into_any(),
        ));
        let token = GenericCancellationToken::<AtomicStorage, ArcOwnership>::new_with_parent(Some(
            scope_ref(&parent),
        ));

        {
            let mut future = std::pin::pin!(token.cancelled());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
            assert_eq!(parent.cancel_token().inner.waiters.lock().len(), 1);
            assert_eq!(grandparent.cancel_token().inner.waiters.lock().len(), 1);
        }

        assert!(token.inner.waiters.lock().is_empty());
        assert!(parent.cancel_token().inner.waiters.lock().is_empty());
        assert!(grandparent.cancel_token().inner.waiters.lock().is_empty());
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_cancel_and_register_leave_waiter_unlinked() {
        struct SharedWaiter(CancellationWaiter);

        unsafe impl Send for SharedWaiter {}
        unsafe impl Sync for SharedWaiter {}

        loom::model(|| {
            let token = loom::sync::Arc::new(
                GenericCancellationToken::<AtomicStorage, ArcOwnership>::new(),
            );
            let waiter = loom::sync::Arc::new(SharedWaiter(new_cancel_waiter()));
            let register_token = token.clone();
            let register_waiter = waiter.clone();
            let register = loom::thread::spawn(move || {
                let waiter_ptr = NonNull::from(&register_waiter.0);
                unsafe { register_token.link_cancel_waiter(waiter_ptr, Waker::noop()) }
            });

            let cancel_token = token.clone();
            let cancel = loom::thread::spawn(move || {
                cancel_token.cancel();
            });

            let result = register.join().expect("registration thread panicked");
            cancel.join().expect("cancellation thread panicked");

            assert!(matches!(
                result,
                CancelWaiterLinkResult::Linked | CancelWaiterLinkResult::RejectedByCancellation
            ));
            assert!(!waiter.0.link.is_linked());
            assert!(token.inner.waiters.lock().is_empty());
        });
    }
}
