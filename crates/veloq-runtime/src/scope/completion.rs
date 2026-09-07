use crate::{
    runtime::primitives::GenericCancellationToken,
    task::{
        AnyScopeRef, ErasedCancellationToken, RawScope, ScopeCancelWaiter, ScopeParent,
        ScopeStorage,
    },
    utils::ownership::{ArcOwnership, Ownership, RcOwnership},
};
use std::{
    any::Any,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    ptr::NonNull,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, StateOptionBox, StrategyType,
};

pub(crate) struct ScopeWakerNode<S: ScopeStorage> {
    pub(crate) waker: Waker,
    pub(crate) link: Link,
    marker: PhantomData<S>,
}

intrusive_adapter!(pub(crate) ScopeWakerAdapter<S> = ScopeWakerNode<S> { link: Link } where S: ScopeStorage);

impl<S: ScopeStorage> ScopeWakerNode<S> {
    fn new(waker: &Waker) -> Self {
        Self {
            waker: waker.clone(),
            link: Link::new(),
            marker: PhantomData,
        }
    }

    /// 尚未拿到真正 waker 的空节点：`register` 第一次被调用时才填上。
    fn detached() -> Self {
        Self {
            waker: Waker::noop().clone(),
            link: Link::new(),
            marker: PhantomData,
        }
    }
}

pub(crate) struct ScopeCompletionRegistration<'a, S: ScopeStorage, O: Ownership> {
    completion: &'a GenericScopeCompletion<S, O>,
    node: Pin<Box<ScopeWakerNode<S>>>,
}

impl<'a, S: ScopeStorage, O: Ownership> ScopeCompletionRegistration<'a, S, O> {
    pub(crate) fn new(completion: &'a GenericScopeCompletion<S, O>, waker: &Waker) -> Self {
        Self {
            completion,
            node: Box::pin(ScopeWakerNode::new(waker)),
        }
    }

    pub(crate) fn register(&mut self, waker: &Waker) {
        self.completion.register(self.node.as_mut(), waker);
    }
}

impl<S: ScopeStorage, O: Ownership> Drop for ScopeCompletionRegistration<'_, S, O> {
    fn drop(&mut self) {
        let node = unsafe { NonNull::from(self.node.as_mut().get_unchecked_mut()) };
        unsafe {
            self.completion.remove_waiter(node);
        }
    }
}

/// 等待一个作用域内全部子任务结束的 future。
///
/// 「等待」必须由 waker 驱动。如果 `wait_all` 同步跑一整个调度循环直到 `remaining == 0`，
/// await 一个 handle 实际等的是整个作用域、栈深度会随作用域嵌套线性增长、外层 `select!` /
/// 超时永远拿不到控制权，非 worker 线程上还会直接 panic。驱动完成队列的职责只属于 worker 顶层循环。
pub(crate) struct ScopeJoinFuture<'a, S: ScopeStorage, O: Ownership> {
    completion: &'a GenericScopeCompletion<S, O>,
    /// 侵入式节点，入链后地址必须稳定 —— 靠 `!Unpin` 保证。
    node: ScopeWakerNode<S>,
    _pin: PhantomPinned,
}

impl<'a, S: ScopeStorage, O: Ownership> ScopeJoinFuture<'a, S, O> {
    pub(crate) fn new(completion: &'a GenericScopeCompletion<S, O>) -> Self {
        Self {
            completion,
            node: ScopeWakerNode::detached(),
            _pin: PhantomPinned,
        }
    }
}

impl<'a, S: ScopeStorage, O: Ownership> Future for ScopeJoinFuture<'a, S, O> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = unsafe { self.get_unchecked_mut() };
        let completion = this.completion;
        if completion.is_done() {
            return Poll::Ready(());
        }

        let node = unsafe { Pin::new_unchecked(&mut this.node) };
        completion.register(node, cx.waker());

        // 入链与「最后一个子任务结算」之间的窗口：`register` 之后再复查一次。
        if completion.is_done() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<S: ScopeStorage, O: Ownership> Drop for ScopeJoinFuture<'_, S, O> {
    fn drop(&mut self) {
        unsafe { self.completion.remove_waiter(NonNull::from(&self.node)) };
    }
}

/// 作用域级别的完成通知：所有子任务完成后唤醒等待者。
pub struct GenericScopeCompletion<S: ScopeStorage, O: Ownership> {
    remaining: S::Usize,
    wakers: S::Lock<LinkedList<ScopeWakerAdapter<S>>>,
    cancel_token: GenericCancellationToken<S, O>,
    panic_info: S::OptionFatBox<dyn Any + Send + 'static>,
    parent: S::Parent,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: ScopeStorage, O: Ownership> GenericScopeCompletion<S, O> {
    pub(crate) fn new(parent: Option<AnyScopeRef>) -> O::Shared<Self> {
        let parent = S::Parent::from_any(parent);
        let cross_parent = if S::strategy_type() != StrategyType::Atomic
            || O::strategy_type() != StrategyType::Atomic
        {
            parent.as_send()
        } else {
            None
        };

        O::new(Self {
            remaining: S::Usize::new(0),
            wakers: S::Lock::new(LinkedList::new(ScopeWakerAdapter::<S>::new())),
            cancel_token: GenericCancellationToken::<S, O>::new_with_parent(cross_parent),
            panic_info: S::OptionFatBox::new(None),
            parent,
        })
    }

    fn drain_wakers(&self) {
        let mut ready = Vec::new();
        {
            let mut wakers = self.wakers.lock();
            while let Some(node) = wakers.pop_front() {
                ready.push(node.as_ref().get_ref().waker.clone());
            }
        }

        for waker in ready {
            waker.wake();
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancel_token.cancel();
        self.drain_wakers();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        if self.cancel_token.is_cancelled() {
            return true;
        }
        if self.parent.is_cancelled() {
            return true;
        }
        false
    }

    pub(crate) fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        &self.cancel_token
    }

    pub(crate) fn register_task(&self) {
        self.remaining.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn settle_task(&self) {
        loop {
            let prev = self.remaining.load(Ordering::Acquire);
            if prev == 0 {
                return;
            }
            if self
                .remaining
                .compare_exchange(prev, prev - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if prev == 1 {
                    self.drain_wakers();
                }
                return;
            }
        }
    }

    /// 注册（或刷新）一个「全部子任务结束」的等待者。
    ///
    /// 判据只有 `remaining == 0`，**不含**取消状态：取消只是请求，子任务还在跑，而结构化
    /// 并发要求等到它们真正停下。把「已取消」也当成立即唤醒会让 [`ScopeJoinFuture`] 每次
    /// poll 都被立刻唤醒，退化成忙转。取消本身会走 `cancel()` → `drain_wakers()`，等待者
    /// 依然会被唤醒一次去复查。
    pub(crate) fn register(&self, mut node: Pin<&mut ScopeWakerNode<S>>, waker: &Waker) {
        if self.remaining.load(Ordering::Acquire) == 0 {
            waker.wake_by_ref();
            return;
        }

        let mut wakers = self.wakers.lock();
        if self.remaining.load(Ordering::Acquire) == 0 {
            drop(wakers);
            waker.wake_by_ref();
            return;
        }

        unsafe {
            let node_ref = node.as_mut().get_unchecked_mut();
            if !node_ref.waker.will_wake(waker) {
                node_ref.waker = waker.clone();
            }
            if !node_ref.link.is_linked() {
                wakers.push_back(node);
            }
        }
    }

    /// # Safety
    ///
    /// `node` 必须指向先前通过 `register` 注册到该 completion 的同一个节点。
    pub(crate) unsafe fn remove_waiter(&self, node: NonNull<ScopeWakerNode<S>>) {
        let mut wakers = self.wakers.lock();
        if unsafe { node.as_ref().link.is_linked() } {
            unsafe {
                let mut cursor = wakers.cursor_mut_from_ptr(node);
                cursor.remove();
            }
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    pub(crate) fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        let _ = self
            .panic_info
            .compare_exchange_none(payload, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(crate) fn take_panic(&self) -> Option<Box<dyn Any + Send + 'static>> {
        self.panic_info.take(Ordering::AcqRel)
    }

    pub fn parent(&self) -> Option<AnyScopeRef> {
        self.parent.as_any()
    }
}

/// panic payload 的抛出点只有 `wait_all()`（以及作用域析构时的上交路径），**不在这里**。
///
/// completion 由 `Arc`/`Rc` 持有，最后一个引用可能落在任意 worker 线程上（task header
/// 通过 `ScopeRef` 持有引用），在这里 `resume_unwind` 等于把 panic 抛到一个与该作用域
/// 毫无关系的线程上，诊断信息完全失真。走到这里还残留 payload 说明作用域析构时也没能把
/// 它交出去（例如根作用域正在 unwind），只能丢弃。
impl<S: ScopeStorage, O: Ownership> Drop for GenericScopeCompletion<S, O> {
    fn drop(&mut self) {
        let mut wakers = self.wakers.lock();
        while wakers.pop_front().is_some() {}
    }
}

impl<S: ScopeStorage, O: Ownership + 'static> RawScope for GenericScopeCompletion<S, O> {
    #[inline]
    fn task_done(&self) {
        self.settle_task();
    }

    #[inline]
    fn cancel(&self) {
        self.cancel();
    }

    #[inline]
    fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        self.report_panic(payload);
    }

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }

    #[inline]
    fn try_link_child(&self, child_token: &ErasedCancellationToken) -> bool {
        if child_token.s_type != S::strategy_type() || child_token.o_type != O::strategy_type() {
            return false;
        }
        unsafe {
            self.cancel_token()
                .try_link_child_raw(child_token.ptr.as_ptr());
        }
        true
    }

    #[inline]
    fn parent(&self) -> Option<AnyScopeRef> {
        self.parent()
    }

    #[inline]
    fn register_cancel_waker(&self, waker: &Waker) {
        self.cancel_token().register_waker(waker);
    }

    #[inline]
    unsafe fn link_cancel_waiter(&self, waiter: NonNull<ScopeCancelWaiter>, waker: &Waker) -> bool {
        unsafe { self.cancel_token().link_cancel_waiter(waiter, waker) }
    }

    #[inline]
    unsafe fn unlink_cancel_waiter(&self, waiter: NonNull<ScopeCancelWaiter>) {
        unsafe { self.cancel_token().unlink_cancel_waiter(waiter) }
    }

    #[inline]
    unsafe fn clone_raw(&self) -> NonNull<dyn RawScope> {
        let ptr = self as *const Self;
        unsafe { O::increment_strong_count(ptr) };
        let dyn_ptr: *const dyn RawScope = ptr;
        unsafe { NonNull::new_unchecked(dyn_ptr as *mut _) }
    }

    #[inline]
    unsafe fn drop_raw(&self) {
        let ptr = self as *const Self;
        unsafe { O::decrement_strong_count(ptr) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::ownership::ArcOwnership;
    use veloq_storage::AtomicStorage;

    #[test]
    fn duplicate_settle_task_does_not_underflow() {
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        completion.register_task();
        completion.settle_task();
        assert!(completion.is_done());
        completion.settle_task();
        assert!(completion.is_done());
    }
}
