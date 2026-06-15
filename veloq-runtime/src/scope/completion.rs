use crate::{
    runtime::primitives::GenericCancellationToken,
    task::{AnyScopeRef, ErasedCancellationToken, RawScope, ScopeParent, ScopeStorage},
    utils::ownership::{ArcOwnership, Ownership, RcOwnership},
};
use std::{
    any::Any, marker::PhantomData, pin::Pin, ptr::NonNull, sync::atomic::Ordering, task::Waker,
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

/// 作用域级别的完成通知：所有子任务完成后唤醒等待者。
pub struct GenericScopeCompletion<S: ScopeStorage, O: Ownership> {
    remaining: S::Usize,
    wakers: S::Lock<LinkedList<ScopeWakerAdapter<S>>>,
    cancel_token: GenericCancellationToken<S, O>,
    panic_info: S::OptionBox<dyn Any + Send + 'static>,
    parent: S::Parent,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: ScopeStorage, O: Ownership> GenericScopeCompletion<S, O> {
    pub fn new(parent: Option<AnyScopeRef>) -> O::Shared<Self> {
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
            panic_info: S::OptionBox::new(None),
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

    pub fn cancel(&self) {
        self.cancel_token.cancel();
        self.drain_wakers();
    }

    pub fn is_cancelled(&self) -> bool {
        if self.cancel_token.is_cancelled() {
            return true;
        }
        if self.parent.is_cancelled() {
            return true;
        }
        false
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
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

    pub(crate) fn register(&self, mut node: Pin<&mut ScopeWakerNode<S>>, waker: &Waker) {
        if self.remaining.load(Ordering::Acquire) == 0 || self.is_cancelled() {
            waker.wake_by_ref();
            return;
        }

        let mut wakers = self.wakers.lock();
        if self.remaining.load(Ordering::Acquire) == 0 || self.is_cancelled() {
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

    pub fn is_done(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    pub fn report_panic(&self, payload: Box<dyn Any + Send + 'static>) {
        let _ = self
            .panic_info
            .compare_exchange_none(payload, Ordering::AcqRel, Ordering::Acquire);
    }

    pub fn take_panic(&self) -> Option<Box<dyn Any + Send + 'static>> {
        self.panic_info.take(Ordering::AcqRel)
    }

    pub fn parent(&self) -> Option<AnyScopeRef> {
        self.parent.as_any()
    }
}

impl<S: ScopeStorage, O: Ownership> Drop for GenericScopeCompletion<S, O> {
    fn drop(&mut self) {
        {
            let mut wakers = self.wakers.lock();
            while wakers.pop_front().is_some() {}
        }

        if let Some(panic_info) = self.take_panic()
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic_info);
        }
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
