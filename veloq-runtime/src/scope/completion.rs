use crate::runtime::primitives::GenericCancellationToken;
use crate::task::AnyScopeRef;
use crate::utils::ownership::{ArcOwnership, Ownership, RcOwnership};
use crate::utils::storage::{
    AtomicStorage, LocalStorage, StateInt, StateOptionBox, StateOptionPtr, Storage, StrategyType,
};
use std::any::Any;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::Waker;

pub(crate) struct WakerNode {
    pub(crate) waker: Waker,
    pub(crate) next: Option<NonNull<WakerNode>>,
}

/// 作用域级别的完成通知：所有子任务完成后唤醒等待者。
pub struct GenericScopeCompletion<S: Storage, O: Ownership> {
    remaining: S::Usize,
    wakers: S::OptionPtr<WakerNode>,
    cancel_token: GenericCancellationToken<S, O>,
    panic_info: S::OptionBox<dyn Any + Send + 'static>,
    parent: Option<AnyScopeRef>,
}

pub type ScopeCompletion = GenericScopeCompletion<AtomicStorage, ArcOwnership>;
pub type LocalScopeCompletion = GenericScopeCompletion<LocalStorage, RcOwnership>;

impl<S: Storage, O: Ownership> GenericScopeCompletion<S, O> {
    pub fn new(parent: Option<AnyScopeRef>) -> O::Shared<Self> {
        let cross_parent = if let Some(ref p) = parent
            && (S::strategy_type() != StrategyType::Atomic
                || O::strategy_type() != StrategyType::Atomic)
            && let AnyScopeRef::Send(_) = p
        {
            Some(p.clone())
        } else {
            None
        };

        O::new(Self {
            remaining: S::Usize::new(0),
            wakers: S::OptionPtr::new(None),
            cancel_token: GenericCancellationToken::<S, O>::new_with_parent(cross_parent),
            panic_info: S::OptionBox::new(None),
            parent,
        })
    }

    fn drain_wakers(&self) {
        let mut current = self.wakers.swap(None, Ordering::AcqRel);
        while let Some(node_ptr) = current {
            unsafe {
                let node = Box::from_raw(node_ptr.as_ptr());
                node.waker.wake();
                current = node.next;
            }
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
        if let Some(ref parent) = self.parent
            && parent.is_cancelled()
        {
            return true;
        }
        false
    }

    pub fn cancel_token(&self) -> &GenericCancellationToken<S, O> {
        &self.cancel_token
    }

    pub fn add_task(&self) {
        self.remaining.fetch_add(1, Ordering::AcqRel);
    }

    pub fn task_done(&self) {
        let remaining = self.remaining.fetch_sub(1, Ordering::AcqRel) - 1;
        if remaining == 0 {
            self.drain_wakers();
        }
    }

    pub fn register(&self, waker: &Waker) {
        if self.remaining.load(Ordering::Acquire) == 0 {
            waker.wake_by_ref();
            return;
        }

        let mut node = Box::new(WakerNode {
            waker: waker.clone(),
            next: None,
        });
        let mut current = self.wakers.load(Ordering::Acquire);
        loop {
            node.next = current;
            let node_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(node)) };
            match self.wakers.compare_exchange_weak(
                current,
                Some(node_ptr),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    node = unsafe { Box::from_raw(node_ptr.as_ptr()) };
                    current = actual;
                }
            }
        }

        if self.remaining.load(Ordering::Acquire) == 0 {
            self.drain_wakers();
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

    pub fn parent(&self) -> &Option<crate::task::AnyScopeRef> {
        &self.parent
    }
}

impl<S: Storage, O: Ownership> Drop for GenericScopeCompletion<S, O> {
    fn drop(&mut self) {
        let mut current = self.wakers.swap(None, Ordering::Relaxed);
        while let Some(node_ptr) = current {
            unsafe {
                let node = Box::from_raw(node_ptr.as_ptr());
                current = node.next;
            }
        }

        if let Some(panic_info) = self.take_panic()
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic_info);
        }
    }
}

impl<S: Storage, O: Ownership + 'static> crate::task::RawScope for GenericScopeCompletion<S, O> {
    #[inline]
    fn task_done(&self) {
        self.task_done();
    }

    #[inline]
    fn cancel(&self) {
        self.cancel();
    }

    #[inline]
    fn report_panic(&self, payload: Box<dyn std::any::Any + Send + 'static>) {
        self.report_panic(payload);
    }

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }

    #[inline]
    fn try_link_child(&self, child_token: &crate::task::ErasedCancellationToken) -> bool {
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
        self.parent().clone()
    }

    #[inline]
    fn register_cancel_waker(&self, waker: &Waker) {
        self.cancel_token().register_waker(waker);
    }

    #[inline]
    unsafe fn clone_raw(&self) -> NonNull<dyn crate::task::RawScope> {
        let ptr = self as *const Self;
        unsafe { O::increment_strong_count(ptr) };
        let dyn_ptr: *const dyn crate::task::RawScope = ptr;
        unsafe { NonNull::new_unchecked(dyn_ptr as *mut _) }
    }

    #[inline]
    unsafe fn drop_raw(&self) {
        let ptr = self as *const Self;
        unsafe { O::decrement_strong_count(ptr) };
    }
}
