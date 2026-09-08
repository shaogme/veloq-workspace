use crate::{
    error::{Result, RuntimeError},
    runtime::{EnqueuePinnedOutcome, RuntimeSharedBase, primitives::CancelWaiterLinkResult},
    task::{
        LocalWakeHeaderGuard, RawScope, ScopeCancelWaiter, ScopeRef, SendTaskRef, TaskHandleRef,
        TaskWakeToken, nodes::TaskStorage,
    },
};
use diagweave::prelude::*;
use std::{
    cell::UnsafeCell,
    marker::{PhantomData, PhantomPinned},
    mem::ManuallyDrop,
    pin::Pin,
    ptr::{self, NonNull},
    sync::{Arc, atomic::Ordering},
    task::{RawWaker, RawWakerVTable, Waker},
};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_storage::{
    AtomicStorage, LocalStorage, StateInt, StateLock, Storage, StrategyType, ThreadSafeStorage,
};

pub(crate) const STATE_COMPLETED: usize = 1 << 0;
pub(crate) const STATE_QUEUED: usize = 1 << 1;
pub(crate) const STATE_READY: usize = 1 << 2;
pub(crate) const STATE_CANCELLED: usize = 1 << 3;
pub(crate) const STATE_POLLING: usize = 1 << 4;
pub(crate) const STATE_WOKEN: usize = 1 << 5;
pub(crate) const STATE_PINNED: usize = 1 << 6;
pub(crate) const STATE_SCOPE_OBLIGATED: usize = 1 << 7;
pub(crate) const STATE_SCOPE_ACKED: usize = 1 << 8;
/// 任务已把自己的等待节点挂到所属 scope 的取消队列上。
pub(crate) const STATE_CANCEL_ARMED: usize = 1 << 9;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    Proceed,
    Yield,
    Complete,
}

pub struct TaskVTable<S: Storage> {
    pub wake: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
    pub wake_by_ref: unsafe fn(data: &GenericTaskHeader<S>),
    pub poll: unsafe fn(data: &GenericTaskHeader<S>, worker_id: usize) -> Result<bool>,
    pub drop: unsafe fn(data: NonNull<GenericTaskHeader<S>>),
}

pub(crate) struct GenericWakerNode<S: Storage> {
    pub(crate) waker: Waker,
    pub(crate) link: Link,
    pub(crate) marker: PhantomData<S>,
    pub(crate) _pin: PhantomPinned,
}

intrusive_adapter!(pub(crate) WakerAdapter<S> = GenericWakerNode<S> { link: Link } where S: Storage);

pub struct GenericTaskHeader<S: Storage> {
    state: S::Usize,
    ref_count: S::Usize,
    wakers: S::Lock<LinkedList<WakerAdapter<S>>>,
    wake_token: Arc<TaskWakeToken<S>>,
    cached_waker: UnsafeCell<Option<Waker>>,
    scope: UnsafeCell<ScopeRef<S>>,
    /// 本任务在所属 scope 取消队列中的等待节点；由 scope 的取消令牌锁保护。
    cancel_waiter: ScopeCancelWaiter,
    runtime: UnsafeCell<Option<NonNull<RuntimeSharedBase>>>,
    worker_id: S::Usize,
    vtable: &'static TaskVTable<S>,
}

unsafe impl<S: ThreadSafeStorage> Send for GenericTaskHeader<S> {}
unsafe impl<S: ThreadSafeStorage> Sync for GenericTaskHeader<S> {}

impl<S: Storage> GenericTaskHeader<S> {
    pub fn new(
        vtable: &'static TaskVTable<S>,
        runtime: &RuntimeSharedBase,
        worker_id: usize,
        scope: ScopeRef<S>,
    ) -> Self {
        let this = Self::new_placeholder(vtable);
        unsafe {
            this.initialize(runtime, worker_id, scope);
        }
        this
    }

    pub(crate) fn new_placeholder(vtable: &'static TaskVTable<S>) -> Self {
        Self {
            state: S::Usize::new(0),
            ref_count: S::Usize::new(1),
            wakers: S::Lock::new(LinkedList::new(WakerAdapter::<S>::new())),
            wake_token: Arc::new(TaskWakeToken::new()),
            cached_waker: UnsafeCell::new(None),
            scope: UnsafeCell::new(ScopeRef::dummy()),
            cancel_waiter: ScopeCancelWaiter::new(),
            runtime: UnsafeCell::new(None),
            worker_id: S::Usize::new(0),
            vtable,
        }
    }

    /// # Safety
    ///
    /// 必须保证该方法在任务被 enqueue 并发布给其他线程前被调用，且在生命周期内仅调用一次。
    pub(crate) unsafe fn initialize(
        &self,
        runtime: &RuntimeSharedBase,
        worker_id: usize,
        scope: ScopeRef<S>,
    ) {
        unsafe {
            *self.runtime.get() = Some(NonNull::from(runtime));
            *self.scope.get() = scope;
        }
        if S::strategy_type() == StrategyType::Local {
            self.wake_token
                .bind_local_target(runtime.local_wake_target(worker_id));
        }
        self.worker_id.store(worker_id, Ordering::Release);
    }

    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_COMPLETED != 0
    }

    #[inline]
    pub(crate) fn is_pinned(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_PINNED != 0
    }

    #[inline]
    pub fn set_pinned(&self) {
        self.state.fetch_or(STATE_PINNED, Ordering::Release);
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        if self.state.load(Ordering::Acquire) & STATE_CANCELLED != 0 {
            return true;
        }
        self.with_scope(|scope| scope.is_cancelled())
    }

    /// 仅置位取消标记，**不唤醒**任务。
    ///
    /// 只用于「任务马上就会被 poll 到」或「唤醒路径本身失败」的场合（后者若在这里唤醒
    /// 会无限递归）。其余场合一律用 [`Self::cancel_and_wake`]。
    #[inline]
    pub(crate) fn cancel(&self) {
        self.state.fetch_or(STATE_CANCELLED, Ordering::Release);
    }

    /// 请求取消，并唤醒任务让它去观察这个状态。
    ///
    /// 取消在本运行时是协作式的：任务只有被 poll 到才会看到 `STATE_CANCELLED`。一个正
    /// 挂起的任务若不被唤醒，就要等到下一次自然唤醒（可能永远不来）才会结束，而
    /// `wait_all` / 作用域析构都要等它结束。
    #[inline]
    pub(crate) fn cancel_and_wake(&self) {
        let old = self.state.fetch_or(STATE_CANCELLED, Ordering::AcqRel);
        if old & (STATE_CANCELLED | STATE_COMPLETED) != 0 {
            return;
        }
        self.wake_by_ref();
    }

    /// 把任务自己挂到所属 scope 的取消队列上，使 scope 取消能唤醒它。
    ///
    /// 在任务第一次返回 `Pending`（即它不再位于任何队列中）时调用一次即可：waker 由同一个
    /// `TaskWakeToken` 派生，跨 poll 稳定。armed 状态在注册协议前发布，表示任务可能需要
    /// 清理节点，而不是证明节点已经入链。注册拒绝时仍执行幂等清理。
    pub(crate) fn arm_scope_cancel_waiter(&self, waker: &Waker) -> bool {
        let old_state = self.state.fetch_or(STATE_CANCEL_ARMED, Ordering::AcqRel);
        if old_state & STATE_CANCEL_ARMED != 0 {
            return true;
        }

        let waiter = NonNull::from(&self.cancel_waiter);
        let result = unsafe {
            self.scope_completion_ref()
                .link_cancel_waiter(waiter, waker)
        };
        match result {
            CancelWaiterLinkResult::Linked => true,
            CancelWaiterLinkResult::RejectedByCancellation => {
                self.disarm_scope_cancel_waiter();
                false
            }
        }
    }

    /// 摘除取消等待节点。必须在 header 析构前完成：`Link` 在仍然入链时析构会 panic。
    fn disarm_scope_cancel_waiter(&self) {
        if self.state.load(Ordering::Acquire) & STATE_CANCEL_ARMED == 0 {
            return;
        }
        let waiter = NonNull::from(&self.cancel_waiter);
        unsafe { self.scope_completion_ref().unlink_cancel_waiter(waiter) };
        self.state.fetch_and(!STATE_CANCEL_ARMED, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn try_mark_queued(&self) -> bool {
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
    pub(crate) fn clear_queued(&self) -> bool {
        let old_state = self.state.fetch_and(!STATE_QUEUED, Ordering::Release);
        if old_state & STATE_QUEUED != 0 && self.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            return true;
        }
        false
    }

    /// 尝试进入 Poll 状态。
    #[inline]
    pub(crate) fn try_enter_poll(&self) -> PollStatus {
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
    pub(crate) fn exit_poll_to_pending(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let mut new_state = state & !STATE_POLLING;
            let was_woken = state & STATE_WOKEN != 0;
            if was_woken {
                new_state &= !STATE_WOKEN;
                new_state |= STATE_POLLING;
            }

            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return was_woken,
                Err(s) => state = s,
            }
        }
    }

    /// 显式退出 Poll 状态，不检查唤醒标记。
    #[inline]
    pub(crate) fn exit_poll(&self) {
        self.state.fetch_and(!STATE_POLLING, Ordering::Release);
    }

    /// 注册（或刷新）一个完成通知节点。
    ///
    /// 语义与 `GenericScopeCompletion::register` 对齐：节点已在链表中时只更新 waker，
    /// **绝不重复 `push_back`** —— 重复入链会覆盖节点的 prev/next，把链表变成自环或
    /// 断链，随后 `notify_completion_wakers` 的遍历会死循环。
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `node` remains valid and pinned at its current memory location
    /// until it is either woken or explicitly removed from the task's waker list.
    pub(crate) unsafe fn register_completion(
        &self,
        mut node: Pin<&mut GenericWakerNode<S>>,
        waker: &Waker,
    ) {
        if self.is_completed() {
            waker.wake_by_ref();
            return;
        }

        let mut wakers = self.wakers.lock();
        if self.is_completed() {
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

    /// 标记任务为完成状态，并通知所有等待完成的 waker。
    pub fn mark_completed_and_notify(&self) {
        let old_state = self
            .state
            .fetch_or(STATE_READY | STATE_COMPLETED, Ordering::AcqRel);
        if old_state & STATE_COMPLETED != 0 {
            return;
        }

        self.disarm_scope_cancel_waiter();
        self.notify_completion_wakers();
    }

    /// 摘下并唤醒全部完成等待者。
    ///
    /// waker 一律在**释放锁之后**才被调用：`wake` 会执行任意用户/运行时代码，可能
    /// 重入 `register_completion` / `remove_waker`，持锁唤醒有重入死锁风险。
    fn notify_completion_wakers(&self) {
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

    #[inline]
    pub fn set_worker_id(&self, worker_id: usize) {
        self.worker_id.store(worker_id, Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn worker_id(&self) -> usize {
        self.worker_id.load(Ordering::Acquire)
    }

    pub(crate) fn claim_scope_obligation(&self) {
        let old = self.state.fetch_or(STATE_SCOPE_OBLIGATED, Ordering::AcqRel);
        debug_assert!(
            old & STATE_SCOPE_OBLIGATED == 0,
            "duplicate scope obligation claim"
        );
    }

    #[inline]
    pub fn has_scope_obligation(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_SCOPE_OBLIGATED != 0
    }

    #[inline]
    pub(crate) fn is_scope_acknowledged(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_SCOPE_ACKED != 0
    }

    pub(crate) fn acknowledge_completion(&self) {
        let old = self.state.fetch_or(STATE_SCOPE_ACKED, Ordering::AcqRel);
        if old & STATE_SCOPE_ACKED != 0 {
            debug_assert!(false, "duplicate acknowledge_completion");
            return;
        }
        debug_assert!(
            old & STATE_SCOPE_OBLIGATED != 0,
            "acknowledge_completion without scope obligation"
        );
        self.scope_completion_ref().task_done();
    }

    /// `acknowledge_completion` 的幂等版本：只有真正翻转 ACK 标记的一方结算 scope。
    ///
    /// 用于「任务被放弃」这类防御路径 —— 那里无法静态断定义务是否已被结算，
    /// 因此不能使用带 `debug_assert!` 的 `acknowledge_completion`。
    pub(crate) fn try_acknowledge_completion(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & STATE_SCOPE_OBLIGATED == 0 || state & STATE_SCOPE_ACKED != 0 {
                return false;
            }
            match self.state.compare_exchange_weak(
                state,
                state | STATE_SCOPE_ACKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.scope_completion_ref().task_done();
                    return true;
                }
                Err(s) => state = s,
            }
        }
    }

    /// 任务在入队失败 / 入队前置校验失败后被放弃。
    ///
    /// 这条路径上任务永远不会被 poll，也没有任何队列引用会被归还，因此必须在这里
    /// 终结它：标记取消 + 完成、唤醒 join 等待者、归还任务自身的引用，并在引用计数
    /// 归零时结算 scope 义务。否则 scope 的 `remaining` 永不归零，`wait_all` 会永久
    /// 挂起。
    ///
    /// 调用者必须先归还 `STATE_QUEUED` 持有的引用（`clear_queued`）；仍处于
    /// `QUEUED` 或已 `COMPLETED` 的任务由出队 / 完成路径负责结算，此处直接跳过。
    pub(crate) fn abandon_before_enqueue(&self) {
        let old = self.state.fetch_or(
            STATE_CANCELLED | STATE_READY | STATE_COMPLETED,
            Ordering::AcqRel,
        );
        if old & (STATE_COMPLETED | STATE_QUEUED) != 0 {
            return;
        }

        self.disarm_scope_cancel_waiter();
        self.notify_completion_wakers();
        if self.decrement_ref_count() {
            self.try_acknowledge_completion();
        }
    }

    /// 任务自身是否被显式取消（不考虑所属 scope 的取消状态）。
    #[inline]
    pub(crate) fn is_locally_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_CANCELLED != 0
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_READY != 0
    }

    pub(crate) fn create_waker(&self, vtable: &'static RawWakerVTable) -> &Waker {
        let cached_waker = unsafe { &mut *self.cached_waker.get() };
        if cached_waker.is_none() {
            self.wake_token.bind_header(NonNull::from(self));
            let data = Arc::into_raw(Arc::clone(&self.wake_token)) as *const ();
            *cached_waker = Some(unsafe { Waker::from_raw(RawWaker::new(data, vtable)) });
        }
        cached_waker
            .as_ref()
            .expect("task waker must be initialized before use")
    }

    #[inline]
    pub(crate) fn decrement_ref_count(&self) -> bool {
        self.ref_count.fetch_sub(1, Ordering::AcqRel) == 1
    }

    #[inline]
    pub(crate) fn with_scope<R>(&self, callback: impl FnOnce(&dyn RawScope) -> R) -> R {
        let scope = unsafe { &*self.scope.get() };
        unsafe { callback(scope.as_ref()) }
    }

    #[inline]
    pub(crate) fn scope_completion_ref(&self) -> ScopeRef<S> {
        unsafe { (*self.scope.get()).clone() }
    }

    #[inline]
    pub(crate) fn runtime(&self) -> Result<&RuntimeSharedBase> {
        unsafe { *self.runtime.get() }
            .map(|ptr| unsafe { ptr.as_ref() })
            .ok_or(RuntimeError::MissingRuntimeBinding)
            .trans()
    }

    #[inline]
    pub(crate) fn notify_runtime_active(&self) -> Result<()> {
        let runtime = self.runtime()?;
        runtime.idle.event_count.notify();
        runtime.wake_worker(self.worker_id())?;
        Ok(())
    }

    /// 唤醒任务（消耗所有权）。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 的有效 non-null 指针。
    #[inline]
    pub unsafe fn wake(self_ptr: NonNull<Self>) {
        let vtable = unsafe { self_ptr.as_ref().vtable };
        unsafe { (vtable.wake)(self_ptr) };
    }

    /// 通过引用唤醒任务。
    #[inline]
    pub(crate) fn wake_by_ref(&self) {
        unsafe { (self.vtable.wake_by_ref)(self) };
    }

    /// 执行任务的 poll。
    ///
    /// # Safety
    /// 调用者必须确保 `self` 处于可被 poll 的正确状态下。
    #[inline]
    pub(crate) unsafe fn poll(&self, worker_id: usize) -> Result<bool> {
        unsafe { (self.vtable.poll)(self, worker_id) }
    }

    /// 释放任务。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 且未被释放的有效 non-null 指针。
    #[inline]
    pub unsafe fn drop_task(self_ptr: NonNull<Self>) {
        let vtable = unsafe { self_ptr.as_ref().vtable };
        unsafe { (vtable.drop)(self_ptr) };
    }

    /// 入队当前任务。
    ///
    /// # Safety
    /// `self_ptr` 必须是指向 `self` 的有效 non-null 指针。
    pub(crate) unsafe fn enqueue_self(&self, self_ptr: NonNull<Self>) -> Result<()>
    where
        S: TaskStorage,
    {
        let runtime = self.runtime()?;
        if !S::IS_LOCAL && self.is_pinned() {
            let task = unsafe { SendTaskRef::from_header(self_ptr.as_ptr() as *const _) };
            match runtime.enqueue_pinned(self.worker_id(), task) {
                EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => {}
                EnqueuePinnedOutcome::AbortedAcknowledged
                | EnqueuePinnedOutcome::AlreadySettled => {}
                // 唤醒一个已完成的任务时**不能**在这里结算。
                //
                // `NeedsCallerSettle` 只是一次观察：「已完成、尚未结算」。这个状态在
                // `TaskFinalizer::finalize` 里转瞬即逝 —— 它先 `mark_completed_and_notify`，
                // 之后才按引用计数结算。在窗口里替它结算有两重错：`remaining` 可能提前归零，
                // 让 `wait_all` 放行、作用域析构、arena 连着任务节点一起释放，而 `finalize`
                // 还在往那块内存里写（`exit_poll`）；随后 finalize 自己的结算也成了重复结算。
                //
                // 任务义务本来就由引用计数协议兜底：最后一个引用（任务自身或某个队列引用）
                // 归还时一定会结算。非 pinned 的 `enqueue_send` / `enqueue_local` 在
                // `is_completed()` 时也是直接返回，这里与它们保持一致。
                EnqueuePinnedOutcome::NeedsCallerSettle => {}
            }
            return Ok(());
        }
        S::enqueue(runtime, self.worker_id(), self_ptr)?;
        Ok(())
    }

    /// 尝试将一个 waker 节点从任务的 waker 列表中移除。
    ///
    /// 这里**不能**用 `is_completed()` 做提前返回：`mark_completed_and_notify` 先置位
    /// `COMPLETED`、之后才拿锁清链，窗口内提前返回会把一个仍然在链表里的节点留下，
    /// 等 arena 释放后链表中就是悬垂指针。正确性由锁 + `is_linked()` 保证。
    ///
    /// # Safety
    /// `node` 指向的节点必须是由 `register_completion` 注册的相同节点。
    pub(crate) unsafe fn remove_waker(&self, node: NonNull<GenericWakerNode<S>>) {
        let mut wakers = self.wakers.lock();
        if unsafe { node.as_ref().link.is_linked() } {
            unsafe {
                let mut cursor = wakers.cursor_mut_from_ptr(node);
                cursor.remove();
            }
        }
    }
}

impl GenericTaskHeader<AtomicStorage> {
    /// # Safety
    ///
    /// `waker` 必须由 send task 的 `create_waker` 创建，且 `vtable` 必须匹配。
    pub(crate) unsafe fn from_waker<'a>(
        waker: &'a Waker,
        vtable: &'static RawWakerVTable,
    ) -> Option<&'a Self> {
        if !ptr::eq(waker.vtable(), vtable) {
            return None;
        }
        let token = unsafe { &*(waker.data() as *const TaskWakeToken<AtomicStorage>) };
        token.header()
    }
}

impl GenericTaskHeader<LocalStorage> {
    /// # Safety
    ///
    /// `waker` 必须由 local task 的 `create_waker` 创建，且 `vtable` 必须匹配。只有 owner
    /// worker 可以取得 header；foreign thread 返回 `None`，不会读取 local header。
    pub(crate) unsafe fn local_from_waker<'a>(
        waker: &'a Waker,
        vtable: &'static RawWakerVTable,
    ) -> Option<LocalWakeHeaderGuard<'a>> {
        if !ptr::eq(waker.vtable(), vtable) {
            return None;
        }
        let token = unsafe { &*(waker.data() as *const TaskWakeToken<LocalStorage>) };
        token.local_header_on_owner()
    }
}

pub static INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| {
        unsafe {
            Arc::increment_strong_count(data as *const TaskWakeToken<AtomicStorage>);
        }
        RawWaker::new(data, &INTRUSIVE_WAKER_VTABLE)
    },
    |data| unsafe {
        let token = Arc::from_raw(data as *const TaskWakeToken<AtomicStorage>);
        token.wake_impl();
    },
    |data| unsafe {
        let token = ManuallyDrop::new(Arc::from_raw(data as *const TaskWakeToken<AtomicStorage>));
        token.wake_impl();
    },
    |data| unsafe {
        drop(Arc::from_raw(data as *const TaskWakeToken<AtomicStorage>));
    },
);

pub static LOCAL_INTRUSIVE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| {
        unsafe {
            Arc::increment_strong_count(data as *const TaskWakeToken<LocalStorage>);
        }
        RawWaker::new(data, &LOCAL_INTRUSIVE_WAKER_VTABLE)
    },
    |data| unsafe {
        let token = Arc::from_raw(data as *const TaskWakeToken<LocalStorage>);
        token.request_local_wake();
    },
    |data| unsafe {
        let token = ManuallyDrop::new(Arc::from_raw(data as *const TaskWakeToken<LocalStorage>));
        (*token).request_local_wake();
    },
    |data| unsafe {
        drop(Arc::from_raw(data as *const TaskWakeToken<LocalStorage>));
    },
);

impl<S: Storage> Drop for GenericTaskHeader<S> {
    fn drop(&mut self) {
        self.wake_token.deactivate_and_wait();
        // 必须先于 `cancel_waiter` 字段析构：`Link` 在仍然入链时析构会 panic，而留在
        // scope 队列里的节点在 header 释放后就是悬垂指针。
        self.disarm_scope_cancel_waiter();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "loom"))]
    use crate::{scope::GenericScopeCompletion, utils::ownership::ArcOwnership};
    use std::sync::atomic::AtomicU32;
    use veloq_intrusive_linklist::Link;

    static TEST_VTABLE: TaskVTable<AtomicStorage> = TaskVTable {
        wake: |_| {},
        wake_by_ref: |_| {},
        poll: |_, _| Ok(true),
        drop: |_| {},
    };

    struct WakeCounter {
        count: AtomicU32,
    }

    static COUNTER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |p| unsafe {
            Arc::increment_strong_count(p as *const WakeCounter);
            RawWaker::new(p, &COUNTER_VTABLE)
        },
        |p| unsafe {
            let counter = Arc::from_raw(p as *const WakeCounter);
            counter.count.fetch_add(1, Ordering::AcqRel);
        },
        |p| unsafe {
            let counter = ManuallyDrop::new(Arc::from_raw(p as *const WakeCounter));
            counter.count.fetch_add(1, Ordering::AcqRel);
        },
        |p| unsafe {
            drop(Arc::from_raw(p as *const WakeCounter));
        },
    );

    fn counting_waker(counter: &Arc<WakeCounter>) -> Waker {
        let raw = Arc::into_raw(Arc::clone(counter)) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(raw, &COUNTER_VTABLE)) }
    }

    fn new_node(waker: &Waker) -> GenericWakerNode<AtomicStorage> {
        GenericWakerNode {
            waker: waker.clone(),
            link: Link::new(),
            marker: PhantomData,
            _pin: PhantomPinned,
        }
    }

    /// 同一个节点用不同 waker 重复注册时只能在链表中出现一次，否则链表成环，
    /// `mark_completed_and_notify` 的遍历会死循环。
    #[test]
    fn register_completion_is_idempotent_for_linked_node() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let first = Arc::new(WakeCounter {
            count: AtomicU32::new(0),
        });
        let second = Arc::new(WakeCounter {
            count: AtomicU32::new(0),
        });

        let mut node = new_node(&counting_waker(&first));
        let mut node = unsafe { Pin::new_unchecked(&mut node) };

        unsafe {
            header.register_completion(node.as_mut(), &counting_waker(&first));
            header.register_completion(node.as_mut(), &counting_waker(&second));
        }

        header.mark_completed_and_notify();

        assert_eq!(first.count.load(Ordering::Acquire), 0);
        assert_eq!(second.count.load(Ordering::Acquire), 1);
        assert!(!node.link.is_linked());
    }

    #[test]
    fn create_waker_reuses_cached_waker() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);

        let first = header.create_waker(&INTRUSIVE_WAKER_VTABLE).clone();
        let second = header.create_waker(&INTRUSIVE_WAKER_VTABLE).clone();

        assert!(first.will_wake(&second));
    }

    /// `remove_waker` 在任务已完成时也必须真正摘链，不能提前返回。
    #[test]
    fn remove_waker_unlinks_even_after_completion() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        let counter = Arc::new(WakeCounter {
            count: AtomicU32::new(0),
        });

        let mut node = new_node(&counting_waker(&counter));
        let mut node = unsafe { Pin::new_unchecked(&mut node) };
        unsafe { header.register_completion(node.as_mut(), &counting_waker(&counter)) };
        assert!(node.link.is_linked());

        // 模拟 `mark_completed_and_notify` 置位 COMPLETED 与清链之间的窗口。
        header.state.fetch_or(STATE_COMPLETED, Ordering::AcqRel);

        let node_ptr = unsafe { NonNull::from(node.as_mut().get_unchecked_mut()) };
        unsafe { header.remove_waker(node_ptr) };
        assert!(!node.link.is_linked());
    }

    /// 入队失败后被放弃的任务必须结算 scope 义务并变为已完成。
    #[test]
    fn abandon_before_enqueue_settles_obligation() {
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        header.claim_scope_obligation();

        header.abandon_before_enqueue();

        assert!(header.is_completed());
        assert!(header.is_locally_cancelled());
        assert!(header.is_scope_acknowledged());

        // 幂等：重复调用不会再次结算。
        header.abandon_before_enqueue();
        assert!(!header.try_acknowledge_completion());
    }

    #[cfg(not(feature = "loom"))]
    fn header_with_scope_cancelled() -> GenericTaskHeader<AtomicStorage> {
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let scope_ptr = unsafe { RawScope::clone_raw(completion.as_ref()) };
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        unsafe {
            *header.scope.get() = ScopeRef::new(scope_ptr);
        }
        completion.cancel();
        header
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn rejected_scope_waiter_registration_disarms_before_drop() {
        let header = header_with_scope_cancelled();

        assert!(!header.arm_scope_cancel_waiter(Waker::noop()));
        assert_eq!(header.state.load(Ordering::Acquire) & STATE_CANCEL_ARMED, 0);
        assert!(!header.cancel_waiter.link.is_linked());
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn scope_waiter_disarm_is_idempotent_after_registration() {
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let scope_ptr = unsafe { RawScope::clone_raw(completion.as_ref()) };
        let header = GenericTaskHeader::<AtomicStorage>::new_placeholder(&TEST_VTABLE);
        unsafe {
            *header.scope.get() = ScopeRef::new(scope_ptr);
        }

        assert!(header.arm_scope_cancel_waiter(Waker::noop()));
        assert!(header.cancel_waiter.link.is_linked());

        header.disarm_scope_cancel_waiter();
        header.disarm_scope_cancel_waiter();

        assert_eq!(header.state.load(Ordering::Acquire) & STATE_CANCEL_ARMED, 0);
        assert!(!header.cancel_waiter.link.is_linked());
    }
}
