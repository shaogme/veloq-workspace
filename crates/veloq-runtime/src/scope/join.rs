use super::{
    AsyncScope, CancelTokenSlot, LocalAsyncScope, ScopeProvider,
    router::{RoutedSpawnState, RoutedTakeReadyOutcome, RoutedTakeResult, RoutedTaskAccess},
};
use crate::{
    error::{EnqueueError, Result as RuntimeResult, RuntimeError},
    runtime::cancellation::{CancelledFuture, GenericCancellationToken},
    task::{
        Arena, GenericTaskHeader, GenericWakerNode, LocalTaskRef, SendTaskRef, TaskError,
        TaskHandleRef, TaskJoinGate, TaskLease,
    },
};
use diagweave::{Report, prelude::*};
use veloq_intrusive_linklist::Link;
use veloq_std::{
    boxed::Box,
    cell::Cell,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    ptr::NonNull,
    sync::NativeArc as Arc,
    task::{Context, Poll},
};
use veloq_storage::{AtomicStorage, StateLock, Storage};

/// Outcome of awaiting a [`JoinHandle`].
#[derive(Debug)]
pub enum JoinOutcome<T> {
    /// The task completed successfully.
    Ok(T),
    /// The task could not be published to its owner worker's local queue.
    Rejected(EnqueueError),
    /// The task failed due to cancellation or panic during execution.
    TaskErr(TaskError),
    /// The runtime encountered a protocol or infrastructure error while joining.
    RuntimeErr(Report<RuntimeError>),
}

impl<T> JoinOutcome<T> {
    pub fn unwrap(self) -> T {
        match self {
            Self::Ok(value) => value,
            Self::Rejected(err) => panic!("task rejected: {err}"),
            Self::TaskErr(err) => panic!("task error: {err:?}"),
            Self::RuntimeErr(err) => panic!("runtime error: {err}"),
        }
    }
    pub fn expect(self, msg: &str) -> T {
        match self {
            Self::Ok(value) => value,
            Self::Rejected(err) => panic!("{msg}: task rejected: {err}"),
            Self::TaskErr(err) => panic!("{msg}: task error: {err:?}"),
            Self::RuntimeErr(err) => panic!("{msg}: runtime error: {err}"),
        }
    }
}

pub(crate) struct ResolvedRoutedTask<'scope_ref, T, R: TaskHandleRef> {
    pub(crate) task: R,
    pub(crate) access: Option<Box<dyn RoutedTaskAccess<T> + 'scope_ref>>,
}

pub(crate) enum JoinSource<'scope_ref, T, R: TaskHandleRef> {
    Direct {
        task: R,
        gate: &'scope_ref dyn TaskJoinGate<T>,
    },
    Routed {
        state: Arc<RoutedSpawnState<'scope_ref, T>>,
        resolved: Option<ResolvedRoutedTask<'scope_ref, T, R>>,
    },
}

/// Join handle for a spawned child task.
///
/// As a `Future`, `await` waits until the task has **finished executing**, not merely
/// until cancellation has been requested. If the task ends due to cancellation, the
/// result is [`JoinOutcome::TaskErr`] with [`TaskError::Cancelled`]. For immediate
/// notification when cancellation is requested, use [`JoinHandle::cancelled`]. A local task
/// whose owner queue is full completes as [`JoinOutcome::Rejected`] with the worker and queue
/// capacity; this is distinct from cancellation and does not imply that the task was polled.
///
/// A join handle has a single owner. Its `poll`, `cancel`, `is_finished`,
/// `is_cancel_requested`, and `Drop` operations must not run concurrently. In
/// particular, this type is not a cross-thread cancellation handle: move the
/// handle to the thread that owns it, or use a separate cancellation token.
pub struct JoinHandle<
    'scope_ref,
    T,
    R: TaskHandleRef,
    S: ScopeProvider<TExtra> + 'scope_ref,
    TExtra,
> where
    S::Arena: 'scope_ref,
{
    pub(crate) source: JoinSource<'scope_ref, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<GenericWakerNode<R::Storage>>,
    pub(crate) allocation: Option<<S::Arena as Arena>::Allocation<'scope_ref>>,
    pub(crate) marker: PhantomData<TExtra>,
    pub(crate) _not_sync: PhantomData<Cell<()>>,
    pub(crate) _pin: PhantomPinned,
}

// Safety: a send join handle is moved as one value between threads. Once moved, the
// handle's `&mut self` poll/cancel contract gives exclusive access to its task reference,
// waker node, reclaim callback, and cancellation slot; none of those fields are shared by
// this implementation.
unsafe impl<'rt, 'scope, 'env, 'scope_ref, T, TExtra> Send
    for JoinHandle<'scope_ref, T, SendTaskRef, AsyncScope<'rt, 'scope, 'env, TExtra>, TExtra>
where
    T: Send,
{
}

pub type LocalJoinHandle<'rt, 'scope_ref, 'env, T, TExtra> =
    JoinHandle<'scope_ref, T, LocalTaskRef, AsyncScope<'rt, 'scope_ref, 'env, TExtra>, TExtra>;
pub type SendJoinHandle<'rt, 'scope_ref, 'env, T, TExtra> =
    JoinHandle<'scope_ref, T, SendTaskRef, AsyncScope<'rt, 'scope_ref, 'env, TExtra>, TExtra>;
pub type LocalAsyncJoinHandle<'rt, 'scope_ref, 'env, T, TExtra> =
    JoinHandle<'scope_ref, T, LocalTaskRef, LocalAsyncScope<'rt, 'scope_ref, 'env, TExtra>, TExtra>;

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra> + 'scope_ref, TExtra>
    JoinHandle<'scope_ref, T, R, S, TExtra>
{
    /// Requests cancellation of the task.
    ///
    /// This only signals cancellation; the task may continue running until it is
    /// polled and observes the cancel state. Use `await` to wait until the task has
    /// actually stopped, or [`JoinHandle::cancelled`] to be notified as soon as
    /// cancellation has been requested.
    pub fn cancel(&mut self) {
        let mut cancel_slot = self.cancel_token.lock();
        if let Some(token) = cancel_slot.take() {
            token.cancel();
        }

        // 取消必须伴随一次唤醒，否则一个正挂起的任务要等到下一次自然唤醒才会观察到
        // 取消状态，而 `await` / `wait_all` 都在等它结束。
        match &self.source {
            JoinSource::Direct { task, .. } => {
                task.header().cancel_and_wake();
            }
            JoinSource::Routed { state, resolved } => {
                state.request_cancel();
                if let Some(resolved) = resolved {
                    resolved.task.header().cancel_and_wake();
                }
            }
        }
    }

    /// Completes when cancellation has been requested, without waiting for the
    /// underlying task to finish executing.
    ///
    /// Use `await` if you need to wait until the task has actually stopped.
    pub fn cancelled(&self) -> CancelledFuture<S::Storage, S::Ownership> {
        self.cancel_token().cancelled()
    }

    /// Returns whether cancellation has been requested (the task may still be running).
    pub fn is_cancel_requested(&self) -> bool {
        match &self.source {
            JoinSource::Direct { task, .. } => task.header().is_cancelled(),
            JoinSource::Routed { state, resolved } => {
                state.is_cancel_requested()
                    || self.scope.completion().is_cancelled()
                    || resolved
                        .as_ref()
                        .is_some_and(|r| r.task.header().is_cancelled())
            }
        }
    }

    /// Returns whether the task has fully completed (equivalent to `await` returning `Ready`).
    pub fn is_finished(&self) -> bool {
        match &self.source {
            JoinSource::Direct { task, .. } => task.header().is_reclaimable(),
            JoinSource::Routed { state, resolved } => {
                if let Some(res) = resolved {
                    res.task.header().is_reclaimable()
                } else {
                    state.has_failed_outcome()
                }
            }
        }
    }

    pub fn cancel_token(&self) -> GenericCancellationToken<S::Storage, S::Ownership> {
        {
            let cancel_slot = self.cancel_token.lock();
            if let Some(token) = cancel_slot.as_ref() {
                return token.clone();
            }
        }

        let token = self.scope.completion().cancel_token().child();
        let is_cancelled = match &self.source {
            JoinSource::Direct { task, .. } => task.header().is_cancelled(),
            JoinSource::Routed { state, resolved } => {
                if state.is_cancel_requested() {
                    true
                } else if let Some(resolved) = resolved {
                    resolved.task.header().is_cancelled()
                } else {
                    false
                }
            }
        };

        if is_cancelled {
            token.cancel();
        }

        let mut cancel_slot = self.cancel_token.lock();
        if let Some(existing) = cancel_slot.as_ref() {
            existing.clone()
        } else {
            cancel_slot.replace(token.clone());
            token
        }
    }

    pub(crate) fn new_direct(
        scope: &'scope_ref S,
        task: R,
        gate: &'scope_ref dyn TaskJoinGate<T>,
        allocation: Option<<S::Arena as Arena>::Allocation<'scope_ref>>,
    ) -> Self {
        Self {
            source: JoinSource::Direct { task, gate },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            allocation,
            marker: PhantomData,
            _not_sync: PhantomData,
            _pin: PhantomPinned,
        }
    }

    pub(crate) fn new_routed(
        scope: &'scope_ref S,
        state: Arc<RoutedSpawnState<'scope_ref, T>>,
    ) -> Self {
        Self {
            source: JoinSource::Routed {
                state,
                resolved: None,
            },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            allocation: None,
            marker: PhantomData,
            _not_sync: PhantomData,
            _pin: PhantomPinned,
        }
    }

    fn register_waker_on<St: Storage>(
        waker_node: &mut Option<GenericWakerNode<St>>,
        header: &GenericTaskHeader<St>,
        cx: &mut Context<'_>,
    ) -> RuntimeResult<()> {
        if waker_node.is_none() {
            *waker_node = Some(GenericWakerNode {
                waker: cx.waker().clone(),
                link: Link::new(),
                marker: PhantomData,
                _pin: PhantomPinned,
            });
        }

        let Some(node) = waker_node.as_mut() else {
            return Err(RuntimeError::InvariantViolation {
                site: "JoinHandle::register_waker_on",
                detail: "waker node missing after initialization".into(),
            }
            .to_report());
        };

        // 「刷新 waker + 入链」统一由 `register_completion` 完成：它自带 `is_linked()`
        // 保护，重复注册不会破坏侵入式链表。
        let node = unsafe { Pin::new_unchecked(node) };
        unsafe { header.register_completion(node, cx.waker()) };
        Ok(())
    }

    fn remove_waker_on<St: Storage>(
        waker_node: &mut Option<GenericWakerNode<St>>,
        header: &GenericTaskHeader<St>,
    ) {
        if let Some(node) = waker_node.as_mut() {
            let node_ptr = NonNull::from(&mut *node);
            unsafe { header.remove_waker(node_ptr) };
        }
        *waker_node = None;
    }
}

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra> + 'scope_ref, TExtra: 'scope_ref>
    Future for JoinHandle<'scope_ref, T, R, S, TExtra>
{
    type Output = JoinOutcome<T>;

    /// 只做「查完成状态 → 未完成则注册 waker → `Pending`」，**不驱动调度器**。
    ///
    /// 避免在 poll 中同步跑调度循环直到整个作用域结束；否则 `handle.await` 会等完本作用域的
    /// **所有**子任务，导致 `select!` 的公平轮询完全失效，且在非 worker 线程上 poll 一个
    /// handle 会因取不到 TLS 直接 panic。
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match &mut this.source {
            JoinSource::Direct { task, gate, .. } => {
                let header = task.header();
                if header.is_reclaimable() {
                    Self::remove_waker_on(&mut this.waker_node, header);
                    let lease = TaskLease::new(this.allocation.take());
                    let outcome = if let Some(reason) = header.take_enqueue_rejection() {
                        JoinOutcome::Rejected(reason)
                    } else if let Some(res) = gate.take_result_erased() {
                        match res {
                            Ok(value) => JoinOutcome::Ok(value),
                            Err(err) => JoinOutcome::TaskErr(err),
                        }
                    } else if header.is_locally_cancelled() {
                        JoinOutcome::TaskErr(TaskError::Cancelled)
                    } else {
                        JoinOutcome::RuntimeErr(
                            RuntimeError::TaskResultUnavailable {
                                stage: "JoinHandle::poll(Direct)",
                            }
                            .to_report(),
                        )
                    };
                    drop(lease);
                    return Poll::Ready(outcome);
                }

                if let Err(err) =
                    Self::register_waker_on::<R::Storage>(&mut this.waker_node, header, cx)
                {
                    return Poll::Ready(JoinOutcome::RuntimeErr(err));
                }
                Poll::Pending
            }
            JoinSource::Routed { state, resolved } => loop {
                if let Some(res) = resolved {
                    let header = res.task.header();
                    if header.is_reclaimable() {
                        Self::remove_waker_on(&mut this.waker_node, header);
                        let Some(access) = res.access.take() else {
                            return Poll::Ready(JoinOutcome::RuntimeErr(
                                RuntimeError::InvariantViolation {
                                    site: "JoinHandle::poll(Routed)",
                                    detail: "routed task access already taken".into(),
                                }
                                .to_report(),
                            ));
                        };
                        let outcome = access.take_result();
                        access.reclaim();
                        return Poll::Ready(match outcome {
                            RoutedTakeResult::Ok(value) => JoinOutcome::Ok(value),
                            RoutedTakeResult::TaskErr(err) => JoinOutcome::TaskErr(err),
                            RoutedTakeResult::RuntimeErr(err) => JoinOutcome::RuntimeErr(err),
                        });
                    }

                    if let Err(err) =
                        Self::register_waker_on::<R::Storage>(&mut this.waker_node, header, cx)
                    {
                        return Poll::Ready(JoinOutcome::RuntimeErr(err));
                    }
                    return Poll::Pending;
                } else {
                    match state.try_take_ready() {
                        RoutedTakeReadyOutcome::Ready(ready) => {
                            let converted_task = unsafe {
                                R::from_header(ready.task.header()
                                    as *const GenericTaskHeader<AtomicStorage>
                                    as *const GenericTaskHeader<R::Storage>)
                            };
                            *resolved = Some(ResolvedRoutedTask {
                                task: converted_task,
                                access: Some(ready.access),
                            });
                        }
                        RoutedTakeReadyOutcome::Pending => {
                            state.register(cx.waker());
                            match state.try_take_ready() {
                                RoutedTakeReadyOutcome::Ready(ready) => {
                                    let converted_task = unsafe {
                                        R::from_header(ready.task.header()
                                            as *const GenericTaskHeader<AtomicStorage>
                                            as *const GenericTaskHeader<R::Storage>)
                                    };
                                    *resolved = Some(ResolvedRoutedTask {
                                        task: converted_task,
                                        access: Some(ready.access),
                                    });
                                    continue;
                                }
                                RoutedTakeReadyOutcome::Pending => return Poll::Pending,
                                RoutedTakeReadyOutcome::TaskErr(err) => {
                                    return Poll::Ready(JoinOutcome::TaskErr(err));
                                }
                                RoutedTakeReadyOutcome::RuntimeErr(err) => {
                                    return Poll::Ready(JoinOutcome::RuntimeErr(err));
                                }
                            }
                        }
                        RoutedTakeReadyOutcome::TaskErr(err) => {
                            return Poll::Ready(JoinOutcome::TaskErr(err));
                        }
                        RoutedTakeReadyOutcome::RuntimeErr(err) => {
                            return Poll::Ready(JoinOutcome::RuntimeErr(err));
                        }
                    }
                }
            },
        }
    }
}

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra> Drop
    for JoinHandle<'scope_ref, T, R, S, TExtra>
{
    fn drop(&mut self) {
        if let Some(node) = self.waker_node.as_mut() {
            let node_ptr = NonNull::from(&mut *node);
            let task = match &self.source {
                JoinSource::Direct { task, .. } => Some(*task),
                JoinSource::Routed { resolved, .. } => resolved.as_ref().map(|r| r.task),
            };

            if let Some(task) = task {
                // 无条件摘链：任务已完成也可能正处在「结果已发布、链表尚未清空」
                // 的窗口里，此时提前返回会留下悬垂节点。
                unsafe {
                    task.header().remove_waker(node_ptr);
                }
            }
        }
    }
}
