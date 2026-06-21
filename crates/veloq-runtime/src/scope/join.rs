use super::{
    AsyncScope, CancelTokenSlot, LocalAsyncScope, ScopeProvider,
    router::{RoutedSpawnState, RoutedTakeReadyOutcome, RoutedTakeResult, RoutedTaskAccess},
};
use crate::{
    error::{Result as RuntimeResult, RuntimeError},
    runtime::{GenericCancellationToken, primitives::CancelledFuture},
    task::{
        Arena, GenericTaskHeader, GenericWakerNode, LocalTaskRef, SendTaskRef, TaskError,
        TaskHandleRef, TaskJoinGate,
    },
};
use diagweave::{Report, prelude::*};
use std::{
    alloc::Layout,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    ptr::{NonNull, drop_in_place, write},
    sync::Arc,
    task::{Context, Poll},
};
use veloq_intrusive_linklist::Link;
use veloq_storage::{AtomicStorage, StateLock, Storage};

pub(crate) type ReclaimFn<'scope_ref, T, A> = unsafe fn(&A, &'scope_ref dyn TaskJoinGate<T>);

/// Outcome of awaiting a [`JoinHandle`].
#[derive(Debug)]
pub enum JoinOutcome<T> {
    /// The task completed successfully.
    Ok(T),
    /// The task failed due to cancellation or panic during execution.
    TaskErr(TaskError),
    /// The runtime encountered a protocol or infrastructure error while joining.
    RuntimeErr(Report<RuntimeError>),
}

impl<T> JoinOutcome<T> {
    pub fn unwrap(self) -> T {
        match self {
            Self::Ok(value) => value,
            Self::TaskErr(err) => panic!("task error: {err:?}"),
            Self::RuntimeErr(err) => panic!("runtime error: {err}"),
        }
    }
    pub fn expect(self, msg: &str) -> T {
        match self {
            Self::Ok(value) => value,
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
/// notification when cancellation is requested, use [`JoinHandle::cancelled`].
pub struct JoinHandle<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra> {
    pub(crate) source: JoinSource<'scope_ref, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<Pin<&'scope_ref mut GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope_ref, T, S::Arena>>,
    pub(crate) marker: PhantomData<TExtra>,
}

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

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra>
    JoinHandle<'scope_ref, T, R, S, TExtra>
{
    /// Requests cancellation of the task.
    ///
    /// This only signals cancellation; the task may continue running until it is
    /// polled and observes the cancel state. Use `await` to wait until the task has
    /// actually stopped, or [`JoinHandle::cancelled`] to be notified as soon as
    /// cancellation has been requested.
    pub fn cancel(&self) {
        let mut cancel_slot = self.cancel_token.lock();
        if let Some(token) = cancel_slot.take() {
            token.cancel();
        }

        match &self.source {
            JoinSource::Direct { task, .. } => {
                task.header().cancel();
            }
            JoinSource::Routed { state, resolved } => {
                state.request_cancel();
                if let Some(resolved) = resolved {
                    resolved.task.header().cancel();
                } else {
                    state.cancel_ready_task_if_any();
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
            JoinSource::Direct { task, .. } => task.header().is_completed(),
            JoinSource::Routed { state, resolved } => {
                if let Some(res) = resolved {
                    res.task.header().is_completed()
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
        reclaim: Option<ReclaimFn<'scope_ref, T, S::Arena>>,
    ) -> Self {
        Self {
            source: JoinSource::Direct { task, gate },
            scope,
            cancel_token: super::new_cancel_slot::<S::Storage, S::Ownership>(),
            waker_node: None,
            reclaim,
            marker: PhantomData,
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
            reclaim: None,
            marker: PhantomData,
        }
    }

    fn register_waker_on<St: Storage>(
        waker_node: &mut Option<Pin<&'scope_ref mut GenericWakerNode<St>>>,
        arena: &dyn Arena,
        header: &GenericTaskHeader<St>,
        cx: &mut Context<'_>,
    ) -> RuntimeResult<()> {
        if let Some(node) = waker_node {
            let mut node = node.as_mut();
            if !node.waker.will_wake(cx.waker()) {
                unsafe {
                    node.as_mut().get_unchecked_mut().waker = cx.waker().clone();
                    header.register_completion(node.as_mut());
                }
            }
            return Ok(());
        } else {
            let node_ptr = unsafe {
                arena.alloc_raw(
                    Layout::new::<GenericWakerNode<St>>(),
                    Some(|ptr| drop_in_place(ptr as *mut GenericWakerNode<St>)),
                )
            };
            let Some(node_ptr) = node_ptr else {
                return Err(RuntimeError::ArenaAllocationNull {
                    op: "JoinHandle::register_waker_on",
                }
                .to_report());
            };
            unsafe {
                write(
                    node_ptr.as_ptr() as *mut GenericWakerNode<St>,
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        link: Link::new(),
                        marker: PhantomData,
                    },
                );
            }
            let node_ref = unsafe {
                Pin::new_unchecked(&mut *(node_ptr.as_ptr() as *mut GenericWakerNode<St>))
            };
            *waker_node = Some(node_ref);
            unsafe {
                if let Some(node) = waker_node.as_mut() {
                    header.register_completion(node.as_mut());
                } else {
                    return Err(RuntimeError::InvariantViolation {
                        site: "JoinHandle::register_waker_on",
                        detail: "waker node missing after initialization",
                    }
                    .to_report());
                }
            }
        }
        Ok(())
    }
}

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra> + 'scope_ref, TExtra: 'scope_ref>
    Future for JoinHandle<'scope_ref, T, R, S, TExtra>
{
    type Output = JoinOutcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Err(err) = this
            .scope
            .runtime()
            .drive_worker::<S::Storage, S::Ownership>(Some(this.scope.completion()))
        {
            return Poll::Ready(JoinOutcome::RuntimeErr(err));
        }

        let arena = this.scope.arena();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate, .. } => {
                let header = task.header();
                if header.is_completed() {
                    let Some(res) = gate.take_result_erased() else {
                        return Poll::Ready(JoinOutcome::RuntimeErr(
                            RuntimeError::TaskResultUnavailable {
                                stage: "JoinHandle::poll(Direct)",
                            }
                            .to_report(),
                        ));
                    };
                    if let Some(reclaim) = reclaim {
                        unsafe { (reclaim)(arena, *gate) };
                    }
                    return Poll::Ready(match res {
                        Ok(value) => JoinOutcome::Ok(value),
                        Err(err) => JoinOutcome::TaskErr(err),
                    });
                }

                if let Err(err) =
                    Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx)
                {
                    return Poll::Ready(JoinOutcome::RuntimeErr(err));
                }
                Poll::Pending
            }
            JoinSource::Routed { state, resolved } => loop {
                if let Some(res) = resolved {
                    let header = res.task.header();
                    if header.is_completed() {
                        let Some(access) = res.access.take() else {
                            return Poll::Ready(JoinOutcome::RuntimeErr(
                                RuntimeError::InvariantViolation {
                                    site: "JoinHandle::poll(Routed)",
                                    detail: "routed task access already taken",
                                }
                                .to_report(),
                            ));
                        };
                        let outcome = access.take_result();
                        access.reclaim(arena);
                        return Poll::Ready(match outcome {
                            RoutedTakeResult::Ok(value) => JoinOutcome::Ok(value),
                            RoutedTakeResult::TaskErr(err) => JoinOutcome::TaskErr(err),
                            RoutedTakeResult::RuntimeErr(err) => JoinOutcome::RuntimeErr(err),
                        });
                    }

                    if let Err(err) =
                        Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx)
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
            let node_ptr = unsafe { NonNull::from(node.as_mut().get_unchecked_mut()) };
            let task = match &self.source {
                JoinSource::Direct { task, .. } => Some(*task),
                JoinSource::Routed { resolved, .. } => resolved.as_ref().map(|r| r.task),
            };

            if let Some(task) = task {
                let header = task.header();
                if !header.is_completed() {
                    unsafe {
                        header.remove_waker(node_ptr);
                    }
                }
            }
        }
    }
}
