use super::{
    AsyncScope, CancelTokenSlot, LocalAsyncScope, ScopeProvider,
    router::{RoutedSpawnState, RoutedTaskAccess},
};
use crate::{
    runtime::GenericCancellationToken,
    task::{
        Arena, GenericTaskHeader, GenericWakerNode, LocalTaskRef, SendTaskRef, TaskError,
        TaskHandleRef, TaskJoinGate,
    },
};
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

pub struct JoinHandle<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra> {
    pub(crate) source: JoinSource<'scope_ref, T, R>,
    pub(crate) scope: &'scope_ref S,
    pub(crate) cancel_token: CancelTokenSlot<S::Storage, S::Ownership>,
    pub(crate) waker_node: Option<Pin<&'scope_ref mut GenericWakerNode<R::Storage>>>,
    pub(crate) reclaim: Option<ReclaimFn<'scope_ref, T, S::Arena>>,
    pub(crate) marker: std::marker::PhantomData<TExtra>,
}

unsafe impl<'rt, 'scope, 'scope_ref, T, TExtra> Send
    for JoinHandle<'scope_ref, T, SendTaskRef, AsyncScope<'rt, 'scope, TExtra>, TExtra>
where
    T: Send,
{
}

pub type LocalJoinHandle<'rt, 'scope_ref, T, TExtra> =
    JoinHandle<'scope_ref, T, LocalTaskRef, AsyncScope<'rt, 'scope_ref, TExtra>, TExtra>;
pub type SendJoinHandle<'rt, 'scope_ref, T, TExtra> =
    JoinHandle<'scope_ref, T, SendTaskRef, AsyncScope<'rt, 'scope_ref, TExtra>, TExtra>;
pub type LocalAsyncJoinHandle<'rt, 'scope_ref, T, TExtra> =
    JoinHandle<'scope_ref, T, LocalTaskRef, LocalAsyncScope<'rt, 'scope_ref, TExtra>, TExtra>;

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra>, TExtra>
    JoinHandle<'scope_ref, T, R, S, TExtra>
{
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
    ) {
        if let Some(node) = waker_node {
            let mut node = node.as_mut();
            if !node.waker.will_wake(cx.waker()) {
                unsafe {
                    node.as_mut().get_unchecked_mut().waker = cx.waker().clone();
                    header.register_completion(node.as_mut());
                }
            }
        } else {
            let node_ptr = unsafe {
                arena.alloc_raw(
                    Layout::new::<GenericWakerNode<St>>(),
                    Some(|ptr| drop_in_place(ptr as *mut GenericWakerNode<St>)),
                ) as *mut GenericWakerNode<St>
            };
            let node_ptr = NonNull::new(node_ptr).expect("arena allocation failed");
            unsafe {
                write(
                    node_ptr.as_ptr(),
                    GenericWakerNode {
                        waker: cx.waker().clone(),
                        link: Link::new(),
                        marker: PhantomData,
                    },
                );
            }
            let node_ref = unsafe { Pin::new_unchecked(&mut *node_ptr.as_ptr()) };
            *waker_node = Some(node_ref);
            unsafe {
                header.register_completion(waker_node.as_mut().unwrap().as_mut());
            }
        }
    }
}

impl<'scope_ref, T, R: TaskHandleRef, S: ScopeProvider<TExtra> + 'scope_ref, TExtra: 'scope_ref>
    Future for JoinHandle<'scope_ref, T, R, S, TExtra>
{
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.scope
            .runtime()
            .drive_worker::<S::Storage, S::Ownership>(Some(this.scope.completion()));

        let arena = this.scope.arena();
        let completion = this.scope.completion();
        let waker_node = &mut this.waker_node;
        let reclaim = this.reclaim;

        match &mut this.source {
            JoinSource::Direct { task, gate, .. } => {
                let header = task.header();
                if header.is_completed() {
                    let res = gate
                        .take_result_erased()
                        .expect("task result already taken");
                    if let Some(reclaim) = reclaim {
                        unsafe { (reclaim)(arena, *gate) };
                    }
                    return Poll::Ready(res);
                }

                if completion.is_cancelled() || header.is_cancelled() {
                    return Poll::Ready(Err(TaskError::Cancelled));
                }

                Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx);
                Poll::Pending
            }
            JoinSource::Routed { state, resolved } => {
                loop {
                    if let Some(res) = resolved {
                        let header = res.task.header();
                        if header.is_completed() {
                            let access =
                                res.access.take().expect("routed task access already taken");
                            let output = access.take_result();
                            access.reclaim(arena);
                            return Poll::Ready(output);
                        }

                        if completion.is_cancelled() || header.is_cancelled() {
                            return Poll::Ready(Err(TaskError::Cancelled));
                        }

                        Self::register_waker_on::<R::Storage>(waker_node, arena, header, cx);
                        return Poll::Pending;
                    } else {
                        match state.try_take_ready() {
                            Ok(Some(ready)) => {
                                let converted_task = unsafe {
                                    R::from_header(ready.task.header()
                                        as *const GenericTaskHeader<AtomicStorage>
                                        as *const GenericTaskHeader<R::Storage>)
                                };
                                *resolved = Some(ResolvedRoutedTask {
                                    task: converted_task,
                                    access: Some(ready.access),
                                });
                                // Continue to poll the newly resolved task
                            }
                            Ok(None) => {
                                state.register(cx.waker());
                                // Double check to avoid race condition
                                if let Some(ready) = state.try_take_ready()? {
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
                                return Poll::Pending;
                            }
                            Err(err) => return Poll::Ready(Err(err)),
                        }
                    }
                }
            }
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
