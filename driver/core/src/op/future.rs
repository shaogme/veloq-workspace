use std::error::Error;
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::trace;

use crate::driver::{
    CompletionRecord, Driver, DriverSubmitResult, PlatformOp, PollRecordResult, RemoteWaker,
    SharedCompletionTable, SubmitStatus, event_res_to_result,
};
use crate::op::{IntoPlatformOp, Op};
use crate::slot::DetachedCancelTable;
use crate::{DriverCoreError, DriverError, DriverReport, DriverResult};

use diagweave::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostReason {
    /// 槽位已被回收，用于新一代操作 (Generation Mismatch)。
    /// 调用方应当认为关联的 IO 后端（如 Socket 或 Buffer）已处于不确定状态。
    GenerationMismatch,
    /// 内部错误：操作负载丢失 (Completion sidecar missing)。
    PayloadMissing,
    /// 其它未知原因造成的资源丢失。
    Other,
}

impl std::fmt::Display for LostReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GenerationMismatch => write!(f, "generation mismatch (slot recycled)"),
            Self::PayloadMissing => write!(f, "payload missing"),
            Self::Other => write!(f, "unknown resource loss"),
        }
    }
}

/// 描述 IO 操作丢失及其原因的结构化错误。
#[derive(Debug)]
pub struct OpError<E> {
    pub reason: LostReason,
    pub source: DriverReport<E>,
}

impl<E> OpError<E> {
    pub fn new(reason: LostReason, source: DriverReport<E>) -> Self {
        Self { reason, source }
    }

    /// 如果原因为 GenerationMismatch，则认为该错误是致命的（资源状态不确定）。
    pub fn is_lethal(&self) -> bool {
        matches!(self.reason, LostReason::GenerationMismatch)
    }
}

impl<E> std::fmt::Display for OpError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason, self.source)
    }
}

/// The result of an IO operation.
#[derive(Debug)]
pub enum OpResult<T, E, R = usize> {
    /// Operation completed (successfully or with IO error).
    Completed(DriverResult<R, E>, T),
    /// Operation failed because the resource ownership was lost.
    ResourceLost(OpError<E>),
}

impl<T, E, R> OpResult<T, E, R>
where
    E: Error + Send + Sync + 'static,
{
    /// Unwraps the result, assuming the operation completed (panics if Lost).
    pub fn unwrap(self) -> (R, T) {
        match self {
            OpResult::Completed(Ok(res), data) => (res, data),
            OpResult::Completed(Err(e), _) => panic!("OpResult::Completed(Err({}))", e),
            OpResult::ResourceLost(e) => panic!("OpResult::ResourceLost({})", e),
        }
    }

    /// Returns the result and the resource implementation (if available).
    pub fn into_inner(self) -> (DriverResult<R, E>, Option<T>) {
        match self {
            OpResult::Completed(res, data) => (res, Some(data)),
            OpResult::ResourceLost(err) => (Err(err.source), None),
        }
    }
}

/// The completion projection for a submitted operation.
#[derive(Debug)]
pub struct OpCompletion<T, E, R = usize> {
    pub result: DriverResult<R, E>,
    pub output: T,
}

impl<T, E, R> OpCompletion<T, E, R> {
    #[inline]
    pub fn new(result: DriverResult<R, E>, output: T) -> Self {
        Self { result, output }
    }

    #[inline]
    pub fn into_parts(self) -> (DriverResult<R, E>, T) {
        (self.result, self.output)
    }
}

#[inline]
pub(crate) fn payload_missing_error<E>() -> OpError<E>
where
    E: DriverError,
{
    OpError::new(
        LostReason::PayloadMissing,
        E::from_core_report(
            DriverCoreError::Internal
                .to_report()
                .push_ctx("scope", "driver-core/op")
                .attach_note("operation payload lost: completion sidecar missing"),
        ),
    )
}

#[inline]
pub(crate) fn generation_mismatch_error<E>() -> OpError<E>
where
    E: DriverError,
{
    OpError::new(
        LostReason::GenerationMismatch,
        E::from_core_report(
            DriverCoreError::Internal
                .to_report()
                .push_ctx("scope", "driver-core/op")
                .attach_note("operation lost: slot recycled (generation mismatch)"),
        ),
    )
}

#[inline]
pub(crate) fn completion_record_to_result<T, O, UP, E, C>(
    record: CompletionRecord<UP, E, C>,
) -> Poll<OpResult<T::Output, E, T::Completion>>
where
    UP: Send,
    O: PlatformOp,
    T: IntoPlatformOp<O, DriverCompletion = C, ErasedPayload = UP, Error = E>,
    E: DriverError,
    C: crate::driver::CompletionValue,
{
    let CompletionRecord {
        event,
        payload: payload_erased,
        detail,
    } = record;
    let Some(payload_erased) = payload_erased else {
        return Poll::Ready(OpResult::ResourceLost(payload_missing_error()));
    };
    let payload = T::payload_from_erased(payload_erased);
    let res = detail.unwrap_or_else(|| event_res_to_result::<C, E>(event.res));
    let completion = T::complete(payload, res);
    Poll::Ready(OpResult::Completed(completion.result, completion.output))
}

#[inline]
pub(crate) fn poll_completion_table_once<T, O, UP, E, C>(
    table: &dyn crate::driver::CompletionAccess<UP, E, C>,
    token: u64,
) -> Poll<OpResult<T::Output, E, T::Completion>>
where
    UP: Send,
    O: PlatformOp,
    T: IntoPlatformOp<O, DriverCompletion = C, ErasedPayload = UP, Error = E>,
    E: DriverError,
    C: crate::driver::CompletionValue,
{
    match table.try_take_record(token) {
        PollRecordResult::Ready(record) => completion_record_to_result::<T, O, UP, E, C>(record),
        PollRecordResult::Stale => Poll::Ready(
            OpResult::<T::Output, E, T::Completion>::ResourceLost(generation_mismatch_error()),
        ),
        PollRecordResult::Pending => Poll::Pending,
    }
}

type DetachedOpMarker<T, UP, E, C, O> = (T, UP, E, C, fn() -> O);

/// A Future representing a detached operation.
pub struct DetachedOp<T, O, E, C>
where
    O: PlatformOp,
    E: DriverError,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C, Error = E>,
{
    pub(crate) completion_table: Option<SharedCompletionTable<T::ErasedPayload, E, C>>,
    pub(crate) cancel_signal: Option<Arc<DetachedCancelTable>>,
    pub(crate) cancel_waker: Option<Arc<dyn RemoteWaker<E>>>,
    pub(crate) token: u64,
    pub(crate) immediate_failure: Option<(DriverReport<E>, T::UserPayload)>,
    pub(crate) _phantom: std::marker::PhantomData<DetachedOpMarker<T, T::ErasedPayload, E, C, O>>,
}

unsafe impl<
    T: IntoPlatformOp<O, DriverCompletion = C, Error = E> + std::marker::Send,
    O: PlatformOp,
    E: DriverError,
    C: crate::driver::CompletionValue,
> std::marker::Send for DetachedOp<T, O, E, C>
{
}

impl<T, O, E, C> Drop for DetachedOp<T, O, E, C>
where
    O: PlatformOp,
    E: DriverError,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C, Error = E>,
{
    fn drop(&mut self) {
        if let Some(table) = self.completion_table.as_ref() {
            table.mark_orphaned(self.token);
        }
        if let Some(cancel_signal) = self.cancel_signal.as_ref() {
            cancel_signal.request_cancel(self.token);
        }
        if let Some(cancel_waker) = self.cancel_waker.as_ref()
            && let Err(e) = cancel_waker.wake()
        {
            trace!("DetachedOp cancel wake failed: {}", e);
        }
    }
}

impl<T, O, E, C> Future for DetachedOp<T, O, E, C>
where
    O: PlatformOp,
    E: DriverError,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C, Error = E>,
{
    type Output = OpResult<T::Output, E, T::Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((e, payload)) = this.immediate_failure.take() {
            let completion = T::complete(payload, Err(e));
            return Poll::Ready(OpResult::Completed(completion.result, completion.output));
        }

        let table = this
            .completion_table
            .as_ref()
            .expect("DetachedOp missing completion_table but no immediate_failure");
        if let Poll::Ready(result) =
            poll_completion_table_once::<T, O, T::ErasedPayload, E, C>(&**table, this.token)
        {
            return Poll::Ready(result);
        }

        table.register_waker(this.token, cx.waker());
        poll_completion_table_once::<T, O, T::ErasedPayload, E, C>(&**table, this.token)
    }
}

#[derive(Clone, Copy)]
pub enum LocalState {
    Defined,
    Submitted,
    Completed,
}

/// A Future wrapper for asynchronous IO operations executed locally.
pub struct LocalOp<'a, T, P>
where
    P: crate::op::DriverProvider,
    T: IntoPlatformOp<
            P::Op,
            DriverCompletion = P::Completion,
            ErasedPayload = P::UP,
            Error = P::Error,
        >,
{
    pub(crate) state: LocalState,
    pub(crate) data: Option<T>,
    pub(crate) provider: P,
    pub(crate) user_data: usize,
    pub(crate) token: u64,
    pub(crate) marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, T, P> LocalOp<'a, T, P>
where
    P: crate::op::DriverProvider,
    T: IntoPlatformOp<
            P::Op,
            DriverCompletion = P::Completion,
            ErasedPayload = P::UP,
            Error = P::Error,
        >,
{
    pub fn new(data: T, provider: P) -> Self {
        Self {
            state: LocalState::Defined,
            data: Some(data),
            provider,
            user_data: 0,
            token: 0,
            marker: std::marker::PhantomData,
        }
    }
}

impl<'a, T, P> Future for LocalOp<'a, T, P>
where
    P: crate::op::DriverProvider,
    T: IntoPlatformOp<
            P::Op,
            DriverCompletion = P::Completion,
            ErasedPayload = P::UP,
            Error = P::Error,
        >,
{
    type Output = OpResult<T::Output, P::Error, T::Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = unsafe { self.get_unchecked_mut() };

        if let LocalState::Defined = op.state {
            trace!(
                op = %std::any::type_name::<T>(),
                "LocalOp::poll: submit begin"
            );

            let data = op.data.take().expect("Op started without data");
            let (driver_op, payload) = data.into_kernel_and_payload();

            let submit_res = op.provider.with_driver(|mut driver| {
                let mut slot = match driver.reserve_op() {
                    Ok(v) => v,
                    Err(e) => return Err((e, driver_op, payload)),
                };
                let user_data = slot.user_data();
                let token = slot.token();
                slot.set_payload(T::payload_into_erased(payload));

                let mut driver_op_opt = Some(driver_op);
                let result = slot.submit(&mut driver_op_opt);

                let mut fallback_payload = None;
                match &result {
                    DriverSubmitResult::Submitted(_)
                    | DriverSubmitResult::Failed {
                        status: SubmitStatus::InFlight,
                        ..
                    } => {
                        let _ = slot.persist();
                    }
                    DriverSubmitResult::Failed {
                        status: SubmitStatus::Void,
                        ..
                    } => {
                        if let Some(val) = driver_op_opt.take() {
                            drop(val);
                        }
                        fallback_payload = Some(slot.recover_payload().unwrap());
                    }
                }
                Ok((user_data, token, result, fallback_payload))
            });

            match submit_res {
                Err((e, driver_op, payload)) => {
                    drop(driver_op);
                    let completion = T::complete(payload, Err(e));
                    return Poll::Ready(OpResult::Completed(completion.result, completion.output));
                }
                Ok((user_data, token, result, fallback_payload)) => {
                    op.user_data = user_data;
                    op.token = token;
                    match result {
                        DriverSubmitResult::Submitted(_) => {
                            op.state = LocalState::Submitted;
                            trace!(
                                op = %std::any::type_name::<T>(),
                                user_data = op.user_data,
                                token = op.token,
                                "LocalOp::poll: submitted"
                            );
                        }
                        DriverSubmitResult::Failed { report, status } => match status {
                            SubmitStatus::Void => {
                                let payload_erased = fallback_payload.unwrap();
                                let payload = T::payload_from_erased(payload_erased);
                                trace!(
                                    op = %std::any::type_name::<T>(),
                                    user_data = op.user_data,
                                    status = ?status,
                                    error = %report,
                                    "LocalOp::poll: submit failed synchronously"
                                );
                                let completion = T::complete(payload, Err(report));
                                return Poll::Ready(OpResult::Completed(
                                    completion.result,
                                    completion.output,
                                ));
                            }
                            SubmitStatus::InFlight => {
                                op.state = LocalState::Submitted;
                                trace!(
                                    op = %std::any::type_name::<T>(),
                                    user_data = op.user_data,
                                    token = op.token,
                                    status = ?status,
                                    "LocalOp::poll: submitted in flight"
                                );
                            }
                        },
                    }
                }
            }
        }

        if let LocalState::Submitted = op.state {
            let token = op.token;
            let res =
                op.provider.with_driver(|mut driver| {
                    let mut is_ready = false;
                    let mut ready_val = None;

                    match poll_completion_table_once::<T, P::Op, P::UP, P::Error, P::Completion>(
                        &*driver.completion_table(),
                        token,
                    ) {
                        Poll::Ready(result) => {
                            is_ready = true;
                            ready_val = Some(result);
                        }
                        Poll::Pending => {
                            driver.register_completion_waker(token, cx.waker());
                            match poll_completion_table_once::<
                                T,
                                P::Op,
                                P::UP,
                                P::Error,
                                P::Completion,
                            >(&*driver.completion_table(), token)
                            {
                                Poll::Ready(result) => {
                                    is_ready = true;
                                    ready_val = Some(result);
                                }
                                Poll::Pending => {}
                            }
                        }
                    }
                    (is_ready, ready_val)
                });

            if res.0 {
                op.state = LocalState::Completed;
                trace!(
                    op = %std::any::type_name::<T>(),
                    user_data = op.user_data,
                    token = op.token,
                    "LocalOp::poll: completion ready"
                );
                Poll::Ready(res.1.unwrap())
            } else {
                trace!(
                    op = %std::any::type_name::<T>(),
                    user_data = op.user_data,
                    token = op.token,
                    "LocalOp::poll: completion pending"
                );
                Poll::Pending
            }
        } else {
            panic!("Polled after completion");
        }
    }
}

impl<'a, T, P> Drop for LocalOp<'a, T, P>
where
    P: crate::op::DriverProvider,
    T: IntoPlatformOp<
            P::Op,
            DriverCompletion = P::Completion,
            ErasedPayload = P::UP,
            Error = P::Error,
        >,
{
    fn drop(&mut self) {
        if let LocalState::Submitted = self.state {
            let user_data = self.user_data;
            let token = self.token;
            self.provider.with_driver(|mut driver| {
                driver.completion_table().mark_orphaned(token);
                driver.cancel_op(user_data);
            });
        }
    }
}

pub trait OpSubmitter<'a, P: crate::op::DriverProvider>: Clone + std::marker::Send + Sync {
    type Future<
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP, Error = P::Error>
            + std::marker::Send,
    >: Future<Output = OpResult<T::Output, P::Error, <T as IntoPlatformOp<P::Op>>::Completion>>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send;

    fn from_current_context() -> Self;
}

pub struct LocalSubmitter<P>(std::marker::PhantomData<fn() -> P>);

impl<P> Clone for LocalSubmitter<P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P> Copy for LocalSubmitter<P> {}

impl<P> LocalSubmitter<P> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}
impl<P> Default for LocalSubmitter<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, P: crate::op::DriverProvider> OpSubmitter<'a, P> for LocalSubmitter<P> {
    type Future<
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    > = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    {
        trace!("Submitting local op");
        op.submit_local(provider)
    }

    fn from_current_context() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
pub struct DetachedSubmitter;

impl DetachedSubmitter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DetachedSubmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, P: crate::op::DriverProvider> OpSubmitter<'a, P> for DetachedSubmitter {
    type Future<
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    > = DetachedOp<T, P::Op, P::Error, P::Completion>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    {
        provider.with_driver(|mut driver| op.submit_detached(&mut driver))
    }

    fn from_current_context() -> Self {
        Self::new()
    }
}
