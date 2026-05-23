use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::trace;

use crate::driver::{
    CompletionRecord, Driver, PlatformOp, PollRecordResult, RemoteWaker, SharedCompletionTable,
    SubmitBinder, SubmitStatus, encode_completion_token, event_res_to_result,
};
use crate::op::{IntoPlatformOp, Op};
use crate::slot::DetachedCancelTable;
use crate::{DriverErrorKind, DriverErrorReport, DriverResult, driver_error};

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
pub struct OpError {
    pub reason: LostReason,
    pub source: DriverErrorReport,
}

impl OpError {
    pub fn new(reason: LostReason, source: DriverErrorReport) -> Self {
        Self { reason, source }
    }

    /// 如果原因为 GenerationMismatch，则认为该错误是致命的（资源状态不确定）。
    pub fn is_lethal(&self) -> bool {
        matches!(self.reason, LostReason::GenerationMismatch)
    }
}

impl std::fmt::Display for OpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason, self.source)
    }
}

/// The result of an IO operation.
#[derive(Debug)]
pub enum OpResult<T, R = usize> {
    /// Operation completed (successfully or with IO error).
    Completed(DriverResult<R>, T),
    /// Operation failed because the resource ownership was lost.
    ResourceLost(OpError),
}

impl<T, R> OpResult<T, R> {
    /// Unwraps the result, assuming the operation completed (panics if Lost).
    pub fn unwrap(self) -> (R, T) {
        match self {
            OpResult::Completed(Ok(res), data) => (res, data),
            OpResult::Completed(Err(e), _) => panic!("OpResult::Completed(Err({}))", e),
            OpResult::ResourceLost(e) => panic!("OpResult::ResourceLost({})", e),
        }
    }

    /// Returns the result and the resource implementation (if available).
    pub fn into_inner(self) -> (DriverResult<R>, Option<T>) {
        match self {
            OpResult::Completed(res, data) => (res, Some(data)),
            OpResult::ResourceLost(err) => (Err(err.source), None),
        }
    }
}

/// The completion projection for a submitted operation.
#[derive(Debug)]
pub struct OpCompletion<T, R = usize> {
    pub result: DriverResult<R>,
    pub output: T,
}

impl<T, R> OpCompletion<T, R> {
    #[inline]
    pub fn new(result: DriverResult<R>, output: T) -> Self {
        Self { result, output }
    }

    #[inline]
    pub fn into_parts(self) -> (DriverResult<R>, T) {
        (self.result, self.output)
    }
}

#[inline]
pub(crate) fn payload_missing_error() -> OpError {
    OpError::new(
        LostReason::PayloadMissing,
        driver_error(
            DriverErrorKind::Internal,
            "driver-core/op",
            "operation payload lost: completion sidecar missing",
        ),
    )
}

#[inline]
pub(crate) fn generation_mismatch_error() -> OpError {
    OpError::new(
        LostReason::GenerationMismatch,
        driver_error(
            DriverErrorKind::Internal,
            "driver-core/op",
            "operation lost: slot recycled (generation mismatch)",
        ),
    )
}

#[inline]
pub(crate) fn completion_record_to_result<T, O, UP, C>(
    record: CompletionRecord<UP, C>,
) -> Poll<OpResult<T::Output, T::Completion>>
where
    UP: Send,
    O: PlatformOp,
    T: IntoPlatformOp<O, DriverCompletion = C, ErasedPayload = UP>,
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
    let res = detail.unwrap_or_else(|| event_res_to_result::<C>(event.res));
    let completion = T::complete(payload, res);
    Poll::Ready(OpResult::Completed(completion.result, completion.output))
}

#[inline]
pub(crate) fn poll_completion_table_once<T, O, UP, C>(
    table: &dyn crate::driver::CompletionAccess<UP, C>,
    token: u64,
) -> Poll<OpResult<T::Output, T::Completion>>
where
    UP: Send,
    O: PlatformOp,
    T: IntoPlatformOp<O, DriverCompletion = C, ErasedPayload = UP>,
    C: crate::driver::CompletionValue,
{
    match table.try_take_record(token) {
        PollRecordResult::Ready(record) => completion_record_to_result::<T, O, UP, C>(record),
        PollRecordResult::Stale => Poll::Ready(OpResult::<T::Output, T::Completion>::ResourceLost(
            generation_mismatch_error(),
        )),
        PollRecordResult::Pending => Poll::Pending,
    }
}

type DetachedOpMarker<T, UP, C, O> = (T, UP, C, fn() -> O);

/// A Future representing a detached operation.
pub struct DetachedOp<T, O, C>
where
    O: PlatformOp,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C>,
{
    pub(crate) completion_table: Option<SharedCompletionTable<T::ErasedPayload, C>>,
    pub(crate) cancel_signal: Option<Arc<DetachedCancelTable>>,
    pub(crate) cancel_waker: Option<Arc<dyn RemoteWaker>>,
    pub(crate) token: u64,
    pub(crate) immediate_failure: Option<(DriverErrorReport, T::UserPayload)>,
    pub(crate) _phantom: std::marker::PhantomData<DetachedOpMarker<T, T::ErasedPayload, C, O>>,
}

unsafe impl<
    T: IntoPlatformOp<O, DriverCompletion = C> + std::marker::Send,
    O: PlatformOp,
    C: crate::driver::CompletionValue,
> std::marker::Send for DetachedOp<T, O, C>
{
}

impl<T, O, C> Drop for DetachedOp<T, O, C>
where
    O: PlatformOp,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C>,
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

impl<T, O, C> Future for DetachedOp<T, O, C>
where
    O: PlatformOp,
    C: crate::driver::CompletionValue,
    T: IntoPlatformOp<O, DriverCompletion = C>,
{
    type Output = OpResult<T::Output, T::Completion>;

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
            poll_completion_table_once::<T, O, T::ErasedPayload, C>(&**table, this.token)
        {
            return Poll::Ready(result);
        }

        table.register_waker(this.token, cx.waker());
        poll_completion_table_once::<T, O, T::ErasedPayload, C>(&**table, this.token)
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
    T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>,
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
    T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>,
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
    T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>,
{
    type Output = OpResult<T::Output, T::Completion>;

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
                let (user_data, generation) = match driver.reserve_op() {
                    Ok(v) => v,
                    Err(e) => return Err((e, driver_op, payload)),
                };
                let token = encode_completion_token(user_data, generation);
                driver.slot_set_payload(user_data, T::payload_into_erased(payload));

                let mut driver_op_opt = Some(driver_op);
                let result = driver
                    .submit(user_data, &mut driver_op_opt, SubmitBinder::new())
                    .into_inner();

                let mut fallback_payload = None;
                if let Err((_, status)) = &result
                    && *status == SubmitStatus::Void
                {
                    if let Some(val) = driver_op_opt.take() {
                        drop(val);
                    }
                    fallback_payload = Some(driver.slot_take_payload(user_data).unwrap());
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
                        Ok(_) => {
                            op.state = LocalState::Submitted;
                            trace!(
                                op = %std::any::type_name::<T>(),
                                user_data = op.user_data,
                                token = op.token,
                                "LocalOp::poll: submitted"
                            );
                        }
                        Err((e, status)) => {
                            if status == SubmitStatus::Void {
                                let payload_erased = fallback_payload.unwrap();
                                let payload = T::payload_from_erased(payload_erased);
                                trace!(
                                    op = %std::any::type_name::<T>(),
                                    user_data = op.user_data,
                                    status = ?status,
                                    error = %e,
                                    "LocalOp::poll: submit failed synchronously"
                                );
                                let completion = T::complete(payload, Err(e));
                                return Poll::Ready(OpResult::Completed(
                                    completion.result,
                                    completion.output,
                                ));
                            } else {
                                op.state = LocalState::Submitted;
                                trace!(
                                    op = %std::any::type_name::<T>(),
                                    user_data = op.user_data,
                                    token = op.token,
                                    status = ?status,
                                    "LocalOp::poll: submitted in flight"
                                );
                            }
                        }
                    }
                }
            }
        }

        if let LocalState::Submitted = op.state {
            let token = op.token;
            let res = op.provider.with_driver(|mut driver| {
                let mut is_ready = false;
                let mut ready_val = None;

                match poll_completion_table_once::<T, P::Op, P::UP, P::Completion>(
                    &*driver.completion_table(),
                    token,
                ) {
                    Poll::Ready(result) => {
                        is_ready = true;
                        ready_val = Some(result);
                    }
                    Poll::Pending => {
                        driver.register_completion_waker(token, cx.waker());
                        match poll_completion_table_once::<T, P::Op, P::UP, P::Completion>(
                            &*driver.completion_table(),
                            token,
                        ) {
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
    T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>,
{
    fn drop(&mut self) {
        if let LocalState::Submitted = self.state {
            let user_data = self.user_data;
            self.provider.with_driver(|mut driver| {
                driver.cancel_op(user_data);
            });
        }
    }
}

pub trait OpSubmitter<'a, P: crate::op::DriverProvider>: Clone + std::marker::Send + Sync {
    type Future<
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send,
    >: Future<Output = OpResult<T::Output, <T as IntoPlatformOp<P::Op>>::Completion>>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send;

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
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send,
    > = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send,
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
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send,
    > = DetachedOp<T, P::Op, P::Completion>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<P::Op, DriverCompletion = P::Completion, ErasedPayload = P::UP>
            + std::marker::Send,
    {
        provider.with_driver(|mut driver| op.submit_detached(&mut driver))
    }

    fn from_current_context() -> Self {
        Self::new()
    }
}
