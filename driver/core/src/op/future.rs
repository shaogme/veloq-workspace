use std::cell::RefCell;
use std::rc::Rc;
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
) -> Poll<OpResult<T, T::Completion>>
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
    let data = T::from_user_payload(payload);
    let res = detail.unwrap_or_else(|| event_res_to_result::<C>(event.res));
    let completion = data.map_completion_result(res);
    Poll::Ready(OpResult::Completed(completion, data))
}

#[inline]
pub(crate) fn poll_completion_table_once<T, O, UP, C>(
    table: &dyn crate::driver::CompletionAccess<UP, C>,
    token: u64,
) -> Poll<OpResult<T, T::Completion>>
where
    UP: Send,
    O: PlatformOp,
    T: IntoPlatformOp<O, DriverCompletion = C, ErasedPayload = UP>,
    C: crate::driver::CompletionValue,
{
    match table.try_take_record(token) {
        PollRecordResult::Ready(record) => completion_record_to_result::<T, O, UP, C>(record),
        PollRecordResult::Stale => Poll::Ready(OpResult::<T, T::Completion>::ResourceLost(
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
    pub(crate) immediate_failure: Option<(DriverErrorReport, T)>,
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
    type Output = OpResult<T, T::Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some((e, data)) = this.immediate_failure.take() {
            return Poll::Ready(OpResult::Completed(Err(e), data));
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
pub struct LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>,
{
    pub(crate) state: LocalState,
    pub(crate) data: Option<T>,
    pub(crate) driver: Rc<RefCell<D>>,
    pub(crate) user_data: usize,
    pub(crate) token: u64,
}

impl<T, D> LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>,
{
    pub fn new(data: T, driver: Rc<RefCell<D>>) -> Self {
        Self {
            state: LocalState::Defined,
            data: Some(data),
            driver,
            user_data: 0,
            token: 0,
        }
    }
}

impl<T, D> Future for LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>,
{
    type Output = OpResult<T, T::Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = unsafe { self.get_unchecked_mut() };

        if let LocalState::Defined = op.state {
            trace!(
                op = %std::any::type_name::<T>(),
                "LocalOp::poll: submit begin"
            );
            let mut driver = op.driver.borrow_mut();

            // Submit to driver
            let data = op.data.take().expect("Op started without data");
            let (driver_op, payload) = data.into_kernel_and_payload();

            let (user_data, generation) = match driver.reserve_op() {
                Ok(v) => v,
                Err(e) => {
                    drop(driver_op);
                    return Poll::Ready(OpResult::Completed(Err(e), T::from_user_payload(payload)));
                }
            };
            op.user_data = user_data;
            op.token = encode_completion_token(user_data, generation);
            driver.slot_set_payload(user_data, T::payload_into_erased(payload));

            let mut driver_op_opt = Some(driver_op);
            let result = driver
                .submit(user_data, &mut driver_op_opt, SubmitBinder::new())
                .into_inner();

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
                        if let Some(val) = driver_op_opt.take() {
                            drop(val);
                        }
                        let payload_erased = driver.slot_take_payload(user_data).unwrap_or_else(|| {
                            panic!(
                                "Payload missing while recovering submit failure: user_data={}, status={:?}, error={}",
                                user_data, status, e
                            )
                        });
                        let payload = T::payload_from_erased(payload_erased);
                        let data = T::from_user_payload(payload);
                        trace!(
                            op = %std::any::type_name::<T>(),
                            user_data = user_data,
                            status = ?status,
                            error = %e,
                            "LocalOp::poll: submit failed synchronously"
                        );
                        return Poll::Ready(OpResult::Completed(Err(e), data));
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

            op.state = LocalState::Submitted;
        }

        if let LocalState::Submitted = op.state {
            let mut driver = op.driver.borrow_mut();
            match poll_completion_table_once::<T, D::Op, D::UP, D::Completion>(
                &*driver.completion_table(),
                op.token,
            ) {
                Poll::Ready(result) => {
                    op.state = LocalState::Completed;
                    trace!(
                        op = %std::any::type_name::<T>(),
                        user_data = op.user_data,
                        token = op.token,
                        "LocalOp::poll: completion ready"
                    );
                    Poll::Ready(result)
                }
                Poll::Pending => {
                    driver.register_completion_waker(op.token, cx.waker());
                    trace!(
                        op = %std::any::type_name::<T>(),
                        user_data = op.user_data,
                        token = op.token,
                        "LocalOp::poll: completion pending"
                    );
                    match poll_completion_table_once::<T, D::Op, D::UP, D::Completion>(
                        &*driver.completion_table(),
                        op.token,
                    ) {
                        Poll::Ready(result) => {
                            op.state = LocalState::Completed;
                            trace!(
                                op = %std::any::type_name::<T>(),
                                user_data = op.user_data,
                                token = op.token,
                                "LocalOp::poll: completion ready after register"
                            );
                            Poll::Ready(result)
                        }
                        Poll::Pending => {
                            trace!(
                                op = %std::any::type_name::<T>(),
                                user_data = op.user_data,
                                token = op.token,
                                "LocalOp::poll: still pending"
                            );
                            Poll::Pending
                        }
                    }
                }
            }
        } else {
            panic!("Polled after completion");
        }
    }
}

impl<T, D> Drop for LocalOp<T, D>
where
    D: Driver,
    T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>,
{
    fn drop(&mut self) {
        if let LocalState::Submitted = self.state {
            self.driver.borrow_mut().cancel_op(self.user_data);
        }
    }
}

pub trait OpSubmitter<D: Driver>: Clone + std::marker::Send + Sync {
    type Future<
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
    >: Future<Output = OpResult<T, <T as IntoPlatformOp<D::Op>>::Completion>>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send;

    fn from_current_context() -> Self;
}

#[derive(Clone, Copy)]
pub struct LocalSubmitter;

impl<D: Driver> OpSubmitter<D> for LocalSubmitter {
    type Future<
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
    > = LocalOp<T, D>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> LocalOp<T, D>
    where
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
    {
        trace!("Submitting local op");
        op.submit_local(driver)
    }

    fn from_current_context() -> Self {
        Self
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

impl<D: Driver> OpSubmitter<D> for DetachedSubmitter {
    type Future<
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
    > = DetachedOp<T, D::Op, D::Completion>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<D>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
    {
        op.submit_detached(&mut *driver.borrow_mut())
    }

    fn from_current_context() -> Self {
        Self::new()
    }
}
