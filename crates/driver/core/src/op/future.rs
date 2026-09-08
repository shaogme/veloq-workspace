use futures_core::Stream;
use tracing::trace;
use veloq_std::{
    any::type_name,
    error::Error,
    fmt, format,
    future::Future,
    marker::{PhantomData, Send, Sync},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    DriverCoreError, DriverError, DriverReport, DriverResult,
    driver::{
        AnomalyAttach, CancelRequest, CompletionAccess, CompletionAnomalyKind,
        CompletionAnomalyReason, CompletionContinuation, CompletionRecord, CompletionToken,
        CompletionValue, Driver, DriverRaw, DriverSubmitResult, OpToken, PollRecordResult,
        RemoteCancelSender, RemoteWaker, SharedCompletionTable, SubmitStatus,
    },
    op::{DriverProvider, IntoPlatformOp, Op, SingleShotOp},
    slot::{SlotError, SlotPayload, SlotSpec},
};

use diagweave::prelude::*;

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostReason {
    /// 槽位已被回收，用于新一代操作 (Generation Mismatch)。
    /// 调用方应当认为关联的 IO 后端（如 Socket 或 Buffer）已处于不确定状态。
    GenerationMismatch,
    /// 内部错误：操作负载丢失 (Completion sidecar missing)。
    PayloadMissing,
    /// 内部错误：擦除后的 payload 与操作类型不匹配。
    PayloadTypeMismatch,
    /// 其它未知原因造成的资源丢失。
    Other,
}

impl fmt::Display for LostReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationMismatch => write!(f, "generation mismatch (slot recycled)"),
            Self::PayloadMissing => write!(f, "payload missing"),
            Self::PayloadTypeMismatch => write!(f, "payload type mismatch"),
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

impl<E> fmt::Display for OpError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub fn new(result: DriverResult<R, E>, output: T) -> Self {
        Self { result, output }
    }

    pub fn into_parts(self) -> (DriverResult<R, E>, T) {
        (self.result, self.output)
    }
}

impl LostReason {
    #[inline]
    pub(crate) fn from_anomaly_reason(reason: CompletionAnomalyReason) -> Self {
        match reason {
            CompletionAnomalyReason::StaleGeneration => Self::GenerationMismatch,
            CompletionAnomalyReason::UnknownSlot
            | CompletionAnomalyReason::NonActiveSlot
            | CompletionAnomalyReason::BackendContextUnknown
            | CompletionAnomalyReason::BackendSpecific(_) => Self::Other,
        }
    }
}

impl<E> OpError<E>
where
    E: DriverError,
{
    #[inline]
    pub(crate) fn payload_missing() -> Self {
        Self::new(
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
    pub(crate) fn payload_projection(source: DriverReport<E>) -> Self {
        Self::new(LostReason::PayloadTypeMismatch, source)
    }

    #[inline]
    pub(crate) fn from_completion_anomaly(
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> Self {
        let reason = LostReason::from_anomaly_reason(kind.reason());

        let mut report = DriverCoreError::Internal
            .to_report()
            .push_ctx("scope", "driver-core/op")
            .with_ctx("completion_token", attach.token.raw())
            .with_ctx("completion_anomaly", format!("{:?}", kind.reason()))
            .attach_note("operation completion became unavailable");

        if let Some(index) = kind.index() {
            report = report.with_ctx("slot_index", index);
        }
        if let Some(expected_generation) = kind.expected_generation() {
            report = report.with_ctx("expected_generation", expected_generation);
        }
        if let Some(actual_generation) = kind.actual_generation() {
            report = report.with_ctx("actual_generation", actual_generation);
        }
        if let Some(status) = kind.status() {
            report = report.with_ctx("slot_status", format!("{status:?}"));
        }
        if let Some(backend) = kind.backend().or_else(|| attach.raw.map(|raw| raw.backend)) {
            report = report.with_ctx("completion_backend", format!("{backend:?}"));
        }
        if let Some(backend_context) = kind.backend_context_value() {
            report = report.with_ctx("completion_backend_context", backend_context);
        }
        if let Some(raw) = attach.raw {
            report = report
                .with_ctx("raw_result", raw.res)
                .with_ctx("completion_flags", raw.flags);
        }

        Self::new(reason, E::from_core_report(report))
    }
}

/// 一个操作产出的一项：单发操作只有一项，multishot 每条完成一项。
pub type OpItem<T, Spec> = OpResult<
    <T as IntoPlatformOp<Spec>>::Output,
    <Spec as SlotSpec>::Error,
    <T as IntoPlatformOp<Spec>>::Completion,
>;

/// 把一条完成记录投影成用户可见的一项，并带出「这个操作还会不会再产出完成」。
///
/// 投影失败时那条记录的 `cleanup` 必须跑——完成式 I/O 下内核可能已经在记录里放了资源
/// （accept 出来的 fd 之类），丢弃记录不等于丢弃资源。
#[inline]
fn item_from_record<T, Spec>(
    record: CompletionRecord<Spec>,
) -> (OpItem<T, Spec>, CompletionContinuation)
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    let CompletionRecord {
        event,
        payload: erased,
        detail,
        mut cleanup,
        continuation,
    } = record;

    let payload = match T::try_record_from_erased(erased) {
        Ok(payload) => payload,
        Err(report) => {
            let _ = cleanup.run();
            return (
                OpResult::ResourceLost(OpError::payload_projection(report)),
                continuation,
            );
        }
    };
    cleanup.disarm();
    let res =
        detail.unwrap_or_else(|| Spec::Completion::from_event_res::<Spec::Error>(event.res()));
    let completion = T::complete(payload, res);
    (
        OpResult::Completed(completion.result, completion.output),
        continuation,
    )
}

/// 从完成表里取一条记录。`Pending` 表示信箱是空的而操作仍在途。
///
/// 取不到记录的那几种异常都算终态：token 已经指不到有效的 slot 了，再等下去也不会有东西。
#[inline]
fn poll_record_once<T, Spec>(
    table: &dyn CompletionAccess<Spec>,
    token: OpToken,
) -> Poll<(OpItem<T, Spec>, CompletionContinuation)>
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    match table.try_take_record(token) {
        Ok(PollRecordResult::Ready(record)) => Poll::Ready(item_from_record::<T, Spec>(record)),
        Ok(PollRecordResult::Unavailable { kind, attach }) => Poll::Ready((
            OpResult::ResourceLost(OpError::from_completion_anomaly(kind, attach)),
            CompletionContinuation::Final,
        )),
        Ok(PollRecordResult::Pending) => Poll::Pending,
        Err(report) => Poll::Ready((
            OpResult::ResourceLost(OpError::new(LostReason::Other, report)),
            CompletionContinuation::Final,
        )),
    }
}

type DetachedOpMarker<T, Spec> = fn() -> (T, Spec);

/// 一个已提交操作的句柄，不借用驱动。
///
/// 它是一条**完成流**（[`futures_core::Stream`]）：单发操作的流只有一项，multishot 的流
/// 每条完成一项，终点由记录携带的
/// [`CompletionContinuation`](crate::driver::CompletionContinuation) 决定。单发操作
/// （[`SingleShotOp`]）额外实现 [`Future`]，于是 `.await` 直接拿那唯一的一项。
pub struct DetachedOp<T, Spec>
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    pub(crate) completion_table: Option<SharedCompletionTable<Spec>>,
    pub(crate) cancel_sender: Option<RemoteCancelSender>,
    pub(crate) cancel_waker: Option<Arc<dyn RemoteWaker<Spec::Error>>>,
    pub(crate) token: Option<OpToken>,
    /// 提交阶段就已经定局的那一项（同步失败或资源丢失）。取走它之后就再没有别的来源，
    /// 流随即结束——所以它和 `token` 不会同时是 `Some`。
    pub(crate) immediate: Option<OpItem<T, Spec>>,
    pub(crate) _phantom: PhantomData<DetachedOpMarker<T, Spec>>,
}

impl<T, Spec> DetachedOp<T, Spec>
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    /// 操作已经进了内核：后续的项都从完成表取。
    pub(crate) fn armed(
        completion_table: SharedCompletionTable<Spec>,
        cancel_sender: RemoteCancelSender,
        cancel_waker: Arc<dyn RemoteWaker<Spec::Error>>,
        token: OpToken,
    ) -> Self {
        Self {
            completion_table: Some(completion_table),
            cancel_sender: Some(cancel_sender),
            cancel_waker: Some(cancel_waker),
            token: Some(token),
            immediate: None,
            _phantom: PhantomData,
        }
    }

    /// 操作从来没进内核：只有一项，就是提交失败本身。
    pub(crate) fn settled(item: OpItem<T, Spec>) -> Self {
        Self {
            completion_table: None,
            cancel_sender: None,
            cancel_waker: None,
            token: None,
            immediate: Some(item),
            _phantom: PhantomData,
        }
    }

    /// 取下一项。`Ready(None)` 表示这个操作已经终止，不会再有项了。
    fn poll_item(&mut self, cx: &mut Context<'_>) -> Poll<Option<OpItem<T, Spec>>> {
        if let Some(item) = self.immediate.take() {
            return Poll::Ready(Some(item));
        }

        let (Some(table), Some(token)) = (self.completion_table.as_ref(), self.token) else {
            return Poll::Ready(None);
        };

        let mut polled = poll_record_once::<T, Spec>(&**table, token);
        if polled.is_pending() {
            // 注册与发布竞速：注册之后再取一次，避免丢唤醒。
            table.register_waker(token, cx.waker());
            polled = poll_record_once::<T, Spec>(&**table, token);
        }

        match polled {
            Poll::Ready((item, continuation)) => {
                if continuation.is_final() {
                    // 终态记录被取走的同时 slot 已经归还、generation 已经推进，token 从此
                    // 失效——`Drop` 不能再拿它去 `mark_orphaned` / 请求取消。
                    self.token = None;
                }
                Poll::Ready(Some(item))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// 这个操作是否还在内核里（诊断与测试用）。
    pub fn is_armed(&self) -> bool {
        self.token.is_some()
    }

    /// 确保该操作已被 Arm。
    pub fn arm(&mut self) -> bool {
        self.is_armed() || self.immediate.is_some()
    }
}

impl<T, Spec> Drop for DetachedOp<T, Spec>
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    fn drop(&mut self) {
        // token 为 `None` 说明这个操作已经取到了它的终态完成：slot 早就归还、generation
        // 也推进过了。此时再 `mark_orphaned` / `abandon` 命中的必然是 generation 校验，
        // 只会往诊断里记一条假的 `StaleGeneration`，把真正的异常淹没掉，还要让驱动为一个
        // 已经结束的操作白跑一次查表与唤醒。
        let Some(token) = self.token else {
            return;
        };

        if let Some(table) = self.completion_table.as_ref() {
            table.mark_orphaned(token);
        }
        if let Some(cancel_sender) = self.cancel_sender.as_ref() {
            let _ = cancel_sender.send(CancelRequest::abandon(token));
        }
        // 唤醒的唯一目的是让驱动线程去处理刚投进去的取消请求，所以它跟着请求走。
        if let Some(cancel_waker) = self.cancel_waker.as_ref()
            && let Err(e) = cancel_waker.wake()
        {
            trace!("DetachedOp cancel wake failed: {}", e);
        }
    }
}

/// 单发操作才是 future：`await` 一个 multishot 操作等于「取第一条完成然后取消」，
/// 那是陷阱不是特性，所以让它在编译期就不成立。见 [`SingleShotOp`]。
impl<T, Spec> Future for DetachedOp<T, Spec>
where
    Spec: SlotSpec,
    T: SingleShotOp<Spec>,
{
    type Output = OpItem<T, Spec>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match this.poll_item(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(item),
            Poll::Ready(None) => panic!("DetachedOp polled after completion"),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, Spec> futures_core::Stream for DetachedOp<T, Spec>
where
    Spec: SlotSpec,
    T: IntoPlatformOp<Spec>,
{
    type Item = OpItem<T, Spec>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        this.poll_item(cx)
    }
}

#[derive(Clone, Copy)]
pub enum LocalState {
    Defined,
    Submitted,
    Completed,
}

/// 一个在当前线程上执行的操作句柄。
///
/// 与 [`DetachedOp`] 的分工是「借不借驱动」，不是「一条还是多条完成」：这里同样既是单发
/// 操作的 [`Future`]，也是任意操作的完成流。thread-local 形态的 multishot 走这条路，不必
/// 为了拿到流而退回 `Arc` 化的 detached 句柄。
pub struct LocalOp<'a, T, P>
where
    P: DriverProvider,
    T: IntoPlatformOp<P::SlotSpec>,
{
    pub(crate) state: LocalState,
    pub(crate) data: Option<T>,
    pub(crate) provider: P,
    pub(crate) token: Option<OpToken>,
    pub(crate) immediate: Option<OpItem<T, P::SlotSpec>>,
    pub(crate) marker: PhantomData<&'a ()>,
}

type LocalSubmitOutcome<P> = (
    OpToken,
    DriverSubmitResult<SlotError<<P as DriverProvider>::SlotSpec>>,
    Option<SlotPayload<<P as DriverProvider>::SlotSpec>>,
);

impl<'a, T, P> LocalOp<'a, T, P>
where
    P: DriverProvider,
    T: IntoPlatformOp<P::SlotSpec>,
{
    pub fn new(data: T, provider: P) -> Self {
        Self {
            state: LocalState::Defined,
            data: Some(data),
            provider,
            token: None,
            immediate: None,
            marker: PhantomData,
        }
    }

    /// 查询该操作当前是否处于 Armed 状态或已同步定局。
    pub fn is_armed(&self) -> bool {
        matches!(self.state, LocalState::Submitted) || self.immediate.is_some()
    }

    /// 确保该操作已被 Arm (提交到底层驱动)。
    pub fn arm(&mut self) -> bool {
        if let LocalState::Defined = self.state
            && let Some(item) = self.submit()
        {
            self.immediate = Some(item);
        }
        self.is_armed()
    }

    /// 提交。返回 `Some` 表示这次提交同步就定局了（操作一条完成都不会产生）。
    fn submit(&mut self) -> Option<OpItem<T, P::SlotSpec>> {
        trace!(
            op = %type_name::<T>(),
            "LocalOp: submit begin"
        );

        let data = self.data.take().expect("Op started without data");
        let (driver_op, payload) = data.into_kernel_and_payload();

        let submit_res: Result<LocalSubmitOutcome<P>, _> =
            self.provider.with_driver(|mut driver| {
                let mut slot = match driver.reserve_op() {
                    Ok(v) => v,
                    Err(e) => return Err((e, driver_op, payload)),
                };
                let token = slot.token();
                slot.set_payload(T::payload_into_erased(payload));

                let mut driver_op_opt = Some(driver_op);
                let result = slot.submit(&mut driver_op_opt);

                let mut recovered = None;
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
                        drop(driver_op_opt.take());
                        recovered = slot.recover_payload();
                    }
                }
                Ok((token, result, recovered))
            });

        let (token, result, recovered) = match submit_res {
            Err((report, driver_op, payload)) => {
                drop(driver_op);
                self.state = LocalState::Completed;
                return Some(T::submit_failed(T::payload_into_erased(payload), report));
            }
            Ok(outcome) => outcome,
        };

        self.token = Some(token);
        match result {
            DriverSubmitResult::Failed {
                report,
                status: SubmitStatus::Void,
            } => {
                trace!(
                    op = %type_name::<T>(),
                    error = %report,
                    "LocalOp: submit failed synchronously"
                );
                self.state = LocalState::Completed;
                let Some(erased) = recovered else {
                    return Some(OpResult::ResourceLost(OpError::payload_missing()));
                };
                Some(T::submit_failed(erased, report))
            }
            DriverSubmitResult::Submitted(_) | DriverSubmitResult::Failed { .. } => {
                self.state = LocalState::Submitted;
                trace!(
                    op = %type_name::<T>(),
                    token = CompletionToken::user(token).raw(),
                    "LocalOp: submitted"
                );
                None
            }
        }
    }

    /// 取下一项。`Ready(None)` 表示这个操作已经终止。
    fn poll_item(&mut self, cx: &mut Context<'_>) -> Poll<Option<OpItem<T, P::SlotSpec>>> {
        if let Some(item) = self.immediate.take() {
            return Poll::Ready(Some(item));
        }

        if let LocalState::Defined = self.state
            && let Some(item) = self.submit()
        {
            return Poll::Ready(Some(item));
        }

        let token = match self.state {
            LocalState::Submitted => self
                .token
                .expect("LocalOp submitted state missing completion token"),
            LocalState::Completed => return Poll::Ready(None),
            LocalState::Defined => unreachable!("submit() always leaves the Defined state"),
        };

        let taken = self.provider.with_driver(|mut driver| {
            let polled = poll_record_once::<T, P::SlotSpec>(&*driver.completion_table(), token);
            if polled.is_ready() {
                return polled;
            }
            driver.register_completion_waker(token, cx.waker());
            poll_record_once::<T, P::SlotSpec>(&*driver.completion_table(), token)
        });

        match taken {
            Poll::Ready((item, continuation)) => {
                if continuation.is_final() {
                    self.state = LocalState::Completed;
                    self.token = None;
                }
                Poll::Ready(Some(item))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 见 [`DetachedOp`] 上同名实现的说明：只有单发操作能 `await`。
impl<'a, T, P> Future for LocalOp<'a, T, P>
where
    P: DriverProvider,
    T: SingleShotOp<P::SlotSpec>,
{
    type Output = OpItem<T, P::SlotSpec>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = unsafe { self.get_unchecked_mut() };
        match op.poll_item(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(item),
            Poll::Ready(None) => panic!("Polled after completion"),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a, T, P> futures_core::Stream for LocalOp<'a, T, P>
where
    P: DriverProvider,
    T: IntoPlatformOp<P::SlotSpec>,
{
    type Item = OpItem<T, P::SlotSpec>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let op = unsafe { self.get_unchecked_mut() };
        op.poll_item(cx)
    }
}

impl<'a, T, P> Drop for LocalOp<'a, T, P>
where
    P: DriverProvider,
    T: IntoPlatformOp<P::SlotSpec>,
{
    fn drop(&mut self) {
        if let LocalState::Submitted = self.state
            && let Some(token) = self.token
        {
            self.provider.with_driver(|mut driver| {
                driver.completion_table().mark_orphaned(token);
                let _ = driver.cancel_op(CancelRequest::abandon(token));
            });
        }
    }
}

pub trait OpSubmitter<'a, P: DriverProvider>: Clone + Send + Sync {
    /// 单发提交的句柄：`await` 得到唯一的那条完成。
    type Future<T: SingleShotOp<P::SlotSpec> + Send>: Future<Output = OpItem<T, P::SlotSpec>>;

    /// 流式提交的句柄：一次提交、多条完成。单发操作在这里退化成只有一项的流。
    ///
    /// 与 `Future` 是**同一个具体类型**的两种看法，不是两条实现路径。
    type Stream<T: IntoPlatformOp<P::SlotSpec> + Send>: Stream<Item = OpItem<T, P::SlotSpec>>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: SingleShotOp<P::SlotSpec> + Send;

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> Self::Stream<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + Send;

    fn from_current_context() -> Self;
}

pub struct LocalSubmitter<P>(PhantomData<fn() -> P>);

impl<P> Clone for LocalSubmitter<P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P> Copy for LocalSubmitter<P> {}

impl<P> LocalSubmitter<P> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<P> Default for LocalSubmitter<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for LocalSubmitter<P> {
    type Future<T: SingleShotOp<P::SlotSpec> + Send> = LocalOp<'a, T, P>;
    type Stream<T: IntoPlatformOp<P::SlotSpec> + Send> = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: SingleShotOp<P::SlotSpec> + Send,
    {
        trace!("Submitting local op");
        op.submit_local(provider)
    }

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<P::SlotSpec> + Send,
    {
        trace!("Submitting local op stream");
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

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for DetachedSubmitter {
    type Future<T: SingleShotOp<P::SlotSpec> + Send> =
        DetachedOp<T, <P::Driver<'a> as DriverRaw>::SlotSpec>;
    type Stream<T: IntoPlatformOp<P::SlotSpec> + Send> =
        DetachedOp<T, <P::Driver<'a> as DriverRaw>::SlotSpec>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: SingleShotOp<P::SlotSpec> + Send,
    {
        provider.with_driver(|mut driver| op.submit_detached(&mut driver))
    }

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> Self::Stream<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + Send,
    {
        provider.with_driver(|mut driver| op.submit_detached(&mut driver))
    }

    fn from_current_context() -> Self {
        Self::new()
    }
}
