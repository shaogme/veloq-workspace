//! `DetachedOp` 的生命周期收尾，以及「单发与 multishot 共用一个句柄」这件事本身。
//!
//! 用例成对出现，因为它们共用同一段 `poll_item` / `Drop`：
//!
//! - 取到终态完成的句柄 drop 时**什么都不该做**，仍在途的句柄 drop 时**必须**放弃 slot
//!   并请求取消；
//! - 单发操作当流用只有一项，multishot 则一直产出到 `Final` 为止——两者的差别全部来自
//!   记录携带的 `CompletionContinuation`，没有第二个句柄类型。

use super::*;

use crate::{
    DriverCoreError, DriverError, DriverResult,
    driver::{
        CompletionAccess, CompletionBackend, CompletionCleanupGuard, CompletionContinuation,
        CompletionMutationOutcome, CompletionPacket, CompletionWritePermit, PlatformOp,
        RecordCompletionResult, UserCompletionEvent,
    },
    op::{IntoPlatformOp, OpKind},
    slot::{self, Generation},
};
use veloq_std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::Waker,
};

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {
    type CleanupContext<'a> = ();
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct DummyError;

impl fmt::Display for DummyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dummy error")
    }
}

impl Error for DummyError {}

impl DriverError for DummyError {
    #[inline]
    fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
        report.map_err(|_| DummyError)
    }
}

struct DummySlotSpec;

impl slot::SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = DummyError;
    type Completion = usize;
    type CompletionDiagnostics = ();
}

struct DummyOp;

impl IntoPlatformOp<DummySlotSpec> for DummyOp {
    type SubmitPayload = ();
    type RecordPayload = ();
    type Output = ();
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn into_kernel_and_payload(self) -> (DummyPlatformOp, Self::SubmitPayload) {
        (DummyPlatformOp, ())
    }

    fn payload_into_erased(_payload: Self::SubmitPayload) {}

    fn try_record_from_erased(_erased: ()) -> DriverResult<Self::RecordPayload, DummyError> {
        Ok(())
    }

    fn complete(
        _payload: Self::RecordPayload,
        res: DriverResult<usize, DummyError>,
    ) -> OpCompletion<Self::Output, DummyError, Self::Completion> {
        OpCompletion::new(res, ())
    }
}

impl SingleShotOp<DummySlotSpec> for DummyOp {}

/// 只回答 `try_take_record` 与 `mark_orphaned` 的最小完成表：前者按「还剩几条记录」回答
/// 「已就绪 / 仍在途」，后者数一下自己被调用了几次。
///
/// 记录在取的时候现造而不是预先存着——存一条 `CompletionRecord` 需要一把锁，而 loom
/// feature 下的锁只能在 model 里用，这里的用例并不是 loom model。
#[derive(Default)]
struct MockTable {
    /// 还没被取走的完成条数；`0` 表示这个操作仍在途。
    ready: AtomicUsize,
    /// 其中前几条带 `More`（模拟 multishot）；其余的是终态。
    more: AtomicUsize,
    orphaned: AtomicUsize,
}

impl MockTable {
    fn with_ready_record() -> Self {
        Self {
            ready: AtomicUsize::new(1),
            more: AtomicUsize::new(0),
            orphaned: AtomicUsize::new(0),
        }
    }

    /// `more` 条中间完成之后跟一条终态完成。
    fn streaming(more: usize) -> Self {
        Self {
            ready: AtomicUsize::new(more + 1),
            more: AtomicUsize::new(more),
            orphaned: AtomicUsize::new(0),
        }
    }

    fn orphaned(&self) -> usize {
        self.orphaned.load(Ordering::Relaxed)
    }

    fn next_continuation(&self) -> CompletionContinuation {
        if self.more.load(Ordering::Relaxed) == 0 {
            return CompletionContinuation::Final;
        }
        self.more.fetch_sub(1, Ordering::Relaxed);
        CompletionContinuation::More
    }
}

impl CompletionAccess<DummySlotSpec> for MockTable {
    fn record_completion(
        &self,
        _permit: CompletionWritePermit,
        _packet: CompletionPacket<DummySlotSpec>,
    ) -> RecordCompletionResult<DummySlotSpec> {
        unreachable!("the mock table is never written to")
    }

    fn try_take_record(
        &self,
        token: OpToken,
    ) -> Result<PollRecordResult<DummySlotSpec>, Report<DummyError>> {
        if self.ready.load(Ordering::Relaxed) == 0 {
            return Ok(PollRecordResult::Pending);
        }
        self.ready.fetch_sub(1, Ordering::Relaxed);
        Ok(PollRecordResult::Ready(CompletionRecord {
            event: UserCompletionEvent::from_parts(CompletionBackend::Core, token, 0, 0),
            payload: (),
            detail: None,
            cleanup: CompletionCleanupGuard::none(),
            continuation: self.next_continuation(),
        }))
    }

    fn register_waker(&self, _token: OpToken, _waker: &Waker) -> CompletionMutationOutcome {
        CompletionMutationOutcome::Applied
    }

    fn mark_waiting(&self, _token: OpToken) -> CompletionMutationOutcome {
        CompletionMutationOutcome::Applied
    }

    fn discard_ready_records(&self, _token: OpToken) -> CompletionMutationOutcome {
        CompletionMutationOutcome::Applied
    }

    fn mark_orphaned(&self, _token: OpToken) -> CompletionMutationOutcome {
        self.orphaned.fetch_add(1, Ordering::Relaxed);
        CompletionMutationOutcome::Applied
    }

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, _idx: usize) -> u8 {
        0
    }

    #[cfg(any(test, feature = "loom"))]
    fn debug_status_string(&self, _idx: usize) -> String {
        String::from("mock")
    }
}

#[derive(Default)]
struct CountingWaker {
    wakes: AtomicUsize,
}

impl RemoteWaker<DummyError> for CountingWaker {
    fn wake(&self) -> DriverResult<(), DummyError> {
        self.wakes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// 一个 multishot 形态的操作：不实现 [`SingleShotOp`]，所以它**不能** `await`，只能当流用。
struct DummyMultiOp;

impl IntoPlatformOp<DummySlotSpec> for DummyMultiOp {
    type SubmitPayload = ();
    type RecordPayload = ();
    type Output = ();
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::AcceptMulti;

    fn into_kernel_and_payload(self) -> (DummyPlatformOp, Self::SubmitPayload) {
        (DummyPlatformOp, ())
    }

    fn payload_into_erased(_payload: Self::SubmitPayload) {}

    fn try_record_from_erased(_erased: ()) -> DriverResult<Self::RecordPayload, DummyError> {
        Ok(())
    }

    fn complete(
        _payload: Self::RecordPayload,
        res: DriverResult<usize, DummyError>,
    ) -> OpCompletion<Self::Output, DummyError, Self::Completion> {
        OpCompletion::new(res, ())
    }
}

struct Harness<T = DummyOp>
where
    T: IntoPlatformOp<DummySlotSpec, Output = (), Completion = usize> + Unpin,
{
    op: Option<DetachedOp<T, DummySlotSpec>>,
    table: Arc<MockTable>,
    waker: Arc<CountingWaker>,
    cancels: mpsc::Receiver<CancelRequest>,
}

impl<T> Harness<T>
where
    T: IntoPlatformOp<DummySlotSpec, Output = (), Completion = usize> + Unpin,
{
    fn new(table: Arc<MockTable>) -> Self {
        let token = test_token();
        let waker = Arc::new(CountingWaker::default());
        let (tx, cancels) = mpsc::channel();
        let op = DetachedOp::armed(
            table.clone() as SharedCompletionTable<DummySlotSpec>,
            tx,
            waker.clone() as Arc<dyn RemoteWaker<DummyError>>,
            token,
        );
        Self {
            op: Some(op),
            table,
            waker,
            cancels,
        }
    }

    fn poll_once(&mut self) -> Poll<OpResult<(), DummyError, usize>>
    where
        T: SingleShotOp<DummySlotSpec>,
    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let op = self.op.as_mut().expect("op still alive");
        Pin::new(op).poll(&mut cx)
    }

    fn poll_next_once(&mut self) -> Poll<Option<OpResult<(), DummyError, usize>>> {
        use futures_core::Stream;

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let op = self.op.as_mut().expect("op still alive");
        Pin::new(op).poll_next(&mut cx)
    }

    fn is_armed(&self) -> bool {
        self.op.as_ref().expect("op still alive").is_armed()
    }

    fn drop_op(&mut self) {
        drop(self.op.take());
    }

    fn wakes(&self) -> usize {
        self.waker.wakes.load(Ordering::Relaxed)
    }
}

fn test_token() -> OpToken {
    OpToken::from_registry_parts(3, Generation::new(7)).expect("test token should be encodable")
}

/// 取到终态完成之后 token 就失效了（slot 已归还、generation 已推进）。此时再
/// `mark_orphaned` / `abandon` 命中的必然是 generation 校验，只会在诊断里记一条假的
/// `StaleGeneration`，并让驱动为一个结束了的操作白跑一次唤醒。
#[test]
fn a_completed_detached_op_leaves_its_slot_alone_on_drop() {
    let mut harness: Harness = Harness::new(Arc::new(MockTable::with_ready_record()));

    assert!(matches!(harness.poll_once(), Poll::Ready(_)));
    harness.drop_op();

    assert_eq!(harness.table.orphaned(), 0, "已完成的操作不该再被放弃");
    assert_eq!(harness.wakes(), 0, "没有取消请求就不该唤醒驱动");
    assert!(
        harness.cancels.try_recv().is_err(),
        "已完成的操作不该再投取消请求"
    );
}

/// 回归锚点：上面那条不能顺手把真正需要的取消路径关掉。
#[test]
fn an_unfinished_detached_op_still_orphans_and_cancels_on_drop() {
    let mut harness: Harness = Harness::new(Arc::new(MockTable::default()));

    assert!(matches!(harness.poll_once(), Poll::Pending));
    harness.drop_op();

    assert_eq!(harness.table.orphaned(), 1);
    assert_eq!(harness.wakes(), 1);
    let cancel = harness
        .cancels
        .try_recv()
        .expect("在途操作必须投出取消请求");
    assert_eq!(cancel.target, test_token());
}

/// 提交就失败的句柄从来没有过 token，drop 时同样什么都不做。
#[test]
fn a_never_submitted_detached_op_has_nothing_to_release() {
    let op: DetachedOp<DummyOp, DummySlotSpec> =
        DetachedOp::settled(OpResult::ResourceLost(OpError::payload_missing()));
    drop(op);
}

/// 同一个句柄当流用：单发操作的流只有一项，取完即结束。
///
/// 这条锚住的是收敛本身——`Future` 与 `Stream` 是同一段 `poll_item` 的两个出口，
/// 不是两条实现。
#[test]
fn a_single_shot_op_is_a_one_item_stream() {
    let mut harness: Harness = Harness::new(Arc::new(MockTable::with_ready_record()));

    assert!(matches!(harness.poll_next_once(), Poll::Ready(Some(_))));
    assert!(matches!(harness.poll_next_once(), Poll::Ready(None)));

    harness.drop_op();
    assert_eq!(harness.table.orphaned(), 0);
    assert_eq!(harness.wakes(), 0);
}

/// 同一个句柄跑 multishot：`More` 的记录一条都不终止流，`Final` 才终止。
///
/// 这是收敛的另一半——multishot 不再需要自己的句柄类型，它和单发共用 `poll_item`，
/// 区别全部来自记录携带的 `CompletionContinuation`。
#[test]
fn a_multishot_op_keeps_its_token_until_the_final_record() {
    const MORE: usize = 2;

    let mut harness: Harness<DummyMultiOp> = Harness::new(Arc::new(MockTable::streaming(MORE)));

    for i in 0..MORE {
        assert!(
            matches!(harness.poll_next_once(), Poll::Ready(Some(_))),
            "中间完成 {i} 应该产出一项"
        );
        assert!(harness.is_armed(), "`More` 之后操作仍在内核里");
    }

    assert!(matches!(harness.poll_next_once(), Poll::Ready(Some(_))));
    assert!(!harness.is_armed(), "`Final` 之后 token 必须失效");
    assert!(matches!(harness.poll_next_once(), Poll::Ready(None)));

    harness.drop_op();
    assert_eq!(harness.table.orphaned(), 0, "已终止的流不该再放弃 slot");
    assert_eq!(harness.wakes(), 0);
}

/// 中途丢弃一条仍在途的 multishot：必须放弃 slot 并请求取消，否则内核会一直投递下去。
#[test]
fn dropping_a_streaming_op_orphans_and_cancels() {
    let mut harness: Harness<DummyMultiOp> = Harness::new(Arc::new(MockTable::streaming(3)));

    assert!(matches!(harness.poll_next_once(), Poll::Ready(Some(_))));
    assert!(harness.is_armed());
    harness.drop_op();

    assert_eq!(harness.table.orphaned(), 1);
    assert_eq!(harness.wakes(), 1);
    let cancel = harness
        .cancels
        .try_recv()
        .expect("在途的 multishot 必须投出取消请求");
    assert_eq!(cancel.target, test_token());
}
