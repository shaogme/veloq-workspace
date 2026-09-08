use super::*;

use crate::{
    DriverCoreError,
    driver::{
        AnomalyAttach, AnomalyOutcome, CompletionAnomalyKind, CompletionAnomalyReason,
        CompletionBackend, CompletionBackendHooks, CompletionCleanup, CompletionCleanupGuard,
        CompletionContinuation, CompletionControl, CompletionEnvelope, CompletionFlowExt,
        CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress, CompletionSource,
        CompletionToken, HookResult, OpToken, PlatformOp, registry::OpRegistry,
    },
    slot::{
        self, CheckedSlotView, Generation, InFlightOrphaned, InFlightWaiting, SlotRegistryExt,
        SlotState, SlotView,
    },
};
use veloq_std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
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

impl crate::DriverError for DummyError {
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

fn test_token(index: usize, generation: u32) -> OpToken {
    OpToken::from_registry_parts(index, Generation::new(generation))
        .expect("test token should be encodable")
}

fn test_event(token: OpToken, res: i32) -> UserCompletionEvent {
    UserCompletionEvent::from_parts(CompletionBackend::Core, token, res, 0)
}

#[derive(Default)]
struct TestHooks {
    cleanup: Option<CompletionCleanupGuard>,
    /// 还要产出多少条 `More` 完成，用来模拟一个 multishot 操作。
    remaining_more: usize,
}

impl TestHooks {
    fn multishot(remaining_more: usize) -> Self {
        Self {
            cleanup: None,
            remaining_more,
        }
    }

    fn next_continuation(&mut self) -> CompletionContinuation {
        if self.remaining_more == 0 {
            return CompletionContinuation::Final;
        }
        self.remaining_more -= 1;
        CompletionContinuation::More
    }
}

impl CompletionBackendHooks<DummySlotSpec> for TestHooks {
    type BackendIngress = ();
    type BackendEffect = ();

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        Ok(CompletionHookOutcome::Ignore { effect: () })
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: slot::Slot<'_, InFlightWaiting, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        let continuation = self.next_continuation();
        if continuation.is_more() {
            // multishot：slot 的 op 与 payload 必须原地留给内核后续的完成，记录里携带的
            // 是**本次**完成新产出的东西（真实后端是一个 fd 或一个 FixedBuf）。
            return Ok(CompletionHookOutcome::User {
                event,
                payload: (),
                detail: None,
                cleanup: self.cleanup.take().unwrap_or_default(),
                continuation,
                effect: (),
            });
        }

        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        Ok(CompletionHookOutcome::User {
            event,
            payload: payload.expect("test slot payload should exist"),
            detail,
            cleanup: self.cleanup.take().unwrap_or_default(),
            continuation,
            effect: (),
        })
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: slot::Slot<'_, InFlightOrphaned, DummySlotSpec>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        let mut completed = slot.complete();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        let _ = payload;
        drop(detail);
        Ok(CompletionHookOutcome::Cleanup {
            cleanup: self.cleanup.take().unwrap_or_default(),
            continuation: CompletionContinuation::Final,
            effect: (),
        })
    }

    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>> {
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            effect: (),
        })
    }

    fn finish_backend_effect(
        &mut self,
        _effect: Self::BackendEffect,
    ) -> HookResult<DummySlotSpec, ()> {
        Ok(())
    }
}

fn active_registry() -> (OpRegistry<DummySlotSpec>, OpToken) {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let token = arm_slot(&mut registry);
    (registry, token)
}

fn arm_slot(registry: &mut OpRegistry<DummySlotSpec>) -> OpToken {
    let handle = registry.alloc(()).expect("slot allocation failed").handle;
    let token = test_token(handle.index, handle.generation.get());
    registry
        .with_slot_storage_mut(token, |_result, payload, _sidecar| {
            *payload = Some(());
        })
        .expect("slot storage should exist");
    let slot = match registry.checked_slot_view(token).unwrap() {
        CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot
            .init_op_with(DummyPlatformOp, |_| {})
            .expect("reserved slot should accept op"),
        _ => panic!("reserved slot should be available"),
    };
    let _in_flight = slot
        .start_submission_with(None)
        .expect("reserved slot should start submission")
        .persist();
    token
}

fn accept_with_hooks(
    registry: &mut OpRegistry<DummySlotSpec>,
    event: UserCompletionEvent,
    hooks: &mut TestHooks,
) -> CompletionFlowOutcome {
    accept_ingress(registry, CompletionIngress::User(event), hooks)
}

/// 模拟内核回传：token 先编码成裸 `u64`，再由 `classify()` 解码回 `OpToken`。
/// 只有走这条路径才会覆盖 `CompletionToken` 的编解码，`CompletionIngress::User`
/// 是直接携带 `OpToken` 的，会绕过它。
fn accept_kernel_raw(
    registry: &mut OpRegistry<DummySlotSpec>,
    token: OpToken,
    res: i32,
    hooks: &mut TestHooks,
) -> CompletionFlowOutcome {
    let envelope = CompletionEnvelope::from_raw_parts(
        CompletionBackend::Core,
        CompletionToken::user(token).raw(),
        res,
        0,
    );
    accept_ingress(registry, CompletionIngress::Kernel(envelope), hooks)
}

fn accept_ingress(
    registry: &mut OpRegistry<DummySlotSpec>,
    ingress: CompletionIngress,
    hooks: &mut TestHooks,
) -> CompletionFlowOutcome {
    let diagnostics = registry.shared.completion_diagnostics();
    let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
    registry
        .accept_completion(&table, &diagnostics, hooks, ingress)
        .expect("test completion should succeed")
}

fn accept_user(registry: &mut OpRegistry<DummySlotSpec>, token: OpToken, res: i32) {
    let mut hooks = TestHooks::default();
    let _ = accept_with_hooks(registry, test_event(token, res), &mut hooks);
}

#[test]
fn record_completion_rejects_idle_future_generation() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let table = registry.shared.clone();
    let token = test_token(0, 1);

    let mut hooks = TestHooks::default();
    let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);

    assert_eq!(outcome.anomaly, 1);
    assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
}

#[test]
fn try_take_record_reports_future_generation_unavailable() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let token = test_token(0, 1);

    match table.try_take_record(token).unwrap() {
        PollRecordResult::Unavailable { kind, .. } => {
            assert_eq!(kind.reason(), CompletionAnomalyReason::NonActiveSlot);
            assert!(matches!(
                kind,
                CompletionAnomalyKind::NonActive {
                    index: 0,
                    generation,
                    ..
                } if generation == Generation::new(1)
            ));
        }
        PollRecordResult::Pending => panic!("future generation token must not stay pending"),
        PollRecordResult::Ready(_) => panic!("future generation token must not become ready"),
    }
}

#[test]
fn mark_waiting_does_not_activate_idle_future_generation() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let token = test_token(0, 1);

    let outcome = table.mark_waiting(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_IDLE);
}

#[test]
fn mark_waiting_does_not_revive_orphaned_slot() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    table.slots[0].reset(Generation::new(1));
    table.slots[0].set_state(SlotState::InFlightOrphaned, Ordering::Release);
    let token = test_token(0, 1);

    let outcome = table.mark_waiting(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_ORPHANED);
}

#[test]
fn mark_orphaned_reports_stale_generation() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    table.slots[0].reset(Generation::new(2));
    table.slots[0].set_state(SlotState::InFlightWaiting, Ordering::Release);
    let token = test_token(0, 1);

    let outcome = table.mark_orphaned(token);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Stale(_))
    ));
    assert_eq!(table.debug_get_state(0), CELL_STATE_WAITING);
    assert_eq!(
        table.completion_diagnostics().snapshot().stale_completion,
        1
    );
}

#[test]
fn register_waker_reports_missing_slot() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    let waker = Waker::noop();
    let token = test_token(3, 1);

    let outcome = table.register_waker(token, waker);

    assert!(matches!(
        outcome,
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Missing(_))
    ));
}

#[test]
fn duplicate_completion_does_not_clear_ready_data() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    let mut hooks = TestHooks::default();
    let first = accept_with_hooks(&mut registry, test_event(token, 11), &mut hooks);
    let duplicate = accept_with_hooks(&mut registry, test_event(token, 22), &mut hooks);

    assert_eq!(first.user_completed, 1);
    assert_eq!(duplicate.anomaly, 1);
    let record = match table.try_take_record(token).unwrap() {
        PollRecordResult::Ready(record) => record,
        PollRecordResult::Pending => panic!("first completion should be ready"),
        PollRecordResult::Unavailable { kind, .. } => {
            panic!("first completion should remain available: {kind:?}")
        }
    };
    assert_eq!(record.event.res(), 11);
}

/// 一轮 alloc/complete/consume 让 slot generation 推进 2。旧的 15 位 token 布局会在
/// generation 跨过 `0x8000` 时把编码截断，完成回来后被判成 Stale 静默丢弃——slot 永远
/// 停留在 `InFlightWaiting`，其 payload 永不归还。这里把单个 slot 跑满一整圈 15 位
/// 空间，确认完成在边界两侧都能正确路由。
#[test]
fn completion_routing_survives_the_legacy_15_bit_generation_boundary() {
    const ROUNDS: u32 = 0x8000 / 2 + 16;

    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let table = registry.shared.clone();

    for round in 0..ROUNDS {
        let token = arm_slot(&mut registry);
        let res = round as i32;
        let mut hooks = TestHooks::default();
        let outcome = accept_kernel_raw(&mut registry, token, res, &mut hooks);

        assert_eq!(
            outcome.user_completed,
            1,
            "round {round}: completion for generation {:#x} was not routed to its slot",
            token.generation()
        );
        match table.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => assert_eq!(record.event.res(), res),
            PollRecordResult::Pending => {
                panic!("round {round}: recorded completion should be ready")
            }
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("round {round}: recorded completion was dropped: {kind:?}")
            }
        }
    }

    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.stale_completion, 0);
    assert!(
        table.slots[0]
            .generation(Ordering::Acquire)
            .is_newer_than(Generation::new(0x8000)),
        "the test must actually cross the 15-bit boundary"
    );
}

/// 双轨收敛后的核心不变量：完成发布之后，slot 的**生命周期**已经回到 `Idle`（driver
/// 线程已把它归还给 free list），而「信箱里有一条待消费的完成」由正交的 `ready` 标志位
/// 表示。两者同时成立，正是旧 `InFlightReady` 单一状态被拆开的地方。
#[test]
fn a_published_completion_is_orthogonal_to_the_slot_lifecycle() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    accept_user(&mut registry, token, 5);

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(
        status.state,
        SlotState::Idle,
        "the slot itself must already be released"
    );
    assert!(status.ready, "the record must stay in the mailbox");
    assert!(!status.finalizing);
    assert!(!status.is_idle(), "a ready slot is not reusable");
    assert_eq!(table.debug_get_state(token.index()), CELL_STATE_READY);
    assert!(table.has_ready_completion());

    match table.try_take_record(token).unwrap() {
        PollRecordResult::Ready(record) => assert_eq!(record.event.res(), 5),
        PollRecordResult::Pending => panic!("published completion should not be pending"),
        PollRecordResult::Unavailable { kind, .. } => {
            panic!("published completion should be takeable: {kind:?}")
        }
    }

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert!(status.is_idle(), "consumption must fully release the slot");
    assert!(!table.has_ready_completion());
}

/// `ready` 必须优先于生命周期被检查：远端 mutator 看到信箱里有记录时，不论 slot 处于
/// 哪个生命周期，都要走信箱路径而不是生命周期路径。
#[test]
fn mailbox_checks_take_priority_over_the_slot_lifecycle() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    accept_user(&mut registry, token, 1);
    assert_eq!(
        table.slots[token.index()].status(Ordering::Acquire).state,
        SlotState::Idle
    );

    // 生命周期是 `Idle`——若按 state 判定，这两个都会被拒成 NonActive。
    assert_eq!(
        table.mark_waiting(token),
        CompletionMutationOutcome::Applied
    );
    assert_eq!(
        table.register_waker(token, Waker::noop()),
        CompletionMutationOutcome::Applied
    );
    assert_eq!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Applied
    );
    assert!(!table.has_ready_completion());
}

/// 生命周期空、信箱也空时，三个 mutator 都必须拒绝——确认上一条不是把判定放宽了。
#[test]
fn an_empty_mailbox_on_an_idle_slot_still_rejects_every_mutation() {
    let table = slot::SlotTable::<DummySlotSpec>::new(1);
    table.slots[0].reset(Generation::new(1));
    let token = test_token(0, 1);

    let status = table.slots[0].status(Ordering::Acquire);
    assert!(status.is_idle());

    assert!(matches!(
        table.mark_waiting(token),
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert!(matches!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert!(matches!(
        table.register_waker(token, Waker::noop()),
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(_))
    ));
    assert!(matches!(
        table.try_take_record(token).unwrap(),
        PollRecordResult::Unavailable { .. }
    ));
}

/// 诊断里携带的是完整 [`slot::SlotStatus`]：旧实现把「已发布」编码进 state，收敛后若
/// 只记 state 就会退化成一个信息量为零的 `Idle`。
#[test]
fn anomaly_diagnostics_keep_the_mailbox_bit() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    accept_user(&mut registry, token, 0);
    // 重复完成：slot 生命周期已是 Idle，但信箱里压着记录。
    let mut hooks = TestHooks::default();
    let outcome = accept_with_hooks(&mut registry, test_event(token, 0), &mut hooks);
    assert_eq!(outcome.anomaly, 1);

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(format!("{status:?}"), "Idle+ready");
}

#[test]
fn ready_mark_orphaned_cleanup_leaves_diagnostic_stale_result() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    accept_user(&mut registry, token, 0);
    assert_eq!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Applied
    );

    assert!(matches!(
        table.try_take_record(token).unwrap(),
        PollRecordResult::Unavailable {
            kind,
            ..
        } if kind.reason() == CompletionAnomalyReason::StaleGeneration
    ));
    let snapshot = table.completion_diagnostics().snapshot();
    assert_eq!(snapshot.stale_completion, 1);
}

// ---------------------------------------------------------------------------
// multishot：一个 slot 产生多条完成
// ---------------------------------------------------------------------------

fn counting_cleanup(counter: &Arc<AtomicUsize>) -> CompletionCleanupGuard {
    let counter = counter.clone();
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        counter.fetch_add(1, Ordering::Release);
        Ok(())
    }))
}

fn take_ready(
    table: &slot::SlotTable<DummySlotSpec>,
    token: OpToken,
) -> CompletionRecord<DummySlotSpec> {
    match table.try_take_record(token).unwrap() {
        PollRecordResult::Ready(record) => record,
        PollRecordResult::Pending => panic!("a published completion must be takeable"),
        PollRecordResult::Unavailable { kind, .. } => panic!("completion unavailable: {kind:?}"),
    }
}

/// `More` 的完成不归还 slot、不推进 generation：同一个 token 必须能继续接收完成。
#[test]
fn a_multishot_slot_keeps_its_token_across_completions() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();
    let mut hooks = TestHooks::multishot(2);

    // 第一条 More。
    let outcome = accept_kernel_raw(&mut registry, token, 1, &mut hooks);
    assert_eq!(outcome.user_completed, 1);

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(status.state, SlotState::InFlightWaiting);
    assert!(status.ready, "the record must be published");
    assert!(status.streaming, "the operation is still in flight");
    assert_eq!(
        table.slots[token.index()].generation(Ordering::Acquire),
        token.generation(),
        "a `More` completion must not invalidate the token"
    );

    // 取走它：信箱空了，但 slot 仍在途，所以既不归还也不推进 generation。
    let record = take_ready(&table, token);
    assert_eq!(record.event.res(), 1);
    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(status.state, SlotState::InFlightWaiting);
    assert!(!status.ready);
    assert!(status.streaming);
    assert_eq!(
        table.slots[token.index()].generation(Ordering::Acquire),
        token.generation()
    );
    assert!(matches!(
        table.try_take_record(token).unwrap(),
        PollRecordResult::Pending
    ));

    // 第二条 More，同一个 token 照常路由。
    let outcome = accept_kernel_raw(&mut registry, token, 2, &mut hooks);
    assert_eq!(outcome.user_completed, 1);
    assert_eq!(take_ready(&table, token).event.res(), 2);

    // 第三条是 Final：这次才归还 slot 并让 token 失效。
    let outcome = accept_kernel_raw(&mut registry, token, 3, &mut hooks);
    assert_eq!(outcome.user_completed, 1);
    assert!(
        !table.slots[token.index()]
            .status(Ordering::Acquire)
            .streaming
    );
    assert_eq!(take_ready(&table, token).event.res(), 3);

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert!(status.is_idle(), "a final completion must recycle the slot");
    assert!(registry.alloc(()).is_ok());
}

/// 消费方跟不上时记录排队，且按到达顺序取出。
///
/// 这条同时是「终态判据为什么不能只看 `streaming`」的锚点：取走第一条（`More`）的那
/// 一刻 `streaming` 已经被第二条（`Final`）清成 false，若据此收尾就会推进 generation，
/// 把第二条永久锁在信箱里。
#[test]
fn queued_completions_are_taken_in_arrival_order() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();
    let mut hooks = TestHooks::multishot(1);

    let _ = accept_kernel_raw(&mut registry, token, 10, &mut hooks); // More
    let _ = accept_kernel_raw(&mut registry, token, 20, &mut hooks); // Final

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert!(status.ready);
    assert!(
        !status.streaming,
        "the final completion cleared `streaming`"
    );

    assert_eq!(take_ready(&table, token).event.res(), 10);
    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert!(
        status.ready,
        "the queued second record must keep the mailbox non-empty"
    );
    assert_eq!(
        table.slots[token.index()].generation(Ordering::Acquire),
        token.generation(),
        "taking a record with another one queued must not invalidate the token"
    );

    assert_eq!(take_ready(&table, token).event.res(), 20);
    assert!(
        table.slots[token.index()]
            .status(Ordering::Acquire)
            .is_idle()
    );
}

/// 放弃一个仍在途的 multishot：信箱清空、cleanup 全跑，但 slot 转入 `InFlightOrphaned`
/// 且 **generation 不动**——内核后续的完成还要靠这个 token 找回 slot 才跑得到
/// `orphan_cleanup`。
#[test]
fn orphaning_a_streaming_slot_keeps_it_in_flight() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();
    let cleanups = Arc::new(AtomicUsize::new(0));

    let mut hooks = TestHooks::multishot(2);
    hooks.cleanup = Some(counting_cleanup(&cleanups));
    let _ = accept_kernel_raw(&mut registry, token, 1, &mut hooks);
    hooks.cleanup = Some(counting_cleanup(&cleanups));
    let _ = accept_kernel_raw(&mut registry, token, 2, &mut hooks);
    assert_eq!(cleanups.load(Ordering::Acquire), 0);

    assert_eq!(
        table.mark_orphaned(token),
        CompletionMutationOutcome::Applied
    );

    assert_eq!(
        cleanups.load(Ordering::Acquire),
        2,
        "every queued record's cleanup must run"
    );
    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(status.state, SlotState::InFlightOrphaned);
    assert!(!status.ready);
    assert!(
        status.streaming,
        "the kernel is still going to complete this"
    );
    assert_eq!(
        table.slots[token.index()].generation(Ordering::Acquire),
        token.generation(),
        "an in-flight multishot must keep its token valid for orphan cleanup"
    );
    assert!(!table.has_ready_completion());

    // 内核的收尾完成仍然找得到这个 slot，走 orphan 路径。
    let outcome = accept_kernel_raw(&mut registry, token, 0, &mut hooks);
    assert_eq!(outcome.orphan_cleaned, 1);
    assert!(
        table.slots[token.index()]
            .status(Ordering::Acquire)
            .is_idle()
    );
}

/// 单发路径的回归锚点：`Final` 完成的行为与队列化之前逐条一致。
#[test]
fn a_final_completion_still_recycles_the_slot() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    let mut hooks = TestHooks::default();
    let outcome = accept_kernel_raw(&mut registry, token, 7, &mut hooks);
    assert_eq!(outcome.user_completed, 1);

    // 发布之后 slot 已被 finalize 归还，信箱里压着唯一一条记录。
    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert_eq!(status.state, SlotState::Idle);
    assert!(status.ready);
    assert!(!status.streaming);
    assert!(!status.is_idle());

    assert_eq!(take_ready(&table, token).event.res(), 7);

    let status = table.slots[token.index()].status(Ordering::Acquire);
    assert!(status.is_idle());
    assert_eq!(
        table.slots[token.index()].generation(Ordering::Acquire),
        token.generation().next(),
        "consuming a final record must invalidate the token"
    );
}

/// 信箱非空时，单发操作的第二条完成仍旧被拒——队列化不能顺手把这道防御放开。
#[test]
fn a_second_completion_on_a_single_shot_slot_is_still_rejected() {
    let (mut registry, token) = active_registry();
    let table = registry.shared.clone();

    let mut hooks = TestHooks::default();
    let _ = accept_kernel_raw(&mut registry, token, 1, &mut hooks);
    let outcome = accept_with_hooks(&mut registry, test_event(token, 2), &mut hooks);
    assert_eq!(outcome.anomaly, 1);

    // 信箱里仍旧只有第一条。
    assert_eq!(take_ready(&table, token).event.res(), 1);
    assert!(matches!(
        table.try_take_record(token).unwrap(),
        PollRecordResult::Unavailable { .. }
    ));
}
