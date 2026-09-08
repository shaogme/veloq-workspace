use crate::{
    DriverCoreError, DriverError, DriverResult, SlotSidecar,
    driver::{
        CompletionValue, DriverCompletionDiagnosticsBackend, OpToken, OpTokenError, PlatformOp,
        registry::OpRegistry,
    },
};
use diagweave::prelude::*;
use veloq_std::{format, marker::PhantomData, sync::atomic::Ordering};

pub trait SlotSpec {
    type Op: PlatformOp;
    type UserPayload: Send;
    type PlatformData: Default;
    type Sidecar: SlotSidecar;
    type Error: DriverError;
    type Completion: CompletionValue;
    type CompletionDiagnostics: DriverCompletionDiagnosticsBackend;
}

pub type SlotOp<Spec> = <Spec as SlotSpec>::Op;
pub type SlotPayload<Spec> = <Spec as SlotSpec>::UserPayload;
pub type SlotPlatformData<Spec> = <Spec as SlotSpec>::PlatformData;
pub type SlotSidecarData<Spec> = <Spec as SlotSpec>::Sidecar;
pub type SlotError<Spec> = <Spec as SlotSpec>::Error;
pub type SlotCompletion<Spec> = <Spec as SlotSpec>::Completion;
pub type SlotCompletionDiagnostics<Spec> = <Spec as SlotSpec>::CompletionDiagnostics;
pub type SlotCompletionData<Spec> = (
    Option<SlotPayload<Spec>>,
    Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
);

mod core;
mod generation;
mod table;

pub use core::*;
pub use generation::Generation;
pub use table::*;

pub trait SlotMarker: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

/// 编译期类型态与运行期 [`SlotState`] 的对应关系：
///
/// | 类型态 | 运行期 | 说明 |
/// |---|---|---|
/// | — | [`SlotState::Idle`] | 无 slot 可借出 |
/// | [`Reserved`] | [`SlotState::Reserved`] | 已预留，尚未提交 |
/// | [`InFlightWaiting`] | [`SlotState::InFlightWaiting`] | 已提交，等待完成 |
/// | [`InFlightOrphaned`] | [`SlotState::InFlightOrphaned`] | 用户已放弃，等待内核收尾 |
/// | [`Draining`] | 沿用进入前的运行期状态 | driver 线程独占地取走 op/payload |
///
/// [`Draining`] 刻意**不**改变运行期状态：它表达的是「driver 线程正在把这个 slot 掏
/// 空」这一独占能力，而不是一个远端可见的相位。远端可见的「完成已发布」由 cell 上的
/// `ready` 标志位表示（见 [`SlotStatus`]），与本轨完全正交。
pub struct Reserved;
pub struct InFlightWaiting;
pub struct InFlightOrphaned;

/// 排空视图：持有它意味着 driver 线程已判定该 slot 不再需要提交，可以取走 `op` 与
/// 完成数据。见 [`Reserved`] 上的对应关系表。
pub struct Draining;

impl sealed::Sealed for Reserved {}
impl sealed::Sealed for InFlightWaiting {}
impl sealed::Sealed for InFlightOrphaned {}
impl sealed::Sealed for Draining {}

impl SlotMarker for Reserved {}
impl SlotMarker for InFlightWaiting {}
impl SlotMarker for InFlightOrphaned {}
impl SlotMarker for Draining {}

pub struct Slot<'a, State: SlotMarker, Spec: SlotSpec> {
    entry: &'a SlotEntry<Spec>,
    op: &'a mut Option<SlotOp<Spec>>,
    storage: &'a mut SlotStorage<Spec>,
    platform: &'a mut SlotPlatformData<Spec>,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotMarker, Spec: SlotSpec> Slot<'a, State, Spec> {
    pub(crate) fn new_internal(
        entry: &'a SlotEntry<Spec>,
        op: &'a mut Option<SlotOp<Spec>>,
        storage: &'a mut SlotStorage<Spec>,
        platform: &'a mut SlotPlatformData<Spec>,
        index: usize,
    ) -> Self {
        Self {
            entry,
            op,
            storage,
            platform,
            index,
            _state: PhantomData,
        }
    }

    pub fn platform(&self) -> &SlotPlatformData<Spec> {
        self.platform
    }

    pub fn platform_mut(&mut self) -> &mut SlotPlatformData<Spec> {
        self.platform
    }

    pub fn snapshot(&self) -> SlotSnapshot {
        let core = self.entry.load_core_state(Ordering::Acquire);
        SlotSnapshot {
            index: self.index,
            generation: core.generation(),
            status: core.status(),
            has_op: self.op.is_some(),
            has_payload: self.storage.payload.is_some(),
        }
    }

    fn access_error(
        &self,
        action: SlotAccessAction,
        reason: SlotAccessErrorReason,
    ) -> SlotAccessError {
        SlotAccessError {
            action,
            reason,
            snapshot: self.snapshot(),
        }
    }

    pub fn with_sidecar_mut<F, X>(&mut self, f: F) -> X
    where
        F: FnOnce(&mut SlotSidecarData<Spec>) -> X,
    {
        self.storage
            .with_mut(|_result, _payload, sidecar| f(sidecar))
    }

    pub fn with_op_and_payload_mut<F, X>(&mut self, f: F) -> SlotAccessOutcome<X>
    where
        F: FnOnce(&mut SlotOp<Spec>, &mut SlotPayload<Spec>) -> X,
    {
        if self.op.is_none() {
            return Err(self.access_error(
                SlotAccessAction::OpPayloadMut,
                SlotAccessErrorReason::MissingOp,
            ));
        }
        if self.storage.payload.is_none() {
            return Err(self.access_error(
                SlotAccessAction::OpPayloadMut,
                SlotAccessErrorReason::MissingPayload,
            ));
        }
        Ok(f(
            self.op.as_mut().expect("checked Some above"),
            self.storage.payload.as_mut().expect("checked Some above"),
        ))
    }
}
impl<'a, Spec: SlotSpec> Slot<'a, Reserved, Spec> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Spec>,
        op: &'a mut Option<SlotOp<Spec>>,
        storage: &'a mut SlotStorage<Spec>,
        platform: &'a mut SlotPlatformData<Spec>,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::Reserved && op.is_none() {
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn has_op(&self) -> bool {
        self.op.is_some()
    }

    pub fn init_op_with<F>(
        self,
        op: SlotOp<Spec>,
        init_sidecar: F,
    ) -> SlotAccessOutcome<Slot<'a, Reserved, Spec>>
    where
        F: FnOnce(&mut SlotSidecarData<Spec>),
    {
        if self.op.is_some() {
            return Err(self.access_error(
                SlotAccessAction::InitOp,
                SlotAccessErrorReason::UnexpectedOp,
            ));
        }
        *self.op = Some(op);
        self.storage
            .with_mut(|_result, _payload, sidecar| init_sidecar(sidecar));

        Ok(Slot::new_internal(
            self.entry,
            self.op,
            self.storage,
            self.platform,
            self.index,
        ))
    }

    pub fn start_submission_with(
        self,
        rollback: Option<SubmissionRollback<'a, Spec>>,
    ) -> SlotAccessOutcome<SubmissionGuard<'a, Spec>> {
        if self.op.is_none() {
            return Err(self.access_error(
                SlotAccessAction::StartSubmission,
                SlotAccessErrorReason::MissingOp,
            ));
        }
        self.entry
            .set_state(SlotState::InFlightWaiting, Ordering::Release);

        Ok(SubmissionGuard {
            slot: Some(self),
            rollback,
            persisted: false,
        })
    }

    pub fn with_op_mut<F, X>(&mut self, f: F) -> SlotAccessOutcome<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        self.op_mut().map(f)
    }

    pub fn op_mut(&mut self) -> SlotAccessOutcome<&mut SlotOp<Spec>> {
        if self.op.is_none() {
            return Err(
                self.access_error(SlotAccessAction::OpMut, SlotAccessErrorReason::MissingOp)
            );
        }
        Ok(self.op.as_mut().expect("checked Some above"))
    }
}

impl<'a, Spec: SlotSpec> Slot<'a, InFlightWaiting, Spec> {
    /// 转入排空视图。**不改变运行期状态**——完成尚未发布，远端观察到的仍是
    /// `InFlightWaiting`；发布由 `CompletionAccess::record_completion` 负责。
    pub fn complete(self) -> Slot<'a, Draining, Spec> {
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn cancel(self) -> Slot<'a, InFlightOrphaned, Spec> {
        self.entry
            .set_state(SlotState::InFlightOrphaned, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn with_op_mut<F, X>(&mut self, f: F) -> SlotAccessOutcome<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        self.op_mut().map(f)
    }

    pub fn op_mut(&mut self) -> SlotAccessOutcome<&mut SlotOp<Spec>> {
        if self.op.is_none() {
            return Err(
                self.access_error(SlotAccessAction::OpMut, SlotAccessErrorReason::MissingOp)
            );
        }
        Ok(self.op.as_mut().expect("checked Some above"))
    }

    /// Access sidecar without state checks.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the slot is in a valid state for sidecar access.
    pub unsafe fn sidecar_unchecked<F, X>(&mut self, f: F) -> X
    where
        F: FnOnce(&mut SlotSidecarData<Spec>) -> X,
    {
        self.storage
            .with_mut(|_result, _payload, sidecar| f(sidecar))
    }
}

impl<'a, Spec: SlotSpec> Slot<'a, Draining, Spec> {
    pub fn take_op(&mut self) -> SlotAccessOutcome<SlotOp<Spec>> {
        self.op.take().ok_or_else(|| {
            self.access_error(SlotAccessAction::TakeOp, SlotAccessErrorReason::MissingOp)
        })
    }

    pub fn with_op_mut<F, X>(&mut self, f: F) -> SlotAccessOutcome<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        if self.op.is_none() {
            return Err(
                self.access_error(SlotAccessAction::OpMut, SlotAccessErrorReason::MissingOp)
            );
        }
        Ok(f(self.op.as_mut().expect("checked Some above")))
    }

    pub fn take_completion_data(&mut self) -> SlotCompletionData<Spec> {
        self.storage
            .with_mut(|result, payload, _sidecar| (payload.take(), result.take()))
    }

    pub fn take_completion_data_checked(&mut self) -> SlotAccessOutcome<SlotCompletionData<Spec>> {
        if self.storage.payload.is_none() {
            return Err(self.access_error(
                SlotAccessAction::TakeCompletionData,
                SlotAccessErrorReason::MissingPayload,
            ));
        }
        Ok(self.take_completion_data())
    }
}

impl<'a, Spec: SlotSpec> Slot<'a, InFlightOrphaned, Spec> {
    /// 转入排空视图，语义同 [`Slot::<InFlightWaiting, _>::complete`]。
    pub fn complete(self) -> Slot<'a, Draining, Spec> {
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn with_op_mut<F, X>(&mut self, f: F) -> SlotAccessOutcome<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        if self.op.is_none() {
            return Err(
                self.access_error(SlotAccessAction::OpMut, SlotAccessErrorReason::MissingOp)
            );
        }
        Ok(f(self.op.as_mut().expect("checked Some above")))
    }
}

type SubmissionRollback<'a, Spec> = fn(&mut Slot<'a, Reserved, Spec>);

pub struct SubmissionGuard<'a, Spec: SlotSpec> {
    pub slot: Option<Slot<'a, Reserved, Spec>>,
    rollback: Option<SubmissionRollback<'a, Spec>>,
    persisted: bool,
}

impl<'a, Spec: SlotSpec> SubmissionGuard<'a, Spec> {
    pub fn persist(mut self) -> Slot<'a, InFlightWaiting, Spec> {
        self.persisted = true;
        let slot = self
            .slot
            .take()
            .expect("submission guard slot missing in persist");
        Slot::new_internal(slot.entry, slot.op, slot.storage, slot.platform, slot.index)
    }
}

impl<'a, Spec: SlotSpec> Drop for SubmissionGuard<'a, Spec> {
    fn drop(&mut self) {
        if !self.persisted
            && let Some(slot) = self.slot.as_mut()
        {
            if let Some(rollback) = self.rollback {
                rollback(slot);
            }
            slot.entry.set_state(SlotState::Reserved, Ordering::Release);
        }
    }
}

pub enum SlotView<'a, Spec: SlotSpec> {
    Reserved(Slot<'a, Reserved, Spec>),
    InFlightWaiting(Slot<'a, InFlightWaiting, Spec>),
    InFlightOrphaned(Slot<'a, InFlightOrphaned, Spec>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotSnapshot {
    pub index: usize,
    pub generation: Generation,
    pub status: SlotStatus,
    pub has_op: bool,
    pub has_payload: bool,
}

impl SlotSnapshot {
    pub const fn try_token(self) -> Result<OpToken, OpTokenError> {
        OpToken::from_registry_parts(self.index, self.generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAccessAction {
    InitOp,
    StartSubmission,
    OpMut,
    OpPayloadMut,
    TakeOp,
    TakeCompletionData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAccessErrorReason {
    MissingOp,
    MissingPayload,
    UnexpectedOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotAccessError {
    pub action: SlotAccessAction,
    pub reason: SlotAccessErrorReason,
    pub snapshot: SlotSnapshot,
}

pub type SlotAccessOutcome<T> = Result<T, SlotAccessError>;

pub enum CheckedSlotView<'a, Spec: SlotSpec> {
    Valid(SlotView<'a, Spec>),
    Missing {
        index: usize,
        expected_generation: Generation,
    },
    Empty(SlotSnapshot),
    Stale(SlotSnapshot),
}

pub trait SlotRegistryExt<Spec: SlotSpec> {
    fn checked_slot_view(
        &mut self,
        token: OpToken,
    ) -> Result<CheckedSlotView<'_, Spec>, Report<Spec::Error>>;
}

impl<Spec: SlotSpec> SlotRegistryExt<Spec> for OpRegistry<Spec> {
    fn checked_slot_view(
        &mut self,
        token: OpToken,
    ) -> Result<CheckedSlotView<'_, Spec>, Report<Spec::Error>> {
        let (index, expected_generation) = token.parts();
        let Some((entry, op_entry, op, storage)) = self.slot_bundle_by_index_mut(index) else {
            return Ok(CheckedSlotView::Missing {
                index,
                expected_generation,
            });
        };
        let core = entry.load_core_state(Ordering::Acquire);
        let generation = core.generation();
        let status = core.status();
        let snapshot = SlotSnapshot {
            index,
            generation,
            status,
            has_op: op.is_some(),
            has_payload: storage.payload.is_some(),
        };

        if generation != expected_generation {
            return Ok(CheckedSlotView::Stale(snapshot));
        }

        // 只 match 一次：四个生命周期状态各自恰好对应一种视图。`ready` / `finalizing`
        // 都是 cell 信箱的标志位，不参与借出判定——信箱里有没有记录不影响 slot 本身
        // 能否被借出，只影响它能否被重新分配（见 `SlotStatus::is_idle`）。
        let platform = &mut op_entry.platform_data;
        match status.state {
            SlotState::Idle => Ok(CheckedSlotView::Empty(snapshot)),
            SlotState::Reserved => {
                Slot::<Reserved, Spec>::try_bind(entry, op, storage, platform, index).map_or_else(
                    || Err(corrupt_slot(snapshot, "try_bind Reserved failed")),
                    |slot| Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))),
                )
            }
            SlotState::InFlightWaiting => {
                ensure_in_flight_payload::<Spec>(snapshot)?;
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(
                    Slot::new_internal(entry, op, storage, platform, index),
                )))
            }
            SlotState::InFlightOrphaned => {
                ensure_in_flight_payload::<Spec>(snapshot)?;
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(
                    Slot::new_internal(entry, op, storage, platform, index),
                )))
            }
        }
    }
}

#[inline]
fn corrupt_slot<E: DriverError>(snapshot: SlotSnapshot, note: &str) -> Report<E> {
    E::from_core_report(
        DriverCoreError::Internal
            .to_report()
            .push_ctx("scope", "checked_slot_view")
            .attach_note(format!("corrupt slot detected ({note}): {snapshot:?}")),
    )
}

/// 在途的 slot 必须同时持有 op 与 payload——完成式 I/O 下内核仍持有指向 payload 的
/// 指针，缺任何一半都说明有人在在途期间把 slot 掏空了。
#[inline]
fn ensure_in_flight_payload<Spec: SlotSpec>(
    snapshot: SlotSnapshot,
) -> Result<(), Report<Spec::Error>> {
    if snapshot.has_op && snapshot.has_payload {
        Ok(())
    } else {
        Err(corrupt_slot(snapshot, "in-flight slot lost op or payload"))
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;
    use crate::driver::{
        CompletionAccess, CompletionBackend, CompletionBackendHooks, CompletionCleanupGuard,
        CompletionContinuation, CompletionControl, CompletionFlowExt, CompletionHookOutcome,
        CompletionIngress, CompletionSource, CompletionToken, HookResult, PlatformOp,
        PollRecordResult, SharedCompletionTable, UserCompletionEvent,
    };
    use crate::{DriverCoreError, DriverError};

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {
        type CleanupContext<'a> = ();
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    struct DummyError;

    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "dummy error")
        }
    }

    impl std::error::Error for DummyError {}

    impl DriverError for DummyError {
        #[inline]
        fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
            report.map_err(|_| DummyError)
        }
    }

    struct DummySlotSpec;

    impl SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = DummyError;
        type Completion = usize;
        type CompletionDiagnostics = ();
    }

    struct TestHooks;

    impl CompletionBackendHooks<DummySlotSpec> for TestHooks {
        type BackendIngress = ();
        type BackendEffect = ();

        fn handle_control(
            &mut self,
            _control: CompletionControl,
        ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>>
        {
            Ok(CompletionHookOutcome::Ignore { effect: () })
        }

        fn complete_waiting(
            &mut self,
            event: UserCompletionEvent,
            slot: Slot<'_, InFlightWaiting, DummySlotSpec>,
            _source: CompletionSource<'_, Self::BackendIngress>,
        ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>>
        {
            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            Ok(CompletionHookOutcome::User {
                event,
                payload: payload.expect("test slot payload should exist"),
                detail,
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::Final,
                effect: (),
            })
        }

        fn complete_orphaned(
            &mut self,
            _event: UserCompletionEvent,
            slot: Slot<'_, InFlightOrphaned, DummySlotSpec>,
            _source: CompletionSource<'_, Self::BackendIngress>,
        ) -> HookResult<DummySlotSpec, CompletionHookOutcome<DummySlotSpec, Self::BackendEffect>>
        {
            let mut completed = slot.complete();
            let _ = completed.take_op();
            let (payload, detail) = completed.take_completion_data();
            let _ = payload;
            drop(detail);
            Ok(CompletionHookOutcome::Cleanup {
                cleanup: CompletionCleanupGuard::default(),
                continuation: CompletionContinuation::Final,
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

    #[test]
    fn ready_slots_are_owned_by_completion_table_not_slot_view() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test handle should be encodable");
        let completion_token = CompletionToken::user(token);

        {
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
        }

        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
        let mut hooks = TestHooks;
        let _ = registry.accept_completion(
            &table,
            &diagnostics,
            &mut hooks,
            CompletionIngress::User(UserCompletionEvent::from_parts(
                CompletionBackend::Core,
                token,
                0,
                0,
            )),
        );

        assert!(matches!(
            registry.checked_slot_view(token).unwrap(),
            CheckedSlotView::Empty(_)
        ));
        let record = match registry.shared.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => record,
            PollRecordResult::Pending => panic!("completion should be ready"),
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion should be available: {kind:?}")
            }
        };
        assert_eq!(record.event.completion_token(), completion_token);
        assert_eq!(record.payload, ());
    }

    #[test]
    fn checked_slot_view_reports_stale_generation() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let stale_token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test handle should be encodable");
        let _ = registry.remove(stale_token);
        let fresh = registry
            .alloc(())
            .expect("slot should be reusable after removal")
            .handle;
        assert_eq!(fresh.index, stale_token.index());
        assert_ne!(fresh.generation, stale_token.generation());

        assert!(matches!(
            registry.checked_slot_view(stale_token).unwrap(),
            CheckedSlotView::Stale(snapshot)
                if snapshot.index == stale_token.index()
                    && snapshot.generation == fresh.generation
        ));
    }

    #[test]
    fn checked_slot_view_reports_idle_as_empty() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test handle should be encodable");

        let _ = registry.remove(token);

        assert!(matches!(
            registry.checked_slot_view(token).unwrap(),
            CheckedSlotView::Empty(snapshot)
                if snapshot.index == token.index()
                    && snapshot.generation == token.generation()
                    && snapshot.status == SlotStatus::of(SlotState::Idle)
        ));
    }

    /// slot 被 `free` 归还后信箱里仍压着一条已发布的完成：生命周期回到 `Idle`，但
    /// `ready` 必须原样保留，否则 detached future 再来 poll 就取不到东西了。
    #[test]
    fn freeing_a_slot_keeps_its_published_completion_in_the_mailbox() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test handle should be encodable");

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

        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<DummySlotSpec> = registry.shared.clone();
        let mut hooks = TestHooks;
        let _ = registry.accept_completion(
            &table,
            &diagnostics,
            &mut hooks,
            CompletionIngress::User(UserCompletionEvent::from_parts(
                CompletionBackend::Core,
                token,
                0,
                0,
            )),
        );

        // `accept_completion` 内部走完 record + finalize，slot 已被归还。
        let status = registry.shared.slots[token.index()].status(Ordering::Acquire);
        assert_eq!(status.state, SlotState::Idle);
        assert!(status.ready, "published completion must survive `free`");
        assert!(!status.is_idle(), "a ready slot must not be reallocated");

        // 容量为 1 的 registry 在完成被消费前拿不到新 slot。
        assert!(registry.alloc(()).is_err());

        let _ = registry.shared.try_take_record(token);
        let status = registry.shared.slots[token.index()].status(Ordering::Acquire);
        assert!(status.is_idle(), "consuming the record must free the slot");
        assert!(registry.alloc(()).is_ok());
    }

    #[test]
    fn checked_slot_view_reports_missing_inflight_payload_as_corrupt() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test handle should be encodable");

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

        assert!(registry.checked_slot_view(token).is_err());
    }

    #[test]
    fn slot_snapshot_try_token_uses_checked_user_index() {
        let snapshot = SlotSnapshot {
            index: 0,
            generation: Generation::new(3),
            status: SlotStatus::of(SlotState::InFlightWaiting),
            has_op: true,
            has_payload: true,
        };

        let token = snapshot
            .try_token()
            .expect("ordinary slot snapshot should encode");

        assert_eq!(token.index(), 0);
        assert_eq!(token.generation(), Generation::new(3));
    }
}
