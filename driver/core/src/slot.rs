use crate::DriverResult;
use crate::SlotSidecar;
use crate::driver::registry::OpRegistry;
use crate::driver::{OpToken, PlatformOp};
use std::marker::PhantomData;
use veloq_shim::atomic::Ordering;

pub trait SlotSpec {
    type Op: PlatformOp;
    type UserPayload: Send;
    type PlatformData: Default;
    type Sidecar: SlotSidecar;
    type Error;
    type Completion;
}

pub type SlotOp<Spec> = <Spec as SlotSpec>::Op;
pub type SlotPayload<Spec> = <Spec as SlotSpec>::UserPayload;
pub type SlotPlatformData<Spec> = <Spec as SlotSpec>::PlatformData;
pub type SlotSidecarData<Spec> = <Spec as SlotSpec>::Sidecar;
pub type SlotError<Spec> = <Spec as SlotSpec>::Error;
pub type SlotCompletion<Spec> = <Spec as SlotSpec>::Completion;
pub type SlotCompletionData<Spec> = (
    Option<SlotPayload<Spec>>,
    Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
);

mod core;
mod table;

pub use core::*;
pub use table::*;

pub trait SlotMarker: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

pub struct Reserved;
pub struct InFlightWaiting;
pub struct InFlightOrphaned;
pub struct Completed;

impl sealed::Sealed for Reserved {}
impl sealed::Sealed for InFlightWaiting {}
impl sealed::Sealed for InFlightOrphaned {}
impl sealed::Sealed for Completed {}

impl SlotMarker for Reserved {}
impl SlotMarker for InFlightWaiting {}
impl SlotMarker for InFlightOrphaned {}
impl SlotMarker for Completed {}

pub struct Slot<'a, State: SlotMarker, Spec: SlotSpec> {
    pub entry: &'a SlotEntry<Spec>,
    pub op: &'a mut Option<SlotOp<Spec>>,
    pub storage: &'a mut SlotStorage<Spec>,
    platform: &'a mut SlotPlatformData<Spec>,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotMarker, Spec: SlotSpec> Slot<'a, State, Spec> {
    #[inline]
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

    #[inline]
    pub fn platform(&self) -> &SlotPlatformData<Spec> {
        self.platform
    }

    #[inline]
    pub fn platform_mut(&mut self) -> &mut SlotPlatformData<Spec> {
        self.platform
    }
}

#[inline]
pub fn is_runnable_state(state: SlotState) -> bool {
    matches!(
        state,
        SlotState::Reserved | SlotState::InFlightWaiting | SlotState::InFlightOrphaned
    )
}

impl<'a, Spec: SlotSpec> Slot<'a, Reserved, Spec> {
    #[inline]
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

    #[inline]
    pub fn has_op(&self) -> bool {
        self.op.is_some()
    }

    pub fn init_op_with<F>(self, op: SlotOp<Spec>, init_sidecar: F) -> Slot<'a, Reserved, Spec>
    where
        F: FnOnce(&mut SlotSidecarData<Spec>),
    {
        assert!(
            self.op.is_none(),
            "slot {} entering Reserved state must not already contain an op",
            self.index
        );
        *self.op = Some(op);
        self.storage
            .with_mut(|_result, _payload, sidecar| init_sidecar(sidecar));

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn start_submission_with(
        self,
        rollback: Option<SubmissionRollback<'a, Spec>>,
    ) -> SubmissionGuard<'a, Spec> {
        assert!(
            self.op.is_some(),
            "slot {} in Reserved state must contain an op",
            self.index
        );
        self.entry
            .set_state(SlotState::InFlightWaiting, Ordering::Release);

        SubmissionGuard {
            slot: Some(self),
            rollback,
            persisted: false,
        }
    }

    #[inline]
    pub fn with_op_mut<F, X>(&mut self, f: F) -> Option<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in Reserved state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut SlotOp<Spec> {
        self.op
            .as_mut()
            .expect("slot in Reserved state must contain an op")
    }
}

impl<'a, Spec: SlotSpec> Slot<'a, InFlightWaiting, Spec> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Spec>,
        op: &'a mut Option<SlotOp<Spec>>,
        storage: &'a mut SlotStorage<Spec>,
        platform: &'a mut SlotPlatformData<Spec>,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::InFlightWaiting && op.is_some() {
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn complete(self) -> Slot<'a, Completed, Spec> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn cancel(self) -> Slot<'a, InFlightOrphaned, Spec> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.entry
            .set_state(SlotState::InFlightOrphaned, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub fn with_op_mut<F, X>(&mut self, f: F) -> Option<X>
    where
        F: FnOnce(&mut SlotOp<Spec>) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut SlotOp<Spec> {
        self.op
            .as_mut()
            .expect("slot in InFlight state must contain an op")
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

impl<'a, Spec: SlotSpec> Slot<'a, Completed, Spec> {
    pub fn reset(self) -> Slot<'a, Reserved, Spec> {
        let _ = self.op.take();
        let generation = self.entry.generation(Ordering::Acquire);
        self.storage.reset();
        self.entry.reset(generation);
        self.entry.set_state(SlotState::Reserved, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub fn take_op(&mut self) -> Option<SlotOp<Spec>> {
        assert!(
            self.op.is_some(),
            "slot {} in Completed state must contain an op",
            self.index
        );
        self.op.take()
    }

    pub fn take_completion_data(&mut self) -> SlotCompletionData<Spec> {
        self.storage
            .with_mut(|result, payload, _sidecar| (payload.take(), result.take()))
    }
}

impl<'a, Spec: SlotSpec> Slot<'a, InFlightOrphaned, Spec> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Spec>,
        op: &'a mut Option<SlotOp<Spec>>,
        storage: &'a mut SlotStorage<Spec>,
        platform: &'a mut SlotPlatformData<Spec>,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::InFlightOrphaned && op.is_some() {
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn complete(self) -> Slot<'a, Completed, Spec> {
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
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
    pub generation: u32,
    pub state: SlotState,
    pub has_op: bool,
    pub has_payload: bool,
}

pub enum CheckedSlotView<'a, Spec: SlotSpec> {
    Valid(SlotView<'a, Spec>),
    Missing {
        index: usize,
        expected_generation: u32,
    },
    Empty(SlotSnapshot),
    Stale(SlotSnapshot),
    Corrupt(SlotSnapshot),
}

pub trait SlotRegistryExt<Spec: SlotSpec> {
    fn unchecked_slot_view(&mut self, index: usize) -> Option<SlotView<'_, Spec>>;
    fn checked_slot_view(&mut self, token: OpToken) -> CheckedSlotView<'_, Spec>;
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Spec>;
}

impl<Spec: SlotSpec> SlotRegistryExt<Spec> for OpRegistry<Spec> {
    #[inline]
    fn unchecked_slot_view(&mut self, index: usize) -> Option<SlotView<'_, Spec>> {
        let (entry, op_entry, op, storage) = self.get_slot_entry_op_storage_and_entry_mut(index)?;
        match entry.state(Ordering::Acquire) {
            SlotState::Reserved => Slot::<Reserved, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Reserved),
            SlotState::InFlightWaiting => Slot::<InFlightWaiting, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlightWaiting),
            SlotState::InFlightOrphaned => Slot::<InFlightOrphaned, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlightOrphaned),
            SlotState::Idle
            | SlotState::InFlightReady
            | SlotState::Finalizing
            | SlotState::ReservedValue => None,
        }
    }

    #[inline]
    fn checked_slot_view(&mut self, token: OpToken) -> CheckedSlotView<'_, Spec> {
        let (index, expected_generation) = token.parts();
        let Some((entry, op_entry, op, storage)) =
            self.get_slot_entry_op_storage_and_entry_mut(index)
        else {
            return CheckedSlotView::Missing {
                index,
                expected_generation,
            };
        };
        let generation = entry.generation(Ordering::Acquire);
        let state = entry.state(Ordering::Acquire);
        let snapshot = SlotSnapshot {
            index,
            generation,
            state,
            has_op: op.is_some(),
            has_payload: storage.payload.is_some(),
        };

        if generation != expected_generation {
            return CheckedSlotView::Stale(snapshot);
        }

        match state {
            SlotState::InFlightWaiting | SlotState::InFlightOrphaned => {
                if !snapshot.has_op || !snapshot.has_payload {
                    return CheckedSlotView::Corrupt(snapshot);
                }
            }
            SlotState::Idle
            | SlotState::InFlightReady
            | SlotState::Finalizing
            | SlotState::ReservedValue => return CheckedSlotView::Empty(snapshot),
            SlotState::Reserved => {
                if !snapshot.has_payload {
                    return CheckedSlotView::Corrupt(snapshot);
                }
            }
        }

        match state {
            SlotState::Reserved => Slot::<Reserved, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map_or(CheckedSlotView::Corrupt(snapshot), |slot| {
                CheckedSlotView::Valid(SlotView::Reserved(slot))
            }),
            SlotState::InFlightWaiting => Slot::<InFlightWaiting, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map_or(CheckedSlotView::Corrupt(snapshot), |slot| {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))
            }),
            SlotState::InFlightOrphaned => Slot::<InFlightOrphaned, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map_or(CheckedSlotView::Corrupt(snapshot), |slot| {
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))
            }),
            SlotState::Idle
            | SlotState::InFlightReady
            | SlotState::Finalizing
            | SlotState::ReservedValue => CheckedSlotView::Empty(snapshot),
        }
    }

    #[inline]
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Spec> {
        let (entry, op_entry, op, storage) = self
            .get_slot_entry_op_storage_and_entry_mut(index)
            .expect("slot missing in registry during reserve");
        entry.set_state(SlotState::Reserved, Ordering::Release);
        Slot::new_internal(entry, op, storage, &mut op_entry.platform_data, index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{CompletionAccess, CompletionEvent, CompletionToken, PlatformOp};

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {}

    struct DummySlotSpec;

    impl SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = ();
        type Completion = usize;
    }

    #[test]
    fn ready_slots_are_owned_by_completion_table_not_slot_view() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(1);
        let handle = registry.alloc(()).expect("slot allocation failed").handle;
        let token = OpToken::new(handle.index, handle.generation);
        let completion_token = CompletionToken::user(token);

        {
            let slot = registry
                .slot_reserve(handle.index)
                .init_op_with(DummyPlatformOp, |_| {});
            let _in_flight = slot.start_submission_with(None).persist();
        }

        registry.shared.record_completion_with_data(
            CompletionEvent {
                token: completion_token,
                res: 0,
                flags: 0,
            },
            Some(()),
            None,
        );

        assert!(registry.unchecked_slot_view(handle.index).is_none());
        let record = match registry.shared.try_take_record(completion_token) {
            crate::driver::PollRecordResult::Ready(record) => record,
            crate::driver::PollRecordResult::Pending => panic!("completion should be ready"),
            crate::driver::PollRecordResult::Stale(anomaly) => {
                panic!("completion should not be stale: {anomaly:?}")
            }
            crate::driver::PollRecordResult::Lost(anomaly) => {
                panic!("completion should not be lost: {anomaly:?}")
            }
        };
        assert_eq!(record.event.token, completion_token);
        assert_eq!(record.payload, Some(()));
    }
}
