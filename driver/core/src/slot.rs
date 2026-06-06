use crate::DriverResult;
use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::driver::registry::{
    OpRegistry, RegistryCompletion, RegistryError, RegistryOp, RegistryPayload,
    RegistryPlatformData, RegistrySidecar, SlotEntryOf, SlotRegistrySpec, SlotStorageOf,
};
use std::marker::PhantomData;
use veloq_shim::atomic::Ordering;

pub trait SlotSpec {
    type Op: PlatformOp;
    type UserPayload: Send;
    type Sidecar: SlotSidecar;
    type Error;
    type Completion;
}

pub type SlotOp<Spec> = <Spec as SlotSpec>::Op;
pub type SlotPayload<Spec> = <Spec as SlotSpec>::UserPayload;
pub type SlotSidecarData<Spec> = <Spec as SlotSpec>::Sidecar;
pub type SlotError<Spec> = <Spec as SlotSpec>::Error;
pub type SlotCompletion<Spec> = <Spec as SlotSpec>::Completion;

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

pub struct Slot<'a, State: SlotMarker, Spec: SlotRegistrySpec> {
    pub entry: &'a SlotEntryOf<Spec>,
    pub op: &'a mut Option<RegistryOp<Spec>>,
    pub storage: &'a mut SlotStorageOf<Spec>,
    platform: &'a mut RegistryPlatformData<Spec>,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotMarker, Spec: SlotRegistrySpec> Slot<'a, State, Spec> {
    #[inline]
    pub(crate) fn new_internal(
        entry: &'a SlotEntryOf<Spec>,
        op: &'a mut Option<RegistryOp<Spec>>,
        storage: &'a mut SlotStorageOf<Spec>,
        platform: &'a mut RegistryPlatformData<Spec>,
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
    pub fn platform_mut(&mut self) -> &mut RegistryPlatformData<Spec> {
        self.platform
    }
}

#[inline]
pub fn is_runnable_state(state: SlotState) -> bool {
    matches!(state, SlotState::Reserved | SlotState::InFlightWaiting)
}

impl<'a, Spec: SlotRegistrySpec> Slot<'a, Reserved, Spec> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntryOf<Spec>,
        op: &'a mut Option<RegistryOp<Spec>>,
        storage: &'a mut SlotStorageOf<Spec>,
        platform: &'a mut RegistryPlatformData<Spec>,
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

    pub fn init_op_with<F>(self, op: RegistryOp<Spec>, init_sidecar: F) -> Slot<'a, Reserved, Spec>
    where
        F: FnOnce(&mut RegistrySidecar<Spec>),
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
        F: FnOnce(&mut RegistryOp<Spec>) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in Reserved state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut RegistryOp<Spec> {
        self.op
            .as_mut()
            .expect("slot in Reserved state must contain an op")
    }
}

impl<'a, Spec: SlotRegistrySpec> Slot<'a, InFlightWaiting, Spec> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntryOf<Spec>,
        op: &'a mut Option<RegistryOp<Spec>>,
        storage: &'a mut SlotStorageOf<Spec>,
        platform: &'a mut RegistryPlatformData<Spec>,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::InFlightWaiting {
            assert!(
                op.is_some(),
                "slot {index} in InFlight state must contain an op"
            );
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
        F: FnOnce(&mut RegistryOp<Spec>) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut RegistryOp<Spec> {
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
        F: FnOnce(&mut RegistrySidecar<Spec>) -> X,
    {
        self.storage
            .with_mut(|_result, _payload, sidecar| f(sidecar))
    }
}

impl<'a, Spec: SlotRegistrySpec> Slot<'a, Completed, Spec> {
    pub fn reset(self) -> Slot<'a, Reserved, Spec> {
        let _ = self.op.take();
        let generation = self.entry.generation(Ordering::Acquire);
        self.storage.reset();
        self.entry.reset(generation);
        self.entry.set_state(SlotState::Reserved, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub fn take_op(&mut self) -> Option<RegistryOp<Spec>> {
        assert!(
            self.op.is_some(),
            "slot {} in Completed state must contain an op",
            self.index
        );
        self.op.take()
    }

    pub fn take_completion_data(
        &mut self,
    ) -> (
        Option<RegistryPayload<Spec>>,
        Option<DriverResult<RegistryCompletion<Spec>, RegistryError<Spec>>>,
    ) {
        self.storage
            .with_mut(|result, payload, _sidecar| (payload.take(), result.take()))
    }
}

impl<'a, Spec: SlotRegistrySpec> Slot<'a, InFlightOrphaned, Spec> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntryOf<Spec>,
        op: &'a mut Option<RegistryOp<Spec>>,
        storage: &'a mut SlotStorageOf<Spec>,
        platform: &'a mut RegistryPlatformData<Spec>,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::InFlightOrphaned {
            assert!(
                op.is_some(),
                "slot {index} in Cancelled state must contain an op"
            );
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

pub struct SubmissionGuard<'a, Spec: SlotRegistrySpec> {
    pub slot: Option<Slot<'a, Reserved, Spec>>,
    rollback: Option<SubmissionRollback<'a, Spec>>,
    persisted: bool,
}

impl<'a, Spec: SlotRegistrySpec> SubmissionGuard<'a, Spec> {
    pub fn persist(mut self) -> Slot<'a, InFlightWaiting, Spec> {
        self.persisted = true;
        let slot = self
            .slot
            .take()
            .expect("submission guard slot missing in persist");
        Slot::new_internal(slot.entry, slot.op, slot.storage, slot.platform, slot.index)
    }
}

impl<'a, Spec: SlotRegistrySpec> Drop for SubmissionGuard<'a, Spec> {
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

pub enum SlotView<'a, Spec: SlotRegistrySpec> {
    Reserved(Slot<'a, Reserved, Spec>),
    InFlightWaiting(Slot<'a, InFlightWaiting, Spec>),
    InFlightOrphaned(Slot<'a, InFlightOrphaned, Spec>),
}

pub trait SlotRegistryExt<Spec: SlotRegistrySpec> {
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Spec>>;
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Spec>;
}

impl<Spec: SlotRegistrySpec> SlotRegistryExt<Spec> for OpRegistry<Spec> {
    #[inline]
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Spec>> {
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
            SlotState::InFlightWaiting | SlotState::InFlightReady => {
                Slot::<InFlightWaiting, Spec>::try_bind(
                    entry,
                    op,
                    storage,
                    &mut op_entry.platform_data,
                    index,
                )
                .map(SlotView::InFlightWaiting)
            }
            SlotState::InFlightOrphaned => Slot::<InFlightOrphaned, Spec>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlightOrphaned),
            SlotState::Idle | SlotState::Finalizing | SlotState::ReservedValue => None,
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
