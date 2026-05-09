use crate::DriverResult;
use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::driver::registry::OpRegistry;
use std::marker::PhantomData;
use veloq_shim::atomic::Ordering;

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

pub struct Slot<'a, State: SlotMarker, Op: PlatformOp, P, S: SlotSidecar, R = usize> {
    pub entry: &'a SlotEntry<Op, S, R>,
    pub op: &'a mut Option<Op>,
    pub storage: &'a mut SlotStorage<Op, S, R>,
    platform: &'a mut P,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotMarker, Op: PlatformOp, P, S: SlotSidecar, R> Slot<'a, State, Op, P, S, R> {
    #[inline]
    pub(crate) fn new_internal(
        entry: &'a SlotEntry<Op, S, R>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S, R>,
        platform: &'a mut P,
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
    pub fn platform_mut(&mut self) -> &mut P {
        self.platform
    }
}

#[inline]
pub fn is_runnable_state(state: SlotState) -> bool {
    matches!(state, SlotState::Reserved | SlotState::InFlightWaiting)
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> Slot<'a, Reserved, Op, P, S, R> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Op, S, R>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S, R>,
        platform: &'a mut P,
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

    pub fn init_op_with<F>(self, op: Op, init_sidecar: F) -> Slot<'a, Reserved, Op, P, S, R>
    where
        F: FnOnce(&mut S),
    {
        assert!(
            self.op.is_none(),
            "slot {} entering Reserved state must not already contain an op",
            self.index
        );
        *self.op = Some(op);
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| init_sidecar(sidecar));

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn start_submission_with(
        self,
        rollback: Option<SubmissionRollback<'a, Op, P, S, R>>,
    ) -> SubmissionGuard<'a, Op, P, S, R> {
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
        F: FnOnce(&mut Op) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in Reserved state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut Op {
        self.op
            .as_mut()
            .expect("slot in Reserved state must contain an op")
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> Slot<'a, InFlightWaiting, Op, P, S, R> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Op, S, R>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S, R>,
        platform: &'a mut P,
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

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S, R> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn cancel(self) -> Slot<'a, InFlightOrphaned, Op, P, S, R> {
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
        F: FnOnce(&mut Op) -> X,
    {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut Op {
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
        F: FnOnce(&mut S) -> X,
    {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| f(sidecar))
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> Slot<'a, Completed, Op, P, S, R> {
    pub fn reset(self) -> Slot<'a, Reserved, Op, P, S, R> {
        let _ = self.op.take();
        let generation = self.entry.generation(Ordering::Acquire);
        self.storage.reset();
        self.entry.reset(generation);
        self.entry.set_state(SlotState::Reserved, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub fn take_op(&mut self) -> Option<Op> {
        assert!(
            self.op.is_some(),
            "slot {} in Completed state must contain an op",
            self.index
        );
        self.op.take()
    }

    pub fn take_completion_data(&mut self) -> (Option<ErasedPayload>, Option<DriverResult<R>>) {
        self.storage
            .with_mut(|_op, result, payload, _sidecar| (payload.take(), result.take()))
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> Slot<'a, InFlightOrphaned, Op, P, S, R> {
    #[inline]
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<Op, S, R>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S, R>,
        platform: &'a mut P,
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

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S, R> {
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

type SubmissionRollback<'a, Op, P, S, R> = fn(&mut Slot<'a, Reserved, Op, P, S, R>);

pub struct SubmissionGuard<'a, Op: PlatformOp, P, S: SlotSidecar, R = usize> {
    pub slot: Option<Slot<'a, Reserved, Op, P, S, R>>,
    rollback: Option<SubmissionRollback<'a, Op, P, S, R>>,
    persisted: bool,
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> SubmissionGuard<'a, Op, P, S, R> {
    pub fn persist(mut self) -> Slot<'a, InFlightWaiting, Op, P, S, R> {
        self.persisted = true;
        let slot = self
            .slot
            .take()
            .expect("submission guard slot missing in persist");
        Slot::new_internal(slot.entry, slot.op, slot.storage, slot.platform, slot.index)
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar, R> Drop for SubmissionGuard<'a, Op, P, S, R> {
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

pub enum SlotView<'a, Op: PlatformOp, P, S: SlotSidecar, R = usize> {
    Reserved(Slot<'a, Reserved, Op, P, S, R>),
    InFlightWaiting(Slot<'a, InFlightWaiting, Op, P, S, R>),
    InFlightOrphaned(Slot<'a, InFlightOrphaned, Op, P, S, R>),
}

pub trait SlotRegistryExt<Op: PlatformOp, P, S: SlotSidecar, R = usize> {
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S, R>>;
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Op, P, S, R>;
}

impl<Op: PlatformOp, P: Default, S: SlotSidecar, R> SlotRegistryExt<Op, P, S, R>
    for OpRegistry<Op, P, S, R>
{
    #[inline]
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S, R>> {
        let (entry, op_entry, op, storage) = self.get_slot_entry_op_storage_and_entry_mut(index)?;
        match entry.state(Ordering::Acquire) {
            SlotState::Reserved => Slot::<Reserved, Op, P, S, R>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Reserved),
            SlotState::InFlightWaiting | SlotState::InFlightReady => {
                Slot::<InFlightWaiting, Op, P, S, R>::try_bind(
                    entry,
                    op,
                    storage,
                    &mut op_entry.platform_data,
                    index,
                )
                .map(SlotView::InFlightWaiting)
            }
            SlotState::InFlightOrphaned => Slot::<InFlightOrphaned, Op, P, S, R>::try_bind(
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
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Op, P, S, R> {
        let (entry, op_entry, op, storage) = self
            .get_slot_entry_op_storage_and_entry_mut(index)
            .expect("slot missing in registry during reserve");
        entry.set_state(SlotState::Reserved, Ordering::Release);
        Slot::new_internal(entry, op, storage, &mut op_entry.platform_data, index)
    }
}
