use crate::driver::{OpLifecycle, UringOpState};
use crate::op::UringOp;
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use veloq_driver_core::slot::{ErasedPayload, SlotEntry, SlotState as CoreState, SlotStorage};

mod sealed {
    pub trait Sealed {}
}

pub(crate) trait SlotState: sealed::Sealed {}

pub(crate) struct Pending;
pub(crate) struct Initialized;
pub(crate) struct InFlight;
pub(crate) struct Completed;
pub(crate) struct Cancelled;

impl sealed::Sealed for Pending {}
impl sealed::Sealed for Initialized {}
impl sealed::Sealed for InFlight {}
impl sealed::Sealed for Completed {}
impl sealed::Sealed for Cancelled {}

impl SlotState for Pending {}
impl SlotState for Initialized {}
impl SlotState for InFlight {}
impl SlotState for Completed {}
impl SlotState for Cancelled {}

pub(crate) struct Slot<'a, State: SlotState> {
    pub(crate) entry: &'a SlotEntry<UringOp, ()>,
    pub(crate) storage: &'a mut SlotStorage<UringOp, ()>,
    pub(crate) platform: &'a mut UringOpState,
    pub(crate) index: usize,
    _state: PhantomData<State>,
}

impl<'a> Slot<'a, Pending> {
    #[inline]
    pub(crate) fn new(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Self {
        entry
            .state
            .store(CoreState::Pending as u8, Ordering::Release);
        platform.lifecycle = OpLifecycle::Pending;
        Self {
            entry,
            storage,
            platform,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn init_op(self, op: UringOp) -> Slot<'a, Initialized> {
        self.storage
            .with_mut(|slot_op, _result, _payload, _sidecar| {
                *slot_op = Some(op);
            });

        self.entry
            .state
            .store(CoreState::Initialized as u8, Ordering::Release);

        Slot {
            entry: self.entry,
            storage: self.storage,
            platform: self.platform,
            index: self.index,
            _state: PhantomData,
        }
    }
}

impl<'a> Slot<'a, Initialized> {
    pub(crate) fn as_initialized(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Self {
        Self {
            entry,
            storage,
            platform,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn start_submission(self) -> SubmissionGuard<'a> {
        self.platform.lifecycle = OpLifecycle::InFlight;
        self.entry
            .state
            .store(CoreState::InFlight as u8, Ordering::Release);

        SubmissionGuard {
            slot: Some(self),
            persisted: false,
        }
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut UringOp) -> R,
    {
        self.storage
            .with_mut(|op, _result, _payload, _sidecar| op.as_mut().map(f))
    }
}

impl<'a> Slot<'a, InFlight> {
    #[inline]
    pub(crate) fn as_in_flight(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Self {
        Self {
            entry,
            storage,
            platform,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .state
            .store(CoreState::Completed as u8, Ordering::Release);
        self.platform.lifecycle = OpLifecycle::Completed;

        Slot {
            entry: self.entry,
            storage: self.storage,
            platform: self.platform,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) fn cancel(self) -> Slot<'a, Cancelled> {
        self.platform.lifecycle = OpLifecycle::Cancelled;

        Slot {
            entry: self.entry,
            storage: self.storage,
            platform: self.platform,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut UringOp) -> R,
    {
        self.storage
            .with_mut(|op, _result, _payload, _sidecar| op.as_mut().map(f))
    }
}

impl<'a> Slot<'a, Cancelled> {
    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .state
            .store(CoreState::Completed as u8, Ordering::Release);
        self.platform.lifecycle = OpLifecycle::Completed;

        Slot {
            entry: self.entry,
            storage: self.storage,
            platform: self.platform,
            index: self.index,
            _state: PhantomData,
        }
    }
}

impl<'a> Slot<'a, Completed> {
    pub(crate) fn take_op(&mut self) -> Option<UringOp> {
        self.storage
            .with_mut(|op, _result, _payload, _sidecar| op.take())
    }

    pub(crate) fn take_completion_data(
        &mut self,
    ) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
        self.storage
            .with_mut(|_op, result, payload, _sidecar| (payload.take(), result.take()))
    }
}

pub(crate) struct SubmissionGuard<'a> {
    pub(crate) slot: Option<Slot<'a, Initialized>>,
    persisted: bool,
}

impl<'a> SubmissionGuard<'a> {
    pub(crate) fn persist(mut self) -> Slot<'a, InFlight> {
        self.persisted = true;
        let slot = self.slot.take().expect("slot missing in SubmissionGuard");
        Slot {
            entry: slot.entry,
            storage: slot.storage,
            platform: slot.platform,
            index: slot.index,
            _state: PhantomData,
        }
    }
}

impl<'a> Drop for SubmissionGuard<'a> {
    fn drop(&mut self) {
        if !self.persisted {
            if let Some(slot) = self.slot.take() {
                slot.entry
                    .state
                    .store(CoreState::Initialized as u8, Ordering::Release);
                slot.platform.lifecycle = OpLifecycle::Pending;
            }
        }
    }
}
