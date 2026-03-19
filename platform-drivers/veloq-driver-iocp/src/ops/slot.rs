use crate::ops::{IocpOp, OverlappedEntry};
use crate::win32::Overlapped;
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use veloq_driver_core::slot::{
    ErasedPayload, SlotEntry, SlotState as CoreState, SlotStorage, SlotTable,
};

mod sealed {
    /// Sealed trait for Slot states.
    pub trait Sealed {}
}

pub(crate) trait SlotState: sealed::Sealed {}

pub(crate) struct Pending;
pub(crate) struct Initialized;
pub(crate) struct InFlight;
pub(crate) struct Cancelled;
pub(crate) struct Completed;

impl sealed::Sealed for Pending {}
impl sealed::Sealed for Initialized {}
impl sealed::Sealed for InFlight {}
impl sealed::Sealed for Cancelled {}
impl sealed::Sealed for Completed {}

impl SlotState for Pending {}
impl SlotState for Initialized {}
impl SlotState for InFlight {}
impl SlotState for Cancelled {}
impl SlotState for Completed {}

pub(crate) struct Slot<'a, State: SlotState> {
    entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
    storage: &'a mut SlotStorage<IocpOp, OverlappedEntry>,
    index: usize,
    _state: PhantomData<State>,
}

#[inline]
fn is_inflight_or_cancelled(state: CoreState) -> bool {
    matches!(state, CoreState::InFlight | CoreState::Cancelled)
}

#[inline]
fn is_cancelled(state: CoreState) -> bool {
    matches!(state, CoreState::Cancelled)
}

impl<'a> Slot<'a, Pending> {
    #[inline]
    pub(crate) fn pending_entry(
        entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
        storage: &'a mut SlotStorage<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        entry.set_state(CoreState::Pending, Ordering::Release);
        Self {
            entry,
            storage,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn init_op(
        self,
        op: IocpOp,
        user_data: usize,
        generation: u32,
    ) -> Slot<'a, Initialized> {
        self.storage
            .with_mut(|slot_op, _result, _payload, sidecar| {
                *slot_op = Some(op);
                sidecar.user_data = user_data;
                sidecar.generation = generation;
                sidecar.blocking_result = None;
                sidecar.in_flight = false;
            });

        self.entry
            .set_state(CoreState::Initialized, Ordering::Release);

        Slot {
            entry: self.entry,
            storage: self.storage,
            index: self.index,
            _state: PhantomData,
        }
    }
}

impl<'a> Slot<'a, Initialized> {
    pub(crate) fn start_submission(self) -> SubmissionGuard<'a> {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| sidecar.in_flight = true);
        self.entry.set_state(CoreState::InFlight, Ordering::Release);

        SubmissionGuard {
            slot: Some(self),
            persisted: false,
        }
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        self.storage
            .with_mut(|op, _result, _payload, _sidecar| op.as_mut().map(f))
    }

    pub(crate) fn overlapped_ptr(&mut self) -> *mut Overlapped {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| &mut sidecar.inner as *mut Overlapped)
    }
}

impl<'a> Slot<'a, InFlight> {
    #[inline]
    pub(crate) fn is_in_flight(table: &SlotTable<IocpOp, OverlappedEntry>, index: usize) -> bool {
        Self::is_in_flight_entry(&table.slots[index])
    }

    #[inline]
    pub(crate) fn is_in_flight_entry(entry: &SlotEntry<IocpOp, OverlappedEntry>) -> bool {
        is_inflight_or_cancelled(entry.state(Ordering::Acquire))
    }

    #[inline]
    pub(crate) fn is_cancelled_entry(entry: &SlotEntry<IocpOp, OverlappedEntry>) -> bool {
        is_cancelled(entry.state(Ordering::Acquire))
    }

    pub(crate) fn as_inflight_entry(
        entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
        storage: &'a mut SlotStorage<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        Self {
            entry,
            storage,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| sidecar.in_flight = false);
        self.entry
            .set_state(CoreState::Completed, Ordering::Release);

        Slot {
            entry: self.entry,
            storage: self.storage,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) fn cancel(self) -> Slot<'a, Cancelled> {
        self.entry
            .set_state(CoreState::Cancelled, Ordering::Release);
        Slot {
            entry: self.entry,
            storage: self.storage,
            index: self.index,
            _state: PhantomData,
        }
    }

    /// Access sidecar without state checks.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the slot is in a valid state for sidecar access.
    pub(crate) unsafe fn sidecar_unchecked<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut OverlappedEntry) -> R,
    {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| f(sidecar))
    }

    /// Access op without state checks.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the slot is in a valid state for op access.
    pub(crate) unsafe fn op_mut_unchecked<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        self.storage
            .with_mut(|op, _result, _payload, _sidecar| op.as_mut().map(f))
    }

    pub(crate) fn overlapped_ptr(&mut self) -> *mut Overlapped {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| &mut sidecar.inner as *mut Overlapped)
    }
}

impl<'a> Slot<'a, Completed> {
    pub(crate) fn reset(self) -> Slot<'a, Pending> {
        let generation = self.entry.generation.load(Ordering::Acquire);
        self.storage.reset();
        self.entry.reset(generation + 1);
        self.entry.set_state(CoreState::Pending, Ordering::Release);
        Slot {
            entry: self.entry,
            storage: self.storage,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) fn take_op(&mut self) -> Option<IocpOp> {
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
    pub(crate) fn persist(mut self) -> io::Result<Slot<'a, InFlight>> {
        self.persisted = true;
        let slot = match self.slot.take() {
            Some(s) => s,
            None => return Err(io::Error::other("submission guard slot missing in persist")),
        };
        Ok(Slot {
            entry: slot.entry,
            storage: slot.storage,
            index: slot.index,
            _state: PhantomData,
        })
    }
}

impl<'a> Drop for SubmissionGuard<'a> {
    fn drop(&mut self) {
        if !self.persisted
            && let Some(slot) = self.slot.as_mut()
        {
            slot.storage
                .with_mut(|_op, _result, _payload, sidecar| sidecar.in_flight = false);
            slot.entry
                .set_state(CoreState::Initialized, Ordering::Release);
        }
    }
}
