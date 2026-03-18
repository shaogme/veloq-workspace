use crate::ops::{IocpOp, OverlappedEntry};
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use veloq_driver_core::slot::{ErasedPayload, SlotEntry, SlotTable};
use windows_sys::Win32::System::IO::OVERLAPPED;

mod sealed {
    /// Sealed trait for SlotState
    pub trait Sealed {}
}

pub(crate) trait SlotState: sealed::Sealed {}

pub(crate) struct Pending;
pub(crate) struct Initialized;
pub(crate) struct InFlight;
pub(crate) struct Completed;

impl sealed::Sealed for Pending {}
impl sealed::Sealed for Initialized {}
impl sealed::Sealed for InFlight {}
impl sealed::Sealed for Completed {}

impl SlotState for Pending {}
impl SlotState for Initialized {}
impl SlotState for InFlight {}
impl SlotState for Completed {}

pub(crate) struct Slot<'a, State: SlotState> {
    entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotState> Slot<'a, State> {
    #[inline]
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

impl<'a> Slot<'a, Pending> {
    #[inline]
    pub(crate) fn pending_entry(
        entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        Self {
            entry,
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
        // SAFETY: We have exclusive access to the slot in Pending state, and it is not in flight.
        unsafe {
            let sidecar = &mut *self.entry.sidecar.get();
            debug_assert!(!sidecar.in_flight, "Cannot init_op on an in-flight slot!");

            *self.entry.op.get() = Some(op);
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
            sidecar.in_flight = false;
        }
        Slot {
            entry: self.entry,
            index: self.index,
            _state: PhantomData,
        }
    }
}

impl<'a> Slot<'a, Initialized> {
    /// # Safety
    ///
    /// The caller must ensure that the slot is not being concurrently accessed in a way that
    /// violates memory safety.
    #[inline]
    pub(crate) unsafe fn entry(&self) -> &'a SlotEntry<IocpOp, OverlappedEntry> {
        self.entry
    }

    pub(crate) fn start_submission(self) -> SubmissionGuard<'a> {
        // SAFETY: We have exclusive access to the slot in Initialized state.
        unsafe {
            (*self.entry.sidecar.get()).in_flight = true;
        }
        SubmissionGuard {
            slot: self,
            persisted: false,
        }
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        // SAFETY: Slot is in Initialized state, so op must be present.
        unsafe { (*self.entry().op.get()).as_mut().map(f) }
    }

    pub(crate) fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        // SAFETY: Slot is in Initialized state, sidecar is valid.
        unsafe { &mut (*self.entry().sidecar.get()).inner as *mut _ }
    }
}

impl<'a> Slot<'a, InFlight> {
    #[inline]
    pub(crate) fn is_in_flight(table: &SlotTable<IocpOp, OverlappedEntry>, index: usize) -> bool {
        Self::is_in_flight_entry(&table.slots[index])
    }

    #[inline]
    pub(crate) fn is_in_flight_entry(entry: &SlotEntry<IocpOp, OverlappedEntry>) -> bool {
        // SAFETY: SlotEntry sidecar is always valid for read.
        unsafe { (*entry.sidecar.get()).in_flight }
    }

    /// # Safety
    ///
    /// The caller must ensure that the slot at index is in InFlight state.
    #[inline]
    pub(crate) unsafe fn assume_in_flight(
        table: &'a SlotTable<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        Self {
            entry: &table.slots[index],
            index,
            _state: PhantomData,
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the entry is in InFlight state.
    #[inline]
    pub(crate) unsafe fn as_inflight_entry(
        entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        Self {
            entry,
            index,
            _state: PhantomData,
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the slot is not being concurrently accessed in a way that
    /// violates memory safety.
    #[inline]
    pub(crate) unsafe fn entry(&self) -> &'a SlotEntry<IocpOp, OverlappedEntry> {
        self.entry
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        // SAFETY: We have exclusive access to the slot in InFlight state.
        unsafe {
            (*self.entry.sidecar.get()).in_flight = false;
        }
        Slot {
            entry: self.entry,
            index: self.index,
            _state: PhantomData,
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the sidecar is not being concurrently accessed.
    pub(crate) unsafe fn sidecar_unchecked<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut OverlappedEntry) -> R,
    {
        // SAFETY: The caller guarantees exclusive access to the sidecar.
        unsafe { f(&mut *self.entry().sidecar.get()) }
    }

    /// # Safety
    ///
    /// The caller must ensure that the op is not being concurrently accessed.
    pub(crate) unsafe fn op_mut_unchecked<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        // SAFETY: The caller guarantees exclusive access to the op.
        unsafe { (*self.entry().op.get()).as_mut().map(f) }
    }

    pub(crate) fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        // SAFETY: Slot is in InFlight state, sidecar is valid.
        unsafe { &mut (*self.entry().sidecar.get()).inner as *mut _ }
    }
}

impl<'a> Slot<'a, Completed> {
    /// # Safety
    ///
    /// The caller must ensure that the slot is not being concurrently accessed.
    #[inline]
    pub(crate) unsafe fn entry(&self) -> &'a SlotEntry<IocpOp, OverlappedEntry> {
        self.entry
    }

    pub(crate) fn reset(self) -> Slot<'a, Pending> {
        let generation = self.entry.generation.load(Ordering::Acquire);
        self.entry.reset(generation + 1);
        Slot {
            entry: self.entry,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) fn take_op(&mut self) -> Option<IocpOp> {
        // SAFETY: Slot is in Completed state, we have exclusive access to take the op.
        unsafe { (*self.entry().op.get()).take() }
    }

    pub(crate) fn take_completion_data(
        &mut self,
    ) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
        // SAFETY: Slot is in Completed state, we have exclusive access.
        unsafe {
            let entry = self.entry();
            let payload = (*entry.payload.get()).take();
            let detail = (*entry.result.get()).take();
            (payload, detail)
        }
    }
}

/// A RAII guard that manages in-flight status during submission.
pub(crate) struct SubmissionGuard<'a> {
    pub(crate) slot: Slot<'a, Initialized>,
    persisted: bool,
}

impl<'a> SubmissionGuard<'a> {
    pub(crate) fn persist(mut self) -> Slot<'a, InFlight> {
        self.persisted = true;
        Slot {
            // SAFETY: Slot is in Initialized state, entry is valid.
            entry: unsafe { self.slot.entry() },
            index: self.slot.index(),
            _state: PhantomData,
        }
    }
}

impl<'a> Drop for SubmissionGuard<'a> {
    fn drop(&mut self) {
        if !self.persisted {
            // SAFETY: We have exclusive access to the slot during drop.
            unsafe {
                (*self.slot.entry().sidecar.get()).in_flight = false;
            }
        }
    }
}
