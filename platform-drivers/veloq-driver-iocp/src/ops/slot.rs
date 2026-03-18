use crate::ops::{IocpOp, OverlappedEntry};
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use veloq_driver_core::slot::{ErasedPayload, SlotEntry, SlotTable};
use windows_sys::Win32::System::IO::OVERLAPPED;

mod sealed {
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
    #[inline]
    pub(crate) unsafe fn entry(&self) -> &'a SlotEntry<IocpOp, OverlappedEntry> {
        self.entry
    }

    pub(crate) fn start_submission(self) -> SubmissionGuard<'a> {
        unsafe {
            (*self.entry.sidecar.get()).in_flight = true;
        }
        SubmissionGuard {
            slot: self,
            persisted: false,
        }
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        unsafe {
            let op = (*self.entry().op.get())
                .as_mut()
                .expect("Op missing in Initialized state");
            f(op)
        }
    }

    pub(crate) fn overlapped_ptr(&self) -> *mut OVERLAPPED {
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
        unsafe { (*entry.sidecar.get()).in_flight }
    }

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

    #[inline]
    pub(crate) unsafe fn assume_in_flight_entry(
        entry: &'a SlotEntry<IocpOp, OverlappedEntry>,
        index: usize,
    ) -> Self {
        Self {
            entry,
            index,
            _state: PhantomData,
        }
    }

    #[inline]
    pub(crate) unsafe fn entry(&self) -> &'a SlotEntry<IocpOp, OverlappedEntry> {
        self.entry
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        unsafe {
            (*self.entry.sidecar.get()).in_flight = false;
        }
        Slot {
            entry: self.entry,
            index: self.index,
            _state: PhantomData,
        }
    }

    pub(crate) unsafe fn with_sidecar_mut_unchecked<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut OverlappedEntry) -> R,
    {
        unsafe { f(&mut *self.entry().sidecar.get()) }
    }

    pub(crate) unsafe fn with_op_mut_unchecked<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut IocpOp) -> R,
    {
        unsafe {
            let op = (*self.entry().op.get())
                .as_mut()
                .expect("Op missing in InFlight state");
            f(op)
        }
    }

    pub(crate) fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        unsafe { &mut (*self.entry().sidecar.get()).inner as *mut _ }
    }
}

impl<'a> Slot<'a, Completed> {
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
        unsafe { (*self.entry().op.get()).take() }
    }

    pub(crate) fn take_completion_data(
        &mut self,
    ) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
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
            entry: unsafe { self.slot.entry() },
            index: self.slot.index(),
            _state: PhantomData,
        }
    }
}

impl<'a> Drop for SubmissionGuard<'a> {
    fn drop(&mut self) {
        if !self.persisted {
            unsafe {
                (*self.slot.entry().sidecar.get()).in_flight = false;
            }
        }
    }
}
