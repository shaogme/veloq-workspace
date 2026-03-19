use crate::driver::UringOpState;
use crate::op::UringOp;
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use veloq_driver_core::op_registry::OpRegistry;
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

pub(crate) struct SlotSession<'a> {
    entry: &'a SlotEntry<UringOp, ()>,
    storage: &'a mut SlotStorage<UringOp, ()>,
    platform: &'a mut UringOpState,
    index: usize,
}

impl<'a, S: SlotState> Slot<'a, S> {
    #[inline]
    fn new_internal(
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
}

impl<'a> SlotSession<'a> {
    #[inline]
    pub(crate) fn bind_pending(self) -> Option<Slot<'a, Pending>> {
        Slot::<Pending>::try_bind(self.entry, self.storage, self.platform, self.index)
    }

    #[inline]
    pub(crate) fn bind_initialized(self) -> Option<Slot<'a, Initialized>> {
        Slot::<Initialized>::try_bind(self.entry, self.storage, self.platform, self.index)
    }

    #[inline]
    pub(crate) fn bind_in_flight(self) -> Option<Slot<'a, InFlight>> {
        Slot::<InFlight>::try_bind(self.entry, self.storage, self.platform, self.index)
    }

    #[inline]
    pub(crate) fn bind_cancelled(self) -> Option<Slot<'a, Cancelled>> {
        Slot::<Cancelled>::try_bind(self.entry, self.storage, self.platform, self.index)
    }
}

impl<'a> Slot<'a, Pending> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state.load(Ordering::Acquire) == CoreState::Pending as u8 {
            Some(Self::new_internal(entry, storage, platform, index))
        } else {
            None
        }
    }

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

        Slot::new_internal(self.entry, self.storage, self.platform, self.index)
    }
}

impl<'a> Slot<'a, Initialized> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state.load(Ordering::Acquire) == CoreState::Initialized as u8 {
            Some(Self::new_internal(entry, storage, platform, index))
        } else {
            None
        }
    }

    pub(crate) fn start_submission(self) -> SubmissionGuard {
        self.entry
            .state
            .store(CoreState::InFlight as u8, Ordering::Release);

        SubmissionGuard {
            entry: self.entry as *const SlotEntry<UringOp, ()>,
            persisted: false,
        }
    }

    #[inline]
    pub(crate) fn op_mut(&mut self) -> &mut UringOp {
        let op_ptr = self
            .storage
            .with_mut(|op, _result, _payload, _sidecar| op.as_mut().map(|op| op as *mut UringOp))
            .expect("slot in Initialized state must contain an op");
        unsafe { &mut *op_ptr }
    }
}

impl<'a> Slot<'a, InFlight> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state.load(Ordering::Acquire) == CoreState::InFlight as u8 {
            Some(Self::new_internal(entry, storage, platform, index))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .state
            .store(CoreState::Completed as u8, Ordering::Release);

        Slot::new_internal(self.entry, self.storage, self.platform, self.index)
    }

    pub(crate) fn cancel(self) -> Slot<'a, Cancelled> {
        self.entry
            .state
            .store(CoreState::Cancelled as u8, Ordering::Release);

        Slot::new_internal(self.entry, self.storage, self.platform, self.index)
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
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state.load(Ordering::Acquire) == CoreState::Cancelled as u8 {
            Some(Self::new_internal(entry, storage, platform, index))
        } else {
            None
        }
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .state
            .store(CoreState::Completed as u8, Ordering::Release);

        Slot::new_internal(self.entry, self.storage, self.platform, self.index)
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

pub(crate) struct SubmissionGuard {
    entry: *const SlotEntry<UringOp, ()>,
    persisted: bool,
}

impl SubmissionGuard {
    pub(crate) fn persist(mut self) {
        self.persisted = true;
    }
}

impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        if !self.persisted {
            unsafe {
                (&*self.entry)
                    .state
                    .store(CoreState::Initialized as u8, Ordering::Release);
            }
        }
    }
}

pub(crate) trait UringOpRegistryExt {
    fn slot_session(&mut self, index: usize) -> Option<SlotSession<'_>>;
    fn slot_pending(&mut self, index: usize) -> Option<Slot<'_, Pending>>;
    fn slot_initialized(&mut self, index: usize) -> Option<Slot<'_, Initialized>>;
    fn slot_in_flight(&mut self, index: usize) -> Option<Slot<'_, InFlight>>;
    fn slot_cancelled(&mut self, index: usize) -> Option<Slot<'_, Cancelled>>;
    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending>;
}

impl UringOpRegistryExt for OpRegistry<UringOp, UringOpState, ()> {
    fn slot_session(&mut self, index: usize) -> Option<SlotSession<'_>> {
        let (entry, op_entry, storage) = self.get_slot_entry_storage_and_entry_mut(index)?;
        Some(SlotSession {
            entry,
            storage,
            platform: &mut op_entry.platform_data,
            index,
        })
    }

    fn slot_pending(&mut self, index: usize) -> Option<Slot<'_, Pending>> {
        self.slot_session(index)?.bind_pending()
    }

    fn slot_initialized(&mut self, index: usize) -> Option<Slot<'_, Initialized>> {
        self.slot_session(index)?.bind_initialized()
    }

    fn slot_in_flight(&mut self, index: usize) -> Option<Slot<'_, InFlight>> {
        self.slot_session(index)?.bind_in_flight()
    }

    fn slot_cancelled(&mut self, index: usize) -> Option<Slot<'_, Cancelled>> {
        self.slot_session(index)?.bind_cancelled()
    }

    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending> {
        let (entry, op_entry, storage) = self
            .get_slot_entry_storage_and_entry_mut(index)
            .expect("slot missing in registry during init");
        Slot::<Pending>::new(entry, storage, &mut op_entry.platform_data, index)
    }
}
