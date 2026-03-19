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
    pub(crate) op: &'a mut Option<UringOp>,
    pub(crate) storage: &'a mut SlotStorage<UringOp, ()>,
    pub(crate) platform: &'a mut UringOpState,
    pub(crate) index: usize,
    _state: PhantomData<State>,
}

pub(crate) struct SlotSession<'a> {
    entry: &'a SlotEntry<UringOp, ()>,
    op: &'a mut Option<UringOp>,
    storage: &'a mut SlotStorage<UringOp, ()>,
    platform: &'a mut UringOpState,
    index: usize,
}

pub(crate) enum SlotView<'a> {
    Pending(Slot<'a, Pending>),
    Initialized(Slot<'a, Initialized>),
    InFlight(Slot<'a, InFlight>),
    Cancelled(Slot<'a, Cancelled>),
}

#[inline]
pub(crate) fn is_runnable_state(state: CoreState) -> bool {
    matches!(
        state,
        CoreState::Pending | CoreState::Initialized | CoreState::InFlight | CoreState::Cancelled
    )
}

impl<'a, S: SlotState> Slot<'a, S> {
    #[inline]
    fn new_internal(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
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
}

impl<'a> SlotSession<'a> {
    #[inline]
    pub(crate) fn view(self) -> Option<SlotView<'a>> {
        match self.entry.state(Ordering::Acquire) {
            CoreState::Pending => Slot::<Pending>::try_bind(
                self.entry,
                self.op,
                self.storage,
                self.platform,
                self.index,
            )
            .map(SlotView::Pending),
            CoreState::Initialized => Slot::<Initialized>::try_bind(
                self.entry,
                self.op,
                self.storage,
                self.platform,
                self.index,
            )
            .map(SlotView::Initialized),
            CoreState::InFlight => Slot::<InFlight>::try_bind(
                self.entry,
                self.op,
                self.storage,
                self.platform,
                self.index,
            )
            .map(SlotView::InFlight),
            CoreState::Completed => None,
            CoreState::Cancelled => Slot::<Cancelled>::try_bind(
                self.entry,
                self.op,
                self.storage,
                self.platform,
                self.index,
            )
            .map(SlotView::Cancelled),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn bind_initialized(self) -> Option<Slot<'a, Initialized>> {
        Slot::<Initialized>::try_bind(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub(crate) fn bind_in_flight(self) -> Option<Slot<'a, InFlight>> {
        Slot::<InFlight>::try_bind(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

impl<'a> Slot<'a, Pending> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == CoreState::Pending {
            assert!(
                op.is_none(),
                "slot {index} in Pending state must not contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn new(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Self {
        assert!(
            op.is_none(),
            "slot {index} entering Pending state must not contain an op"
        );
        entry.set_state(CoreState::Pending, Ordering::Release);
        Self {
            entry,
            op,
            storage,
            platform,
            index,
            _state: PhantomData,
        }
    }

    pub(crate) fn init_op(self, op: UringOp) -> Slot<'a, Initialized> {
        assert!(
            self.op.is_none(),
            "slot {} entering Initialized state must not already contain an op",
            self.index
        );
        *self.op = Some(op);

        self.entry
            .set_state(CoreState::Initialized, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

impl<'a> Slot<'a, Initialized> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == CoreState::Initialized {
            assert!(
                op.is_some(),
                "slot {index} in Initialized state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub(crate) fn start_submission(self) -> SubmissionGuard {
        self.entry.set_state(CoreState::InFlight, Ordering::Release);

        SubmissionGuard {
            entry: self.entry as *const SlotEntry<UringOp, ()>,
            persisted: false,
        }
    }

    #[inline]
    pub(crate) fn op_mut(&mut self) -> &mut UringOp {
        self.op
            .as_mut()
            .expect("slot in Initialized state must contain an op")
    }
}

impl<'a> Slot<'a, InFlight> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == CoreState::InFlight {
            assert!(
                op.is_some(),
                "slot {index} in InFlight state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .set_state(CoreState::Completed, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub(crate) fn cancel(self) -> Slot<'a, Cancelled> {
        self.entry
            .set_state(CoreState::Cancelled, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub(crate) fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut UringOp) -> R,
    {
        self.op.as_mut().map(f)
    }
}

impl<'a> Slot<'a, Cancelled> {
    pub(crate) fn try_bind(
        entry: &'a SlotEntry<UringOp, ()>,
        op: &'a mut Option<UringOp>,
        storage: &'a mut SlotStorage<UringOp, ()>,
        platform: &'a mut UringOpState,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == CoreState::Cancelled {
            assert!(
                op.is_some(),
                "slot {index} in Cancelled state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub(crate) fn complete(self) -> Slot<'a, Completed> {
        self.entry
            .set_state(CoreState::Completed, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

impl<'a> Slot<'a, Completed> {
    pub(crate) fn take_op(&mut self) -> Option<UringOp> {
        self.op.take()
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
                (&*self.entry).set_state(CoreState::Initialized, Ordering::Release);
            }
        }
    }
}

pub(crate) trait UringOpRegistryExt {
    fn slot_session(&mut self, index: usize) -> Option<SlotSession<'_>>;
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_>>;
    fn slot_initialized(&mut self, index: usize) -> Option<Slot<'_, Initialized>>;
    fn slot_in_flight(&mut self, index: usize) -> Option<Slot<'_, InFlight>>;
    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending>;
}

impl UringOpRegistryExt for OpRegistry<UringOp, UringOpState, ()> {
    fn slot_session(&mut self, index: usize) -> Option<SlotSession<'_>> {
        let (entry, op_entry, op, storage) = self.get_slot_entry_op_storage_and_entry_mut(index)?;
        Some(SlotSession {
            entry,
            op,
            storage,
            platform: &mut op_entry.platform_data,
            index,
        })
    }

    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_>> {
        self.slot_session(index)?.view()
    }

    fn slot_initialized(&mut self, index: usize) -> Option<Slot<'_, Initialized>> {
        self.slot_session(index)?.bind_initialized()
    }

    fn slot_in_flight(&mut self, index: usize) -> Option<Slot<'_, InFlight>> {
        self.slot_session(index)?.bind_in_flight()
    }

    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending> {
        let (entry, op_entry, op, storage) = self
            .get_slot_entry_op_storage_and_entry_mut(index)
            .expect("slot missing in registry during init");
        Slot::<Pending>::new(entry, op, storage, &mut op_entry.platform_data, index)
    }
}
