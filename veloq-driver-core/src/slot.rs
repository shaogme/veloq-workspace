use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::op_registry::OpRegistry;
use crossbeam_utils::CachePadded;
use std::io;
use std::marker::PhantomData;
use veloq_shim::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

/// Manual payload container: raw pointer + static kind + drop fn.
pub struct ErasedPayload {
    pub ptr: *mut (),
    pub kind: u16,
    pub drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for ErasedPayload {}

impl ErasedPayload {
    #[inline]
    pub fn leak_ptr(self) -> *mut () {
        let this = std::mem::ManuallyDrop::new(self);
        this.ptr
    }
}

impl Drop for ErasedPayload {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { (self.drop_fn)(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Free,
    Pending,
    Initialized,
    InFlight,
    Cancelled,
    Completed,
}

#[derive(Debug)]
pub struct AtomicSlotState(AtomicU8);

impl AtomicSlotState {
    #[inline]
    #[cfg(not(feature = "loom"))]
    pub const fn new(state: SlotState) -> Self {
        Self(AtomicU8::new(encode_slot_state(state)))
    }

    #[inline]
    #[cfg(feature = "loom")]
    pub fn new(state: SlotState) -> Self {
        Self(AtomicU8::new(encode_slot_state(state)))
    }

    #[inline]
    pub fn load(&self, ordering: Ordering) -> SlotState {
        decode_slot_state(self.0.load(ordering))
    }

    #[inline]
    pub fn store(&self, state: SlotState, ordering: Ordering) {
        self.0.store(encode_slot_state(state), ordering);
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: SlotState,
        new: SlotState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<SlotState, SlotState> {
        self.0
            .compare_exchange(
                encode_slot_state(current),
                encode_slot_state(new),
                success,
                failure,
            )
            .map(decode_slot_state)
            .map_err(decode_slot_state)
    }
}

#[inline]
const fn encode_slot_state(state: SlotState) -> u8 {
    match state {
        SlotState::Free => 0,
        SlotState::Pending => 1,
        SlotState::Initialized => 2,
        SlotState::InFlight => 3,
        SlotState::Cancelled => 4,
        SlotState::Completed => 5,
    }
}

#[inline]
fn decode_slot_state(raw: u8) -> SlotState {
    match raw {
        0 => SlotState::Free,
        1 => SlotState::Pending,
        2 => SlotState::Initialized,
        3 => SlotState::InFlight,
        4 => SlotState::Cancelled,
        5 => SlotState::Completed,
        _ => panic!("invalid SlotState encoding: {raw}"),
    }
}

pub struct SlotStorage<Op, S: SlotSidecar> {
    op: Option<Op>,
    result: Option<std::io::Result<usize>>,
    payload: Option<ErasedPayload>,
    sidecar: S,
}

impl<Op, S: SlotSidecar> SlotStorage<Op, S> {
    #[inline]
    pub fn new() -> Self {
        Self {
            op: None,
            result: None,
            payload: None,
            sidecar: S::default(),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    #[inline]
    pub fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(
            &mut Option<Op>,
            &mut Option<std::io::Result<usize>>,
            &mut Option<ErasedPayload>,
            &mut S,
        ) -> R,
    {
        f(
            &mut self.op,
            &mut self.result,
            &mut self.payload,
            &mut self.sidecar,
        )
    }
}

impl<Op, S: SlotSidecar> Default for SlotStorage<Op, S> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct SlotData<Op, S: SlotSidecar> {
    pub generation: AtomicU32,
    pub next_free: AtomicUsize,
    state: AtomicSlotState,
    _marker: PhantomData<fn() -> (Op, S)>,
}

impl<Op, S: SlotSidecar> SlotData<Op, S> {
    const NULL_INDEX: usize = usize::MAX;

    pub fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            state: AtomicSlotState::new(SlotState::Free),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn state(&self, ordering: Ordering) -> SlotState {
        self.state.load(ordering)
    }

    #[inline]
    fn set_state(&self, state: SlotState, ordering: Ordering) {
        self.state.store(state, ordering);
    }

    pub(crate) fn reset(&self, generation: u32) {
        self.set_state(SlotState::Free, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
    }

    pub(crate) fn free(&self) {
        self.set_state(SlotState::Free, Ordering::Release);
    }
}

impl<Op, S: SlotSidecar> Default for SlotData<Op, S> {
    fn default() -> Self {
        Self::new()
    }
}

pub type SlotEntry<Op, S> = CachePadded<SlotData<Op, S>>;

pub struct SlotTable<Op, S: SlotSidecar> {
    pub slots: Box<[SlotEntry<Op, S>]>,
    pub remote_free_head: AtomicUsize,
}

unsafe impl<Op: Send, S: SlotSidecar> Sync for SlotTable<Op, S> {}
unsafe impl<Op: Send, S: SlotSidecar> Send for SlotTable<Op, S> {}

impl<Op, S: SlotSidecar> SlotTable<Op, S> {
    pub const NULL_INDEX: usize = usize::MAX;

    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(CachePadded::new(SlotData::new()));
        }
        Self {
            slots: slots.into_boxed_slice(),
            remote_free_head: AtomicUsize::new(Self::NULL_INDEX),
        }
    }

    pub fn push_free(&self, idx: usize) {
        let slot = &self.slots[idx];
        let mut head = self.remote_free_head.load(Ordering::Relaxed);
        loop {
            slot.next_free.store(head, Ordering::Relaxed);
            match self.remote_free_head.compare_exchange_weak(
                head,
                idx,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
    }

    pub fn pop_all(&self) -> usize {
        self.remote_free_head
            .swap(Self::NULL_INDEX, Ordering::Acquire)
    }
}

pub trait SlotMarker: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

pub struct Pending;
pub struct Initialized;
pub struct InFlight;
pub struct Cancelled;
pub struct Completed;

impl sealed::Sealed for Pending {}
impl sealed::Sealed for Initialized {}
impl sealed::Sealed for InFlight {}
impl sealed::Sealed for Cancelled {}
impl sealed::Sealed for Completed {}

impl SlotMarker for Pending {}
impl SlotMarker for Initialized {}
impl SlotMarker for InFlight {}
impl SlotMarker for Cancelled {}
impl SlotMarker for Completed {}

pub struct Slot<'a, State: SlotMarker, Op: PlatformOp, P, S: SlotSidecar> {
    pub entry: &'a SlotEntry<Op, S>,
    pub op: &'a mut Option<Op>,
    pub storage: &'a mut SlotStorage<Op, S>,
    platform: &'a mut P,
    index: usize,
    _state: PhantomData<State>,
}

impl<'a, State: SlotMarker, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, State, Op, P, S> {
    #[inline]
    fn new_internal(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
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
    matches!(
        state,
        SlotState::Pending | SlotState::Initialized | SlotState::InFlight | SlotState::Cancelled
    )
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, Pending, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
        platform: &'a mut P,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::Pending {
            assert!(
                op.is_none(),
                "slot {index} in Pending state must not contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn init_op_with<F>(self, op: Op, init_sidecar: F) -> Slot<'a, Initialized, Op, P, S>
    where
        F: FnOnce(&mut S),
    {
        assert!(
            self.op.is_none(),
            "slot {} entering Initialized state must not already contain an op",
            self.index
        );
        *self.op = Some(op);
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| init_sidecar(sidecar));

        self.entry
            .set_state(SlotState::Initialized, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, Initialized, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
        platform: &'a mut P,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::Initialized {
            assert!(
                op.is_some(),
                "slot {index} in Initialized state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn start_submission_with(
        self,
        rollback: Option<fn(&mut Slot<'a, Initialized, Op, P, S>)>,
    ) -> SubmissionGuard<'a, Op, P, S> {
        assert!(
            self.op.is_some(),
            "slot {} in Initialized state must contain an op",
            self.index
        );
        self.entry.set_state(SlotState::InFlight, Ordering::Release);

        SubmissionGuard {
            slot: Some(self),
            rollback,
            persisted: false,
        }
    }

    #[inline]
    pub fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut Op) -> R,
    {
        assert!(
            self.op.is_some(),
            "slot {} in Initialized state must contain an op",
            self.index
        );
        self.op.as_mut().map(f)
    }

    #[inline]
    pub fn op_mut(&mut self) -> &mut Op {
        self.op
            .as_mut()
            .expect("slot in Initialized state must contain an op")
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, InFlight, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
        platform: &'a mut P,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::InFlight {
            assert!(
                op.is_some(),
                "slot {index} in InFlight state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.entry
            .set_state(SlotState::Completed, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn cancel(self) -> Slot<'a, Cancelled, Op, P, S> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        self.entry
            .set_state(SlotState::Cancelled, Ordering::Release);
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    #[inline]
    pub fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut Op) -> R,
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
    pub unsafe fn sidecar_unchecked<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut S) -> R,
    {
        self.storage
            .with_mut(|_op, _result, _payload, sidecar| f(sidecar))
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, Completed, Op, P, S> {
    pub fn reset(self) -> Slot<'a, Pending, Op, P, S> {
        let _ = self.op.take();
        let generation = self.entry.generation.load(Ordering::Acquire);
        self.storage.reset();
        self.entry.reset(generation + 1);
        self.entry.set_state(SlotState::Pending, Ordering::Release);
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

    pub fn take_completion_data(&mut self) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
        self.storage
            .with_mut(|_op, result, payload, _sidecar| (payload.take(), result.take()))
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, Cancelled, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
        platform: &'a mut P,
        index: usize,
    ) -> Option<Self> {
        if entry.state(Ordering::Acquire) == SlotState::Cancelled {
            assert!(
                op.is_some(),
                "slot {index} in Cancelled state must contain an op"
            );
            Some(Self::new_internal(entry, op, storage, platform, index))
        } else {
            None
        }
    }

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S> {
        self.entry
            .set_state(SlotState::Completed, Ordering::Release);

        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

pub struct SubmissionGuard<'a, Op: PlatformOp, P, S: SlotSidecar> {
    pub slot: Option<Slot<'a, Initialized, Op, P, S>>,
    rollback: Option<fn(&mut Slot<'a, Initialized, Op, P, S>)>,
    persisted: bool,
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> SubmissionGuard<'a, Op, P, S> {
    pub fn persist(mut self) -> Slot<'a, InFlight, Op, P, S> {
        self.persisted = true;
        let slot = self
            .slot
            .take()
            .expect("submission guard slot missing in persist");
        Slot::new_internal(slot.entry, slot.op, slot.storage, slot.platform, slot.index)
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Drop for SubmissionGuard<'a, Op, P, S> {
    fn drop(&mut self) {
        if !self.persisted
            && let Some(slot) = self.slot.as_mut()
        {
            if let Some(rollback) = self.rollback {
                rollback(slot);
            }
            slot.entry
                .set_state(SlotState::Initialized, Ordering::Release);
        }
    }
}

pub enum SlotView<'a, Op: PlatformOp, P, S: SlotSidecar> {
    Pending(Slot<'a, Pending, Op, P, S>),
    Initialized(Slot<'a, Initialized, Op, P, S>),
    InFlight(Slot<'a, InFlight, Op, P, S>),
    Cancelled(Slot<'a, Cancelled, Op, P, S>),
}

pub trait SlotRegistryExt<Op: PlatformOp, P, S: SlotSidecar> {
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S>>;
    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending, Op, P, S>;
}

impl<Op: PlatformOp, P: Default, S: SlotSidecar> SlotRegistryExt<Op, P, S>
    for OpRegistry<Op, P, S>
{
    #[inline]
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S>> {
        let (entry, op_entry, op, storage) = self.get_slot_entry_op_storage_and_entry_mut(index)?;
        match entry.state(Ordering::Acquire) {
            SlotState::Pending => Slot::<Pending, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Pending),
            SlotState::Initialized => Slot::<Initialized, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Initialized),
            SlotState::InFlight => Slot::<InFlight, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlight),
            SlotState::Cancelled => Slot::<Cancelled, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Cancelled),
            SlotState::Completed | SlotState::Free => None,
        }
    }

    #[inline]
    fn slot_init_pending(&mut self, index: usize) -> Slot<'_, Pending, Op, P, S> {
        let (entry, op_entry, op, storage) = self
            .get_slot_entry_op_storage_and_entry_mut(index)
            .expect("slot missing in registry during init");
        assert!(
            op.is_none(),
            "slot {index} entering Pending state must not contain an op"
        );
        entry.set_state(SlotState::Pending, Ordering::Release);
        Slot::new_internal(entry, op, storage, &mut op_entry.platform_data, index)
    }
}
