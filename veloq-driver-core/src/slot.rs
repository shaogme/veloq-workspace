use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::op_registry::OpRegistry;
use crossbeam_utils::CachePadded;
use std::io;
use std::marker::PhantomData;
use veloq_atomic_waker::AtomicWaker;
use veloq_shim::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use veloq_shim::sync::Mutex;

/// Manual payload container: raw pointer + static kind + drop fn.
#[derive(Debug)]
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
    Idle,
    Reserved,
    InFlightWaiting,
    InFlightReady,
    InFlightOrphaned,
    Finalizing,
}

#[inline]
const fn encode_slot_state(state: SlotState) -> u8 {
    match state {
        SlotState::Idle => 0,
        SlotState::Reserved => 1,
        SlotState::InFlightWaiting => 2,
        SlotState::InFlightReady => 3,
        SlotState::InFlightOrphaned => 4,
        SlotState::Finalizing => 5,
    }
}

#[inline]
fn decode_slot_state(raw: u8) -> SlotState {
    match raw {
        0 => SlotState::Idle,
        1 => SlotState::Reserved,
        2 => SlotState::InFlightWaiting,
        3 => SlotState::InFlightReady,
        4 => SlotState::InFlightOrphaned,
        5 => SlotState::Finalizing,
        _ => panic!("invalid SlotState encoding: {raw}"),
    }
}

const CORE_GENERATION_SHIFT: u32 = 0;
const CORE_STATE_SHIFT: u32 = 32;
const CORE_FLAGS_SHIFT: u32 = 40;

const CORE_GENERATION_MASK: u64 = 0xffff_ffff;
const CORE_STATE_MASK: u64 = 0xff << CORE_STATE_SHIFT;

#[inline]
pub(crate) const fn pack_core_state(generation: u32, state: SlotState, flags: u32) -> u64 {
    ((generation as u64) << CORE_GENERATION_SHIFT)
        | (((encode_slot_state(state) as u64) & 0xff) << CORE_STATE_SHIFT)
        | (((flags as u64) & 0x00ff_ffff) << CORE_FLAGS_SHIFT)
}

#[inline]
pub(crate) const fn core_generation(raw: u64) -> u32 {
    (raw & CORE_GENERATION_MASK) as u32
}

#[inline]
pub(crate) fn core_state(raw: u64) -> SlotState {
    decode_slot_state(((raw & CORE_STATE_MASK) >> CORE_STATE_SHIFT) as u8)
}

#[inline]
pub(crate) const fn core_with_state(raw: u64, state: SlotState) -> u64 {
    (raw & !CORE_STATE_MASK) | (((encode_slot_state(state) as u64) & 0xff) << CORE_STATE_SHIFT)
}

#[inline]
pub(crate) const fn core_with_state_generation(raw: u64, state: SlotState, generation: u32) -> u64 {
    (raw & !(CORE_STATE_MASK | CORE_GENERATION_MASK))
        | (((encode_slot_state(state) as u64) & 0xff) << CORE_STATE_SHIFT)
        | ((generation as u64) << CORE_GENERATION_SHIFT)
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
    core_state: AtomicU64,
    pub next_free: AtomicUsize,
    pub(crate) completion_res: AtomicI32,
    pub(crate) completion_flags: AtomicU32,
    pub(crate) completion_data: Mutex<CompletionData>,
    pub(crate) completion_waker: AtomicWaker,
    _marker: PhantomData<fn() -> (Op, S)>,
}

#[derive(Debug, Default)]
pub(crate) struct CompletionData {
    pub payload: Option<ErasedPayload>,
    pub detail: Option<io::Result<usize>>,
}

impl<Op, S: SlotSidecar> SlotData<Op, S> {
    const NULL_INDEX: usize = usize::MAX;

    pub fn new() -> Self {
        Self {
            core_state: AtomicU64::new(pack_core_state(0, SlotState::Idle, 0)),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            completion_res: AtomicI32::new(0),
            completion_flags: AtomicU32::new(0),
            completion_data: Mutex::new(CompletionData::default()),
            completion_waker: AtomicWaker::new(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn state(&self, ordering: Ordering) -> SlotState {
        core_state(self.core_state.load(ordering))
    }

    #[inline]
    pub fn generation(&self, ordering: Ordering) -> u32 {
        core_generation(self.core_state.load(ordering))
    }

    #[inline]
    pub(crate) fn core_state(&self, ordering: Ordering) -> u64 {
        self.core_state.load(ordering)
    }

    #[inline]
    pub(crate) fn compare_exchange_core_state(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.core_state
            .compare_exchange(current, new, success, failure)
    }

    #[inline]
    pub(crate) fn set_state_generation(
        &self,
        state: SlotState,
        generation: u32,
        success: Ordering,
        failure: Ordering,
    ) -> u64 {
        let mut current = self.core_state.load(failure);
        loop {
            let new = core_with_state_generation(current, state, generation);
            match self
                .core_state
                .compare_exchange_weak(current, new, success, failure)
            {
                Ok(_) => return new,
                Err(next) => current = next,
            }
        }
    }

    #[inline]
    pub(crate) fn set_state(&self, state: SlotState, ordering: Ordering) {
        let mut current = self.core_state.load(Ordering::Acquire);
        loop {
            let new = core_with_state(current, state);
            match self
                .core_state
                .compare_exchange_weak(current, new, ordering, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    pub(crate) fn reset(&self, generation: u32) {
        self.core_state.store(
            pack_core_state(generation, SlotState::Idle, 0),
            Ordering::Release,
        );
    }

    pub(crate) fn free(&self) {
        let mut current = self.core_state.load(Ordering::Acquire);
        loop {
            let state = core_state(current);
            // Preserve READY state so detached completion can still be consumed.
            let target = if state == SlotState::InFlightReady {
                SlotState::InFlightReady
            } else {
                SlotState::Idle
            };
            let new = core_with_state(current, target);
            match self.core_state.compare_exchange_weak(
                current,
                new,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    #[inline]
    pub(crate) fn completion_with_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Option<ErasedPayload>, &mut Option<io::Result<usize>>) -> R,
    {
        let mut data = self.completion_data.lock();
        let CompletionData { payload, detail } = &mut *data;
        f(payload, detail)
    }
}

impl<Op, S: SlotSidecar> Default for SlotData<Op, S> {
    fn default() -> Self {
        Self::new()
    }
}

pub type SlotEntry<Op, S> = CachePadded<SlotData<Op, S>>;

pub struct DetachedCancelTable {
    slot_count: usize,
    cancel_words: Box<[CachePadded<AtomicU64>]>,
    cancel_generations: Box<[CachePadded<AtomicU64>]>,
}

impl DetachedCancelTable {
    pub fn new(capacity: usize) -> Self {
        let word_count = capacity.div_ceil(64);
        let mut cancel_words = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            cancel_words.push(CachePadded::new(AtomicU64::new(0)));
        }
        let mut cancel_generations = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            cancel_generations.push(CachePadded::new(AtomicU64::new(0)));
        }
        Self {
            slot_count: capacity,
            cancel_words: cancel_words.into_boxed_slice(),
            cancel_generations: cancel_generations.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn request_cancel(&self, token: u64) {
        let (idx, generation) = crate::driver::decode_completion_token(token);
        if idx >= self.slot_count {
            return;
        }

        let generation = generation as u64;
        let cell = &self.cancel_generations[idx];
        let mut current = cell.load(Ordering::Acquire);
        while generation > current {
            match cell.compare_exchange_weak(
                current,
                generation,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }

        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        self.cancel_words[word_idx].fetch_or(1u64 << bit_idx, Ordering::Release);
    }

    #[inline]
    pub fn cancel_word_count(&self) -> usize {
        self.cancel_words.len()
    }

    #[inline]
    pub fn take_cancel_word(&self, word_idx: usize) -> u64 {
        self.cancel_words[word_idx].fetch_and(0, Ordering::AcqRel)
    }

    #[inline]
    pub(crate) fn cancel_generation(&self, idx: usize) -> u64 {
        self.cancel_generations[idx].load(Ordering::Acquire)
    }
}

pub struct SlotTable<Op, S: SlotSidecar> {
    pub slots: Box<[SlotEntry<Op, S>]>,
    pub remote_free_head: AtomicUsize,
}

unsafe impl<Op, S: SlotSidecar> Sync for SlotTable<Op, S> {}

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

    #[inline]
    pub(crate) fn slot_snapshot(&self, idx: usize) -> Option<(u32, SlotState)> {
        self.slots.get(idx).map(|slot| {
            let core = slot.core_state(Ordering::Acquire);
            (core_generation(core), core_state(core))
        })
    }
}

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
    matches!(state, SlotState::Reserved | SlotState::InFlightWaiting)
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, Reserved, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
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

    pub fn init_op_with<F>(self, op: Op, init_sidecar: F) -> Slot<'a, Reserved, Op, P, S>
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
        rollback: Option<SubmissionRollback<'a, Op, P, S>>,
    ) -> SubmissionGuard<'a, Op, P, S> {
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
    pub fn with_op_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut Op) -> R,
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

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, InFlightWaiting, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
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

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S> {
        assert!(
            self.op.is_some(),
            "slot {} in InFlight state must contain an op",
            self.index
        );
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }

    pub fn cancel(self) -> Slot<'a, InFlightOrphaned, Op, P, S> {
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
    pub fn reset(self) -> Slot<'a, Reserved, Op, P, S> {
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

    pub fn take_completion_data(&mut self) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
        self.storage
            .with_mut(|_op, result, payload, _sidecar| (payload.take(), result.take()))
    }
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> Slot<'a, InFlightOrphaned, Op, P, S> {
    #[inline]
    fn try_bind(
        entry: &'a SlotEntry<Op, S>,
        op: &'a mut Option<Op>,
        storage: &'a mut SlotStorage<Op, S>,
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

    pub fn complete(self) -> Slot<'a, Completed, Op, P, S> {
        Slot::new_internal(self.entry, self.op, self.storage, self.platform, self.index)
    }
}

type SubmissionRollback<'a, Op, P, S> = fn(&mut Slot<'a, Reserved, Op, P, S>);

pub struct SubmissionGuard<'a, Op: PlatformOp, P, S: SlotSidecar> {
    pub slot: Option<Slot<'a, Reserved, Op, P, S>>,
    rollback: Option<SubmissionRollback<'a, Op, P, S>>,
    persisted: bool,
}

impl<'a, Op: PlatformOp, P, S: SlotSidecar> SubmissionGuard<'a, Op, P, S> {
    pub fn persist(mut self) -> Slot<'a, InFlightWaiting, Op, P, S> {
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
            slot.entry.set_state(SlotState::Reserved, Ordering::Release);
        }
    }
}

pub enum SlotView<'a, Op: PlatformOp, P, S: SlotSidecar> {
    Reserved(Slot<'a, Reserved, Op, P, S>),
    InFlightWaiting(Slot<'a, InFlightWaiting, Op, P, S>),
    InFlightOrphaned(Slot<'a, InFlightOrphaned, Op, P, S>),
}

pub trait SlotRegistryExt<Op: PlatformOp, P, S: SlotSidecar> {
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S>>;
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Op, P, S>;
}

impl<Op: PlatformOp, P: Default, S: SlotSidecar> SlotRegistryExt<Op, P, S>
    for OpRegistry<Op, P, S>
{
    #[inline]
    fn slot_view(&mut self, index: usize) -> Option<SlotView<'_, Op, P, S>> {
        let (entry, op_entry, op, storage) = self.get_slot_entry_op_storage_and_entry_mut(index)?;
        match entry.state(Ordering::Acquire) {
            SlotState::Reserved => Slot::<Reserved, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::Reserved),
            SlotState::InFlightWaiting => Slot::<InFlightWaiting, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlightWaiting),
            SlotState::InFlightOrphaned => Slot::<InFlightOrphaned, Op, P, S>::try_bind(
                entry,
                op,
                storage,
                &mut op_entry.platform_data,
                index,
            )
            .map(SlotView::InFlightOrphaned),
            SlotState::Idle | SlotState::InFlightReady | SlotState::Finalizing => None,
        }
    }

    #[inline]
    fn slot_reserve(&mut self, index: usize) -> Slot<'_, Reserved, Op, P, S> {
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
    use crate::driver::encode_completion_token;

    #[test]
    fn request_cancel_sets_bit_and_generation() {
        let cancel_table = DetachedCancelTable::new(64);
        cancel_table.request_cancel(encode_completion_token(5, 7));

        assert_eq!(cancel_table.take_cancel_word(0), 1u64 << 5);
        assert_eq!(cancel_table.cancel_generation(5), 7);
    }

    #[test]
    fn request_cancel_keeps_newest_generation() {
        let cancel_table = DetachedCancelTable::new(64);

        cancel_table.request_cancel(encode_completion_token(5, 7));
        cancel_table.request_cancel(encode_completion_token(5, 11));
        cancel_table.request_cancel(encode_completion_token(5, 9));

        assert_eq!(cancel_table.cancel_generation(5), 11);
    }
}
