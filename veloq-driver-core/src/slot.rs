use crate::SlotSidecar;
use crossbeam_utils::CachePadded;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

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
        let this = ManuallyDrop::new(self);
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
    pub const fn new(state: SlotState) -> Self {
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
    pub fn state(&self, ordering: Ordering) -> SlotState {
        self.state.load(ordering)
    }

    #[inline]
    pub fn set_state(&self, state: SlotState, ordering: Ordering) {
        self.state.store(state, ordering);
    }

    pub fn reset(&self, generation: u32) {
        self.set_state(SlotState::Free, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
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
