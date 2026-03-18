use crate::SlotSidecar;
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
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
#[repr(u8)]
pub enum SlotState {
    Free = 0,
    Pending = 1,
    Initialized = 2,
    InFlight = 3,
    Completed = 4,
}

#[derive(Debug)]
pub struct SlotData<Op, S: SlotSidecar> {
    pub generation: AtomicU32,
    pub next_free: AtomicUsize,
    pub state: AtomicU8,
    op: UnsafeCell<Option<Op>>,
    result: UnsafeCell<Option<std::io::Result<usize>>>,
    payload: UnsafeCell<Option<ErasedPayload>>,
    sidecar: UnsafeCell<S>,
}

unsafe impl<Op: Send, S: SlotSidecar> Sync for SlotData<Op, S> {}

impl<Op, S: SlotSidecar> SlotData<Op, S> {
    const NULL_INDEX: usize = usize::MAX;

    pub fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            state: AtomicU8::new(SlotState::Free as u8),
            op: UnsafeCell::new(None),
            result: UnsafeCell::new(None),
            payload: UnsafeCell::new(None),
            sidecar: UnsafeCell::new(S::default()),
        }
    }

    pub fn reset(&self, generation: u32) {
        unsafe {
            *self.op.get() = None;
            *self.result.get() = None;
            *self.payload.get() = None;
            *self.sidecar.get() = S::default();
        }
        self.state.store(SlotState::Free as u8, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
    }

    /// # Safety
    ///
    /// Caller must ensure exclusive access based on state.
    #[inline]
    pub unsafe fn op_mut(&self) -> &mut Option<Op> {
        unsafe { &mut *self.op.get() }
    }

    /// # Safety
    ///
    /// Caller must ensure exclusive access based on state.
    #[inline]
    pub unsafe fn result_mut(&self) -> &mut Option<std::io::Result<usize>> {
        unsafe { &mut *self.result.get() }
    }

    /// # Safety
    ///
    /// Caller must ensure exclusive access based on state.
    #[inline]
    pub unsafe fn payload_mut(&self) -> &mut Option<ErasedPayload> {
        unsafe { &mut *self.payload.get() }
    }

    /// # Safety
    ///
    /// Caller must ensure exclusive access based on state.
    #[inline]
    pub unsafe fn sidecar_mut(&self) -> &mut S {
        unsafe { &mut *self.sidecar.get() }
    }

    /// # Safety
    ///
    /// Caller must ensure state-based safety for reading.
    #[inline]
    pub unsafe fn sidecar_ref(&self) -> &S {
        unsafe { &*self.sidecar.get() }
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
