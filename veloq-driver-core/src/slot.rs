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

struct SlotStorage<Op, S: SlotSidecar> {
    op: Option<Op>,
    result: Option<std::io::Result<usize>>,
    payload: Option<ErasedPayload>,
    sidecar: S,
}

impl<Op, S: SlotSidecar> SlotStorage<Op, S> {
    #[inline]
    fn new() -> Self {
        Self {
            op: None,
            result: None,
            payload: None,
            sidecar: S::default(),
        }
    }
}

#[derive(Debug)]
pub struct SlotData<Op, S: SlotSidecar> {
    pub generation: AtomicU32,
    pub next_free: AtomicUsize,
    pub state: AtomicU8,
    storage: UnsafeCell<SlotStorage<Op, S>>,
}

unsafe impl<Op: Send, S: SlotSidecar> Sync for SlotData<Op, S> {}

impl<Op, S: SlotSidecar> SlotData<Op, S> {
    const NULL_INDEX: usize = usize::MAX;

    pub fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            state: AtomicU8::new(SlotState::Free as u8),
            storage: UnsafeCell::new(SlotStorage::new()),
        }
    }

    pub fn reset(&self, generation: u32) {
        unsafe {
            *self.storage.get() = SlotStorage::new();
        }
        self.state.store(SlotState::Free as u8, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
    }

    /// # Safety
    ///
    /// Caller must ensure state-based exclusive access to all mutable slot fields.
    #[inline]
    pub unsafe fn with_storage_unchecked<F, R>(&self, f: F) -> R
    where
        F: FnOnce(
            &mut Option<Op>,
            &mut Option<std::io::Result<usize>>,
            &mut Option<ErasedPayload>,
            &mut S,
        ) -> R,
    {
        // SAFETY: Caller guarantees exclusive access to the mutable slot storage.
        let storage = unsafe { &mut *self.storage.get() };
        f(
            &mut storage.op,
            &mut storage.result,
            &mut storage.payload,
            &mut storage.sidecar,
        )
    }

    /// # Safety
    ///
    /// Caller must ensure state-based safety for reading.
    #[inline]
    pub unsafe fn sidecar_ref(&self) -> &S {
        // SAFETY: delegated to caller via method contract.
        unsafe { &(*self.storage.get()).sidecar }
    }

    /// Forcefully clear slot contents and reset slot to Free state.
    ///
    /// This helper centralizes unsafe field access when caller needs to
    /// retrieve completion data from a non-InFlight slot and immediately reset
    /// it for reuse.
    #[inline]
    pub fn force_reset_to_free(
        &self,
        next_generation: u32,
    ) -> (Option<ErasedPayload>, Option<std::io::Result<usize>>) {
        // SAFETY: Caller guarantees this reset path has exclusive ownership for
        // the current slot lifecycle transition.
        unsafe {
            let (_op, payload, detail) = self.with_storage_unchecked(|op, result, payload, _| {
                let op_taken = op.take();
                let payload_taken = payload.take();
                let detail_taken = result.take();
                (op_taken, payload_taken, detail_taken)
            });
            self.reset(next_generation);
            (payload, detail)
        }
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
