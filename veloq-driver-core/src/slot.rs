use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

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

#[repr(C)]
#[cfg(windows)]
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub generation: u32,
    pub blocking_result: Option<std::io::Result<usize>>,
}

#[cfg(windows)]
impl Default for OverlappedEntry {
    fn default() -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() },
            user_data: 0,
            generation: 0,
            blocking_result: None,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(windows, repr(C))]
pub struct Slot<Op> {
    #[cfg(windows)]
    index: usize,
    pub generation: AtomicU32,
    pub next_free: AtomicUsize,
    pub op: UnsafeCell<Option<Op>>,
    pub result: UnsafeCell<Option<std::io::Result<usize>>>,
    pub payload: UnsafeCell<Option<ErasedPayload>>,
    #[cfg(windows)]
    pub overlapped: UnsafeCell<OverlappedEntry>,
}

unsafe impl<Op: Send> Sync for Slot<Op> {}

impl<Op> Slot<Op> {
    const NULL_INDEX: usize = usize::MAX;

    #[cfg_attr(not(windows), allow(clippy::new_without_default))]
    pub fn new(#[cfg(windows)] index: usize) -> Self {
        Self {
            #[cfg(windows)]
            index,
            generation: AtomicU32::new(0),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            op: UnsafeCell::new(None),
            result: UnsafeCell::new(None),
            payload: UnsafeCell::new(None),
            #[cfg(windows)]
            overlapped: UnsafeCell::new(OverlappedEntry {
                inner: unsafe { std::mem::zeroed() },
                user_data: index,
                generation: 0,
                blocking_result: None,
            }),
        }
    }

    pub fn reset(&self, generation: u32) {
        unsafe {
            *self.op.get() = None;
            *self.result.get() = None;
            *self.payload.get() = None;
        }
        self.generation.store(generation, Ordering::Release);
        #[cfg(windows)]
        unsafe {
            let entry = OverlappedEntry {
                user_data: self.index,
                generation,
                ..Default::default()
            };
            *self.overlapped.get() = entry;
        }
    }

    #[cfg(windows)]
    pub fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        unsafe { &mut (*self.overlapped.get()).inner }
    }
}

pub type SlotEntry<Op> = CachePadded<Slot<Op>>;

pub struct SlotTable<Op> {
    pub slots: Box<[SlotEntry<Op>]>,
    pub remote_free_head: AtomicUsize,
}

unsafe impl<Op: Send> Sync for SlotTable<Op> {}
unsafe impl<Op: Send> Send for SlotTable<Op> {}

impl<Op> SlotTable<Op> {
    pub const NULL_INDEX: usize = usize::MAX;

    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _i in 0..capacity {
            slots.push(CachePadded::new(Slot::new(
                #[cfg(windows)]
                _i,
            )));
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
