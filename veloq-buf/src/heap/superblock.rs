use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Order of the Superblock (64 Slots = 2^6)
pub const SUPERBLOCK_ORDER: usize = 6;
/// Number of slots in a Superblock
pub const SUPERBLOCK_SIZE: usize = 1 << SUPERBLOCK_ORDER;

/// State of a Superblock
///
/// Tracks allocation status and ownership state.
///
/// # Concurrency
/// Uses `SeqCst` ordering to prevent race conditions between "Retiring a Superblock"
/// and "Last Slot Deallocation".
#[derive(Debug)]
pub struct SuperblockState {
    /// Bitmap of free slots. 1 = Free, 0 = Used.
    pub free_mask: AtomicU64,
    /// Indicates if a thread is currently holding this superblock as its active allocation buffer.
    /// If true, the superblock cannot be returned to the global buddy system even if empty.
    pub is_active: AtomicBool,
}

impl SuperblockState {
    pub const fn new() -> Self {
        Self {
            // Initialize to 0 (All Used).
            // This is "safe" because the superblock is Inactive.
            // It effectively treats the uninitialized state as "Full and Inactive".
            // The actual state is set to "All Free" in `init()` when acquired from Buddy.
            free_mask: AtomicU64::new(0),
            is_active: AtomicBool::new(false),
        }
    }

    /// Reset state for reuse (Called when acquiring from Buddy)
    pub fn init(&self) {
        self.free_mask.store(u64::MAX, Ordering::Release);
        self.is_active.store(true, Ordering::Release);
    }

    /// Try to allocate one slot `(0..63)`.
    pub fn alloc_one(&self) -> Option<u16> {
        let mut old = self.free_mask.load(Ordering::Relaxed);
        loop {
            if old == 0 {
                return None;
            }
            let idx = old.trailing_zeros();
            let new = old & !(1u64 << idx);
            match self.free_mask.compare_exchange_weak(
                old,
                new,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(idx as u16),
                Err(x) => old = x,
            }
        }
    }

    /// Mark a slot as free.
    /// Returns `true` if the superblock is NOW eligible for return to Buddy System.
    /// (i.e., it is Empty AND Not Active).
    pub fn free_one(&self, idx: u16) -> bool {
        let mask = 1u64 << idx;

        // SeqCst is required here to synchronize with `set_inactive`.
        // We need to ensure that if we see active=true, the Retiring thread
        // will definitely see our bit update.
        let prev = self.free_mask.fetch_or(mask, Ordering::SeqCst);

        let new_mask = prev | mask;

        if new_mask == u64::MAX {
            // It is empty. Check if it is active.
            !self.is_active.load(Ordering::SeqCst)
        } else {
            false
        }
    }

    /// Mark the superblock as inactive (Thread gave up on it).
    /// Returns `true` if the superblock is Empty and should be returned to Buddy System.
    pub fn set_inactive(&self) -> bool {
        // SeqCst required.
        self.is_active.store(false, Ordering::SeqCst);

        // internal check
        let mask = self.free_mask.load(Ordering::SeqCst);
        mask == u64::MAX
    }
}
