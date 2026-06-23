//! Thread-local cache for the heap allocator.

use super::{
    pool::Chunk,
    units::{ChunkId, SlotIndex, SuperblockIndex, SuperblockState},
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    ptr::{self, NonNull},
    sync::Arc,
};

pub(crate) struct LocalCacheEntry {
    pub(crate) chunk: Arc<Chunk>,
    pub(crate) sb_idx: SuperblockIndex,
    pub(crate) chunk_id: ChunkId,
}

impl Drop for LocalCacheEntry {
    fn drop(&mut self) {
        // Retire active superblock on thread exit
        let should_free = self.chunk.superblocks[self.sb_idx.get()].set_inactive();
        if should_free {
            self.chunk.dealloc_superblock(self.sb_idx);
        }
    }
}

/// Local Cache Constants
pub(crate) type PoolId = usize;

/// Internal structure to manage the "hot" Fast Path of the TLS cache.
/// Uses individual Cells to ensure zero-cost register-friendly access.
pub(crate) struct HotSegment {
    pub(crate) pool_id: Cell<PoolId>,

    // --- Fast Path Flattened Primitives ---
    // These pointers/values are updated whenever `owner` is set.
    pub(crate) sb_state: Cell<*const SuperblockState>,
    pub(crate) chunk_base: Cell<*const u8>,
    pub(crate) sb_idx: Cell<SuperblockIndex>,
    pub(crate) chunk_id: Cell<ChunkId>,

    // Ownership preservation.
    // Setting this to None will automatically trigger Superblock retirement via `LocalCacheEntry::drop`.
    pub(crate) owner: Cell<Option<LocalCacheEntry>>,
}

impl HotSegment {
    pub(crate) const fn new() -> Self {
        Self {
            pool_id: Cell::new(0),
            sb_state: Cell::new(ptr::null()),
            chunk_base: Cell::new(ptr::null()),
            sb_idx: Cell::new(SuperblockIndex::new(0)),
            chunk_id: Cell::new(ChunkId::ZERO),
            owner: Cell::new(None),
        }
    }

    /// Update the hot segment with a new entry.
    pub(crate) fn set(&self, pool_id: PoolId, entry: LocalCacheEntry) {
        // Mirror metadata to primitive cells for fast access
        let sb_ref = &*entry.chunk.superblocks[entry.sb_idx.get()];
        self.sb_state.set(sb_ref as *const SuperblockState);
        self.chunk_base.set(entry.chunk.memory.as_ptr());
        self.sb_idx.set(entry.sb_idx);
        self.chunk_id.set(entry.chunk_id);
        self.pool_id.set(pool_id);

        // Ownership transfer
        self.owner.set(Some(entry));
    }

    /// Clear the hot segment. This triggers the retirement of the active superblock.
    pub(crate) fn clear(&self) {
        self.sb_state.set(ptr::null());
        self.chunk_base.set(ptr::null());
        self.owner.set(None); // Triggers Drop logic in LocalCacheEntry
    }

    /// Transfers ownership of the current hot entry without clearing primitives immediately.
    /// Caller is responsible for re-syncing or clearing.
    pub(crate) fn take_owner(&self) -> Option<LocalCacheEntry> {
        self.owner.take()
    }
}

pub(crate) struct LocalCache {
    pub(crate) hot: HotSegment,
    // Fallback for multi-pool scenarios (rare)
    pub(crate) others: RefCell<HashMap<PoolId, LocalCacheEntry>>,
}

impl LocalCache {
    pub(crate) fn try_alloc(&self, pool_id: PoolId) -> Option<(ChunkId, SlotIndex, NonNull<u8>)> {
        let sb_ptr = self.hot.sb_state.get();

        // One-check fast path: identity match and non-null state
        if self.hot.pool_id.get() == pool_id && !sb_ptr.is_null() {
            // Safety: hot.owner guarantees that Chunk and its SuperblockState remain valid.
            let sb = unsafe { &*sb_ptr };

            if let Some(offset) = sb.alloc_one() {
                let sb_idx = self.hot.sb_idx.get();
                let chunk_id = self.hot.chunk_id.get();
                let global_idx = SlotIndex::from_superblock_offset(sb_idx, offset);

                let ptr = unsafe {
                    NonNull::new_unchecked(
                        self.hot.chunk_base.get().add(global_idx.offset()) as *mut u8
                    )
                };

                return Some((chunk_id, global_idx, ptr));
            }

            // Superblock Full! Clear hot segment to retire it and trigger global dealloc if empty.
            self.hot.clear();
        }
        self.try_alloc_slow(pool_id)
    }

    #[inline(never)]
    fn try_alloc_slow(&self, pool_id: PoolId) -> Option<(ChunkId, SlotIndex, NonNull<u8>)> {
        let mut others = self.others.borrow_mut();
        if let Some(entry) = others.remove(&pool_id) {
            // 1. Move current hot entry to others
            if let Some(old_hot_entry) = self.hot.take_owner() {
                others.insert(self.hot.pool_id.get(), old_hot_entry);
            }

            // 2. Promote this entry to hot
            self.hot.set(pool_id, entry);

            // 3. Re-try (will now hit the fast path)
            drop(others);
            return self.try_alloc(pool_id);
        }
        None
    }

    pub(crate) fn insert(&self, pool_id: PoolId, entry: LocalCacheEntry) {
        let mut others = self.others.borrow_mut();
        // Retire current hot if any
        if let Some(old_hot_entry) = self.hot.take_owner() {
            others.insert(self.hot.pool_id.get(), old_hot_entry);
        }

        // Apply new hot
        self.hot.set(pool_id, entry);
    }
}

thread_local! {
    pub(crate) static TLS_CACHE: LocalCache = LocalCache {
        hot: HotSegment::new(),
        others: RefCell::new(HashMap::new()),
    };
}
