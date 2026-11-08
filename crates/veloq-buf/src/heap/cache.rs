//! Thread-local cache for the heap allocator.

use super::{
    pool::Chunk,
    units::{ChunkId, SlotAllocation, SlotIndex, SuperblockIndex, SuperblockState},
};
use veloq_std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    ptr::{self, NonNull},
    sync::Arc,
};
use veloq_tls::veloq_tls;

pub(crate) struct LocalCacheEntry {
    chunk: Arc<Chunk>,
    sb_idx: SuperblockIndex,
    chunk_id: ChunkId,
}

impl LocalCacheEntry {
    pub(crate) fn new(chunk: Arc<Chunk>, sb_idx: SuperblockIndex, chunk_id: ChunkId) -> Self {
        Self {
            chunk,
            sb_idx,
            chunk_id,
        }
    }
}

impl Drop for LocalCacheEntry {
    fn drop(&mut self) {
        let should_free = self.chunk.superblock_state(self.sb_idx).set_inactive();
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
    pool_id: Cell<PoolId>,

    // --- Fast Path Flattened Primitives ---
    // These pointers/values are updated whenever `owner` is set.
    sb_state: Cell<*const SuperblockState>,
    chunk_base: Cell<*const u8>,
    sb_idx: Cell<SuperblockIndex>,
    chunk_id: Cell<ChunkId>,

    // Ownership preservation.
    // Setting this to None will automatically trigger Superblock retirement via `LocalCacheEntry::drop`.
    owner: Cell<Option<LocalCacheEntry>>,
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
        let sb_ref = entry.chunk.superblock_state(entry.sb_idx);
        self.sb_state.set(sb_ref as *const SuperblockState);
        self.chunk_base.set(entry.chunk.memory_base_ptr());
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
    hot: HotSegment,
    // Fallback for multi-pool scenarios (rare)
    others: RefCell<HashMap<(PoolId, ChunkId), LocalCacheEntry>>,
}

impl LocalCache {
    pub(crate) fn try_alloc(
        &self,
        pool_id: PoolId,
        allowed_chunks: &[ChunkId],
    ) -> Option<SlotAllocation> {
        let sb_ptr = self.hot.sb_state.get();
        let hot_chunk = self.hot.chunk_id.get();

        // One-check fast path: identity match and non-null state
        if self.hot.pool_id.get() == pool_id
            && allowed_chunks.contains(&hot_chunk)
            && !sb_ptr.is_null()
        {
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

                return Some(SlotAllocation {
                    chunk_id,
                    slot_idx: global_idx,
                    ptr,
                });
            }

            // Superblock Full! Clear hot segment to retire it and trigger global dealloc if empty.
            self.hot.clear();
        }
        self.try_alloc_slow(pool_id, allowed_chunks)
    }

    #[inline(never)]
    fn try_alloc_slow(
        &self,
        pool_id: PoolId,
        allowed_chunks: &[ChunkId],
    ) -> Option<SlotAllocation> {
        let mut others = self.others.borrow_mut();
        let entry = allowed_chunks
            .iter()
            .find_map(|chunk_id| others.remove(&(pool_id, *chunk_id)));
        if let Some(entry) = entry {
            // 1. Move current hot entry to others
            if let Some(old_hot_entry) = self.hot.take_owner() {
                others.insert(
                    (self.hot.pool_id.get(), old_hot_entry.chunk_id),
                    old_hot_entry,
                );
            }

            // 2. Promote this entry to hot
            self.hot.set(pool_id, entry);

            // 3. Re-try (will now hit the fast path)
            drop(others);
            return self.try_alloc(pool_id, allowed_chunks);
        }
        None
    }

    pub(crate) fn insert(&self, pool_id: PoolId, entry: LocalCacheEntry) {
        let mut others = self.others.borrow_mut();
        // Retire current hot if any
        if let Some(old_hot_entry) = self.hot.take_owner() {
            others.insert(
                (self.hot.pool_id.get(), old_hot_entry.chunk_id),
                old_hot_entry,
            );
        }

        // Apply new hot
        self.hot.set(pool_id, entry);
    }
}

pub(crate) struct TlsCacheWrapper;

impl TlsCacheWrapper {
    #[inline(always)]
    pub(crate) fn with<R>(&self, f: impl FnOnce(&LocalCache) -> R) -> R {
        TLS_CACHE_INNER.with_or_init(f, TLS_CACHE_INNER::init)
    }
}

veloq_tls! {
    static TLS_CACHE_INNER: LocalCache = LocalCache {
        hot: HotSegment::new(),
        others: RefCell::new(HashMap::default()),
    };
}

pub(crate) static TLS_CACHE: TlsCacheWrapper = TlsCacheWrapper;
