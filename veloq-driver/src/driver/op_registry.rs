use crate::driver::PlatformOp;
use crate::driver::slot::SlotTable;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

pub struct OpEntry<P> {
    pub platform_data: P,
}

impl<P> OpEntry<P> {
    pub fn new(platform_data: P) -> Self {
        Self { platform_data }
    }
}

pub struct OpRegistry<Op: PlatformOp, P> {
    pub shared: Arc<SlotTable<Op>>,
    // Local state storage, indexed by slot index
    pub local: Box<[OpEntry<P>]>,
    // Stack of free indices
    pub free_indices: Vec<usize>,
}

impl<Op: PlatformOp, P: Default> OpRegistry<Op, P> {
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(SlotTable::new(capacity));
        let mut local = Vec::with_capacity(capacity);
        let mut free_indices = Vec::with_capacity(capacity);

        for i in 0..capacity {
            local.push(OpEntry {
                platform_data: P::default(),
            });
            // Initialize free list in reverse order so we pop 0 first
            free_indices.push(capacity - 1 - i);
        }

        Self {
            shared,
            local: local.into_boxed_slice(),
            free_indices,
        }
    }

    pub fn alloc(&mut self, data: P) -> Option<(usize, u32, &mut OpEntry<P>)> {
        // 1. Recycle remote indices
        while let Some(idx) = self.shared.remote_free_queue.pop() {
            if idx < self.local.len() {
                self.free_indices.push(idx);
            }
        }

        // 2. Alloc from free list
        if let Some(idx) = self.free_indices.pop() {
            // Reset slot generation
            let slot = &self.shared.slots[idx];
            let new_gen = slot
                .generation
                .load(std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1);
            slot.reset(new_gen);

            // Initialize local data
            self.local[idx].platform_data = data;

            Some((idx, new_gen, &mut self.local[idx]))
        } else {
            None
        }
    }

    /// Insert equivalent (for compatibility with previous interface)
    /// Note: This consumes entry but we only need platform_data.
    /// The actual resource Op should be placed into slot by caller.
    pub fn insert(&mut self, entry: OpEntry<P>) -> (usize, u32) {
        if let Some((idx, generation, _dest)) = self.alloc(entry.platform_data) {
            (idx, generation)
        } else {
            // If full, we can't easily expand with this design (fixed size).
            panic!("OpRegistry is full");
        }
    }

    pub fn get(&self, user_data: usize) -> Option<&OpEntry<P>> {
        self.local.get(user_data)
    }

    pub fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<P>> {
        self.local.get_mut(user_data)
    }

    pub fn contains(&self, user_data: usize) -> bool {
        user_data < self.local.len()
    }

    pub fn remove(&mut self, user_data: usize) -> OpEntry<P> {
        // In this fixed-size registry, remove doesn't actually remove memory.
        // It just marks index as free.
        // We return the data by replacing it with default.
        if user_data >= self.local.len() {
            panic!("Invalid user_data for remove");
        }

        let data = std::mem::replace(&mut self.local[user_data].platform_data, P::default());
        self.free_indices.push(user_data);

        OpEntry {
            platform_data: data,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn is_empty(&self) -> bool {
        self.free_indices.len() == self.local.len()
    }

    // Windows compatibility helpers
    #[cfg(target_os = "windows")]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut OpEntry<P>)> {
        self.local.iter_mut().enumerate()
    }

    #[cfg(target_os = "windows")]
    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        // Expose the slot memory. Since it's a single contiguous block,
        // page 0 is the whole thing.
        if page_idx == 0 {
            let ptr = self.shared.slots.as_ptr() as *const u8;
            let len = std::mem::size_of_val(&*self.shared.slots);
            Some((ptr, len))
        } else {
            None
        }
    }

    #[cfg(target_os = "windows")]
    pub fn page_count(&self) -> usize {
        1
    }
}

impl<Op: PlatformOp, P> Index<usize> for OpRegistry<Op, P> {
    type Output = OpEntry<P>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.local[index]
    }
}

impl<Op: PlatformOp, P> IndexMut<usize> for OpRegistry<Op, P> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.local[index]
    }
}
