use std::{
    mem::MaybeUninit,
    ops::{Index, IndexMut},
};
use tracing::{debug, trace};

const PAGE_SHIFT: usize = 10;
const PAGE_SIZE: usize = 1 << PAGE_SHIFT; // 1024
const PAGE_MASK: usize = PAGE_SIZE - 1;

// 32-bit generation, 32-bit index
const INDEX_MASK: usize = 0xFFFF_FFFF;
const GEN_SHIFT: usize = 32;

enum SlotState<T> {
    Occupied(T),
    Vacant(usize),
}

struct SlotEntry<T> {
    generation: u32,
    state: SlotState<T>,
}

pub struct StableSlab<T> {
    pages: Vec<Box<[SlotEntry<T>; PAGE_SIZE]>>,
    free_head: usize,
    len: usize,
}

impl<T> StableSlab<T> {
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            free_head: usize::MAX,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut slab = Self::new();
        slab.reserve(capacity);
        slab
    }

    pub fn insert(&mut self, val: T) -> usize {
        trace!("StableSlab insert");
        let idx = if self.free_head != usize::MAX {
            self.free_head
        } else {
            self.add_page();
            self.free_head
        };

        let (page_idx, slot_idx) = Self::unpack_idx(idx);
        let slot = &mut self.pages[page_idx][slot_idx];

        match slot.state {
            SlotState::Vacant(next) => {
                self.free_head = next;
                slot.state = SlotState::Occupied(val);
                self.len += 1;
                Self::pack_key(idx, slot.generation)
            }
            SlotState::Occupied(_) => unreachable!("Corrupted free list"),
        }
    }

    pub fn remove(&mut self, key: usize) -> T {
        trace!(key, "StableSlab remove");
        let (idx, generation) = Self::unpack_key(key);
        let (page_idx, slot_idx) = Self::unpack_idx(idx);

        let slot = &mut self.pages[page_idx][slot_idx];

        if slot.generation != generation {
            panic!("StableSlab: Stale key used for removal");
        }

        // Increment generation to invalidate old keys
        slot.generation = slot.generation.wrapping_add(1);

        let new_state = SlotState::Vacant(self.free_head);
        let old_state = std::mem::replace(&mut slot.state, new_state);

        match old_state {
            SlotState::Occupied(val) => {
                self.free_head = idx;
                self.len -= 1;
                val
            }
            SlotState::Vacant(_) => panic!("StableSlab: Removing already vacant slot"),
        }
    }

    pub fn get(&self, key: usize) -> Option<&T> {
        let (idx, generation) = Self::unpack_key(key);
        let (page_idx, slot_idx) = Self::unpack_idx(idx);

        if let Some(page) = self.pages.get(page_idx) {
            // SAFETY: slot_idx is masked by PAGE_MASK (1023)
            let slot = unsafe { page.get_unchecked(slot_idx) };
            if slot.generation == generation {
                if let SlotState::Occupied(val) = &slot.state {
                    return Some(val);
                }
            }
        }
        None
    }

    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        let (idx, generation) = Self::unpack_key(key);
        let (page_idx, slot_idx) = Self::unpack_idx(idx);

        if let Some(page) = self.pages.get_mut(page_idx) {
            let slot = unsafe { page.get_unchecked_mut(slot_idx) };
            if slot.generation == generation {
                if let SlotState::Occupied(val) = &mut slot.state {
                    return Some(val);
                }
            }
        }
        None
    }

    pub fn contains(&self, key: usize) -> bool {
        self.get(key).is_some()
    }

    #[cfg(target_os = "linux")]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn reserve(&mut self, additional: usize) {
        let available = (self.pages.len() * PAGE_SIZE) - self.len;
        if additional > available {
            let needed = additional - available;
            let pages_needed = needed.div_ceil(PAGE_SIZE);
            debug!(pages_needed, "Reserving pages");
            for _ in 0..pages_needed {
                self.add_page();
            }
        }
    }

    fn add_page(&mut self) {
        let page_idx = self.pages.len();
        debug!(page_idx, "Adding new slab page");
        let start_idx = page_idx * PAGE_SIZE;

        let mut page: Box<[MaybeUninit<SlotEntry<T>>]> = Box::new_uninit_slice(PAGE_SIZE);

        let old_head = self.free_head;
        for i in 0..PAGE_SIZE - 1 {
            let slot = SlotEntry {
                generation: 0,
                state: SlotState::Vacant(start_idx + i + 1),
            };
            page[i].write(slot);
        }
        // Last one points to old head
        page[PAGE_SIZE - 1].write(SlotEntry {
            generation: 0,
            state: SlotState::Vacant(old_head),
        });

        let page = unsafe { page.assume_init() };
        let page_ptr = Box::into_raw(page) as *mut [SlotEntry<T>; PAGE_SIZE];
        let boxed_page = unsafe { Box::from_raw(page_ptr) };
        self.pages.push(boxed_page);
        self.free_head = start_idx;
    }

    #[cfg(target_os = "windows")]
    pub const PAGE_SHIFT: usize = PAGE_SHIFT;

    // Returns raw index only.
    #[inline(always)]
    #[cfg(target_os = "windows")]
    pub const fn index_mask() -> usize {
        INDEX_MASK
    }

    #[inline(always)]
    fn unpack_idx(idx: usize) -> (usize, usize) {
        (idx >> PAGE_SHIFT, idx & PAGE_MASK)
    }

    #[inline(always)]
    fn pack_key(idx: usize, generation: u32) -> usize {
        ((generation as usize) << GEN_SHIFT) | idx
    }

    #[inline(always)]
    fn unpack_key(key: usize) -> (usize, u32) {
        (key & INDEX_MASK, (key >> GEN_SHIFT) as u32)
    }

    /// Returns the raw memory slice for a given page index.
    #[cfg(target_os = "windows")]
    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        self.pages.get(page_idx).map(|page| {
            let ptr = page.as_ptr() as *const u8;
            let len = std::mem::size_of_val(&**page);
            (ptr, len)
        })
    }

    #[cfg(target_os = "windows")]
    #[inline(always)]
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }
}

impl<T> Index<usize> for StableSlab<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("invalid key")
    }
}

impl<T> IndexMut<usize> for StableSlab<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("invalid key")
    }
}

#[cfg(target_os = "windows")]
impl<T> StableSlab<T> {
    // Note: Iteration yields Valid Entries.
    // The key returned must be a packed key (Generation + Index).
    // This is expensive if we don't store the full key, but we store generation.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut T)> {
        self.pages
            .iter_mut()
            .enumerate()
            .flat_map(|(page_idx, page)| {
                page.iter_mut()
                    .enumerate()
                    .map(move |(slot_idx, slot)| (page_idx, slot_idx, slot))
            })
            .filter_map(|(page_idx, slot_idx, slot)| {
                if let SlotState::Occupied(val) = &mut slot.state {
                    let idx = (page_idx << PAGE_SHIFT) | slot_idx;
                    let key = StableSlab::<T>::pack_key(idx, slot.generation);
                    Some((key, val))
                } else {
                    None
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_crud() {
        let mut slab = StableSlab::new();
        let k1 = slab.insert(10);
        let k2 = slab.insert(20);

        assert_eq!(slab[k1], 10);
        assert_eq!(slab[k2], 20);
        assert_eq!(slab.len(), 2);

        let v = slab.remove(k1);
        assert_eq!(v, 10);
        assert!(slab.get(k1).is_none());
        assert_eq!(slab.len(), 1);

        // Re-insert should reuse slot but update generation
        let k3 = slab.insert(30);
        assert_eq!(slab[k3], 30);

        let (idx1, gen1) = StableSlab::<i32>::unpack_key(k1);
        let (idx3, gen3) = StableSlab::<i32>::unpack_key(k3);

        assert_eq!(idx1, idx3); // Same slot index
        assert_ne!(gen1, gen3); // Different generation check
        assert!(slab.get(k1).is_none()); // Old key is invalid
    }

    #[test]
    fn test_growth_and_stability() {
        let mut slab = StableSlab::new();
        let mut keys = Vec::new();

        // Fill first page
        for i in 0..PAGE_SIZE {
            keys.push(slab.insert(i));
        }

        // Capture address of first element
        let ptr1 = &slab[keys[0]] as *const _ as usize;

        // Add one more
        let k_new = slab.insert(9999);
        keys.push(k_new);

        // Check address of first element again
        let ptr2 = &slab[keys[0]] as *const _ as usize;
        assert_eq!(ptr1, ptr2, "Address must remain stable");

        // Verify Content
        for i in 0..PAGE_SIZE {
            assert_eq!(slab[keys[i]], i);
        }
        assert_eq!(slab[k_new], 9999);
    }

    #[test]
    fn test_sparse_removal() {
        let mut slab = StableSlab::new();
        let mut keys = Vec::new();
        for i in 0..2000 {
            keys.push(slab.insert(i));
        }

        for i in (0..2000).step_by(2) {
            slab.remove(keys[i]);
        }

        assert_eq!(slab.len(), 1000);

        for i in (1..2000).step_by(2) {
            assert_eq!(slab[keys[i]], i);
        }

        for i in 0..500 {
            slab.insert(10000 + i);
        }

        assert_eq!(slab.len(), 1500);
    }
}
