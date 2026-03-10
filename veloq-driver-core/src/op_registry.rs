use crate::driver::PlatformOp;
use crate::slot::{SlotEntry, SlotTable};
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
    pub local: Box<[OpEntry<P>]>,
    local_free_head: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpHandle {
    pub index: usize,
    pub generation: u32,
}

pub struct AllocResult {
    pub handle: OpHandle,
}

impl<Op: PlatformOp, P: Default> OpRegistry<Op, P> {
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(SlotTable::new(capacity));
        let mut local = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            local.push(OpEntry {
                platform_data: P::default(),
            });
        }
        for i in (0..capacity).rev() {
            shared.push_free(i);
        }

        Self {
            shared,
            local: local.into_boxed_slice(),
            local_free_head: SlotTable::<Op>::NULL_INDEX,
        }
    }

    pub fn alloc(&mut self, data: P) -> Result<AllocResult, P> {
        if self.local_free_head == SlotTable::<Op>::NULL_INDEX {
            self.local_free_head = self.shared.pop_all();
        }

        if self.local_free_head != SlotTable::<Op>::NULL_INDEX {
            let idx = self.local_free_head;
            self.local_free_head = self.shared.slots[idx]
                .next_free
                .load(std::sync::atomic::Ordering::Relaxed);

            let slot = &self.shared.slots[idx];
            let new_gen = slot
                .generation
                .load(std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1);
            slot.reset(new_gen);

            self.local[idx].platform_data = data;

            Ok(AllocResult {
                handle: OpHandle {
                    index: idx,
                    generation: new_gen,
                },
            })
        } else {
            Err(data)
        }
    }

    pub fn insert(&mut self, entry: OpEntry<P>) -> Result<OpHandle, OpEntry<P>> {
        match self.alloc(entry.platform_data) {
            Ok(res) => Ok(res.handle),
            Err(data) => Err(OpEntry {
                platform_data: data,
            }),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn get(&self, user_data: usize) -> Option<&OpEntry<P>> {
        self.local.get(user_data)
    }

    #[cfg(target_os = "linux")]
    pub fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<P>> {
        self.local.get_mut(user_data)
    }

    pub fn get_slot_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<(&SlotEntry<Op>, &mut OpEntry<P>)> {
        if user_data < self.local.len() {
            Some((&self.shared.slots[user_data], &mut self.local[user_data]))
        } else {
            None
        }
    }

    pub fn contains(&self, user_data: usize) -> bool {
        user_data < self.local.len()
    }

    pub fn remove(&mut self, user_data: usize) -> OpEntry<P> {
        if user_data >= self.local.len() {
            panic!("Invalid user_data for remove");
        }

        let data = std::mem::take(&mut self.local[user_data].platform_data);
        self.shared.push_free(user_data);

        OpEntry {
            platform_data: data,
        }
    }

    #[cfg(target_os = "windows")]
    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        if page_idx == 0 {
            let ptr = self.shared.slots.as_ptr() as *const u8;
            let len = std::mem::size_of_val(&*self.shared.slots);
            Some((ptr, len))
        } else {
            None
        }
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
