use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::slot::{ErasedPayload, SlotEntry, SlotStorage, SlotTable};
use std::ops::{Index, IndexMut};
use std::sync::Arc;
use veloq_shim::atomic::Ordering;

pub struct OpEntry<P> {
    pub platform_data: P,
}

impl<P> OpEntry<P> {
    pub fn new(platform_data: P) -> Self {
        Self { platform_data }
    }
}

pub struct LocalSlot<Op, P, S: SlotSidecar> {
    op: Option<Op>,
    pub entry: OpEntry<P>,
    pub storage: SlotStorage<Op, S>,
}

impl<Op, P: Default, S: SlotSidecar> LocalSlot<Op, P, S> {
    #[inline]
    fn new() -> Self {
        Self {
            op: None,
            entry: OpEntry {
                platform_data: P::default(),
            },
            storage: SlotStorage::new(),
        }
    }
}

pub struct OpRegistry<Op: PlatformOp, P, S: SlotSidecar> {
    pub shared: Arc<SlotTable<Op, S>>,
    pub local: Box<[LocalSlot<Op, P, S>]>,
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

type SlotEntryOpBundle<'a, Op, P, S> = (
    &'a SlotEntry<Op, S>,
    &'a mut OpEntry<P>,
    &'a mut Option<Op>,
    &'a mut SlotStorage<Op, S>,
);

impl<Op: PlatformOp, P: Default, S: SlotSidecar> OpRegistry<Op, P, S> {
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(SlotTable::new(capacity));
        let mut local = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            local.push(LocalSlot::new());
        }
        for i in (0..capacity).rev() {
            shared.push_free(i);
        }

        Self {
            shared,
            local: local.into_boxed_slice(),
            local_free_head: SlotTable::<Op, S>::NULL_INDEX,
        }
    }

    pub fn alloc(&mut self, data: P) -> Result<AllocResult, P> {
        if self.local_free_head == SlotTable::<Op, S>::NULL_INDEX {
            self.local_free_head = self.shared.pop_all();
        }

        if self.local_free_head != SlotTable::<Op, S>::NULL_INDEX {
            let idx = self.local_free_head;
            self.local_free_head = self.shared.slots[idx].next_free.load(Ordering::Relaxed);

            let slot = &self.shared.slots[idx];
            let new_gen = slot.generation.load(Ordering::Relaxed).wrapping_add(1);
            slot.reset(new_gen);
            slot.set_state(crate::slot::SlotState::Pending, Ordering::Release);

            self.local[idx].op = None;
            self.local[idx].entry.platform_data = data;
            self.local[idx].storage.reset();

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

    pub fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<P>> {
        self.local.get_mut(user_data).map(|v| &mut v.entry)
    }

    pub fn get_slot_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<(&SlotEntry<Op, S>, &mut OpEntry<P>)> {
        if user_data < self.local.len() {
            Some((
                &self.shared.slots[user_data],
                &mut self.local[user_data].entry,
            ))
        } else {
            None
        }
    }

    pub fn get_slot_entry_op_storage_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<SlotEntryOpBundle<'_, Op, P, S>> {
        if user_data < self.local.len() {
            let local = &mut self.local[user_data];
            Some((
                &self.shared.slots[user_data],
                &mut local.entry,
                &mut local.op,
                &mut local.storage,
            ))
        } else {
            None
        }
    }

    #[inline]
    pub fn with_slot_storage_mut<F, R>(&mut self, user_data: usize, f: F) -> Option<R>
    where
        F: FnOnce(
            &mut Option<Op>,
            &mut Option<std::io::Result<usize>>,
            &mut Option<ErasedPayload>,
            &mut S,
        ) -> R,
    {
        self.local
            .get_mut(user_data)
            .map(|local| local.storage.with_mut(f))
    }

    pub fn slot_storage_mut(&mut self, user_data: usize) -> Option<&mut SlotStorage<Op, S>> {
        if user_data < self.local.len() {
            Some(&mut self.local[user_data].storage)
        } else {
            None
        }
    }

    pub fn contains(&self, user_data: usize) -> bool {
        user_data < self.local.len()
    }

    pub fn remove(&mut self, user_data: usize) -> OpEntry<P> {
        assert!(user_data < self.local.len(), "Invalid user_data for remove");

        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let data = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();
        self.shared.slots[user_data].free();
        self.shared.push_free(user_data);

        OpEntry {
            platform_data: data,
        }
    }

    pub fn recycle(&mut self, user_data: usize, generation: u32) {
        assert!(
            user_data < self.local.len(),
            "Invalid user_data for recycle"
        );

        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let _ = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();

        self.shared.slots[user_data].reset(generation);
        self.shared.push_free(user_data);
    }

    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        if page_idx == 0 {
            let ptr = self.local.as_ptr() as *const u8;
            let len = std::mem::size_of_val(&*self.local);
            Some((ptr, len))
        } else {
            None
        }
    }
}

impl<Op: PlatformOp, P, S: SlotSidecar> Index<usize> for OpRegistry<Op, P, S> {
    type Output = OpEntry<P>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.local[index].entry
    }
}

impl<Op: PlatformOp, P, S: SlotSidecar> IndexMut<usize> for OpRegistry<Op, P, S> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.local[index].entry
    }
}
